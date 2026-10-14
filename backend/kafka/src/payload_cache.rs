//! The glue between a fetch and [`salty_cache::MessageCache`].
//!
//! The cache itself lives in its own crate and knows nothing about Kafka; this
//! module is the part that does: which fetches are a "click on a row" read
//! that the cache may answer, how a polled message becomes a cache entry, and
//! how an entry becomes the same `TopicMessage` the broker would have given.
//! Pure, so each rule is unit-tested without a broker.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use salty_cache::{CacheConfig, CachedHeader, CachedMessage, MessageCache};
use salty_core::{MessageFilter, MessageHeader, TopicMessage};

/// The one message a filter asks for, when it is a request for that message
/// *whole* — which is what opening a row in the payload viewer sends.
///
/// Exactly one partition, an explicit offset, a total of one, no timestamp
/// bounds, and no preview cap. Anything else is a browse, which the cache
/// never answers (it would have to know the range is complete) — only feeds.
pub fn point_read_target(filter: &MessageFilter) -> Option<(i32, i64)> {
    let [partition] = filter.partitions.as_deref()? else {
        return None;
    };
    let offset = filter.offset?;
    let whole = filter.max_total_messages == Some(1)
        && filter.max_payload_preview_bytes.is_none()
        && filter.from_timestamp_ms.is_none()
        && filter.to_timestamp_ms.is_none();
    whole.then_some((*partition, offset))
}

/// A polled message as a cache entry: the whole payload, plus what the viewer
/// needs beside it.
pub fn to_cached(
    timestamp_ms: Option<i64>,
    key: Option<&[u8]>,
    headers: &[MessageHeader],
    payload: &[u8],
) -> CachedMessage {
    to_cached_into(
        Vec::with_capacity(payload.len()),
        timestamp_ms,
        key,
        headers,
        payload,
    )
}

/// [`to_cached`], copying the payload into `buffer` — one from
/// [`MessageCache::take_buffer`], so the copy on the fetch thread reuses
/// memory instead of allocating it.
pub fn to_cached_into(
    mut buffer: Vec<u8>,
    timestamp_ms: Option<i64>,
    key: Option<&[u8]>,
    headers: &[MessageHeader],
    payload: &[u8],
) -> CachedMessage {
    buffer.clear();
    buffer.extend_from_slice(payload);
    CachedMessage {
        timestamp_ms,
        key: key.map(<[u8]>::to_vec),
        headers: headers
            .iter()
            .map(|header| CachedHeader {
                key: header.key.clone(),
                value: header
                    .value_base64
                    .as_deref()
                    .and_then(|v| BASE64.decode(v).ok()),
            })
            .collect(),
        payload: buffer,
    }
}

/// What the broker's own answer would have been for this message.
pub fn to_topic_message(partition: i32, offset: i64, cached: &CachedMessage) -> TopicMessage {
    TopicMessage {
        partition,
        offset,
        timestamp_ms: cached.timestamp_ms,
        key_base64: cached.key.as_deref().map(|k| BASE64.encode(k)),
        payload_base64: Some(BASE64.encode(&cached.payload)),
        payload_size_bytes: Some(cached.payload.len() as u64),
        headers: cached
            .headers
            .iter()
            .map(|header| MessageHeader {
                key: header.key.clone(),
                value_base64: header.value.as_deref().map(|v| BASE64.encode(v)),
            })
            .collect(),
    }
}

/// The app's shared payload cache, reporting to the Logs panel through the
/// client log sink. A newtype so `RdKafkaClient` can keep deriving `Default`.
pub struct PayloadCache(pub MessageCache);

impl Default for PayloadCache {
    fn default() -> Self {
        Self(MessageCache::with_listener(
            CacheConfig::default(),
            |event| crate::client_log::log_cache_line(event.message()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point() -> MessageFilter {
        MessageFilter {
            partitions: Some(vec![2]),
            max_messages_per_partition: Some(1),
            max_total_messages: Some(1),
            offset: Some(1042),
            ..MessageFilter::default()
        }
    }

    #[test]
    fn a_single_message_whole_payload_request_is_a_point_read() {
        assert_eq!(point_read_target(&point()), Some((2, 1042)));
    }

    #[test]
    fn a_browse_is_never_a_point_read() {
        assert_eq!(point_read_target(&MessageFilter::default()), None);
        let mut many = point();
        many.max_total_messages = Some(100);
        assert_eq!(point_read_target(&many), None);
        let mut two_partitions = point();
        two_partitions.partitions = Some(vec![0, 1]);
        assert_eq!(point_read_target(&two_partitions), None);
        let mut no_offset = point();
        no_offset.offset = None;
        assert_eq!(point_read_target(&no_offset), None);
    }

    #[test]
    fn a_capped_preview_request_is_not_a_point_read() {
        // It asks for a truncated payload; the cache holds whole ones.
        let mut capped = point();
        capped.max_payload_preview_bytes = Some(4096);
        assert_eq!(point_read_target(&capped), None);
    }

    #[test]
    fn a_timestamp_bounded_request_is_not_a_point_read() {
        let mut timed = point();
        timed.from_timestamp_ms = Some(1);
        assert_eq!(point_read_target(&timed), None);
    }

    #[test]
    fn copying_into_a_supplied_buffer_keeps_its_allocation_and_clears_it() {
        let mut buffer = Vec::with_capacity(1_000);
        buffer.extend_from_slice(b"stale bytes from an earlier message");
        let pointer = buffer.as_ptr();
        let cached = to_cached_into(buffer, Some(1), None, &[], b"fresh");
        assert_eq!(cached.payload, b"fresh");
        assert_eq!(cached.payload.as_ptr(), pointer);
    }

    #[test]
    fn a_message_survives_the_trip_through_the_cache_shape() {
        let headers = vec![
            MessageHeader {
                key: "trace".into(),
                value_base64: Some(BASE64.encode(b"abc")),
            },
            MessageHeader {
                key: "empty".into(),
                value_base64: None,
            },
        ];
        let cached = to_cached(Some(7), Some(b"k1"), &headers, b"payload bytes");

        let message = to_topic_message(2, 1042, &cached);

        assert_eq!(message.partition, 2);
        assert_eq!(message.offset, 1042);
        assert_eq!(message.timestamp_ms, Some(7));
        assert_eq!(message.key_base64, Some(BASE64.encode(b"k1")));
        assert_eq!(
            message.payload_base64,
            Some(BASE64.encode(b"payload bytes"))
        );
        assert_eq!(message.payload_size_bytes, Some(13));
        assert_eq!(message.headers, headers);
    }
}
