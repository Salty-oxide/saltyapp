//! Publishing messages to a partition: the wire types the Publish tab sends,
//! every validation rule applied to them, and the gate that decides whether a
//! publish is allowed to reach a broker at all.
//!
//! **Why all of this lives here rather than in the command layer.** Publishing
//! is the one action in this app that writes to someone's cluster, so the
//! rules that govern it are the rules most worth testing. `src-tauri` cannot
//! be built without a desktop toolchain and is excluded from coverage
//! entirely (see the Conventions in CLAUDE.md), and `backend/kafka` needs a
//! real broker for anything interesting. This module depends on neither: it
//! turns text into bytes, and answers "may this proceed", as plain functions.
//! The Tauri command is a wrapper that calls them and therefore cannot get the
//! order wrong.
//!
//! **Why encoding happens in Rust and not in the frontend.** The Publish tab
//! validates as the user types, but that is a convenience, not a control: a
//! crafted IPC call reaches the command directly. So the frontend's encoded
//! bytes are never trusted — it sends the declared encoding plus the raw text,
//! and [`encode_messages`] is the only thing that produces bytes.

use crate::Result;
use base64::Engine;
use error_stack::Report;
use serde::{Deserialize, Serialize};

use crate::error::AppError;

/// How the text entered in a key, value, or header-value field becomes bytes.
///
/// `Null` is a distinct choice rather than "the field was left empty" because
/// Kafka distinguishes a null key from a zero-length key: a null key is
/// routed round-robin and read back as `None`, an empty key hashes like any
/// other and is read back as `Some([])`. Only the user can say which was
/// meant, so the UI asks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PayloadEncoding {
    /// No bytes at all — a null key/value/header value.
    Null,
    /// The entered string's UTF-8 bytes, verbatim.
    Text,
    /// Parsed as JSON first, then sent as the same UTF-8 bytes. The parse is
    /// purely a gate: malformed JSON is rejected here instead of landing on
    /// the topic, where nothing downstream can fix it.
    Json,
    /// Decoded from base64 into arbitrary binary — the escape hatch for
    /// payloads that are not text, mirroring how the Data tab already carries
    /// payloads in both directions.
    Base64,
}

/// One entered field, as the Publish tab sends it: what the user chose, and
/// what they typed. `text` is ignored for [`PayloadEncoding::Null`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishField {
    pub encoding: PayloadEncoding,
    #[serde(default)]
    pub text: String,
}

impl PublishField {
    pub fn null() -> Self {
        PublishField {
            encoding: PayloadEncoding::Null,
            text: String::new(),
        }
    }

    pub fn text(text: impl Into<String>) -> Self {
        PublishField {
            encoding: PayloadEncoding::Text,
            text: text.into(),
        }
    }
}

/// A header pair as entered. The key is always text — the Kafka protocol
/// defines header keys as strings — while the value is an arbitrary byte
/// string and so gets the same encoding choice as a payload. See
/// [`crate::MessageHeader`], which carries the read side of the same
/// asymmetry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishHeaderInput {
    pub key: String,
    pub value: PublishField,
}

/// One message the user wants to publish, before validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewPublishMessage {
    pub key: PublishField,
    pub value: PublishField,
    #[serde(default)]
    pub headers: Vec<PublishHeaderInput>,
}

/// A validated message: bytes, ready to hand to a producer. Constructed only
/// by [`encode_messages`], so possessing one is proof the rules were applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedRecord {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
}

impl EncodedRecord {
    /// Total bytes this record will occupy, counting the header keys and
    /// values as well as the key and payload. The broker's own
    /// `message.max.bytes` check covers the whole record, so a size check
    /// that looked only at the payload would pass records the broker then
    /// rejects — after some of the batch had already been written.
    pub fn size_bytes(&self) -> u64 {
        let key = self.key.as_ref().map_or(0, |bytes| bytes.len());
        let value = self.value.as_ref().map_or(0, |bytes| bytes.len());
        let headers: usize = self
            .headers
            .iter()
            .map(|(name, value)| name.len() + value.as_ref().map_or(0, |bytes| bytes.len()))
            .sum();
        (key + value + headers) as u64
    }
}

/// The ceilings a batch is checked against.
///
/// `max_message_size_bytes` is the user's General settings > Messages > Max
/// Message Size, the same value the fetch path applies as
/// `max.partition.fetch.bytes` — a record larger than the size they told the
/// app to deal with is a mistake worth catching before the broker does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishLimits {
    pub max_message_size_bytes: u32,
    pub max_batch_messages: usize,
    pub max_batch_bytes: u64,
}

/// How many messages one publish may carry.
///
/// A ceiling exists at all because this dialog sends records one at a time and
/// awaits each acknowledgement (see the producer): a runaway batch would hold
/// the UI for as long as it took, with no way to tell how far it had got. A
/// hundred is well past what anyone hand-authors in a list of editable rows,
/// and far short of anything that looks like a load test — which this feature
/// deliberately is not.
pub const MAX_PUBLISH_BATCH_MESSAGES: usize = 100;

/// How many bytes one publish may carry in total, across every message.
/// Bounds the case the per-message ceiling misses: a hundred individually
/// legal multi-megabyte records.
pub const MAX_PUBLISH_BATCH_BYTES: u64 = 64 * 1024 * 1024;

impl PublishLimits {
    pub fn for_max_message_size(max_message_size_bytes: u32) -> Self {
        PublishLimits {
            max_message_size_bytes,
            max_batch_messages: MAX_PUBLISH_BATCH_MESSAGES,
            max_batch_bytes: MAX_PUBLISH_BATCH_BYTES,
        }
    }
}

fn encode_field(field: &PublishField, what: &str) -> Result<Option<Vec<u8>>, AppError> {
    match field.encoding {
        PayloadEncoding::Null => Ok(None),
        PayloadEncoding::Text => Ok(Some(field.text.as_bytes().to_vec())),
        PayloadEncoding::Json => {
            serde_json::from_str::<serde_json::Value>(&field.text).map_err(|err| {
                Report::new(AppError::Validation)
                    .attach(format!("the {what} is not valid JSON: {err}"))
            })?;
            Ok(Some(field.text.as_bytes().to_vec()))
        }
        PayloadEncoding::Base64 => {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(field.text.trim())
                .map_err(|err| {
                    Report::new(AppError::Validation)
                        .attach(format!("the {what} is not valid base64: {err}"))
                })?;
            Ok(Some(bytes))
        }
    }
}

/// Turns entered messages into records, or explains why it cannot.
///
/// Every message is validated before any is returned, so a batch either
/// reaches the broker whole or never starts. Discovering message 40's bad
/// base64 after 39 records had been written would leave the topic in a state
/// the user did not ask for and cannot undo.
pub fn encode_messages(
    messages: &[NewPublishMessage],
    limits: &PublishLimits,
) -> Result<Vec<EncodedRecord>, AppError> {
    if messages.is_empty() {
        return Err(Report::new(AppError::Validation).attach("there are no messages to publish"));
    }
    if messages.len() > limits.max_batch_messages {
        return Err(Report::new(AppError::Validation).attach(format!(
            "one publish carries at most {} messages, but {} were given",
            limits.max_batch_messages,
            messages.len()
        )));
    }

    let mut records = Vec::with_capacity(messages.len());
    let mut total_bytes: u64 = 0;

    for (index, message) in messages.iter().enumerate() {
        // One-based in every message the user reads: the rows are numbered
        // that way in the dialog, and "message 0" sends them counting.
        let position = index + 1;
        let key = encode_field(&message.key, &format!("key of message {position}"))?;
        let value = encode_field(&message.value, &format!("value of message {position}"))?;

        let mut headers = Vec::with_capacity(message.headers.len());
        for header in &message.headers {
            if header.key.trim().is_empty() {
                return Err(Report::new(AppError::Validation)
                    .attach(format!("message {position} has a header with no name")));
            }
            let bytes = encode_field(
                &header.value,
                &format!("value of header \"{}\" on message {position}", header.key),
            )?;
            headers.push((header.key.clone(), bytes));
        }

        let record = EncodedRecord {
            key,
            value,
            headers,
        };
        let size = record.size_bytes();
        if size > u64::from(limits.max_message_size_bytes) {
            return Err(Report::new(AppError::Validation).attach(format!(
                "message {position} is {size} bytes, over the {} byte Max Message Size",
                limits.max_message_size_bytes
            )));
        }
        total_bytes = total_bytes.saturating_add(size);
        if total_bytes > limits.max_batch_bytes {
            return Err(Report::new(AppError::Validation).attach(format!(
                "these messages total more than the {} byte publish limit",
                limits.max_batch_bytes
            )));
        }
        records.push(record);
    }

    Ok(records)
}

/// A message that reached the broker and was acknowledged, identified by its
/// position in the submitted batch so the UI can mark that row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliveredRecord {
    /// Zero-based index into the submitted message list.
    pub index: usize,
    pub partition: i32,
    pub offset: i64,
}

/// What kind of failure stopped a publish.
///
/// Carried alongside the reason string rather than left to be re-derived from
/// it, because the command layer acts differently on each: `Authorization` is
/// recorded against (connection, topic) and blocks further publishes to that
/// topic; `Authentication` is fed to the credential circuit breaker; the other
/// two are reported and forgotten. Matching on wording to decide that would
/// break the first time librdkafka rephrased a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PublishFailureKind {
    /// The broker refused the write: this principal lacks the ACL for it.
    Authorization,
    /// The broker rejected the connection's credentials.
    Authentication,
    /// The message itself was unacceptable — too large, unknown partition.
    Validation,
    /// Anything worth trying again: a timeout, a transport failure.
    Transient,
}

impl PublishFailureKind {
    pub fn from_error(error: &AppError) -> Self {
        match error {
            AppError::Authorization => PublishFailureKind::Authorization,
            AppError::Authentication => PublishFailureKind::Authentication,
            AppError::Validation => PublishFailureKind::Validation,
            _ => PublishFailureKind::Transient,
        }
    }
}

/// The one message that failed, and why. There is at most one: the producer
/// stops at the first failure rather than pressing on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishFailure {
    pub index: usize,
    pub kind: PublishFailureKind,
    pub reason: String,
}

/// What a publish did, in full: what landed, what failed, and what was never
/// tried. Reported even when the publish failed part-way — especially then,
/// because "some of your messages are on the topic" is the thing the user most
/// needs to know and cannot find out any other way.
///
/// `rename_all = "camelCase"` matters here as much as it does for
/// `MessagesBatchEvent`, whose `request_id` once crossed the boundary
/// snake_case and silently broke every frontend match against it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PublishOutcome {
    pub delivered: Vec<DeliveredRecord>,
    pub failure: Option<PublishFailure>,
    /// Messages after the failure, which were deliberately not sent.
    pub not_attempted: usize,
}

impl PublishOutcome {
    pub fn succeeded(&self) -> bool {
        self.failure.is_none()
    }
}

/// Why a publish is being refused without contacting the broker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind", content = "detail")]
pub enum PublishRefusal {
    /// The cluster is not connected in this run of the app.
    NotConnected,
    /// This connection's `allow_publishing` flag is off.
    NotAllowed,
    /// The broker has already refused a publish to this topic for this
    /// connection; the reason it gave is carried along.
    BrokerDenied(String),
}

impl PublishRefusal {
    /// The sentence shown to the user, and the one returned from the command.
    /// Each says what to do about it, because each has a different answer.
    pub fn message(&self, topic: &str) -> String {
        match self {
            PublishRefusal::NotConnected => {
                "this cluster is not connected — use Connect before publishing".to_string()
            }
            PublishRefusal::NotAllowed => {
                "publishing is disabled for this connection — enable \"Allow publishing\" in the \
                 connection's Properties tab if you intend to write to this cluster"
                    .to_string()
            }
            PublishRefusal::BrokerDenied(reason) => format!(
                "the broker refused an earlier publish to \"{topic}\": {reason}. \
                 Publishing to it is blocked until you reconnect."
            ),
        }
    }

    /// Which `AppError` this refusal reports as. A missing write grant is an
    /// authorization failure; the other two are the app's own state.
    pub fn error(&self) -> AppError {
        match self {
            PublishRefusal::NotConnected | PublishRefusal::NotAllowed => AppError::Validation,
            PublishRefusal::BrokerDenied(_) => AppError::Authorization,
        }
    }
}

/// The gate: whether a publish may proceed, given everything knowable without
/// touching the network.
///
/// Ordered deliberately, and the order is the point of testing it here. A
/// disconnected cluster is reported as disconnected even if publishing is also
/// disabled, because Connect is the first thing the user must do either way;
/// and a connection with publishing switched off is told so rather than being
/// shown a stale broker denial it can no longer act on.
pub fn publish_refusal(
    connected: bool,
    allow_publishing: bool,
    broker_denial: Option<&str>,
) -> Option<PublishRefusal> {
    if !connected {
        return Some(PublishRefusal::NotConnected);
    }
    if !allow_publishing {
        return Some(PublishRefusal::NotAllowed);
    }
    broker_denial.map(|reason| PublishRefusal::BrokerDenied(reason.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> PublishLimits {
        PublishLimits::for_max_message_size(1024)
    }

    fn message(key: PublishField, value: PublishField) -> NewPublishMessage {
        NewPublishMessage {
            key,
            value,
            headers: Vec::new(),
        }
    }

    fn reasons(report: &Report<AppError>) -> String {
        use error_stack::{AttachmentKind, FrameKind};
        report
            .frames()
            .filter_map(|frame| match frame.kind() {
                FrameKind::Attachment(AttachmentKind::Printable(printable)) => {
                    Some(printable.to_string())
                }
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("; ")
    }

    #[test]
    fn text_is_sent_as_its_utf8_bytes() {
        let records = encode_messages(
            &[message(PublishField::null(), PublishField::text("héllo"))],
            &limits(),
        )
        .expect("text should encode");
        assert_eq!(records[0].value, Some("héllo".as_bytes().to_vec()));
    }

    #[test]
    fn a_null_key_is_not_an_empty_key() {
        let records = encode_messages(
            &[
                message(PublishField::null(), PublishField::text("a")),
                message(PublishField::text(""), PublishField::text("b")),
            ],
            &limits(),
        )
        .expect("both forms should encode");
        assert_eq!(records[0].key, None, "a null key must stay absent");
        assert_eq!(
            records[1].key,
            Some(Vec::new()),
            "an empty key is present and zero-length, which Kafka partitions differently"
        );
    }

    #[test]
    fn a_null_value_is_a_tombstone_not_an_empty_payload() {
        let records = encode_messages(
            &[message(PublishField::text("k"), PublishField::null())],
            &limits(),
        )
        .expect("a null value should encode");
        assert_eq!(records[0].value, None);
    }

    #[test]
    fn valid_json_is_accepted_and_sent_verbatim() {
        let json = r#"{"b":1,"a":2}"#;
        let records = encode_messages(
            &[message(
                PublishField::null(),
                PublishField {
                    encoding: PayloadEncoding::Json,
                    text: json.to_string(),
                },
            )],
            &limits(),
        )
        .expect("valid JSON should encode");
        assert_eq!(
            records[0].value,
            Some(json.as_bytes().to_vec()),
            "the bytes must be what the user typed, not a re-serialization that reorders keys"
        );
    }

    #[test]
    fn malformed_json_is_rejected_rather_than_published() {
        let report = encode_messages(
            &[message(
                PublishField::null(),
                PublishField {
                    encoding: PayloadEncoding::Json,
                    text: "{\"a\": ".to_string(),
                },
            )],
            &limits(),
        )
        .expect_err("malformed JSON must not reach a topic");
        assert!(matches!(report.current_context(), AppError::Validation));
        assert!(reasons(&report).contains("value of message 1 is not valid JSON"));
    }

    #[test]
    fn base64_is_decoded_to_binary() {
        let records = encode_messages(
            &[message(
                PublishField::null(),
                PublishField {
                    encoding: PayloadEncoding::Base64,
                    text: "AAECqg==".to_string(),
                },
            )],
            &limits(),
        )
        .expect("valid base64 should encode");
        assert_eq!(records[0].value, Some(vec![0x00, 0x01, 0x02, 0xaa]));
    }

    #[test]
    fn base64_tolerates_the_whitespace_a_paste_brings_with_it() {
        let records = encode_messages(
            &[message(
                PublishField::null(),
                PublishField {
                    encoding: PayloadEncoding::Base64,
                    text: "  AAECqg==\n".to_string(),
                },
            )],
            &limits(),
        )
        .expect("surrounding whitespace should not fail a paste");
        assert_eq!(records[0].value, Some(vec![0x00, 0x01, 0x02, 0xaa]));
    }

    #[test]
    fn invalid_base64_is_rejected() {
        let report = encode_messages(
            &[message(
                PublishField::null(),
                PublishField {
                    encoding: PayloadEncoding::Base64,
                    text: "not base64!".to_string(),
                },
            )],
            &limits(),
        )
        .expect_err("invalid base64 must be refused");
        assert!(matches!(report.current_context(), AppError::Validation));
        assert!(reasons(&report).contains("not valid base64"));
    }

    #[test]
    fn an_invalid_key_is_named_as_the_key() {
        let report = encode_messages(
            &[message(
                PublishField {
                    encoding: PayloadEncoding::Json,
                    text: "nope".to_string(),
                },
                PublishField::text("v"),
            )],
            &limits(),
        )
        .expect_err("an invalid key must be refused");
        assert!(reasons(&report).contains("key of message 1"));
    }

    #[test]
    fn the_failing_message_is_identified_by_its_row_number() {
        let report = encode_messages(
            &[
                message(PublishField::null(), PublishField::text("fine")),
                message(PublishField::null(), PublishField::text("fine")),
                message(
                    PublishField::null(),
                    PublishField {
                        encoding: PayloadEncoding::Json,
                        text: "{".to_string(),
                    },
                ),
            ],
            &limits(),
        )
        .expect_err("the third message is invalid");
        assert!(
            reasons(&report).contains("message 3"),
            "row numbers are one-based in the dialog, so they must be here too: {}",
            reasons(&report)
        );
    }

    #[test]
    fn headers_are_encoded_the_same_way_payloads_are() {
        let records = encode_messages(
            &[NewPublishMessage {
                key: PublishField::null(),
                value: PublishField::text("v"),
                headers: vec![
                    PublishHeaderInput {
                        key: "content-type".to_string(),
                        value: PublishField::text("application/json"),
                    },
                    PublishHeaderInput {
                        key: "trace-id".to_string(),
                        value: PublishField {
                            encoding: PayloadEncoding::Base64,
                            text: "AAEC".to_string(),
                        },
                    },
                    PublishHeaderInput {
                        key: "flag".to_string(),
                        value: PublishField::null(),
                    },
                ],
            }],
            &limits(),
        )
        .expect("headers should encode");
        assert_eq!(
            records[0].headers,
            vec![
                (
                    "content-type".to_string(),
                    Some(b"application/json".to_vec())
                ),
                ("trace-id".to_string(), Some(vec![0x00, 0x01, 0x02])),
                ("flag".to_string(), None),
            ]
        );
    }

    #[test]
    fn a_header_with_no_name_is_rejected() {
        let report = encode_messages(
            &[NewPublishMessage {
                key: PublishField::null(),
                value: PublishField::text("v"),
                headers: vec![PublishHeaderInput {
                    key: "   ".to_string(),
                    value: PublishField::text("x"),
                }],
            }],
            &limits(),
        )
        .expect_err("a nameless header must be refused");
        assert!(reasons(&report).contains("header with no name"));
    }

    #[test]
    fn an_invalid_header_value_names_its_header() {
        let report = encode_messages(
            &[NewPublishMessage {
                key: PublishField::null(),
                value: PublishField::text("v"),
                headers: vec![PublishHeaderInput {
                    key: "trace-id".to_string(),
                    value: PublishField {
                        encoding: PayloadEncoding::Base64,
                        text: "!!".to_string(),
                    },
                }],
            }],
            &limits(),
        )
        .expect_err("an invalid header value must be refused");
        assert!(reasons(&report).contains("header \"trace-id\""));
    }

    #[test]
    fn an_empty_batch_is_refused() {
        let report = encode_messages(&[], &limits()).expect_err("nothing to publish");
        assert!(matches!(report.current_context(), AppError::Validation));
        assert!(reasons(&report).contains("no messages to publish"));
    }

    #[test]
    fn a_batch_over_the_message_count_ceiling_is_refused() {
        let one = message(PublishField::null(), PublishField::text("x"));
        let too_many = vec![one; MAX_PUBLISH_BATCH_MESSAGES + 1];
        let report = encode_messages(&too_many, &limits()).expect_err("over the count ceiling");
        assert!(reasons(&report).contains("at most 100 messages"));
    }

    #[test]
    fn a_batch_at_the_message_count_ceiling_is_allowed() {
        let one = message(PublishField::null(), PublishField::text("x"));
        let exactly = vec![one; MAX_PUBLISH_BATCH_MESSAGES];
        let records = encode_messages(&exactly, &limits()).expect("the ceiling itself is allowed");
        assert_eq!(records.len(), MAX_PUBLISH_BATCH_MESSAGES);
    }

    #[test]
    fn a_record_over_max_message_size_is_refused_before_the_broker_sees_it() {
        let limits = PublishLimits::for_max_message_size(16);
        let report = encode_messages(
            &[message(
                PublishField::null(),
                PublishField::text("x".repeat(17)),
            )],
            &limits,
        )
        .expect_err("over the per-message ceiling");
        assert!(reasons(&report).contains("over the 16 byte Max Message Size"));
    }

    #[test]
    fn the_size_check_counts_the_key_and_headers_not_just_the_payload() {
        // 8-byte payload, 4-byte key, and a header of 3 + 4 bytes = 19 total,
        // so a 16-byte ceiling must reject it even though no single field is
        // over. The broker's own message.max.bytes covers the whole record;
        // a payload-only check here would pass records it then rejects, after
        // earlier messages in the batch had already been written.
        let limits = PublishLimits::for_max_message_size(16);
        let report = encode_messages(
            &[NewPublishMessage {
                key: PublishField::text("keyy"),
                value: PublishField::text("12345678"),
                headers: vec![PublishHeaderInput {
                    key: "abc".to_string(),
                    value: PublishField::text("defg"),
                }],
            }],
            &limits,
        )
        .expect_err("the whole record is over the ceiling");
        assert!(reasons(&report).contains("19 bytes"));
    }

    #[test]
    fn a_batch_over_the_total_byte_ceiling_is_refused() {
        let limits = PublishLimits {
            max_message_size_bytes: 1024,
            max_batch_messages: 10,
            max_batch_bytes: 10,
        };
        let one = message(PublishField::null(), PublishField::text("123456"));
        let report = encode_messages(&[one.clone(), one], &limits)
            .expect_err("12 bytes over a 10 byte batch ceiling");
        assert!(reasons(&report).contains("10 byte publish limit"));
    }

    #[test]
    fn nothing_is_returned_when_any_message_is_invalid() {
        // The whole batch or nothing: a caller that received the first 39
        // records and an error would have no way to know the other 61 were
        // never encoded, and publishing what it got would leave the topic in
        // a state nobody asked for.
        let report = encode_messages(
            &[
                message(PublishField::null(), PublishField::text("fine")),
                message(
                    PublishField::null(),
                    PublishField {
                        encoding: PayloadEncoding::Base64,
                        text: "%%%".to_string(),
                    },
                ),
            ],
            &limits(),
        );
        assert!(report.is_err());
    }

    #[test]
    fn size_bytes_counts_every_part_of_a_record() {
        let record = EncodedRecord {
            key: Some(vec![0; 3]),
            value: Some(vec![0; 5]),
            headers: vec![
                ("ab".to_string(), Some(vec![0; 7])),
                ("cd".to_string(), None),
            ],
        };
        assert_eq!(record.size_bytes(), 3 + 5 + 2 + 7 + 2);
    }

    #[test]
    fn size_bytes_of_an_entirely_null_record_is_zero() {
        let record = EncodedRecord {
            key: None,
            value: None,
            headers: Vec::new(),
        };
        assert_eq!(record.size_bytes(), 0);
    }

    // --- the gate ---------------------------------------------------------

    #[test]
    fn a_connected_allowed_undenied_connection_may_publish() {
        assert_eq!(publish_refusal(true, true, None), None);
    }

    #[test]
    fn a_disconnected_connection_may_not_publish() {
        assert_eq!(
            publish_refusal(false, true, None),
            Some(PublishRefusal::NotConnected)
        );
    }

    #[test]
    fn a_connection_without_allow_publishing_may_not_publish() {
        assert_eq!(
            publish_refusal(true, false, None),
            Some(PublishRefusal::NotAllowed)
        );
    }

    #[test]
    fn a_topic_the_broker_has_refused_may_not_be_published_to() {
        assert_eq!(
            publish_refusal(true, true, Some("Broker: Topic authorization failed")),
            Some(PublishRefusal::BrokerDenied(
                "Broker: Topic authorization failed".to_string()
            ))
        );
    }

    #[test]
    fn being_disconnected_is_reported_ahead_of_every_other_reason() {
        assert_eq!(
            publish_refusal(false, false, Some("denied")),
            Some(PublishRefusal::NotConnected),
            "Connect is the first thing the user must do either way"
        );
    }

    #[test]
    fn publishing_being_disabled_is_reported_ahead_of_a_stale_denial() {
        assert_eq!(
            publish_refusal(true, false, Some("denied")),
            Some(PublishRefusal::NotAllowed),
            "a connection that may not publish cannot act on a broker's verdict"
        );
    }

    #[test]
    fn the_default_for_a_new_connection_is_refusal() {
        // `allow_publishing` defaults to 0 in the database, so this is the
        // state every existing and newly created connection starts in.
        assert_eq!(
            publish_refusal(true, false, None),
            Some(PublishRefusal::NotAllowed)
        );
    }

    #[test]
    fn each_refusal_says_what_to_do_about_it() {
        assert!(
            PublishRefusal::NotConnected
                .message("orders")
                .contains("Connect")
        );
        assert!(
            PublishRefusal::NotAllowed
                .message("orders")
                .contains("Allow publishing")
        );
        let denied = PublishRefusal::BrokerDenied("Topic authorization failed".to_string());
        let message = denied.message("orders");
        assert!(
            message.contains("orders"),
            "the topic must be named: {message}"
        );
        assert!(message.contains("reconnect"));
    }

    #[test]
    fn only_a_broker_denial_reports_as_an_authorization_failure() {
        assert!(matches!(
            PublishRefusal::NotConnected.error(),
            AppError::Validation
        ));
        assert!(matches!(
            PublishRefusal::NotAllowed.error(),
            AppError::Validation
        ));
        assert!(matches!(
            PublishRefusal::BrokerDenied("x".to_string()).error(),
            AppError::Authorization
        ));
    }

    // --- wire shape -------------------------------------------------------

    #[test]
    fn the_outcome_crosses_the_boundary_in_camel_case() {
        let json = serde_json::to_string(&PublishOutcome {
            delivered: vec![DeliveredRecord {
                index: 0,
                partition: 2,
                offset: 41,
            }],
            failure: Some(PublishFailure {
                index: 1,
                kind: PublishFailureKind::Authorization,
                reason: "nope".to_string(),
            }),
            not_attempted: 3,
        })
        .expect("the outcome should serialize");
        assert!(json.contains("\"notAttempted\":3"), "{json}");
        assert!(!json.contains("not_attempted"), "{json}");
    }

    #[test]
    fn an_entered_message_is_parsed_from_camel_case() {
        let parsed: NewPublishMessage = serde_json::from_str(
            r#"{"key":{"encoding":"null","text":""},
                "value":{"encoding":"text","text":"hi"},
                "headers":[{"key":"h","value":{"encoding":"base64","text":"AAEC"}}]}"#,
        )
        .expect("the frontend's shape should parse");
        assert_eq!(parsed.value.encoding, PayloadEncoding::Text);
        assert_eq!(parsed.headers[0].value.encoding, PayloadEncoding::Base64);
    }

    #[test]
    fn headers_may_be_omitted_entirely_by_the_caller() {
        let parsed: NewPublishMessage = serde_json::from_str(
            r#"{"key":{"encoding":"null"},"value":{"encoding":"text","text":"hi"}}"#,
        )
        .expect("headers and text should both default");
        assert!(parsed.headers.is_empty());
        assert_eq!(parsed.key.text, "");
    }

    #[test]
    fn a_successful_outcome_is_distinguishable_from_a_failed_one() {
        let ok = PublishOutcome {
            delivered: vec![],
            failure: None,
            not_attempted: 0,
        };
        assert!(ok.succeeded());
        let failed = PublishOutcome {
            delivered: vec![],
            failure: Some(PublishFailure {
                index: 0,
                kind: PublishFailureKind::Transient,
                reason: "x".to_string(),
            }),
            not_attempted: 0,
        };
        assert!(!failed.succeeded());
    }

    #[test]
    fn a_failure_kind_is_derived_from_the_error_not_from_its_wording() {
        assert_eq!(
            PublishFailureKind::from_error(&AppError::Authorization),
            PublishFailureKind::Authorization
        );
        assert_eq!(
            PublishFailureKind::from_error(&AppError::Authentication),
            PublishFailureKind::Authentication
        );
        assert_eq!(
            PublishFailureKind::from_error(&AppError::Validation),
            PublishFailureKind::Validation
        );
        assert_eq!(
            PublishFailureKind::from_error(&AppError::Kafka),
            PublishFailureKind::Transient
        );
        assert_eq!(
            PublishFailureKind::from_error(&AppError::Db),
            PublishFailureKind::Transient
        );
    }

    #[test]
    fn a_failure_kind_crosses_the_boundary_in_camel_case() {
        let json = serde_json::to_string(&PublishFailureKind::Authorization).unwrap();
        assert_eq!(json, "\"authorization\"");
    }
}
