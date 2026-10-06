//! The cluster-read half of `KafkaClient`, against a real broker.
//!
//! Everything here talks to librdkafka's metadata/consumer/admin APIs, so
//! none of it can be exercised without a broker: the unit tests beside
//! `client.rs` can only reach these calls' *failure* paths (a closed port),
//! which leaves every success path — the code that actually decodes a
//! metadata response, a watermark pair, a DescribeConfigs result, or a
//! group's committed offsets — unrun.
//!
//! ```bash
//! docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0
//! # fixtures: `e2e-basic` (3 partitions x ~20 keyed records), a committed
//! # `e2e-group` on it, and `e2e-headers` (2 records carrying headers)
//! SALTY_E2E_BOOTSTRAP=localhost:9092 \
//!   cargo test -p salty-kafka --test cluster_reads
//! ```

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use salty_core::{Connection, MessageFilter, SecurityProtocol, TopicMessage};
use salty_kafka::{BrokerSslConfig, KafkaClient, RdKafkaClient};

const TOPIC: &str = "e2e-basic";
const HEADERS_TOPIC: &str = "e2e-headers";
const GROUP: &str = "e2e-group";
/// A group with a consumer actually running, so its members carry partition
/// assignments to decode — the only shape in which lag rows are reported.
const LIVE_GROUP: &str = "e2e-live";
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_MESSAGE_SIZE: u32 = 12 * 1024 * 1024;

fn bootstrap_servers() -> Option<String> {
    std::env::var("SALTY_E2E_BOOTSTRAP").ok().filter(|value| !value.is_empty())
}

/// Every test bails out identically without a broker, so the suite stays
/// green on a machine that has none — same contract as the other e2e files
/// here.
macro_rules! broker {
    () => {
        match bootstrap_servers() {
            Some(bootstrap) => bootstrap,
            None => {
                eprintln!("skipped: set SALTY_E2E_BOOTSTRAP to run this test");
                return;
            }
        }
    };
}

fn connection(bootstrap_servers: String) -> Connection {
    Connection {
        id: "e2e".into(),
        name: "e2e".into(),
        bootstrap_servers,
        kafka_version: "3.9".into(),
        zookeeper_enabled: false,
        zookeeper_host: None,
        zookeeper_port: None,
        zookeeper_chroot_path: None,
        security_protocol: SecurityProtocol::Plaintext,
        sasl_mechanism: None,
        sasl_username: None,
        sasl_password: None,
        sasl_oauth_url: None,
        schema_registry_endpoint: None,
        ksqldb_endpoint: None,
        ksqldb_basic_auth_credentials: None,
        schema_registry_basic_auth_credentials: None,
        schema_registry_trust_store_location: None,
        schema_registry_trust_store_password: None,
        schema_registry_keystore_location: None,
        schema_registry_keystore_password: None,
        schema_registry_keystore_key_password: None,
        ssl_truststore_location: None,
        ssl_truststore_password: None,
        ssl_keystore_location: None,
        ssl_keystore_password: None,
        ssl_keystore_key_password: None,
        allow_publishing: false,
        created_at: "now".into(),
        updated_at: "now".into(),
    }
}

fn all_of(topic_filter: MessageFilter) -> MessageFilter {
    MessageFilter { max_messages_per_partition: Some(500), max_total_messages: Some(500), ..topic_filter }
}

async fn fetch(client: &RdKafkaClient, connection: &Connection, topic: &str, filter: MessageFilter) -> Vec<TopicMessage> {
    client
        .fetch_messages(
            connection,
            topic,
            &all_of(filter),
            None,
            READ_TIMEOUT,
            MAX_MESSAGE_SIZE,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("fetch failed")
        .messages
}

#[tokio::test(flavor = "multi_thread")]
async fn list_brokers_returns_the_cluster_the_connection_points_at() {
    let client = RdKafkaClient::new();
    let connection = connection(broker!());

    let brokers = client.list_brokers(&connection, READ_TIMEOUT).await.expect("list_brokers failed");

    assert!(!brokers.is_empty(), "a running cluster has at least one broker");
    for broker in &brokers {
        assert!(!broker.host.is_empty(), "every broker reports a host: {broker:?}");
        assert!(broker.port > 0, "every broker reports a port: {broker:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn list_topics_reports_each_topics_partition_count() {
    let client = RdKafkaClient::new();
    let connection = connection(broker!());

    let topics = client.list_topics(&connection, READ_TIMEOUT).await.expect("list_topics failed");

    let basic = topics
        .iter()
        .find(|topic| topic.name == TOPIC)
        .unwrap_or_else(|| panic!("fixture topic {TOPIC} missing; got {:?}", topics.iter().map(|t| &t.name).collect::<Vec<_>>()));
    assert_eq!(basic.partition_count, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn list_consumer_groups_includes_a_group_that_has_committed_offsets() {
    let client = RdKafkaClient::new();
    let connection = connection(broker!());

    let groups = client.list_consumer_groups(&connection, READ_TIMEOUT).await.expect("list_consumer_groups failed");

    let group = groups
        .iter()
        .find(|group| group.group_id == GROUP)
        .unwrap_or_else(|| panic!("fixture group {GROUP} missing; got {groups:?}"));
    assert!(!group.state.is_empty(), "a listed group always carries a state");
}

/// The count is watermark arithmetic (high - low, summed), not a scan, so it
/// has to agree with what a full fetch actually returns.
#[tokio::test(flavor = "multi_thread")]
async fn count_topic_messages_agrees_with_what_a_full_fetch_returns() {
    let client = RdKafkaClient::new();
    let connection = connection(broker!());

    let counted = client.count_topic_messages(&connection, TOPIC, READ_TIMEOUT).await.expect("count failed");
    let fetched = fetch(&client, &connection, TOPIC, MessageFilter::default()).await;

    assert!(counted > 0, "the fixture topic is not empty");
    assert_eq!(counted, fetched.len() as u64);
}

#[tokio::test(flavor = "multi_thread")]
async fn list_partitions_reports_leader_replicas_and_watermarks_for_every_partition() {
    let client = RdKafkaClient::new();
    let connection = connection(broker!());

    let mut partitions = client.list_partitions(&connection, TOPIC, READ_TIMEOUT).await.expect("list_partitions failed");
    partitions.sort_by_key(|partition| partition.id);

    assert_eq!(partitions.iter().map(|p| p.id).collect::<Vec<_>>(), vec![0, 1, 2]);
    for partition in &partitions {
        assert!(partition.replicas.contains(&partition.leader), "the leader is one of the replicas: {partition:?}");
        assert!(!partition.isr.is_empty(), "a healthy partition has an in-sync replica: {partition:?}");
        assert!(partition.high_offset >= partition.low_offset, "watermarks are ordered: {partition:?}");
        assert!(partition.high_offset > 0, "the fixture partitions all hold messages: {partition:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_topic_config_returns_the_brokers_topic_level_settings() {
    let client = RdKafkaClient::new();
    let connection = connection(broker!());

    let config = client.describe_topic_config(&connection, TOPIC, READ_TIMEOUT).await.expect("describe failed");

    assert!(!config.is_empty(), "DescribeConfigs always returns the topic's settings");
    let retention = config
        .iter()
        .find(|entry| entry.name == "cleanup.policy")
        .unwrap_or_else(|| panic!("cleanup.policy missing from {:?}", config.iter().map(|e| &e.name).collect::<Vec<_>>()));
    assert!(retention.value.is_some(), "a described setting carries its value");
}

/// The committed offsets come from a throwaway consumer scoped to the group,
/// and the log-end offsets from a watermark walk; the lag is the difference,
/// and each partition is attributed to the member currently assigned it
/// (decoded out of the group's member assignments).
///
/// Needs the group to have a *live* member: the partition set reported here is
/// derived from the members' assignments, so a group with none reports nothing
/// — see `an_idle_group_reports_its_state_but_no_partitions`.
#[tokio::test(flavor = "multi_thread")]
async fn fetch_consumer_group_lag_subtracts_committed_offsets_from_log_end_offsets() {
    let client = RdKafkaClient::new();
    let connection = connection(broker!());

    let lag = client.fetch_consumer_group_lag(&connection, LIVE_GROUP, READ_TIMEOUT).await.expect("lag failed");

    assert!(!lag.state.is_empty(), "the group's state is reported alongside its partitions");
    assert_eq!(
        lag.partitions.len(),
        3,
        "the live member is assigned all 3 partitions of the fixture topic: {:?}",
        lag.partitions
    );
    for partition in &lag.partitions {
        assert_eq!(partition.topic, TOPIC);
        assert!(partition.log_end_offset > 0, "the fixture partitions all hold messages: {partition:?}");
        let current = partition.current_offset.expect("the fixture group has committed every partition");
        assert_eq!(
            partition.lag,
            Some(partition.log_end_offset - current),
            "lag is log-end minus committed: {partition:?}"
        );
        assert!(partition.client_id.is_some(), "an assigned partition names its owner: {partition:?}");
        assert!(partition.client_host.is_some(), "an assigned partition names its owner's host: {partition:?}");
    }
}

/// An idle group — committed offsets, no consumer currently running, so Kafka
/// reports it `Empty` — is the resting state of most groups the Consumers
/// panel lists.
///
/// Two things are being pinned here. The first is that asking for one does not
/// kill the process: librdkafka leaves such a group's member array NULL, and
/// reading it as a slice is undefined behaviour that aborts outright under
/// debug assertions (see `group_members` in `client.rs`).
///
/// The second is a characterization, not an endorsement: because the partition
/// set is derived from member assignments, an idle group reports *no rows at
/// all* rather than its committed offsets and their lag. That is the current
/// behaviour, and this test exists so that changing it is a deliberate act
/// with a failing test attached, rather than a silent one.
#[tokio::test(flavor = "multi_thread")]
async fn an_idle_group_reports_its_state_but_no_partitions() {
    let client = RdKafkaClient::new();
    let connection = connection(broker!());

    let lag = client.fetch_consumer_group_lag(&connection, GROUP, READ_TIMEOUT).await.expect("lag failed");

    assert_eq!(lag.state, "Empty", "the fixture group has committed offsets but no live member");
    assert!(lag.partitions.is_empty(), "an idle group's partitions are not reported; got {:?}", lag.partitions);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_partition_filter_returns_only_that_partitions_messages() {
    let client = RdKafkaClient::new();
    let connection = connection(broker!());

    let messages = fetch(
        &client,
        &connection,
        TOPIC,
        MessageFilter { partitions: Some(vec![1]), ..MessageFilter::default() },
    )
    .await;

    assert!(!messages.is_empty(), "partition 1 of the fixture topic is not empty");
    assert!(messages.iter().all(|message| message.partition == 1), "no other partition leaks in");
}

/// The offset filter is clamped to each partition's watermarks rather than
/// erroring, so the same filter is valid on partitions of differing lengths.
#[tokio::test(flavor = "multi_thread")]
async fn an_offset_filter_starts_each_partition_at_that_offset() {
    let client = RdKafkaClient::new();
    let connection = connection(broker!());

    let all = fetch(&client, &connection, TOPIC, MessageFilter::default()).await;
    let from_five = fetch(&client, &connection, TOPIC, MessageFilter { offset: Some(5), ..MessageFilter::default() }).await;

    assert!(!from_five.is_empty(), "the fixture partitions run past offset 5");
    assert!(from_five.len() < all.len(), "starting at offset 5 skips what came before it");
    assert!(from_five.iter().all(|message| message.offset >= 5), "nothing before the requested offset is returned");
}

/// `offsets_for_times` resolves each partition independently, and falls back
/// to a watermark lookup for any partition it cannot place — a timestamp
/// older than every message must therefore still yield the whole topic.
#[tokio::test(flavor = "multi_thread")]
async fn a_from_timestamp_before_the_topic_existed_resolves_to_the_whole_topic() {
    let client = RdKafkaClient::new();
    let connection = connection(broker!());

    let all = fetch(&client, &connection, TOPIC, MessageFilter::default()).await;
    let since_epoch = fetch(
        &client,
        &connection,
        TOPIC,
        MessageFilter { from_timestamp_ms: Some(1), ..MessageFilter::default() },
    )
    .await;

    assert_eq!(since_epoch.len(), all.len());
}

/// The other side of the fallback — corrected, having been pinned here as a
/// characterization of behaviour that looked wrong (which it was).
///
/// `offsets_for_times` cannot place a timestamp that is after every message
/// in a partition, so the fallback runs, and for a *from* timestamp it used
/// to be the **low** watermark: setting the Data tab's "From" to a future
/// date returned the entire topic rather than nothing, the exact opposite of
/// what the filter reads as. It is now the high watermark, which is the
/// offset that matches nothing. (`low` is only ever right for a timestamp
/// *older* than the partition, which librdkafka resolves on its own without
/// ever reaching the fallback — see the test above.)
#[tokio::test(flavor = "multi_thread")]
async fn a_from_timestamp_in_the_future_matches_nothing() {
    let client = RdKafkaClient::new();
    let connection = connection(broker!());

    let far_future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock is after the epoch")
        .as_millis() as i64
        + 86_400_000;
    let all = fetch(&client, &connection, TOPIC, MessageFilter::default()).await;
    assert!(!all.is_empty(), "fixture topic is empty — run scripts/e2e-fixtures.sh");

    let from_the_future = fetch(
        &client,
        &connection,
        TOPIC,
        MessageFilter { from_timestamp_ms: Some(far_future), ..MessageFilter::default() },
    )
    .await;

    assert!(
        from_the_future.is_empty(),
        "a from-timestamp after every message must match nothing, not fall back to the whole topic \
         ({} of {} message(s) returned)",
        from_the_future.len(),
        all.len(),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_to_timestamp_in_the_past_matches_nothing() {
    let client = RdKafkaClient::new();
    let connection = connection(broker!());

    let messages = fetch(
        &client,
        &connection,
        TOPIC,
        MessageFilter { to_timestamp_ms: Some(1), ..MessageFilter::default() },
    )
    .await;

    assert!(messages.is_empty(), "nothing was produced before 1970; got {} rows", messages.len());
}

/// A message's key and headers travel regardless of the payload checkbox —
/// they are what the grid renders in a metadata-only browse — while the
/// payload itself is withheld and only its size reported.
#[tokio::test(flavor = "multi_thread")]
async fn a_metadata_only_fetch_still_carries_keys_headers_and_payload_sizes() {
    let client = RdKafkaClient::new();
    let connection = connection(broker!());

    let messages = fetch(&client, &connection, HEADERS_TOPIC, MessageFilter::default()).await;

    assert_eq!(messages.len(), 2);
    let first = &messages[0];
    assert!(first.payload_base64.is_none(), "the payload is withheld when it was not asked for");
    assert_eq!(first.payload_size_bytes, Some("body-1".len() as u64), "its size is reported anyway");
    assert_eq!(
        first.key_base64.as_deref(),
        Some(base64_of("key-1").as_str()),
        "the key is base64-encoded, not lossy-decoded"
    );

    let headers: Vec<_> = first.headers.iter().map(|header| header.key.as_str()).collect();
    assert_eq!(headers, vec!["trace-id", "content-type"], "headers keep the order the producer set");
    assert_eq!(first.headers[0].value_base64.as_deref(), Some(base64_of("abc123").as_str()));
}

#[tokio::test(flavor = "multi_thread")]
async fn asking_for_payloads_returns_them_base64_encoded() {
    let client = RdKafkaClient::new();
    let connection = connection(broker!());

    let messages = fetch(
        &client,
        &connection,
        HEADERS_TOPIC,
        MessageFilter { include_payload: true, ..MessageFilter::default() },
    )
    .await;

    let bodies: Vec<_> = messages.iter().filter_map(|message| message.payload_base64.as_deref()).collect();
    assert_eq!(bodies, vec![base64_of("body-1"), base64_of("body-2")]);
}

/// The preview bound truncates what is carried back without changing what is
/// reported about the message — that difference is how the viewer knows to
/// re-fetch a row in full before decoding it.
#[tokio::test(flavor = "multi_thread")]
async fn a_payload_preview_bound_truncates_the_payload_but_not_its_reported_size() {
    let client = RdKafkaClient::new();
    let connection = connection(broker!());

    let messages = fetch(
        &client,
        &connection,
        HEADERS_TOPIC,
        MessageFilter { include_payload: true, max_payload_preview_bytes: Some(3), ..MessageFilter::default() },
    )
    .await;

    let first = &messages[0];
    assert_eq!(first.payload_base64.as_deref(), Some(base64_of("bod").as_str()), "only the first 3 bytes travel");
    assert_eq!(first.payload_size_bytes, Some("body-1".len() as u64), "the true length is still reported");
}

/// The streaming feed and the returned result are the same messages: the
/// channel is a progress feed, not a partial one, so a caller that painted
/// rows from it must not be sent them a second time by the result.
#[tokio::test(flavor = "multi_thread")]
async fn every_message_is_streamed_as_it_is_polled_as_well_as_returned() {
    let client = RdKafkaClient::new();
    let connection = connection(broker!());
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

    let result = client
        .fetch_messages(
            &connection,
            TOPIC,
            &all_of(MessageFilter::default()),
            Some(sender),
            READ_TIMEOUT,
            MAX_MESSAGE_SIZE,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("fetch failed");

    let mut streamed = Vec::new();
    while let Ok(message) = receiver.try_recv() {
        streamed.push(message);
    }

    assert!(!result.messages.is_empty());
    assert_eq!(
        streamed.iter().map(|m| (m.partition, m.offset)).collect::<std::collections::BTreeSet<_>>(),
        result.messages.iter().map(|m| (m.partition, m.offset)).collect::<std::collections::BTreeSet<_>>(),
        "the stream and the result describe the same messages"
    );
}

/// A cancellation flag that is already set must end the fetch at its first
/// poll slice rather than after it has read the topic.
#[tokio::test(flavor = "multi_thread")]
async fn an_already_cancelled_fetch_returns_without_reading_the_topic() {
    let client = RdKafkaClient::new();
    let connection = connection(broker!());

    let result = client
        .fetch_messages(
            &connection,
            TOPIC,
            &all_of(MessageFilter::default()),
            None,
            READ_TIMEOUT,
            MAX_MESSAGE_SIZE,
            None,
            Arc::new(AtomicBool::new(true)),
        )
        .await
        .expect("a cancelled fetch is not an error");

    assert!(result.messages.is_empty(), "a cancelled fetch returns no rows; got {}", result.messages.len());
}

/// `max_total_messages` caps the rows pulled, but `total_matching` reports
/// what the filter matches regardless — that is what lets the Data tab say
/// "N loaded of M matching" instead of implying the topic is short.
#[tokio::test(flavor = "multi_thread")]
async fn a_message_cap_bounds_the_rows_without_hiding_how_many_matched() {
    let client = RdKafkaClient::new();
    let connection = connection(broker!());

    let result = client
        .fetch_messages(
            &connection,
            TOPIC,
            &MessageFilter { max_total_messages: Some(5), ..MessageFilter::default() },
            None,
            READ_TIMEOUT,
            MAX_MESSAGE_SIZE,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("fetch failed");

    assert_eq!(result.messages.len(), 5);
    assert!(result.total_matching > 5, "the topic holds more than the cap: {}", result.total_matching);
}

/// A connect is what promotes a connection to "connected" in the tree, and
/// `release` drops the client it pooled. Both run against a live broker here
/// because the unit tests can only reach their failure paths.
#[tokio::test(flavor = "multi_thread")]
async fn connecting_reports_reachable_and_releasing_drops_the_pooled_client() {
    let client = RdKafkaClient::new();
    let connection = connection(broker!());

    let status = client.connect(&connection).await.expect("connect failed");
    assert!(matches!(status, salty_core::ConnectionStatus::Reachable), "got {status:?}");

    let checked = client.check_status(&connection).await.expect("check_status failed");
    assert!(matches!(checked, salty_core::ConnectionStatus::Reachable), "got {checked:?}");

    client.release(&connection.id);

    // Releasing only drops the pooled client; the cluster is still reachable,
    // so the next request rebuilds one and succeeds.
    let after_release = client.connect(&connection).await.expect("connect after release failed");
    assert!(matches!(after_release, salty_core::ConnectionStatus::Reachable), "got {after_release:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_connection_succeeds_against_a_reachable_cluster() {
    let client = RdKafkaClient::new();
    let connection = connection(broker!());

    let status = client
        .test_connection(
            &connection.bootstrap_servers,
            SecurityProtocol::Plaintext,
            None,
            None,
            None,
            BrokerSslConfig::default(),
        )
        .await
        .expect("test_connection failed");

    assert!(matches!(status, salty_core::ConnectionStatus::Reachable), "got {status:?}");
}

fn base64_of(value: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(value)
}
