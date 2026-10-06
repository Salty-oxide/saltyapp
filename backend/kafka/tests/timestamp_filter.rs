//! What the Data tab's From/To filters actually select, against a real broker.
//!
//! The filters are resolved to *offsets* — each boundary goes to the broker
//! as a `ListOffsets`-by-timestamp request (`offsets_for_times`), and the
//! fetch then reads the offset range between the two. Nothing compares an
//! individual message's timestamp, so what these tests pin down is that the
//! offsets those boundaries resolve to actually bracket the messages the user
//! asked for — including the two edge cases where no offset comes back at
//! all.
//!
//! ```bash
//! scripts/e2e-fixtures.sh
//! SALTY_E2E_BOOTSTRAP=localhost:9092 \
//!   cargo test -p salty-kafka --test timestamp_filter -- --nocapture
//! ```

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use salty_core::{Connection, MessageFilter, SecurityProtocol};
use salty_kafka::{KafkaClient, RdKafkaClient};

/// Written by `scripts/e2e-fixtures.sh`.
const DEFAULT_TOPIC: &str = "e2e-basic";

fn bootstrap_servers() -> Option<String> {
    std::env::var("SALTY_E2E_BOOTSTRAP").ok().filter(|value| !value.is_empty())
}

fn topic() -> String {
    std::env::var("SALTY_E2E_TOPIC").unwrap_or_else(|_| DEFAULT_TOPIC.to_string())
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

fn filter(from_timestamp_ms: Option<i64>, to_timestamp_ms: Option<i64>) -> MessageFilter {
    MessageFilter {
        partitions: None,
        max_messages_per_partition: Some(1000),
        max_total_messages: Some(1000),
        from_timestamp_ms,
        to_timestamp_ms,
        offset: None,
        include_payload: false,
        max_payload_preview_bytes: None,
    }
}

async fn fetch(client: &RdKafkaClient, connection: &Connection, topic: &str, filter: &MessageFilter) -> Vec<i64> {
    client
        .fetch_messages(
            connection,
            topic,
            filter,
            None,
            Duration::from_secs(30),
            1024 * 1024,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("fetch failed")
        .messages
        .iter()
        .filter_map(|m| m.timestamp_ms)
        .collect()
}

/// Every assertion below is stated relative to what the topic actually
/// holds, so the fixtures can be regenerated without rewriting the test.
struct Bounds {
    oldest: i64,
    newest: i64,
    count: usize,
}

async fn bounds(client: &RdKafkaClient, connection: &Connection, topic: &str) -> Bounds {
    let timestamps = fetch(client, connection, topic, &filter(None, None)).await;
    assert!(!timestamps.is_empty(), "fixture topic {topic} is empty — run scripts/e2e-fixtures.sh");
    Bounds {
        oldest: *timestamps.iter().min().expect("non-empty"),
        newest: *timestamps.iter().max().expect("non-empty"),
        count: timestamps.len(),
    }
}

/// The bug this test exists for: a From later than every message in a
/// partition left `offsets_for_times` with no offset to give, and the
/// fallback read that partition **from its low watermark** — so asking for
/// "the last five minutes" of an idle topic returned its oldest messages
/// instead. Every partition is in that state at once on a topic nobody is
/// producing to, which is what made From look like it was being ignored.
#[tokio::test(flavor = "multi_thread")]
async fn a_from_after_every_message_matches_nothing_rather_than_everything() {
    let Some(bootstrap) = bootstrap_servers() else {
        eprintln!("skipped: set SALTY_E2E_BOOTSTRAP to run this test");
        return;
    };
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    let topic = topic();
    let bounds = bounds(&client, &connection, &topic).await;

    let matched = fetch(&client, &connection, &topic, &filter(Some(bounds.newest + 1), None)).await;

    println!(
        "topic holds {} messages spanning {}..={}; From {} matched {}",
        bounds.count,
        bounds.oldest,
        bounds.newest,
        bounds.newest + 1,
        matched.len()
    );
    assert!(matched.is_empty(), "From after the newest message returned {} message(s)", matched.len());
}

/// The other end of the same fallback, which was always right: a To after
/// every message means "read to the end".
#[tokio::test(flavor = "multi_thread")]
async fn a_to_after_every_message_matches_the_whole_topic() {
    let Some(bootstrap) = bootstrap_servers() else {
        eprintln!("skipped: set SALTY_E2E_BOOTSTRAP to run this test");
        return;
    };
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    let topic = topic();
    let bounds = bounds(&client, &connection, &topic).await;

    let matched = fetch(&client, &connection, &topic, &filter(None, Some(bounds.newest + 1))).await;

    assert_eq!(matched.len(), bounds.count);
}

/// A To *before* every message is the mirror of the first test, and has to
/// resolve to each partition's start rather than its end.
#[tokio::test(flavor = "multi_thread")]
async fn a_to_before_every_message_matches_nothing() {
    let Some(bootstrap) = bootstrap_servers() else {
        eprintln!("skipped: set SALTY_E2E_BOOTSTRAP to run this test");
        return;
    };
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    let topic = topic();
    let bounds = bounds(&client, &connection, &topic).await;

    let matched = fetch(&client, &connection, &topic, &filter(None, Some(bounds.oldest))).await;

    assert!(matched.is_empty(), "To at the oldest timestamp is exclusive, so nothing precedes it");
}

/// A From at or before the oldest message takes the whole topic — the
/// boundary is inclusive of a message stamped exactly at it.
#[tokio::test(flavor = "multi_thread")]
async fn a_from_at_the_oldest_message_matches_the_whole_topic() {
    let Some(bootstrap) = bootstrap_servers() else {
        eprintln!("skipped: set SALTY_E2E_BOOTSTRAP to run this test");
        return;
    };
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    let topic = topic();
    let bounds = bounds(&client, &connection, &topic).await;

    let matched = fetch(&client, &connection, &topic, &filter(Some(bounds.oldest), None)).await;

    assert_eq!(matched.len(), bounds.count);
}

/// The whole point of the pair: every message handed back sits inside the
/// window, and the window is read from the message timestamps the grid then
/// displays.
#[tokio::test(flavor = "multi_thread")]
async fn messages_returned_for_a_window_all_carry_timestamps_inside_it() {
    let Some(bootstrap) = bootstrap_servers() else {
        eprintln!("skipped: set SALTY_E2E_BOOTSTRAP to run this test");
        return;
    };
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    let topic = topic();
    let bounds = bounds(&client, &connection, &topic).await;

    let from = bounds.oldest;
    let to = bounds.newest + 1;
    let matched = fetch(&client, &connection, &topic, &filter(Some(from), Some(to))).await;

    assert!(!matched.is_empty(), "the full span should match every message");
    for timestamp in &matched {
        assert!(
            *timestamp >= from && *timestamp < to,
            "message stamped {timestamp} fell outside the requested window {from}..{to}"
        );
    }
}
