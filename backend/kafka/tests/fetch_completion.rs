//! Guards the fetch against waiting out its idle timeout on a topic whose
//! offsets are not all readable messages.
//!
//! `fetch_messages` works out how many messages it expects from the
//! partitions' watermarks — `high - low`, narrowed by the filter's caps —
//! and the poll loop runs until it has collected that many. That target is
//! only ever an **upper bound**: an offset can exist without a consumer ever
//! being handed a message for it.
//!
//! * A **transaction** writes a commit (or abort) marker into each partition
//!   it touched. The marker takes an offset; the broker never delivers it.
//! * **Compaction** removes superseded records, leaving their offsets behind.
//!
//! When the target cannot be reached, the only remaining way out of the loop
//! used to be `IDLE_TIMEOUT` — ten seconds of empty 500 ms polls, after which
//! the fetch returned the messages it had all along. Measured against a real
//! broker before the fix: an ordinary "newest 100" browse of a transactional
//! topic took **10,009 ms** and returned 94 messages, against **5-9 ms** for
//! the same browse of a plain one.
//!
//! It is the ordinary case, not an edge case: the newest offset in a
//! transactional partition is usually the last transaction's commit marker,
//! so a newest-first browse is short by at least one message almost every
//! time. Any Kafka Streams output topic is written this way.
//!
//! ```bash
//! docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0
//! ./scripts/e2e-fixtures.sh
//! SALTY_E2E_BOOTSTRAP=localhost:9092 \
//!   cargo test -p salty-kafka --test fetch_completion -- --nocapture
//! ```

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use salty_core::{Connection, MessageFilter, SecurityProtocol};
use salty_kafka::{KafkaClient, RdKafkaClient};

/// Created by `scripts/e2e-fixtures.sh`: 10,000 transactional records over 6
/// partitions, occupying rather more than 10,000 offsets.
const TOPIC: &str = "perf-txn";

/// The bug spent a flat `IDLE_TIMEOUT` (10 s) on every fetch of this topic,
/// and the fix does the same work in single-digit milliseconds. One second
/// separates those by an order of magnitude in both directions without
/// turning the test into a benchmark of whatever machine runs it.
const BUDGET: Duration = Duration::from_secs(1);

fn bootstrap_servers() -> Option<String> {
    std::env::var("SALTY_E2E_BOOTSTRAP").ok().filter(|value| !value.is_empty())
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

async fn timed_fetch(topic: &str, filter: MessageFilter) -> Option<(Duration, usize, Option<String>)> {
    let bootstrap = bootstrap_servers()?;
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    // The app reaches a Data tab through the tree, which lists topics first,
    // so the pooled metadata client is warm by the time Fetch is clicked.
    client.list_topics(&connection, Duration::from_secs(30)).await.expect("list_topics failed");

    let started = Instant::now();
    let result = client
        .fetch_messages(
            &connection,
            topic,
            &filter,
            None,
            Duration::from_secs(30),
            1_048_576,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("fetch failed");
    Some((started.elapsed(), result.messages.len(), result.poll_error))
}

fn browse(max: u32) -> MessageFilter {
    MessageFilter {
        partitions: None,
        max_messages_per_partition: Some(max),
        max_total_messages: Some(max),
        from_timestamp_ms: None,
        to_timestamp_ms: None,
        offset: None,
        include_payload: true,
        max_payload_preview_bytes: Some(4096),
    }
}

/// The common case: the Data tab's default browse of the newest messages.
#[tokio::test(flavor = "multi_thread")]
async fn a_newest_first_browse_of_a_transactional_topic_finishes_promptly() {
    let Some((elapsed, count, poll_error)) = timed_fetch(TOPIC, browse(100)).await else {
        eprintln!("skipped: set SALTY_E2E_BOOTSTRAP (and run scripts/e2e-fixtures.sh)");
        return;
    };

    println!("newest-100 browse of {TOPIC}: {count} messages in {} ms", elapsed.as_millis());
    assert!(count > 0, "the fixture topic is empty");
    assert!(
        elapsed < BUDGET,
        "a 100-message browse took {} ms. A transaction's commit marker occupies an offset \
         the broker never delivers, so the poll loop's message-count target is unreachable \
         and it waited out IDLE_TIMEOUT instead of stopping at the end of the partitions.",
        elapsed.as_millis(),
    );
    // Reaching the end of a partition is how this fetch finishes; it is not
    // something to report to the user as a failed read.
    assert_eq!(poll_error, None, "partition EOF leaked into the user-visible poll error");
}

/// The whole topic, where the shortfall is the same but the fetch has real
/// work to do as well — so this pins that the fix ends the fetch at the end
/// of the data rather than merely making the timeout shorter.
#[tokio::test(flavor = "multi_thread")]
async fn draining_a_transactional_topic_stops_at_the_end_of_the_data() {
    let Some((elapsed, count, poll_error)) = timed_fetch(TOPIC, browse(30_000)).await else {
        eprintln!("skipped: set SALTY_E2E_BOOTSTRAP (and run scripts/e2e-fixtures.sh)");
        return;
    };

    println!("full drain of {TOPIC}: {count} messages in {} ms", elapsed.as_millis());
    assert_eq!(count, 10_000, "the fixture topic is not the expected size");
    assert!(
        elapsed < BUDGET,
        "draining {count} messages took {} ms — see the note above; the commit markers \
         make `high - low` larger than the number of messages that can be collected.",
        elapsed.as_millis(),
    );
    assert_eq!(poll_error, None, "partition EOF leaked into the user-visible poll error");
}

/// The negative control for the two above: the same shapes against a topic
/// with no offset gaps must stay exactly as fast as they were, so a fix that
/// works by ending fetches early cannot pass by truncating ordinary ones.
#[tokio::test(flavor = "multi_thread")]
async fn an_ordinary_topic_still_returns_every_message_it_should() {
    let Some((elapsed, count, _)) = timed_fetch("perf-probe", browse(30_000)).await else {
        eprintln!("skipped: set SALTY_E2E_BOOTSTRAP (and run scripts/e2e-fixtures.sh)");
        return;
    };

    println!("full drain of perf-probe: {count} messages in {} ms", elapsed.as_millis());
    assert_eq!(count, 30_000, "an ordinary fetch lost messages");
    assert!(elapsed < Duration::from_secs(3), "took {} ms", elapsed.as_millis());
}
