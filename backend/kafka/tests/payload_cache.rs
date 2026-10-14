//! The payload cache against a real broker: a row's whole payload is stored
//! from the fetch that brought its preview, and opening the row is then
//! answered from memory.
//!
//! Needs the `big-2mb` topic from `scripts/e2e-fixtures.sh`:
//!
//! ```bash
//! SALTY_E2E_BOOTSTRAP=localhost:9092 \
//!   cargo test -p salty-kafka --test payload_cache -- --nocapture
//! ```

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use salty_core::{Connection, MessageFilter, SecurityProtocol};
use salty_kafka::{KafkaClient, RdKafkaClient};

const DEFAULT_TOPIC: &str = "big-2mb";

fn bootstrap_servers() -> Option<String> {
    std::env::var("SALTY_E2E_BOOTSTRAP")
        .ok()
        .filter(|value| !value.is_empty())
}

fn topic() -> String {
    std::env::var("SALTY_E2E_LARGE_TOPIC").unwrap_or_else(|_| DEFAULT_TOPIC.to_string())
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

async fn fetch(
    client: &RdKafkaClient,
    connection: &Connection,
    topic: &str,
    filter: &MessageFilter,
) -> salty_core::MessageFetchResult {
    client
        .fetch_messages(
            connection,
            topic,
            filter,
            None,
            Duration::from_secs(30),
            12 * 1024 * 1024,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("fetch failed")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_grid_fetch_fills_the_cache_and_opening_a_row_is_served_from_it() {
    let Some(bootstrap) = bootstrap_servers() else {
        eprintln!("skipped: set SALTY_E2E_BOOTSTRAP to run this test");
        return;
    };
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    let topic = topic();

    // What the Data tab's grid asks for: a small bounded preview of each row.
    let grid = MessageFilter {
        max_messages_per_partition: Some(5),
        max_total_messages: Some(5),
        max_payload_preview_bytes: Some(4096),
        ..MessageFilter::default()
    };
    let rows = fetch(&client, &connection, &topic, &grid).await;
    let row = rows
        .messages
        .iter()
        .find(|m| m.payload_size_bytes.is_some_and(|size| size > 4096))
        .expect("the fixture topic should hold messages over 4 KB");
    let size = row.payload_size_bytes.unwrap();

    // The store is a background job; give it a moment.
    let cache = client.payload_cache().clone();
    let deadline = Instant::now() + Duration::from_secs(20);
    while cache.entry_count() == 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(cache.entry_count() > 0, "nothing was cached from the fetch");

    // Opening the row: one message, whole.
    let open = MessageFilter {
        partitions: Some(vec![row.partition]),
        max_messages_per_partition: Some(1),
        max_total_messages: Some(1),
        offset: Some(row.offset),
        max_payload_preview_bytes: None,
        ..MessageFilter::default()
    };
    let started = Instant::now();
    let opened = fetch(&client, &connection, &topic, &open).await;
    let elapsed = started.elapsed();
    println!("served from cache in {elapsed:?}");

    let message = opened.messages.first().expect("the row should come back");
    assert_eq!(message.payload_size_bytes, Some(size));
    assert_eq!(
        base64_len(message.payload_base64.as_deref().unwrap()),
        size as usize,
        "the whole payload must come back"
    );
    assert_eq!(cache.hits(), 1, "opening the row should have been a hit");
}

fn base64_len(encoded: &str) -> usize {
    let padding = encoded.bytes().rev().take_while(|&b| b == b'=').count();
    encoded.len() / 4 * 3 - padding
}

/// What the cache is for, as a comparison: the same row opened by a client
/// with a cold cache (a broker round trip) and by one with a warm cache.
/// Also the miss path: a cold open must itself fill the entry.
#[tokio::test(flavor = "multi_thread")]
async fn a_miss_goes_to_the_broker_and_fills_the_cache_and_release_empties_it() {
    let Some(bootstrap) = bootstrap_servers() else {
        eprintln!("skipped: set SALTY_E2E_BOOTSTRAP to run this test");
        return;
    };
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    let topic = topic();

    let grid = MessageFilter {
        max_messages_per_partition: Some(3),
        max_total_messages: Some(3),
        max_payload_preview_bytes: Some(4096),
        ..MessageFilter::default()
    };
    let rows = fetch(&client, &connection, &topic, &grid).await;
    let row = rows
        .messages
        .iter()
        .find(|m| m.payload_size_bytes.is_some_and(|size| size > 4096))
        .expect("the fixture topic should hold messages over 4 KB");
    let open = MessageFilter {
        partitions: Some(vec![row.partition]),
        max_messages_per_partition: Some(1),
        max_total_messages: Some(1),
        offset: Some(row.offset),
        max_payload_preview_bytes: None,
        ..MessageFilter::default()
    };

    // Cold: this client has never stored or read anything, so the open goes
    // to the broker.
    let cold_client = RdKafkaClient::new();
    let started = Instant::now();
    let cold = fetch(&cold_client, &connection, &topic, &open).await;
    let cold_elapsed = started.elapsed();
    assert_eq!(cold_client.payload_cache().misses(), 1);
    assert_eq!(cold_client.payload_cache().hits(), 0);

    // That open stored its result, in the background.
    let deadline = Instant::now() + Duration::from_secs(20);
    while cold_client.payload_cache().entry_count() == 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        cold_client.payload_cache().entry_count(),
        1,
        "a miss should add the entry"
    );

    // Warm: same client, same row.
    let started = Instant::now();
    let warm = fetch(&cold_client, &connection, &topic, &open).await;
    let warm_elapsed = started.elapsed();
    println!(
        "open of one {}-byte row: broker {cold_elapsed:?}, cache {warm_elapsed:?}",
        row.payload_size_bytes.unwrap()
    );
    assert_eq!(cold_client.payload_cache().hits(), 1);
    assert_eq!(
        cold.messages[0], warm.messages[0],
        "a hit must equal what the broker said"
    );
<<<<<<< HEAD
    // The two times are printed above, not compared: on a loopback broker the
    // whole gap is a few milliseconds and an instrumented CI runner can invert
    // it, which would fail the build for noise. What proves the cache served
    // the second open is the hit counter, which timing cannot fake.
=======
    assert!(
        warm_elapsed < cold_elapsed,
        "serving from the cache ({warm_elapsed:?}) should beat the broker ({cold_elapsed:?})"
    );
>>>>>>> origin/main

    // Releasing the connection forgets everything cached for it.
    cold_client.release(&connection.id);
    assert_eq!(cold_client.payload_cache().entry_count(), 0);
}
