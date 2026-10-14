//! Opening a row while a large fetch is still streaming must not wait for
//! that fetch, or for the compression of what it has already read.
//!
//! ```bash
//! SALTY_E2E_BOOTSTRAP=localhost:9092 \
//!   cargo test -p salty-kafka --test payload_cache_concurrent -- --nocapture
//! ```

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use salty_core::{Connection, MessageFilter, SecurityProtocol};
use salty_kafka::{KafkaClient, RdKafkaClient};

fn connection(bootstrap_servers: String) -> Connection {
    Connection {
        id: "e2e-concurrent".into(),
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

async fn run(cache_on: bool) -> (Duration, Duration, usize) {
    let bootstrap = std::env::var("SALTY_E2E_BOOTSTRAP").unwrap();
    let topic = std::env::var("SALTY_E2E_LARGE_TOPIC").unwrap_or_else(|_| "big-2mb".into());
    let client = Arc::new(RdKafkaClient::new());
    if !cache_on {
        client.set_payload_cache_limit(0);
    } else if let Some(mb) = std::env::var("SALTY_E2E_CACHE_MB")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        client.set_payload_cache_limit(mb * 1024 * 1024);
    }
    let connection = Arc::new(connection(bootstrap));

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let streamed = tokio::spawn(async move {
        let mut n = 0usize;
        while rx.recv().await.is_some() {
            n += 1;
        }
        n
    });
    let big = {
        let (client, connection, topic) = (client.clone(), connection.clone(), topic.clone());
        tokio::spawn(async move {
            let started = Instant::now();
            let filter = MessageFilter {
                max_payload_preview_bytes: Some(
                    std::env::var("SALTY_E2E_PREVIEW_BYTES")
                        .ok()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(4096),
                ),
                max_messages_per_partition: Some(1000),
                max_total_messages: Some(1500),
                ..MessageFilter::default()
            };
            let result = client
                .fetch_messages(
                    &connection,
                    &topic,
                    &filter,
                    Some(tx),
                    Duration::from_secs(30),
                    12 * 1024 * 1024,
                    None,
                    Arc::new(AtomicBool::new(false)),
                )
                .await
                .expect("big fetch");
            (started.elapsed(), result.messages.len())
        })
    };

    // Opening a row part-way through, as the user does.
    tokio::time::sleep(Duration::from_millis(
        std::env::var("SALTY_E2E_OPEN_AFTER_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(400),
    ))
    .await;
    let started = Instant::now();
    let one = MessageFilter {
        partitions: Some(vec![2]),
        max_messages_per_partition: Some(1),
        max_total_messages: Some(1),
        offset: Some(
            std::env::var("SALTY_E2E_ROW_OFFSET")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(11),
        ),
        ..MessageFilter::default()
    };
    let row = tokio::time::timeout(
        Duration::from_secs(60),
        client.fetch_messages(
            &connection,
            &topic,
            &one,
            None,
            Duration::from_secs(30),
            12 * 1024 * 1024,
            None,
            Arc::new(AtomicBool::new(false)),
        ),
    )
    .await
    .expect("opening a row hung")
    .expect("row fetch");
    let open = started.elapsed();
    assert_eq!(row.messages.len(), 1);

    let (big_elapsed, count) = big.await.unwrap();
    let _ = streamed.await;
    (open, big_elapsed, count)
}

#[tokio::test(flavor = "multi_thread")]
async fn opening_a_row_during_a_streaming_fetch_is_not_held_up() {
    if std::env::var("SALTY_E2E_BOOTSTRAP").is_err() {
        eprintln!("skipped: set SALTY_E2E_BOOTSTRAP to run this test");
        return;
    }
    for round in 0..2 {
        let (open_off, big_off, n_off) = run(false).await;
        let (open_on, big_on, n_on) = run(true).await;
        eprintln!(
            "round {round}: cache OFF open={open_off:?} fetch={big_off:?} ({n_off}) | cache ON open={open_on:?} fetch={big_on:?} ({n_on})"
        );
        assert!(open_on < Duration::from_secs(10), "{open_on:?}");
    }
}
