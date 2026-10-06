//! What a fetch actually costs on a topic of large messages.
//!
//! The Data tab's two count filters used to combine into "read the whole
//! per-partition window from every partition, then show the first N" — so a
//! 100-message browse of a 12-partition topic read 1,200 records and threw
//! 1,100 away. On a topic of 2-10 MB JSON that is gigabytes of network for
//! one click, and it is why the tab felt broken rather than slow.
//!
//! This measures the two shapes against a real broker: a fetch with an
//! overall budget, and the same fetch without one (which is what every
//! default fetch used to be). Run it against a topic of large messages:
//!
//! ```powershell
//! pwsh scripts/e2e-large-message-fixtures.ps1
//! $env:SALTY_E2E_BOOTSTRAP = "localhost:9092"
//! cargo test -p salty-kafka --test fetch_budget -- --nocapture
//! ```

use std::collections::BTreeSet;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use salty_core::{Connection, MessageFilter, SecurityProtocol};
use salty_kafka::{KafkaClient, RdKafkaClient};

/// Matches `scripts/e2e-large-message-fixtures.ps1`.
const DEFAULT_TOPIC: &str = "big-2mb";
/// Overridable so the same measurement can be pointed at a real topic
/// shape, e.g. a 100-message browse of a 600k-message topic.
fn budget() -> u32 {
    std::env::var("SALTY_E2E_BUDGET").ok().and_then(|v| v.parse().ok()).unwrap_or(20)
}

fn bootstrap_servers() -> Option<String> {
    std::env::var("SALTY_E2E_BOOTSTRAP").ok().filter(|value| !value.is_empty())
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

fn filter(max_total_messages: Option<u32>) -> MessageFilter {
    MessageFilter {
        partitions: None,
        max_messages_per_partition: Some(100),
        max_total_messages,
        from_timestamp_ms: None,
        to_timestamp_ms: None,
        offset: None,
        include_payload: true,
        // Measures what a fetch costs on the wire, which preview truncation
        // does not change — librdkafka still receives the whole record.
        max_payload_preview_bytes: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_overall_budget_reads_only_what_it_returns() {
    let Some(bootstrap) = bootstrap_servers() else {
        eprintln!("skipped: set SALTY_E2E_BOOTSTRAP to run this test");
        return;
    };

    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    let topic = topic();

    let mut timings = Vec::new();
    for (label, max_total) in [("with an overall budget", Some(budget())), ("no budget (the old default)", None)] {
        let started = Instant::now();
        let fetched = client
            .fetch_messages(
                &connection,
                &topic,
                &filter(max_total),
                None,
                Duration::from_secs(30),
                12 * 1024 * 1024,
                None,
                Arc::new(AtomicBool::new(false)),
            )
            .await
            .expect("fetch failed");
        let elapsed = started.elapsed();

        let bytes: usize = fetched
            .messages
            .iter()
            .map(|m| m.payload_base64.as_ref().map(|p| p.len() * 3 / 4).unwrap_or(0))
            .sum();
        let partitions: BTreeSet<i32> = fetched.messages.iter().map(|m| m.partition).collect();

        println!(
            "{label:<28} {:>4} messages  {:>7.1} MB  {:>7.2}s  across {} partition(s)",
            fetched.messages.len(),
            bytes as f64 / 1_048_576.0,
            elapsed.as_secs_f64(),
            partitions.len(),
        );
        timings.push((fetched.messages.len(), bytes, elapsed, partitions));
    }

    let (budgeted_count, budgeted_bytes, budgeted_elapsed, budgeted_partitions) = timings[0].clone();
    let (_unbudgeted_count, unbudgeted_bytes, _, _) = timings[1].clone();

    assert_eq!(
        budgeted_count, budget() as usize,
        "a budget of {} should read exactly that many messages, got {budgeted_count}", budget()
    );

    // The regression that made this tab unusable: the budget used to be
    // spent entirely on the lowest-numbered partition, so "the newest 20 in
    // this topic" was really "the newest 20 in partition 0".
    assert!(
        budgeted_partitions.len() > 1,
        "the budget should be spread over the topic's partitions, but every message came from {budgeted_partitions:?}"
    );

    assert!(
        budgeted_bytes < unbudgeted_bytes,
        "a budgeted fetch must move less data than an unbudgeted one ({budgeted_bytes} vs {unbudgeted_bytes} bytes)"
    );

    println!(
        "budgeted fetch moved {:.1}x less data and took {:.2}s",
        unbudgeted_bytes as f64 / budgeted_bytes.max(1) as f64,
        budgeted_elapsed.as_secs_f64(),
    );
}
