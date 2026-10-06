//! Proof that this build can actually *read* a topic compressed with any
//! codec a real producer may have used.
//!
//! Everything else that checks this — `build_info`'s unit tests, the startup
//! log line — asks librdkafka what it believes it supports. That is one step
//! removed from the thing users hit, which is a fetch against real broker
//! data: a batch arrives, the decompression switch in
//! `rdkafka_msgset_reader.c` has no arm for its codec, and every poll fails
//! with `NotImplemented (Local: Not implemented)` while the topic looks
//! perfectly healthy from every other tab.
//!
//! So this test does the whole round trip. Point it at a broker that already
//! holds compressed data (see `scripts/e2e-compression-fixtures.ps1`, which
//! produces it with the *Java* client — the same producer a user's services
//! run, and independent of whatever this build compiled in) and it fetches
//! each topic through the very code path the Data tab uses.
//!
//! ```powershell
//! $env:SALTY_E2E_BOOTSTRAP = "localhost:9092"
//! cargo test -p salty-kafka --test compression_codecs -- --nocapture
//! ```
//!
//! With no `SALTY_E2E_BOOTSTRAP` set there is no broker to talk to, so
//! the test reports itself skipped rather than failing — `cargo test` on a
//! machine with no Kafka stays green.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use salty_core::{Connection, MessageFilter, SecurityProtocol};
use salty_kafka::{KafkaClient, RdKafkaClient};

/// Codecs a producer can choose, and which this app therefore has to be able
/// to read. `lz4` is bundled into librdkafka unconditionally, so it doubles
/// as a control: if it fails too, the broker/fixtures are wrong rather than
/// the build.
const CODECS: &[&str] = &["gzip", "snappy", "lz4", "zstd"];

/// How many messages `scripts/e2e-compression-fixtures.ps1` writes per topic.
const EXPECTED_MESSAGES: usize = 20;

fn bootstrap_servers() -> Option<String> {
    std::env::var("SALTY_E2E_BOOTSTRAP").ok().filter(|value| !value.is_empty())
}

/// Topics are named `<prefix><codec>`; the prefix is overridable so a run
/// against a shared cluster doesn't have to own the plain names.
fn topic_for(codec: &str) -> String {
    let prefix = std::env::var("SALTY_E2E_TOPIC_PREFIX").unwrap_or_else(|_| "c-".to_string());
    format!("{prefix}{codec}")
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

/// The Data tab's own defaults: newest-first, payloads included (the payload
/// is the part that has to survive decompression).
fn filter() -> MessageFilter {
    MessageFilter {
        partitions: None,
        max_messages_per_partition: Some(EXPECTED_MESSAGES as u32),
        max_total_messages: Some(EXPECTED_MESSAGES as u32),
        from_timestamp_ms: None,
        to_timestamp_ms: None,
        offset: None,
        include_payload: true,
        // Asserts on the decompressed payload itself, so it needs all of it.
        max_payload_preview_bytes: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn every_compression_codec_can_be_fetched() {
    let Some(bootstrap) = bootstrap_servers() else {
        eprintln!(
            "skipped: set SALTY_E2E_BOOTSTRAP (and run \
             scripts/e2e-compression-fixtures.ps1) to run this test"
        );
        return;
    };

    println!("librdkafka builtin.features = {}", salty_kafka::build_info::builtin_features());

    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    let mut failures = Vec::new();

    for codec in CODECS {
        let topic = topic_for(codec);
        let result = client
            .fetch_messages(
                &connection,
                &topic,
                &filter(),
                None,
                Duration::from_secs(10),
                1_048_576,
                None,
                Arc::new(AtomicBool::new(false)),
            )
            .await;

        match result {
            Err(err) => failures.push(format!("{codec}: fetch failed outright: {err:?}")),
            Ok(fetched) => {
                // A poll error is the shape this bug takes: the fetch
                // "succeeds" with zero messages while every poll underneath
                // it failed, which is exactly why the Data tab looks empty
                // rather than broken.
                if let Some(poll_error) = &fetched.poll_error {
                    failures.push(format!("{codec}: poll error: {poll_error}"));
                }
                if fetched.messages.len() != EXPECTED_MESSAGES {
                    failures.push(format!(
                        "{codec}: got {} messages, expected {EXPECTED_MESSAGES}",
                        fetched.messages.len()
                    ));
                    continue;
                }

                // Decompression that silently produces garbage would pass a
                // count-only check, so look at the bytes.
                for message in &fetched.messages {
                    let payload = message
                        .payload_base64
                        .as_ref()
                        .map(|encoded| BASE64.decode(encoded).expect("payload is base64"))
                        .unwrap_or_default();
                    let text = String::from_utf8_lossy(&payload);
                    if !text.starts_with("msg-") {
                        failures.push(format!(
                            "{codec}: payload at offset {} did not decompress to the \
                             produced text, got {text:?}",
                            message.offset
                        ));
                        break;
                    }
                }
                println!("{codec}: {} messages read and decompressed", fetched.messages.len());
            }
        }
    }

    assert!(
        failures.is_empty(),
        "this build cannot read every compression codec:\n  {}\nlibrdkafka builtin.features = {}",
        failures.join("\n  "),
        salty_kafka::build_info::builtin_features(),
    );
}
