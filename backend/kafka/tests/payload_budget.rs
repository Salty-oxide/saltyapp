//! What Max Total Fetch Size charges for, against a real broker.
//!
//! The budget bounds what a fetch *keeps*, not what it reads past. A browse
//! with "Fetch message payload" off keeps none of the payload, so it must
//! never be stopped by a size limit — before this, browsing a topic of
//! multi-megabyte records stopped after a handful of rows and reported a
//! size limit against a result that carried no payloads at all.
//!
//! Needs a topic of large messages:
//!
//! ```bash
//! docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0
//! docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
//!   --create --topic big-msgs --partitions 1 --replication-factor 1
//! # ...produce 20 x 512 KB records into it...
//! SALTY_E2E_BOOTSTRAP=localhost:9092 \
//!   cargo test -p salty-kafka --test payload_budget -- --nocapture
//! ```

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use salty_core::{Connection, MessageFilter, SecurityProtocol};
use salty_kafka::{KafkaClient, RdKafkaClient};

const DEFAULT_TOPIC: &str = "big-msgs";

/// Far smaller than the topic's contents (20 x 512 KB), so a fetch that is
/// charged for payloads is certain to stop early on it.
const BUDGET_BYTES: u64 = 2 * 1024 * 1024;

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

fn filter(include_payload: bool) -> MessageFilter {
    MessageFilter {
        partitions: None,
        max_messages_per_partition: Some(100),
        max_total_messages: Some(100),
        from_timestamp_ms: None,
        to_timestamp_ms: None,
        offset: None,
        include_payload,
        max_payload_preview_bytes: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_byte_budget_charges_only_for_payloads_the_fetch_keeps() {
    let Some(bootstrap) = bootstrap_servers() else {
        eprintln!("skipped: set SALTY_E2E_BOOTSTRAP to run this test");
        return;
    };

    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    let topic = topic();

    let fetch = |include_payload: bool| {
        let client = &client;
        let connection = &connection;
        let topic = topic.clone();
        async move {
            client
                .fetch_messages(
                    connection,
                    &topic,
                    &filter(include_payload),
                    None,
                    Duration::from_secs(30),
                    12 * 1024 * 1024,
                    Some(BUDGET_BYTES),
                    Arc::new(AtomicBool::new(false)),
                )
                .await
                .expect("fetch failed")
        }
    };

    let browse = fetch(false).await;
    let with_payloads = fetch(true).await;

    println!(
        "payload off: {:>3} messages, {} bytes charged, stopped_at_byte_budget = {}",
        browse.messages.len(),
        browse.payload_bytes_read,
        browse.stopped_at_byte_budget,
    );
    println!(
        "payload on : {:>3} messages, {} bytes charged, stopped_at_byte_budget = {}",
        with_payloads.messages.len(),
        with_payloads.payload_bytes_read,
        with_payloads.stopped_at_byte_budget,
    );

    // The browse keeps nothing, so it is charged nothing and the budget
    // never ends it.
    assert_eq!(
        browse.payload_bytes_read, 0,
        "a metadata-only browse must not be charged for the payloads it drops"
    );
    assert!(
        !browse.stopped_at_byte_budget,
        "a metadata-only browse must never stop on the byte budget"
    );
    assert!(
        browse.messages.iter().all(|m| m.payload_base64.is_none()),
        "a metadata-only browse must not carry payloads"
    );
    // Sizes are still reported, which is what the grid shows and what the
    // per-row 'Fetch payload' button is charged for later.
    assert!(
        browse.messages.iter().all(|m| m.payload_size_bytes.is_some_and(|size| size > 0)),
        "a metadata-only browse must still report each message's real size"
    );

    // The same topic, the same budget, payloads kept: the guard still bites.
    assert!(
        with_payloads.stopped_at_byte_budget,
        "a fetch that keeps payloads must still stop on the byte budget"
    );
    assert!(
        with_payloads.payload_bytes_read >= BUDGET_BYTES,
        "a fetch stopped on the budget must have read at least it, got {}",
        with_payloads.payload_bytes_read
    );

    // The point of the change, stated as the comparison the user reported.
    assert!(
        browse.messages.len() > with_payloads.messages.len(),
        "the browse should reach further into the topic than the payload fetch the budget stops \
         ({} vs {} messages)",
        browse.messages.len(),
        with_payloads.messages.len()
    );
}
