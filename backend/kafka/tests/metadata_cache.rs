//! The 24 h metadata cache (`salty_kafka::metadata_cache`) against a real
//! broker.
//!
//! The unit tests beside `client.rs` prove hit / miss / refresh / purge against
//! a closed port, where the only live read is a failure. What only a broker can
//! show is that a hit really is *the earlier answer* (not a quiet re-read),
//! that a refresh replaces it with the broker's current answer, and that a
//! successful read is what fills the cache.
//!
//! ```bash
//! docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0
//! scripts/e2e-fixtures.sh   # creates `e2e-basic` (3 partitions)
//! SALTY_E2E_BOOTSTRAP=localhost:9092 \
//!   cargo test -p salty-kafka --test metadata_cache
//! ```

use std::time::Duration;

use salty_core::{Connection, SecurityProtocol};
use salty_kafka::{KafkaClient, RdKafkaClient};

const TOPIC: &str = "e2e-basic";
const READ_TIMEOUT: Duration = Duration::from_secs(30);

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn bootstrap_servers() -> Option<String> {
    std::env::var("SALTY_E2E_BOOTSTRAP")
        .ok()
        .filter(|value| !value.is_empty())
}

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

#[tokio::test]
async fn a_read_fills_the_cache_and_the_next_one_is_the_same_answer() {
    let bootstrap = broker!();
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);

    let first = client
        .cached_partitions(&connection, TOPIC, false, READ_TIMEOUT)
        .await
        .expect("first read goes to the broker");
    assert_eq!(first.value.len(), 3);

    // A re-read would stamp a later time; a hit returns the stored one.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let second = client
        .cached_partitions(&connection, TOPIC, false, READ_TIMEOUT)
        .await
        .unwrap();

    assert_eq!(second, first);
}

#[tokio::test]
async fn refresh_replaces_a_stale_entry_with_the_brokers_current_answer() {
    let bootstrap = broker!();
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    // Something the broker would never say, from a minute ago: inside the TTL,
    // so a plain read must serve it.
    let seeded_at = now_ms() - 60_000;
    client.metadata_cache().store_partitions(
        &connection.id,
        TOPIC,
        Vec::new(),
        seeded_at,
        salty_kafka::metadata_cache::Session::ANY,
    );

    let served = client
        .cached_partitions(&connection, TOPIC, false, READ_TIMEOUT)
        .await
        .unwrap();
    assert!(
        served.value.is_empty(),
        "a hit is served without asking the broker"
    );
    assert_eq!(served.fetched_at_ms, seeded_at);

    let refreshed = client
        .cached_partitions(&connection, TOPIC, true, READ_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(refreshed.value.len(), 3);
    assert!(refreshed.fetched_at_ms > seeded_at);

    // ...and what it read is what the next plain read returns.
    let after = client
        .cached_partitions(&connection, TOPIC, false, READ_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(after, refreshed);
}

#[tokio::test]
async fn the_listings_and_config_are_cached_and_a_release_empties_them() {
    let bootstrap = broker!();
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);

    let brokers = client
        .cached_brokers(&connection, false, READ_TIMEOUT)
        .await
        .unwrap();
    let topics = client
        .cached_topics(&connection, false, READ_TIMEOUT)
        .await
        .unwrap();
    let config = client
        .cached_topic_config(&connection, TOPIC, false, READ_TIMEOUT)
        .await
        .unwrap();
    assert!(!brokers.value.is_empty());
    assert!(topics.value.iter().any(|t| t.name == TOPIC));
    assert!(!config.value.is_empty());
    assert_eq!(client.metadata_cache().len(), 3);

    client.release(&connection.id);

    assert!(client.metadata_cache().is_empty());
}

#[tokio::test]
async fn a_new_connect_starts_from_an_empty_cache() {
    let bootstrap = broker!();
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    client
        .cached_topics(&connection, false, READ_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(client.metadata_cache().len(), 1);

    client.connect(&connection).await.expect("connects");

    assert!(client.metadata_cache().is_empty());
}
