//! The pooled client's own account of the cluster, against a real broker.
//!
//! `check_status` answers from librdkafka's statistics for a connected
//! cluster instead of dialling the broker, so a status poll costs the cluster
//! nothing. The unit tests beside `client.rs` pin the decision logic; what only
//! a broker can show is that the statistics really do report the connection as
//! up, and keep doing so once the connect probe's own success is too old to
//! count.
//!
//! ```bash
//! docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0
//! SALTY_E2E_BOOTSTRAP=localhost:9092 \
//!   cargo test -p salty-kafka --test liveness_status
//! ```

use std::time::Duration;

use salty_core::{Connection, ConnectionStatus, INTERVAL_SLACK_MS, SecurityProtocol};
use salty_kafka::{KafkaClient, RdKafkaClient};

fn bootstrap_servers() -> Option<String> {
    std::env::var("SALTY_E2E_BOOTSTRAP")
        .ok()
        .filter(|value| !value.is_empty())
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

#[tokio::test]
async fn a_connected_cluster_reads_as_reachable_straight_after_connect() {
    let bootstrap = broker!();
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);

    client.connect(&connection).await.unwrap();

    assert_eq!(
        client.check_status(&connection).await.unwrap(),
        ConnectionStatus::Reachable
    );
}

// Waits out `INTERVAL_SLACK_MS` with no request in between, so the connect
// probe's own success is too old to count and only the statistics callback —
// delivered when the status read serves the client's queue — can still say the
// broker is up.
#[tokio::test]
async fn a_connected_cluster_stays_reachable_on_statistics_alone() {
    let bootstrap = broker!();
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    client.connect(&connection).await.unwrap();

    tokio::time::sleep(Duration::from_millis(INTERVAL_SLACK_MS + 2_500)).await;

    assert_eq!(
        client.check_status(&connection).await.unwrap(),
        ConnectionStatus::Reachable
    );
}

// The regression this exists for: the app checks status every 10 s while
// librdkafka emits a statistics report every 5 s, so two reports queue up
// between checks. A drain that served only one per check fell further behind
// each time, read ever-staler sightings, and a perfectly healthy idle cluster
// went Reachable -> Unknown -> Unreachable by 30 s — which the frontend turns
// into an automatic disconnect. Polling faster than the reports arrive (as the
// other tests here do) hides it completely.
#[tokio::test]
async fn an_idle_connected_cluster_stays_reachable_at_the_apps_polling_cadence() {
    let bootstrap = broker!();
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    client.connect(&connection).await.unwrap();

    for check in 0..5 {
        assert_eq!(
            client.check_status(&connection).await.unwrap(),
            ConnectionStatus::Reachable,
            "check {check}, {}s in, with nothing happening on the cluster",
            check * 10
        );
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}
