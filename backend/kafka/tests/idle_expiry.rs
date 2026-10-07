//! A pooled client nobody asks for is closed — against a broker that rejects
//! its credentials.
//!
//! librdkafka has no "give up on bad credentials" setting: a client whose
//! password the broker refuses keeps re-dialling it, every 30 seconds once
//! backed off, for as long as it lives. If a late request recreates a pooled
//! client after the user has disconnected, nothing would ever close it. Idle
//! expiry does.
//!
//! What makes this checkable without reading the broker's log: the broker's
//! port is *open*, so the TCP fallback in `check_status` answers `Reachable`,
//! while a pooled client that cannot authenticate never reports a broker up.
//! `Reachable` therefore means "no client left".
//!
//! ```bash
//! ./scripts/e2e-acl-fixtures.sh
//! SALTY_E2E_ACL_BOOTSTRAP=localhost:9192 \
//!   cargo test -p salty-kafka --test idle_expiry
//! ```

use std::time::Duration;

use salty_core::{Connection, ConnectionStatus, SaslMechanism, SecurityProtocol};
use salty_kafka::{KafkaClient, RdKafkaClient};

fn bootstrap_servers() -> Option<String> {
    std::env::var("SALTY_E2E_ACL_BOOTSTRAP")
        .ok()
        .filter(|value| !value.is_empty())
}

macro_rules! acl_broker {
    () => {
        match bootstrap_servers() {
            Some(bootstrap) => bootstrap,
            None => {
                eprintln!("skipped: run ./scripts/e2e-acl-fixtures.sh and set SALTY_E2E_ACL_BOOTSTRAP to run this test");
                return;
            }
        }
    };
}

fn connection_as(bootstrap_servers: String, username: &str, password: &str) -> Connection {
    Connection {
        id: format!("e2e-acl-{username}"),
        name: format!("e2e-acl-{username}"),
        bootstrap_servers,
        kafka_version: "3.9".into(),
        zookeeper_enabled: false,
        zookeeper_host: None,
        zookeeper_port: None,
        zookeeper_chroot_path: None,
        security_protocol: SecurityProtocol::SaslPlaintext,
        sasl_mechanism: Some(SaslMechanism::Plain),
        sasl_username: Some(username.into()),
        sasl_password: Some(password.into()),
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
        // Deliberately true for both principals. The point of this file is what
        // happens when the app's own gate has been cleared: the broker is the
        // authority, and it must refuse `reader` regardless.
        allow_publishing: true,
        created_at: "now".into(),
        updated_at: "now".into(),
    }
}

#[tokio::test]
async fn an_abandoned_client_with_rejected_credentials_is_closed_after_the_ttl() {
    let bootstrap = acl_broker!();
    let client =
        RdKafkaClient::new().with_idle_expiry(Duration::from_secs(6), Duration::from_millis(500));
    let connection = connection_as(bootstrap, "writer", "WRONG-PASSWORD");

    // The request that leaves a pooled client behind. Nothing releases it —
    // that is the lingering case.
    assert!(
        client
            .list_brokers(&connection, Duration::from_secs(4))
            .await
            .is_err()
    );
    assert_ne!(
        client.check_status(&connection).await.unwrap(),
        ConnectionStatus::Reachable,
        "the pooled client should still exist and still be failing"
    );

    // Nobody touches it again. (`check_status` above was the last touch.) The
    // TTL is longer than the request above, which spends its whole timeout
    // failing to authenticate and does not refresh the client while it runs.
    tokio::time::sleep(Duration::from_secs(8)).await;

    assert_eq!(
        client.check_status(&connection).await.unwrap(),
        ConnectionStatus::Reachable,
        "the client should be gone, leaving only the TCP fallback to find the open port"
    );
}

#[tokio::test]
async fn a_client_that_keeps_being_polled_is_not_closed() {
    let bootstrap = acl_broker!();
    let client =
        RdKafkaClient::new().with_idle_expiry(Duration::from_secs(6), Duration::from_millis(500));
    let connection = connection_as(bootstrap, "writer", "WRONG-PASSWORD");
    assert!(
        client
            .list_brokers(&connection, Duration::from_secs(4))
            .await
            .is_err()
    );

    // Polled well inside the TTL for longer than the TTL.
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_ne!(
            client.check_status(&connection).await.unwrap(),
            ConnectionStatus::Reachable,
            "a polled client must not be closed"
        );
    }
}
