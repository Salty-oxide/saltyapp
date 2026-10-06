//! `DescribeAcls` against brokers that really have (and really lack) an
//! authorizer.
//!
//! The FFI wrapper in `salty_kafka::acl` cannot be unit-tested: every line of
//! it either calls into librdkafka or reads what librdkafka returned. What it
//! *can* be checked against is the two brokers this repo already knows how to
//! start, which between them cover the two outcomes the whole feature is
//! built around.
//!
//! The ACL broker, for the listing itself and for the three-state
//! classification when an authorizer is running:
//!
//! ```bash
//! ./scripts/e2e-acl-fixtures.sh
//! SALTY_E2E_ACL_BOOTSTRAP=localhost:9192 \
//!   cargo test -p salty-kafka --test acl_describe
//! ```
//!
//! These tests also pin down the librdkafka behaviour that forced the
//! availability design to change. `rd_kafka_DescribeAclsResponse_parse`
//! (librdkafka 2.12.1) reads the response's `error_code`, uses it only to
//! reassign a local `errstr` pointer, and then returns
//! `RD_KAFKA_RESP_ERR_NO_ERROR` unconditionally — so a refusal and an
//! unsecured broker both reach the client as a successful, empty listing.
//! The app recovers what it can from the broker's own
//! `authorizer.class.name` and declines to guess the rest.
//!
//! The ordinary broker, which has no authorizer at all, for the
//! `NoAuthorizer` state. On this broker a naive implementation reports an
//! empty list — which reads as "nothing is granted" when the truth is the
//! exact opposite:
//!
//! ```bash
//! ./scripts/e2e-fixtures.sh
//! SALTY_E2E_BOOTSTRAP=localhost:9092 \
//!   cargo test -p salty-kafka --test acl_describe
//! ```

use std::time::Duration;

use salty_core::{
    AclAvailability, AclFilter, AclOperation, AclPermission, Connection, PatternType, ResourceType,
    SaslMechanism, SecurityProtocol,
};

use salty_kafka::{KafkaClient, RdKafkaClient};

/// Created by `scripts/e2e-acl-fixtures.sh`.
const TOPIC: &str = "e2e-acl-publish";
/// The prefixed grant `scripts/e2e-acl-fixtures.sh` gives `writer`.
const PREFIX: &str = "e2e-acl-prefixed-";
const READ_TIMEOUT: Duration = Duration::from_secs(30);

fn acl_bootstrap() -> Option<String> {
    std::env::var("SALTY_E2E_ACL_BOOTSTRAP").ok().filter(|v| !v.is_empty())
}

fn plain_bootstrap() -> Option<String> {
    std::env::var("SALTY_E2E_BOOTSTRAP").ok().filter(|v| !v.is_empty())
}

macro_rules! acl_broker {
    () => {
        match acl_bootstrap() {
            Some(bootstrap) => bootstrap,
            None => {
                eprintln!(
                    "skipped: run ./scripts/e2e-acl-fixtures.sh and set \
                     SALTY_E2E_ACL_BOOTSTRAP to run this test"
                );
                return;
            }
        }
    };
}

macro_rules! plain_broker {
    () => {
        match plain_bootstrap() {
            Some(bootstrap) => bootstrap,
            None => {
                eprintln!(
                    "skipped: run ./scripts/e2e-fixtures.sh and set \
                     SALTY_E2E_BOOTSTRAP to run this test"
                );
                return;
            }
        }
    };
}

fn blank_connection(id: &str, bootstrap_servers: String) -> Connection {
    Connection {
        id: id.into(),
        name: id.into(),
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

/// A connection authenticating as one of the ACL fixture's principals.
fn connection_as(bootstrap_servers: String, username: &str, password: &str) -> Connection {
    Connection {
        security_protocol: SecurityProtocol::SaslPlaintext,
        sasl_mechanism: Some(SaslMechanism::Plain),
        sasl_username: Some(username.into()),
        sasl_password: Some(password.into()),
        ..blank_connection(&format!("e2e-acl-describe-{username}"), bootstrap_servers)
    }
}

// The listing has to come back through the FFI with its fields intact — this
// is the one test that proves the C-to-Rust field mapping is not scrambled,
// since every enum in it is a separate numeric conversion.
#[tokio::test]
async fn lists_the_fixtures_acls_for_a_super_user() {
    let bootstrap = acl_broker!();
    let client = RdKafkaClient::new();
    let admin = connection_as(bootstrap, "admin", "admin-secret");

    let listing = client
        .describe_acls(&admin, AclFilter::any(), READ_TIMEOUT)
        .await
        .expect("admin should be able to list ACLs");

    assert_eq!(listing.availability, AclAvailability::Available);
    assert!(
        !listing.bindings.is_empty(),
        "the fixture grants several ACLs; got none"
    );
    assert!(
        listing.binding_errors.is_empty(),
        "unexpected per-binding errors: {:?}",
        listing.binding_errors
    );

    // Every field decoded, on a binding whose exact shape the fixture script
    // controls.
    let writer_write = listing
        .bindings
        .iter()
        .find(|binding| {
            binding.principal == "User:writer"
                && binding.resource_name == TOPIC
                && binding.operation == AclOperation::Write
        })
        .expect("the fixture grants User:writer Write on the topic");

    assert_eq!(writer_write.resource_type, ResourceType::Topic);
    assert_eq!(writer_write.pattern_type, PatternType::Literal);
    assert_eq!(writer_write.permission, AclPermission::Allow);
    assert_eq!(writer_write.host, "*");
}

// The omission `publish_authorization.rs` depends on, observed from the other
// side: the ACL listing must show that `reader` has no Write grant. If this
// ever fails, that file is passing for the wrong reason.
#[tokio::test]
async fn shows_that_the_read_only_principal_has_no_write_grant() {
    let bootstrap = acl_broker!();
    let client = RdKafkaClient::new();
    let admin = connection_as(bootstrap, "admin", "admin-secret");

    let listing = client
        .describe_acls(&admin, AclFilter::any(), READ_TIMEOUT)
        .await
        .expect("admin should be able to list ACLs");

    let reader_ops: Vec<AclOperation> = listing
        .bindings
        .iter()
        .filter(|binding| binding.principal == "User:reader" && binding.resource_name == TOPIC)
        .map(|binding| binding.operation)
        .collect();

    assert!(
        reader_ops.contains(&AclOperation::Read),
        "expected reader to hold Read; got {reader_ops:?}"
    );
    assert!(
        !reader_ops.contains(&AclOperation::Write),
        "reader must not hold Write; got {reader_ops:?}"
    );
}

// MATCH mode is what lets the app do no pattern matching of its own, so it
// has to actually work: asking about a concrete topic name must return the
// PREFIXED binding that governs it, not just literal matches on that name.
#[tokio::test]
async fn resolves_a_prefixed_pattern_against_a_concrete_topic_name() {
    let bootstrap = acl_broker!();
    let client = RdKafkaClient::new();
    let admin = connection_as(bootstrap, "admin", "admin-secret");
    let concrete = format!("{PREFIX}orders");

    let listing = client
        .describe_acls(
            &admin,
            AclFilter::governing(ResourceType::Topic, &concrete),
            READ_TIMEOUT,
        )
        .await
        .expect("admin should be able to list ACLs");

    assert_eq!(listing.availability, AclAvailability::Available);
    let prefixed = listing
        .bindings
        .iter()
        .find(|binding| binding.pattern_type == PatternType::Prefixed)
        .unwrap_or_else(|| {
            panic!("a PREFIXED binding on {PREFIX} should govern {concrete}; got {:?}", listing.bindings)
        });

    assert_eq!(prefixed.resource_name, PREFIX);
    assert_eq!(prefixed.principal, "User:writer");
}

// A principal the fixture never granted Describe on Cluster. The broker
// really does refuse this — `kafka-acls.sh --list` as `reader` raises
// ClusterAuthorizationException — but librdkafka discards that, so the app
// cannot claim "not permitted". It must say so rather than report an empty
// policy, which would be the opposite of the truth on a cluster that denies
// by default.
//
// It must also stay a *successful* listing rather than an error: an error
// here would reach the credential circuit breaker and take an otherwise
// working cluster offline inside the app.
#[tokio::test]
async fn cannot_tell_a_refusal_from_an_empty_policy_and_says_so() {
    let bootstrap = acl_broker!();
    let client = RdKafkaClient::new();
    let reader = connection_as(bootstrap, "reader", "reader-secret");

    let listing = client
        .describe_acls(&reader, AclFilter::any(), READ_TIMEOUT)
        .await
        .expect("a refused ACL listing is a state, not an error");

    assert_eq!(listing.availability, AclAvailability::Indeterminate);
    assert!(listing.bindings.is_empty());
}

// The same broker, read by a principal that *can* see everything, still
// reports its authorizer — so `Indeterminate` above is a statement about
// this principal's visibility, not about the cluster being unreadable.
#[tokio::test]
async fn does_not_mistake_an_authorized_cluster_for_an_unsecured_one() {
    let bootstrap = acl_broker!();
    let client = RdKafkaClient::new();
    let admin = connection_as(bootstrap, "admin", "admin-secret");

    let listing = client
        .describe_acls(&admin, AclFilter::any(), READ_TIMEOUT)
        .await
        .expect("admin should be able to list ACLs");

    assert_ne!(listing.availability, AclAvailability::NoAuthorizer);
}

// The other half of the classification, and the one a naive implementation
// gets backwards. This broker has no authorizer, so every principal may do
// everything — an empty list here means "unrestricted", and reporting it as
// `Available` with no bindings would tell the user the exact opposite.
//
// librdkafka reports this listing as a plain empty success too, so the
// answer comes from the broker's own `authorizer.class.name` being empty.
#[tokio::test]
async fn reports_no_authorizer_on_a_broker_without_one() {
    let bootstrap = plain_broker!();
    let client = RdKafkaClient::new();
    let connection = blank_connection("e2e-acl-describe-plain", bootstrap);

    let listing = client
        .describe_acls(&connection, AclFilter::any(), READ_TIMEOUT)
        .await
        .expect("an unsecured broker is a state, not an error");

    assert_eq!(listing.availability, AclAvailability::NoAuthorizer);
    assert!(listing.bindings.is_empty());
}
