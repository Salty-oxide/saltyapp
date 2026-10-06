//! The read-only guarantee, against a broker that actually enforces ACLs.
//!
//! Every other test of this feature can only show that the app's *own* gates
//! refuse a publish. This file shows the thing those gates are not allowed to
//! be trusted for: that a principal with no `Write` grant cannot put a message
//! on a topic even when every app-side gate has been cleared, because the
//! broker refuses it — and that a principal *with* the grant still can, so the
//! protection has not been bought by blocking everyone.
//!
//! It needs a cluster with an authorizer and `allow.everyone.if.no.acl.found`
//! off. The ordinary e2e broker has neither, and on it `reader` would publish
//! perfectly happily — the test would pass for the wrong reason, which is why
//! this file is gated on its own variable rather than `SALTY_E2E_BOOTSTRAP`.
//!
//! ```bash
//! ./scripts/e2e-acl-fixtures.sh
//! SALTY_E2E_ACL_BOOTSTRAP=localhost:9192 \
//!   cargo test -p salty-kafka --test publish_authorization
//! ```

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use salty_core::{
    encode_messages, AppError, Connection, ConnectionRegistry, MessageFilter, NewPublishMessage,
    PublishField, PublishFailureKind, PublishLimits, PublishOutcome, PublishRefusal, SaslMechanism,
    SecurityProtocol,
};
use salty_kafka::{KafkaClient, RdKafkaClient};

/// Created by `scripts/e2e-acl-fixtures.sh`, with Write granted to `writer` and
/// deliberately not to `reader`.
const TOPIC: &str = "e2e-acl-publish";
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const WRITE_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_MESSAGE_SIZE: u32 = 1024 * 1024;

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
                eprintln!(
                    "skipped: run ./scripts/e2e-acl-fixtures.sh and set \
                     SALTY_E2E_ACL_BOOTSTRAP to run this test"
                );
                return;
            }
        }
    };
}

/// A connection authenticating as one of the fixture's principals.
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

fn reader(bootstrap: String) -> Connection {
    connection_as(bootstrap, "reader", "reader-secret")
}

fn writer(bootstrap: String) -> Connection {
    connection_as(bootstrap, "writer", "writer-secret")
}

fn marker(what: &str) -> String {
    format!(
        "{what}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

async fn try_publish(
    client: &RdKafkaClient,
    connection: &Connection,
    partition: i32,
    value: &str,
) -> PublishOutcome {
    let messages = vec![NewPublishMessage {
        key: PublishField::text(value),
        value: PublishField::text(value),
        headers: Vec::new(),
    }];
    let records = encode_messages(
        &messages,
        &PublishLimits::for_max_message_size(MAX_MESSAGE_SIZE),
    )
    .expect("the fixture message is valid");
    client
        .publish_messages(
            connection,
            TOPIC,
            partition,
            &records,
            MAX_MESSAGE_SIZE,
            WRITE_TIMEOUT,
        )
        .await
        .expect("a refused publish is an outcome, not an error")
}

/// How many messages the topic holds, read as the principal that *may* read it.
/// This is the independent check: the outcome reported by a publish is one
/// claim, and the topic's own contents are another.
async fn message_count(client: &RdKafkaClient, connection: &Connection, partition: i32) -> usize {
    let filter = MessageFilter {
        partitions: Some(vec![partition]),
        include_payload: true,
        ..MessageFilter::default()
    };
    client
        .fetch_messages(
            connection,
            TOPIC,
            &filter,
            None,
            READ_TIMEOUT,
            MAX_MESSAGE_SIZE,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("the reader principal may read this topic")
        .messages
        .len()
}

#[tokio::test]
async fn a_read_only_principal_cannot_publish_and_the_topic_is_unchanged() {
    let bootstrap = acl_broker!();
    let client = RdKafkaClient::new();
    let reader = reader(bootstrap);
    let value = marker("denied");

    let before = message_count(&client, &reader, 0).await;

    let outcome = try_publish(&client, &reader, 0, &value).await;

    assert!(
        outcome.delivered.is_empty(),
        "a principal without Write must deliver nothing: {outcome:?}"
    );
    let failure = outcome
        .failure
        .as_ref()
        .expect("the publish must have failed");
    assert_eq!(
        failure.kind,
        PublishFailureKind::Authorization,
        "the refusal must be classified as authorization, not as a transient error \
         that the UI would invite the user to retry: {failure:?}"
    );
    assert!(
        failure.reason.contains("write access"),
        "the reason must say what was missing: {}",
        failure.reason
    );
    assert!(
        failure.reason.contains(TOPIC),
        "the reason must name the topic: {}",
        failure.reason
    );

    // The load-bearing assertion of this whole feature: the topic did not
    // change. Everything else is a report about what happened; this is what
    // happened.
    let after = message_count(&client, &reader, 0).await;
    assert_eq!(
        after, before,
        "a refused publish must leave the topic exactly as it was"
    );
}

#[tokio::test]
async fn a_read_only_principal_is_refused_every_message_of_a_batch() {
    let bootstrap = acl_broker!();
    let client = RdKafkaClient::new();
    let reader = reader(bootstrap);
    let run = marker("denied-batch");

    let before = message_count(&client, &reader, 1).await;

    let messages: Vec<NewPublishMessage> = (1..=5)
        .map(|n| NewPublishMessage {
            key: PublishField::text(format!("{run}-{n}")),
            value: PublishField::text(format!("{run}-{n}")),
            headers: Vec::new(),
        })
        .collect();
    let records = encode_messages(
        &messages,
        &PublishLimits::for_max_message_size(MAX_MESSAGE_SIZE),
    )
    .unwrap();
    let outcome = client
        .publish_messages(
            &reader,
            TOPIC,
            1,
            &records,
            MAX_MESSAGE_SIZE,
            WRITE_TIMEOUT,
        )
        .await
        .expect("a refused publish is an outcome, not an error");

    // Sending one record at a time means the very first one is the
    // authorization probe: it is refused, the remaining four are never
    // attempted, and nothing partial can have landed.
    assert!(outcome.delivered.is_empty());
    assert_eq!(outcome.failure.as_ref().unwrap().index, 0);
    assert_eq!(outcome.not_attempted, 4);
    assert_eq!(
        message_count(&client, &reader, 1).await,
        before,
        "not one message of a refused batch may reach the topic"
    );
}

#[tokio::test]
async fn a_principal_with_write_access_publishes_normally() {
    // The other half of the requirement: the protection must not be achieved by
    // blocking everyone. Same topic, same code path, a principal that holds
    // Write — and it works.
    let bootstrap = acl_broker!();
    let client = RdKafkaClient::new();
    let writer = writer(bootstrap.clone());
    let value = marker("allowed");

    let outcome = try_publish(&client, &writer, 2, &value).await;

    assert!(
        outcome.succeeded(),
        "a principal with Write must be able to publish: {outcome:?}"
    );
    assert_eq!(outcome.delivered.len(), 1);
    assert_eq!(outcome.delivered[0].partition, 2);

    // And it is really there, read back as the reader principal.
    let filter = MessageFilter {
        partitions: Some(vec![2]),
        include_payload: true,
        ..MessageFilter::default()
    };
    let messages = client
        .fetch_messages(
            &reader(bootstrap),
            TOPIC,
            &filter,
            None,
            READ_TIMEOUT,
            MAX_MESSAGE_SIZE,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("reading back should succeed")
        .messages;
    let landed = messages.iter().any(|message| {
        message
            .key_base64
            .as_deref()
            .map(|key| {
                use base64::engine::general_purpose::STANDARD as BASE64;
                use base64::Engine;
                BASE64.decode(key).unwrap() == value.as_bytes()
            })
            .unwrap_or(false)
    });
    assert!(landed, "the published message should be on the topic");
}

#[tokio::test]
async fn a_denial_blocks_the_next_attempt_without_asking_the_broker_again() {
    // What the command layer does with the verdict: record it, then answer the
    // next publish from memory. Exercised here with the real denial rather than
    // a hand-written reason, so the two halves — what the broker says, and what
    // the app remembers — are shown to fit together.
    let bootstrap = acl_broker!();
    let client = RdKafkaClient::new();
    let reader = reader(bootstrap);
    let registry = ConnectionRegistry::default();
    registry.mark_connected(&reader.id);

    // Nothing is blocked before the first attempt: the app cannot know.
    assert_eq!(
        salty_core::publish_refusal(true, true, registry.write_denied_reason(&reader.id, TOPIC).as_deref()),
        None
    );

    let outcome = try_publish(&client, &reader, 0, &marker("cached")).await;
    let failure = outcome.failure.as_ref().expect("the publish must fail");
    assert_eq!(failure.kind, PublishFailureKind::Authorization);
    registry.record_write_denied(&reader.id, TOPIC, &failure.reason);

    // Now it is, and the refusal reports as an authorization failure rather than
    // as the app's own state.
    let refusal = salty_core::publish_refusal(
        true,
        true,
        registry.write_denied_reason(&reader.id, TOPIC).as_deref(),
    )
    .expect("the topic must now be blocked");
    assert!(matches!(refusal, PublishRefusal::BrokerDenied(_)));
    assert!(matches!(refusal.error(), AppError::Authorization));
    assert!(refusal.message(TOPIC).contains("reconnect"));

    // Another topic is unaffected — the denial is about one topic, not the
    // connection.
    assert_eq!(
        salty_core::publish_refusal(
            true,
            true,
            registry.write_denied_reason(&reader.id, "some-other-topic").as_deref()
        ),
        None
    );

    // And a reconnect clears it, so a granted ACL can take effect without a
    // restart.
    registry.clear_auth_failures(&reader.id);
    assert_eq!(
        salty_core::publish_refusal(
            true,
            true,
            registry.write_denied_reason(&reader.id, TOPIC).as_deref()
        ),
        None
    );
}

#[tokio::test]
async fn being_refused_a_publish_does_not_break_reading_the_cluster() {
    // The reason `AppError::Authorization` exists apart from `Authentication`.
    // A denied publish must leave the connection entirely usable: if it counted
    // against the credential circuit breaker, a read-only user's one attempt to
    // publish would black out their brokers, topics and fetches.
    let bootstrap = acl_broker!();
    let client = RdKafkaClient::new();
    let reader = reader(bootstrap);

    let outcome = try_publish(&client, &reader, 0, &marker("still-readable")).await;
    assert_eq!(
        outcome.failure.as_ref().unwrap().kind,
        PublishFailureKind::Authorization,
        "must not be reported as an authentication failure"
    );

    // Reads still work afterwards, on the same connection.
    let topics = client
        .list_topics(&reader, READ_TIMEOUT)
        .await
        .expect("listing topics must still work after a refused publish");
    assert!(topics.iter().any(|topic| topic.name == TOPIC));
    assert!(message_count(&client, &reader, 0).await < usize::MAX);
}

#[tokio::test]
async fn wrong_credentials_are_an_authentication_failure_not_an_authorization_one() {
    // The other side of the same distinction, and the one that must still reach
    // the credential breaker: a bad password is about the connection, not about
    // one topic's ACLs.
    let bootstrap = acl_broker!();
    let client = RdKafkaClient::new();
    let imposter = connection_as(bootstrap, "reader", "not-the-password");

    let outcome = try_publish(&client, &imposter, 0, &marker("bad-password")).await;
    let failure = outcome.failure.as_ref().expect("the publish must fail");
    assert_eq!(
        failure.kind,
        PublishFailureKind::Authentication,
        "a rejected password must not be mistaken for a missing ACL: {failure:?}"
    );
    assert!(outcome.delivered.is_empty());
}
