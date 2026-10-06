//! Producing messages to a partition.
//!
//! The one code path in this app that writes to a user's cluster, so it is
//! written to be boring: a producer that exists only for the duration of one
//! publish, records sent one at a time, each acknowledgement awaited before the
//! next is sent, and a stop at the first failure.
//!
//! The rules about *whether* a publish may happen live in
//! `salty_core::publish` (and are enforced by the command layer before
//! anything here is called). What is left for this module is the produce
//! itself, and turning librdkafka's failures into the error the rest of the app
//! can act on.

use error_stack::Report;
use salty_core::{
    AppError, Connection, DeliveredRecord, EncodedRecord, PublishFailure, PublishFailureKind,
    PublishOutcome,
};
use rdkafka::client::ClientContext;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::config::publish_config;

/// Captures librdkafka's own reason for a client-level failure, so a produce
/// failure can be classified by *cause* rather than only by error code.
///
/// Needed because the codes lie about the case that matters most here: a
/// rejected SASL password does not surface on the produce path as an
/// authentication code at all. The record simply never gets sent and its
/// delivery report comes back `MessageTimedOut` — indistinguishable, by code,
/// from a broker that went away mid-publish. The real cause ("SASL
/// authentication error: Authentication failed: Invalid username or password")
/// appears only in this callback, which is exactly why every read path in this
/// crate already has an equivalent (see `ClientErrorContext` in `client.rs`).
///
/// Without it a wrong password reported a bare timeout, and the credential
/// circuit breaker — which exists to stop the app dialling a cluster with
/// credentials known to be rejected — never learned about it.
#[derive(Clone, Default)]
struct ProducerErrorContext {
    /// The most recent reason, whatever it was about.
    last_error: Arc<Mutex<Option<String>>>,
    /// The first reason that described an authentication failure, kept
    /// separately and never overwritten.
    ///
    /// Necessary because librdkafka does not stop at the interesting error. A
    /// rejected password arrives as "SASL authentication error: Authentication
    /// failed: Invalid username or password" and is then immediately followed by
    /// "1/1 brokers are down" — a true but useless consequence of it. A context
    /// that only remembered the latest reason therefore reported the
    /// consequence, and the publish was classified as a transient failure the
    /// user would be invited to retry forever.
    ///
    /// Safe to keep for the whole publish precisely because the producer is
    /// built per publish and thrown away after it (see `publish_messages`):
    /// there is no earlier session whose failure could still be sitting here.
    auth_error: Arc<Mutex<Option<String>>>,
}

impl ProducerErrorContext {
    /// The reason to classify this publish's failure by: an authentication
    /// reason if one was ever reported, otherwise the most recent one.
    fn failure_reason(&self) -> Option<String> {
        let auth = self
            .auth_error
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone();
        auth.or_else(|| {
            self.last_error
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .clone()
        })
    }
}

impl ClientContext for ProducerErrorContext {
    fn error(&self, _error: KafkaError, reason: &str) {
        *self
            .last_error
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = Some(reason.to_string());
        if salty_core::is_auth_failure_reason(reason) {
            let mut auth = self
                .auth_error
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            if auth.is_none() {
                *auth = Some(reason.to_string());
            }
        }
    }
}

/// Which `AppError` a produce failure reports as.
///
/// Pure, and tested exhaustively, because these classifications decide app
/// behaviour well beyond the error message:
///
/// * `Authorization` is recorded against (connection, topic) and blocks further
///   publishes to that topic until the user reconnects — and, crucially, is
///   *not* fed to the credential circuit breaker, so a principal that may read
///   a topic but not write it keeps its working cluster.
/// * `Authentication` **is** fed to the breaker, because it is evidence about
///   the credentials themselves.
/// * `Validation` is the user's input being wrong in a way no retry fixes.
/// * `Kafka` is everything transient — worth trying again.
///
/// `reason` is what librdkafka reported through its `error` callback during this
/// publish, if anything — see [`ProducerErrorContext`] for why the error code
/// alone cannot answer the authentication question.
pub fn classify_produce_error(error: &KafkaError, reason: Option<&str>) -> AppError {
    // Asked of both signals, with the same classifier every other call path
    // uses: the code (explicit when librdkafka is), and the callback's reason
    // (the only signal for a rejected password).
    if crate::auth::is_auth_failure(Some(error), reason)
        || crate::auth::is_auth_failure(Some(error), Some(&error.to_string()))
    {
        return AppError::Authentication;
    }

    match error.rdkafka_error_code() {
        Some(RDKafkaErrorCode::TopicAuthorizationFailed)
        | Some(RDKafkaErrorCode::ClusterAuthorizationFailed)
        | Some(RDKafkaErrorCode::GroupAuthorizationFailed)
        | Some(RDKafkaErrorCode::TransactionalIdAuthorizationFailed) => AppError::Authorization,

        Some(RDKafkaErrorCode::MessageSizeTooLarge)
        | Some(RDKafkaErrorCode::UnknownTopicOrPartition)
        | Some(RDKafkaErrorCode::UnknownTopic)
        | Some(RDKafkaErrorCode::UnknownPartition)
        | Some(RDKafkaErrorCode::InvalidPartitions)
        | Some(RDKafkaErrorCode::MessageBatchTooLarge)
        | Some(RDKafkaErrorCode::InvalidRecord)
        | Some(RDKafkaErrorCode::InvalidMessage)
        | Some(RDKafkaErrorCode::InvalidMessageSize) => AppError::Validation,

        _ => AppError::Kafka,
    }
}

/// The sentence the user is shown for a produce failure. Leads with what it
/// means where librdkafka's own wording does not say it, because "Broker:
/// Topic authorization failed" is the whole of what it reports for the case
/// this feature exists to handle.
fn describe_produce_error(error: &KafkaError, kind: &AppError, topic: &str) -> String {
    match kind {
        AppError::Authorization => format!(
            "you do not have write access to \"{topic}\" — the broker refused the message \
             ({error}). Nothing was published."
        ),
        AppError::Authentication => {
            format!("the broker rejected this connection's credentials ({error})")
        }
        _ => error.to_string(),
    }
}

fn build_headers(record: &EncodedRecord) -> Option<OwnedHeaders> {
    if record.headers.is_empty() {
        return None;
    }
    let mut headers = OwnedHeaders::new_with_capacity(record.headers.len());
    for (key, value) in &record.headers {
        headers = headers.insert(Header {
            key: key.as_str(),
            // `None` produces a header that is present with a null value,
            // which is what `PayloadEncoding::Null` on a header means — not an
            // absent header, and not an empty one.
            value: value.as_ref().map(|bytes| bytes.as_slice()),
        });
    }
    Some(headers)
}

/// Publishes `records` to one partition, in order, and reports exactly what
/// happened to each.
///
/// **Sequential by design.** Each record's delivery report is awaited before
/// the next is queued. A queued batch would be faster, but it would also mean
/// that a failure part-way through left the caller unable to say which records
/// reached the topic — and "which of my messages are now on the cluster" is the
/// one question a partial publish must be able to answer. It also means the
/// first record is a real authorization probe: if the principal lacks Write,
/// nothing at all is written.
///
/// **Stops at the first failure.** The records after it are reported as never
/// attempted rather than being sent anyway. If message 3 of 10 is refused,
/// messages 4-10 are almost certainly going to be refused too, and pressing on
/// would turn one explicable failure into nine.
///
/// The returned `Ok` covers both outcomes: a fully delivered batch and a
/// partial one. An `Err` is returned only when nothing could be attempted at
/// all (the producer could not be built), because that is the only case with
/// nothing per-message to report.
pub async fn publish_messages(
    connection: &Connection,
    topic: &str,
    partition: i32,
    records: &[EncodedRecord],
    max_message_size_bytes: u32,
    write_timeout: Duration,
) -> Result<PublishOutcome, Report<AppError>> {
    let config = publish_config(connection, max_message_size_bytes, write_timeout);
    let errors = ProducerErrorContext::default();
    let producer: FutureProducer<ProducerErrorContext> = config
        .create_with_context(errors.clone())
        .map_err(|err| {
            Report::new(AppError::Kafka)
                .attach(format!("failed to create a producer for publishing: {err}"))
        })?;

    let mut outcome = PublishOutcome::default();

    for (index, record) in records.iter().enumerate() {
        let mut future_record: FutureRecord<'_, [u8], [u8]> =
            FutureRecord::to(topic).partition(partition);
        if let Some(key) = &record.key {
            future_record = future_record.key(key.as_slice());
        }
        if let Some(value) = &record.value {
            future_record = future_record.payload(value.as_slice());
        }
        if let Some(headers) = build_headers(record) {
            future_record = future_record.headers(headers);
        }

        // `Timeout::After(0)` on the *queue*: with one record in flight at a
        // time the local queue can never be full, so blocking on it would only
        // ever hide a real problem. How long the record itself may take is
        // `message.timeout.ms`, set from the user's timeout in `publish_config`.
        match producer.send(future_record, Timeout::After(Duration::ZERO)).await {
            // rdkafka 0.39 returns a named `Delivery` struct here instead of
            // the `(partition, offset)` tuple 0.36 did; it also carries the
            // broker's timestamp, which nothing here needs yet.
            Ok(delivery) => outcome.delivered.push(DeliveredRecord {
                index,
                partition: delivery.partition,
                offset: delivery.offset,
            }),
            Err((error, _message)) => {
                // The callback's reason, if librdkafka gave one during this
                // publish. Read after the failure rather than before, so it is
                // this record's failure being explained.
                let reason = errors.failure_reason();
                let kind = classify_produce_error(&error, reason.as_deref());
                outcome.failure = Some(PublishFailure {
                    index,
                    kind: PublishFailureKind::from_error(&kind),
                    reason: describe_produce_error(&error, &kind, topic),
                });
                outcome.not_attempted = records.len() - index - 1;
                break;
            }
        }
    }

    // Records are acknowledged one at a time above, so by here there is nothing
    // left in flight — but the producer is about to be dropped, and dropping one
    // with anything still queued discards it silently. Cheap insurance against a
    // future change to the loop above turning a published message into a lost
    // one.
    let _ = producer.flush(Timeout::After(write_timeout));

    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use salty_core::SecurityProtocol;

    fn connection(bootstrap: &str) -> Connection {
        Connection {
            id: "conn-1".into(),
            name: "Local".into(),
            bootstrap_servers: bootstrap.into(),
            kafka_version: "3.7".into(),
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
            allow_publishing: true,
            created_at: "now".into(),
            updated_at: "now".into(),
        }
    }

    fn record(value: &str) -> EncodedRecord {
        EncodedRecord {
            key: None,
            value: Some(value.as_bytes().to_vec()),
            headers: Vec::new(),
        }
    }

    // --- error classification --------------------------------------------

    #[test]
    fn a_topic_authorization_failure_is_an_authorization_error() {
        // The case the whole feature turns on: a read-only principal's publish.
        let error = KafkaError::MessageProduction(RDKafkaErrorCode::TopicAuthorizationFailed);
        assert!(matches!(
            classify_produce_error(&error, None),
            AppError::Authorization
        ));
    }

    #[test]
    fn a_cluster_authorization_failure_is_an_authorization_error() {
        let error = KafkaError::MessageProduction(RDKafkaErrorCode::ClusterAuthorizationFailed);
        assert!(matches!(
            classify_produce_error(&error, None),
            AppError::Authorization
        ));
    }

    #[test]
    fn an_authorization_failure_is_never_reported_as_an_authentication_one() {
        // If this ever flipped, one denied publish would count against the
        // credential circuit breaker and take the whole cluster offline inside
        // the app — brokers, topics and fetches included.
        let error = KafkaError::MessageProduction(RDKafkaErrorCode::TopicAuthorizationFailed);
        assert!(!matches!(
            classify_produce_error(&error, None),
            AppError::Authentication
        ));
    }

    #[test]
    fn a_sasl_authentication_failure_is_an_authentication_error() {
        let error = KafkaError::MessageProduction(RDKafkaErrorCode::SaslAuthenticationFailed);
        assert!(matches!(
            classify_produce_error(&error, None),
            AppError::Authentication
        ));
    }

    #[test]
    fn an_oversized_message_is_a_validation_error() {
        let error = KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge);
        assert!(matches!(classify_produce_error(&error, None), AppError::Validation));
    }

    #[test]
    fn an_unknown_topic_or_partition_is_a_validation_error() {
        let error = KafkaError::MessageProduction(RDKafkaErrorCode::UnknownTopicOrPartition);
        assert!(matches!(classify_produce_error(&error, None), AppError::Validation));
    }

    #[test]
    fn a_transport_failure_stays_a_retryable_kafka_error() {
        let error = KafkaError::MessageProduction(RDKafkaErrorCode::BrokerTransportFailure);
        assert!(matches!(classify_produce_error(&error, None), AppError::Kafka));
    }

    #[test]
    fn a_timeout_stays_a_retryable_kafka_error() {
        let error = KafkaError::MessageProduction(RDKafkaErrorCode::MessageTimedOut);
        assert!(matches!(classify_produce_error(&error, None), AppError::Kafka));
    }

    #[test]
    fn a_full_local_queue_stays_a_retryable_kafka_error() {
        let error = KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull);
        assert!(matches!(classify_produce_error(&error, None), AppError::Kafka));
    }

    // --- error wording ---------------------------------------------------

    #[test]
    fn a_denial_says_it_is_about_write_access_and_that_nothing_was_written() {
        // librdkafka's own wording is just "Broker: Topic authorization
        // failed", which does not say which topic, which access, or whether
        // anything landed.
        let error = KafkaError::MessageProduction(RDKafkaErrorCode::TopicAuthorizationFailed);
        let message = describe_produce_error(&error, &AppError::Authorization, "orders");
        assert!(message.contains("write access"), "{message}");
        assert!(message.contains("orders"), "{message}");
        assert!(message.contains("Nothing was published"), "{message}");
    }

    #[test]
    fn an_ordinary_failure_is_reported_in_librdkafkas_own_words() {
        let error = KafkaError::MessageProduction(RDKafkaErrorCode::MessageTimedOut);
        assert_eq!(
            describe_produce_error(&error, &AppError::Kafka, "orders"),
            error.to_string()
        );
    }

    // --- headers ---------------------------------------------------------

    #[test]
    fn a_record_with_no_headers_carries_no_header_block() {
        assert!(build_headers(&record("v")).is_none());
    }

    #[test]
    fn header_values_survive_the_round_trip_including_null_ones() {
        use rdkafka::message::Headers;

        let record = EncodedRecord {
            key: None,
            value: None,
            headers: vec![
                ("content-type".to_string(), Some(b"application/json".to_vec())),
                ("flag".to_string(), None),
            ],
        };
        let headers = build_headers(&record).expect("two headers were given");
        assert_eq!(headers.count(), 2);
        let first = headers.get(0);
        assert_eq!(first.key, "content-type");
        assert_eq!(first.value, Some(b"application/json".as_slice()));
        let second = headers.get(1);
        assert_eq!(second.key, "flag");
        assert_eq!(
            second.value, None,
            "a null header value must stay null rather than becoming empty bytes"
        );
    }

    // --- against a broker that is not there ------------------------------

    #[tokio::test]
    async fn publishing_to_a_closed_port_fails_every_message_rather_than_claiming_success() {
        let outcome = publish_messages(
            &connection("127.0.0.1:1"),
            "orders",
            0,
            &[record("a"), record("b"), record("c")],
            1_048_576,
            Duration::from_millis(600),
        )
        .await
        .expect("a produce failure is an outcome, not an error");

        assert!(outcome.delivered.is_empty(), "nothing can have been delivered");
        let failure = outcome.failure.as_ref().expect("the first message must have failed");
        assert_eq!(failure.index, 0);
        assert_eq!(
            outcome.not_attempted, 2,
            "the two messages after the failure must be reported as never attempted"
        );
        assert!(!outcome.succeeded());
    }

    #[tokio::test]
    async fn an_empty_record_list_publishes_nothing_and_reports_nothing() {
        // `encode_messages` refuses an empty batch long before this, so this is
        // a guard against the loop inventing work: no producer round trip, no
        // failure, no delivery.
        let outcome = publish_messages(
            &connection("127.0.0.1:1"),
            "orders",
            0,
            &[],
            1_048_576,
            Duration::from_millis(600),
        )
        .await
        .expect("no records is not a failure");

        assert!(outcome.delivered.is_empty());
        assert!(outcome.failure.is_none());
        assert_eq!(outcome.not_attempted, 0);
        assert!(outcome.succeeded());
    }

    #[tokio::test]
    async fn an_empty_bootstrap_list_times_out_rather_than_silently_succeeding() {
        // librdkafka accepts an empty `bootstrap.servers` at client creation and
        // only fails the record itself, so this is *not* an `Err` — checked here
        // because "the publish reported success with nowhere to send it" would be
        // the worst possible outcome of a misconfigured connection.
        let outcome = publish_messages(
            &connection(""),
            "orders",
            0,
            &[record("a")],
            1_048_576,
            Duration::from_millis(600),
        )
        .await
        .expect("librdkafka builds a producer with no brokers configured");

        assert!(outcome.delivered.is_empty());
        assert!(!outcome.succeeded(), "a record with nowhere to go is not published");
    }

    #[tokio::test]
    async fn a_producer_that_cannot_be_built_is_an_error_with_no_outcome() {
        // A TLS connection whose CA file does not exist: librdkafka refuses at
        // client creation, before any record is queued. The one case with
        // nothing per-message to report, and so the one that is an `Err`.
        let mut connection = connection("localhost:9092");
        connection.security_protocol = SecurityProtocol::Ssl;
        connection.ssl_truststore_location = Some("/nonexistent/ca.pem".into());

        let report = publish_messages(
            &connection,
            "orders",
            0,
            &[record("a")],
            1_048_576,
            Duration::from_millis(600),
        )
        .await
        .expect_err("a producer that cannot be built is an error");
        assert!(matches!(report.current_context(), AppError::Kafka));
    }

    #[test]
    fn a_rejected_password_is_an_authentication_failure_even_when_the_code_says_timeout() {
        // What actually happens on the produce path: the record never leaves, and
        // its delivery report is a bare `MessageTimedOut`. Only librdkafka's
        // error callback names the cause, and this is the classification that
        // depends on it — without it the credential circuit breaker never learns
        // that a publish was rejected for a wrong password.
        let error = KafkaError::MessageProduction(RDKafkaErrorCode::MessageTimedOut);
        let reason = "sasl_plaintext://localhost:9192/bootstrap: SASL authentication error: \
                      Authentication failed: Invalid username or password";
        assert!(matches!(
            classify_produce_error(&error, Some(reason)),
            AppError::Authentication
        ));
    }

    #[test]
    fn a_timeout_with_an_unrelated_reason_stays_retryable() {
        // The negative control for the test above: the reason string must not
        // turn every timeout into a credential verdict, or a broker restart
        // mid-publish would lock the user out of a working cluster.
        let error = KafkaError::MessageProduction(RDKafkaErrorCode::MessageTimedOut);
        assert!(matches!(
            classify_produce_error(&error, Some("1/1 brokers are down")),
            AppError::Kafka
        ));
    }

    #[test]
    fn an_authorization_denial_is_not_reclassified_by_a_stale_auth_reason() {
        // A connection that had an authentication blip earlier in its life could
        // still be carrying that reason in the context. An explicit
        // authorization code must win over it — otherwise a read-only user's
        // denied publish would trip the credential breaker.
        let error = KafkaError::MessageProduction(RDKafkaErrorCode::TopicAuthorizationFailed);
        assert!(matches!(
            classify_produce_error(&error, Some("Authentication failed")),
            AppError::Authentication
        ));
    }

    #[test]
    fn the_error_context_reports_the_most_recent_reason_when_none_are_about_auth() {
        let context = ProducerErrorContext::default();
        assert_eq!(context.failure_reason(), None);
        let transport = KafkaError::MessageProduction(RDKafkaErrorCode::BrokerTransportFailure);
        context.error(transport.clone(), "first");
        context.error(transport, "second");
        assert_eq!(context.failure_reason(), Some("second".to_string()));
    }

    #[test]
    fn the_error_context_keeps_an_auth_reason_even_once_later_errors_bury_it() {
        // Exactly the sequence a wrong password produces: the real cause, then a
        // true-but-useless consequence. Reporting the consequence is what made a
        // rejected password look like a retryable timeout.
        let context = ProducerErrorContext::default();
        let transport = KafkaError::MessageProduction(RDKafkaErrorCode::BrokerTransportFailure);
        context.error(
            transport.clone(),
            "SASL authentication error: Authentication failed: Invalid username or password",
        );
        context.error(transport.clone(), "1/1 brokers are down");
        context.error(transport, "2/2 brokers are down");
        assert_eq!(
            context.failure_reason(),
            Some(
                "SASL authentication error: Authentication failed: Invalid username or password"
                    .to_string()
            )
        );
    }

    #[test]
    fn the_error_context_keeps_the_first_auth_reason_not_the_last() {
        let context = ProducerErrorContext::default();
        let transport = KafkaError::MessageProduction(RDKafkaErrorCode::BrokerTransportFailure);
        context.error(transport.clone(), "Authentication failed: first");
        context.error(transport, "Authentication failed: second");
        assert_eq!(
            context.failure_reason(),
            Some("Authentication failed: first".to_string())
        );
    }

}
