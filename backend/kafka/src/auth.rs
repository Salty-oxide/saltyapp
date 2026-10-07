use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use salty_core::is_auth_failure_reason;

/// Whether a librdkafka failure means "these credentials were rejected"
/// rather than "the broker was momentarily unreachable".
///
/// Two independent signals, either of which is enough:
///
/// * the error *code*, when librdkafka is explicit about it
///   (`_AUTHENTICATION` locally, `SASL_AUTHENTICATION_FAILED` from the
///   broker); and
/// * the *reason* string captured from librdkafka's `error` callback, which
///   is the only signal available in the common case — a wrong password
///   surfaces as a plain `BrokerTransportFailure`, indistinguishable by code
///   from a closed port. See [`is_auth_failure_reason`] for what counts.
///
/// The caller feeds the result to the connection's circuit breaker, so a
/// false positive costs the user a locked-out cluster: when in doubt, this
/// says `false`.
pub fn is_auth_failure(error: Option<&KafkaError>, reason: Option<&str>) -> bool {
    let code_says_auth = matches!(
        error.and_then(KafkaError::rdkafka_error_code),
        Some(RDKafkaErrorCode::Authentication) | Some(RDKafkaErrorCode::SaslAuthenticationFailed)
    );
    code_says_auth || reason.is_some_and(is_auth_failure_reason)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};

    #[test]
    fn classifies_librdkafkas_local_authentication_code_as_an_auth_failure() {
        let err = KafkaError::MetadataFetch(RDKafkaErrorCode::Authentication);
        assert!(is_auth_failure(Some(&err), None));
    }

    #[test]
    fn classifies_the_brokers_sasl_authentication_failed_code_as_an_auth_failure() {
        let err = KafkaError::MetadataFetch(RDKafkaErrorCode::SaslAuthenticationFailed);
        assert!(is_auth_failure(Some(&err), None));
    }

    #[test]
    fn classifies_a_transport_failure_carrying_a_sasl_reason_as_an_auth_failure() {
        // The case that matters in practice: librdkafka reports a bad
        // password as a generic transport failure, and only the `error`
        // callback's reason names the real cause.
        let err = KafkaError::MetadataFetch(RDKafkaErrorCode::BrokerTransportFailure);
        let reason =
            "SASL authentication error: Authentication failed: Invalid username or password";
        assert!(is_auth_failure(Some(&err), Some(reason)));
    }

    #[test]
    fn classifies_a_failed_ssl_handshake_as_an_auth_failure() {
        let reason = "SSL handshake failed: certificate verify failed: unable to get local issuer certificate";
        assert!(is_auth_failure(None, Some(reason)));
    }

    #[test]
    fn does_not_classify_a_plain_transport_failure_as_an_auth_failure() {
        // A broker restart or a network blip must stay retryable — treating
        // it as an auth failure would trip the breaker and lock the user out
        // of a cluster whose credentials are perfectly good.
        let err = KafkaError::MetadataFetch(RDKafkaErrorCode::BrokerTransportFailure);
        assert!(!is_auth_failure(Some(&err), Some("Connection refused")));
    }

    #[test]
    fn does_not_classify_a_timeout_as_an_auth_failure() {
        let err = KafkaError::MetadataFetch(RDKafkaErrorCode::OperationTimedOut);
        assert!(!is_auth_failure(Some(&err), Some("Local: Timed out")));
    }

    #[test]
    fn does_not_classify_all_brokers_down_as_an_auth_failure() {
        let err = KafkaError::MetadataFetch(RDKafkaErrorCode::AllBrokersDown);
        assert!(!is_auth_failure(Some(&err), None));
    }

    #[test]
    fn does_not_classify_an_authorization_failure_as_an_auth_failure() {
        // Authorization is per-resource, not per-connection: the credentials
        // are valid, this user just can't read this topic. Tripping the
        // connection-wide breaker on it would be wrong.
        let err = KafkaError::MetadataFetch(RDKafkaErrorCode::TopicAuthorizationFailed);
        assert!(!is_auth_failure(Some(&err), None));
    }

    #[test]
    fn matches_a_reason_regardless_of_case() {
        assert!(is_auth_failure(None, Some("sasl authentication error")));
    }

    #[test]
    fn is_not_an_auth_failure_when_there_is_neither_a_code_nor_a_reason() {
        assert!(!is_auth_failure(None, None));
    }
}
