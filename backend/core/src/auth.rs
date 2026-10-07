/// Reason fragments librdkafka uses when a connection is rejected for
/// *authentication* — a wrong SASL password, a mechanism the broker doesn't
/// offer, a TLS handshake the peer refuses.
///
/// Matched as lowercase substrings of librdkafka's `error` callback reason,
/// because the error *code* alone can't tell these apart: a bad password, a
/// closed port and a failed TLS handshake all surface as the same generic
/// `BrokerTransportFailure`. The reason string is the only place the real
/// cause appears.
///
/// Deliberately narrow. Everything absent from this list — refused
/// connections, timeouts, transport failures, all-brokers-down — stays
/// retryable, because misclassifying a broker restart as bad credentials
/// would lock a user out of a cluster they can perfectly well reach.
/// Authorization failures are also excluded: those are per-resource ("this
/// principal can't read this topic"), not a statement about the connection's
/// credentials.
const AUTH_FAILURE_REASONS: &[&str] = &[
    "authentication failed",
    "authentication error",
    "sasl authentication",
    "invalid username or password",
    "unsupported sasl mechanism",
    "ssl handshake failed",
    "certificate verify failed",
];

/// Whether librdkafka's failure reason describes an authentication problem,
/// i.e. one that retrying with these same connection settings cannot fix.
/// See [`AUTH_FAILURE_REASONS`] for what counts and why the list is short.
pub fn is_auth_failure_reason(reason: &str) -> bool {
    let reason = reason.to_lowercase();
    AUTH_FAILURE_REASONS
        .iter()
        .any(|fragment| reason.contains(fragment))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognises_a_sasl_authentication_error() {
        assert!(is_auth_failure_reason(
            "SASL authentication error: Authentication failed: Invalid username or password"
        ));
    }

    #[test]
    fn recognises_a_bare_authentication_failure() {
        assert!(is_auth_failure_reason("Authentication failed"));
    }

    #[test]
    fn recognises_an_unsupported_sasl_mechanism() {
        assert!(is_auth_failure_reason(
            "Unsupported SASL mechanism: broker's supported mechanisms: SCRAM-SHA-512"
        ));
    }

    #[test]
    fn recognises_a_failed_ssl_handshake() {
        assert!(is_auth_failure_reason(
            "SSL handshake failed: error:0A000086:SSL routines::certificate verify failed"
        ));
    }

    #[test]
    fn recognises_a_certificate_verification_failure() {
        assert!(is_auth_failure_reason("certificate verify failed"));
    }

    #[test]
    fn matches_regardless_of_case() {
        assert!(is_auth_failure_reason("sasl authentication error"));
    }

    #[test]
    fn does_not_recognise_a_refused_connection() {
        // A broker restart or a network blip has to stay retryable —
        // classifying it here would trip the breaker and lock a user out of
        // a cluster whose credentials are perfectly good.
        assert!(!is_auth_failure_reason(
            "Connect to ipv4#10.0.0.1:9092 failed: Connection refused"
        ));
    }

    #[test]
    fn does_not_recognise_a_timeout() {
        assert!(!is_auth_failure_reason("Local: Timed out"));
    }

    #[test]
    fn does_not_recognise_a_transport_failure() {
        assert!(!is_auth_failure_reason("Local: Broker transport failure"));
    }

    #[test]
    fn does_not_recognise_an_authorization_failure() {
        // Authorization is per-resource, not per-connection: the credentials
        // are valid, this principal just can't read this topic. Tripping the
        // connection-wide breaker on it would be wrong.
        assert!(!is_auth_failure_reason(
            "Topic authorization failed for topic orders"
        ));
    }

    #[test]
    fn does_not_recognise_an_empty_reason() {
        assert!(!is_auth_failure_reason(""));
    }
}
