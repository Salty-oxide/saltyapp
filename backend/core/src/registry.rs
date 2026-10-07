use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

/// How many authentication attempts a connection gets before the app stops
/// dialling it altogether.
///
/// Two, not one: a single rejection can be a genuine blip — an OAuth/IAM
/// token that expired between fetch and use, a broker part-way through a
/// rolling restart with a stale credential cache. Two consecutive rejections
/// is a wrong password.
///
/// Two, not more: every extra attempt is a full TCP + TLS + SASL handshake
/// that the broker has to process and log, multiplied by every desktop user
/// running this app against that cluster. The old Offset Explorer's habit of
/// retrying rejected credentials indefinitely is precisely what made a
/// handful of users with a stale password visible in production broker load.
pub const MAX_AUTH_ATTEMPTS: u32 = 2;

/// Tracks which connection ids currently have a "live" session, for the
/// New Connection... no — for the cluster detail panel's Reconnect/
/// Disconnect lifecycle: which clusters the user has explicitly connected
/// to this run of the app. Deliberately in-memory only (not persisted) —
/// on app restart every cluster starts disconnected again, same as a real
/// Kafka client session would.
/// A connection's authentication history this run of the app.
#[derive(Debug, Clone)]
struct AuthFailures {
    /// Consecutive rejected authentication attempts. Reset by a success or
    /// by [`ConnectionRegistry::clear_auth_failures`].
    attempts: u32,
    /// librdkafka's own reason for the most recent rejection, shown to the
    /// user and returned in place of every subsequent request once blocked.
    reason: String,
}

#[derive(Default)]
pub struct ConnectionRegistry {
    connected: Mutex<HashSet<String>>,
    auth_failures: Mutex<HashMap<String, AuthFailures>>,
    /// Topics this connection's principal has been refused *write* access to,
    /// keyed by `(connection id, topic)`, with the broker's own reason.
    ///
    /// Separate from `auth_failures` on purpose. A rejected password is a fact
    /// about the connection, so it blocks everything; a missing Write grant is
    /// a fact about one topic, so it blocks only publishing to that topic —
    /// reading it, and writing to others, must keep working. Mixing the two
    /// would let a single denied publish take a working cluster offline inside
    /// the app.
    write_denials: Mutex<HashMap<(String, String), String>>,
}

impl ConnectionRegistry {
    pub fn mark_connected(&self, connection_id: &str) {
        self.connected
            .lock()
            .unwrap()
            .insert(connection_id.to_string());
    }

    pub fn mark_disconnected(&self, connection_id: &str) {
        self.connected.lock().unwrap().remove(connection_id);
        // A write denial is a verdict about a session that has just ended.
        // Unlike a rejected password — which disconnect deliberately does not
        // forget, so that disconnect/reconnect cannot become a way to keep
        // dialling with credentials known to be wrong — re-asking the broker
        // about an ACL costs one produce request that the broker was going to
        // authorize or refuse anyway, and ACLs do get granted while the app is
        // open.
        self.clear_write_denials(connection_id);
    }

    pub fn is_connected(&self, connection_id: &str) -> bool {
        self.connected.lock().unwrap().contains(connection_id)
    }

    /// Records that the broker rejected this connection's credentials.
    /// Once [`MAX_AUTH_ATTEMPTS`] consecutive rejections have been recorded
    /// the connection is blocked — see [`Self::auth_block_reason`] — and
    /// dropped from the connected set, so the tree stops presenting it as a
    /// live cluster to expand.
    pub fn record_auth_failure(&self, connection_id: &str, reason: &str) {
        let mut failures = self.auth_failures.lock().unwrap();
        let entry = failures
            .entry(connection_id.to_string())
            .or_insert(AuthFailures {
                attempts: 0,
                reason: String::new(),
            });
        entry.attempts = entry.attempts.saturating_add(1);
        entry.reason = reason.to_string();
        let blocked = entry.attempts >= MAX_AUTH_ATTEMPTS;
        drop(failures);

        if blocked {
            self.mark_disconnected(connection_id);
        }
    }

    /// Records that this connection authenticated successfully, clearing any
    /// accumulated failures — the credentials work now, whatever happened
    /// before.
    pub fn record_auth_success(&self, connection_id: &str) {
        self.clear_auth_failures(connection_id);
    }

    /// Forgets this connection's authentication history. Called when the
    /// user edits the connection (the settings that were rejected no longer
    /// exist, so the verdict on them is meaningless) and when they
    /// explicitly ask to connect again.
    pub fn clear_auth_failures(&self, connection_id: &str) {
        self.auth_failures.lock().unwrap().remove(connection_id);
        // Both of this method's callers — an explicit Connect/Reconnect, and a
        // connection edit — are the user saying "try again with this". A
        // per-topic write verdict from the previous session is no more valid
        // than the credential verdict beside it.
        self.clear_write_denials(connection_id);
    }

    /// The reason to refuse this connection's requests without dialling the
    /// broker at all, or `None` while it still has attempts left. This is
    /// the fail-fast gate: a blocked connection costs the cluster nothing,
    /// however hard the user clicks.
    pub fn auth_block_reason(&self, connection_id: &str) -> Option<String> {
        self.auth_failures
            .lock()
            .unwrap()
            .get(connection_id)
            .filter(|failures| failures.attempts >= MAX_AUTH_ATTEMPTS)
            .map(|failures| failures.reason.clone())
    }

    /// Records that the broker refused to let this connection write to this
    /// topic, with the reason it gave.
    ///
    /// One refusal is enough — unlike the credential breaker, which allows two
    /// attempts because a single rejection can be a blip. An ACL check is not
    /// a blip: the broker consulted its authorizer and said no, and it will
    /// say no to every identical request until someone changes the ACL. Trying
    /// again only asks the cluster to re-run an authorization check and log a
    /// second denial.
    pub fn record_write_denied(&self, connection_id: &str, topic: &str, reason: &str) {
        self.write_denials.lock().unwrap().insert(
            (connection_id.to_string(), topic.to_string()),
            reason.to_string(),
        );
    }

    /// Why publishing to this topic is being refused without contacting the
    /// broker, or `None` if it isn't.
    pub fn write_denied_reason(&self, connection_id: &str, topic: &str) -> Option<String> {
        self.write_denials
            .lock()
            .unwrap()
            .get(&(connection_id.to_string(), topic.to_string()))
            .cloned()
    }

    /// Forgets every write denial recorded against this connection.
    pub fn clear_write_denials(&self, connection_id: &str) {
        self.write_denials
            .lock()
            .unwrap()
            .retain(|(id, _), _| id != connection_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_freshly_created_connection_is_not_connected() {
        let registry = ConnectionRegistry::default();
        assert!(!registry.is_connected("conn-1"));
    }

    #[test]
    fn marking_connected_makes_is_connected_true() {
        let registry = ConnectionRegistry::default();
        registry.mark_connected("conn-1");
        assert!(registry.is_connected("conn-1"));
    }

    #[test]
    fn marking_disconnected_makes_is_connected_false_again() {
        let registry = ConnectionRegistry::default();
        registry.mark_connected("conn-1");
        registry.mark_disconnected("conn-1");
        assert!(!registry.is_connected("conn-1"));
    }

    #[test]
    fn disconnecting_a_never_connected_id_is_a_no_op() {
        let registry = ConnectionRegistry::default();
        registry.mark_disconnected("conn-1");
        assert!(!registry.is_connected("conn-1"));
    }

    #[test]
    fn tracks_multiple_connections_independently() {
        let registry = ConnectionRegistry::default();
        registry.mark_connected("conn-1");
        registry.mark_connected("conn-2");
        registry.mark_disconnected("conn-1");

        assert!(!registry.is_connected("conn-1"));
        assert!(registry.is_connected("conn-2"));
    }

    #[test]
    fn a_connection_with_no_history_is_not_auth_blocked() {
        let registry = ConnectionRegistry::default();
        assert_eq!(registry.auth_block_reason("conn-1"), None);
    }

    #[test]
    fn a_single_auth_failure_does_not_block_yet() {
        // One failure can be a genuine blip — an expired OAuth/IAM token, a
        // broker mid-rolling-restart. The user gets one more attempt.
        let registry = ConnectionRegistry::default();
        registry.record_auth_failure("conn-1", "Authentication failed");
        assert_eq!(registry.auth_block_reason("conn-1"), None);
    }

    #[test]
    fn blocks_once_the_attempt_allowance_is_used_up_and_reports_the_last_reason() {
        let registry = ConnectionRegistry::default();
        for _ in 0..MAX_AUTH_ATTEMPTS {
            registry.record_auth_failure("conn-1", "Invalid username or password");
        }
        assert_eq!(
            registry.auth_block_reason("conn-1"),
            Some("Invalid username or password".to_string())
        );
    }

    #[test]
    fn stays_blocked_however_many_further_failures_arrive() {
        let registry = ConnectionRegistry::default();
        for _ in 0..MAX_AUTH_ATTEMPTS + 3 {
            registry.record_auth_failure("conn-1", "Authentication failed");
        }
        assert!(registry.auth_block_reason("conn-1").is_some());
    }

    #[test]
    fn tripping_the_breaker_also_drops_the_connection_from_connected() {
        // Otherwise the tree keeps showing a green, expandable cluster whose
        // every request is now refused before it leaves the app.
        let registry = ConnectionRegistry::default();
        registry.mark_connected("conn-1");
        for _ in 0..MAX_AUTH_ATTEMPTS {
            registry.record_auth_failure("conn-1", "Authentication failed");
        }
        assert!(!registry.is_connected("conn-1"));
    }

    #[test]
    fn a_successful_authentication_resets_the_failure_count() {
        let registry = ConnectionRegistry::default();
        registry.record_auth_failure("conn-1", "Authentication failed");
        registry.record_auth_success("conn-1");
        registry.record_auth_failure("conn-1", "Authentication failed");

        assert_eq!(registry.auth_block_reason("conn-1"), None);
    }

    #[test]
    fn a_successful_authentication_clears_an_existing_block() {
        let registry = ConnectionRegistry::default();
        for _ in 0..MAX_AUTH_ATTEMPTS {
            registry.record_auth_failure("conn-1", "Authentication failed");
        }
        registry.record_auth_success("conn-1");

        assert_eq!(registry.auth_block_reason("conn-1"), None);
    }

    #[test]
    fn clearing_the_breaker_lets_the_full_allowance_be_used_again() {
        // What editing the connection's credentials does: the settings that
        // failed no longer exist, so the old verdict is meaningless.
        let registry = ConnectionRegistry::default();
        for _ in 0..MAX_AUTH_ATTEMPTS {
            registry.record_auth_failure("conn-1", "Authentication failed");
        }
        registry.clear_auth_failures("conn-1");

        assert_eq!(registry.auth_block_reason("conn-1"), None);
        registry.record_auth_failure("conn-1", "Authentication failed");
        assert_eq!(registry.auth_block_reason("conn-1"), None);
    }

    #[test]
    fn tracks_auth_failures_per_connection_independently() {
        let registry = ConnectionRegistry::default();
        for _ in 0..MAX_AUTH_ATTEMPTS {
            registry.record_auth_failure("conn-1", "Authentication failed");
        }
        registry.record_auth_failure("conn-2", "Authentication failed");

        assert!(registry.auth_block_reason("conn-1").is_some());
        assert_eq!(registry.auth_block_reason("conn-2"), None);
    }

    #[test]
    fn disconnecting_does_not_clear_a_tripped_breaker() {
        // Disconnect/reconnect must not become a way to keep dialling a
        // cluster with credentials already known to be rejected; only an
        // explicit reconnect or a credential edit clears it.
        let registry = ConnectionRegistry::default();
        for _ in 0..MAX_AUTH_ATTEMPTS {
            registry.record_auth_failure("conn-1", "Authentication failed");
        }
        registry.mark_disconnected("conn-1");

        assert!(registry.auth_block_reason("conn-1").is_some());
    }

    #[test]
    fn a_topic_with_no_history_is_not_write_denied() {
        let registry = ConnectionRegistry::default();
        assert_eq!(registry.write_denied_reason("conn-1", "orders"), None);
    }

    #[test]
    fn one_write_denial_is_enough_to_block_that_topic() {
        // Unlike the credential breaker's two attempts: an ACL check is not a
        // blip, and retrying only asks the cluster to log a second denial.
        let registry = ConnectionRegistry::default();
        registry.record_write_denied("conn-1", "orders", "Broker: Topic authorization failed");
        assert_eq!(
            registry.write_denied_reason("conn-1", "orders"),
            Some("Broker: Topic authorization failed".to_string())
        );
    }

    #[test]
    fn a_write_denial_blocks_only_the_topic_it_was_recorded_for() {
        let registry = ConnectionRegistry::default();
        registry.record_write_denied("conn-1", "orders", "denied");
        assert_eq!(registry.write_denied_reason("conn-1", "payments"), None);
    }

    #[test]
    fn a_write_denial_blocks_only_the_connection_it_was_recorded_for() {
        let registry = ConnectionRegistry::default();
        registry.record_write_denied("conn-1", "orders", "denied");
        assert_eq!(registry.write_denied_reason("conn-2", "orders"), None);
    }

    #[test]
    fn a_later_denial_replaces_the_reason_for_the_same_topic() {
        let registry = ConnectionRegistry::default();
        registry.record_write_denied("conn-1", "orders", "first");
        registry.record_write_denied("conn-1", "orders", "second");
        assert_eq!(
            registry.write_denied_reason("conn-1", "orders"),
            Some("second".to_string())
        );
    }

    #[test]
    fn a_write_denial_does_not_count_against_the_credential_breaker() {
        // The whole reason AppError::Authorization exists separately: a
        // principal that may read a topic but not write it has perfectly good
        // credentials, and blocking the connection would stop topics, brokers
        // and fetches from loading over a capability it never had.
        let registry = ConnectionRegistry::default();
        registry.mark_connected("conn-1");
        for _ in 0..MAX_AUTH_ATTEMPTS + 2 {
            registry.record_write_denied("conn-1", "orders", "Topic authorization failed");
        }
        assert_eq!(registry.auth_block_reason("conn-1"), None);
        assert!(registry.is_connected("conn-1"));
    }

    #[test]
    fn reconnecting_clears_write_denials_so_a_granted_acl_can_take_effect() {
        let registry = ConnectionRegistry::default();
        registry.record_write_denied("conn-1", "orders", "denied");
        registry.clear_auth_failures("conn-1");
        assert_eq!(registry.write_denied_reason("conn-1", "orders"), None);
    }

    #[test]
    fn disconnecting_clears_write_denials() {
        let registry = ConnectionRegistry::default();
        registry.mark_connected("conn-1");
        registry.record_write_denied("conn-1", "orders", "denied");
        registry.mark_disconnected("conn-1");
        assert_eq!(registry.write_denied_reason("conn-1", "orders"), None);
    }

    #[test]
    fn clearing_one_connections_denials_leaves_anothers_alone() {
        let registry = ConnectionRegistry::default();
        registry.record_write_denied("conn-1", "orders", "denied");
        registry.record_write_denied("conn-2", "orders", "denied");
        registry.clear_write_denials("conn-1");
        assert_eq!(registry.write_denied_reason("conn-1", "orders"), None);
        assert_eq!(
            registry.write_denied_reason("conn-2", "orders"),
            Some("denied".to_string())
        );
    }

    #[test]
    fn clearing_denials_clears_every_topic_of_that_connection() {
        let registry = ConnectionRegistry::default();
        registry.record_write_denied("conn-1", "orders", "denied");
        registry.record_write_denied("conn-1", "payments", "denied");
        registry.clear_write_denials("conn-1");
        assert_eq!(registry.write_denied_reason("conn-1", "orders"), None);
        assert_eq!(registry.write_denied_reason("conn-1", "payments"), None);
    }

    #[test]
    fn a_tripped_credential_breaker_does_not_leave_stale_write_denials_behind() {
        // Tripping the breaker marks the connection disconnected, which clears
        // them — so the reason the user is shown after fixing their password is
        // the current one, not a verdict from the previous session.
        let registry = ConnectionRegistry::default();
        registry.mark_connected("conn-1");
        registry.record_write_denied("conn-1", "orders", "denied");
        for _ in 0..MAX_AUTH_ATTEMPTS {
            registry.record_auth_failure("conn-1", "Authentication failed");
        }
        assert_eq!(registry.write_denied_reason("conn-1", "orders"), None);
    }
}
