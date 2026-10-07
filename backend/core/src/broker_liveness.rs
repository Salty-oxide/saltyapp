//! Reading a cluster's reachability off a client that is already connected,
//! instead of dialling the broker to ask.
//!
//! librdkafka reports every broker's connection state through its statistics
//! callback. That costs the cluster nothing — no socket, no request — so a
//! pooled client can answer "is the cluster still there?" as often as the UI
//! likes. This module is the judgement applied to those reports; it is pure
//! (the caller supplies the clock) so every branch is testable without a
//! broker.
//!
//! The judgement has to be a *duration*, not a single sample. Measured against
//! a real broker: when a broker closes an idle connection (its
//! `connections.max.idle.ms`), librdkafka reports `AllBrokersDown` and the
//! broker state drops to `INIT`, then reconnects by itself within a couple of
//! seconds. A genuinely dead broker looks identical at first and then simply
//! never comes back up. Only the length of the gap tells them apart.

use crate::ConnectionStatus;

/// How long no broker may be `UP` before the cluster counts as unreachable.
///
/// Several statistics intervals, so one missed report is never a verdict, and
/// comfortably longer than the idle-close reconnect seen in practice (about two
/// to three seconds).
pub const LIVENESS_GRACE_MS: u64 = 15_000;

/// How often librdkafka is asked to emit statistics. Reports queue until the
/// client is next polled, so this is also the rate at which an unpolled client
/// accumulates them — kept slow so a window left in the background for hours
/// costs little.
pub const STATS_INTERVAL_MS: u64 = 5_000;

/// Whether a librdkafka broker state (the `state` field of a statistics
/// broker entry) means the connection is established.
///
/// `UPDATE` is the brief state while API versions are re-requested on an
/// otherwise live connection.
pub fn broker_state_is_up(state: &str) -> bool {
    matches!(state, "UP" | "UPDATE")
}

/// When a client was created and when any of its brokers was last seen `UP`.
#[derive(Debug, Clone, Copy)]
pub struct LivenessTracker {
    started_at_ms: u64,
    last_up_ms: Option<u64>,
}

impl LivenessTracker {
    pub fn new(now_ms: u64) -> Self {
        Self {
            started_at_ms: now_ms,
            last_up_ms: None,
        }
    }

    /// Records one statistics report. `any_up` is whether at least one broker
    /// was connected in it.
    pub fn observe(&mut self, now_ms: u64, any_up: bool) {
        if any_up {
            self.last_up_ms = Some(self.last_up_ms.map_or(now_ms, |seen| seen.max(now_ms)));
        }
    }

    /// `Reachable` while a broker is up or was moments ago; `Unreachable` once
    /// none has been for `grace_ms`; `Unknown` in between, which is also what a
    /// client that has not yet had time to connect reports.
    ///
    /// The clock for a client that has never been `UP` starts at its creation,
    /// so a cluster that is down from the very first connect is still reported
    /// unreachable after the same grace period.
    pub fn status(&self, now_ms: u64, grace_ms: u64) -> ConnectionStatus {
        let reference = self.last_up_ms.unwrap_or(self.started_at_ms);
        let silent_for = now_ms.saturating_sub(reference);
        match self.last_up_ms {
            // Up right now, to within a statistics interval.
            Some(seen) if now_ms.saturating_sub(seen) <= INTERVAL_SLACK_MS => {
                ConnectionStatus::Reachable
            }
            _ if silent_for >= grace_ms => ConnectionStatus::Unreachable,
            _ => ConnectionStatus::Unknown,
        }
    }
}

/// How stale an `UP` sighting may be and still count as "up now": one
/// statistics interval plus a margin for the delay before the report is read.
pub const INTERVAL_SLACK_MS: u64 = STATS_INTERVAL_MS + 2_500;

#[cfg(test)]
mod tests {
    use super::*;

    const GRACE: u64 = LIVENESS_GRACE_MS;

    #[test]
    fn up_and_upgrading_connections_count_as_up() {
        assert!(broker_state_is_up("UP"));
        assert!(broker_state_is_up("UPDATE"));
    }

    #[test]
    fn every_other_state_is_down() {
        for state in [
            "INIT",
            "DOWN",
            "CONNECT",
            "AUTH_LEGACY",
            "APIVERSION_QUERY",
            "TRY_CONNECT",
            "",
        ] {
            assert!(!broker_state_is_up(state), "{state}");
        }
    }

    #[test]
    fn a_fresh_client_has_not_yet_had_the_chance_to_connect() {
        let tracker = LivenessTracker::new(1_000);
        assert_eq!(tracker.status(1_000, GRACE), ConnectionStatus::Unknown);
        assert_eq!(
            tracker.status(1_000 + GRACE - 1, GRACE),
            ConnectionStatus::Unknown
        );
    }

    #[test]
    fn a_cluster_that_never_comes_up_is_unreachable_after_the_grace_period() {
        let tracker = LivenessTracker::new(1_000);
        assert_eq!(
            tracker.status(1_000 + GRACE, GRACE),
            ConnectionStatus::Unreachable
        );
    }

    #[test]
    fn a_broker_seen_up_just_now_is_reachable() {
        let mut tracker = LivenessTracker::new(0);
        tracker.observe(5_000, true);
        assert_eq!(tracker.status(5_000, GRACE), ConnectionStatus::Reachable);
        assert_eq!(
            tracker.status(5_000 + INTERVAL_SLACK_MS, GRACE),
            ConnectionStatus::Reachable
        );
    }

    // The idle-close case measured against a real broker: UP, then a gap of a
    // few seconds while librdkafka reconnects. It must never be reported as a
    // failure, or an idle cluster would be disconnected out from under the user.
    #[test]
    fn a_short_gap_after_an_up_sighting_is_unknown_not_unreachable() {
        let mut tracker = LivenessTracker::new(0);
        tracker.observe(10_000, true);
        assert_eq!(
            tracker.status(10_000 + INTERVAL_SLACK_MS + 1, GRACE),
            ConnectionStatus::Unknown
        );
        assert_eq!(
            tracker.status(10_000 + GRACE - 1, GRACE),
            ConnectionStatus::Unknown
        );
    }

    #[test]
    fn a_long_gap_after_an_up_sighting_is_unreachable() {
        let mut tracker = LivenessTracker::new(0);
        tracker.observe(10_000, true);
        assert_eq!(
            tracker.status(10_000 + GRACE, GRACE),
            ConnectionStatus::Unreachable
        );
    }

    #[test]
    fn coming_back_up_clears_an_unreachable_verdict() {
        let mut tracker = LivenessTracker::new(0);
        assert_eq!(
            tracker.status(GRACE + 5_000, GRACE),
            ConnectionStatus::Unreachable
        );
        tracker.observe(GRACE + 6_000, true);
        assert_eq!(
            tracker.status(GRACE + 6_000, GRACE),
            ConnectionStatus::Reachable
        );
    }

    #[test]
    fn a_report_with_no_broker_up_does_not_reset_the_clock() {
        let mut tracker = LivenessTracker::new(0);
        tracker.observe(1_000, true);
        tracker.observe(1_000 + GRACE / 2, false);
        tracker.observe(1_000 + GRACE, false);
        assert_eq!(
            tracker.status(1_000 + GRACE, GRACE),
            ConnectionStatus::Unreachable
        );
    }

    #[test]
    fn an_out_of_order_older_sighting_does_not_move_the_clock_backwards() {
        let mut tracker = LivenessTracker::new(0);
        tracker.observe(9_000, true);
        tracker.observe(4_000, true);
        assert_eq!(tracker.status(9_000, GRACE), ConnectionStatus::Reachable);
    }
}
