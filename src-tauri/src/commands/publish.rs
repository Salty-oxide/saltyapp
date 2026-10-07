//! The publish command — the only place in this app that writes to a user's
//! Kafka cluster.
//!
//! Deliberately its own file rather than more of `connections.rs`: everything
//! here is a gate, and gates are easier to audit when nothing else shares the
//! module.
//!
//! **This file holds no logic of its own.** Whether a publish may proceed is
//! `salty_core::publish_refusal`; what the entered text becomes is
//! `salty_core::encode_messages`; how a produce failure is classified is
//! `salty_kafka::producer::classify_produce_error`. All three live in
//! crates that can be built and tested anywhere, because `src-tauri` needs a
//! desktop toolchain and is excluded from coverage (see CLAUDE.md). What is
//! left here is the order the gates run in, and what their verdicts are
//! recorded against.

use crate::commands::connections::{connection_for_request, log_broker_call, CommandError};
use crate::state::AppState;
use salty_core::{
    encode_messages, publish_refusal, NewPublishMessage, PublishFailureKind, PublishLimits,
    PublishOutcome,
};
use std::time::Duration;
use tauri::{AppHandle, State};

/// Publishes messages to one partition of one topic.
///
/// Five gates, in this order, and the first four never touch the network:
///
/// 1. **The credential circuit breaker**, via `connection_for_request` — a
///    connection whose credentials the broker has already rejected is refused
///    from memory.
/// 2. **Connected**, 3. **`allow_publishing`**, 4. **a cached broker denial** —
///    all three via `publish_refusal`, which owns their precedence.
/// 5. **Validation**, via `encode_messages` — nothing malformed or oversized
///    gets as far as a producer existing.
///
/// Gate 3 is the one worth being explicit about: `allow_publishing` is read
/// from SQLite *here*, on every publish, as part of the connection this
/// command loads for itself. The frontend also disables its Publish tab when
/// the flag is off, but that is a courtesy to the user, not a control — this
/// read is the control, and a direct IPC call cannot get past it.
///
/// A refused publish returns an `Err` and nothing reaches the cluster. A
/// publish that reached the cluster returns `Ok` even when it failed part-way,
/// because the outcome then carries something no error can: which messages
/// landed, and at which offsets.
#[tauri::command]
#[allow(clippy::too_many_arguments)]
pub async fn connection_publish_messages(
    app: AppHandle,
    state: State<'_, AppState>,
    id: String,
    topic: String,
    partition: i32,
    messages: Vec<NewPublishMessage>,
    max_message_size_bytes: u32,
    write_timeout_ms: u64,
) -> Result<PublishOutcome, CommandError> {
    let connection = connection_for_request(&state, &id).await?;

    if let Some(refusal) = publish_refusal(
        state.connections.is_connected(&id),
        connection.allow_publishing,
        state
            .connections
            .write_denied_reason(&id, &topic)
            .as_deref(),
    ) {
        // Logged as well as returned: a refused write is exactly the kind of
        // thing someone will later want evidence of, and the Logs panel is
        // where this app keeps that.
        crate::logging::emit_log(
            &app,
            "warn",
            format!(
                "Refused to publish {} message(s) to {topic}[{partition}]: {}",
                messages.len(),
                refusal.message(&topic)
            ),
        );
        return Err(CommandError {
            message: format!("{}: {}", refusal.error(), refusal.message(&topic)),
        });
    }

    // Validation before the producer exists, so a batch the user got wrong
    // costs no connection to their cluster and leaves no trace on it.
    let records = encode_messages(
        &messages,
        &PublishLimits::for_max_message_size(max_message_size_bytes),
    )?;

    let started = std::time::Instant::now();
    let result = state
        .kafka
        .publish_messages(
            &connection,
            &topic,
            partition,
            &records,
            max_message_size_bytes,
            Duration::from_millis(write_timeout_ms),
        )
        .await;

    let outcome = result?;

    // A delivered message is proof the credentials work, whatever happened to
    // the rest of the batch.
    if !outcome.delivered.is_empty() {
        state.connections.record_auth_success(&id);
    }

    if let Some(failure) = &outcome.failure {
        match failure.kind {
            // Remember the refusal so the Publish tab can disable itself, and
            // so a second attempt is answered from memory instead of asking the
            // broker to authorize and log the same denial again.
            PublishFailureKind::Authorization => {
                state
                    .connections
                    .record_write_denied(&id, &topic, &failure.reason);
            }
            // The credentials themselves were rejected: this one *does* belong
            // to the connection-wide breaker, and the pooled client built from
            // them cannot serve the next request either.
            PublishFailureKind::Authentication => {
                state.connections.record_auth_failure(&id, &failure.reason);
                state.kafka.release(&id);
            }
            PublishFailureKind::Validation | PublishFailureKind::Transient => {}
        }
    }

    // The audit line. Says what was attempted, where, and what came of it —
    // and never any part of a payload, key or header, which is the difference
    // between an audit trail and a data leak into a log file the user may
    // paste into a bug report.
    let outcome_text = match &outcome.failure {
        None => format!("delivered {} message(s)", outcome.delivered.len()),
        Some(failure) => format!(
            "failed after {} of {} message(s): {}",
            outcome.delivered.len(),
            records.len(),
            failure.reason
        ),
    };
    log_broker_call(
        &app,
        &format!(
            "Publishing {} message(s) to {topic}[{partition}] —",
            records.len()
        ),
        started,
        &outcome_text,
    );

    Ok(outcome)
}

/// Why publishing to this topic is blocked without asking the broker, or
/// `None` if it isn't. Lets the Publish tab explain itself before the user
/// types anything, rather than only after a refused attempt.
#[tauri::command]
pub async fn connection_write_denied_reason(
    state: State<'_, AppState>,
    id: String,
    topic: String,
) -> Result<Option<String>, CommandError> {
    Ok(state.connections.write_denied_reason(&id, &topic))
}
