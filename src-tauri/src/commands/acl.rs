//! ACL listing commands.
//!
//! Wrappers only. The FFI lives in `salty_kafka::acl`, the classification of
//! what the broker said and the authorization logic live in `salty_core`, and
//! both are tested where `cargo test` can reach them. Keeping these functions
//! logic-free is what lets `src-tauri` stay outside the coverage ratio
//! without leaving anything untested — see the Conventions in CLAUDE.md.

use super::connections::{connection_for_request, log_broker_call, record_auth_success_only, CommandError};
use crate::state::AppState;
use salty_core::{resource_access, AclFilter, AclListing, AclOperation, ResourceAccess, ResourceType};
use std::time::Duration;
use tauri::{AppHandle, State};

/// Backs the tree's Access Control category: every ACL the broker will show
/// this principal.
#[tauri::command]
pub async fn acl_list(
    app: AppHandle,
    state: State<'_, AppState>,
    id: String,
    read_timeout_ms: u64,
) -> Result<AclListing, CommandError> {
    let connection = connection_for_request(&state, &id).await?;
    let started = std::time::Instant::now();
    let result = state
        .kafka
        .describe_acls(&connection, AclFilter::any(), Duration::from_millis(read_timeout_ms))
        .await;
    // The success-only breaker, for exactly the reason it exists for the
    // consumer-group calls: listing ACLs needs `Describe` on `Cluster`, so a
    // principal with perfectly valid credentials and full access to every
    // topic can still be refused here. Counting that as an authentication
    // failure would take a working cluster offline inside the app over a
    // capability the user may never have needed.
    record_auth_success_only(&state, &id, &result);
    log_broker_call(&app, "Listing ACLs", started, if result.is_ok() { "finished" } else { "failed" });
    Ok(result?)
}

/// Backs the Access tab on a topic or consumer group: every ACL that governs
/// one named resource, **and** the verdict those ACLs add up to per
/// principal.
///
/// The matrix is computed here rather than in the frontend so that Kafka's
/// deny-precedence and implication rules have exactly one implementation —
/// the one in `salty_core` with unit tests around it. The frontend renders
/// the result and decides nothing.
///
/// `resource_type` is the `salty_core` enum, so an unrecognised value is
/// rejected at deserialization rather than being silently treated as "any"
/// — which would answer a question about one topic with the whole cluster's
/// ACLs.
#[tauri::command]
pub async fn acl_for_resource(
    app: AppHandle,
    state: State<'_, AppState>,
    id: String,
    resource_type: ResourceType,
    resource_name: String,
    read_timeout_ms: u64,
) -> Result<ResourceAccess, CommandError> {
    let connection = connection_for_request(&state, &id).await?;
    let started = std::time::Instant::now();
    let result = state
        .kafka
        .describe_acls(
            &connection,
            // MATCH mode: the broker resolves which literal, prefixed and
            // wildcard patterns govern this name. See `AclFilter::governing`.
            AclFilter::governing(resource_type, &resource_name),
            Duration::from_millis(read_timeout_ms),
        )
        .await;
    record_auth_success_only(&state, &id, &result);
    log_broker_call(
        &app,
        "Listing ACLs for resource",
        started,
        if result.is_ok() { "finished" } else { "failed" },
    );

    // Which operations get a column depends on what the resource is: a
    // consumer group cannot be written to or have its configs read, so
    // showing those columns would be showing permanent crosses.
    let operations: &[AclOperation] = match resource_type {
        ResourceType::Group => &AclOperation::GROUP_COLUMNS,
        _ => &AclOperation::TOPIC_COLUMNS,
    };
    Ok(resource_access(result?, operations))
}
