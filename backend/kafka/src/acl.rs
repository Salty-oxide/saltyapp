//! `DescribeAcls`, over raw librdkafka FFI.
//!
//! **Why this is unsafe code and not a call into rdkafka's admin API.**
//! rdkafka 0.39's safe `AdminClient` exposes create/delete topics, delete
//! groups, create partitions, delete records, and describe/alter configs —
//! and nothing else. `grep -i acl` over `rdkafka-0.39.0/src/` finds nothing.
//! But rdkafka re-exports the sys crate wholesale (`pub use
//! rdkafka_sys::{bindings, helpers, types};`, `lib.rs:275`), and
//! rdkafka-sys 4.10.0+2.12.1 binds the complete C ACL API. Since
//! `AdminClient::inner()` hands back a `Client` whose `native_ptr()` is
//! public, the ACL calls are reachable from the *existing pooled admin
//! client* — no fork, no vendored patch, no second client type, and no
//! hand-rolled Kafka protocol implementation.
//!
//! **The containment rule.** Every `unsafe` block in the ACL feature is in
//! this file. Callers receive owned `salty_core` types and never see a raw
//! pointer. The classification of *what the response meant* and the
//! authorization logic both live in `salty_core`, where they are unit-tested
//! without a broker; this module only moves bytes across the FFI boundary.

use std::ffi::{CStr, CString};
use std::ptr;
use std::sync::Arc;
use std::time::Duration;

use error_stack::Report;
use rdkafka::admin::AdminClient;
use rdkafka::bindings as rd;
use rdkafka::client::DefaultClientContext;
use salty_core::{
    AclAvailability, AclBinding, AclFilter, AclListing, AclOperation, AclPermission, AppError,
    PatternType, ResourceType, Result,
};

/// How long to wait for the broker's ACL response before giving up.
///
/// Passed to librdkafka as the admin request timeout *and* used as the queue
/// poll timeout, with a margin on the poll: if the request times out
/// internally, librdkafka still enqueues a timeout *event*, and polling for
/// slightly longer than the request lets us read that event and report the
/// real reason instead of a bare "nothing arrived".
const POLL_MARGIN: Duration = Duration::from_secs(2);

/// Reads a C string that librdkafka guarantees is non-null, lossily.
///
/// Lossy because a principal or resource name is arbitrary bytes as far as
/// this process is concerned, and a malformed one should show as `�` in the
/// table rather than fail the entire listing.
///
/// # Safety
/// `ptr` must be null or point to a NUL-terminated string that outlives the
/// call.
unsafe fn owned_string(ptr: *const std::os::raw::c_char) -> String {
    if ptr.is_null() {
        return String::new();
    }
    unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned()
}

/// Maps a resource type onto librdkafka's own enum.
///
/// Written out rather than transmuted from the numeric wire value: the two
/// sides agree today, and an explicit match is what makes a future
/// disagreement a compile error instead of a silently mis-typed request.
fn native_resource_type(value: ResourceType) -> rd::rd_kafka_ResourceType_t {
    use rd::rd_kafka_ResourceType_t as T;
    match value {
        ResourceType::Unknown => T::RD_KAFKA_RESOURCE_UNKNOWN,
        ResourceType::Any => T::RD_KAFKA_RESOURCE_ANY,
        ResourceType::Topic => T::RD_KAFKA_RESOURCE_TOPIC,
        ResourceType::Group => T::RD_KAFKA_RESOURCE_GROUP,
        ResourceType::Broker => T::RD_KAFKA_RESOURCE_BROKER,
        ResourceType::TransactionalId => T::RD_KAFKA_RESOURCE_TRANSACTIONAL_ID,
    }
}

/// See [`native_resource_type`] for why these are spelled out.
fn native_pattern_type(value: PatternType) -> rd::rd_kafka_ResourcePatternType_t {
    use rd::rd_kafka_ResourcePatternType_t as T;
    match value {
        PatternType::Unknown => T::RD_KAFKA_RESOURCE_PATTERN_UNKNOWN,
        PatternType::Any => T::RD_KAFKA_RESOURCE_PATTERN_ANY,
        PatternType::Match => T::RD_KAFKA_RESOURCE_PATTERN_MATCH,
        PatternType::Literal => T::RD_KAFKA_RESOURCE_PATTERN_LITERAL,
        PatternType::Prefixed => T::RD_KAFKA_RESOURCE_PATTERN_PREFIXED,
    }
}

/// See [`native_resource_type`] for why these are spelled out.
fn native_operation(value: AclOperation) -> rd::rd_kafka_AclOperation_t {
    use rd::rd_kafka_AclOperation_t as T;
    match value {
        AclOperation::Unknown => T::RD_KAFKA_ACL_OPERATION_UNKNOWN,
        AclOperation::Any => T::RD_KAFKA_ACL_OPERATION_ANY,
        AclOperation::All => T::RD_KAFKA_ACL_OPERATION_ALL,
        AclOperation::Read => T::RD_KAFKA_ACL_OPERATION_READ,
        AclOperation::Write => T::RD_KAFKA_ACL_OPERATION_WRITE,
        AclOperation::Create => T::RD_KAFKA_ACL_OPERATION_CREATE,
        AclOperation::Delete => T::RD_KAFKA_ACL_OPERATION_DELETE,
        AclOperation::Alter => T::RD_KAFKA_ACL_OPERATION_ALTER,
        AclOperation::Describe => T::RD_KAFKA_ACL_OPERATION_DESCRIBE,
        AclOperation::ClusterAction => T::RD_KAFKA_ACL_OPERATION_CLUSTER_ACTION,
        AclOperation::DescribeConfigs => T::RD_KAFKA_ACL_OPERATION_DESCRIBE_CONFIGS,
        AclOperation::AlterConfigs => T::RD_KAFKA_ACL_OPERATION_ALTER_CONFIGS,
        AclOperation::IdempotentWrite => T::RD_KAFKA_ACL_OPERATION_IDEMPOTENT_WRITE,
    }
}

/// See [`native_resource_type`] for why these are spelled out.
fn native_permission(value: AclPermission) -> rd::rd_kafka_AclPermissionType_t {
    use rd::rd_kafka_AclPermissionType_t as T;
    match value {
        AclPermission::Unknown => T::RD_KAFKA_ACL_PERMISSION_TYPE_UNKNOWN,
        AclPermission::Any => T::RD_KAFKA_ACL_PERMISSION_TYPE_ANY,
        AclPermission::Deny => T::RD_KAFKA_ACL_PERMISSION_TYPE_DENY,
        AclPermission::Allow => T::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW,
    }
}

/// A `CString` for an optional filter field, or null for "do not narrow".
fn optional_c_string(value: Option<&str>) -> Result<Option<CString>, AppError> {
    match value {
        None => Ok(None),
        Some(text) => CString::new(text)
            .map(Some)
            .map_err(|_| Report::new(AppError::Validation).attach("filter value contains a NUL byte")),
    }
}

/// Asks the broker which ACL bindings match `filter`.
///
/// Returns an [`AclListing`] rather than a bare `Vec`, because two of the
/// broker's possible answers are *facts about the cluster* rather than
/// failures of the call: `SECURITY_DISABLED` means no authorizer is running
/// (so everything is permitted), and `CLUSTER_AUTHORIZATION_FAILED` means one
/// is running but this principal may not read it. Folding those into an error
/// would make them indistinguishable from each other and from a genuine
/// timeout — and would let the authorization failure reach the credential
/// circuit breaker, which is exactly the hazard `AppError::Authorization`
/// exists to avoid. Every *other* error code is a real failure and is
/// returned as one.
pub async fn describe_acls(
    admin: Arc<AdminClient<DefaultClientContext>>,
    filter: AclFilter,
    timeout: Duration,
) -> Result<AclListing, AppError> {
    tokio::task::spawn_blocking(move || describe_acls_blocking(&admin, &filter, timeout))
        .await
        .map_err(|_| Report::new(AppError::Kafka).attach("describe_acls task panicked"))?
}

/// The blocking half, run on `spawn_blocking`.
///
/// librdkafka's admin API is queue-based: submit the request against a queue,
/// then block on that queue for the reply. That is a blocking wait on a
/// native thread, so it belongs off the async runtime — the same reason the
/// fetch path uses `BaseConsumer` inside `spawn_blocking` rather than
/// `StreamConsumer`.
fn describe_acls_blocking(
    admin: &AdminClient<DefaultClientContext>,
    filter: &AclFilter,
    timeout: Duration,
) -> Result<AclListing, AppError> {
    let name = optional_c_string(filter.resource_name.as_deref())?;
    let principal = optional_c_string(filter.principal.as_deref())?;
    let host = optional_c_string(filter.host.as_deref())?;

    let client = admin.inner().native_ptr();
    let mut errstr = vec![0_i8; 512];

    // SAFETY: `client` comes from a live `AdminClient` held by the caller for
    // the whole call. Every pointer handed to librdkafka below either is null
    // or points to a CString that outlives this function. Each owned native
    // object is destroyed exactly once on every path, including the early
    // returns, via the explicit destroy calls below — there is no branch that
    // leaves one alive.
    unsafe {
        let native_filter = rd::rd_kafka_AclBindingFilter_new(
            native_resource_type(filter.resource_type),
            name.as_ref().map_or(ptr::null(), |value| value.as_ptr()),
            native_pattern_type(filter.pattern_type),
            principal.as_ref().map_or(ptr::null(), |value| value.as_ptr()),
            host.as_ref().map_or(ptr::null(), |value| value.as_ptr()),
            native_operation(filter.operation),
            native_permission(filter.permission),
            errstr.as_mut_ptr() as *mut std::os::raw::c_char,
            errstr.len(),
        );
        if native_filter.is_null() {
            let reason = owned_string(errstr.as_ptr() as *const std::os::raw::c_char);
            return Err(Report::new(AppError::Validation)
                .attach(format!("could not build ACL filter: {reason}")));
        }

        let options = rd::rd_kafka_AdminOptions_new(
            client,
            rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBEACLS,
        );
        if !options.is_null() {
            rd::rd_kafka_AdminOptions_set_request_timeout(
                options,
                timeout.as_millis() as i32,
                errstr.as_mut_ptr() as *mut std::os::raw::c_char,
                errstr.len(),
            );
        }

        let queue = rd::rd_kafka_queue_new(client);
        if queue.is_null() {
            rd::rd_kafka_AclBinding_destroy(native_filter);
            if !options.is_null() {
                rd::rd_kafka_AdminOptions_destroy(options);
            }
            return Err(Report::new(AppError::Kafka).attach("could not create an admin queue"));
        }

        rd::rd_kafka_DescribeAcls(client, native_filter, options, queue);

        // librdkafka copies what it needs out of the filter and options when
        // the request is queued, so both are ours to release immediately.
        rd::rd_kafka_AclBinding_destroy(native_filter);
        if !options.is_null() {
            rd::rd_kafka_AdminOptions_destroy(options);
        }

        let poll_for = timeout.saturating_add(POLL_MARGIN);
        let event = rd::rd_kafka_queue_poll(queue, poll_for.as_millis() as i32);
        rd::rd_kafka_queue_destroy(queue);

        if event.is_null() {
            return Err(Report::new(AppError::Kafka)
                .attach("the broker did not answer the ACL request in time"));
        }

        let listing = read_describe_event(event);
        rd::rd_kafka_event_destroy(event);
        listing
    }
}

/// Turns a `DescribeAcls` result event into owned Rust values.
///
/// # Safety
/// `event` must be a live event from `rd_kafka_queue_poll`, not yet
/// destroyed. Nothing this returns borrows from it: every string is copied,
/// which is what makes destroying the event immediately afterwards sound.
unsafe fn read_describe_event(event: *mut rd::rd_kafka_event_t) -> Result<AclListing, AppError> {
    let code = unsafe { rd::rd_kafka_event_error(event) } as i32;

    // Classified in `salty_core` from the numeric code alone — never from
    // librdkafka's message text, which is a human-facing string that has
    // changed between releases.
    //
    // **This path is defensive, not the one that runs.** librdkafka 2.12.1
    // discards the DescribeAcls error code entirely
    // (`rd_kafka_DescribeAclsResponse_parse` returns
    // `RD_KAFKA_RESP_ERR_NO_ERROR` unconditionally), so both
    // `SECURITY_DISABLED` and `CLUSTER_AUTHORIZATION_FAILED` arrive here as
    // code 0 with an empty binding list — verified against real brokers in
    // `tests/acl_describe.rs`. The caller therefore re-derives the meaning of
    // an empty listing from the broker's `authorizer.class.name`; see
    // `AclAvailability::for_empty_listing`. Kept anyway so that a librdkafka
    // release which starts propagating the code is handled correctly without
    // a change here.
    match AclAvailability::from_error_code(code) {
        Some(AclAvailability::NoAuthorizer) => return Ok(AclListing::no_authorizer()),
        Some(AclAvailability::Indeterminate) => return Ok(AclListing::indeterminate()),
        Some(AclAvailability::Available) => {}
        None => {
            let reason = unsafe { owned_string(rd::rd_kafka_event_error_string(event)) };
            return Err(Report::new(AppError::Kafka)
                .attach(format!("failed to list ACLs: {reason} (code {code})")));
        }
    }

    let result = unsafe { rd::rd_kafka_event_DescribeAcls_result(event) };
    if result.is_null() {
        return Err(Report::new(AppError::Kafka)
            .attach("the broker's reply was not an ACL listing"));
    }

    let mut count: usize = 0;
    let acls = unsafe { rd::rd_kafka_DescribeAcls_result_acls(result, &mut count) };
    if acls.is_null() {
        return Ok(AclListing {
            availability: AclAvailability::Available,
            bindings: Vec::new(),
            binding_errors: Vec::new(),
        });
    }

    let mut bindings = Vec::with_capacity(count);
    let mut binding_errors = Vec::new();

    for index in 0..count {
        // SAFETY: `acls` points to `count` valid binding pointers owned by
        // `event`, which is still alive for the whole loop.
        let binding = unsafe { *acls.add(index) };
        if binding.is_null() {
            continue;
        }

        // A binding can carry an error of its own. Collected and reported
        // rather than allowed to fail the listing: one bad entry must not
        // hide every good one.
        let error = unsafe { rd::rd_kafka_AclBinding_error(binding) };
        if !error.is_null() {
            let reason = unsafe { owned_string(rd::rd_kafka_error_string(error)) };
            binding_errors.push(reason);
            continue;
        }

        bindings.push(AclBinding {
            resource_type: ResourceType::from_wire(unsafe {
                rd::rd_kafka_AclBinding_restype(binding)
            } as i32),
            resource_name: unsafe { owned_string(rd::rd_kafka_AclBinding_name(binding)) },
            pattern_type: PatternType::from_wire(unsafe {
                rd::rd_kafka_AclBinding_resource_pattern_type(binding)
            } as i32),
            principal: unsafe { owned_string(rd::rd_kafka_AclBinding_principal(binding)) },
            host: unsafe { owned_string(rd::rd_kafka_AclBinding_host(binding)) },
            operation: AclOperation::from_wire(unsafe {
                rd::rd_kafka_AclBinding_operation(binding)
            } as i32),
            permission: AclPermission::from_wire(unsafe {
                rd::rd_kafka_AclBinding_permission_type(binding)
            } as i32),
        });
    }

    Ok(AclListing {
        availability: AclAvailability::Available,
        bindings,
        binding_errors,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // The filter fields are built into C strings before they cross the
    // boundary, and a NUL byte in the middle of one would silently truncate
    // the filter rather than fail it. Rejected here instead.
    #[test]
    fn rejects_a_filter_value_containing_a_nul_byte() {
        let result = optional_c_string(Some("ord\0ers"));

        assert!(result.is_err());
    }

    #[test]
    fn passes_an_ordinary_filter_value_through() {
        let value = optional_c_string(Some("orders")).expect("should convert");

        assert_eq!(value.expect("should be present").to_str(), Ok("orders"));
    }

    #[test]
    fn maps_an_absent_filter_value_to_nothing() {
        assert!(optional_c_string(None).expect("should convert").is_none());
    }

    #[test]
    fn reads_a_null_c_string_as_empty_rather_than_crashing() {
        // SAFETY: passing null is exactly the case under test.
        let value = unsafe { owned_string(ptr::null()) };

        assert_eq!(value, "");
    }
}
