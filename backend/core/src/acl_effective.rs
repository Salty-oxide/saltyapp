//! Turning a set of ACL bindings into "may this principal do this?".
//!
//! **What this does not do.** It does not match resource patterns. Every
//! function here takes bindings the *broker* already decided are relevant,
//! via a `PatternType::Match` filter (see [`crate::AclFilter::governing`]).
//! Literal, prefixed and wildcard resolution is therefore done by the same
//! authorizer that will enforce it, not by a reimplementation that could
//! disagree with it.
//!
//! **What it does do** is Kafka's own precedence and implication rules, which
//! are the part people get wrong by hand — and which are asymmetric between
//! allow and deny in a way that is easy to miss. See [`effective`].
//!
//! This is the highest-risk logic in the ACL feature and it lives in
//! `salty_core` precisely so `cargo test` can reach it without a broker or a
//! desktop toolchain, exactly as `publish_refusal` does.

use serde::{Deserialize, Serialize};

use crate::acl::{AclBinding, AclListing, AclOperation, AclPermission};

/// Kafka's any-principal wildcard. A binding on this principal applies to
/// every principal, so it is folded into each one's verdict rather than being
/// left to be noticed by eye in a separate row.
pub const WILDCARD_PRINCIPAL: &str = "User:*";

/// Why a verdict came out the way it did.
///
/// Carried alongside the yes/no so the UI can label a `✓` as *implied* rather
/// than presenting it as a grant somebody wrote. A reader who cannot tell the
/// difference cannot tell which ACL to change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum VerdictReason {
    /// A `Deny` binding matched. Beats every allow, however specific.
    ExplicitDeny,
    /// An `Allow` binding names this exact operation (or `All`).
    DirectAllow,
    /// No binding names this operation, but one grants an operation that
    /// implicitly carries it — `via` is that operation.
    ImpliedAllow { via: AclOperation },
    /// Nothing matched. Kafka denies by default when an authorizer is
    /// running and any ACL exists for the resource.
    DefaultDeny,
}

/// One cell of the Access tab's matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Verdict {
    pub allowed: bool,
    pub reason: VerdictReason,
    /// True when the binding that decided this was written against
    /// [`WILDCARD_PRINCIPAL`] rather than naming this principal. Worth
    /// surfacing: revoking it affects everyone, not just this principal.
    pub via_wildcard_principal: bool,
}

/// Whether a binding written for `binding_principal` applies to `principal`.
fn principal_applies(binding_principal: &str, principal: &str) -> bool {
    binding_principal == principal || binding_principal == WILDCARD_PRINCIPAL
}

/// The operations whose **allow** implies `wanted`.
///
/// Kafka's own expansion (`AclAuthorizer`'s `allowOps`): describing a
/// resource is implied by any operation that requires knowing it exists, and
/// reading its configs is implied by altering them. `All` is handled
/// separately because it implies everything rather than a listed set.
fn implying_operations(wanted: AclOperation) -> &'static [AclOperation] {
    match wanted {
        AclOperation::Describe => &[
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::Delete,
            AclOperation::Alter,
        ],
        AclOperation::DescribeConfigs => &[AclOperation::AlterConfigs],
        _ => &[],
    }
}

/// May `principal` perform `operation`, given the bindings that govern the
/// resource?
///
/// The precedence, in Kafka's order:
///
/// 1. **Any matching `Deny` refuses.** Checked first, and checked *only
///    against the exact operation or `All`* — the implication rules below do
///    **not** apply to denies. This asymmetry is real and load-bearing: a
///    `Deny Read` does not deny `Describe`, because denying the ability to
///    read a topic is not a statement about whether you may know it exists.
///    Treating deny symmetrically would have this app report a lockout that
///    the broker would not actually impose.
/// 2. **A matching `Allow` on the exact operation (or `All`) permits.**
/// 3. **A matching `Allow` on an operation that implies it permits**, and the
///    verdict records which one.
/// 4. **Otherwise denied.**
///
/// Note the caller's responsibility: with no authorizer running
/// (`AclAvailability::NoAuthorizer`) every principal may do everything, and
/// there are no bindings to reason about. Do not render this function's
/// `DefaultDeny` in that case — it would state the exact opposite of the
/// truth. The UI shows the no-authorizer notice instead of a matrix.
pub fn effective(bindings: &[AclBinding], principal: &str, operation: AclOperation) -> Verdict {
    let applicable = || {
        bindings
            .iter()
            .filter(|binding| principal_applies(&binding.principal, principal))
    };

    // 1. Deny, on the exact operation or All — never on an implying one.
    if let Some(binding) = applicable().find(|binding| {
        binding.permission == AclPermission::Deny
            && (binding.operation == operation || binding.operation == AclOperation::All)
    }) {
        return Verdict {
            allowed: false,
            reason: VerdictReason::ExplicitDeny,
            via_wildcard_principal: binding.principal == WILDCARD_PRINCIPAL,
        };
    }

    // 2. Allow, on the exact operation or All.
    if let Some(binding) = applicable().find(|binding| {
        binding.permission == AclPermission::Allow
            && (binding.operation == operation || binding.operation == AclOperation::All)
    }) {
        return Verdict {
            allowed: true,
            reason: VerdictReason::DirectAllow,
            via_wildcard_principal: binding.principal == WILDCARD_PRINCIPAL,
        };
    }

    // 3. Allow, on an operation that implies this one.
    let implying = implying_operations(operation);
    if let Some(binding) = applicable().find(|binding| {
        binding.permission == AclPermission::Allow && implying.contains(&binding.operation)
    }) {
        return Verdict {
            allowed: true,
            reason: VerdictReason::ImpliedAllow {
                via: binding.operation,
            },
            via_wildcard_principal: binding.principal == WILDCARD_PRINCIPAL,
        };
    }

    // 4. Nothing said yes.
    Verdict {
        allowed: false,
        reason: VerdictReason::DefaultDeny,
        via_wildcard_principal: false,
    }
}

/// The distinct principals appearing in a set of bindings, sorted.
///
/// Sorted so the tree and the matrix are stable between fetches — the broker
/// does not promise an order, and a list that reshuffles on every refresh is
/// unusable.
pub fn principals(bindings: &[AclBinding]) -> Vec<String> {
    let mut found: Vec<String> = bindings
        .iter()
        .map(|binding| binding.principal.clone())
        .collect();
    found.sort();
    found.dedup();
    found
}

/// One operation's outcome for one principal, ready to render.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationVerdict {
    pub operation: AclOperation,
    #[serde(flatten)]
    pub verdict: Verdict,
}

/// One row of the Access tab's matrix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrincipalAccess {
    pub principal: String,
    pub verdicts: Vec<OperationVerdict>,
}

/// Everything the Access tab needs about one resource: the bindings that
/// govern it, and the verdict they add up to for each principal.
///
/// Computed here rather than in the frontend deliberately. The rules are
/// subtle enough that a second implementation in TypeScript would be a
/// second chance to get deny-precedence or the implication rules wrong, with
/// no unit tests of the kind this module has. The frontend renders what this
/// produces and decides nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceAccess {
    pub listing: AclListing,
    pub access: Vec<PrincipalAccess>,
}

/// Builds the matrix: every principal named in the listing, against every
/// operation the resource's view shows a column for.
pub fn resource_access(listing: AclListing, operations: &[AclOperation]) -> ResourceAccess {
    let access = principals(&listing.bindings)
        .into_iter()
        .map(|principal| PrincipalAccess {
            verdicts: operations
                .iter()
                .map(|&operation| OperationVerdict {
                    operation,
                    verdict: effective(&listing.bindings, &principal, operation),
                })
                .collect(),
            principal,
        })
        .collect();
    ResourceAccess { listing, access }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acl::{AclAvailability, AclBinding, PatternType, ResourceType};

    fn binding(
        principal: &str,
        operation: AclOperation,
        permission: AclPermission,
    ) -> AclBinding {
        AclBinding {
            resource_type: ResourceType::Topic,
            resource_name: "orders".into(),
            pattern_type: PatternType::Literal,
            principal: principal.into(),
            host: "*".into(),
            operation,
            permission,
        }
    }

    #[test]
    fn allows_an_operation_a_binding_names_outright() {
        let bindings = [binding("User:w", AclOperation::Write, AclPermission::Allow)];

        let verdict = effective(&bindings, "User:w", AclOperation::Write);

        assert!(verdict.allowed);
        assert_eq!(verdict.reason, VerdictReason::DirectAllow);
    }

    #[test]
    fn denies_an_operation_nothing_grants() {
        let bindings = [binding("User:w", AclOperation::Read, AclPermission::Allow)];

        let verdict = effective(&bindings, "User:w", AclOperation::Write);

        assert!(!verdict.allowed);
        assert_eq!(verdict.reason, VerdictReason::DefaultDeny);
    }

    #[test]
    fn ignores_bindings_belonging_to_another_principal() {
        let bindings = [binding("User:other", AclOperation::Write, AclPermission::Allow)];

        let verdict = effective(&bindings, "User:w", AclOperation::Write);

        assert!(!verdict.allowed);
    }

    // `All` is a grant of everything, not an operation in its own right.
    #[test]
    fn the_all_operation_grants_every_operation() {
        let bindings = [binding("User:admin", AclOperation::All, AclPermission::Allow)];

        for operation in AclOperation::TOPIC_COLUMNS {
            let verdict = effective(&bindings, "User:admin", operation);
            assert!(verdict.allowed, "expected All to grant {operation:?}");
            assert_eq!(verdict.reason, VerdictReason::DirectAllow);
        }
    }

    #[test]
    fn a_deny_on_all_refuses_every_operation() {
        let bindings = [
            binding("User:w", AclOperation::All, AclPermission::Deny),
            binding("User:w", AclOperation::Read, AclPermission::Allow),
        ];

        let verdict = effective(&bindings, "User:w", AclOperation::Read);

        assert!(!verdict.allowed);
        assert_eq!(verdict.reason, VerdictReason::ExplicitDeny);
    }

    // Deny wins regardless of which was written first, so the order the
    // broker happens to return bindings in cannot change the answer.
    #[test]
    fn deny_beats_allow_whichever_order_they_arrive_in() {
        let allow_first = [
            binding("User:w", AclOperation::Read, AclPermission::Allow),
            binding("User:w", AclOperation::Read, AclPermission::Deny),
        ];
        let deny_first = [
            binding("User:w", AclOperation::Read, AclPermission::Deny),
            binding("User:w", AclOperation::Read, AclPermission::Allow),
        ];

        assert!(!effective(&allow_first, "User:w", AclOperation::Read).allowed);
        assert!(!effective(&deny_first, "User:w", AclOperation::Read).allowed);
    }

    #[test]
    fn read_implies_describe() {
        let bindings = [binding("User:r", AclOperation::Read, AclPermission::Allow)];

        let verdict = effective(&bindings, "User:r", AclOperation::Describe);

        assert!(verdict.allowed);
        assert_eq!(
            verdict.reason,
            VerdictReason::ImpliedAllow {
                via: AclOperation::Read
            }
        );
    }

    #[test]
    fn write_delete_and_alter_each_imply_describe() {
        for granted in [
            AclOperation::Write,
            AclOperation::Delete,
            AclOperation::Alter,
        ] {
            let bindings = [binding("User:x", granted, AclPermission::Allow)];

            let verdict = effective(&bindings, "User:x", AclOperation::Describe);

            assert!(verdict.allowed, "expected {granted:?} to imply Describe");
            assert_eq!(
                verdict.reason,
                VerdictReason::ImpliedAllow { via: granted }
            );
        }
    }

    #[test]
    fn alter_configs_implies_describe_configs() {
        let bindings = [binding(
            "User:x",
            AclOperation::AlterConfigs,
            AclPermission::Allow,
        )];

        let verdict = effective(&bindings, "User:x", AclOperation::DescribeConfigs);

        assert!(verdict.allowed);
        assert_eq!(
            verdict.reason,
            VerdictReason::ImpliedAllow {
                via: AclOperation::AlterConfigs
            }
        );
    }

    // Implication runs one way only. Being allowed to Describe a topic says
    // nothing about being allowed to read it.
    #[test]
    fn describe_does_not_imply_the_operations_that_imply_it() {
        let bindings = [binding("User:d", AclOperation::Describe, AclPermission::Allow)];

        for operation in [
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::Delete,
            AclOperation::Alter,
        ] {
            assert!(
                !effective(&bindings, "User:d", operation).allowed,
                "Describe must not imply {operation:?}"
            );
        }
    }

    // The asymmetry that is easiest to get wrong. Kafka expands the operation
    // set only when looking for an *allow*; a deny is matched against the
    // exact operation. So a Deny Read leaves Describe untouched, and the
    // separate Allow Read still implies it.
    #[test]
    fn a_deny_on_read_does_not_deny_describe() {
        let bindings = [
            binding("User:r", AclOperation::Read, AclPermission::Deny),
            binding("User:r", AclOperation::Write, AclPermission::Allow),
        ];

        let read = effective(&bindings, "User:r", AclOperation::Read);
        let describe = effective(&bindings, "User:r", AclOperation::Describe);

        assert!(!read.allowed);
        assert_eq!(read.reason, VerdictReason::ExplicitDeny);
        assert!(describe.allowed);
        assert_eq!(
            describe.reason,
            VerdictReason::ImpliedAllow {
                via: AclOperation::Write
            }
        );
    }

    #[test]
    fn a_direct_allow_is_preferred_over_an_implied_one() {
        let bindings = [
            binding("User:x", AclOperation::Read, AclPermission::Allow),
            binding("User:x", AclOperation::Describe, AclPermission::Allow),
        ];

        let verdict = effective(&bindings, "User:x", AclOperation::Describe);

        assert_eq!(verdict.reason, VerdictReason::DirectAllow);
    }

    #[test]
    fn a_wildcard_principal_binding_applies_to_everyone() {
        let bindings = [binding(
            WILDCARD_PRINCIPAL,
            AclOperation::Read,
            AclPermission::Allow,
        )];

        let verdict = effective(&bindings, "User:anyone", AclOperation::Read);

        assert!(verdict.allowed);
        assert!(verdict.via_wildcard_principal);
    }

    #[test]
    fn a_wildcard_deny_refuses_a_named_principals_own_allow() {
        let bindings = [
            binding("User:w", AclOperation::Write, AclPermission::Allow),
            binding(WILDCARD_PRINCIPAL, AclOperation::Write, AclPermission::Deny),
        ];

        let verdict = effective(&bindings, "User:w", AclOperation::Write);

        assert!(!verdict.allowed);
        assert_eq!(verdict.reason, VerdictReason::ExplicitDeny);
        assert!(verdict.via_wildcard_principal);
    }

    // A grant naming the principal is not "via wildcard", even when a
    // wildcard binding for some other operation is also present.
    #[test]
    fn does_not_report_a_wildcard_source_for_a_directly_named_grant() {
        let bindings = [
            binding("User:w", AclOperation::Write, AclPermission::Allow),
            binding(WILDCARD_PRINCIPAL, AclOperation::Read, AclPermission::Allow),
        ];

        let verdict = effective(&bindings, "User:w", AclOperation::Write);

        assert!(verdict.allowed);
        assert!(!verdict.via_wildcard_principal);
    }

    #[test]
    fn no_bindings_at_all_denies_by_default() {
        let verdict = effective(&[], "User:w", AclOperation::Read);

        assert!(!verdict.allowed);
        assert_eq!(verdict.reason, VerdictReason::DefaultDeny);
    }

    #[test]
    fn lists_distinct_principals_in_a_stable_order() {
        let bindings = [
            binding("User:zoe", AclOperation::Read, AclPermission::Allow),
            binding("User:amy", AclOperation::Read, AclPermission::Allow),
            binding("User:zoe", AclOperation::Write, AclPermission::Allow),
        ];

        assert_eq!(principals(&bindings), vec!["User:amy", "User:zoe"]);
    }

    #[test]
    fn lists_no_principals_for_an_empty_binding_set() {
        assert!(principals(&[]).is_empty());
    }

    fn listing(bindings: Vec<AclBinding>) -> AclListing {
        AclListing {
            availability: AclAvailability::Available,
            bindings,
            binding_errors: Vec::new(),
        }
    }

    #[test]
    fn builds_a_row_per_principal_and_a_cell_per_operation() {
        let access = resource_access(
            listing(vec![
                binding("User:w", AclOperation::Write, AclPermission::Allow),
                binding("User:r", AclOperation::Read, AclPermission::Allow),
            ]),
            &AclOperation::TOPIC_COLUMNS,
        );

        assert_eq!(access.access.len(), 2);
        assert_eq!(access.access[0].principal, "User:r");
        assert_eq!(
            access.access[0].verdicts.len(),
            AclOperation::TOPIC_COLUMNS.len()
        );
    }

    // The matrix has to carry the *provenance*, not just the boolean, or the
    // UI cannot tell an implied Describe from one somebody granted.
    #[test]
    fn the_matrix_carries_why_each_cell_came_out_the_way_it_did() {
        let access = resource_access(
            listing(vec![binding(
                "User:r",
                AclOperation::Read,
                AclPermission::Allow,
            )]),
            &[AclOperation::Describe, AclOperation::Write],
        );

        let row = &access.access[0];
        assert_eq!(
            row.verdicts[0].verdict.reason,
            VerdictReason::ImpliedAllow {
                via: AclOperation::Read
            }
        );
        assert!(row.verdicts[0].verdict.allowed);
        assert_eq!(row.verdicts[1].verdict.reason, VerdictReason::DefaultDeny);
        assert!(!row.verdicts[1].verdict.allowed);
    }

    #[test]
    fn builds_an_empty_matrix_when_nothing_governs_the_resource() {
        let access = resource_access(listing(Vec::new()), &AclOperation::TOPIC_COLUMNS);

        assert!(access.access.is_empty());
        assert!(access.listing.bindings.is_empty());
    }

    // The listing travels with the matrix so the Access tab can show the
    // bindings as evidence beside the derived verdicts.
    #[test]
    fn keeps_the_bindings_alongside_the_derived_matrix() {
        let access = resource_access(
            listing(vec![binding(
                "User:w",
                AclOperation::Write,
                AclPermission::Allow,
            )]),
            &AclOperation::TOPIC_COLUMNS,
        );

        assert_eq!(access.listing.bindings.len(), 1);
        assert_eq!(access.listing.availability, AclAvailability::Available);
    }
}
