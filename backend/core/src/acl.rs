//! Kafka ACL bindings: the wire types the Access Control views read, and the
//! classification of what a `DescribeAcls` response actually told us.
//!
//! **Why these live here rather than in `backend/kafka`.** The enums below
//! are the vocabulary of the whole feature — the command layer, the FFI
//! wrapper and the frontend all speak them — and every one of them has a
//! numeric wire value that has to survive a round trip through librdkafka's C
//! enums without drifting. `src-tauri` cannot be built here and
//! `backend/kafka` needs a real broker, so the conversions are put where
//! `cargo test` can check them unaided. Same reasoning as `publish_refusal`.

use serde::{Deserialize, Serialize};

/// What kind of thing an ACL binding governs.
///
/// The discriminants are librdkafka's own (`RD_KAFKA_RESOURCE_*`), so a
/// binding read off the wire and one written back describe the same resource.
/// `Broker` is what librdkafka calls the cluster resource — Kafka's own tools
/// and documentation say "Cluster" for the same thing, and the UI follows
/// Kafka rather than librdkafka because that is the word a user's ACL
/// commands are written in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ResourceType {
    Unknown,
    /// Only meaningful in a filter: matches every resource type.
    Any,
    Topic,
    Group,
    /// The cluster itself. Kafka calls this `Cluster`; librdkafka calls it
    /// `BROKER`.
    Broker,
    TransactionalId,
}

impl ResourceType {
    /// librdkafka's numeric value for this type.
    pub fn as_wire(self) -> i32 {
        match self {
            ResourceType::Unknown => 0,
            ResourceType::Any => 1,
            ResourceType::Topic => 2,
            ResourceType::Group => 3,
            ResourceType::Broker => 4,
            ResourceType::TransactionalId => 5,
        }
    }

    /// Reads librdkafka's numeric value back. An unrecognised value becomes
    /// [`ResourceType::Unknown`] rather than panicking: a newer broker may
    /// name a resource kind this build has never heard of, and dropping the
    /// whole listing over one such binding would be a worse outcome than
    /// showing it as unknown.
    pub fn from_wire(value: i32) -> Self {
        match value {
            1 => ResourceType::Any,
            2 => ResourceType::Topic,
            3 => ResourceType::Group,
            4 => ResourceType::Broker,
            5 => ResourceType::TransactionalId,
            _ => ResourceType::Unknown,
        }
    }

    /// The name Kafka's own tooling uses, which is what the UI shows.
    pub fn label(self) -> &'static str {
        match self {
            ResourceType::Unknown => "Unknown",
            ResourceType::Any => "Any",
            ResourceType::Topic => "Topic",
            ResourceType::Group => "Group",
            ResourceType::Broker => "Cluster",
            ResourceType::TransactionalId => "TransactionalId",
        }
    }
}

/// How a binding's resource name is matched against real resource names.
///
/// This is the attribute the UI makes most prominent, because it is the one
/// that decides whether a grant covers resources that do not exist yet: a
/// `Prefixed` binding on `payments-` governs `payments-retry` from the moment
/// someone creates it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PatternType {
    Unknown,
    /// Only meaningful in a filter: matches bindings of any pattern type.
    Any,
    /// Only meaningful in a filter: asks the **broker** to return every
    /// binding whose pattern governs the given name — literal, prefixed and
    /// wildcard alike.
    ///
    /// This is why the app does no pattern matching of its own. See
    /// `acl_effective`.
    Match,
    /// The name matches exactly. The literal name `*` is Kafka's wildcard and
    /// matches every resource of the type.
    Literal,
    /// Every resource whose name starts with this one.
    Prefixed,
}

impl PatternType {
    pub fn as_wire(self) -> i32 {
        match self {
            PatternType::Unknown => 0,
            PatternType::Any => 1,
            PatternType::Match => 2,
            PatternType::Literal => 3,
            PatternType::Prefixed => 4,
        }
    }

    /// See [`ResourceType::from_wire`] for why an unknown value degrades
    /// rather than panics.
    pub fn from_wire(value: i32) -> Self {
        match value {
            1 => PatternType::Any,
            2 => PatternType::Match,
            3 => PatternType::Literal,
            4 => PatternType::Prefixed,
            _ => PatternType::Unknown,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            PatternType::Unknown => "UNKNOWN",
            PatternType::Any => "ANY",
            PatternType::Match => "MATCH",
            PatternType::Literal => "LITERAL",
            PatternType::Prefixed => "PREFIXED",
        }
    }
}

/// An operation an ACL binding grants or denies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AclOperation {
    Unknown,
    /// Only meaningful in a filter.
    Any,
    /// Grants every operation on the resource.
    All,
    Read,
    Write,
    Create,
    Delete,
    Alter,
    Describe,
    ClusterAction,
    DescribeConfigs,
    AlterConfigs,
    IdempotentWrite,
}

impl AclOperation {
    pub fn as_wire(self) -> i32 {
        match self {
            AclOperation::Unknown => 0,
            AclOperation::Any => 1,
            AclOperation::All => 2,
            AclOperation::Read => 3,
            AclOperation::Write => 4,
            AclOperation::Create => 5,
            AclOperation::Delete => 6,
            AclOperation::Alter => 7,
            AclOperation::Describe => 8,
            AclOperation::ClusterAction => 9,
            AclOperation::DescribeConfigs => 10,
            AclOperation::AlterConfigs => 11,
            AclOperation::IdempotentWrite => 12,
        }
    }

    /// See [`ResourceType::from_wire`] for why an unknown value degrades
    /// rather than panics.
    pub fn from_wire(value: i32) -> Self {
        match value {
            1 => AclOperation::Any,
            2 => AclOperation::All,
            3 => AclOperation::Read,
            4 => AclOperation::Write,
            5 => AclOperation::Create,
            6 => AclOperation::Delete,
            7 => AclOperation::Alter,
            8 => AclOperation::Describe,
            9 => AclOperation::ClusterAction,
            10 => AclOperation::DescribeConfigs,
            11 => AclOperation::AlterConfigs,
            12 => AclOperation::IdempotentWrite,
            _ => AclOperation::Unknown,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            AclOperation::Unknown => "Unknown",
            AclOperation::Any => "Any",
            AclOperation::All => "All",
            AclOperation::Read => "Read",
            AclOperation::Write => "Write",
            AclOperation::Create => "Create",
            AclOperation::Delete => "Delete",
            AclOperation::Alter => "Alter",
            AclOperation::Describe => "Describe",
            AclOperation::ClusterAction => "ClusterAction",
            AclOperation::DescribeConfigs => "DescribeConfigs",
            AclOperation::AlterConfigs => "AlterConfigs",
            AclOperation::IdempotentWrite => "IdempotentWrite",
        }
    }

    /// The operations a topic's Access tab shows a column for.
    ///
    /// Not every operation in the enum: `ClusterAction` is broker-to-broker
    /// and never granted to a user, and the filter-only values mean nothing
    /// on a real binding. Ordered as a reader thinks about them — what can
    /// you see, what can you consume, what can you produce, what can you
    /// change — rather than by the numeric wire order.
    pub const TOPIC_COLUMNS: [AclOperation; 6] = [
        AclOperation::Describe,
        AclOperation::Read,
        AclOperation::Write,
        AclOperation::Delete,
        AclOperation::Alter,
        AclOperation::DescribeConfigs,
    ];

    /// The operations a consumer group's Access tab shows a column for.
    /// A group can be described, read (joined), and deleted; nothing else
    /// applies.
    pub const GROUP_COLUMNS: [AclOperation; 3] =
        [AclOperation::Describe, AclOperation::Read, AclOperation::Delete];
}

/// Whether a binding grants or refuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AclPermission {
    Unknown,
    /// Only meaningful in a filter.
    Any,
    Deny,
    Allow,
}

impl AclPermission {
    pub fn as_wire(self) -> i32 {
        match self {
            AclPermission::Unknown => 0,
            AclPermission::Any => 1,
            AclPermission::Deny => 2,
            AclPermission::Allow => 3,
        }
    }

    /// See [`ResourceType::from_wire`] for why an unknown value degrades
    /// rather than panics.
    pub fn from_wire(value: i32) -> Self {
        match value {
            1 => AclPermission::Any,
            2 => AclPermission::Deny,
            3 => AclPermission::Allow,
            _ => AclPermission::Unknown,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            AclPermission::Unknown => "Unknown",
            AclPermission::Any => "Any",
            AclPermission::Deny => "Deny",
            AclPermission::Allow => "Allow",
        }
    }
}

/// One ACL binding, exactly as the broker reported it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AclBinding {
    pub resource_type: ResourceType,
    /// The resource name or pattern. Read together with `pattern_type`: the
    /// same string means different things under `Literal` and `Prefixed`.
    pub resource_name: String,
    pub pattern_type: PatternType,
    /// Kafka's principal string, conventionally `User:alice`. `User:*` is the
    /// any-principal wildcard.
    pub principal: String,
    /// The host the binding applies from; `*` for any.
    pub host: String,
    pub operation: AclOperation,
    pub permission: AclPermission,
}

/// What a `DescribeAcls` call was actually able to tell us.
///
/// An empty ACL listing is dangerously ambiguous — it can mean "this cluster
/// is unsecured and everyone may do anything" or "everything is denied" or
/// "you are not allowed to look", which are not merely different but
/// opposite. Carrying the interpretation alongside the bindings is why
/// [`AclListing`] exists rather than a bare `Vec`.
///
/// # Why there is no `NotPermitted`
///
/// The original design had one, classified from the response's error code.
/// That is not implementable against librdkafka 2.12.1:
/// `rd_kafka_DescribeAclsResponse_parse` reads the response's `error_code`,
/// uses it only to reassign a local `errstr` pointer — which does nothing
/// observable to the caller — and then returns
/// `RD_KAFKA_RESP_ERR_NO_ERROR` unconditionally. A
/// `CLUSTER_AUTHORIZATION_FAILED` and a `SECURITY_DISABLED` both reach the
/// client as a *successful, empty* result, verified against real brokers in
/// `backend/kafka/tests/acl_describe.rs`.
///
/// So the distinction is recovered where it still can be — from the broker's
/// own `authorizer.class.name`, read via `DescribeConfigs`, which does
/// propagate errors properly — and where it cannot be recovered, it is
/// reported as [`AclAvailability::Indeterminate`] rather than guessed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AclAvailability {
    /// The broker reports no authorizer class, so ACLs are not consulted at
    /// all and every principal may do anything. An empty listing here means
    /// "unrestricted", not "nothing granted". Definitive.
    NoAuthorizer,
    /// The broker returned ACL bindings, or confirmed there are none to
    /// return. An empty listing here means nothing is granted — which, under
    /// `allow.everyone.if.no.acl.found=false`, denies everything.
    Available,
    /// The broker returned nothing and the client cannot tell why: either no
    /// ACLs are defined, or this principal lacks `Describe` on `Cluster` and
    /// the refusal was discarded by librdkafka before it reached us.
    ///
    /// Shown to the user as exactly that — an honest "cannot determine" —
    /// because both readings are plausible and they imply opposite things
    /// about the cluster.
    Indeterminate,
}

/// librdkafka's `RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED`.
const ERR_CLUSTER_AUTHORIZATION_FAILED: i32 = 31;
/// librdkafka's `RD_KAFKA_RESP_ERR_SECURITY_DISABLED`.
const ERR_SECURITY_DISABLED: i32 = 54;

impl AclAvailability {
    /// Classifies a `DescribeAcls` response by its **error code**.
    ///
    /// Deliberately not by matching librdkafka's message text: that string is
    /// a human-facing description which has changed between librdkafka
    /// releases, and the difference between "this cluster is unsecured" and
    /// "you are not allowed to look" is too important to hang on it.
    ///
    /// Returns `None` for codes that are genuine failures — a timeout, a
    /// transport error, a broker that will not answer. Those stay in the
    /// `Result`'s error channel and surface as a category load failure; only
    /// the two codes below are facts about the cluster rather than failures
    /// of the call.
    pub fn from_error_code(code: i32) -> Option<Self> {
        match code {
            0 => Some(AclAvailability::Available),
            ERR_SECURITY_DISABLED => Some(AclAvailability::NoAuthorizer),
            ERR_CLUSTER_AUTHORIZATION_FAILED => Some(AclAvailability::Indeterminate),
            _ => None,
        }
    }

    /// Interprets an **empty** ACL listing using the broker's own
    /// `authorizer.class.name`.
    ///
    /// This is the fallback that exists because librdkafka discards the
    /// DescribeAcls error code (see the type's own docs). `DescribeConfigs`
    /// is used instead because it *does* report its failures, so a principal
    /// that may not read broker configs produces `None` here rather than a
    /// false answer.
    ///
    /// - `Some("")` — the broker has no authorizer. Unrestricted, definitive.
    /// - `Some(class)` — an authorizer is running, so an empty listing is
    ///   either a genuinely empty policy or a refusal we cannot see. Note
    ///   that reading configs needs `DescribeConfigs` on `Cluster` while
    ///   listing ACLs needs `Describe` on `Cluster` — holding one does not
    ///   imply holding the other, which is exactly why this does not
    ///   conclude "Available" and quietly claim the policy is empty.
    /// - `None` — the config could not be read, so nothing can be concluded.
    pub fn for_empty_listing(authorizer_class: Option<&str>) -> Self {
        match authorizer_class {
            Some(class) if class.trim().is_empty() => AclAvailability::NoAuthorizer,
            _ => AclAvailability::Indeterminate,
        }
    }
}

/// The result of a `DescribeAcls`: what the broker said, and what it means.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AclListing {
    pub availability: AclAvailability,
    pub bindings: Vec<AclBinding>,
    /// Bindings the broker returned carrying an error of their own
    /// (`rd_kafka_AclBinding_error`). Reported rather than allowed to fail
    /// the whole listing — one bad entry should not hide the good ones.
    #[serde(default)]
    pub binding_errors: Vec<String>,
}

impl AclListing {
    /// The listing for a cluster that is not running an authorizer.
    pub fn no_authorizer() -> Self {
        AclListing {
            availability: AclAvailability::NoAuthorizer,
            bindings: Vec::new(),
            binding_errors: Vec::new(),
        }
    }

    /// The listing for an empty result whose meaning could not be pinned
    /// down — see [`AclAvailability::Indeterminate`].
    pub fn indeterminate() -> Self {
        AclListing {
            availability: AclAvailability::Indeterminate,
            bindings: Vec::new(),
            binding_errors: Vec::new(),
        }
    }
}

/// Which bindings to ask the broker for.
///
/// Every field is optional in the Kafka sense — the filter-only `Any` values,
/// and `None` for a name/principal/host, mean "do not narrow on this".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclFilter {
    pub resource_type: ResourceType,
    pub resource_name: Option<String>,
    pub pattern_type: PatternType,
    pub principal: Option<String>,
    pub host: Option<String>,
    pub operation: AclOperation,
    pub permission: AclPermission,
}

impl AclFilter {
    /// Every ACL the broker will show us — what the tree's Access Control
    /// category lists.
    pub fn any() -> Self {
        AclFilter {
            resource_type: ResourceType::Any,
            resource_name: None,
            pattern_type: PatternType::Any,
            principal: None,
            host: None,
            operation: AclOperation::Any,
            permission: AclPermission::Any,
        }
    }

    /// Every ACL that **governs** one named resource.
    ///
    /// The pattern type is [`PatternType::Match`], which is the whole reason
    /// this app does no pattern matching of its own: MATCH asks the broker to
    /// resolve literal, prefixed and wildcard patterns against the name, so
    /// the set of bindings that apply comes from the authorizer that will
    /// actually enforce them rather than from a reimplementation here.
    pub fn governing(resource_type: ResourceType, resource_name: impl Into<String>) -> Self {
        AclFilter {
            resource_type,
            resource_name: Some(resource_name.into()),
            pattern_type: PatternType::Match,
            principal: None,
            host: None,
            operation: AclOperation::Any,
            permission: AclPermission::Any,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_type_round_trips_through_its_wire_value() {
        for value in [
            ResourceType::Any,
            ResourceType::Topic,
            ResourceType::Group,
            ResourceType::Broker,
            ResourceType::TransactionalId,
        ] {
            assert_eq!(ResourceType::from_wire(value.as_wire()), value);
        }
    }

    #[test]
    fn pattern_type_round_trips_through_its_wire_value() {
        for value in [
            PatternType::Any,
            PatternType::Match,
            PatternType::Literal,
            PatternType::Prefixed,
        ] {
            assert_eq!(PatternType::from_wire(value.as_wire()), value);
        }
    }

    #[test]
    fn operation_round_trips_through_its_wire_value() {
        for value in [
            AclOperation::Any,
            AclOperation::All,
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::Create,
            AclOperation::Delete,
            AclOperation::Alter,
            AclOperation::Describe,
            AclOperation::ClusterAction,
            AclOperation::DescribeConfigs,
            AclOperation::AlterConfigs,
            AclOperation::IdempotentWrite,
        ] {
            assert_eq!(AclOperation::from_wire(value.as_wire()), value);
        }
    }

    #[test]
    fn permission_round_trips_through_its_wire_value() {
        for value in [AclPermission::Any, AclPermission::Deny, AclPermission::Allow] {
            assert_eq!(AclPermission::from_wire(value.as_wire()), value);
        }
    }

    // A broker newer than this build can name a resource kind, pattern type
    // or operation we have never heard of. Degrading that one binding to
    // Unknown keeps the rest of the listing readable; panicking or erroring
    // would throw away everything the user *can* see over one entry.
    #[test]
    fn an_unrecognised_wire_value_becomes_unknown_rather_than_failing() {
        assert_eq!(ResourceType::from_wire(99), ResourceType::Unknown);
        assert_eq!(PatternType::from_wire(99), PatternType::Unknown);
        assert_eq!(AclOperation::from_wire(99), AclOperation::Unknown);
        assert_eq!(AclPermission::from_wire(99), AclPermission::Unknown);
    }

    // The three states the whole feature is built around. Keyed off the
    // numeric code because librdkafka's message text is not a stable API.
    #[test]
    fn classifies_security_disabled_as_no_authorizer() {
        assert_eq!(
            AclAvailability::from_error_code(54),
            Some(AclAvailability::NoAuthorizer)
        );
    }

    // Kept as a defensive path even though librdkafka 2.12.1 never delivers
    // this code for DescribeAcls: if a later release starts propagating it,
    // the classification is already correct rather than silently ignored.
    #[test]
    fn classifies_cluster_authorization_failed_as_indeterminate() {
        assert_eq!(
            AclAvailability::from_error_code(31),
            Some(AclAvailability::Indeterminate)
        );
    }

    // The fallback that actually runs today, because the error code above is
    // discarded before it reaches us.
    #[test]
    fn reads_an_empty_authorizer_class_as_no_authorizer() {
        assert_eq!(
            AclAvailability::for_empty_listing(Some("")),
            AclAvailability::NoAuthorizer
        );
        assert_eq!(
            AclAvailability::for_empty_listing(Some("   ")),
            AclAvailability::NoAuthorizer
        );
    }

    // An authorizer is running, so an empty listing could be an empty policy
    // or a refusal we cannot see. Claiming "Available" here would assert the
    // policy is empty on the strength of a *different* ACL than the one that
    // governs the listing.
    #[test]
    fn refuses_to_conclude_anything_from_an_empty_listing_under_an_authorizer() {
        assert_eq!(
            AclAvailability::for_empty_listing(Some(
                "org.apache.kafka.metadata.authorizer.StandardAuthorizer"
            )),
            AclAvailability::Indeterminate
        );
    }

    #[test]
    fn reports_indeterminate_when_the_broker_config_could_not_be_read() {
        assert_eq!(
            AclAvailability::for_empty_listing(None),
            AclAvailability::Indeterminate
        );
    }

    #[test]
    fn classifies_no_error_as_available() {
        assert_eq!(
            AclAvailability::from_error_code(0),
            Some(AclAvailability::Available)
        );
    }

    // Anything else is a real failure — a timeout, a dead socket — and must
    // stay in the error channel rather than being dressed up as one of the
    // three meaningful states.
    #[test]
    fn leaves_any_other_error_code_unclassified() {
        assert_eq!(AclAvailability::from_error_code(7), None);
        assert_eq!(AclAvailability::from_error_code(-185), None);
    }

    #[test]
    fn the_governing_filter_asks_the_broker_to_match_patterns() {
        let filter = AclFilter::governing(ResourceType::Topic, "orders");

        assert_eq!(filter.pattern_type, PatternType::Match);
        assert_eq!(filter.resource_name.as_deref(), Some("orders"));
        // Not narrowed to a principal: the topic's Access tab asks "who can
        // touch this", which is every principal.
        assert_eq!(filter.principal, None);
    }

    #[test]
    fn the_any_filter_narrows_on_nothing() {
        let filter = AclFilter::any();

        assert_eq!(filter.resource_type, ResourceType::Any);
        assert_eq!(filter.pattern_type, PatternType::Any);
        assert_eq!(filter.operation, AclOperation::Any);
        assert_eq!(filter.permission, AclPermission::Any);
        assert_eq!(filter.resource_name, None);
    }

    // The UI shows Kafka's vocabulary, not librdkafka's: a user's ACLs were
    // written with kafka-acls.sh, which says --cluster, never --broker.
    #[test]
    fn labels_the_cluster_resource_the_way_kafka_does() {
        assert_eq!(ResourceType::Broker.label(), "Cluster");
    }
}
