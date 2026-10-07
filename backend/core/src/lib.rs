mod acl;
mod acl_effective;
mod auth;
mod broker_liveness;
mod cluster;
mod cluster_mode;
mod connection;
mod connection_export;
mod data_migration;
mod error;
mod fetch_cancellation;
mod message;
mod message_stream;
mod publish;
mod registry;

pub use acl::{
    AclAvailability, AclBinding, AclFilter, AclListing, AclOperation, AclPermission, PatternType,
    ResourceType,
};
pub use acl_effective::{
    OperationVerdict, PrincipalAccess, ResourceAccess, Verdict, VerdictReason, WILDCARD_PRINCIPAL,
    effective, principals, resource_access,
};
pub use auth::is_auth_failure_reason;
pub use broker_liveness::{
    INTERVAL_SLACK_MS, LIVENESS_GRACE_MS, LivenessTracker, STATS_INTERVAL_MS, broker_state_is_up,
};
pub use cluster::{
    BrokerSummary, ConfigEntry, ConsumerGroupLag, ConsumerGroupSummary, PartitionLag,
    PartitionSummary, TopicSummary,
};
pub use cluster_mode::{
    ClusterVersionReport, INTER_BROKER_PROTOCOL_VERSION_CONFIG, MetadataMode, PROCESS_ROLES_CONFIG,
    cluster_version_report,
};
pub use connection::{
    Connection, ConnectionStatus, NewConnection, SaslMechanism, SecurityProtocol,
};
pub use connection_export::{
    CURRENT_EXPORT_VERSION, ConnectionExportFile, PortableConnection, partition_importable,
    select_for_export,
};
pub use data_migration::{
    AdoptedData, DB_FILE, LEGACY_DB_FILE, LEGACY_IDENTIFIER, adopt_legacy_app_data,
};
pub use error::AppError;
pub use message::{
    MessageFetchResult, MessageFilter, MessageHeader, MessagesBatchEvent, TopicMessage,
};
pub use message_stream::forward_in_batches;
pub use publish::{
    DeliveredRecord, EncodedRecord, MAX_PUBLISH_BATCH_BYTES, MAX_PUBLISH_BATCH_MESSAGES,
    NewPublishMessage, PayloadEncoding, PublishFailure, PublishFailureKind, PublishField,
    PublishHeaderInput, PublishLimits, PublishOutcome, PublishRefusal, encode_messages,
    publish_refusal,
};
pub use registry::MAX_AUTH_ATTEMPTS;

/// A fallible result carrying an `error_stack::Report`.
///
/// error-stack 0.8 removed its own `Result` alias, and every crate here used
/// it in public signatures. Defining it once in the workspace's shared crate
/// keeps those signatures reading exactly as they did, rather than spelling
/// out `core::result::Result<T, Report<C>>` at a hundred call sites or
/// redefining the same alias in six crates.
pub type Result<T, C> = core::result::Result<T, error_stack::Report<C>>;
pub use fetch_cancellation::FetchCancellations;
pub use registry::ConnectionRegistry;
