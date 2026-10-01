use serde::{Deserialize, Serialize};

/// The broker config naming the KRaft roles a node runs. Absent entirely
/// before Kafka 2.8; present but empty on a 3.x node running in ZooKeeper
/// mode; non-empty (`broker`, `controller`, or both) under KRaft.
pub const PROCESS_ROLES_CONFIG: &str = "process.roles";

/// The broker config naming the inter-broker protocol version, e.g.
/// `4.1-IV0`. Read as a *hint* at the cluster's version — see
/// [`ClusterVersionReport::suggested_version`].
pub const INTER_BROKER_PROTOCOL_VERSION_CONFIG: &str = "inter.broker.protocol.version";

/// How a cluster stores its metadata, as the broker itself reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MetadataMode {
    /// KRaft: metadata lives in the controller quorum. The only mode from
    /// Kafka 4.0 on.
    Kraft,
    /// ZooKeeper-backed metadata. Possible up to and including 3.9.
    Zookeeper,
    /// The broker did not return the configs this is derived from — which
    /// is also what a principal lacking `DescribeConfigs` on the cluster
    /// sees, since that refusal comes back as an empty result rather than
    /// as an error.
    Unknown,
}

/// What the New Connection modal's Detect button learned from the cluster.
///
/// Every field is optional because this is assembled from a broker's answer
/// to `DescribeConfigs`, and a principal who may not read broker configs
/// gets an *empty, successful* result back rather than a refusal — so
/// "could not tell" is a normal outcome, not an error. `note` is what the
/// UI shows the user in that case.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterVersionReport {
    pub mode: MetadataMode,
    /// `process.roles` exactly as the broker reported it, when readable.
    pub process_roles: Option<String>,
    /// `inter.broker.protocol.version` exactly as reported, e.g. `4.1-IV0`.
    pub inter_broker_protocol_version: Option<String>,
    /// The `major.minor` the version dropdown should be set to, derived from
    /// `inter_broker_protocol_version`.
    ///
    /// A *suggestion*, not an assertion, and deliberately not checked
    /// against the list of versions the UI offers — keeping that list in one
    /// place (the frontend) rather than duplicating it here. A value outside
    /// the list still works end to end: the dropdown appends an unlisted
    /// version as its own option, and the KRaft gate keys on the major
    /// component alone.
    pub suggested_version: Option<String>,
    /// Why the answer is partial or qualified, shown verbatim by the UI.
    pub note: Option<String>,
}

/// Derives a report from the two broker configs it is built on.
///
/// Pure, and kept here rather than in `salty-kafka` for the same reason
/// `publish_refusal` and `acl_effective` are: every branch below is
/// reachable in a unit test, where a broker is not.
///
/// The mode rules, in order:
///
/// | `process.roles` | `inter.broker.protocol.version` | Mode |
/// |---|---|---|
/// | non-empty | any | `Kraft` |
/// | present, empty | any | `Zookeeper` |
/// | absent | >= 4.0 | `Kraft` — no 4.x broker can run ZooKeeper |
/// | absent | < 4.0 | `Zookeeper` — the config predates 2.8 |
/// | absent | absent/unparseable | `Unknown` |
pub fn cluster_version_report(
    process_roles: Option<&str>,
    inter_broker_protocol_version: Option<&str>,
) -> ClusterVersionReport {
    let suggested_version = inter_broker_protocol_version.and_then(suggested_version_from);

    let mode = match process_roles {
        Some(roles) if !roles.trim().is_empty() => MetadataMode::Kraft,
        Some(_) => MetadataMode::Zookeeper,
        None => match inter_broker_protocol_version.and_then(major_version_from) {
            Some(major) if major >= 4 => MetadataMode::Kraft,
            Some(_) => MetadataMode::Zookeeper,
            None => MetadataMode::Unknown,
        },
    };

    let note = match mode {
        MetadataMode::Unknown => Some(
            "The broker returned neither process.roles nor inter.broker.protocol.version. \
             That is also what a principal without DescribeConfigs on the cluster sees, \
             because the broker reports that refusal as an empty result rather than as an \
             error — so choose the version manually."
                .to_string(),
        ),
        // On a KRaft cluster the authoritative metadata level is
        // metadata.version, read via DescribeFeatures, which rdkafka's safe
        // wrapper does not expose. inter.broker.protocol.version is not
        // authoritative there, so say so rather than present it as fact.
        MetadataMode::Kraft if suggested_version.is_some() => Some(
            "On a KRaft cluster the authoritative metadata level is metadata.version, which \
             this client cannot read, so the version was derived from \
             inter.broker.protocol.version and is a suggestion."
                .to_string(),
        ),
        MetadataMode::Kraft | MetadataMode::Zookeeper => None,
    };

    ClusterVersionReport {
        mode,
        process_roles: process_roles.map(str::to_string),
        inter_broker_protocol_version: inter_broker_protocol_version.map(str::to_string),
        suggested_version,
        note,
    }
}

/// `"4.1-IV0"` -> `"4.1"`, `"0.11.0-IV2"` -> `"0.11"`. `None` for anything
/// without two leading numeric components, which is what the dropdown's
/// values are shaped like.
fn suggested_version_from(raw: &str) -> Option<String> {
    let base = raw.split('-').next()?.trim();
    let mut parts = base.split('.');
    let major = parts.next()?;
    let minor = parts.next()?;
    let numeric = |part: &str| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit());
    (numeric(major) && numeric(minor)).then(|| format!("{major}.{minor}"))
}

/// The leading major component of a protocol version, for the "no 4.x
/// broker can run ZooKeeper" rule.
fn major_version_from(raw: &str) -> Option<u32> {
    raw.split('-').next()?.trim().split('.').next()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_empty_process_roles_means_kraft() {
        let report = cluster_version_report(Some("broker,controller"), Some("4.1-IV0"));
        assert_eq!(report.mode, MetadataMode::Kraft);
        assert_eq!(report.process_roles.as_deref(), Some("broker,controller"));
        assert_eq!(report.suggested_version.as_deref(), Some("4.1"));
    }

    #[test]
    fn empty_process_roles_means_zookeeper() {
        // A 3.x broker running in ZooKeeper mode returns the config with an
        // empty value, not absent.
        let report = cluster_version_report(Some(""), Some("3.9-IV0"));
        assert_eq!(report.mode, MetadataMode::Zookeeper);
        assert_eq!(report.suggested_version.as_deref(), Some("3.9"));
        assert_eq!(report.note, None);
    }

    #[test]
    fn whitespace_only_process_roles_means_zookeeper() {
        let report = cluster_version_report(Some("   "), Some("3.6-IV2"));
        assert_eq!(report.mode, MetadataMode::Zookeeper);
    }

    #[test]
    fn absent_process_roles_with_a_4_x_protocol_version_means_kraft() {
        // process.roles did not exist before 2.8, but no 4.x broker can run
        // ZooKeeper, so a 4.x protocol version settles it on its own.
        let report = cluster_version_report(None, Some("4.0-IV3"));
        assert_eq!(report.mode, MetadataMode::Kraft);
        assert_eq!(report.process_roles, None);
    }

    #[test]
    fn absent_process_roles_with_an_older_protocol_version_means_zookeeper() {
        let report = cluster_version_report(None, Some("2.4-IV1"));
        assert_eq!(report.mode, MetadataMode::Zookeeper);
        assert_eq!(report.suggested_version.as_deref(), Some("2.4"));
    }

    #[test]
    fn neither_config_readable_is_unknown_and_says_why() {
        // This is also exactly the shape a denied DescribeConfigs takes: the
        // call returns no entries at all.
        let report = cluster_version_report(None, None);
        assert_eq!(report.mode, MetadataMode::Unknown);
        assert_eq!(report.suggested_version, None);
        let note = report.note.expect("an unknown mode must explain itself");
        assert!(note.contains("DescribeConfigs"), "note was: {note}");
    }

    #[test]
    fn a_kraft_report_flags_its_version_as_a_suggestion() {
        // metadata.version is the authority on a KRaft cluster and this
        // client cannot read it, so the figure offered is a hint.
        let report = cluster_version_report(Some("broker"), Some("4.1-IV0"));
        let note = report.note.expect("a kraft version must be flagged as derived");
        assert!(note.contains("metadata.version"), "note was: {note}");
    }

    #[test]
    fn a_kraft_report_with_no_version_has_nothing_to_qualify() {
        let report = cluster_version_report(Some("broker"), None);
        assert_eq!(report.mode, MetadataMode::Kraft);
        assert_eq!(report.suggested_version, None);
        assert_eq!(report.note, None);
    }

    #[test]
    fn strips_the_protocol_version_suffix_down_to_major_minor() {
        assert_eq!(suggested_version_from("4.1-IV0").as_deref(), Some("4.1"));
        assert_eq!(suggested_version_from("3.9-IV0").as_deref(), Some("3.9"));
        assert_eq!(suggested_version_from("2.8").as_deref(), Some("2.8"));
        // Pre-1.0 protocol versions carry a third component; the dropdown
        // offers "0.11", so the first two are what matter.
        assert_eq!(suggested_version_from("0.11.0-IV2").as_deref(), Some("0.11"));
    }

    #[test]
    fn rejects_a_protocol_version_it_cannot_read() {
        assert_eq!(suggested_version_from(""), None);
        assert_eq!(suggested_version_from("4"), None);
        assert_eq!(suggested_version_from("latest"), None);
        assert_eq!(suggested_version_from("x.y"), None);
    }

    #[test]
    fn serializes_for_the_frontend_as_camel_case() {
        let report = cluster_version_report(Some("broker"), Some("4.1-IV0"));
        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("\"mode\":\"kraft\""), "json was: {json}");
        assert!(json.contains("\"processRoles\":\"broker\""), "json was: {json}");
        assert!(json.contains("\"interBrokerProtocolVersion\":\"4.1-IV0\""), "json was: {json}");
        assert!(json.contains("\"suggestedVersion\":\"4.1\""), "json was: {json}");
    }

    #[test]
    fn zookeeper_mode_serializes_as_zookeeper() {
        let report = cluster_version_report(Some(""), Some("3.9-IV0"));
        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("\"mode\":\"zookeeper\""), "json was: {json}");
    }

    // The four below were added after review found them reachable but
    // unexercised by the original test list.

    #[test]
    fn unknown_mode_serializes_as_unknown() {
        // The frontend's MetadataMode union is written against exactly
        // "kraft" | "zookeeper" | "unknown"; this is the one variant no
        // other serialization test covers.
        let report = cluster_version_report(None, None);
        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("\"mode\":\"unknown\""), "json was: {json}");
    }

    #[test]
    fn zookeeper_inferred_from_an_old_protocol_version_carries_no_note() {
        // A distinct match arm from the present-but-empty path: this one is
        // reached with process.roles absent entirely, and must still be
        // unqualified — there is nothing tentative about it.
        let report = cluster_version_report(None, Some("2.4-IV1"));
        assert_eq!(report.mode, MetadataMode::Zookeeper);
        assert_eq!(report.note, None);
    }

    #[test]
    fn a_bare_major_protocol_version_settles_the_mode_but_offers_nothing() {
        // `major_version_from` accepts "4" while `suggested_version_from`
        // rejects it — the dropdown wants major.minor. So the mode is
        // decided and there is deliberately no version to apply, and no
        // note, because nothing was derived that needs qualifying.
        let report = cluster_version_report(None, Some("4"));
        assert_eq!(report.mode, MetadataMode::Kraft);
        assert_eq!(report.suggested_version, None);
        assert_eq!(report.note, None);
    }

    #[test]
    fn an_unparseable_protocol_version_alone_is_unknown() {
        let report = cluster_version_report(None, Some("latest"));
        assert_eq!(report.mode, MetadataMode::Unknown);
        assert_eq!(report.suggested_version, None);
    }
}
