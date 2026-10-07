//! The authorization-refusal path of `detect_cluster_version`.
//!
//! This is the branch the whole error policy rests on: librdkafka reports a
//! denied `DescribeConfigs` as an *empty, successful* result, so a principal
//! who may not read broker configs must come back as
//! `Ok(MetadataMode::Unknown)` with a note — never `Err`. Nothing else can
//! show that. The ordinary e2e broker has no authorizer, so `reader` would
//! read configs happily there and the test would pass for the wrong reason —
//! the same trap `publish_authorization.rs` documents.
//!
//! ```bash
//! scripts/e2e-acl-fixtures.sh
//! SALTY_E2E_ACL_BOOTSTRAP=localhost:9192 \
//!   cargo test -p salty-kafka --test cluster_mode_authorization
//! ```

use std::time::Duration;

use salty_core::{MetadataMode, SaslMechanism, SecurityProtocol};
use salty_kafka::{BrokerSslConfig, KafkaClient, RdKafkaClient};

const READ_TIMEOUT: Duration = Duration::from_secs(30);

fn acl_bootstrap() -> Option<String> {
    std::env::var("SALTY_E2E_ACL_BOOTSTRAP")
        .ok()
        .filter(|value| !value.is_empty())
}

macro_rules! acl_broker {
    () => {
        match acl_bootstrap() {
            Some(bootstrap) => bootstrap,
            None => {
                eprintln!("skipped: set SALTY_E2E_ACL_BOOTSTRAP to run this test");
                return;
            }
        }
    };
}

async fn detect_as(
    bootstrap: &str,
    user: &str,
    password: &str,
) -> salty_core::ClusterVersionReport {
    RdKafkaClient::new()
        .detect_cluster_version(
            bootstrap,
            SecurityProtocol::SaslPlaintext,
            Some(SaslMechanism::Plain),
            Some(user),
            Some(password),
            BrokerSslConfig::default(),
            READ_TIMEOUT,
        )
        .await
        .expect("a refusal is a successful answer, not an error")
}

#[tokio::test]
async fn a_principal_who_may_not_read_broker_configs_gets_unknown_not_an_error() {
    let bootstrap = acl_broker!();
    // `reader` holds Describe+Read on topics only — no DescribeConfigs on the
    // cluster. The `.expect` in `detect_as` is itself half the assertion.
    let report = detect_as(&bootstrap, "reader", "reader-secret").await;

    assert_eq!(report.mode, MetadataMode::Unknown, "report was: {report:?}");
    assert_eq!(report.suggested_version, None);
    let note = report.note.expect("an unknown mode must explain itself");
    assert!(note.contains("DescribeConfigs"), "note was: {note}");
}

#[tokio::test]
async fn the_super_user_reads_the_same_cluster_fine() {
    // The control: same broker, same call, a principal that *may* read
    // configs. Without this, the test above would pass just as well against
    // a broker that was simply unreachable.
    //
    // Asserting the real values rather than merely `!= Unknown`: that weaker
    // form would still pass if a mode were ever reported with no data behind
    // it. It cannot be today — the mode is derived from `process_roles`'s
    // presence — but the control is the wrong place to lean on that.
    //
    // `admin` is the only principal that works here: the fixture script
    // grants `writer` and `reader` topic-level ACLs only, so neither can read
    // cluster configs either.
    let bootstrap = acl_broker!();
    let report = detect_as(&bootstrap, "admin", "admin-secret").await;

    assert_eq!(report.mode, MetadataMode::Kraft, "report was: {report:?}");
    let roles = report
        .process_roles
        .as_deref()
        .expect("a super user may read process.roles");
    assert!(!roles.trim().is_empty(), "process.roles was: {roles:?}");
}
