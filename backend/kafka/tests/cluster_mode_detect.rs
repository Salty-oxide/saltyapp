//! `detect_cluster_version` against a real broker.
//!
//! The unit test beside `client.rs` can only reach the failure path (a
//! closed port). Decoding an actual `DescribeConfigs` response — which is
//! the whole point of this call — needs a broker.
//!
//! ```bash
//! docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0
//! SALTY_E2E_BOOTSTRAP=localhost:9092 \
//!   cargo test -p salty-kafka --test cluster_mode_detect
//! ```
//!
//! Note the fixture broker runs **KRaft**, which is what makes this worth
//! asserting: `apache/kafka:3.9.0` in KRaft mode is the exact case the
//! derivation has to get right from `process.roles` alone, without the
//! protocol version being 4.x.

use std::time::Duration;

use salty_core::{MetadataMode, SecurityProtocol};
use salty_kafka::{BrokerSslConfig, KafkaClient, RdKafkaClient};

const READ_TIMEOUT: Duration = Duration::from_secs(30);

fn bootstrap_servers() -> Option<String> {
    std::env::var("SALTY_E2E_BOOTSTRAP")
        .ok()
        .filter(|value| !value.is_empty())
}

macro_rules! broker {
    () => {
        match bootstrap_servers() {
            Some(bootstrap) => bootstrap,
            None => {
                eprintln!("skipped: set SALTY_E2E_BOOTSTRAP to run this test");
                return;
            }
        }
    };
}

async fn detect(bootstrap: &str) -> salty_core::ClusterVersionReport {
    RdKafkaClient::new()
        .detect_cluster_version(
            bootstrap,
            SecurityProtocol::Plaintext,
            None,
            None,
            None,
            BrokerSslConfig::default(),
            READ_TIMEOUT,
        )
        .await
        .expect("detection should succeed against a reachable broker")
}

#[tokio::test]
async fn reports_the_fixture_broker_as_kraft() {
    let bootstrap = broker!();
    let report = detect(&bootstrap).await;

    assert_eq!(report.mode, MetadataMode::Kraft, "report was: {report:?}");
    let roles = report
        .process_roles
        .as_deref()
        .expect("process.roles should be readable");
    assert!(!roles.trim().is_empty(), "process.roles was: {roles:?}");
}

#[tokio::test]
async fn suggests_a_version_the_dropdown_can_hold() {
    let bootstrap = broker!();
    let report = detect(&bootstrap).await;

    let suggested = report
        .suggested_version
        .as_deref()
        .expect("the broker should report inter.broker.protocol.version");
    // Shaped like the dropdown's values — `major.minor`, digits only, no
    // `-IVn` suffix left on it.
    let (major, minor) = suggested
        .split_once('.')
        .expect("suggested version should be major.minor");
    assert!(
        major.chars().all(|c| c.is_ascii_digit()),
        "suggested was: {suggested}"
    );
    assert!(
        minor.chars().all(|c| c.is_ascii_digit()),
        "suggested was: {suggested}"
    );
}

#[tokio::test]
async fn a_kraft_report_flags_its_version_as_derived() {
    let bootstrap = broker!();
    let report = detect(&bootstrap).await;

    let note = report
        .note
        .expect("a kraft report with a version must carry a note");
    assert!(note.contains("metadata.version"), "note was: {note}");
}
