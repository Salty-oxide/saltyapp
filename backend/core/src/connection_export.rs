use std::collections::HashSet;

use error_stack::{Report, ResultExt};
use crate::Result;
use serde::{Deserialize, Serialize};

use crate::connection::{Connection, NewConnection, SaslMechanism, SecurityProtocol};
use crate::error::AppError;

pub const CURRENT_EXPORT_VERSION: u32 = 1;

/// The portable subset of a `Connection`'s fields — no `id`/timestamps
/// (meaningless once moved to a different machine or database) and no
/// credentials (those never leave the OS keychain; see `Connection`'s own
/// doc comment). This is the shape written to and read from export files.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PortableConnection {
    pub name: String,
    pub bootstrap_servers: String,
    pub kafka_version: String,
    pub zookeeper_enabled: bool,
    pub zookeeper_host: Option<String>,
    pub zookeeper_port: Option<i64>,
    pub zookeeper_chroot_path: Option<String>,
    pub security_protocol: SecurityProtocol,
    pub sasl_mechanism: Option<SaslMechanism>,
    pub sasl_username: Option<String>,
    pub sasl_oauth_url: Option<String>,
    pub schema_registry_endpoint: Option<String>,
    /// Carried: an endpoint is an address, not a credential. Its basic-auth
    /// credential is deliberately absent, like every other secret here.
    pub ksqldb_endpoint: Option<String>,
    pub schema_registry_trust_store_location: Option<String>,
    pub schema_registry_keystore_location: Option<String>,
    pub ssl_truststore_location: Option<String>,
    pub ssl_keystore_location: Option<String>,
}

impl From<&Connection> for PortableConnection {
    fn from(connection: &Connection) -> Self {
        PortableConnection {
            name: connection.name.clone(),
            bootstrap_servers: connection.bootstrap_servers.clone(),
            kafka_version: connection.kafka_version.clone(),
            zookeeper_enabled: connection.zookeeper_enabled,
            zookeeper_host: connection.zookeeper_host.clone(),
            zookeeper_port: connection.zookeeper_port,
            zookeeper_chroot_path: connection.zookeeper_chroot_path.clone(),
            security_protocol: connection.security_protocol,
            sasl_mechanism: connection.sasl_mechanism,
            sasl_username: connection.sasl_username.clone(),
            sasl_oauth_url: connection.sasl_oauth_url.clone(),
            schema_registry_endpoint: connection.schema_registry_endpoint.clone(),
            ksqldb_endpoint: connection.ksqldb_endpoint.clone(),
            schema_registry_trust_store_location: connection.schema_registry_trust_store_location.clone(),
            schema_registry_keystore_location: connection.schema_registry_keystore_location.clone(),
            ssl_truststore_location: connection.ssl_truststore_location.clone(),
            ssl_keystore_location: connection.ssl_keystore_location.clone(),
        }
    }
}

impl From<PortableConnection> for NewConnection {
    fn from(portable: PortableConnection) -> Self {
        NewConnection {
            name: portable.name,
            bootstrap_servers: portable.bootstrap_servers,
            kafka_version: portable.kafka_version,
            zookeeper_enabled: portable.zookeeper_enabled,
            zookeeper_host: portable.zookeeper_host,
            zookeeper_port: portable.zookeeper_port,
            zookeeper_chroot_path: portable.zookeeper_chroot_path,
            security_protocol: portable.security_protocol,
            sasl_mechanism: portable.sasl_mechanism,
            sasl_username: portable.sasl_username,
            sasl_password: None,
            sasl_oauth_url: portable.sasl_oauth_url,
            schema_registry_endpoint: portable.schema_registry_endpoint,
            schema_registry_basic_auth_credentials: None,
            ksqldb_endpoint: portable.ksqldb_endpoint,
            // A secret, so it is not in the file and cannot be restored from
            // one. The importer re-enters it on their own machine.
            ksqldb_basic_auth_credentials: None,
            schema_registry_trust_store_location: portable.schema_registry_trust_store_location,
            schema_registry_trust_store_password: None,
            schema_registry_keystore_location: portable.schema_registry_keystore_location,
            schema_registry_keystore_password: None,
            schema_registry_keystore_key_password: None,
            ssl_truststore_location: portable.ssl_truststore_location,
            ssl_truststore_password: None,
            ssl_keystore_location: portable.ssl_keystore_location,
            ssl_keystore_password: None,
            ssl_keystore_key_password: None,
            // Deliberately not carried by `PortableConnection`, and so
            // deliberately false here: an export file is something users mail
            // each other, and a connection that arrives able to write to a
            // production cluster is exactly the surprise this feature must not
            // create. The importer decides to publish, on their own machine,
            // by ticking the box themselves.
            allow_publishing: false,
        }
    }
}

/// The on-disk shape of a connections export file — versioned so a future
/// format change can be detected and rejected with a clear error instead of
/// silently misreading fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionExportFile {
    /// Serialises as `saltyConnectionsVersion`.
    ///
    /// The alias is the app's previous name, and it is load-bearing: this
    /// field is the first thing [`parse`] checks, so without it every file
    /// exported before the rename would be rejected as "not a valid
    /// connections export file" — a user's saved export silently becoming
    /// unreadable because the product changed its name. Exports written from
    /// here use the new spelling; both are accepted on the way in.
    #[serde(alias = "kafkaoxideConnectionsVersion")]
    pub salty_connections_version: u32,
    pub connections: Vec<PortableConnection>,
}

impl ConnectionExportFile {
    pub fn new(connections: Vec<PortableConnection>) -> Self {
        ConnectionExportFile {
            salty_connections_version: CURRENT_EXPORT_VERSION,
            connections,
        }
    }

    pub fn to_json_pretty(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }

    pub fn parse(text: &str) -> Result<Self, AppError> {
        let file: Self = serde_json::from_str(text)
            .change_context(AppError::Validation)
            .attach("not a valid salty connections export file")?;
        if file.salty_connections_version != CURRENT_EXPORT_VERSION {
            return Err(Report::new(AppError::Validation).attach(format!(
                "unsupported connections export file version {} (expected {CURRENT_EXPORT_VERSION})",
                file.salty_connections_version
            )));
        }
        Ok(file)
    }
}

/// Selects which connections an export should include: every connection
/// when `ids` is `None` (the "export all" action), or only the matching
/// ones when `ids` is `Some` (a single connection's "Export" context-menu
/// action, or any future multi-select).
pub fn select_for_export(all: &[Connection], ids: Option<&[String]>) -> Vec<PortableConnection> {
    match ids {
        None => all.iter().map(PortableConnection::from).collect(),
        Some(ids) => all
            .iter()
            .filter(|connection| ids.contains(&connection.id))
            .map(PortableConnection::from)
            .collect(),
    }
}

/// Splits an imported file's entries into those safe to create (no existing
/// connection has the same name) and a count of the rest, which are left
/// untouched rather than overwritten or duplicated.
pub fn partition_importable<'a>(
    entries: &'a [PortableConnection],
    existing_names: &HashSet<String>,
) -> (Vec<&'a PortableConnection>, usize) {
    let mut importable = Vec::new();
    let mut skipped = 0;
    for entry in entries {
        if existing_names.contains(&entry.name) {
            skipped += 1;
        } else {
            importable.push(entry);
        }
    }
    (importable, skipped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::{Connection, NewConnection, SaslMechanism, SecurityProtocol};
    use std::collections::HashSet;

    fn sample_connection(id: &str, name: &str) -> Connection {
        Connection {
            id: id.into(),
            name: name.into(),
            bootstrap_servers: "localhost:9092".into(),
            kafka_version: "3.7".into(),
            zookeeper_enabled: true,
            zookeeper_host: Some("localhost".into()),
            zookeeper_port: Some(2181),
            zookeeper_chroot_path: Some("/kafka".into()),
            security_protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::ScramSha512),
            sasl_username: Some("alice".into()),
            sasl_password: Some("alice-secret".into()),
            sasl_oauth_url: Some("https://oauth.example.com".into()),
            schema_registry_endpoint: Some("https://schema.example.com".into()),
            schema_registry_basic_auth_credentials: Some("sr-user:sr-secret".into()),
            ksqldb_endpoint: Some("https://ksql.example.com".into()),
            ksqldb_basic_auth_credentials: Some("ksql-user:ksql-secret".into()),
            schema_registry_trust_store_location: Some("/certs/truststore.jks".into()),
            schema_registry_trust_store_password: Some("sr-ts-secret".into()),
            schema_registry_keystore_location: Some("/certs/keystore.jks".into()),
            schema_registry_keystore_password: Some("sr-ks-secret".into()),
            schema_registry_keystore_key_password: Some("sr-ks-key-secret".into()),
            ssl_truststore_location: Some("/certs/broker-truststore.jks".into()),
            ssl_truststore_password: Some("broker-ts-secret".into()),
            ssl_keystore_location: Some("/certs/broker-keystore.jks".into()),
            ssl_keystore_password: Some("broker-ks-secret".into()),
            ssl_keystore_key_password: Some("broker-ks-key-secret".into()),
            allow_publishing: true,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
        }
    }

    #[test]
    fn portable_connection_maps_every_non_secret_field_from_a_connection() {
        let connection = sample_connection("1", "Prod");

        let portable = PortableConnection::from(&connection);

        assert_eq!(portable.name, "Prod");
        assert_eq!(portable.bootstrap_servers, "localhost:9092");
        assert_eq!(portable.kafka_version, "3.7");
        assert!(portable.zookeeper_enabled);
        assert_eq!(portable.zookeeper_host.as_deref(), Some("localhost"));
        assert_eq!(portable.zookeeper_port, Some(2181));
        assert_eq!(portable.zookeeper_chroot_path.as_deref(), Some("/kafka"));
        assert_eq!(portable.security_protocol, SecurityProtocol::SaslSsl);
        assert_eq!(portable.sasl_mechanism, Some(SaslMechanism::ScramSha512));
        assert_eq!(portable.sasl_username.as_deref(), Some("alice"));
        assert_eq!(portable.sasl_oauth_url.as_deref(), Some("https://oauth.example.com"));
        assert_eq!(
            portable.schema_registry_endpoint.as_deref(),
            Some("https://schema.example.com")
        );
        assert_eq!(
            portable.schema_registry_trust_store_location.as_deref(),
            Some("/certs/truststore.jks")
        );
        assert_eq!(
            portable.schema_registry_keystore_location.as_deref(),
            Some("/certs/keystore.jks")
        );
        assert_eq!(
            portable.ssl_truststore_location.as_deref(),
            Some("/certs/broker-truststore.jks")
        );
        assert_eq!(portable.ssl_keystore_location.as_deref(), Some("/certs/broker-keystore.jks"));
    }

    #[test]
    fn an_export_never_carries_permission_to_publish() {
        // `sample_connection` has `allow_publishing: true`, so this fails if
        // the field is ever added to `PortableConnection`.
        let connection = sample_connection("1", "Prod");
        assert!(connection.allow_publishing);
        let json = serde_json::to_string(&PortableConnection::from(&connection)).unwrap();
        assert!(
            !json.contains("allowPublishing") && !json.contains("allow_publishing"),
            "an export file must not be able to grant publishing: {json}"
        );
    }

    #[test]
    fn an_imported_connection_starts_unable_to_publish() {
        let connection = sample_connection("1", "Prod");
        let imported: NewConnection = PortableConnection::from(&connection).into();
        assert!(
            !imported.allow_publishing,
            "importing a connection must never arrive publish-enabled"
        );
    }

    // An endpoint is an address, not a credential, so it is carried — while
    // the credential beside it is not. Asserted as a pair because the value of
    // each only makes sense against the other.
    #[test]
    fn portable_connection_carries_the_ksqldb_address_but_not_its_credential() {
        let portable = PortableConnection::from(&sample_connection("1", "Prod"));

        assert_eq!(
            portable.ksqldb_endpoint.as_deref(),
            Some("https://ksql.example.com")
        );
        let json = serde_json::to_string(&portable).unwrap();
        assert!(!json.contains("ksql-user"));
        assert!(!json.contains("ksql-secret"));
    }

    // An imported connection arrives able to query but not to authenticate,
    // which is the same shape every other credential here takes.
    #[test]
    fn an_imported_connection_keeps_the_ksqldb_address_and_drops_the_credential() {
        let portable = PortableConnection::from(&sample_connection("1", "Prod"));

        let imported: crate::connection::NewConnection = portable.into();

        assert_eq!(
            imported.ksqldb_endpoint.as_deref(),
            Some("https://ksql.example.com")
        );
        assert!(imported.ksqldb_basic_auth_credentials.is_none());
    }

    #[test]
    fn portable_connection_never_carries_any_secret_from_a_connection() {
        // Regression guard: `Connection` now stores real secret values
        // (plaintext DB storage), unlike before when these fields didn't
        // exist on the struct at all. `PortableConnection` — and therefore
        // the export file on disk — must still never carry them.
        let connection = sample_connection("1", "Prod");
        let json = serde_json::to_string(&PortableConnection::from(&connection)).unwrap();

        for secret in [
            "alice-secret",
            "sr-user:sr-secret",
            "ksql-user:ksql-secret",
            "sr-ts-secret",
            "sr-ks-secret",
            "sr-ks-key-secret",
            "broker-ts-secret",
            "broker-ks-secret",
            "broker-ks-key-secret",
        ] {
            assert!(!json.contains(secret), "export JSON leaked a secret value: {secret}");
        }
    }

    #[test]
    fn new_connection_from_portable_leaves_every_secret_field_none() {
        let portable = PortableConnection::from(&sample_connection("1", "Prod"));

        let new_connection: crate::connection::NewConnection = portable.into();

        assert_eq!(new_connection.name, "Prod");
        assert_eq!(new_connection.sasl_username.as_deref(), Some("alice"));
        assert!(new_connection.sasl_password.is_none());
        assert!(new_connection.schema_registry_basic_auth_credentials.is_none());
        assert!(new_connection.ksqldb_basic_auth_credentials.is_none());
        assert!(new_connection.schema_registry_trust_store_password.is_none());
        assert!(new_connection.schema_registry_keystore_password.is_none());
        assert!(new_connection.schema_registry_keystore_key_password.is_none());
        assert!(new_connection.ssl_truststore_password.is_none());
        assert!(new_connection.ssl_keystore_password.is_none());
        assert!(new_connection.ssl_keystore_key_password.is_none());
    }

    #[test]
    fn export_file_round_trips_through_json() {
        let portables = vec![
            PortableConnection::from(&sample_connection("1", "Prod")),
            PortableConnection::from(&sample_connection("2", "Staging")),
        ];
        let file = ConnectionExportFile::new(portables.clone());

        let json = file.to_json_pretty().unwrap();
        let parsed = ConnectionExportFile::parse(&json).unwrap();

        assert_eq!(parsed.connections, portables);
        assert_eq!(parsed.salty_connections_version, CURRENT_EXPORT_VERSION);
    }

    #[test]
    fn parse_rejects_a_future_export_version() {
        let json = r#"{"saltyConnectionsVersion":999,"connections":[]}"#;
        assert!(ConnectionExportFile::parse(json).is_err());
    }

    /// A file exported before the app was renamed spells the version field
    /// with the old product name. Those files sit in users' folders and must
    /// keep importing.
    #[test]
    fn parse_accepts_an_export_written_under_the_old_name() {
        let json = r#"{"kafkaoxideConnectionsVersion":1,"connections":[]}"#;

        let parsed = ConnectionExportFile::parse(json).expect("legacy export should still import");

        assert_eq!(parsed.salty_connections_version, CURRENT_EXPORT_VERSION);
    }

    /// ...but only for reading. Anything written now carries the new spelling.
    #[test]
    fn exports_are_written_with_the_current_name() {
        let json = ConnectionExportFile::new(vec![]).to_json_pretty().unwrap();

        assert!(json.contains("saltyConnectionsVersion"), "got {json}");
        assert!(!json.contains("kafkaoxide"), "got {json}");
    }

    #[test]
    fn parse_rejects_malformed_json() {
        assert!(ConnectionExportFile::parse("not json").is_err());
    }

    #[test]
    fn select_for_export_returns_every_connection_when_ids_is_none() {
        let all = vec![sample_connection("1", "Prod"), sample_connection("2", "Staging")];

        let selected = select_for_export(&all, None);

        assert_eq!(selected.len(), 2);
    }

    #[test]
    fn select_for_export_returns_only_the_matching_ids() {
        let all = vec![sample_connection("1", "Prod"), sample_connection("2", "Staging")];

        let selected = select_for_export(&all, Some(&["2".to_string()]));

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].name, "Staging");
    }

    #[test]
    fn partition_importable_skips_entries_whose_name_already_exists() {
        let entries = vec![
            PortableConnection::from(&sample_connection("1", "Prod")),
            PortableConnection::from(&sample_connection("2", "Staging")),
        ];
        let existing_names = HashSet::from(["Prod".to_string()]);

        let (importable, skipped) = partition_importable(&entries, &existing_names);

        assert_eq!(importable.len(), 1);
        assert_eq!(importable[0].name, "Staging");
        assert_eq!(skipped, 1);
    }

    #[test]
    fn partition_importable_keeps_every_entry_when_no_names_collide() {
        let entries = vec![PortableConnection::from(&sample_connection("1", "Prod"))];
        let existing_names = HashSet::new();

        let (importable, skipped) = partition_importable(&entries, &existing_names);

        assert_eq!(importable.len(), 1);
        assert_eq!(skipped, 0);
    }
}
