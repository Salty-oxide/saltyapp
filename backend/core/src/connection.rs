use std::fmt;

use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Display, EnumString)]
pub enum SecurityProtocol {
    #[strum(serialize = "PLAINTEXT")]
    #[serde(rename = "PLAINTEXT")]
    Plaintext,
    #[strum(serialize = "SSL")]
    #[serde(rename = "SSL")]
    Ssl,
    #[strum(serialize = "SASL_PLAINTEXT")]
    #[serde(rename = "SASL_PLAINTEXT")]
    SaslPlaintext,
    #[strum(serialize = "SASL_SSL")]
    #[serde(rename = "SASL_SSL")]
    SaslSsl,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Display, EnumString)]
pub enum SaslMechanism {
    #[strum(serialize = "PLAIN")]
    #[serde(rename = "PLAIN")]
    Plain,
    #[strum(serialize = "SCRAM-SHA-256")]
    #[serde(rename = "SCRAM-SHA-256")]
    ScramSha256,
    #[strum(serialize = "SCRAM-SHA-512")]
    #[serde(rename = "SCRAM-SHA-512")]
    ScramSha512,
}

/// A saved Kafka connection profile, as returned to the frontend — including
/// secrets (SASL password, schema-registry credentials, keystore/truststore
/// passwords). These are stored as plaintext columns in the local SQLite
/// database, a deliberate tradeoff made after the OS-keychain-backed storage
/// this app used previously proved unreliable on Windows (writes to
/// Credential Manager silently failing for some users, with no working
/// fallback). This database is local to the user's machine, not synced or
/// shared.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Connection {
    pub id: String,
    /// The "Cluster name" field in the New Connection modal's General section.
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
    pub sasl_password: Option<String>,
    pub sasl_oauth_url: Option<String>,
    pub schema_registry_endpoint: Option<String>,
    pub schema_registry_basic_auth_credentials: Option<String>,
    /// Base URL of this cluster's ksqlDB server, e.g. `http://localhost:8088`.
    /// Absent for the many clusters that do not run one.
    pub ksqldb_endpoint: Option<String>,
    /// `user:password`, the same shape as the schema registry's credentials.
    /// A secret: excluded from `PortableConnection`.
    pub ksqldb_basic_auth_credentials: Option<String>,
    pub schema_registry_trust_store_location: Option<String>,
    pub schema_registry_trust_store_password: Option<String>,
    pub schema_registry_keystore_location: Option<String>,
    pub schema_registry_keystore_password: Option<String>,
    pub schema_registry_keystore_key_password: Option<String>,
    /// Broker security tab — SSL/TLS material.
    pub ssl_truststore_location: Option<String>,
    pub ssl_truststore_password: Option<String>,
    pub ssl_keystore_location: Option<String>,
    pub ssl_keystore_password: Option<String>,
    pub ssl_keystore_key_password: Option<String>,
    /// Whether this connection may publish messages to its topics at all.
    ///
    /// Defaults to `false` for every connection, existing and new (see the
    /// `0006_allow_publishing` migration), so publishing is something the user
    /// switches on per cluster after deciding they intend to write to it —
    /// never something a fresh install, or a connections file someone sent
    /// them, can do on its own. Re-read from the database inside the publish
    /// command on every publish, so this is a gate and not a UI hint: the
    /// frontend's opinion about it is never consulted.
    ///
    /// It is not, and cannot be, a substitute for the broker's ACLs — a
    /// principal with no Write grant is refused whatever this says. It guards
    /// the other direction: a principal that *does* have write access, against
    /// publishing to the wrong cluster by accident.
    pub allow_publishing: bool,
    pub created_at: String,
    pub updated_at: String,
}

/// The full set of fields submitted from the New/Edit Connection modal,
/// including secrets — persisted as-is (see `salty_db::connections`).
/// `Debug` still redacts secret values (see below), so they never end up in
/// logs even though they're stored in plain columns.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewConnection {
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
    pub sasl_password: Option<String>,
    pub sasl_oauth_url: Option<String>,
    pub schema_registry_endpoint: Option<String>,
    pub schema_registry_basic_auth_credentials: Option<String>,
    /// Base URL of this cluster's ksqlDB server, e.g. `http://localhost:8088`.
    /// Absent for the many clusters that do not run one.
    pub ksqldb_endpoint: Option<String>,
    /// `user:password`, the same shape as the schema registry's credentials.
    /// A secret: excluded from `PortableConnection`.
    pub ksqldb_basic_auth_credentials: Option<String>,
    pub schema_registry_trust_store_location: Option<String>,
    pub schema_registry_trust_store_password: Option<String>,
    pub schema_registry_keystore_location: Option<String>,
    pub schema_registry_keystore_password: Option<String>,
    pub schema_registry_keystore_key_password: Option<String>,
    /// Broker security tab's "Broker security" section.
    pub ssl_truststore_location: Option<String>,
    pub ssl_truststore_password: Option<String>,
    pub ssl_keystore_location: Option<String>,
    pub ssl_keystore_password: Option<String>,
    pub ssl_keystore_key_password: Option<String>,
    /// See [`Connection::allow_publishing`]. `#[serde(default)]` so a payload
    /// that omits it — an older frontend, or an imported connection — means
    /// "not allowed" rather than failing to parse.
    #[serde(default)]
    pub allow_publishing: bool,
}

impl fmt::Debug for NewConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn redacted(value: &Option<String>) -> Option<&'static str> {
            value.as_ref().map(|_| "[redacted]")
        }

        f.debug_struct("NewConnection")
            .field("name", &self.name)
            .field("bootstrap_servers", &self.bootstrap_servers)
            .field("kafka_version", &self.kafka_version)
            .field("zookeeper_enabled", &self.zookeeper_enabled)
            .field("zookeeper_host", &self.zookeeper_host)
            .field("zookeeper_port", &self.zookeeper_port)
            .field("zookeeper_chroot_path", &self.zookeeper_chroot_path)
            .field("security_protocol", &self.security_protocol)
            .field("sasl_mechanism", &self.sasl_mechanism)
            .field("sasl_username", &self.sasl_username)
            .field("sasl_password", &redacted(&self.sasl_password))
            .field("sasl_oauth_url", &self.sasl_oauth_url)
            .field("schema_registry_endpoint", &self.schema_registry_endpoint)
            .field(
                "schema_registry_basic_auth_credentials",
                &redacted(&self.schema_registry_basic_auth_credentials),
            )
            .field(
                "schema_registry_trust_store_location",
                &self.schema_registry_trust_store_location,
            )
            .field(
                "schema_registry_trust_store_password",
                &redacted(&self.schema_registry_trust_store_password),
            )
            .field(
                "schema_registry_keystore_location",
                &self.schema_registry_keystore_location,
            )
            .field(
                "schema_registry_keystore_password",
                &redacted(&self.schema_registry_keystore_password),
            )
            .field(
                "schema_registry_keystore_key_password",
                &redacted(&self.schema_registry_keystore_key_password),
            )
            .field("ssl_truststore_location", &self.ssl_truststore_location)
            .field("ssl_truststore_password", &redacted(&self.ssl_truststore_password))
            .field("ssl_keystore_location", &self.ssl_keystore_location)
            .field("ssl_keystore_password", &redacted(&self.ssl_keystore_password))
            .field(
                "ssl_keystore_key_password",
                &redacted(&self.ssl_keystore_key_password),
            )
            .field("allow_publishing", &self.allow_publishing)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConnectionStatus {
    Unknown,
    Reachable,
    Unreachable,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn security_protocol_round_trips_through_display_and_fromstr() {
        for protocol in [
            SecurityProtocol::Plaintext,
            SecurityProtocol::Ssl,
            SecurityProtocol::SaslPlaintext,
            SecurityProtocol::SaslSsl,
        ] {
            let text = protocol.to_string();
            assert_eq!(SecurityProtocol::from_str(&text).unwrap(), protocol);
        }
    }

    #[test]
    fn sasl_mechanism_round_trips_through_display_and_fromstr() {
        for mechanism in [
            SaslMechanism::Plain,
            SaslMechanism::ScramSha256,
            SaslMechanism::ScramSha512,
        ] {
            let text = mechanism.to_string();
            assert_eq!(SaslMechanism::from_str(&text).unwrap(), mechanism);
        }
    }

    fn sample_connection() -> Connection {
        Connection {
            id: "1".into(),
            name: "Local".into(),
            bootstrap_servers: "localhost:9092".into(),
            kafka_version: "3.7".into(),
            zookeeper_enabled: false,
            zookeeper_host: None,
            zookeeper_port: None,
            zookeeper_chroot_path: None,
            security_protocol: SecurityProtocol::Plaintext,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            sasl_oauth_url: None,
            schema_registry_endpoint: None,
            schema_registry_basic_auth_credentials: None,
            ksqldb_endpoint: None,
            ksqldb_basic_auth_credentials: None,
            schema_registry_trust_store_location: None,
            schema_registry_trust_store_password: None,
            schema_registry_keystore_location: None,
            schema_registry_keystore_password: None,
            schema_registry_keystore_key_password: None,
            ssl_truststore_location: None,
            ssl_truststore_password: None,
            ssl_keystore_location: None,
            ssl_keystore_password: None,
            ssl_keystore_key_password: None,
            allow_publishing: false,
            created_at: "now".into(),
            updated_at: "now".into(),
        }
    }

    fn sample_new_connection() -> NewConnection {
        NewConnection {
            name: "Local".into(),
            bootstrap_servers: "localhost:9092".into(),
            kafka_version: "3.7".into(),
            zookeeper_enabled: false,
            zookeeper_host: None,
            zookeeper_port: None,
            zookeeper_chroot_path: None,
            security_protocol: SecurityProtocol::Plaintext,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            sasl_oauth_url: None,
            schema_registry_endpoint: None,
            schema_registry_basic_auth_credentials: None,
            ksqldb_endpoint: None,
            ksqldb_basic_auth_credentials: None,
            schema_registry_trust_store_location: None,
            schema_registry_trust_store_password: None,
            schema_registry_keystore_location: None,
            schema_registry_keystore_password: None,
            schema_registry_keystore_key_password: None,
            ssl_truststore_location: None,
            ssl_truststore_password: None,
            ssl_keystore_location: None,
            ssl_keystore_password: None,
            ssl_keystore_key_password: None,
            allow_publishing: false,
        }
    }

    #[test]
    fn connection_serializes_fields_as_camel_case() {
        let connection = sample_connection();
        let json = serde_json::to_string(&connection).unwrap();
        assert!(json.contains("\"bootstrapServers\":\"localhost:9092\""));
        assert!(json.contains("\"securityProtocol\":\"PLAINTEXT\""));
        assert!(json.contains("\"kafkaVersion\":\"3.7\""));
        assert!(json.contains("\"zookeeperEnabled\":false"));
        assert!(json.contains("\"schemaRegistryEndpoint\":null"));
    }

    #[test]
    fn new_connection_debug_output_never_contains_any_of_the_real_secrets() {
        let mut new_connection = sample_new_connection();
        new_connection.sasl_password = Some("sasl-secret".into());
        new_connection.schema_registry_basic_auth_credentials = Some("user:super-secret-value".into());
        new_connection.schema_registry_trust_store_password = Some("trust-store-secret".into());
        new_connection.schema_registry_keystore_password = Some("keystore-secret".into());
        new_connection.schema_registry_keystore_key_password = Some("keystore-key-secret".into());
        new_connection.ssl_truststore_password = Some("broker-trust-store-secret".into());
        new_connection.ssl_keystore_password = Some("broker-keystore-secret".into());
        new_connection.ssl_keystore_key_password = Some("broker-keystore-key-secret".into());

        let debug_output = format!("{:?}", new_connection);

        assert!(!debug_output.contains("sasl-secret"));
        assert!(!debug_output.contains("super-secret-value"));
        assert!(!debug_output.contains("trust-store-secret"));
        assert!(!debug_output.contains("keystore-secret"));
        assert!(!debug_output.contains("keystore-key-secret"));
        assert!(!debug_output.contains("broker-trust-store-secret"));
        assert!(!debug_output.contains("broker-keystore-secret"));
        assert!(!debug_output.contains("broker-keystore-key-secret"));
        assert_eq!(debug_output.matches("[redacted]").count(), 8);
    }

    #[test]
    fn new_connection_debug_output_shows_none_for_absent_secrets() {
        let debug_output = format!("{:?}", sample_new_connection());
        assert_eq!(debug_output.matches("[redacted]").count(), 0);
    }
}
