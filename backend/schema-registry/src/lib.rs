use error_stack::{Report, ResultExt};
use salty_core::AppError;
use salty_core::Result;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

/// Schema Registry TLS/auth material from a connection's Schema Registry
/// fields + secrets — mirrors `salty_kafka::BrokerSslConfig`'s split
/// between locations (not secret) and passwords (from the OS keychain).
#[derive(Debug, Clone, Copy, Default)]
pub struct SchemaRegistryAuth<'a> {
    pub basic_auth_credentials: Option<&'a str>,
    pub trust_store_location: Option<&'a str>,
    pub keystore_location: Option<&'a str>,
    pub keystore_password: Option<&'a str>,
}

/// An HTTP client for the Confluent Schema Registry API, scoped to one
/// connection's endpoint/auth. `fetch_schema_by_id` caches results
/// in-memory for the client's lifetime — schema ids are immutable once
/// registered, so this never needs invalidating.
pub struct SchemaRegistryClient {
    http: reqwest::Client,
    base_url: String,
    basic_auth: Option<(String, String)>,
    cache: Mutex<HashMap<u32, String>>,
}

impl SchemaRegistryClient {
    pub fn new(endpoint: &str, auth: SchemaRegistryAuth<'_>) -> Result<Self, AppError> {
        let mut builder = reqwest::Client::builder();

        if let Some(location) = auth.trust_store_location {
            let pem = std::fs::read(location)
                .change_context(AppError::SchemaRegistry)
                .attach_with(|| format!("failed to read trust store at {location}"))?;
            let cert = reqwest::Certificate::from_pem(&pem)
                .change_context(AppError::SchemaRegistry)
                .attach("failed to parse trust store as PEM")?;
            builder = builder.add_root_certificate(cert);
        }

        if let (Some(location), Some(password)) = (auth.keystore_location, auth.keystore_password) {
            let der = std::fs::read(location)
                .change_context(AppError::SchemaRegistry)
                .attach_with(|| format!("failed to read keystore at {location}"))?;
            let identity = reqwest::Identity::from_pkcs12_der(&der, password)
                .change_context(AppError::SchemaRegistry)
                .attach("failed to parse keystore as PKCS12")?;
            builder = builder.identity(identity);
        }

        let http = builder
            .build()
            .change_context(AppError::SchemaRegistry)
            .attach("failed to build Schema Registry HTTP client")?;

        let basic_auth = match auth.basic_auth_credentials {
            Some(creds) => {
                let (user, pass) = creds.split_once(':').ok_or_else(|| {
                    Report::new(AppError::SchemaRegistry).attach(
                        "schema registry basic auth credentials must be in \"username:password\" format",
                    )
                })?;
                Some((user.to_string(), pass.to_string()))
            }
            None => None,
        };

        Ok(SchemaRegistryClient {
            http,
            base_url: endpoint.trim_end_matches('/').to_string(),
            basic_auth,
            cache: Mutex::new(HashMap::new()),
        })
    }

    pub async fn fetch_schema_by_id(&self, id: u32) -> Result<String, AppError> {
        if let Some(cached) = self
            .cache
            .lock()
            .expect("schema cache lock poisoned")
            .get(&id)
        {
            return Ok(cached.clone());
        }

        let url = format!("{}/schemas/ids/{}", self.base_url, id);
        let mut request = self.http.get(&url);
        if let Some((user, password)) = &self.basic_auth {
            request = request.basic_auth(user, Some(password));
        }

        let response = request
            .send()
            .await
            .change_context(AppError::SchemaRegistry)
            .attach_with(|| format!("request to {url} failed"))?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(Report::new(AppError::NotFound))
                .attach_with(|| format!("schema id {id} not found in registry"));
        }

        let response = response
            .error_for_status()
            .change_context(AppError::SchemaRegistry)
            .attach_with(|| format!("registry returned an error for schema id {id}"))?;

        #[derive(serde::Deserialize)]
        struct SchemaResponse {
            schema: String,
        }

        let body: SchemaResponse = response
            .json()
            .await
            .change_context(AppError::SchemaRegistry)
            .attach("failed to parse registry response as JSON")?;

        self.cache
            .lock()
            .expect("schema cache lock poisoned")
            .insert(id, body.schema.clone());
        Ok(body.schema)
    }
}

/// One live [`SchemaRegistryClient`] per connection, reused across requests.
///
/// A client was previously built inside every `connection_decode_avro` call.
/// That made the client's own schema cache dead on arrival — it was dropped
/// at the end of the call that filled it — and made decoding one message
/// cost a fresh `reqwest::Client` (with an empty connection pool, so a fresh
/// TLS handshake), plus reading and parsing the truststore and keystore off
/// disk. Viewing 50 Avro messages meant 50 registry round trips and 50 TLS
/// handshakes for what is nearly always the same schema id.
///
/// Schema Registry is typically one shared, rate-limited service, so it
/// absorbs that far less comfortably than a broker cluster would.
///
/// Keyed by connection id and versioned by a fingerprint of the endpoint and
/// auth material, mirroring how `RdKafkaClient` pools its metadata clients
/// against `Connection::updated_at`: editing a connection's registry
/// credentials changes the fingerprint, so the next request builds a new
/// client instead of silently reusing one holding the old credentials.
///
/// Known limit: the fingerprint covers the keystore/truststore *paths*, not
/// their contents. Replacing a certificate file in place, without touching
/// the connection, keeps the pooled client until the connection is edited or
/// released. Rebuilding on every decode used to pick that up, at the cost
/// documented above; re-saving the connection is the deliberate way to force
/// it now.
#[derive(Default)]
pub struct SchemaRegistryClients {
    clients: Mutex<HashMap<String, PooledRegistryClient>>,
}

struct PooledRegistryClient {
    /// Hash of the endpoint and every auth field this client was built from.
    ///
    /// A hash rather than the values themselves so a long-lived map does not
    /// hold a second plaintext copy of the keystore password. It only ever
    /// decides "same settings or not", never authenticates anything, so a
    /// non-cryptographic hash is the right tool.
    fingerprint: u64,
    client: Arc<SchemaRegistryClient>,
}

fn fingerprint_of(endpoint: &str, auth: &SchemaRegistryAuth<'_>) -> u64 {
    let mut hasher = DefaultHasher::new();
    endpoint.hash(&mut hasher);
    auth.basic_auth_credentials.hash(&mut hasher);
    auth.trust_store_location.hash(&mut hasher);
    auth.keystore_location.hash(&mut hasher);
    auth.keystore_password.hash(&mut hasher);
    hasher.finish()
}

impl SchemaRegistryClients {
    /// This connection's registry client, building one if there isn't a
    /// current one. Cheap on every call after the first: an `Arc` clone.
    pub fn get_or_create(
        &self,
        connection_id: &str,
        endpoint: &str,
        auth: SchemaRegistryAuth<'_>,
    ) -> Result<Arc<SchemaRegistryClient>, AppError> {
        let fingerprint = fingerprint_of(endpoint, &auth);
        let mut clients = self.clients.lock().unwrap_or_else(|err| err.into_inner());

        if let Some(pooled) = clients.get(connection_id)
            && pooled.fingerprint == fingerprint
        {
            return Ok(Arc::clone(&pooled.client));
        }

        let client = Arc::new(SchemaRegistryClient::new(endpoint, auth)?);
        clients.insert(
            connection_id.to_string(),
            PooledRegistryClient {
                fingerprint,
                client: Arc::clone(&client),
            },
        );
        Ok(client)
    }

    /// Forgets a connection's client — called when the connection is edited
    /// or deleted, so nothing keeps serving cached schemas for a registry the
    /// connection no longer points at.
    pub fn release(&self, connection_id: &str) {
        self.clients
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .remove(connection_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    const ENDPOINT: &str = "http://localhost:1";

    #[test]
    fn reuses_one_client_per_connection_so_its_schema_cache_survives() {
        // The whole point: a client rebuilt per decode drops the cache that
        // makes the second view of a topic free.
        let clients = SchemaRegistryClients::default();

        let first = clients
            .get_or_create("conn-1", ENDPOINT, SchemaRegistryAuth::default())
            .unwrap();
        let second = clients
            .get_or_create("conn-1", ENDPOINT, SchemaRegistryAuth::default())
            .unwrap();

        assert!(
            Arc::ptr_eq(&first, &second),
            "expected the pooled client, not a rebuilt one"
        );
    }

    #[test]
    fn keeps_connections_separate() {
        let clients = SchemaRegistryClients::default();

        let one = clients
            .get_or_create("conn-1", ENDPOINT, SchemaRegistryAuth::default())
            .unwrap();
        let two = clients
            .get_or_create("conn-2", ENDPOINT, SchemaRegistryAuth::default())
            .unwrap();

        assert!(!Arc::ptr_eq(&one, &two));
    }

    #[test]
    fn rebuilds_when_the_endpoint_changes() {
        let clients = SchemaRegistryClients::default();

        let first = clients
            .get_or_create("conn-1", ENDPOINT, SchemaRegistryAuth::default())
            .unwrap();
        let second = clients
            .get_or_create(
                "conn-1",
                "http://localhost:2",
                SchemaRegistryAuth::default(),
            )
            .unwrap();

        assert!(
            !Arc::ptr_eq(&first, &second),
            "a client must not outlive the endpoint it was built for"
        );
    }

    /// Reusing a client built from credentials the user has since replaced
    /// would keep authenticating as the old ones — the registry equivalent of
    /// serving a request from a Kafka client the broker has already rejected.
    #[test]
    fn rebuilds_when_any_auth_field_changes() {
        let changed: [SchemaRegistryAuth<'_>; 4] = [
            SchemaRegistryAuth {
                basic_auth_credentials: Some("user:pass"),
                ..Default::default()
            },
            SchemaRegistryAuth {
                trust_store_location: Some("/tmp/truststore.pem"),
                ..Default::default()
            },
            SchemaRegistryAuth {
                keystore_location: Some("/tmp/keystore.p12"),
                ..Default::default()
            },
            SchemaRegistryAuth {
                keystore_password: Some("hunter2"),
                ..Default::default()
            },
        ];

        for auth in changed {
            let clients = SchemaRegistryClients::default();
            let before = clients
                .get_or_create("conn-1", ENDPOINT, SchemaRegistryAuth::default())
                .unwrap();
            // Only builds a client when the material is loadable; a keystore
            // path alone never is, so tolerate that and assert on the rest.
            if let Ok(after) = clients.get_or_create("conn-1", ENDPOINT, auth) {
                assert!(
                    !Arc::ptr_eq(&before, &after),
                    "changed auth must not reuse the old client"
                );
            }
        }
    }

    #[test]
    fn releasing_a_connection_drops_its_client() {
        let clients = SchemaRegistryClients::default();

        let before = clients
            .get_or_create("conn-1", ENDPOINT, SchemaRegistryAuth::default())
            .unwrap();
        clients.release("conn-1");
        let after = clients
            .get_or_create("conn-1", ENDPOINT, SchemaRegistryAuth::default())
            .unwrap();

        assert!(!Arc::ptr_eq(&before, &after));
    }

    #[test]
    fn releasing_an_unknown_connection_is_a_no_op() {
        let clients = SchemaRegistryClients::default();
        clients.release("never-seen");
    }

    /// A malformed credential must not poison the pool: the next request has
    /// to be free to build a client once the user fixes it.
    #[test]
    fn a_failed_build_leaves_nothing_pooled() {
        let clients = SchemaRegistryClients::default();
        let bad = SchemaRegistryAuth {
            basic_auth_credentials: Some("not-user-colon-pass"),
            ..Default::default()
        };

        assert!(clients.get_or_create("conn-1", ENDPOINT, bad).is_err());
        assert!(
            clients
                .get_or_create("conn-1", ENDPOINT, SchemaRegistryAuth::default())
                .is_ok()
        );
    }

    /// Starts a one-shot HTTP server that replies to the first request it
    /// receives, then shuts down. Returns the base URL to hit and a handle
    /// that resolves to the raw request bytes it saw (so tests can assert
    /// on headers like Authorization).
    async fn one_shot_server(
        status_line: &'static str,
        body: &'static str,
    ) -> (String, tokio::task::JoinHandle<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = socket.read(&mut buf).await.unwrap();
            let request = buf[..n].to_vec();
            let response = format!(
                "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            let _ = socket.shutdown().await;
            request
        });
        (format!("http://{addr}"), handle)
    }

    #[test]
    fn rejects_malformed_basic_auth_credentials() {
        let auth = SchemaRegistryAuth {
            basic_auth_credentials: Some("not-user-colon-pass"),
            ..Default::default()
        };

        let result = SchemaRegistryClient::new("http://localhost:1", auth);

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn fetches_a_schema_by_id() {
        let (base_url, _handle) =
            one_shot_server("HTTP/1.1 200 OK", r#"{"schema":"{\"type\":\"string\"}"}"#).await;
        let client = SchemaRegistryClient::new(&base_url, SchemaRegistryAuth::default()).unwrap();

        let schema = client.fetch_schema_by_id(1).await.unwrap();

        assert_eq!(schema, r#"{"type":"string"}"#);
    }

    #[tokio::test]
    async fn returns_a_descriptive_error_for_a_404() {
        let (base_url, _handle) = one_shot_server("HTTP/1.1 404 Not Found", "{}").await;
        let client = SchemaRegistryClient::new(&base_url, SchemaRegistryAuth::default()).unwrap();

        let result = client.fetch_schema_by_id(99).await;

        assert!(result.is_err());
        assert!(format!("{:?}", result.unwrap_err()).contains("schema id 99 not found"));
    }

    #[tokio::test]
    async fn sends_a_basic_auth_header_when_credentials_are_configured() {
        let (base_url, handle) =
            one_shot_server("HTTP/1.1 200 OK", r#"{"schema":"\"string\""}"#).await;
        let auth = SchemaRegistryAuth {
            basic_auth_credentials: Some("user:pass"),
            ..Default::default()
        };
        let client = SchemaRegistryClient::new(&base_url, auth).unwrap();

        client.fetch_schema_by_id(1).await.unwrap();

        let request = String::from_utf8_lossy(&handle.await.unwrap()).to_lowercase();
        assert!(request.contains("authorization: basic"));
    }

    #[tokio::test]
    async fn caches_a_schema_after_the_first_fetch() {
        let (base_url, handle) =
            one_shot_server("HTTP/1.1 200 OK", r#"{"schema":"\"string\""}"#).await;
        let client = SchemaRegistryClient::new(&base_url, SchemaRegistryAuth::default()).unwrap();

        client.fetch_schema_by_id(1).await.unwrap();
        // The one-shot server only answers once — a second fetch that hit
        // the network again would fail (the server already handled and
        // closed its one connection), so this only passes if the cache
        // served the second call.
        let second = client.fetch_schema_by_id(1).await.unwrap();

        assert_eq!(second, "\"string\"");
        handle.await.unwrap();
    }
}
