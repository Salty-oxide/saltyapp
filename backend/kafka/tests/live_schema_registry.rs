//! One Avro message, followed the whole way.
//!
//! The unit tests cover each link separately — `SchemaRegistryClient` against
//! a hand-rolled TCP server, `salty_avro` against hand-built bytes — and
//! each passes while saying nothing about whether the links join up: whether a
//! real producer's framing is what `detect_wire_format` expects, whether a
//! real registry's response parses, and whether the schema it returns actually
//! decodes those bytes. This is the chain `connection_decode_avro` performs,
//! tested where it can be built (`src-tauri` needs a desktop toolchain).
//!
//! ```bash
//! docker run -d --name kafka --network host apache/kafka:3.9.0
//! docker run -d --name schema-registry --network host \
//!   -e SCHEMA_REGISTRY_HOST_NAME=localhost \
//!   -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://localhost:9092 \
//!   -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 \
//!   confluentinc/cp-schema-registry:7.6.0
//! # produce into `avro-orders` with kafka-avro-console-producer
//! SALTY_E2E_BOOTSTRAP=localhost:9092 \
//! SALTY_E2E_SCHEMA_REGISTRY=http://localhost:8081 \
//!   cargo test -p salty-kafka --test live_schema_registry -- --nocapture
//! ```

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use salty_core::{Connection, MessageFilter, SecurityProtocol};
use salty_kafka::{KafkaClient, RdKafkaClient};
use salty_schema_registry::{SchemaRegistryAuth, SchemaRegistryClients};

const TOPIC: &str = "avro-orders";

fn bootstrap() -> Option<String> {
    std::env::var("SALTY_E2E_BOOTSTRAP").ok().filter(|v| !v.is_empty())
}

fn registry() -> Option<String> {
    std::env::var("SALTY_E2E_SCHEMA_REGISTRY").ok().filter(|v| !v.is_empty())
}

fn connection(bootstrap_servers: String, schema_registry_endpoint: Option<String>) -> Connection {
    Connection {
        id: "e2e".into(),
        name: "e2e".into(),
        bootstrap_servers,
        kafka_version: "3.9".into(),
        zookeeper_enabled: false,
        zookeeper_host: None,
        zookeeper_port: None,
        zookeeper_chroot_path: None,
        security_protocol: SecurityProtocol::Plaintext,
        sasl_mechanism: None,
        sasl_username: None,
        sasl_password: None,
        sasl_oauth_url: None,
        schema_registry_endpoint,
        ksqldb_endpoint: None,
        ksqldb_basic_auth_credentials: None,
        schema_registry_basic_auth_credentials: None,
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

/// Exactly what the Data tab asks for, including the payload.
fn filter() -> MessageFilter {
    MessageFilter {
        partitions: None,
        max_messages_per_partition: Some(100),
        max_total_messages: Some(100),
        from_timestamp_ms: None,
        to_timestamp_ms: None,
        offset: None,
        include_payload: true,
        max_payload_preview_bytes: None,
    }
}

async fn fetch_payloads(bootstrap: String) -> Vec<Vec<u8>> {
    let client = RdKafkaClient::new();
    let result = client
        .fetch_messages(
            &connection(bootstrap, None),
            TOPIC,
            &filter(),
            None,
            Duration::from_secs(30),
            12 * 1024 * 1024,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("fetch failed");
    let mut messages = result.messages;
    // Offset order, so the assertions below can name specific records.
    messages.sort_by_key(|m| m.offset);
    messages
        .iter()
        .map(|m| {
            BASE64
                .decode(m.payload_base64.as_ref().expect("include_payload was set"))
                .expect("payload must be valid base64")
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn decodes_a_real_producers_avro_message_using_a_real_registry() {
    let (Some(bootstrap), Some(registry)) = (bootstrap(), registry()) else {
        eprintln!("skipped: set SALTY_E2E_BOOTSTRAP and SALTY_E2E_SCHEMA_REGISTRY");
        return;
    };

    let payloads = fetch_payloads(bootstrap).await;
    assert_eq!(payloads.len(), 3, "expected the three fixture records");

    let clients = SchemaRegistryClients::default();
    let client = clients
        .get_or_create("e2e", &registry, SchemaRegistryAuth::default())
        .expect("failed to build a registry client");

    let mut decoded = Vec::new();
    for payload in &payloads {
        // 1. A real producer's framing is the framing we detect.
        let schema_id = salty_avro::detect_wire_format(payload)
            .expect("a Confluent-produced payload must be recognised as wire format");
        // 2. A real registry's response parses, over real HTTP.
        let schema = client.fetch_schema_by_id(schema_id).await.expect("failed to fetch schema");
        // 3. That schema actually decodes those bytes — after the 5-byte header.
        let value =
            salty_avro::decode(&payload[5..], &schema).expect("failed to decode with the fetched schema");
        println!("offset payload -> schema id {schema_id} -> {value}");
        decoded.push(value);
    }

    // The records the producer was given, round-tripped through broker,
    // registry and decoder — including an array, an empty array, and both
    // branches of a nullable union, which is where a decoder usually goes
    // wrong.
    assert_eq!(decoded[0]["id"], "order-1");
    assert_eq!(decoded[0]["total"], 42.5);
    assert_eq!(decoded[0]["tags"], serde_json::json!(["new", "priority"]));
    assert_eq!(decoded[0]["note"], "first", "a populated union must unwrap to its inner value");

    assert_eq!(decoded[1]["id"], "order-2");
    assert_eq!(decoded[1]["total"], 17.25);
    assert_eq!(decoded[1]["tags"], serde_json::json!([]), "an empty array must stay an empty array");
    assert_eq!(decoded[1]["note"], serde_json::Value::Null, "an empty union must decode as null");

    assert_eq!(decoded[2]["id"], "order-3");
    assert_eq!(decoded[2]["total"], 1250.0);
    assert_eq!(decoded[2]["tags"], serde_json::json!(["bulk"]));
}

/// The pooled client's cache is what stops browsing one Avro topic from
/// re-asking a shared, rate-limited registry once per message opened.
#[tokio::test(flavor = "multi_thread")]
async fn the_registry_is_asked_once_however_many_messages_share_a_schema() {
    let (Some(bootstrap), Some(registry)) = (bootstrap(), registry()) else { return };

    let payloads = fetch_payloads(bootstrap).await;
    let clients = SchemaRegistryClients::default();
    let client = clients.get_or_create("e2e", &registry, SchemaRegistryAuth::default()).unwrap();

    let schema_id = salty_avro::detect_wire_format(&payloads[0]).unwrap();
    let first = client.fetch_schema_by_id(schema_id).await.expect("first fetch");

    // Point the client at an endpoint that cannot answer. A cached schema is
    // served without touching it; an uncached one could not be.
    let offline = SchemaRegistryClients::default();
    let offline_client = offline.get_or_create("e2e", "http://127.0.0.1:1", SchemaRegistryAuth::default()).unwrap();
    assert!(
        offline_client.fetch_schema_by_id(schema_id).await.is_err(),
        "sanity: an unreachable registry must fail when nothing is cached"
    );

    for payload in &payloads {
        let id = salty_avro::detect_wire_format(payload).unwrap();
        assert_eq!(id, schema_id, "the fixtures were produced with one schema");
        assert_eq!(client.fetch_schema_by_id(id).await.expect("cached fetch"), first);
    }
}

/// Basic auth against a registry that actually enforces it.
///
/// The unit test asserts the header this client *sends* to a hand-rolled TCP
/// server, which cannot tell whether a real registry accepts it — and this is
/// the auth every managed registry (Confluent Cloud among them) uses, so
/// "the header looks right" is not enough on its own.
#[tokio::test(flavor = "multi_thread")]
async fn authenticates_against_a_registry_that_requires_basic_auth() {
    let Some(registry) = std::env::var("SALTY_E2E_SCHEMA_REGISTRY_AUTH").ok().filter(|v| !v.is_empty()) else {
        eprintln!("skipped: set SALTY_E2E_SCHEMA_REGISTRY_AUTH to run this test");
        return;
    };
    let credentials =
        std::env::var("SALTY_E2E_SCHEMA_REGISTRY_CREDENTIALS").expect("set ..._CREDENTIALS as user:password");
    let clients = SchemaRegistryClients::default();

    // Without credentials the registry refuses, so this must fail rather than
    // return something unusable.
    let anonymous = clients.get_or_create("anon", &registry, SchemaRegistryAuth::default()).unwrap();
    assert!(
        anonymous.fetch_schema_by_id(1).await.is_err(),
        "a registry requiring auth must not appear to work without credentials"
    );

    let authenticated = clients
        .get_or_create(
            "authed",
            &registry,
            SchemaRegistryAuth { basic_auth_credentials: Some(&credentials), ..Default::default() },
        )
        .unwrap();
    let schema = authenticated.fetch_schema_by_id(1).await.expect("configured credentials must be accepted");

    assert!(schema.contains("\"name\":\"Order\""), "expected the fixture schema, got: {schema}");

    // And the credentials really are what made the difference: the wrong ones
    // must fail against the same endpoint.
    let wrong = clients
        .get_or_create(
            "wrong",
            &registry,
            SchemaRegistryAuth { basic_auth_credentials: Some("sruser:nope"), ..Default::default() },
        )
        .unwrap();
    assert!(wrong.fetch_schema_by_id(1).await.is_err(), "wrong credentials must be rejected");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_schema_id_is_reported_as_not_found() {
    let Some(registry) = registry() else { return };
    let clients = SchemaRegistryClients::default();
    let client = clients.get_or_create("e2e", &registry, SchemaRegistryAuth::default()).unwrap();

    let err = client.fetch_schema_by_id(987_654).await.expect_err("an unknown id must fail");

    assert!(format!("{err:?}").contains("not found"), "expected a not-found error, got: {err:?}");
}
