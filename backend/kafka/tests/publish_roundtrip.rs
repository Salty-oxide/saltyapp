//! Publishing, against a real broker.
//!
//! The produce path cannot be exercised without one: the unit tests beside
//! `producer.rs` reach only its failure paths (a closed port, a producer that
//! cannot be built), which leaves everything that matters — that the bytes on
//! the topic are the bytes the user entered, in the partition they chose, in
//! the order they wrote them — unrun.
//!
//! Every test here reads its messages back through `fetch_messages`, the same
//! path the Data tab uses, rather than trusting the delivery report. A delivery
//! report says the broker accepted a record; only a read says what it stored.
//!
//! ```bash
//! docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0
//! # fixture: `e2e-publish`, 3 partitions (scripts/e2e-fixtures.sh creates it)
//! SALTY_E2E_BOOTSTRAP=localhost:9092 \
//!   cargo test -p salty-kafka --test publish_roundtrip
//! ```

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use salty_core::{
    encode_messages, Connection, MessageFilter, NewPublishMessage, PayloadEncoding, PublishField,
    PublishHeaderInput, PublishLimits, SecurityProtocol, TopicMessage,
};
use salty_kafka::{KafkaClient, RdKafkaClient};

const TOPIC: &str = "e2e-publish";
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_MESSAGE_SIZE: u32 = 12 * 1024 * 1024;

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

fn connection(bootstrap_servers: String) -> Connection {
    Connection {
        id: "e2e-publish".into(),
        name: "e2e-publish".into(),
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
        schema_registry_endpoint: None,
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
        allow_publishing: true,
        created_at: "now".into(),
        updated_at: "now".into(),
    }
}

/// A run-unique marker, so tests can find their own records on a topic that
/// accumulates them across runs (and across the other tests in this file)
/// without needing a fresh topic each time.
fn marker(what: &str) -> String {
    format!(
        "{what}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn text_message(key: Option<&str>, value: &str) -> NewPublishMessage {
    NewPublishMessage {
        key: match key {
            Some(key) => PublishField::text(key),
            None => PublishField::null(),
        },
        value: PublishField::text(value),
        headers: Vec::new(),
    }
}

async fn publish(
    client: &RdKafkaClient,
    connection: &Connection,
    partition: i32,
    messages: &[NewPublishMessage],
) -> salty_core::PublishOutcome {
    let records = encode_messages(messages, &PublishLimits::for_max_message_size(MAX_MESSAGE_SIZE))
        .expect("the fixtures in this file are all valid");
    client
        .publish_messages(
            connection,
            TOPIC,
            partition,
            &records,
            MAX_MESSAGE_SIZE,
            WRITE_TIMEOUT,
        )
        .await
        .expect("the publish should reach the broker")
}

/// Reads a partition back through the same path the Data tab uses, with
/// payloads on.
async fn read_partition(
    client: &RdKafkaClient,
    connection: &Connection,
    partition: i32,
) -> Vec<TopicMessage> {
    let filter = MessageFilter {
        partitions: Some(vec![partition]),
        include_payload: true,
        ..MessageFilter::default()
    };
    client
        .fetch_messages(
            connection,
            TOPIC,
            &filter,
            None,
            READ_TIMEOUT,
            MAX_MESSAGE_SIZE,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("reading the topic back should succeed")
        .messages
}

fn payload_of(message: &TopicMessage) -> Vec<u8> {
    BASE64
        .decode(message.payload_base64.as_deref().unwrap_or_default())
        .expect("a payload this test published is valid base64")
}

fn key_of(message: &TopicMessage) -> Option<Vec<u8>> {
    message
        .key_base64
        .as_deref()
        .map(|key| BASE64.decode(key).expect("a key this test published is valid base64"))
}

#[tokio::test]
async fn three_distinct_messages_land_in_the_chosen_partition_in_order() {
    let bootstrap = broker!();
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    let run = marker("ordered");

    let messages = vec![
        text_message(Some(&format!("{run}-k1")), &format!("{run}-one")),
        text_message(Some(&format!("{run}-k2")), &format!("{run}-two")),
        text_message(Some(&format!("{run}-k3")), &format!("{run}-three")),
    ];
    let outcome = publish(&client, &connection, 1, &messages).await;

    assert!(outcome.failure.is_none(), "unexpected failure: {outcome:?}");
    assert_eq!(outcome.delivered.len(), 3);
    assert_eq!(outcome.not_attempted, 0);

    // Every record went to the partition that was asked for, not wherever the
    // key hashed to — the whole point of a partition-targeted publish.
    for delivered in &outcome.delivered {
        assert_eq!(delivered.partition, 1, "record {} went elsewhere", delivered.index);
    }

    // Offsets increase with the batch order, and each is reported against the
    // right row.
    assert_eq!(outcome.delivered[0].index, 0);
    assert!(outcome.delivered[0].offset < outcome.delivered[1].offset);
    assert!(outcome.delivered[1].offset < outcome.delivered[2].offset);

    let read_back = read_partition(&client, &connection, 1).await;
    let ours: Vec<&TopicMessage> = read_back
        .iter()
        .filter(|message| String::from_utf8_lossy(&payload_of(message)).starts_with(&run))
        .collect();
    assert_eq!(ours.len(), 3, "all three messages should be readable back");

    let mut by_offset = ours.clone();
    by_offset.sort_by_key(|message| message.offset);
    assert_eq!(
        by_offset
            .iter()
            .map(|message| String::from_utf8(payload_of(message)).unwrap())
            .collect::<Vec<_>>(),
        vec![
            format!("{run}-one"),
            format!("{run}-two"),
            format!("{run}-three")
        ],
        "the order on the topic must be the order in the dialog"
    );
    assert_eq!(
        by_offset
            .iter()
            .map(|message| String::from_utf8(key_of(message).unwrap()).unwrap())
            .collect::<Vec<_>>(),
        vec![
            format!("{run}-k1"),
            format!("{run}-k2"),
            format!("{run}-k3")
        ]
    );
}

#[tokio::test]
async fn a_single_message_is_the_same_path_as_many() {
    let bootstrap = broker!();
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    let run = marker("single");

    let outcome = publish(&client, &connection, 0, &[text_message(None, &run)]).await;
    assert!(outcome.succeeded(), "unexpected failure: {outcome:?}");
    assert_eq!(outcome.delivered.len(), 1);

    let read_back = read_partition(&client, &connection, 0).await;
    let ours = read_back
        .iter()
        .find(|message| String::from_utf8_lossy(&payload_of(message)) == run)
        .expect("the message should be readable back");
    assert_eq!(ours.partition, 0);
    assert_eq!(
        key_of(ours),
        None,
        "a null key must arrive null, not as empty bytes"
    );
}

#[tokio::test]
async fn binary_payloads_and_headers_survive_the_round_trip() {
    let bootstrap = broker!();
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    let run = marker("binary");

    // Bytes that are not valid UTF-8, so anything that lossily decoded them on
    // the way out or back would corrupt them visibly.
    let binary: Vec<u8> = vec![0x00, 0x01, 0xff, 0xfe, 0x80, 0x7f];
    let mut payload = run.as_bytes().to_vec();
    payload.extend_from_slice(&binary);

    let message = NewPublishMessage {
        key: PublishField::text(&run),
        value: PublishField {
            encoding: PayloadEncoding::Base64,
            text: BASE64.encode(&payload),
        },
        headers: vec![
            PublishHeaderInput {
                key: "content-type".into(),
                value: PublishField::text("application/octet-stream"),
            },
            PublishHeaderInput {
                key: "trace-id".into(),
                value: PublishField {
                    encoding: PayloadEncoding::Base64,
                    text: BASE64.encode([0xde, 0xad, 0xbe, 0xef]),
                },
            },
        ],
    };

    let outcome = publish(&client, &connection, 2, &[message]).await;
    assert!(outcome.succeeded(), "unexpected failure: {outcome:?}");

    let read_back = read_partition(&client, &connection, 2).await;
    let ours = read_back
        .iter()
        .find(|message| key_of(message).as_deref() == Some(run.as_bytes()))
        .expect("the message should be readable back");

    assert_eq!(payload_of(ours), payload, "binary payload was altered in flight");

    let header_value = |name: &str| {
        ours.headers
            .iter()
            .find(|header| header.key == name)
            .map(|header| {
                BASE64
                    .decode(header.value_base64.as_deref().unwrap_or_default())
                    .unwrap()
            })
    };
    assert_eq!(
        header_value("content-type"),
        Some(b"application/octet-stream".to_vec())
    );
    assert_eq!(
        header_value("trace-id"),
        Some(vec![0xde, 0xad, 0xbe, 0xef]),
        "a binary header value must survive as bytes"
    );
}

#[tokio::test]
async fn a_json_message_is_stored_exactly_as_it_was_typed() {
    let bootstrap = broker!();
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    let run = marker("json");

    // Keys deliberately out of alphabetical order: a re-serialization
    // somewhere in the chain would reorder them, and what lands on the topic
    // must be the bytes the user wrote.
    let json = format!(r#"{{"zeta":1,"alpha":"{run}","nested":{{"b":2,"a":3}}}}"#);
    let message = NewPublishMessage {
        key: PublishField::null(),
        value: PublishField {
            encoding: PayloadEncoding::Json,
            text: json.clone(),
        },
        headers: Vec::new(),
    };

    let outcome = publish(&client, &connection, 0, &[message]).await;
    assert!(outcome.succeeded(), "unexpected failure: {outcome:?}");

    let read_back = read_partition(&client, &connection, 0).await;
    let ours = read_back
        .iter()
        .find(|message| String::from_utf8_lossy(&payload_of(message)).contains(&run))
        .expect("the message should be readable back");
    assert_eq!(String::from_utf8(payload_of(ours)).unwrap(), json);
}

#[tokio::test]
async fn a_null_value_is_published_as_a_tombstone_and_an_empty_one_is_not() {
    let bootstrap = broker!();
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    let run = marker("tombstone");

    let messages = vec![
        NewPublishMessage {
            key: PublishField::text(format!("{run}-null")),
            value: PublishField::null(),
            headers: Vec::new(),
        },
        NewPublishMessage {
            key: PublishField::text(format!("{run}-empty")),
            value: PublishField::text(""),
            headers: Vec::new(),
        },
    ];
    let outcome = publish(&client, &connection, 0, &messages).await;
    assert!(outcome.succeeded(), "unexpected failure: {outcome:?}");

    let read_back = read_partition(&client, &connection, 0).await;
    let find = |suffix: &str| {
        let wanted = format!("{run}-{suffix}");
        read_back
            .iter()
            .find(|message| key_of(message).as_deref() == Some(wanted.as_bytes()))
            .unwrap_or_else(|| panic!("the {suffix} message should be readable back"))
    };

    // Asserted on `payload_size_bytes`, not `payload_base64`: the read path
    // base64-encodes `payload().unwrap_or_default()`, so a null payload and an
    // empty one both come back as `Some("")` there. `payload_size_bytes` comes
    // from `payload().map(..)` and so is `None` only for a genuine null — it is
    // the one field that can tell a tombstone from an empty record.
    assert_eq!(
        find("null").payload_size_bytes,
        None,
        "a null value must be published as a tombstone"
    );
    assert_eq!(
        find("empty").payload_size_bytes,
        Some(0),
        "an empty value must be published as a zero-length payload, not a tombstone"
    );
}

#[tokio::test]
async fn publishing_to_an_unknown_topic_fails_and_does_not_create_it() {
    let bootstrap = broker!();
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap.clone());
    let unknown = marker("e2e-never-created");

    let records = encode_messages(
        &[text_message(None, "x")],
        &PublishLimits::for_max_message_size(MAX_MESSAGE_SIZE),
    )
    .unwrap();
    let outcome = client
        .publish_messages(
            &connection,
            &unknown,
            0,
            &records,
            MAX_MESSAGE_SIZE,
            Duration::from_secs(10),
        )
        .await
        .expect("a rejected publish is an outcome, not an error");

    assert!(outcome.delivered.is_empty(), "nothing can have been published");
    assert!(outcome.failure.is_some());

    // The topic must not exist afterwards. `allow.auto.create.topics=false` in
    // `publish_config` is what guarantees this; a broker with auto-creation
    // enabled would otherwise have quietly created a topic because someone
    // mistyped a name.
    let topics = client
        .list_topics(&connection, READ_TIMEOUT)
        .await
        .expect("listing topics should succeed");
    assert!(
        !topics.iter().any(|topic| topic.name == unknown),
        "a failed publish created the topic it was refused"
    );
}

#[tokio::test]
async fn a_partial_publish_reports_exactly_which_messages_landed() {
    let bootstrap = broker!();
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    let run = marker("partial");

    // Message 2 is over the broker's `message.max.bytes` (1 MB by default),
    // while 1 and 3 are tiny. Sending one at a time and stopping at the first
    // failure means exactly one record lands and one is never attempted — the
    // property the whole outcome type exists to report.
    let records = encode_messages(
        &[
            text_message(Some(&format!("{run}-1")), &format!("{run}-first")),
            text_message(Some(&format!("{run}-2")), &"x".repeat(2 * 1024 * 1024)),
            text_message(Some(&format!("{run}-3")), &format!("{run}-third")),
        ],
        &PublishLimits::for_max_message_size(MAX_MESSAGE_SIZE),
    )
    .expect("a 2 MB record is under the 12 MB app-side ceiling, so this passes validation");

    let outcome = client
        .publish_messages(
            &connection,
            TOPIC,
            0,
            &records,
            MAX_MESSAGE_SIZE,
            Duration::from_secs(15),
        )
        .await
        .expect("a partial publish is an outcome, not an error");

    assert_eq!(
        outcome.delivered.len(),
        1,
        "only the first message should have landed: {outcome:?}"
    );
    assert_eq!(outcome.delivered[0].index, 0);
    let failure = outcome.failure.as_ref().expect("message 2 must have failed");
    assert_eq!(failure.index, 1);
    assert_eq!(
        outcome.not_attempted, 1,
        "the third message must be reported as never attempted, not as failed"
    );

    // And the topic agrees: the third message is not there.
    let read_back = read_partition(&client, &connection, 0).await;
    let ours: Vec<&TopicMessage> = read_back
        .iter()
        .filter(|message| {
            key_of(message)
                .map(|key| String::from_utf8_lossy(&key).starts_with(&run))
                .unwrap_or(false)
        })
        .collect();
    assert_eq!(
        ours.len(),
        1,
        "the topic must hold exactly the one message the outcome claims"
    );
    assert_eq!(
        String::from_utf8(key_of(ours[0]).unwrap()).unwrap(),
        format!("{run}-1")
    );
}
