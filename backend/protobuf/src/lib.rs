//! Protobuf payload decoding, deliberately shaped like `salty_avro` so
//! the two read the same way: detect the framing, decide a strategy without
//! doing any I/O, then decode.
//!
//! The one real difference is that protobuf has a useful answer even with no
//! schema at all — see [`wire::decode_raw`] — so this crate refuses far less
//! often than the Avro one does.

pub mod wire;

use error_stack::{Report, ResultExt};
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, SerializeOptions};
use protox::Compiler;
use protox::file::{ChainFileResolver, File, FileResolver, GoogleFileResolver};
use salty_core::AppError;
use salty_core::Result;
use serde::Serialize;
use std::sync::{Mutex, OnceLock};

/// The name a pasted schema is compiled under.
///
/// Protobuf requires every file to have one, and it shows up in error
/// messages — `schema.proto` reads as "the schema you pasted", which is what
/// it is, where protox's default of an empty name reads as a bug.
const SCHEMA_FILE_NAME: &str = "schema.proto";

/// What a Confluent-framed protobuf payload's header says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfluentHeader {
    pub schema_id: u32,
    /// Which message in the schema file the payload is an instance of, as a
    /// path of indexes: `[0]` is the first top-level message, `[0, 2]` the
    /// third message nested inside it.
    pub message_index: Vec<i32>,
    /// Where the protobuf message itself starts.
    pub body_offset: usize,
}

/// Parses Confluent's protobuf wire framing: magic byte `0x00`, a 4-byte
/// big-endian schema id, then a message-index path, then the message.
///
/// The index path is the part that differs from Avro's framing, and it has a
/// special case worth stating: an all-zero path — by far the most common,
/// since most schemas declare one message — is written as the **single byte
/// `0`** rather than as a length of 1 followed by a 0. Reading that byte as a
/// length would consume the first byte of the message as an index and shift
/// every field after it.
///
/// The indexes are zigzag varints, as Confluent's serializer writes them.
pub fn detect_confluent_header(bytes: &[u8]) -> Option<ConfluentHeader> {
    if bytes.len() < 5 || bytes[0] != 0x00 {
        return None;
    }
    let schema_id = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    let mut offset = 5;

    let (first, used) = wire::read_varint(bytes, offset)?;
    offset += used;
    if first == 0 {
        return Some(ConfluentHeader {
            schema_id,
            message_index: vec![0],
            body_offset: offset,
        });
    }

    let count = usize::try_from(wire::zigzag_decode(first)).ok()?;
    // A schema nested a hundred deep does not exist; a large count here means
    // the bytes are not Confluent-framed protobuf, and trusting it would walk
    // off the end of the payload.
    if count > 32 {
        return None;
    }
    let mut message_index = Vec::with_capacity(count);
    for _ in 0..count {
        let (raw, used) = wire::read_varint(bytes, offset)?;
        offset += used;
        message_index.push(i32::try_from(wire::zigzag_decode(raw)).ok()?);
    }
    Some(ConfluentHeader {
        schema_id,
        message_index,
        body_offset: offset,
    })
}

/// How one payload should be decoded, decided before any I/O happens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtobufDecodeStrategy {
    /// Decode with the topic's manually-entered `.proto`.
    ManualSchema {
        body_offset: usize,
        message_index: Vec<i32>,
    },
    /// Fetch this id from the Schema Registry, then decode.
    SchemaRegistry {
        schema_id: u32,
        body_offset: usize,
        message_index: Vec<i32>,
    },
    /// No schema anywhere — show field numbers and wire types.
    RawFields { body_offset: usize },
}

/// Decides how to decode, from what the bytes say and what is configured.
///
/// Pure, so the precedence is testable without a desktop toolchain — the same
/// reason `salty_avro::decide_decode_strategy` exists.
///
/// Two things differ from the Avro rule, both because of how protobuf is
/// framed:
///
/// 1. **The framing is decided separately from the schema.** A Confluent
///    header must be stripped whoever supplies the schema, so a manual schema
///    does *not* imply decoding the payload whole the way it does for Avro.
///    Leaving the header on would parse the magic byte as a field key.
/// 2. **There is no refusal.** With no schema from either source the wire
///    format still yields field numbers and values, which beats an error
///    message — so this returns a strategy in every case.
pub fn decide_decode_strategy(
    bytes: &[u8],
    has_manual_schema: bool,
    has_registry: bool,
) -> ProtobufDecodeStrategy {
    let header = detect_confluent_header(bytes);
    let (body_offset, message_index, schema_id) = match &header {
        Some(header) => (
            header.body_offset,
            header.message_index.clone(),
            Some(header.schema_id),
        ),
        // No header: the payload is a bare message, and a manual schema
        // describes it from byte zero.
        None => (0, vec![0], None),
    };

    if has_manual_schema {
        return ProtobufDecodeStrategy::ManualSchema {
            body_offset,
            message_index,
        };
    }
    match schema_id {
        Some(schema_id) if has_registry => ProtobufDecodeStrategy::SchemaRegistry {
            schema_id,
            body_offset,
            message_index,
        },
        _ => ProtobufDecodeStrategy::RawFields { body_offset },
    }
}

/// Where the schema used for a decode came from — shown to the user, because
/// "these are the field names from your schema" and "these are the field
/// numbers I guessed from the bytes" are very different claims.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ProtobufSchemaSource {
    Manual,
    Registry,
    /// Decoded from the wire format alone — keys are field numbers, not names.
    None,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtobufDecoded {
    pub value: serde_json::Value,
    pub source: ProtobufSchemaSource,
    /// The fully-qualified message name, when a schema named one.
    pub message_type: Option<String>,
}

/// Serves a single in-memory `.proto` to the compiler.
///
/// protox reads schemas from disk by default; the schemas here are pasted
/// into the Schema tab or fetched from a registry and never touch the
/// filesystem. Writing them to a temp file to compile them would be both
/// slower and a way to leak a user's schema onto disk.
struct InMemoryResolver {
    source: String,
}

impl FileResolver for InMemoryResolver {
    fn open_file(&self, name: &str) -> std::result::Result<File, protox::Error> {
        if name == SCHEMA_FILE_NAME {
            File::from_source(name, &self.source)
        } else {
            Err(protox::Error::file_not_found(name))
        }
    }
}

/// Compiles `.proto` source text into a descriptor pool.
///
/// `GoogleFileResolver` is chained in so a schema may `import
/// "google/protobuf/timestamp.proto"` and friends — those are bundled with
/// protox, and a schema using `Timestamp` (most schemas that carry an event
/// time) would otherwise fail to compile with a confusing "file not found".
/// Compiled schemas, keyed by their source text.
///
/// `decode` is called once per message the user opens, and compiling a
/// `.proto` is not cheap — protox parses and links it, then `DescriptorPool`
/// loads the result, and a schema importing a well-known type pulls those in
/// too. Clicking through a hundred grid rows in Protobuf mode was doing all
/// of that a hundred times for the same unchanged text. It runs on the
/// blocking pool so the UI never stalled, but it is tens of milliseconds of
/// pure waste per row.
///
/// Keyed by the text itself rather than by topic, so a registry schema and a
/// pasted one that happen to be identical share an entry and editing a pasted
/// schema can never serve the previous version. Bounded because the key is
/// user-supplied and unbounded growth on a long session is a leak; the bound
/// is generous next to the handful of schemas one session touches.
fn schema_cache() -> &'static Mutex<Vec<(String, DescriptorPool)>> {
    static CACHE: OnceLock<Mutex<Vec<(String, DescriptorPool)>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(Vec::new()))
}

const SCHEMA_CACHE_ENTRIES: usize = 8;

fn compile(proto_text: &str) -> Result<DescriptorPool, AppError> {
    // `unwrap_or_else(PoisonError::into_inner)` rather than `unwrap`: a panic
    // in another thread while it held this lock says nothing about whether
    // the Vec is usable, and poisoning an app-wide cache into permanent
    // failure is a worse outcome than reusing it.
    if let Some(pool) = schema_cache()
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .iter()
        .find(|(text, _)| text == proto_text)
        .map(|(_, pool)| pool.clone())
    {
        return Ok(pool);
    }

    let pool = compile_uncached(proto_text)?;

    let mut cache = schema_cache().lock().unwrap_or_else(|err| err.into_inner());
    // Another thread may have inserted the same schema while this one was
    // compiling. Harmless either way — both pools are equivalent — but
    // de-duplicating keeps the bound meaningful.
    if !cache.iter().any(|(text, _)| text == proto_text) {
        if cache.len() >= SCHEMA_CACHE_ENTRIES {
            cache.remove(0);
        }
        cache.push((proto_text.to_string(), pool.clone()));
    }
    Ok(pool)
}

fn compile_uncached(proto_text: &str) -> Result<DescriptorPool, AppError> {
    let mut resolver = ChainFileResolver::new();
    resolver.add(InMemoryResolver {
        source: proto_text.to_string(),
    });
    resolver.add(GoogleFileResolver::new());

    let mut compiler = Compiler::with_file_resolver(resolver);
    // Without this, `file_descriptor_set()` returns only the file that was
    // explicitly opened, and `DescriptorPool` then rejects it with "imported
    // file ... has not been added" — so any schema importing a well-known
    // type (`Timestamp`, `Duration`, `Struct`) failed to load even though it
    // compiled cleanly.
    compiler.include_imports(true);
    compiler
        .open_file(SCHEMA_FILE_NAME)
        .change_context(AppError::Decode)
        .attach("invalid .proto schema")?;

    DescriptorPool::from_file_descriptor_set(compiler.file_descriptor_set())
        .change_context(AppError::Decode)
        .attach("the .proto schema compiled but could not be loaded")
}

/// Parses `proto_text` without decoding anything — rejects an invalid schema
/// at save time rather than the first time someone opens a message with it.
pub fn validate_schema(proto_text: &str) -> Result<(), AppError> {
    let pool = compile(proto_text)?;
    if pool
        .files()
        .find(|file| file.name() == SCHEMA_FILE_NAME)
        .is_some_and(|file| file.messages().len() == 0)
    {
        return Err(Report::new(AppError::Decode).attach("the .proto schema declares no messages"));
    }
    Ok(())
}

/// Walks a Confluent message-index path to the message it names.
///
/// The path indexes into the file's top-level messages, then into each one's
/// nested messages — `[1, 0]` is the first message nested inside the second
/// top-level one. An out-of-range index means the schema and the payload
/// disagree, which is worth saying plainly: it is what happens when a topic
/// is pointed at the wrong schema.
fn message_at(pool: &DescriptorPool, index: &[i32]) -> Result<MessageDescriptor, AppError> {
    let file = pool
        .files()
        .find(|file| file.name() == SCHEMA_FILE_NAME)
        .ok_or_else(|| {
            Report::new(AppError::Decode).attach("the schema file went missing after compiling")
        })?;

    let mut messages: Vec<MessageDescriptor> = file.messages().collect();
    let mut current: Option<MessageDescriptor> = None;
    for step in index {
        let position = usize::try_from(*step)
            .ok()
            .filter(|position| *position < messages.len())
            .ok_or_else(|| {
                Report::new(AppError::Decode).attach(format!(
                    "the payload names message index {step}, which this schema does not have — \
                     the topic may be pointed at the wrong schema"
                ))
            })?;
        let message = messages[position].clone();
        messages = message.child_messages().collect();
        current = Some(message);
    }

    current.ok_or_else(|| {
        Report::new(AppError::Decode).attach("the .proto schema declares no messages")
    })
}

/// Decodes `bytes` against `proto_text`, returning JSON the existing tree
/// viewer can render.
///
/// `use_proto_field_name` is set so fields come out named as the `.proto`
/// declares them (`order_id`) rather than lowerCamelCased by protobuf's JSON
/// mapping (`orderId`) — the schema in front of the user is the one they
/// pasted, and silently renaming its fields makes the two disagree.
///
/// `stringify_64_bit_integers` follows the protobuf JSON spec: an int64 past
/// 2^53 cannot survive a JSON number, and the frontend parses this with
/// `JSON.parse`. Emitting it as a number would quietly round Kafka offsets
/// and snowflake ids.
pub fn decode(
    bytes: &[u8],
    proto_text: &str,
    message_index: &[i32],
    source: ProtobufSchemaSource,
) -> Result<ProtobufDecoded, AppError> {
    let pool = compile(proto_text)?;
    let descriptor = resolve_message(&pool, message_index, source)?;
    let message = DynamicMessage::decode(descriptor.clone(), bytes)
        .change_context(AppError::Decode)
        .attach("the payload does not match this .proto schema")?;

    let mut serializer = serde_json::Serializer::pretty(Vec::new());
    let options = SerializeOptions::new()
        .use_proto_field_name(true)
        .stringify_64_bit_integers(true);
    message
        .serialize_with_options(&mut serializer, &options)
        .change_context(AppError::Decode)
        .attach("the decoded message could not be rendered as JSON")?;
    let json = serializer.into_inner();
    let value = serde_json::from_slice(&json)
        .change_context(AppError::Decode)
        .attach("the decoded message could not be rendered as JSON")?;

    Ok(ProtobufDecoded {
        value,
        // Passed in rather than inferred: the same decode serves a pasted
        // schema and one fetched from the registry, and the UI says which of
        // those produced what is on screen.
        source,
        message_type: Some(descriptor.full_name().to_string()),
    })
}

/// The single message a schema declares, when it declares exactly one.
///
/// "Exactly one" is the whole point: with one message there is nothing to
/// choose between, so using it cannot be wrong. With two or more, guessing
/// would decode against the wrong type and produce plausible nonsense, which
/// is worse than saying so.
fn sole_message(pool: &DescriptorPool) -> Option<MessageDescriptor> {
    let file = pool.files().find(|file| file.name() == SCHEMA_FILE_NAME)?;
    let mut messages = file.messages();
    let first = messages.next()?;
    messages.next().is_none().then_some(first)
}

/// Picks the message to decode against, given where the schema came from.
///
/// The message index is read from the payload's Confluent header, and that
/// header was written against the **producer's** schema file. A registry
/// schema *is* that file, so an index it doesn't have means the payload and
/// the schema genuinely disagree and the error is the right answer.
///
/// A pasted schema is a different situation. The usual way to write one is to
/// copy out the single message you care about, which renumbers everything —
/// so a topic whose payloads say "message index 2" refused to decode against
/// a perfectly good one-message schema, telling the user their schema was
/// wrong when it wasn't. Where the pasted schema declares exactly one
/// message there is nothing to disambiguate, so that one is used.
fn resolve_message(
    pool: &DescriptorPool,
    message_index: &[i32],
    source: ProtobufSchemaSource,
) -> Result<MessageDescriptor, AppError> {
    match message_at(pool, message_index) {
        Ok(descriptor) => Ok(descriptor),
        Err(report) if source == ProtobufSchemaSource::Manual => sole_message(pool).ok_or(report),
        Err(report) => Err(report),
    }
}

/// Decodes using the wire format alone — field numbers instead of names.
pub fn decode_without_schema(bytes: &[u8]) -> Result<ProtobufDecoded, AppError> {
    let value = wire::decode_raw(bytes).ok_or_else(|| {
        Report::new(AppError::Decode).attach(
            "payload isn't valid protobuf — no schema is set for this topic and the bytes \
             don't parse as protobuf wire format either",
        )
    })?;
    Ok(ProtobufDecoded {
        value,
        source: ProtobufSchemaSource::None,
        message_type: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORDER_PROTO: &str = r#"
        syntax = "proto3";
        package shop;
        message Order {
          string order_id = 1;
          int32 quantity = 2;
          bool paid = 3;
          repeated string tags = 4;
          Customer customer = 5;
          message Line { string sku = 1; }
        }
        message Customer { string name = 1; }
    "#;

    /// Hand-builds the wire encoding for `Order { order_id, quantity }` —
    /// field 1 (length-delimited string) and field 2 (varint).
    fn encoded_order(order_id: &str, quantity: u8) -> Vec<u8> {
        let mut bytes = vec![0x0a, order_id.len() as u8];
        bytes.extend_from_slice(order_id.as_bytes());
        bytes.extend_from_slice(&[0x10, quantity]);
        bytes
    }

    fn confluent_framed(schema_id: u32, index: &[u8], body: &[u8]) -> Vec<u8> {
        let mut bytes = vec![0x00];
        bytes.extend_from_slice(&schema_id.to_be_bytes());
        bytes.extend_from_slice(index);
        bytes.extend_from_slice(body);
        bytes
    }

    mod header {
        use super::*;

        #[test]
        fn reads_the_schema_id_and_the_single_zero_index_shorthand() {
            let bytes = confluent_framed(42, &[0x00], &encoded_order("ORD-1", 2));

            let header = detect_confluent_header(&bytes).expect("a Confluent header");
            assert_eq!(header.schema_id, 42);
            assert_eq!(header.message_index, vec![0]);
            assert_eq!(header.body_offset, 6);
        }

        /// The shorthand is the case most payloads take, and reading that
        /// single `0` as a *length* would eat the first byte of the message.
        #[test]
        fn the_shorthand_does_not_consume_a_byte_of_the_message() {
            let body = encoded_order("ORD-1", 2);
            let bytes = confluent_framed(7, &[0x00], &body);

            let header = detect_confluent_header(&bytes).unwrap();
            assert_eq!(&bytes[header.body_offset..], &body[..]);
        }

        #[test]
        fn reads_an_explicit_nested_index_path() {
            // Length 2, then indexes 1 and 0 — all zigzag encoded.
            let bytes = confluent_framed(9, &[0x04, 0x02, 0x00], &encoded_order("ORD-1", 1));

            let header = detect_confluent_header(&bytes).unwrap();
            assert_eq!(header.message_index, vec![1, 0]);
            assert_eq!(header.body_offset, 8);
        }

        #[test]
        fn rejects_a_payload_with_no_magic_byte() {
            assert_eq!(detect_confluent_header(&encoded_order("ORD-1", 1)), None);
        }

        #[test]
        fn rejects_a_payload_too_short_to_carry_a_header() {
            assert_eq!(detect_confluent_header(&[0x00, 0x00, 0x01]), None);
        }

        /// An absurd index count means these are not Confluent-framed bytes;
        /// trusting it walks off the end of the payload.
        #[test]
        fn rejects_an_implausible_index_count() {
            let bytes = confluent_framed(1, &[0x7f], &encoded_order("ORD-1", 1));

            assert_eq!(detect_confluent_header(&bytes), None);
        }
    }

    mod strategy {
        use super::*;

        #[test]
        fn a_manual_schema_wins_over_the_registry() {
            let bytes = confluent_framed(5, &[0x00], &encoded_order("ORD-1", 1));

            assert_eq!(
                decide_decode_strategy(&bytes, true, true),
                ProtobufDecodeStrategy::ManualSchema {
                    body_offset: 6,
                    message_index: vec![0]
                }
            );
        }

        /// The header has to be stripped whoever supplies the schema — this
        /// is where protobuf differs from Avro, whose manual-schema path
        /// decodes the payload whole.
        #[test]
        fn a_manual_schema_still_strips_the_confluent_header() {
            let bytes = confluent_framed(5, &[0x00], &encoded_order("ORD-1", 1));

            match decide_decode_strategy(&bytes, true, false) {
                ProtobufDecodeStrategy::ManualSchema { body_offset, .. } => {
                    assert_eq!(body_offset, 6)
                }
                other => panic!("expected ManualSchema, got {other:?}"),
            }
        }

        #[test]
        fn a_bare_message_with_a_manual_schema_is_decoded_from_byte_zero() {
            match decide_decode_strategy(&encoded_order("ORD-1", 1), true, false) {
                ProtobufDecodeStrategy::ManualSchema { body_offset, .. } => {
                    assert_eq!(body_offset, 0)
                }
                other => panic!("expected ManualSchema, got {other:?}"),
            }
        }

        #[test]
        fn a_confluent_header_with_a_registry_and_no_manual_schema_uses_the_registry() {
            let bytes = confluent_framed(11, &[0x00], &encoded_order("ORD-1", 1));

            assert_eq!(
                decide_decode_strategy(&bytes, false, true),
                ProtobufDecodeStrategy::SchemaRegistry {
                    schema_id: 11,
                    body_offset: 6,
                    message_index: vec![0]
                }
            );
        }

        /// Unlike Avro, there is no refusal: field numbers beat an error.
        #[test]
        fn falls_back_to_raw_fields_with_no_schema_anywhere() {
            assert_eq!(
                decide_decode_strategy(&encoded_order("ORD-1", 1), false, false),
                ProtobufDecodeStrategy::RawFields { body_offset: 0 }
            );
        }

        #[test]
        fn falls_back_to_raw_fields_when_a_header_has_no_registry_to_resolve_it() {
            let bytes = confluent_framed(3, &[0x00], &encoded_order("ORD-1", 1));

            assert_eq!(
                decide_decode_strategy(&bytes, false, false),
                ProtobufDecodeStrategy::RawFields { body_offset: 6 }
            );
        }
    }

    mod schema_decoding {
        use super::*;

        #[test]
        fn decodes_a_message_with_its_schema() {
            let decoded = decode(
                &encoded_order("ORD-42", 3),
                ORDER_PROTO,
                &[0],
                ProtobufSchemaSource::Manual,
            )
            .unwrap();

            assert_eq!(decoded.value["order_id"], "ORD-42");
            assert_eq!(decoded.value["quantity"], 3);
            assert_eq!(decoded.message_type.as_deref(), Some("shop.Order"));
        }

        /// The user pasted `order_id`; renaming it to `orderId` on the way to
        /// the screen makes the schema and the payload disagree.
        #[test]
        fn keeps_the_field_names_the_schema_declares() {
            let decoded = decode(
                &encoded_order("ORD-42", 3),
                ORDER_PROTO,
                &[0],
                ProtobufSchemaSource::Manual,
            )
            .unwrap();

            assert!(decoded.value.get("order_id").is_some());
            assert!(decoded.value.get("orderId").is_none());
        }

        #[test]
        fn walks_an_index_path_to_a_top_level_message() {
            // `[1]` is Customer: field 1 is its `name`.
            let mut bytes = vec![0x0a, 0x03];
            bytes.extend_from_slice(b"Ada");

            let decoded = decode(&bytes, ORDER_PROTO, &[1], ProtobufSchemaSource::Manual).unwrap();

            assert_eq!(decoded.message_type.as_deref(), Some("shop.Customer"));
            assert_eq!(decoded.value["name"], "Ada");
        }

        #[test]
        fn walks_an_index_path_into_a_nested_message() {
            let mut bytes = vec![0x0a, 0x05];
            bytes.extend_from_slice(b"SKU-1");

            let decoded =
                decode(&bytes, ORDER_PROTO, &[0, 0], ProtobufSchemaSource::Manual).unwrap();

            assert_eq!(decoded.message_type.as_deref(), Some("shop.Order.Line"));
            assert_eq!(decoded.value["sku"], "SKU-1");
        }

        /// The message the payload names isn't in this schema — which is what
        /// a topic pointed at the wrong schema looks like.
        #[test]
        fn reports_an_index_the_schema_does_not_have() {
            let error = decode(
                &encoded_order("ORD-1", 1),
                ORDER_PROTO,
                &[9],
                ProtobufSchemaSource::Manual,
            )
            .unwrap_err();

            assert!(format!("{error:?}").contains("message index 9"));
        }

        #[test]
        fn reports_a_payload_that_does_not_match_the_schema() {
            // Wire type 3 (start group) is not something this schema has.
            let error = decode(
                &[0xff, 0xff, 0xff],
                ORDER_PROTO,
                &[0],
                ProtobufSchemaSource::Manual,
            )
            .unwrap_err();

            assert!(format!("{error:?}").contains("does not match"));
        }
    }

    mod validation {
        use super::*;

        #[test]
        fn accepts_a_well_formed_schema() {
            assert!(validate_schema(ORDER_PROTO).is_ok());
        }

        #[test]
        fn rejects_a_syntactically_invalid_schema() {
            assert!(validate_schema("syntax = \"proto3\"; message {").is_err());
        }

        /// A file that compiles but declares nothing to decode against is
        /// useless, and saying so at save time beats a confusing failure the
        /// first time a message is opened.
        #[test]
        fn rejects_a_schema_with_no_messages() {
            assert!(validate_schema("syntax = \"proto3\"; package empty;").is_err());
        }

        /// Most schemas that carry an event time import a well-known type;
        /// without them bundled that fails with a bare "file not found".
        #[test]
        fn accepts_a_schema_importing_a_well_known_type() {
            let proto = r#"
                syntax = "proto3";
                import "google/protobuf/timestamp.proto";
                message Event { google.protobuf.Timestamp at = 1; }
            "#;

            assert!(
                validate_schema(proto).is_ok(),
                "well-known imports should resolve"
            );
        }
    }

    mod schemaless {
        use super::*;

        #[test]
        fn shows_field_numbers_when_no_schema_is_available() {
            let decoded = decode_without_schema(&encoded_order("ORD-42", 3)).unwrap();

            assert_eq!(decoded.value["1"], "ORD-42");
            assert_eq!(decoded.value["2"], 3);
            assert_eq!(decoded.source, ProtobufSchemaSource::None);
        }

        #[test]
        fn reports_bytes_that_are_not_protobuf_at_all() {
            assert!(decode_without_schema(b"this is plain text, not protobuf").is_err());
        }

        #[test]
        fn repeats_become_an_array_rather_than_overwriting() {
            // Field 4 written twice — protobuf's encoding for `repeated`.
            let mut bytes = vec![0x22, 0x03];
            bytes.extend_from_slice(b"one");
            bytes.extend_from_slice(&[0x22, 0x03]);
            bytes.extend_from_slice(b"two");

            let decoded = decode_without_schema(&bytes).unwrap();

            assert_eq!(decoded.value["4"], serde_json::json!(["one", "two"]));
        }

        /// protobuf writes a negative int32/int64 as a 10-byte two's
        /// complement varint, so -1 arrives as 18446744073709551615 — past
        /// what a JSON number holds exactly, and meaningless without the
        /// signed reading beside it.
        #[test]
        fn annotates_a_varint_too_large_for_a_json_number() {
            // Field 2, varint, all ones: -1 as an int64.
            let bytes = vec![
                0x10, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
            ];

            let decoded = decode_without_schema(&bytes).unwrap();

            assert_eq!(decoded.value["2"], "18446744073709551615 (signed -1)");
        }

        /// The previous rule guessed zigzag whenever it differed from the
        /// unsigned reading, which is *every odd number* — `quantity: 3` was
        /// rendered as "3 (zigzag -2)".
        #[test]
        fn leaves_an_ordinary_odd_number_alone() {
            let decoded = decode_without_schema(&encoded_order("ORD-1", 3)).unwrap();

            assert_eq!(decoded.value["2"], 3);
        }

        #[test]
        fn recurses_into_a_nested_message() {
            let inner = encoded_order("ORD-1", 1);
            let mut bytes = vec![0x2a, inner.len() as u8];
            bytes.extend_from_slice(&inner);

            let decoded = decode_without_schema(&bytes).unwrap();

            assert_eq!(decoded.value["5"]["1"], "ORD-1");
        }
    }
}

#[cfg(test)]
mod source_tests {
    use super::*;

    const MINIMAL: &str = r#"syntax = "proto3"; message M { string a = 1; }"#;

    /// The registry path and the manual path share one decode, so the caller
    /// has to be able to say which one it is — the UI labels the difference.
    #[test]
    fn reports_the_schema_source_it_was_given() {
        let bytes = vec![0x0a, 0x01, b'x'];

        let decoded = decode(&bytes, MINIMAL, &[0], ProtobufSchemaSource::Registry).unwrap();

        assert_eq!(decoded.source, ProtobufSchemaSource::Registry);
    }
}

#[cfg(test)]
mod manual_index_fallback {
    use super::*;

    const ONE_MESSAGE: &str = r#"syntax = "proto3"; message Order { string order_id = 1; }"#;
    const TWO_MESSAGES: &str = r#"
        syntax = "proto3";
        message Order { string order_id = 1; }
        message Customer { string order_id = 1; }
    "#;

    fn body() -> Vec<u8> {
        let mut bytes = vec![0x0a, 0x05];
        bytes.extend_from_slice(b"ORD-1");
        bytes
    }

    /// The realistic way a schema gets pasted: copy out the one message you
    /// care about from a larger file. That renumbers it to 0, while the
    /// payload's header still names its position in the producer's file.
    #[test]
    fn a_pasted_single_message_schema_ignores_a_header_index_it_does_not_have() {
        let decoded = decode(&body(), ONE_MESSAGE, &[2], ProtobufSchemaSource::Manual).unwrap();

        assert_eq!(decoded.value["order_id"], "ORD-1");
        assert_eq!(decoded.message_type.as_deref(), Some("Order"));
    }

    /// With more than one message there is a real choice to get wrong, and
    /// decoding against the wrong type produces plausible nonsense.
    #[test]
    fn a_pasted_multi_message_schema_still_reports_an_index_it_does_not_have() {
        let error = decode(&body(), TWO_MESSAGES, &[7], ProtobufSchemaSource::Manual).unwrap_err();

        assert!(format!("{error:?}").contains("message index 7"));
    }

    /// A registry schema *is* the producer's file, so a mismatch there is a
    /// genuine disagreement worth reporting rather than papering over.
    #[test]
    fn a_registry_schema_never_falls_back() {
        let error = decode(&body(), ONE_MESSAGE, &[2], ProtobufSchemaSource::Registry).unwrap_err();

        assert!(format!("{error:?}").contains("message index 2"));
    }

    /// The ordinary case must be untouched: a valid index still wins.
    #[test]
    fn a_valid_index_is_still_honoured() {
        let decoded = decode(&body(), TWO_MESSAGES, &[1], ProtobufSchemaSource::Manual).unwrap();

        assert_eq!(decoded.message_type.as_deref(), Some("Customer"));
    }
}

#[cfg(test)]
mod cache_tests {
    use super::*;

    const SCHEMA: &str = r#"
        syntax = "proto3";
        import "google/protobuf/timestamp.proto";
        message Cached { string a = 1; google.protobuf.Timestamp at = 2; }
    "#;

    /// Recompiling the same text per message was tens of milliseconds of
    /// waste on every grid row.
    #[test]
    fn reuses_a_compiled_schema() {
        // Warm it, so the measurement below isn't the first compile.
        compile(SCHEMA).unwrap();

        let started = std::time::Instant::now();
        for _ in 0..50 {
            compile(SCHEMA).unwrap();
        }

        assert!(
            started.elapsed() < std::time::Duration::from_millis(50),
            "50 cache hits took {:?} — the schema is being recompiled",
            started.elapsed()
        );
    }

    /// Keyed by the text, so editing a pasted schema can never serve the
    /// version before the edit.
    #[test]
    fn a_changed_schema_is_compiled_again() {
        let first = compile(r#"syntax = "proto3"; message A { string a = 1; }"#).unwrap();
        let second =
            compile(r#"syntax = "proto3"; message A { string a = 1; int32 b = 2; }"#).unwrap();

        let fields = |pool: &DescriptorPool| {
            pool.files()
                .find(|file| file.name() == SCHEMA_FILE_NAME)
                .and_then(|file| file.messages().next())
                .map(|message| message.fields().len())
                .unwrap_or(0)
        };
        assert_eq!(fields(&first), 1);
        assert_eq!(fields(&second), 2);
    }

    /// The key is user-supplied, so the cache must not grow without bound
    /// over a long session.
    #[test]
    fn is_bounded() {
        for i in 0..(SCHEMA_CACHE_ENTRIES * 3) {
            compile(&format!(
                r#"syntax = "proto3"; message M{i} {{ string a = 1; }}"#
            ))
            .unwrap();
        }

        let held = schema_cache()
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .len();
        assert!(held <= SCHEMA_CACHE_ENTRIES, "cache held {held} entries");
    }

    /// A cached pool has to still decode — a stale or half-built entry would
    /// be worse than no cache at all.
    #[test]
    fn a_cached_schema_still_decodes() {
        let proto = r#"syntax = "proto3"; message Order { string order_id = 1; }"#;
        let mut body = vec![0x0a, 0x05];
        body.extend_from_slice(b"ORD-1");

        let first = decode(&body, proto, &[0], ProtobufSchemaSource::Manual).unwrap();
        let second = decode(&body, proto, &[0], ProtobufSchemaSource::Manual).unwrap();

        assert_eq!(first.value["order_id"], "ORD-1");
        assert_eq!(second.value, first.value);
    }
}
