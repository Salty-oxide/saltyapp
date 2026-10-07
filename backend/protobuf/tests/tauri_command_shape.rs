//! Type-checks the body of `src-tauri`'s `connection_decode_protobuf`.
//!
//! `src-tauri` needs GTK/pkg-config to build and cannot be compiled in every
//! environment this workspace is developed in, so the parts of that command
//! that are easy to get wrong — the `spawn_blocking` closures and what they
//! move, the double-`?` on a `JoinHandle<Result<..>>`, the `change_context`
//! over `JoinError`, and `ProtobufDecoded` being serialisable across Tauri's
//! JSON IPC — are mirrored here against the real crate. The database and
//! registry lookups are stubbed; the decode orchestration is verbatim.

// The whole point of this file is that it mirrors `src-tauri`'s command
// *verbatim*, so clippy's suggestion to drop the `Ok(...?)` wrapper is
// declined here: taking it would make the mirror stop matching the code it
// exists to type-check, which is the one property that makes it useful.
#![allow(clippy::needless_question_mark)]

use error_stack::ResultExt;
use salty_core::AppError;
use salty_core::Result;
use salty_protobuf::{
    ProtobufDecodeStrategy, ProtobufDecoded, ProtobufSchemaSource, decide_decode_strategy,
};

async fn decode_protobuf(
    bytes: Vec<u8>,
    manual_schema: Option<String>,
    registry_schema: Option<String>,
) -> Result<ProtobufDecoded, AppError> {
    let strategy =
        decide_decode_strategy(&bytes, manual_schema.is_some(), registry_schema.is_some());

    let (body_offset, message_index) = match strategy {
        ProtobufDecodeStrategy::RawFields { body_offset } => {
            return Ok(tokio::task::spawn_blocking(move || {
                salty_protobuf::decode_without_schema(&bytes[body_offset..])
            })
            .await
            .change_context(AppError::Decode)
            .attach("protobuf decode task panicked")??);
        }
        ProtobufDecodeStrategy::ManualSchema {
            body_offset,
            message_index,
        } => {
            let schema =
                manual_schema.expect("the strategy is only chosen when a manual schema exists");
            return Ok(tokio::task::spawn_blocking(move || {
                salty_protobuf::decode(
                    &bytes[body_offset..],
                    &schema,
                    &message_index,
                    ProtobufSchemaSource::Manual,
                )
            })
            .await
            .change_context(AppError::Decode)
            .attach("protobuf decode task panicked")??);
        }
        ProtobufDecodeStrategy::SchemaRegistry {
            body_offset,
            message_index,
            ..
        } => (body_offset, message_index),
    };

    let schema_text =
        registry_schema.expect("the strategy is only chosen when an endpoint is configured");
    Ok(tokio::task::spawn_blocking(move || {
        salty_protobuf::decode(
            &bytes[body_offset..],
            &schema_text,
            &message_index,
            ProtobufSchemaSource::Registry,
        )
    })
    .await
    .change_context(AppError::Decode)
    .attach("protobuf decode task panicked")??)
}

const ORDER_PROTO: &str = r#"
    syntax = "proto3";
    message Order { string order_id = 1; int32 quantity = 2; }
"#;

fn encoded_order() -> Vec<u8> {
    let mut bytes = vec![0x0a, 0x05];
    bytes.extend_from_slice(b"ORD-1");
    bytes.extend_from_slice(&[0x10, 0x07]);
    bytes
}

fn confluent_framed(body: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0x00, 0x00, 0x00, 0x00, 0x2a, 0x00];
    bytes.extend_from_slice(body);
    bytes
}

#[tokio::test]
async fn decodes_with_a_manual_schema_through_the_command_shape() {
    let decoded = decode_protobuf(encoded_order(), Some(ORDER_PROTO.to_string()), None)
        .await
        .unwrap();

    assert_eq!(decoded.value["order_id"], "ORD-1");
    assert_eq!(decoded.source, ProtobufSchemaSource::Manual);
}

/// The registry path is the one that reports a different source than
/// `decode` itself can know about.
#[tokio::test]
async fn labels_a_registry_decode_as_coming_from_the_registry() {
    let decoded = decode_protobuf(
        confluent_framed(&encoded_order()),
        None,
        Some(ORDER_PROTO.to_string()),
    )
    .await
    .unwrap();

    assert_eq!(decoded.value["quantity"], 7);
    assert_eq!(decoded.source, ProtobufSchemaSource::Registry);
}

/// The Confluent header must be stripped before the message is decoded,
/// whichever side supplies the schema — with it left on, the magic byte is
/// read as a field key.
#[tokio::test]
async fn strips_the_confluent_header_on_the_manual_path_too() {
    let decoded = decode_protobuf(
        confluent_framed(&encoded_order()),
        Some(ORDER_PROTO.to_string()),
        None,
    )
    .await
    .unwrap();

    assert_eq!(decoded.value["order_id"], "ORD-1");
}

#[tokio::test]
async fn falls_back_to_field_numbers_with_no_schema_at_all() {
    let decoded = decode_protobuf(encoded_order(), None, None).await.unwrap();

    assert_eq!(decoded.value["1"], "ORD-1");
    assert_eq!(decoded.source, ProtobufSchemaSource::None);
}

/// Tauri's IPC is JSON, so the command's return type has to serialise — and
/// in the camelCase shape the frontend's `ProtobufDecodeResult` expects.
#[test]
fn serialises_across_the_ipc_boundary_in_camel_case() {
    let decoded = salty_protobuf::decode(
        &encoded_order(),
        ORDER_PROTO,
        &[0],
        ProtobufSchemaSource::Manual,
    )
    .unwrap();

    let json = serde_json::to_value(&decoded).unwrap();
    assert_eq!(json["source"], "manual");
    assert_eq!(json["messageType"], "Order");
    assert_eq!(json["value"]["order_id"], "ORD-1");
}
