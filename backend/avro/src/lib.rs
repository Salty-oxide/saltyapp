use apache_avro::Schema;
use apache_avro::schema::{DecimalSchema, NamesRef, ResolvedSchema};
use apache_avro::types::Value;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, NaiveTime, Timelike};
use error_stack::{Report, ResultExt};
use num_bigint::BigInt;
use salty_core::AppError;
use salty_core::Result;

/// Confluent wire-format header: a leading magic byte (0x00) followed by a
/// 4-byte big-endian schema id. Mirrors the frontend's
/// `detectConfluentAvro` (`payloadDecoding.ts`) — used here to decide
/// whether a payload is eligible for a Schema Registry lookup.
pub fn detect_wire_format(bytes: &[u8]) -> Option<u32> {
    if bytes.len() < 5 || bytes[0] != 0x00 {
        return None;
    }
    Some(u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]))
}

/// Parses `schema_json` without decoding anything — used to reject an
/// invalid manually-entered schema at save time instead of only failing
/// later, the first time someone tries to view a message with it.
pub fn validate_schema(schema_json: &str) -> Result<(), AppError> {
    Schema::parse_str(schema_json)
        .change_context(AppError::Decode)
        .attach("invalid Avro schema")?;
    Ok(())
}

/// Avro Object Container File magic: the ASCII bytes `Obj` followed by
/// format version 1. Distinguishes a self-framed payload (schema embedded
/// in the file header) from a bare Confluent-wire-format datum, which never
/// starts with this sequence.
const CONTAINER_FILE_MAGIC: [u8; 4] = [0x4f, 0x62, 0x6a, 0x01];

pub fn detect_container_file(bytes: &[u8]) -> bool {
    bytes.starts_with(&CONTAINER_FILE_MAGIC)
}

/// How one payload should be decoded — the whole precedence rule, decided
/// before any I/O happens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AvroDecodeStrategy {
    /// The payload carries its own schema; nothing else is consulted.
    ContainerFile,
    /// Decode the payload whole with the topic's manually-entered schema.
    ManualSchema,
    /// Confluent wire format: fetch this id from the registry and decode the
    /// bytes *after* the 5-byte header.
    SchemaRegistry { schema_id: u32 },
}

/// Why a payload cannot be decoded as Avro at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AvroDecodeRefusal {
    /// Not a container file, no manual schema, and no Confluent header —
    /// there is simply nothing that says how to read these bytes.
    NoSchemaAvailable,
    /// A Confluent header names a schema id, but this connection has no
    /// Schema Registry to resolve it against.
    NoRegistryConfigured,
}

/// Decides how to decode a payload, given what the bytes say and what is
/// configured. Pure, so the precedence can be pinned by tests — it used to
/// live inline in the Tauri command, which needs a desktop toolchain to
/// build and so could not be tested at all.
///
/// The order is deliberate and each step earns its place:
///
/// 1. **A container file wins outright.** Its schema is inside the payload,
///    and its framing is incompatible with both of the others — a manual or
///    registry schema applied to it would decode the file header as data.
/// 2. **A manual schema beats the registry.** Pasting one into the Schema
///    tab is an explicit override, and the reason to paste one is usually
///    that registry lookup is not doing what you want. It also decodes the
///    payload *whole*, with no header to strip.
/// 3. **Otherwise the Confluent header decides**, and needs a registry to
///    resolve against.
pub fn decide_decode_strategy(
    bytes: &[u8],
    has_manual_schema: bool,
    has_registry: bool,
) -> std::result::Result<AvroDecodeStrategy, AvroDecodeRefusal> {
    if detect_container_file(bytes) {
        return Ok(AvroDecodeStrategy::ContainerFile);
    }
    if has_manual_schema {
        return Ok(AvroDecodeStrategy::ManualSchema);
    }
    match detect_wire_format(bytes) {
        Some(schema_id) if has_registry => Ok(AvroDecodeStrategy::SchemaRegistry { schema_id }),
        Some(_) => Err(AvroDecodeRefusal::NoRegistryConfigured),
        None => Err(AvroDecodeRefusal::NoSchemaAvailable),
    }
}

impl AvroDecodeRefusal {
    /// The message shown to the user, kept next to the rule that produces it.
    pub fn message(self) -> &'static str {
        match self {
            AvroDecodeRefusal::NoSchemaAvailable => {
                "payload has no Confluent Avro wire-format header and no manual schema is set for this topic"
            }
            AvroDecodeRefusal::NoRegistryConfigured => {
                "no manual schema is set for this topic and this connection has no Schema Registry configured"
            }
        }
    }
}

/// Decodes an Avro Object Container File: unlike `decode`, no external
/// schema is needed — the writer schema is embedded in the file header, so
/// `apache_avro::Reader` extracts and applies it itself. A single-record
/// container (the common case for a producer that wraps each Kafka message
/// as its own container file) decodes to that record directly; a container
/// with multiple records decodes to a JSON array of them.
pub fn decode_container(bytes: &[u8]) -> Result<serde_json::Value, AppError> {
    let reader = apache_avro::Reader::new(bytes)
        .change_context(AppError::Decode)
        .attach("payload isn't a valid Avro container file")?;

    // Cloned before the iterator consumes `reader`: the writer schema is
    // what gives `to_json` the scale of a decimal and the field order of a
    // record, and it is only reachable through the reader that is about to
    // be moved.
    let schema = reader.writer_schema().clone();

    let mut records = Vec::new();
    for value in reader {
        let value = value
            .change_context(AppError::Decode)
            .attach("payload isn't a valid Avro container file")?;
        records.push(to_json(value, &schema));
    }

    match records.len() {
        0 => Err(Report::new(AppError::Decode).attach("Avro container file has no records")),
        1 => Ok(records.into_iter().next().unwrap()),
        _ => Ok(serde_json::Value::Array(records)),
    }
}

/// Decodes `bytes` as a single Avro value against `schema_json`, returning
/// it as a `serde_json::Value` the existing JSON tree viewer can render
/// directly. No container-file framing — Kafka messages are individual
/// records, not Avro Object Container Files.
pub fn decode(bytes: &[u8], schema_json: &str) -> Result<serde_json::Value, AppError> {
    let schema = Schema::parse_str(schema_json)
        .change_context(AppError::Decode)
        .attach("invalid Avro schema")?;
    let mut reader = bytes;
    // `GenericDatumReader` replaced the free `from_avro_datum`, which
    // apache-avro 0.22 deprecates. Same single-datum read (no container
    // framing), same "reader schema defaults to the writer schema".
    let value = apache_avro::reader::datum::GenericDatumReader::builder(&schema)
        .build()
        .change_context(AppError::Decode)
        .attach("invalid Avro schema")?
        .read_value(&mut reader)
        .change_context(AppError::Decode)
        .attach("payload isn't valid Avro for this schema")?;
    Ok(to_json(value, &schema))
}

/// Converts a decoded Avro value to JSON *with its schema alongside*.
///
/// The schema is not optional decoration — two of the things the payload
/// viewer shows can only be got from it:
///
/// - **A decimal's scale.** `Value::Decimal` carries the unscaled integer
///   and nothing else, so `1234` is `12.34`, `1.234` or `1234` depending on
///   a number that lives only in the schema.
/// - **Named type references.** A schema that names a record once and
///   refers to it later (`Schema::Ref`) would otherwise lose the field
///   schemas for every occurrence after the first.
///
/// `ResolvedSchema` builds the name → schema table for the second. If it
/// fails, decoding still proceeds against an empty table: a `Ref` then
/// simply decodes without schema hints, which is what happened for
/// everything before this.
fn to_json(value: Value, schema: &Schema) -> serde_json::Value {
    let resolved = ResolvedSchema::try_from(schema).ok();
    let empty = NamesRef::new();
    let names = resolved.as_ref().map_or(&empty, |r| r.get_names());
    to_json_value(value, Some(schema), names)
}

/// Follows a `Schema::Ref` to the named schema it stands for. A reference
/// that resolves to nothing (a schema whose names table could not be built)
/// becomes `None` rather than being treated as a type in its own right.
fn deref<'s>(schema: Option<&'s Schema>, names: &NamesRef<'s>) -> Option<&'s Schema> {
    match schema {
        Some(Schema::Ref { name }) => names.get(name).copied(),
        other => other,
    }
}

fn to_json_value<'s>(
    value: Value,
    schema: Option<&'s Schema>,
    names: &NamesRef<'s>,
) -> serde_json::Value {
    let schema = deref(schema, names);
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(b) => serde_json::Value::Bool(b),
        Value::Int(i) => serde_json::Value::from(i),
        Value::Long(i) => serde_json::Value::from(i),
        Value::Float(f) => serde_json::json!(f),
        Value::Double(f) => serde_json::json!(f),
        Value::Bytes(b) => serde_json::Value::String(BASE64.encode(b)),
        Value::String(s) => serde_json::Value::String(s),
        Value::Fixed(_, b) => serde_json::Value::String(BASE64.encode(b)),
        Value::Enum(_, s) => serde_json::Value::String(s),
        Value::Uuid(uuid) => serde_json::Value::String(uuid.to_string()),

        // Logical types. Every one of these used to fall through to the
        // `{other:?}` catch-all below and reach the viewer as a chunk of
        // Rust debug output — `Decimal(Decimal { value: .., len: 9 })`,
        // `TimestampMillis(1703142881240)`. They are rendered here as the
        // strings the value actually means.
        Value::Decimal(decimal) => {
            let scale = match schema {
                Some(Schema::Decimal(DecimalSchema { scale, .. })) => *scale,
                // No schema in hand: the unscaled integer is the honest
                // answer — inventing a decimal point would be worse than
                // showing none.
                _ => 0,
            };
            serde_json::Value::String(format_decimal(&BigInt::from(decimal), scale))
        }
        Value::Date(days) => format_date(days),
        Value::TimeMillis(millis) => format_time(i64::from(millis), 1_000, 3),
        Value::TimeMicros(micros) => format_time(micros, 1_000_000, 6),
        Value::TimestampMillis(millis) => format_timestamp(millis, 1_000, 3, true),
        Value::TimestampMicros(micros) => format_timestamp(micros, 1_000_000, 6, true),
        Value::LocalTimestampMillis(millis) => format_timestamp(millis, 1_000, 3, false),
        Value::LocalTimestampMicros(micros) => format_timestamp(micros, 1_000_000, 6, false),
        // Nanosecond precision, added to the Avro spec after the rest of
        // these and surfaced by apache-avro 0.17. Same treatment: the
        // fraction is nine digits wide rather than three or six.
        Value::TimestampNanos(nanos) => format_timestamp(nanos, 1_000_000_000, 9, true),
        Value::LocalTimestampNanos(nanos) => format_timestamp(nanos, 1_000_000_000, 9, false),
        // Unlike `Value::Decimal`, this one carries its own scale, so it
        // needs no help from the schema — and `Display` already renders it
        // as the decimal it is rather than as debug output.
        Value::BigDecimal(decimal) => serde_json::Value::String(decimal.to_string()),
        // Not an instant and not a duration any calendar can flatten — the
        // Avro spec keeps the three components deliberately separate, and
        // so does this.
        Value::Duration(duration) => serde_json::json!({
            "months": u32::from(duration.months()),
            "days": u32::from(duration.days()),
            "milliseconds": u32::from(duration.millis()),
        }),

        Value::Union(index, inner) => {
            let variant = match schema {
                Some(Schema::Union(union)) => union.variants().get(index as usize),
                _ => None,
            };
            to_json_value(*inner, variant, names)
        }
        Value::Array(items) => {
            // apache-avro 0.17 replaced `Schema::Array(Box<Schema>)` with
            // an `ArraySchema` struct carrying the element schema plus its
            // attributes; the element schema is what this needs.
            let items_schema = match schema {
                Some(Schema::Array(inner)) => Some(&*inner.items),
                _ => None,
            };
            serde_json::Value::Array(
                items
                    .into_iter()
                    .map(|item| to_json_value(item, items_schema, names))
                    .collect(),
            )
        }
        Value::Map(map) => {
            // Same shape change as `Schema::Array` above — `MapSchema` names
            // its value schema `types`.
            let values_schema = match schema {
                Some(Schema::Map(inner)) => Some(&*inner.types),
                _ => None,
            };
            // Avro maps are genuinely unordered and arrive here in a
            // `HashMap`, whose iteration order changes run to run. Records
            // keep their schema order (see below); maps get sorted keys, so
            // that re-opening the same message does not reshuffle them.
            let mut entries: Vec<_> = map.into_iter().collect();
            entries.sort_by(|(a, _), (b, _)| a.cmp(b));
            serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(k, v)| (k, to_json_value(v, values_schema, names)))
                    .collect(),
            )
        }
        // `from_avro_datum` yields a record's fields in schema order, and
        // `serde_json`'s `preserve_order` feature (enabled in the workspace
        // manifest) keeps them that way instead of sorting the keys
        // alphabetically — the payload viewer shows the message in the
        // order the schema declares it.
        Value::Record(fields) => {
            let record = match schema {
                Some(Schema::Record(record)) => Some(record),
                _ => None,
            };
            serde_json::Value::Object(
                fields
                    .into_iter()
                    .map(|(name, v)| {
                        let field_schema = record.and_then(|record| {
                            let index = *record.lookup.get(&name)?;
                            record.fields.get(index).map(|field| &field.schema)
                        });
                        let json = to_json_value(v, field_schema, names);
                        (name, json)
                    })
                    .collect(),
            )
        }
    }
}

/// Places the decimal point in an unscaled integer: Avro stores `12.34`
/// with scale 2 as the integer `1234`, and only the schema knows the 2.
fn format_decimal(unscaled: &BigInt, scale: usize) -> String {
    let digits = unscaled.to_string();
    if scale == 0 {
        return digits;
    }
    let (sign, digits) = match digits.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", digits.as_str()),
    };
    if digits.len() > scale {
        let (whole, fraction) = digits.split_at(digits.len() - scale);
        format!("{sign}{whole}.{fraction}")
    } else {
        // Fewer digits than the scale: the value is entirely fractional and
        // needs leading zeros — unscaled 5 at scale 3 is 0.005.
        format!("{sign}0.{:0>width$}", digits, width = scale)
    }
}

/// `date` is a count of days since the Unix epoch; ISO-8601 calls that
/// `YYYY-MM-DD`.
fn format_date(days: i32) -> serde_json::Value {
    match DateTime::from_timestamp(i64::from(days) * 86_400, 0) {
        Some(instant) => serde_json::Value::String(instant.date_naive().to_string()),
        // Out of chrono's range — far outside anything a real message
        // holds, but the raw number is still better than dropping it.
        None => serde_json::Value::from(days),
    }
}

/// `time-millis`/`time-micros` are an offset from midnight with no date and
/// no zone attached, so they render as a bare `HH:MM:SS.fff` clock time.
fn format_time(value: i64, per_second: i64, fraction_digits: usize) -> serde_json::Value {
    let seconds = value.div_euclid(per_second);
    let nanos = value.rem_euclid(per_second) * (1_000_000_000 / per_second);
    let time = u32::try_from(seconds)
        .ok()
        .zip(u32::try_from(nanos).ok())
        .and_then(|(seconds, nanos)| NaiveTime::from_num_seconds_from_midnight_opt(seconds, nanos));
    match time {
        Some(time) => serde_json::Value::String(render_time(time, fraction_digits)),
        None => serde_json::Value::from(value),
    }
}

/// `timestamp-*` is an instant in UTC and renders with a trailing `Z`;
/// `local-timestamp-*` is deliberately zone-less and renders without one,
/// because the zone it belongs to is not in the message.
fn format_timestamp(
    value: i64,
    per_second: i64,
    fraction_digits: usize,
    utc: bool,
) -> serde_json::Value {
    let seconds = value.div_euclid(per_second);
    let nanos = value.rem_euclid(per_second) * (1_000_000_000 / per_second);
    let instant = u32::try_from(nanos)
        .ok()
        .and_then(|nanos| DateTime::from_timestamp(seconds, nanos));
    match instant {
        Some(instant) => {
            let naive = instant.naive_utc();
            let rendered = format!(
                "{}T{}{}",
                naive.date(),
                render_time(naive.time(), fraction_digits),
                if utc { "Z" } else { "" }
            );
            serde_json::Value::String(rendered)
        }
        None => serde_json::Value::from(value),
    }
}

/// `HH:MM:SS` plus exactly `fraction_digits` of sub-second precision —
/// fixed rather than chrono's `%.3f`-style trimming, so every value of one
/// logical type is the same width down a column.
fn render_time(time: NaiveTime, fraction_digits: usize) -> String {
    let nanos = time.nanosecond();
    let scale = 10u32.pow(9 - fraction_digits as u32);
    format!(
        "{:02}:{:02}:{:02}.{:0>width$}",
        time.hour(),
        time.minute(),
        time.second(),
        nanos / scale,
        width = fraction_digits
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const USER_SCHEMA: &str = r#"
    {
      "type": "record",
      "name": "User",
      "fields": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": "string"},
        {"name": "active", "type": "boolean"},
        {"name": "nickname", "type": ["null", "string"], "default": null}
      ]
    }
    "#;

    const EVENT_SCHEMA: &str = r#"
    {
      "type": "record",
      "name": "Event",
      "fields": [
        {"name": "id", "type": "long"},
        {"name": "tags", "type": {"type": "array", "items": "string"}},
        {"name": "counts", "type": {"type": "map", "values": "long"}},
        {"name": "raw", "type": "bytes"},
        {"name": "owner", "type": {
          "type": "record",
          "name": "Owner",
          "fields": [{"name": "email", "type": "string"}]
        }}
      ]
    }
    "#;

    /// Builds Avro bytes for a `Value` by hand (not via `apache_avro`'s
    /// `Record`/`put` builder, which needs schema-internal field lookups
    /// for nested records) — this accepts any `Value` shaped like the
    /// schema, builder or not.
    fn encode(schema_json: &str, value: Value) -> Vec<u8> {
        let schema = Schema::parse_str(schema_json).unwrap();
        apache_avro::writer::datum::GenericDatumWriter::builder(&schema)
            .build()
            .unwrap()
            .write_value_to_vec(value)
            .unwrap()
    }

    #[test]
    fn detects_a_valid_wire_format_header() {
        let bytes = [0x00, 0x00, 0x00, 0x00, 0x2a, 0x01, 0x02];
        assert_eq!(detect_wire_format(&bytes), Some(42));
    }

    #[test]
    fn rejects_a_payload_shorter_than_the_header() {
        assert_eq!(detect_wire_format(&[0x00, 0x00, 0x00, 0x00]), None);
    }

    #[test]
    fn rejects_a_payload_with_the_wrong_magic_byte() {
        let bytes = [0x01, 0x00, 0x00, 0x00, 0x2a, 0x01, 0x02];
        assert_eq!(detect_wire_format(&bytes), None);
    }

    #[test]
    fn validate_schema_accepts_valid_avro_schema_json() {
        assert!(validate_schema(USER_SCHEMA).is_ok());
    }

    /// A container file's schema is inside the payload, and its framing is
    /// incompatible with the other two — decoding it with a manual or
    /// registry schema would read the file header as data.
    #[test]
    fn a_container_file_wins_over_everything_else() {
        let container = [0x4f, 0x62, 0x6a, 0x01, 0xff];
        assert_eq!(
            decide_decode_strategy(&container, true, true),
            Ok(AvroDecodeStrategy::ContainerFile)
        );
        assert_eq!(
            decide_decode_strategy(&container, false, false),
            Ok(AvroDecodeStrategy::ContainerFile)
        );
    }

    /// Pasting a schema into the Schema tab is an explicit override, and the
    /// usual reason to do it is that registry lookup is not doing what you
    /// want — so it must beat a payload that also carries a registry header.
    #[test]
    fn a_manual_schema_beats_the_registry() {
        let confluent = [0x00, 0x00, 0x00, 0x00, 0x07, 0xff];
        assert_eq!(
            decide_decode_strategy(&confluent, true, true),
            Ok(AvroDecodeStrategy::ManualSchema)
        );
    }

    #[test]
    fn a_manual_schema_decodes_a_payload_with_no_framing_at_all() {
        assert_eq!(
            decide_decode_strategy(&[1, 2, 3], true, false),
            Ok(AvroDecodeStrategy::ManualSchema)
        );
    }

    #[test]
    fn the_confluent_header_is_used_when_nothing_overrides_it() {
        let confluent = [0x00, 0x00, 0x00, 0x00, 0x07, 0xff];
        assert_eq!(
            decide_decode_strategy(&confluent, false, true),
            Ok(AvroDecodeStrategy::SchemaRegistry { schema_id: 7 })
        );
    }

    /// The two refusals are distinct on purpose: one tells the user to
    /// configure a registry, the other that nothing identifies these bytes.
    #[test]
    fn a_registry_payload_without_a_configured_registry_says_so() {
        let confluent = [0x00, 0x00, 0x00, 0x00, 0x07, 0xff];
        assert_eq!(
            decide_decode_strategy(&confluent, false, false),
            Err(AvroDecodeRefusal::NoRegistryConfigured)
        );
    }

    #[test]
    fn an_unidentifiable_payload_says_that_instead() {
        assert_eq!(
            decide_decode_strategy(&[1, 2, 3], false, true),
            Err(AvroDecodeRefusal::NoSchemaAvailable)
        );
        assert_eq!(
            decide_decode_strategy(&[], false, true),
            Err(AvroDecodeRefusal::NoSchemaAvailable)
        );
    }

    #[test]
    fn each_refusal_has_its_own_message() {
        assert_ne!(
            AvroDecodeRefusal::NoSchemaAvailable.message(),
            AvroDecodeRefusal::NoRegistryConfigured.message()
        );
    }

    #[test]
    fn validate_schema_rejects_invalid_json() {
        assert!(validate_schema("{not valid avro schema").is_err());
    }

    #[test]
    fn decodes_primitive_and_string_fields() {
        let value = Value::Record(vec![
            ("id".to_string(), Value::Long(42)),
            ("name".to_string(), Value::String("Ada".into())),
            ("active".to_string(), Value::Boolean(true)),
            (
                "nickname".to_string(),
                Value::Union(0, Box::new(Value::Null)),
            ),
        ]);
        let bytes = encode(USER_SCHEMA, value);

        let decoded = decode(&bytes, USER_SCHEMA).unwrap();

        assert_eq!(decoded["id"], serde_json::json!(42));
        assert_eq!(decoded["name"], serde_json::json!("Ada"));
        assert_eq!(decoded["active"], serde_json::json!(true));
        assert_eq!(decoded["nickname"], serde_json::Value::Null);
    }

    #[test]
    fn unwraps_a_populated_union_to_its_inner_value() {
        let value = Value::Record(vec![
            ("id".to_string(), Value::Long(1)),
            ("name".to_string(), Value::String("Grace".into())),
            ("active".to_string(), Value::Boolean(false)),
            (
                "nickname".to_string(),
                Value::Union(1, Box::new(Value::String("G".into()))),
            ),
        ]);
        let bytes = encode(USER_SCHEMA, value);

        let decoded = decode(&bytes, USER_SCHEMA).unwrap();

        assert_eq!(decoded["nickname"], serde_json::json!("G"));
    }

    #[test]
    fn decodes_arrays_maps_bytes_and_nested_records() {
        let value = Value::Record(vec![
            ("id".to_string(), Value::Long(7)),
            (
                "tags".to_string(),
                Value::Array(vec![Value::String("a".into()), Value::String("b".into())]),
            ),
            (
                "counts".to_string(),
                Value::Map(HashMap::from([("x".to_string(), Value::Long(3))])),
            ),
            ("raw".to_string(), Value::Bytes(vec![1, 2, 3])),
            (
                "owner".to_string(),
                Value::Record(vec![(
                    "email".to_string(),
                    Value::String("ada@example.com".into()),
                )]),
            ),
        ]);
        let bytes = encode(EVENT_SCHEMA, value);

        let decoded = decode(&bytes, EVENT_SCHEMA).unwrap();

        assert_eq!(decoded["id"], serde_json::json!(7));
        assert_eq!(decoded["tags"], serde_json::json!(["a", "b"]));
        assert_eq!(decoded["counts"]["x"], serde_json::json!(3));
        assert_eq!(decoded["raw"], serde_json::json!("AQID"));
        assert_eq!(
            decoded["owner"]["email"],
            serde_json::json!("ada@example.com")
        );
    }

    #[test]
    fn returns_an_error_for_a_payload_that_does_not_match_the_schema() {
        let bytes = vec![0xff, 0xff, 0xff];
        assert!(decode(&bytes, USER_SCHEMA).is_err());
    }

    #[test]
    fn returns_an_error_for_an_invalid_schema() {
        let bytes = vec![0x00];
        assert!(decode(&bytes, "{not valid avro schema").is_err());
    }

    fn encode_container(schema_json: &str, values: Vec<Value>) -> Vec<u8> {
        let schema = Schema::parse_str(schema_json).unwrap();
        // apache-avro 0.22 made `Writer::new` fallible.
        let mut writer = apache_avro::Writer::new(&schema, Vec::new()).unwrap();
        for value in values {
            writer.append_value(value).unwrap();
        }
        writer.into_inner().unwrap()
    }

    #[test]
    fn detects_a_valid_container_file_magic_header() {
        let bytes = encode_container(
            USER_SCHEMA,
            vec![Value::Record(vec![
                ("id".to_string(), Value::Long(1)),
                ("name".to_string(), Value::String("Ada".into())),
                ("active".to_string(), Value::Boolean(true)),
                (
                    "nickname".to_string(),
                    Value::Union(0, Box::new(Value::Null)),
                ),
            ])],
        );
        assert!(detect_container_file(&bytes));
    }

    #[test]
    fn rejects_bytes_without_the_container_file_magic() {
        let bytes = [0x00, 0x00, 0x00, 0x00, 0x2a, 0x01, 0x02];
        assert!(!detect_container_file(&bytes));
    }

    #[test]
    fn rejects_a_payload_shorter_than_the_container_file_magic() {
        assert!(!detect_container_file(&[0x4f, 0x62]));
    }

    #[test]
    fn decode_container_decodes_a_single_record_directly() {
        let bytes = encode_container(
            USER_SCHEMA,
            vec![Value::Record(vec![
                ("id".to_string(), Value::Long(42)),
                ("name".to_string(), Value::String("Ada".into())),
                ("active".to_string(), Value::Boolean(true)),
                (
                    "nickname".to_string(),
                    Value::Union(0, Box::new(Value::Null)),
                ),
            ])],
        );

        let decoded = decode_container(&bytes).unwrap();

        assert_eq!(decoded["id"], serde_json::json!(42));
        assert_eq!(decoded["name"], serde_json::json!("Ada"));
    }

    #[test]
    fn decode_container_decodes_multiple_records_as_an_array() {
        let bytes = encode_container(
            USER_SCHEMA,
            vec![
                Value::Record(vec![
                    ("id".to_string(), Value::Long(1)),
                    ("name".to_string(), Value::String("Ada".into())),
                    ("active".to_string(), Value::Boolean(true)),
                    (
                        "nickname".to_string(),
                        Value::Union(0, Box::new(Value::Null)),
                    ),
                ]),
                Value::Record(vec![
                    ("id".to_string(), Value::Long(2)),
                    ("name".to_string(), Value::String("Grace".into())),
                    ("active".to_string(), Value::Boolean(false)),
                    (
                        "nickname".to_string(),
                        Value::Union(0, Box::new(Value::Null)),
                    ),
                ]),
            ],
        );

        let decoded = decode_container(&bytes).unwrap();

        let records = decoded.as_array().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["name"], serde_json::json!("Ada"));
        assert_eq!(records[1]["name"], serde_json::json!("Grace"));
    }

    /// Every logical type in one record, in an order that is deliberately
    /// not alphabetical — see `fields_keep_their_schema_order`.
    const LOGICAL_SCHEMA: &str = r#"
    {
      "type": "record",
      "name": "Reading",
      "fields": [
        {"name": "zulu", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 20, "scale": 2}},
        {"name": "identifier", "type": {"type": "bytes", "logicalType": "decimal", "precision": 20, "scale": 0}},
        {"name": "day", "type": {"type": "int", "logicalType": "date"}},
        {"name": "moment", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "clock", "type": {"type": "int", "logicalType": "time-millis"}},
        {"name": "wall", "type": {"type": "long", "logicalType": "local-timestamp-millis"}}
      ]
    }
    "#;

    fn decimal(unscaled: &str) -> Value {
        Value::Decimal(apache_avro::Decimal::from(
            unscaled.parse::<BigInt>().unwrap().to_signed_bytes_be(),
        ))
    }

    fn logical_record() -> Value {
        Value::Record(vec![
            (
                "zulu".to_string(),
                Value::TimestampMillis(1_703_142_881_240),
            ),
            ("amount".to_string(), decimal("123456")),
            ("identifier".to_string(), decimal("20231221071441240935")),
            ("day".to_string(), Value::Date(19_712)),
            (
                "moment".to_string(),
                Value::TimestampMicros(1_703_142_881_240_935),
            ),
            ("clock".to_string(), Value::TimeMillis(26_081_240)),
            (
                "wall".to_string(),
                Value::LocalTimestampMillis(1_703_142_881_240),
            ),
        ])
    }

    /// The payload viewer shows the record in the order the schema declares
    /// it. Before `serde_json`'s `preserve_order`, `serde_json::Map` was a
    /// `BTreeMap` and every message came out alphabetised — here that would
    /// be `amount, clock, day, identifier, moment, wall, zulu`.
    #[test]
    fn fields_keep_their_schema_order() {
        let bytes = encode(LOGICAL_SCHEMA, logical_record());

        let decoded = decode(&bytes, LOGICAL_SCHEMA).unwrap();

        let keys: Vec<&str> = decoded
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            [
                "zulu",
                "amount",
                "identifier",
                "day",
                "moment",
                "clock",
                "wall"
            ]
        );
    }

    /// Nested records keep their own order too, rather than only the
    /// top-level one being fixed up.
    #[test]
    fn nested_records_keep_their_schema_order() {
        const SCHEMA: &str = r#"
        {
          "type": "record",
          "name": "Outer",
          "fields": [{"name": "inner", "type": {
            "type": "record",
            "name": "Inner",
            "fields": [
              {"name": "zebra", "type": "string"},
              {"name": "apple", "type": "string"}
            ]
          }}]
        }
        "#;
        let bytes = encode(
            SCHEMA,
            Value::Record(vec![(
                "inner".to_string(),
                Value::Record(vec![
                    ("zebra".to_string(), Value::String("z".into())),
                    ("apple".to_string(), Value::String("a".into())),
                ]),
            )]),
        );

        let decoded = decode(&bytes, SCHEMA).unwrap();

        let keys: Vec<&str> = decoded["inner"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, ["zebra", "apple"]);
    }

    /// A decimal reached the viewer as `Decimal(Decimal { value: .., len: 9 })`
    /// — Rust debug output — because it fell through to a `{:?}` catch-all.
    /// It is a number, and it is shown as one: a string, because a
    /// 20-digit unscaled value does not survive JSON's f64 numbers.
    #[test]
    fn decimals_are_strings_with_the_schema_s_scale_applied() {
        let bytes = encode(LOGICAL_SCHEMA, logical_record());

        let decoded = decode(&bytes, LOGICAL_SCHEMA).unwrap();

        assert_eq!(decoded["amount"], serde_json::json!("1234.56"));
        assert_eq!(
            decoded["identifier"],
            serde_json::json!("20231221071441240935")
        );
    }

    #[test]
    fn a_decimal_smaller_than_its_scale_gets_leading_zeros() {
        assert_eq!(format_decimal(&BigInt::from(5), 3), "0.005");
        assert_eq!(format_decimal(&BigInt::from(-5), 3), "-0.005");
        assert_eq!(format_decimal(&BigInt::from(-1234), 2), "-12.34");
        assert_eq!(format_decimal(&BigInt::from(0), 2), "0.00");
        assert_eq!(format_decimal(&BigInt::from(1234), 0), "1234");
    }

    /// `date`, `timestamp-*` and `time-*` all used to render as debug
    /// output too — `TimestampMillis(1703142881240)`. ISO-8601 instead:
    /// UTC instants carry a `Z`, and `local-timestamp-*` deliberately does
    /// not, because the zone it belongs to is not in the message.
    #[test]
    fn dates_and_timestamps_are_iso_8601_strings() {
        let bytes = encode(LOGICAL_SCHEMA, logical_record());

        let decoded = decode(&bytes, LOGICAL_SCHEMA).unwrap();

        assert_eq!(decoded["day"], serde_json::json!("2023-12-21"));
        assert_eq!(
            decoded["zulu"],
            serde_json::json!("2023-12-21T07:14:41.240Z")
        );
        assert_eq!(
            decoded["moment"],
            serde_json::json!("2023-12-21T07:14:41.240935Z")
        );
        assert_eq!(
            decoded["wall"],
            serde_json::json!("2023-12-21T07:14:41.240")
        );
        assert_eq!(decoded["clock"], serde_json::json!("07:14:41.240"));
    }

    #[test]
    fn timestamps_before_the_epoch_stay_correct() {
        assert_eq!(
            format_timestamp(-1, 1_000, 3, true),
            serde_json::json!("1969-12-31T23:59:59.999Z")
        );
        assert_eq!(format_date(-1), serde_json::json!("1969-12-31"));
    }

    #[test]
    fn a_uuid_is_its_canonical_string() {
        const SCHEMA: &str = r#"{"type": "string", "logicalType": "uuid"}"#;
        let uuid = uuid::Uuid::parse_str("936da01f-9abd-4d9d-80c7-02af85c822a8").unwrap();
        let bytes = encode(SCHEMA, Value::Uuid(uuid));

        assert_eq!(
            decode(&bytes, SCHEMA).unwrap(),
            serde_json::json!("936da01f-9abd-4d9d-80c7-02af85c822a8")
        );
    }

    /// A duration is months + days + millis that no calendar can flatten
    /// into one number, so all three are kept.
    #[test]
    fn a_duration_keeps_its_three_components() {
        const SCHEMA: &str =
            r#"{"type": "fixed", "name": "d", "size": 12, "logicalType": "duration"}"#;
        let duration = apache_avro::Duration::new(
            apache_avro::Months::new(1),
            apache_avro::Days::new(2),
            apache_avro::Millis::new(3),
        );
        let bytes = encode(SCHEMA, Value::Duration(duration));

        let decoded = decode(&bytes, SCHEMA).unwrap();

        assert_eq!(decoded["months"], serde_json::json!(1));
        assert_eq!(decoded["days"], serde_json::json!(2));
        assert_eq!(decoded["milliseconds"], serde_json::json!(3));
    }

    /// A schema that names a record once and refers to it by name later
    /// (`Schema::Ref`) must resolve, or the second occurrence loses the
    /// scale and its decimal renders as a raw unscaled integer.
    #[test]
    fn a_named_type_reference_still_finds_the_scale() {
        const SCHEMA: &str = r#"
        {
          "type": "record",
          "name": "Pair",
          "fields": [
            {"name": "first", "type": {
              "type": "record",
              "name": "Money",
              "fields": [{"name": "amount", "type": {
                "type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2
              }}]
            }},
            {"name": "second", "type": "Money"}
          ]
        }
        "#;
        let money = |unscaled| Value::Record(vec![("amount".to_string(), decimal(unscaled))]);
        let bytes = encode(
            SCHEMA,
            Value::Record(vec![
                ("first".to_string(), money("1234")),
                ("second".to_string(), money("5678")),
            ]),
        );

        let decoded = decode(&bytes, SCHEMA).unwrap();

        assert_eq!(decoded["first"]["amount"], serde_json::json!("12.34"));
        assert_eq!(decoded["second"]["amount"], serde_json::json!("56.78"));
    }

    /// The schema has to reach through unions, arrays and maps as well —
    /// an optional decimal is the shape a nullable database column takes.
    #[test]
    fn unions_arrays_and_maps_carry_the_schema_through() {
        const SCHEMA: &str = r#"
        {
          "type": "record",
          "name": "Holder",
          "fields": [
            {"name": "maybe", "type": ["null", {
              "type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2
            }], "default": null},
            {"name": "many", "type": {"type": "array", "items": {
              "type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 3
            }}},
            {"name": "keyed", "type": {"type": "map", "values": {
              "type": "long", "logicalType": "timestamp-millis"
            }}}
          ]
        }
        "#;
        let bytes = encode(
            SCHEMA,
            Value::Record(vec![
                (
                    "maybe".to_string(),
                    Value::Union(1, Box::new(decimal("1234"))),
                ),
                ("many".to_string(), Value::Array(vec![decimal("1234")])),
                (
                    "keyed".to_string(),
                    Value::Map(HashMap::from([(
                        "at".to_string(),
                        Value::TimestampMillis(1_703_142_881_240),
                    )])),
                ),
            ]),
        );

        let decoded = decode(&bytes, SCHEMA).unwrap();

        assert_eq!(decoded["maybe"], serde_json::json!("12.34"));
        assert_eq!(decoded["many"], serde_json::json!(["1.234"]));
        assert_eq!(
            decoded["keyed"]["at"],
            serde_json::json!("2023-12-21T07:14:41.240Z")
        );
    }

    /// Records keep schema order; maps cannot, because Avro maps are
    /// unordered and arrive in a `HashMap` whose iteration order changes
    /// between runs. They are sorted so that re-opening the same message
    /// does not reshuffle them.
    #[test]
    fn map_keys_are_sorted_for_a_stable_display() {
        const SCHEMA: &str = r#"{"type": "map", "values": "long"}"#;
        let bytes = encode(
            SCHEMA,
            Value::Map(HashMap::from([
                ("zebra".to_string(), Value::Long(1)),
                ("apple".to_string(), Value::Long(2)),
                ("mango".to_string(), Value::Long(3)),
            ])),
        );

        let decoded = decode(&bytes, SCHEMA).unwrap();

        let keys: Vec<&str> = decoded
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, ["apple", "mango", "zebra"]);
    }

    /// A container file carries its own writer schema, so it gets the same
    /// treatment — the schema is read off the reader before the iterator
    /// consumes it.
    #[test]
    fn a_container_file_gets_the_order_and_the_logical_types_too() {
        let bytes = encode_container(LOGICAL_SCHEMA, vec![logical_record()]);

        let decoded = decode_container(&bytes).unwrap();

        let keys: Vec<&str> = decoded
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            [
                "zulu",
                "amount",
                "identifier",
                "day",
                "moment",
                "clock",
                "wall"
            ]
        );
        assert_eq!(decoded["amount"], serde_json::json!("1234.56"));
        assert_eq!(decoded["day"], serde_json::json!("2023-12-21"));
    }

    #[test]
    fn decode_container_returns_an_error_for_bytes_that_are_not_a_container_file() {
        let bytes = vec![0xff, 0xff, 0xff];
        assert!(decode_container(&bytes).is_err());
    }
}
