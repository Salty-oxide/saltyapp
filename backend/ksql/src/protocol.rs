//! Reading what ksqlDB sends back.
//!
//! Kept free of any HTTP so the wire formats can be tested against fixture
//! strings rather than a running server — which matters, because the server is
//! a JVM process this repo does not start for unit tests.

use serde::{Deserialize, Serialize};

/// A column in a query result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KsqlColumn {
    pub name: String,
    /// ksqlDB's own type name — `STRING`, `BIGINT`, `DOUBLE`, `BOOLEAN`, …
    ///
    /// Carried through to the UI rather than mapped to something of our own,
    /// so a numeric column can get a numeric filter in the grid instead of a
    /// string one, and so an unfamiliar type still renders as itself.
    pub kind: String,
}

/// The first frame of a `/query-stream` response: the query's id and its shape.
///
/// The id is what `/close-query` needs, which is why Stop is reliable rather
/// than "probably terminated when we dropped the socket".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KsqlQueryHeader {
    pub query_id: Option<String>,
    pub columns: Vec<KsqlColumn>,
}

/// One row, as the JSON values ksqlDB sent, in column order.
pub type KsqlRow = Vec<serde_json::Value>;

/// A failure ksqlDB described in its response body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KsqlServerError {
    pub error_code: Option<i64>,
    pub message: String,
}

/// Parses the header frame of a `/query-stream` response.
///
/// Returns `None` for a line that is not a header, so the caller can tell
/// "this stream opened with something unexpected" from "this is a row".
pub fn parse_header(line: &str) -> Option<KsqlQueryHeader> {
    let value: serde_json::Value = serde_json::from_str(line).ok()?;
    let object = value.as_object()?;
    let names = object.get("columnNames")?.as_array()?;
    // `columnTypes` is absent on some ksqlDB versions for statements with no
    // typed schema. Falling back to an empty list keeps the column *names* —
    // which are what the grid needs most — rather than failing the query.
    let empty = Vec::new();
    let types = object
        .get("columnTypes")
        .and_then(|value| value.as_array())
        .unwrap_or(&empty);

    let columns = names
        .iter()
        .enumerate()
        .map(|(index, name)| KsqlColumn {
            name: name.as_str().unwrap_or_default().to_string(),
            kind: types
                .get(index)
                .and_then(|kind| kind.as_str())
                .unwrap_or("STRING")
                .to_string(),
        })
        .collect();

    Some(KsqlQueryHeader {
        query_id: object
            .get("queryId")
            .and_then(|id| id.as_str())
            .map(str::to_string),
        columns,
    })
}

/// Parses one row frame.
///
/// Rows arrive as bare JSON arrays. A trailing comma is tolerated because
/// ksqlDB frames the stream as a JSON array on some versions, so lines arrive
/// as `[…],` rather than `[…]`.
pub fn parse_row(line: &str) -> Option<KsqlRow> {
    let trimmed = line.trim().trim_end_matches(',');
    if trimmed.is_empty() || trimmed == "[" || trimmed == "]" {
        return None;
    }
    serde_json::from_str::<serde_json::Value>(trimmed)
        .ok()?
        .as_array()
        .cloned()
}

/// Parses an error body, from either endpoint.
///
/// ksqlDB reports failures in the body with a real message — an HTTP status
/// alone tells the user nothing they can act on. Both the `/ksql` shape
/// (`message`) and the `/query-stream` shape (`@type` + `message`) carry it in
/// the same field.
pub fn parse_server_error(body: &str) -> Option<KsqlServerError> {
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    let object = value.as_object()?;
    let message = object
        .get("message")
        .and_then(|message| message.as_str())
        // `/ksql` uses `errorMessage` for statement errors in some versions.
        .or_else(|| {
            object
                .get("errorMessage")
                .and_then(|error| error.get("message"))
                .and_then(|message| message.as_str())
        })?;

    Some(KsqlServerError {
        error_code: object.get("error_code").and_then(|code| code.as_i64()),
        message: message.to_string(),
    })
}

/// The stream names ksqlDB reported, paired with the topic each is built on.
///
/// Backs the topic tab's "is there a stream over this topic?" question. Parsed
/// from a `LIST STREAMS;` response, whose payload is an array with one entry
/// of `@type: "streams"`.
pub fn parse_streams(body: &str) -> Vec<(String, String)> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return Vec::new();
    };
    let Some(entries) = value.as_array() else {
        return Vec::new();
    };

    entries
        .iter()
        .filter_map(|entry| entry.get("streams")?.as_array())
        .flatten()
        .filter_map(|stream| {
            Some((
                stream.get("name")?.as_str()?.to_string(),
                stream.get("topic")?.as_str()?.to_string(),
            ))
        })
        .collect()
}

/// The stream registered over `topic`, if there is one.
///
/// Case-insensitive on the topic name because ksqlDB upper-cases identifiers
/// but not topic names, and the two are compared constantly.
pub fn stream_for_topic(streams: &[(String, String)], topic: &str) -> Option<String> {
    streams
        .iter()
        .find(|(_, stream_topic)| stream_topic.eq_ignore_ascii_case(topic))
        .map(|(name, _)| name.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: &str = r#"{"queryId":"q1","columnNames":["ID","AMOUNT"],"columnTypes":["STRING","BIGINT"]}"#;

    #[test]
    fn reads_column_names_and_types_from_the_header() {
        let header = parse_header(HEADER).expect("should parse");

        assert_eq!(header.query_id.as_deref(), Some("q1"));
        assert_eq!(
            header.columns,
            vec![
                KsqlColumn { name: "ID".into(), kind: "STRING".into() },
                KsqlColumn { name: "AMOUNT".into(), kind: "BIGINT".into() },
            ]
        );
    }

    // The id is what `/close-query` needs; a pull query has none, and that is
    // not a parse failure.
    #[test]
    fn accepts_a_header_with_no_query_id() {
        let header = parse_header(r#"{"columnNames":["A"],"columnTypes":["STRING"]}"#)
            .expect("should parse");

        assert_eq!(header.query_id, None);
        assert_eq!(header.columns.len(), 1);
    }

    // Keeping the names is worth more than failing over missing types: the
    // grid needs names to draw columns at all.
    #[test]
    fn falls_back_to_string_when_types_are_absent() {
        let header = parse_header(r#"{"columnNames":["A","B"]}"#).expect("should parse");

        assert_eq!(header.columns[0].kind, "STRING");
        assert_eq!(header.columns[1].kind, "STRING");
    }

    #[test]
    fn is_not_a_header_when_there_are_no_column_names() {
        assert!(parse_header(r#"{"queryId":"q1"}"#).is_none());
        assert!(parse_header("[\"a\",\"b\"]").is_none());
        assert!(parse_header("not json").is_none());
    }

    #[test]
    fn reads_a_row_as_its_values_in_column_order() {
        let row = parse_row(r#"["order-1",42]"#).expect("should parse");

        assert_eq!(row[0], serde_json::json!("order-1"));
        assert_eq!(row[1], serde_json::json!(42));
    }

    // Some ksqlDB versions frame the stream as a JSON array, so rows arrive
    // with the array's punctuation attached.
    #[test]
    fn tolerates_the_json_array_framing_some_versions_use() {
        assert_eq!(
            parse_row(r#"["a",1],"#).expect("should parse"),
            vec![serde_json::json!("a"), serde_json::json!(1)]
        );
        assert!(parse_row("[").is_none());
        assert!(parse_row("]").is_none());
        assert!(parse_row("   ").is_none());
    }

    #[test]
    fn keeps_nulls_in_a_row_rather_than_dropping_the_column() {
        let row = parse_row(r#"["a",null,3]"#).expect("should parse");

        assert_eq!(row.len(), 3);
        assert_eq!(row[1], serde_json::Value::Null);
    }

    #[test]
    fn reads_the_message_out_of_a_server_error() {
        let error = parse_server_error(
            r#"{"@type":"generic_error","error_code":40001,"message":"Statement is invalid"}"#,
        )
        .expect("should parse");

        assert_eq!(error.error_code, Some(40001));
        assert_eq!(error.message, "Statement is invalid");
    }

    #[test]
    fn reads_the_nested_statement_error_shape_too() {
        let error = parse_server_error(r#"{"errorMessage":{"message":"Line 1: no viable alt"}}"#)
            .expect("should parse");

        assert_eq!(error.message, "Line 1: no viable alt");
    }

    #[test]
    fn is_not_an_error_when_the_body_carries_no_message() {
        assert!(parse_server_error(r#"{"ok":true}"#).is_none());
        assert!(parse_server_error("not json").is_none());
    }

    const STREAMS: &str = r#"[{"@type":"streams","streams":[
        {"name":"ORDERS_STREAM","topic":"orders","format":"AVRO"},
        {"name":"SHIPMENTS","topic":"shipments","format":"JSON"}]}]"#;

    #[test]
    fn lists_each_stream_with_the_topic_it_is_built_on() {
        let streams = parse_streams(STREAMS);

        assert_eq!(
            streams,
            vec![
                ("ORDERS_STREAM".to_string(), "orders".to_string()),
                ("SHIPMENTS".to_string(), "shipments".to_string()),
            ]
        );
    }

    #[test]
    fn finds_the_stream_registered_over_a_topic() {
        let streams = parse_streams(STREAMS);

        assert_eq!(
            stream_for_topic(&streams, "orders").as_deref(),
            Some("ORDERS_STREAM")
        );
    }

    // ksqlDB upper-cases identifiers but not topic names, and the two get
    // compared constantly.
    #[test]
    fn matches_a_topic_name_regardless_of_case() {
        let streams = parse_streams(STREAMS);

        assert_eq!(
            stream_for_topic(&streams, "ORDERS").as_deref(),
            Some("ORDERS_STREAM")
        );
    }

    #[test]
    fn finds_nothing_for_a_topic_with_no_stream() {
        let streams = parse_streams(STREAMS);

        assert_eq!(stream_for_topic(&streams, "payments"), None);
    }

    #[test]
    fn survives_a_response_that_is_not_a_stream_listing() {
        assert!(parse_streams("{}").is_empty());
        assert!(parse_streams("not json").is_empty());
        assert!(parse_streams(r#"[{"@type":"topics","topics":[]}]"#).is_empty());
    }
}
