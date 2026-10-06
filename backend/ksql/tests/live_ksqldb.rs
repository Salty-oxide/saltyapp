//! The client, against a real ksqlDB server.
//!
//! Every other test in this crate parses fixture strings. These are the ones
//! that can show the frames are actually shaped the way the parser assumes,
//! that a push query streams rather than buffering, and that Stop ends one —
//! none of which a fixture can prove.
//!
//! ```bash
//! ./scripts/e2e-ksql-fixtures.sh
//! SALTY_E2E_KSQL_URL=http://localhost:8088 \
//!   cargo test -p salty-ksql --test live_ksqldb
//! ```
//!
//! Without the variable every test here skips itself, so the suite stays
//! runnable on a machine with no ksqlDB server — the same gating the broker
//! tests use.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use salty_ksql::{parse_streams, stream_for_topic, KsqlClient, KsqlEndpoint};

/// Created by `scripts/e2e-ksql-fixtures.sh` over the `e2e-basic` topic.
const STREAM: &str = "E2E_BASIC_STREAM";
const TOPIC: &str = "e2e-basic";

fn ksql_url() -> Option<String> {
    std::env::var("SALTY_E2E_KSQL_URL").ok().filter(|value| !value.is_empty())
}

macro_rules! ksql {
    () => {
        match ksql_url() {
            Some(url) => KsqlClient::new(KsqlEndpoint { url, basic_auth: None })
                .expect("should build a client"),
            None => {
                eprintln!(
                    "skipped: run ./scripts/e2e-ksql-fixtures.sh and set \
                     SALTY_E2E_KSQL_URL to run this test"
                );
                return;
            }
        }
    };
}

// The frames really are what the parser expects. If ksqlDB ever changes its
// wire shape, this is what says so.
#[tokio::test]
async fn lists_the_streams_the_server_knows_about() {
    let client = ksql!();

    let body = client.statement("LIST STREAMS;").await.expect("should list streams");
    let streams = parse_streams(&body);

    assert!(
        !streams.is_empty(),
        "the fixture registers a stream; got none from {body}"
    );
    assert_eq!(
        stream_for_topic(&streams, TOPIC).as_deref(),
        Some(STREAM),
        "expected a stream over {TOPIC}; got {streams:?}"
    );
}

#[tokio::test]
async fn finds_the_stream_registered_over_a_topic() {
    let client = ksql!();

    let found = client.stream_for_topic(TOPIC).await.expect("should query");

    assert_eq!(found.as_deref(), Some(STREAM));
}

#[tokio::test]
async fn reports_no_stream_for_a_topic_that_has_none() {
    let client = ksql!();

    let found = client
        .stream_for_topic("a-topic-no-stream-covers")
        .await
        .expect("should query");

    assert_eq!(found, None);
}

// The header frame is what gives the grid typed columns, and it has to arrive
// *before* the rows — a push query never ends, so a caller that waited for the
// query to finish would never draw anything.
#[tokio::test]
async fn delivers_the_column_header_before_any_row() {
    let client = ksql!();
    let cancelled = Arc::new(AtomicBool::new(false));
    // Shared rather than two `mut` captures: both callbacks record into the
    // same log, and the order between them is the whole assertion. A mutex
    // rather than a `RefCell` because the callbacks are `Send`.
    let order: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());
    let columns = Mutex::new(Vec::new());

    let outcome = client
        .query_stream(
            &format!("SELECT * FROM {STREAM} EMIT CHANGES LIMIT 5;"),
            cancelled,
            |header| {
                order.lock().unwrap().push("header");
                *columns.lock().unwrap() = header.columns.clone();
            },
            |_row| order.lock().unwrap().push("row"),
        )
        .await
        .expect("query should run");

    let order = order.into_inner().unwrap();
    let columns = columns.into_inner().unwrap();
    assert_eq!(order.first(), Some(&"header"), "header must precede rows");
    assert!(!columns.is_empty(), "the header should name the columns");
    assert!(
        outcome.header.is_some(),
        "the outcome should carry the header too"
    );
    // Every column should name a ksqlDB type the grid can act on.
    for column in &columns {
        assert!(!column.name.is_empty());
        assert!(!column.kind.is_empty());
    }
}

// A bounded push query ends on its own, and the rows are the fixture's.
//
// **`LIMIT` on a push query is best-effort, not exact.** Measured against a
// real server on this 3-partition topic: 24 of 25 runs returned exactly 5
// rows and one returned 6 — six *distinct* rows, so the client was faithful
// and the server had simply overshot. Each partition's task counts toward the
// limit independently, so a query can exceed it by up to one row per extra
// partition before every task notices and stops.
//
// Asserting `== LIMIT` here failed roughly one run in twenty-five, which is
// exactly the kind of test that gets rerun until it passes and then trusted.
// The bound below is the real contract.
#[tokio::test]
async fn streams_the_rows_of_a_bounded_push_query() {
    let client = ksql!();
    let cancelled = Arc::new(AtomicBool::new(false));
    let mut rows = Vec::new();

    let outcome = client
        .query_stream(
            &format!("SELECT * FROM {STREAM} EMIT CHANGES LIMIT 5;"),
            cancelled,
            |_| {},
            |row| rows.push(row),
        )
        .await
        .expect("query should run");

    assert!(
        (5..=8).contains(&rows.len()),
        "LIMIT 5 over 3 partitions should yield 5 rows, or a little over — got {}",
        rows.len()
    );
    assert!(!outcome.cancelled, "a LIMIT query ends by itself, not by Stop");
    // Each row should have one value per column.
    let width = outcome.header.expect("header").columns.len();
    for row in &rows {
        assert_eq!(row.len(), width, "row width should match the header");
    }
}

// What the Stop button does. The query is unbounded, so the only thing that
// can end it is the cancellation flag.
#[tokio::test]
async fn stop_ends_an_unbounded_push_query() {
    let client = ksql!();
    let cancelled = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&cancelled);

    // Cancel shortly after it starts, the way a user reaching for Stop would.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        flag.store(true, Ordering::Relaxed);
    });

    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        client.query_stream(
            &format!("SELECT * FROM {STREAM} EMIT CHANGES;"),
            cancelled,
            |_| {},
            |_| {},
        ),
    )
    .await
    .expect("Stop must end the query rather than hanging")
    .expect("query should run");

    assert!(
        outcome.cancelled,
        "the outcome should report that Stop ended it, not the server"
    );
}

// ksqlDB explains its failures in the body. Surfacing that message rather than
// a bare status is the difference between an actionable error and a shrug.
#[tokio::test]
async fn surfaces_the_servers_own_message_for_a_bad_statement() {
    let client = ksql!();

    let error = client
        .statement("SELECT nonsense FROM does_not_exist;")
        .await
        .expect_err("should be refused");

    let rendered = format!("{error:?}");
    assert!(
        rendered.to_uppercase().contains("DOES_NOT_EXIST")
            || rendered.to_lowercase().contains("does not exist"),
        "the error should name what ksqlDB objected to; got {rendered}"
    );
}

// A registration statement goes to `/ksql`, not `/query-stream`, and is
// idempotent enough to re-run: the fixture script already created this stream.
#[tokio::test]
async fn describes_an_existing_stream() {
    let client = ksql!();

    let body = client
        .statement(&format!("DESCRIBE {STREAM};"))
        .await
        .expect("should describe");

    assert!(body.contains(STREAM), "the description should name the stream");
}
