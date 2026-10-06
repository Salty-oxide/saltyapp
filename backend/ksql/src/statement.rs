//! What a ksql statement is, and whether Salty will send it.
//!
//! Two jobs, both decided before anything reaches the network:
//!
//! 1. **Which endpoint.** ksqlDB splits its API — `/ksql` takes `SHOW`,
//!    `DESCRIBE`, `EXPLAIN` and `CREATE`, while `/query-stream` takes `SELECT`
//!    and `PRINT`. The statement's own shape decides, so there is no second
//!    parser.
//! 2. **Whether it may run at all.** A free-text editor that reaches a cluster
//!    is the one place in this app where a user could write to a topic without
//!    passing the publishing gate, or delete a topic outright. Neither is
//!    allowed to happen by accident.
//!
//! **Why this lives here and not in `salty_core`.** `publish_refusal` went to
//! core because `salty-kafka` needs a real broker to test anything, so its
//! policy had nowhere else to be exercised. This crate needs nothing but
//! string parsing: `cargo test -p salty-ksql` reaches every line below
//! directly, so the policy stays next to the client that applies it and core
//! stays unbloated.
//!
//! **This is not a SQL parser and must not become one.** It classifies a
//! statement by its leading keywords, which is all that is needed to route it
//! and to refuse the dangerous shapes. Anything it cannot recognise is
//! [`StatementKind::Unknown`] and is refused rather than guessed at — see
//! [`decide`].

use serde::{Deserialize, Serialize};

/// What kind of ksql statement this is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StatementKind {
    /// `SELECT … EMIT CHANGES` — a push query, streamed until stopped.
    Select,
    /// `PRINT 'topic'` — raw topic output, also streamed.
    Print,
    /// `SHOW`/`LIST`, `DESCRIBE`, `EXPLAIN` — metadata reads.
    Describe,
    /// `CREATE STREAM/TABLE … WITH (…)` — registers a view over an existing
    /// topic. Writes no data and starts no query.
    CreateRegistration,
    /// `CREATE STREAM/TABLE … AS SELECT` — starts a persistent query that
    /// keeps running on the server after Salty closes.
    CreateAsSelect,
    /// `INSERT INTO … VALUES` — writes records to a topic.
    Insert,
    /// `DROP STREAM/TABLE`, optionally deleting the backing topic.
    Drop,
    /// `TERMINATE <queryId>` — stops someone else's persistent query.
    Terminate,
    /// Anything this classifier does not recognise.
    Unknown,
}

impl StatementKind {
    /// Whether the result arrives as a stream of rows rather than one response.
    ///
    /// This is what decides the endpoint: streaming kinds go to
    /// `/query-stream`, everything else to `/ksql`.
    pub fn is_streaming(self) -> bool {
        matches!(self, StatementKind::Select | StatementKind::Print)
    }
}

/// Whether a statement may be sent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "outcome")]
pub enum Decision {
    /// Send it.
    Allowed,
    /// Refused, with a reason written for the person who typed it.
    Refused { reason: String },
}

impl Decision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Decision::Allowed)
    }
}

/// Strips comments and leading whitespace so the keyword check sees the
/// statement itself.
///
/// ksql uses SQL's comment syntax, and a pasted query routinely opens with a
/// `--` line. Without this, a commented statement classifies as `Unknown` and
/// is refused — which reads as Salty rejecting valid ksql.
fn strip_leading_noise(sql: &str) -> String {
    let mut rest = sql.trim_start();
    loop {
        if let Some(after) = rest.strip_prefix("--") {
            // Line comment: drop through the newline, or the whole remainder
            // when the comment is the last thing in the box.
            rest = match after.find('\n') {
                Some(index) => after[index + 1..].trim_start(),
                None => "",
            };
        } else if let Some(after) = rest.strip_prefix("/*") {
            // Block comment. An unterminated one consumes the rest, which then
            // classifies as Unknown and is refused — the right outcome for a
            // statement that is not valid ksql either.
            rest = match after.find("*/") {
                Some(index) => after[index + 2..].trim_start(),
                None => "",
            };
        } else {
            return rest.to_string();
        }
    }
}

/// Collapses runs of whitespace so multi-line statements match the same way
/// single-line ones do.
fn normalized(sql: &str) -> String {
    strip_leading_noise(sql)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_uppercase()
}

/// Classifies a statement by its leading keywords.
pub fn classify(sql: &str) -> StatementKind {
    let text = normalized(sql);

    if text.starts_with("SELECT ") || text == "SELECT" {
        return StatementKind::Select;
    }
    if text.starts_with("PRINT ") || text == "PRINT" {
        return StatementKind::Print;
    }
    if text.starts_with("SHOW ")
        || text.starts_with("LIST ")
        || text.starts_with("DESCRIBE ")
        || text.starts_with("EXPLAIN ")
    {
        return StatementKind::Describe;
    }
    if text.starts_with("INSERT INTO ") {
        return StatementKind::Insert;
    }
    if text.starts_with("DROP ") {
        return StatementKind::Drop;
    }
    if text.starts_with("TERMINATE ") {
        return StatementKind::Terminate;
    }
    if text.starts_with("CREATE ") {
        // The distinction that matters most in this file. Both forms start
        // identically; only the presence of `AS SELECT` says whether this
        // registers a view over an existing topic or starts a query that
        // outlives the app. `CREATE OR REPLACE` and `CREATE SOURCE` are both
        // covered, since neither changes the tail.
        return if text.contains(" AS SELECT") {
            StatementKind::CreateAsSelect
        } else {
            StatementKind::CreateRegistration
        };
    }
    StatementKind::Unknown
}

/// Whether a statement of this kind may be sent from this connection.
///
/// `allow_publishing` is the connection's existing column — the same one the
/// Publish tab is gated on. Routing `INSERT INTO` through it rather than a
/// parallel switch is the point: without that, a text box would outrank a
/// connection explicitly marked as unable to publish, and the four gates that
/// stand in front of every other write (`allow_publishing` defaulting to 0,
/// the connected-cluster check, the cached broker denial, `encode_messages`
/// validation) would all be moot.
pub fn decide(kind: StatementKind, allow_publishing: bool) -> Decision {
    match kind {
        StatementKind::Select
        | StatementKind::Print
        | StatementKind::Describe
        | StatementKind::CreateRegistration => Decision::Allowed,

        StatementKind::Insert => {
            if allow_publishing {
                Decision::Allowed
            } else {
                Decision::Refused {
                    reason: "This connection is not allowed to publish. \
                             Enable publishing for it before using INSERT INTO."
                        .into(),
                }
            }
        }

        StatementKind::CreateAsSelect => Decision::Refused {
            reason: "CREATE … AS SELECT starts a persistent query that keeps running on the \
                     ksqlDB server after Salty closes. Salty does not create or manage those — \
                     use the ksqlDB CLI if you need one."
                .into(),
        },

        StatementKind::Drop => Decision::Refused {
            reason: "DROP is not available from Salty. It can delete a stream's backing Kafka \
                     topic, and nothing here can put the records back."
                .into(),
        },

        StatementKind::Terminate => Decision::Refused {
            reason: "TERMINATE stops a persistent query that something else is relying on. \
                     Salty does not manage those — use the ksqlDB CLI."
                .into(),
        },

        StatementKind::Unknown => Decision::Refused {
            reason: "Salty does not recognise this statement, so it will not send it. \
                     Supported: SELECT, PRINT, SHOW, LIST, DESCRIBE, EXPLAIN, \
                     CREATE STREAM/TABLE and INSERT INTO."
                .into(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_a_push_query() {
        assert_eq!(
            classify("SELECT * FROM orders EMIT CHANGES;"),
            StatementKind::Select
        );
    }

    #[test]
    fn classifies_print() {
        assert_eq!(classify("PRINT 'orders' FROM BEGINNING;"), StatementKind::Print);
    }

    #[test]
    fn classifies_the_metadata_reads() {
        for sql in [
            "SHOW STREAMS;",
            "LIST TOPICS;",
            "DESCRIBE orders;",
            "EXPLAIN my_query;",
        ] {
            assert_eq!(classify(sql), StatementKind::Describe, "for {sql}");
        }
    }

    #[test]
    fn classifies_insert() {
        assert_eq!(
            classify("INSERT INTO orders (id) VALUES ('a');"),
            StatementKind::Insert
        );
    }

    #[test]
    fn classifies_drop_and_terminate() {
        assert_eq!(classify("DROP STREAM orders;"), StatementKind::Drop);
        assert_eq!(classify("TERMINATE CTAS_ORDERS_5;"), StatementKind::Terminate);
    }

    // The distinction the whole classifier exists for. Both start with CREATE
    // STREAM; only one of them leaves a query running on the server.
    #[test]
    fn separates_registering_a_stream_from_starting_a_persistent_query() {
        assert_eq!(
            classify("CREATE STREAM s WITH (KAFKA_TOPIC='orders', VALUE_FORMAT='AVRO');"),
            StatementKind::CreateRegistration
        );
        assert_eq!(
            classify("CREATE STREAM s AS SELECT * FROM t EMIT CHANGES;"),
            StatementKind::CreateAsSelect
        );
    }

    #[test]
    fn treats_create_table_as_select_the_same_way() {
        assert_eq!(
            classify("CREATE TABLE t AS SELECT id, COUNT(*) FROM s GROUP BY id;"),
            StatementKind::CreateAsSelect
        );
    }

    #[test]
    fn handles_create_or_replace_and_source_forms() {
        assert_eq!(
            classify("CREATE OR REPLACE STREAM s WITH (KAFKA_TOPIC='t');"),
            StatementKind::CreateRegistration
        );
        assert_eq!(
            classify("CREATE SOURCE TABLE t AS SELECT * FROM s;"),
            StatementKind::CreateAsSelect
        );
    }

    #[test]
    fn is_case_insensitive() {
        assert_eq!(classify("select * from orders;"), StatementKind::Select);
        assert_eq!(classify("DrOp StReAm orders;"), StatementKind::Drop);
    }

    #[test]
    fn ignores_leading_whitespace_and_newlines() {
        assert_eq!(classify("\n\n   SELECT 1;"), StatementKind::Select);
    }

    // A pasted query routinely opens with a comment. Classifying that as
    // Unknown would read as Salty rejecting valid ksql.
    #[test]
    fn looks_past_a_leading_line_comment() {
        assert_eq!(
            classify("-- last hour of orders\nSELECT * FROM orders EMIT CHANGES;"),
            StatementKind::Select
        );
    }

    #[test]
    fn looks_past_several_leading_comments() {
        assert_eq!(
            classify("-- one\n-- two\n/* three */\nDROP STREAM s;"),
            StatementKind::Drop
        );
    }

    #[test]
    fn looks_past_a_leading_block_comment() {
        assert_eq!(classify("/* note */ SELECT 1;"), StatementKind::Select);
    }

    #[test]
    fn treats_an_unterminated_block_comment_as_unrecognised() {
        assert_eq!(classify("/* never closed SELECT 1;"), StatementKind::Unknown);
    }

    #[test]
    fn treats_a_comment_only_box_as_unrecognised() {
        assert_eq!(classify("-- just a note"), StatementKind::Unknown);
    }

    #[test]
    fn treats_an_empty_statement_as_unrecognised() {
        assert_eq!(classify(""), StatementKind::Unknown);
        assert_eq!(classify("   \n  "), StatementKind::Unknown);
    }

    // A statement split across lines has to classify as the same thing as the
    // single-line form — the editor is a textarea and people use it.
    #[test]
    fn collapses_whitespace_so_multiline_statements_classify_the_same() {
        assert_eq!(
            classify("CREATE STREAM s\n  AS\n  SELECT * FROM t;"),
            StatementKind::CreateAsSelect
        );
    }

    // A word starting with a keyword is not that keyword.
    #[test]
    fn does_not_match_a_keyword_that_is_only_a_prefix_of_an_identifier() {
        assert_eq!(classify("SELECTED_VALUES FROM x;"), StatementKind::Unknown);
        assert_eq!(classify("DROPLET;"), StatementKind::Unknown);
    }

    #[test]
    fn routes_only_queries_to_the_streaming_endpoint() {
        assert!(StatementKind::Select.is_streaming());
        assert!(StatementKind::Print.is_streaming());
        assert!(!StatementKind::Describe.is_streaming());
        assert!(!StatementKind::CreateRegistration.is_streaming());
        assert!(!StatementKind::Insert.is_streaming());
    }

    #[test]
    fn allows_reads_and_registration_regardless_of_the_publishing_gate() {
        for kind in [
            StatementKind::Select,
            StatementKind::Print,
            StatementKind::Describe,
            StatementKind::CreateRegistration,
        ] {
            assert!(decide(kind, false).is_allowed(), "for {kind:?}");
            assert!(decide(kind, true).is_allowed(), "for {kind:?}");
        }
    }

    // The gate that stops a text box outranking the Publish tab.
    #[test]
    fn refuses_insert_when_the_connection_may_not_publish() {
        let decision = decide(StatementKind::Insert, false);

        assert!(!decision.is_allowed());
        match decision {
            Decision::Refused { reason } => assert!(reason.contains("not allowed to publish")),
            Decision::Allowed => unreachable!(),
        }
    }

    #[test]
    fn allows_insert_when_the_connection_may_publish() {
        assert!(decide(StatementKind::Insert, true).is_allowed());
    }

    // Enabling publishing must not become a skeleton key for the destructive
    // statements: it is a statement about writing records, not about dropping
    // topics or killing other people's queries.
    #[test]
    fn refuses_the_destructive_statements_even_with_publishing_enabled() {
        for kind in [
            StatementKind::Drop,
            StatementKind::Terminate,
            StatementKind::CreateAsSelect,
        ] {
            assert!(!decide(kind, true).is_allowed(), "for {kind:?}");
        }
    }

    #[test]
    fn refuses_what_it_does_not_recognise_rather_than_guessing() {
        assert!(!decide(StatementKind::Unknown, true).is_allowed());
    }

    #[test]
    fn every_refusal_explains_itself() {
        for kind in [
            StatementKind::Drop,
            StatementKind::Terminate,
            StatementKind::CreateAsSelect,
            StatementKind::Unknown,
            StatementKind::Insert,
        ] {
            match decide(kind, false) {
                Decision::Refused { reason } => {
                    assert!(!reason.is_empty(), "for {kind:?}");
                    // A refusal a user cannot act on is not much better than a
                    // silent failure.
                    assert!(reason.len() > 20, "reason too terse for {kind:?}: {reason}");
                }
                Decision::Allowed => unreachable!("{kind:?} must be refused"),
            }
        }
    }
}
