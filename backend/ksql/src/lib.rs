//! A ksqlDB client.
//!
//! **Salty is a client, not an engine.** ksqlDB is a JVM server that owns
//! query execution; there is no Rust implementation of it to embed. (The
//! `ksql` crate on crates.io is an unrelated JSON expression parser, and
//! `ksqldb` is a pre-release REST wrapper.) So this crate speaks ksqlDB's HTTP
//! API and nothing more, and a ksqlDB server is a prerequisite the user must
//! already run.
//!
//! Three modules, and only one of them touches the network:
//!
//! - [`statement`] decides what may be sent and which endpoint it goes to.
//! - [`protocol`] reads what comes back.
//! - [`client`] moves the bytes.
//!
//! The split is what makes this crate testable without a JVM: everything that
//! decides anything is a pure function over strings.

pub mod client;
pub mod protocol;
pub mod statement;

pub use client::{KsqlClient, KsqlEndpoint};
pub use protocol::{
    parse_header, parse_row, parse_server_error, parse_streams, stream_for_topic, KsqlColumn,
    KsqlQueryHeader, KsqlRow, KsqlServerError,
};
pub use statement::{classify, decide, Decision, StatementKind};
