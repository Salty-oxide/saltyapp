//! ksqlDB commands.
//!
//! Wrappers only. The statement policy, the protocol and the HTTP all live in
//! `salty_ksql`, where `cargo test -p salty-ksql` reaches them — this crate
//! needs a desktop toolchain and a running Tauri app to exercise at all, and
//! is excluded from the coverage ratio for that reason.

use std::time::Duration;

use salty_core::Connection;
use salty_ksql::{classify, decide, Decision, KsqlClient, KsqlEndpoint, KsqlRow};
use serde::Serialize;
use tauri::{AppHandle, Emitter, State};

use super::connections::{connection_for_request, log_broker_call, CommandError};
use crate::state::AppState;

/// How many rows travel in one `"ksql-rows"` event, and how long a partly
/// filled batch waits. Matched to the message stream's own values — the grid
/// on the other end coalesces arrivals the same way, so there is nothing to
/// gain from a different cadence.
const ROW_BATCH_SIZE: usize = 100;
const ROW_BATCH_INTERVAL: Duration = Duration::from_millis(100);
/// Rows between Logs-panel progress lines, as for a fetch.
const PROGRESS_LOG_INTERVAL: usize = 500;

/// The columns a query returned, sent once before any rows.
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KsqlHeaderEvent {
    pub request_id: String,
    pub columns: Vec<salty_ksql::KsqlColumn>,
}

/// A batch of result rows.
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KsqlRowsEvent {
    pub request_id: String,
    pub rows: Vec<KsqlRow>,
}

/// What a finished query produced.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KsqlQueryOutcome {
    /// True when Stop ended it rather than the server closing the stream.
    pub cancelled: bool,
    pub row_count: usize,
}

/// A non-streaming statement's response, as ksqlDB's own JSON.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KsqlStatementResult {
    pub body: String,
}

/// Builds the client for a connection, or explains why it cannot.
///
/// A connection with no endpoint is **not** an error the user should see as a
/// failure — most clusters run no ksqlDB server, and such a connection is not
/// misconfigured. It is reported as a plain message the workspace turns into a
/// notice pointing at the ksqlDB tab.
fn client_for(connection: &Connection) -> Result<KsqlClient, CommandError> {
    let url = connection
        .ksqldb_endpoint
        .as_deref()
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .ok_or_else(|| CommandError {
            message: "This connection has no ksqlDB server configured. \
                      Add one on the connection's ksqlDB tab."
                .to_string(),
        })?;

    Ok(KsqlClient::new(KsqlEndpoint {
        url: url.to_string(),
        basic_auth: connection.ksqldb_basic_auth_credentials.clone(),
    })?)
}

/// Applies the statement policy before anything reaches the network.
fn permit(sql: &str, connection: &Connection) -> Result<salty_ksql::StatementKind, CommandError> {
    let kind = classify(sql);
    match decide(kind, connection.allow_publishing) {
        Decision::Allowed => Ok(kind),
        Decision::Refused { reason } => Err(CommandError { message: reason }),
    }
}

/// Runs a statement that returns one response — `SHOW`, `DESCRIBE`,
/// `EXPLAIN`, `CREATE STREAM`.
#[tauri::command]
pub async fn ksql_statement(
    app: AppHandle,
    state: State<'_, AppState>,
    id: String,
    sql: String,
) -> Result<KsqlStatementResult, CommandError> {
    let connection = connection_for_request(&state, &id).await?;
    let kind = permit(&sql, &connection)?;
    if kind.is_streaming() {
        return Err(CommandError {
            message: "This statement streams rows — run it as a query instead.".to_string(),
        });
    }
    let client = client_for(&connection)?;

    let started = std::time::Instant::now();
    let result = client.statement(&sql).await;
    log_broker_call(
        &app,
        "Running ksqlDB statement",
        started,
        if result.is_ok() { "finished" } else { "failed" },
    );
    Ok(KsqlStatementResult { body: result? })
}

/// The stream registered over a topic, if any.
///
/// Backs the topic tab, which cannot offer a query until it knows the stream's
/// name — ksqlDB cannot `SELECT` from a raw topic.
#[tauri::command]
pub async fn ksql_stream_for_topic(
    state: State<'_, AppState>,
    id: String,
    topic: String,
) -> Result<Option<String>, CommandError> {
    let connection = connection_for_request(&state, &id).await?;
    let client = client_for(&connection)?;
    Ok(client.stream_for_topic(&topic).await?)
}

/// Runs a push query, streaming its rows to the frontend as they arrive.
///
/// Rows travel in `"ksql-rows"` events tagged with `request_id`, so a
/// superseded query's rows cannot land in a new query's grid — the same
/// arrangement the message stream uses, reusing the same batching helper.
#[tauri::command]
pub async fn ksql_query(
    app: AppHandle,
    state: State<'_, AppState>,
    id: String,
    sql: String,
    request_id: String,
) -> Result<KsqlQueryOutcome, CommandError> {
    let cancelled = state.fetch_cancellations.begin_for_connection(&request_id, &id);

    // Everything that can refuse the query before it starts, in one place.
    // Each of these used to release the registration itself, which meant four
    // copies of the same cleanup and four chances to forget one — a forgotten
    // release leaks the entry and leaves Stop cancelling a request that no
    // longer exists.
    let prepared = async {
        let connection = connection_for_request(&state, &id).await?;
        let kind = permit(&sql, &connection)?;
        if !kind.is_streaming() {
            return Err(CommandError {
                message: "This statement returns a single response — run it as a statement instead."
                    .to_string(),
            });
        }
        client_for(&connection)
    }
    .await;

    let client = match prepared {
        Ok(client) => client,
        Err(err) => {
            state.fetch_cancellations.finish(&request_id);
            return Err(err);
        }
    };

    crate::logging::emit_log(&app, "info", "Running ksqlDB query...".to_string());
    let started = std::time::Instant::now();

    // The rows cross into the batcher on a channel so the HTTP read loop never
    // waits on the webview.
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<KsqlRow>();
    let emit_app = app.clone();
    let emit_request_id = request_id.clone();
    let progress_app = app.clone();
    let batch_cancelled = std::sync::Arc::clone(&cancelled);
    let forward = tokio::spawn(async move {
        salty_core::forward_in_batches(
            rx,
            batch_cancelled,
            ROW_BATCH_SIZE,
            ROW_BATCH_INTERVAL,
            PROGRESS_LOG_INTERVAL,
            move |rows| {
                let _ = emit_app.emit(
                    "ksql-rows",
                    KsqlRowsEvent { request_id: emit_request_id.clone(), rows },
                );
            },
            move |seen| {
                crate::logging::emit_log(
                    &progress_app,
                    "info",
                    format!("ksqlDB query: {seen} rows so far..."),
                );
            },
        )
        .await
    });

    // Emitted from inside the stream, the moment the column frame arrives.
    // A push query does not end until it is stopped, so a grid that waited for
    // this call to return would never learn its columns at all.
    let header_app = app.clone();
    let header_request_id = request_id.clone();
    let outcome = client
        .query_stream(
            &sql,
            std::sync::Arc::clone(&cancelled),
            |header| {
                let _ = header_app.emit(
                    "ksql-header",
                    KsqlHeaderEvent {
                        request_id: header_request_id.clone(),
                        columns: header.columns.clone(),
                    },
                );
            },
            |row| {
                let _ = tx.send(row);
            },
        )
        .await;

    // Dropping the sender is what lets the batcher finish its last partial
    // batch and return.
    drop(tx);
    let row_count = forward.await.unwrap_or(0);
    state.fetch_cancellations.finish(&request_id);

    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(err) => {
            log_broker_call(&app, "Running ksqlDB query", started, "failed");
            return Err(err.into());
        }
    };

    log_broker_call(&app, "Running ksqlDB query", started, "finished");

    // Best effort, and deliberately not fatal: the rows are already delivered,
    // and the query is very likely gone with the socket. Reporting a failure
    // here would turn a successful query into an error.
    if let Some(query_id) = outcome.header.as_ref().and_then(|h| h.query_id.as_deref())
        && outcome.cancelled
    {
        let _ = client.close_query(query_id).await;
    }

    Ok(KsqlQueryOutcome { cancelled: outcome.cancelled, row_count })
}

/// Stops a running query. The Stop button.
#[tauri::command]
pub async fn ksql_cancel(state: State<'_, AppState>, request_id: String) -> Result<(), CommandError> {
    state.fetch_cancellations.cancel(&request_id);
    Ok(())
}
