use crate::state::AppState;
use error_stack::ResultExt;
use salty_core::{
    partition_importable, select_for_export, AppError, Connection, ConnectionExportFile,
    ConnectionStatus, MessagesBatchEvent, NewConnection,
};
use salty_kafka::BrokerSslConfig;
use std::collections::HashSet;
use std::time::Duration;
use tauri::{AppHandle, Emitter, State};

#[derive(serde::Serialize)]
pub struct CommandError {
    pub message: String,
}

impl From<error_stack::Report<salty_core::AppError>> for CommandError {
    fn from(report: error_stack::Report<salty_core::AppError>) -> Self {
        CommandError {
            message: format_report(&report),
        }
    }
}

/// Renders a report as a single readable line for the frontend to show
/// as-is: the top-level `AppError`'s message, followed by each
/// `.attach(...)` reason in the chain. Plain `{report:?}` isn't
/// fit for end users — it includes box-drawing characters and `at
/// file:line` source locations meant for developers reading logs.
fn format_report(report: &error_stack::Report<salty_core::AppError>) -> String {
    let mut parts = vec![report.current_context().to_string()];
    parts.push(report_reasons(report));
    parts.retain(|part| !part.is_empty());
    parts.join(": ")
}

/// Just the `.attach(...)` reasons from a report, without the
/// `AppError` heading — for callers that already say what went wrong and
/// only need the detail (e.g. the reason stored against a connection whose
/// credentials were rejected, which is shown under its name in the tree).
pub(crate) fn report_reasons(report: &error_stack::Report<salty_core::AppError>) -> String {
    use error_stack::{AttachmentKind, FrameKind};

    report
        .frames()
        .filter_map(|frame| match frame.kind() {
            FrameKind::Attachment(AttachmentKind::Printable(printable)) => {
                Some(printable.to_string())
            }
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(": ")
}

/// Loads a saved connection for a request that will talk to the broker,
/// refusing it outright when the connection's authentication circuit breaker
/// has tripped.
///
/// This is the fail-fast gate. Once the broker has rejected a connection's
/// credentials `MAX_AUTH_ATTEMPTS` times, every further request for it is
/// answered from memory: no client is created, no socket is opened, no
/// handshake reaches the cluster — however hard the user clicks, and however
/// many users have the same stale password saved. Retrying rejected
/// credentials cannot succeed; all it can do is cost the brokers CPU and
/// fill their logs.
///
/// The breaker is cleared by editing the connection (see
/// `connection_update`) or by an explicit Reconnect (see
/// `connection_connect`, which clears it before calling this) — both are
/// deliberate acts by a user who has had a chance to fix the credentials.
pub(crate) async fn connection_for_request(
    state: &AppState,
    id: &str,
) -> Result<Connection, CommandError> {
    if let Some(reason) = state.connections.auth_block_reason(id) {
        return Err(CommandError {
            message: format!(
                "{}: authentication failed for this connection, so it is not being retried: {reason}. \
                 Use Reconnect to try again, or edit the connection's credentials and save.",
                AppError::Authentication
            ),
        });
    }
    Ok(salty_db::connections::get(&state.pool, id).await?)
}

/// Feeds a broker call's outcome back to the connection's circuit breaker:
/// a rejection counts against its attempt allowance, and any success clears
/// the slate. Only `AppError::Authentication` counts — a timeout or a
/// transport failure must stay retryable (see `salty_kafka::auth`).
fn record_auth_outcome<T>(
    state: &AppState,
    id: &str,
    result: &Result<T, error_stack::Report<AppError>>,
) {
    match result {
        Ok(_) => state.connections.record_auth_success(id),
        Err(report) => {
            if matches!(report.current_context(), AppError::Authentication) {
                state
                    .connections
                    .record_auth_failure(id, &report_reasons(report));
                // Keeping a client the broker has rejected would mean the
                // next request reuses a connection that cannot work.
                state.kafka.release(id);
            }
        }
    }
}

/// Feeds only the *success* half of a broker call's outcome to the breaker.
///
/// Used by the consumer-group calls, and only by them. Listing groups sits
/// behind ACLs of its own — `Describe` on the `Group` resource, and a
/// cluster-wide list — so a principal with perfectly valid credentials and
/// full access to every topic can still be refused here. Counting that
/// against the connection's attempt allowance would take an otherwise
/// working cluster offline inside the app over a capability the user may
/// never have needed: brokers and topics would stop loading because
/// consumer groups didn't.
///
/// So a failure here is reported to the caller and nowhere else. A success
/// still clears the slate, because it is proof the credentials work.
///
/// The pooled client is still dropped on an authentication failure — a
/// client the broker has rejected cannot serve the next request either.
pub(crate) fn record_auth_success_only<T>(
    state: &AppState,
    id: &str,
    result: &Result<T, error_stack::Report<AppError>>,
) {
    match result {
        Ok(_) => state.connections.record_auth_success(id),
        Err(report) => {
            if matches!(report.current_context(), AppError::Authentication) {
                state.kafka.release(id);
            }
        }
    }
}

/// Logs how long a broker round trip took, to the Logs panel.
///
/// Every one of these commands builds a Kafka client from scratch, so its
/// duration is a connection setup (TCP, plus TLS and SASL on a secured
/// cluster) *plus* the request itself — not the request alone. When someone
/// asks why listing topics on a freshly connected cluster isn't instant,
/// this is the number that answers it, per call, from their own machine and
/// their own cluster.
pub(crate) fn log_broker_call(
    app: &AppHandle,
    what: &str,
    started: std::time::Instant,
    outcome: &str,
) {
    crate::logging::emit_log(
        app,
        "info",
        format!("{what} {outcome} in {} ms", started.elapsed().as_millis()),
    );
}

#[tauri::command]
pub async fn connection_list(state: State<'_, AppState>) -> Result<Vec<Connection>, CommandError> {
    Ok(salty_db::connections::list(&state.pool).await?)
}

#[tauri::command]
pub async fn connection_create(
    app: AppHandle,
    state: State<'_, AppState>,
    new_connection: NewConnection,
) -> Result<Connection, CommandError> {
    let started = std::time::Instant::now();
    let connection = salty_db::connections::create(&state.pool, &new_connection).await?;
    crate::logging::emit_log(
        &app,
        "info",
        format!(
            "Created connection \"{}\" in {} ms",
            connection.name,
            started.elapsed().as_millis()
        ),
    );
    Ok(connection)
}

#[tauri::command]
pub async fn connection_update(
    app: AppHandle,
    state: State<'_, AppState>,
    id: String,
    new_connection: NewConnection,
) -> Result<Connection, CommandError> {
    let started = std::time::Instant::now();
    let connection = salty_db::connections::update(&state.pool, &id, &new_connection).await?;
    // The settings the broker rejected no longer exist, so the verdict on
    // them is meaningless — give the edited connection a clean slate.
    state.connections.clear_auth_failures(&id);
    // The pooled client was built from the settings that just changed.
    state.kafka.release(&id);
    // Same reasoning for the Schema Registry client: its endpoint and
    // credentials come from this connection, and its cache is keyed by schema
    // id — which means nothing once the registry it points at may have
    // changed. `get_or_create` would notice via its fingerprint anyway; this
    // makes it immediate and keeps the two pools behaving identically.
    state.schema_registry.release(&id);
    crate::logging::emit_log(
        &app,
        "info",
        format!(
            "Updated connection \"{}\" in {} ms",
            connection.name,
            started.elapsed().as_millis()
        ),
    );
    Ok(connection)
}

#[tauri::command]
pub async fn connection_delete(
    app: AppHandle,
    state: State<'_, AppState>,
    id: String,
) -> Result<(), CommandError> {
    let started = std::time::Instant::now();
    salty_db::connections::delete(&state.pool, &id).await?;
    salty_db::topic_schemas::delete_all_for_connection(&state.pool, &id).await?;
    state.connections.mark_disconnected(&id);
    state.connections.clear_auth_failures(&id);
    state.kafka.release(&id);
    // Nothing will ever ask for this connection's schemas again, and the
    // client holds an open HTTPS connection to the registry.
    state.schema_registry.release(&id);
    crate::logging::emit_log(
        &app,
        "info",
        format!(
            "Deleted connection {id} in {} ms",
            started.elapsed().as_millis()
        ),
    );
    Ok(())
}

#[derive(serde::Serialize)]
pub struct ImportSummary {
    pub imported: usize,
    pub skipped: usize,
}

/// Backs the sidebar's per-connection "Export Connection" context-menu item
/// (`ids: Some([id])`) and its "Export All" button (`ids: None`). `path` is
/// resolved by the frontend beforehand via the native save dialog — this
/// command only builds the file contents and writes them. Never includes
/// credentials, even though `Connection` itself now carries them:
/// `select_for_export` only ever produces `PortableConnection`s, whose field
/// list deliberately excludes every secret (see its doc comment).
#[tauri::command]
pub async fn connections_export(
    state: State<'_, AppState>,
    ids: Option<Vec<String>>,
    path: String,
) -> Result<(), CommandError> {
    let all = salty_db::connections::list(&state.pool).await?;
    let portable = select_for_export(&all, ids.as_deref());
    let file = ConnectionExportFile::new(portable);
    let json = file
        .to_json_pretty()
        .change_context(AppError::Validation)
        .attach("failed to serialize the connections export file")?;
    std::fs::write(&path, json)
        .change_context(AppError::Validation)
        .attach("failed to write the connections export file")?;
    Ok(())
}

/// Backs the sidebar's "Import" button. `path` is resolved by the frontend
/// via the native open dialog. Connections whose name matches an existing
/// one are left untouched (see `partition_importable`'s doc comment) rather
/// than overwritten or duplicated; imported connections always land with
/// empty credential fields, same as any other freshly created connection.
#[tauri::command]
pub async fn connections_import(
    state: State<'_, AppState>,
    path: String,
) -> Result<ImportSummary, CommandError> {
    let text = std::fs::read_to_string(&path)
        .change_context(AppError::Validation)
        .attach("failed to read the connections export file")?;
    let file = ConnectionExportFile::parse(&text)?;

    let existing = salty_db::connections::list(&state.pool).await?;
    let existing_names: HashSet<String> = existing
        .into_iter()
        .map(|connection| connection.name)
        .collect();
    let (importable, skipped) = partition_importable(&file.connections, &existing_names);
    let imported = importable.len();

    for portable in importable {
        let new_connection: NewConnection = portable.clone().into();
        salty_db::connections::create(&state.pool, &new_connection).await?;
    }

    Ok(ImportSummary { imported, skipped })
}

#[tauri::command]
pub async fn connection_check_status(
    state: State<'_, AppState>,
    id: String,
) -> Result<ConnectionStatus, CommandError> {
    let connection = salty_db::connections::get(&state.pool, &id).await?;
    Ok(state.kafka.check_status(&connection).await?)
}

/// Backs the ping button next to "Bootstrap servers" in the New Connection
/// modal's General section — a plain TCP reachability check of whatever the
/// user has typed so far, independent of the Security tab.
#[tauri::command]
pub async fn connection_ping_bootstrap(
    state: State<'_, AppState>,
    bootstrap_servers: String,
) -> Result<ConnectionStatus, CommandError> {
    Ok(state.kafka.ping_bootstrap(&bootstrap_servers).await?)
}

/// Backs the ping button next to "Host" in the New Connection modal's
/// Zookeeper section. `timeout_ms` is the user's General settings >
/// Zookeeper > Timeout value.
#[tauri::command]
pub async fn connection_ping_zookeeper(
    state: State<'_, AppState>,
    host: String,
    port: u16,
    timeout_ms: u64,
) -> Result<ConnectionStatus, CommandError> {
    Ok(state.zookeeper.ping(&host, port, timeout_ms).await)
}

/// Backs the New Connection modal's bottom "Test" button — tests
/// connectivity using every value currently entered in the modal, without
/// requiring the connection to be saved first.
#[tauri::command]
pub async fn connection_test(
    state: State<'_, AppState>,
    new_connection: NewConnection,
) -> Result<ConnectionStatus, CommandError> {
    Ok(state
        .kafka
        .test_connection(
            &new_connection.bootstrap_servers,
            new_connection.security_protocol,
            new_connection.sasl_mechanism,
            new_connection.sasl_username.as_deref(),
            new_connection.sasl_password.as_deref(),
            BrokerSslConfig {
                truststore_location: new_connection.ssl_truststore_location.as_deref(),
                keystore_location: new_connection.ssl_keystore_location.as_deref(),
                keystore_password: new_connection.ssl_keystore_password.as_deref(),
                keystore_key_password: new_connection.ssl_keystore_key_password.as_deref(),
            },
        )
        .await?)
}

/// Backs the cluster detail panel's "Reconnect" button. Authenticates
/// against the saved connection and, only on success, marks it connected in
/// `AppState::connections` — this is what gates the tree's Brokers/Topics/
/// Consumers expansion and the panel's field-disabling.
///
/// Reconnect *spends* one of the connection's authentication attempts; it
/// does not hand back a fresh allowance. Otherwise clicking Reconnect would
/// reset the counter every time and the breaker could never trip on the one
/// path that matters most — a saved-but-wrong password, which is exactly
/// what a user clicks Reconnect at. Editing the connection is what clears
/// the slate, because that is the act that can actually fix the problem.
#[tauri::command]
pub async fn connection_connect(
    app: AppHandle,
    state: State<'_, AppState>,
    id: String,
) -> Result<ConnectionStatus, CommandError> {
    // A click on Reconnect is the deliberate retry the breaker is meant to
    // allow — `connection_for_request`'s own documentation has said so all
    // along, and this is what finally makes it true. Without it that function
    // refused this call too, so a connection whose credentials had been
    // rejected could only be un-blocked by opening it, changing something and
    // saving: the tree's Reconnect answered "not being retried" however many
    // times it was clicked, including after the password had been fixed on
    // the broker's side rather than in the app.
    //
    // Only this explicit, human-initiated path clears it. Everything the app
    // does on its own — the tree's listings, the status polls, a fetch — still
    // goes through `connection_for_request` and stays blocked, which is the
    // hammering the breaker exists to prevent.
    state.connections.clear_auth_failures(&id);
    let connection = connection_for_request(&state, &id).await?;

    let started = std::time::Instant::now();
    // Authenticates *and* leaves the connection open for the requests that
    // follow — the tree's brokers/topics/consumers land on this same client
    // instead of each paying for its own handshake.
    let result = state.kafka.connect(&connection).await;
    record_auth_outcome(&state, &id, &result);
    log_broker_call(
        &app,
        "Connecting",
        started,
        if result.is_ok() { "finished" } else { "failed" },
    );

    let status = result?;
    if status == ConnectionStatus::Reachable {
        state.connections.mark_connected(&id);
    }
    Ok(status)
}

/// Backs the cluster detail panel's "Disconnect" button.
#[tauri::command]
pub async fn connection_disconnect(
    app: AppHandle,
    state: State<'_, AppState>,
    id: String,
) -> Result<(), CommandError> {
    let started = std::time::Instant::now();
    state.connections.mark_disconnected(&id);
    // A fetch already inside its poll loop holds its own consumer, so
    // releasing the pool below stops new requests but not that one — it would
    // keep pulling messages from a cluster the user has just disconnected
    // from, streaming them at a Data tab that has already been cleared.
    let cancelled = state.fetch_cancellations.cancel_all_for_connection(&id);
    if !cancelled.is_empty() {
        crate::logging::emit_log(
            &app,
            "info",
            format!(
                "Stopped {} in-flight fetch(es) on disconnect in {} ms",
                cancelled.len(),
                started.elapsed().as_millis()
            ),
        );
    }
    // Disconnect means disconnect: drop the pooled client so the socket
    // actually closes, rather than leaving an idle connection open against
    // the cluster. This is also what the 120-minute idle auto-disconnect
    // ends up calling.
    state.kafka.release(&id);
    // The Schema Registry client is per connection too, and holds an open
    // HTTPS connection plus a cache of schemas read from this cluster's
    // registry. Left behind, disconnecting closed the broker socket and left
    // the registry one open, and a later reconnect went on serving schemas
    // fetched before the disconnect.
    state.schema_registry.release(&id);
    Ok(())
}

#[tauri::command]
pub async fn connection_is_connected(
    state: State<'_, AppState>,
    id: String,
) -> Result<bool, CommandError> {
    Ok(state.connections.is_connected(&id))
}

/// Why this connection's requests are being refused without reaching the
/// broker, or `None` if they aren't. Lets the tree show the user *that* the
/// credentials were rejected — and what the broker said — instead of leaving
/// them clicking a cluster whose every request fails identically.
#[tauri::command]
pub async fn connection_auth_block_reason(
    state: State<'_, AppState>,
    id: String,
) -> Result<Option<String>, CommandError> {
    Ok(state.connections.auth_block_reason(&id))
}

/// Backs the tree's "Brokers" sub-list once a cluster is connected.
/// `read_timeout_ms` is the user's General settings > Brokers > Read Timeout
/// value.
#[tauri::command]
pub async fn connection_list_brokers(
    app: AppHandle,
    state: State<'_, AppState>,
    id: String,
    read_timeout_ms: u64,
) -> Result<Vec<salty_core::BrokerSummary>, CommandError> {
    let connection = connection_for_request(&state, &id).await?;
    let started = std::time::Instant::now();
    let result = state
        .kafka
        .list_brokers(&connection, Duration::from_millis(read_timeout_ms))
        .await;
    record_auth_outcome(&state, &id, &result);
    log_broker_call(
        &app,
        "Listing brokers",
        started,
        if result.is_ok() { "finished" } else { "failed" },
    );
    Ok(result?)
}

/// Backs the tree's "Topics" sub-list once a cluster is connected.
#[tauri::command]
pub async fn connection_list_topics(
    app: AppHandle,
    state: State<'_, AppState>,
    id: String,
    read_timeout_ms: u64,
) -> Result<Vec<salty_core::TopicSummary>, CommandError> {
    let connection = connection_for_request(&state, &id).await?;
    let started = std::time::Instant::now();
    let result = state
        .kafka
        .list_topics(&connection, Duration::from_millis(read_timeout_ms))
        .await;
    record_auth_outcome(&state, &id, &result);
    log_broker_call(
        &app,
        "Listing topics",
        started,
        if result.is_ok() { "finished" } else { "failed" },
    );
    Ok(result?)
}

/// Backs the tree's "Consumers" sub-list once a cluster is connected.
#[tauri::command]
pub async fn connection_list_consumer_groups(
    app: AppHandle,
    state: State<'_, AppState>,
    id: String,
    read_timeout_ms: u64,
) -> Result<Vec<salty_core::ConsumerGroupSummary>, CommandError> {
    let connection = connection_for_request(&state, &id).await?;
    let started = std::time::Instant::now();
    let result = state
        .kafka
        .list_consumer_groups(&connection, Duration::from_millis(read_timeout_ms))
        .await;
    record_auth_success_only(&state, &id, &result);
    log_broker_call(
        &app,
        "Listing consumer groups",
        started,
        if result.is_ok() { "finished" } else { "failed" },
    );
    Ok(result?)
}

/// Backs the topic detail panel's Properties > Messages "Refresh" button.
#[tauri::command]
pub async fn connection_count_topic_messages(
    app: AppHandle,
    state: State<'_, AppState>,
    id: String,
    topic: String,
    read_timeout_ms: u64,
) -> Result<u64, CommandError> {
    let connection = connection_for_request(&state, &id).await?;
    let started = std::time::Instant::now();
    let result = state
        .kafka
        .count_topic_messages(&connection, &topic, Duration::from_millis(read_timeout_ms))
        .await;
    record_auth_outcome(&state, &id, &result);
    log_broker_call(
        &app,
        "Counting topic messages",
        started,
        if result.is_ok() { "finished" } else { "failed" },
    );
    Ok(result?)
}

/// How often (in messages received) to log fetch progress to the Logs
/// panel — frequent enough to reassure the user a large fetch is still
/// moving, without flooding the panel on a fast, high-volume topic.
///
/// Matched to `STREAM_BATCH_SIZE` so one line covers one batch: at 25 a
/// 1,000-message fetch wrote 40 near-identical lines, and the panel became
/// something to scroll past rather than read.
const PROGRESS_LOG_INTERVAL: usize = 100;

/// How many streamed messages to carry in one `"messages-batch"` event.
///
/// Every event is a separate serialization and a separate dispatch into the
/// webview, and this used to send one message per event — 1,000 of them for a
/// 1,000-message fetch, to feed a frontend that already coalesces arrivals
/// into at most ten renders a second. Batching is pure saving: the rows
/// arrive in the same order, in the same buffer, just in far fewer hops.
///
/// A bigger batch costs no latency: `STREAM_BATCH_INTERVAL` below sends a
/// partly-filled one anyway, so 100 is a ceiling on how many messages *may*
/// share an event, never a threshold rows have to reach before they appear.
const STREAM_BATCH_SIZE: usize = 100;

/// How long a partly-filled batch may wait for the messages that would fill
/// it before being sent anyway.
///
/// Without this, a slow topic (or the tail of a fetch) would hold rows back
/// until 64 of them existed — which on a fetch that returns 20 messages means
/// they never stream at all and the grid stays empty until the fetch ends.
/// Matched to the frontend's own 100ms flush window, so it adds no delay the
/// user could perceive that was not there already.
const STREAM_BATCH_INTERVAL: Duration = Duration::from_millis(100);

/// Backs the topic Data tab's Fetch button, and its per-row "Fetch payload".
///
/// `stream_updates` picks between the two, and decides how the messages come
/// back:
///
/// * **`true`** (the Data tab's Fetch): messages are streamed to the frontend
///   in `"messages-batch"` events as they are polled, and the returned
///   `MessageFetchResult` carries only the messages the stream did *not*
///   deliver — normally none. Every message used to be sent twice, once
///   streamed and once again in the result, so a fetch moved twice the base64
///   it needed to and the webview briefly held both copies. The frontend
///   keeps what it streamed instead.
/// * **`false`** (the per-row payload fetch): nothing is streamed — that
///   caller wants one specific message back, and its events were emitted only
///   to be discarded by a listener that filters on a request id it
///   deliberately does not match — and the result carries the message.
///
/// Either way `MessageFetchResult::total_matching` lets the Data tab show
/// "42 loaded of 150 matching" so the user can tell whether more messages
/// remain beyond what this fetch actually pulled.
///
/// `max_total_payload_bytes` is General settings > Messages > Max Total Fetch
/// Size. It is the only cap here measured in bytes — every other one counts
/// messages, which on a topic of multi-megabyte records says nothing about
/// what a fetch costs. Paired with the filter's `max_payload_preview_bytes`
/// (the Data tab sends the few KB its grid can actually display), it is what
/// keeps a large fetch from moving gigabytes of base64 into the webview and
/// killing it.
///
/// It charges only for payloads the fetch keeps, so a metadata-only browse
/// (`include_payload` off) is never stopped by it. The Data tab spends the
/// same budget across its own per-row "Fetch payload" clicks, which retain
/// bytes into the same cached rows this call's result does — see
/// `payloadBytesByTab` in `useTabDataStore`.
#[tauri::command]
#[allow(clippy::too_many_arguments)]
pub async fn connection_fetch_messages(
    app: AppHandle,
    state: State<'_, AppState>,
    id: String,
    topic: String,
    filter: salty_core::MessageFilter,
    request_id: String,
    read_timeout_ms: u64,
    max_message_size_bytes: u32,
    max_total_payload_bytes: Option<u64>,
    stream_updates: bool,
) -> Result<salty_core::MessageFetchResult, CommandError> {
    // Registered before anything that can await, so the window in which a
    // Stop click has nothing to cancel is as small as the IPC hop that
    // carries it. `connection_for_request` reads SQLite, and doing that
    // first meant a Stop pressed early in a fetch arrived while this id was
    // still unknown. `FetchCancellations::cancel` now records such a cancel
    // regardless, so this ordering is belt to that braces rather than the
    // whole fix.
    // Registered against this connection, so disconnecting the cluster can
    // stop it — see `FetchCancellations::cancel_all_for_connection`.
    // Timed from here — the whole fetch as the user experiences it, including
    // the connection lookup, the client build and the handshake, not just the
    // poll loop. `Instant` is `Copy`, so the progress closure below gets its
    // own copy of the same start.
    let started = std::time::Instant::now();
    let cancelled = state
        .fetch_cancellations
        .begin_for_connection(&request_id, &id);
    let connection = match connection_for_request(&state, &id).await {
        Ok(connection) => connection,
        Err(err) => {
            // Nothing else will call `finish` for this id now.
            state.fetch_cancellations.finish(&request_id);
            return Err(err);
        }
    };
    crate::logging::emit_log(
        &app,
        "info",
        format!("Fetching messages for topic \"{topic}\"..."),
    );
    // Only when the caller is streaming. Handing the fetch a sender it does
    // not need would have it clone every message into a channel nobody reads.
    //
    // The batching itself lives in `salty_core::forward_in_batches`,
    // which is where it can be tested — this crate needs a desktop toolchain
    // and a running Tauri app to exercise at all. What stays here is the part
    // that is genuinely Tauri's: turning a batch into an event, and a running
    // total into a Logs panel line.
    let (sender, forward_task) = if stream_updates {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<salty_core::TopicMessage>();
        let cancelled = std::sync::Arc::clone(&cancelled);
        let emit_app = app.clone();
        let emit_request_id = request_id.clone();
        let progress_app = app.clone();
        let progress_topic = topic.clone();
        // Returns how many messages it actually emitted, which is what lets
        // the result below carry the remainder and nothing more.
        let task = tokio::spawn(async move {
            salty_core::forward_in_batches(
                rx,
                cancelled,
                STREAM_BATCH_SIZE,
                STREAM_BATCH_INTERVAL,
                PROGRESS_LOG_INTERVAL,
                move |messages| {
                    let _ = emit_app.emit(
                        "messages-batch",
                        MessagesBatchEvent {
                            request_id: emit_request_id.clone(),
                            messages,
                        },
                    );
                },
                move |seen| {
                    crate::logging::emit_log(
                        &progress_app,
                        "info",
                        format!(
                            "Fetched {seen} messages for topic \"{progress_topic}\" so far in {} ms...",
                            started.elapsed().as_millis()
                        ),
                    );
                },
            )
            .await
        });
        (Some(tx), Some(task))
    } else {
        (None, None)
    };

    let was_cancelled = std::sync::Arc::clone(&cancelled);
    let result = state
        .kafka
        .fetch_messages(
            &connection,
            &topic,
            &filter,
            sender,
            Duration::from_millis(read_timeout_ms),
            max_message_size_bytes,
            max_total_payload_bytes,
            cancelled,
        )
        .await;
    // A panicked forwarding task counts as having emitted nothing, so the
    // result below carries every message rather than assuming they arrived.
    let streamed = match forward_task {
        Some(task) => task.await.unwrap_or(0),
        None => 0,
    };
    state.fetch_cancellations.finish(&request_id);
    let was_cancelled = was_cancelled.load(std::sync::atomic::Ordering::Relaxed);
    record_auth_outcome(&state, &id, &result);

    match result {
        Ok(mut fetch_result) => {
            // How many the fetch actually produced — read before the streamed
            // ones are dropped below, because it is what the log lines and
            // the user's "fetched N" mean.
            let fetched = fetch_result.messages.len();
            // The stream already delivered the first `streamed` of them, in
            // this exact order (the fetch loop sends each message and then
            // pushes the same one onto this vector). Sending them a second
            // time in the response doubled the base64 crossing the IPC
            // boundary and made the webview hold both copies at once. What is
            // left is the tail the stream did not carry: nothing at all in the
            // ordinary case, and exactly the missing messages if the
            // forwarding task ended early.
            fetch_result.messages.drain(..streamed.min(fetched));
            // Says so explicitly when the user stopped it. Without this the
            // last thing in the panel was an ordinary "Fetched N of M" line,
            // which reads as the fetch having run to completion regardless of
            // the Stop that ended it.
            crate::logging::emit_log(
                &app,
                "info",
                if was_cancelled {
                    format!(
                        "Stopped fetching messages for topic \"{topic}\" after {fetched} message(s) in {} ms",
                        started.elapsed().as_millis()
                    )
                } else {
                    format!(
                        "Fetched {fetched} of {} matching messages for topic \"{topic}\" in {} ms",
                        fetch_result.total_matching,
                        started.elapsed().as_millis()
                    )
                },
            );
            if fetch_result.stopped_at_byte_budget {
                crate::logging::emit_log(
                    &app,
                    "warn",
                    format!(
                        "Fetch for topic \"{topic}\" stopped after loading {} MB of payloads — raise \
                         General settings > Messages > Max Total Fetch Size, or narrow the filter, \
                         to pull more.",
                        fetch_result.payload_bytes_read / (1024 * 1024)
                    ),
                );
            }
            if let Some(poll_error) = &fetch_result.poll_error {
                crate::logging::emit_log(
                    &app,
                    "error",
                    format!(
                        "Fetch for topic \"{topic}\" hit a poll error before finishing: {poll_error}"
                    ),
                );
            }
            Ok(fetch_result)
        }
        Err(err) => {
            let command_err: CommandError = err.into();
            crate::logging::emit_log(
                &app,
                "error",
                format!(
                    "Failed to fetch messages for topic \"{topic}\" after {} ms: {}",
                    started.elapsed().as_millis(),
                    command_err.message
                ),
            );
            Err(command_err)
        }
    }
}

/// Backs the Data tab's Stop button, and the Data tab switching to a
/// different topic/partition/connection while a fetch is still in flight —
/// see the frontend's `stopActiveFetch`. Interrupts `connection_fetch_messages`'s
/// poll loop at its next ~500ms check instead of only discarding the result
/// once the fetch eventually finishes on its own, which otherwise keeps
/// dialling the broker for data nothing is waiting for.
///
/// A no-op, not an error, when `request_id` names a fetch that already
/// finished or was never started — Stop racing the fetch's own completion
/// is the ordinary case.
#[tauri::command]
pub async fn connection_cancel_fetch(
    state: State<'_, AppState>,
    request_id: String,
) -> Result<(), CommandError> {
    state.fetch_cancellations.cancel(&request_id);
    Ok(())
}

/// Backs the topic detail panel's Partitions tab.
#[tauri::command]
pub async fn connection_list_partitions(
    app: AppHandle,
    state: State<'_, AppState>,
    id: String,
    topic: String,
    read_timeout_ms: u64,
) -> Result<Vec<salty_core::PartitionSummary>, CommandError> {
    let connection = connection_for_request(&state, &id).await?;
    let started = std::time::Instant::now();
    let result = state
        .kafka
        .list_partitions(&connection, &topic, Duration::from_millis(read_timeout_ms))
        .await;
    record_auth_outcome(&state, &id, &result);
    log_broker_call(
        &app,
        "Listing partitions",
        started,
        if result.is_ok() { "finished" } else { "failed" },
    );
    Ok(result?)
}

/// Backs the topic Metrics tab's partition-skew chart when a From/To window is set.
#[tauri::command]
pub async fn connection_count_partition_messages(
    app: AppHandle,
    state: State<'_, AppState>,
    id: String,
    topic: String,
    from_timestamp_ms: Option<i64>,
    to_timestamp_ms: Option<i64>,
    read_timeout_ms: u64,
) -> Result<Vec<salty_core::PartitionMessageCount>, CommandError> {
    let connection = connection_for_request(&state, &id).await?;
    let started = std::time::Instant::now();
    let result = state
        .kafka
        .count_partition_messages(
            &connection,
            &topic,
            from_timestamp_ms,
            to_timestamp_ms,
            Duration::from_millis(read_timeout_ms),
        )
        .await;
    record_auth_outcome(&state, &id, &result);
    log_broker_call(
        &app,
        "Counting partition messages",
        started,
        if result.is_ok() { "finished" } else { "failed" },
    );
    Ok(result?)
}

/// Backs the topic detail panel's Config tab.
#[tauri::command]
pub async fn connection_describe_topic_config(
    app: AppHandle,
    state: State<'_, AppState>,
    id: String,
    topic: String,
    read_timeout_ms: u64,
) -> Result<Vec<salty_core::ConfigEntry>, CommandError> {
    let connection = connection_for_request(&state, &id).await?;
    let started = std::time::Instant::now();
    let result = state
        .kafka
        .describe_topic_config(&connection, &topic, Duration::from_millis(read_timeout_ms))
        .await;
    record_auth_outcome(&state, &id, &result);
    log_broker_call(
        &app,
        "Describing topic config",
        started,
        if result.is_ok() { "finished" } else { "failed" },
    );
    Ok(result?)
}

/// Backs the consumer group detail panel's "Refresh" button.
#[tauri::command]
pub async fn connection_fetch_consumer_group_lag(
    app: AppHandle,
    state: State<'_, AppState>,
    id: String,
    group_id: String,
    read_timeout_ms: u64,
) -> Result<salty_core::ConsumerGroupLag, CommandError> {
    let connection = connection_for_request(&state, &id).await?;
    let started = std::time::Instant::now();
    let result = state
        .kafka
        .fetch_consumer_group_lag(
            &connection,
            &group_id,
            Duration::from_millis(read_timeout_ms),
        )
        .await;
    record_auth_success_only(&state, &id, &result);
    log_broker_call(
        &app,
        "Fetching consumer group lag",
        started,
        if result.is_ok() { "finished" } else { "failed" },
    );
    Ok(result?)
}
