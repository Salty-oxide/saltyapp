use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use error_stack::ResultExt;
use salty_core::Result;
use salty_core::{
    cluster_version_report, AclAvailability, AclFilter, AclListing, AppError, BrokerSummary, ClusterVersionReport,
    ConfigEntry, Connection, ConnectionStatus, ConsumerGroupLag, ConsumerGroupSummary, EncodedRecord,
    MessageFetchResult, MessageFilter, MessageHeader, PartitionLag, PartitionSummary, PublishOutcome, SaslMechanism,
    SecurityProtocol, TopicMessage, TopicSummary, INTER_BROKER_PROTOCOL_VERSION_CONFIG, PROCESS_ROLES_CONFIG,
};
use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
use rdkafka::client::{ClientContext, DefaultClientContext};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::groups::{GroupInfo, GroupMemberInfo};
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::{ClientConfig, Message};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::assignment::decode_consumer_protocol_assignment;
use crate::config::{build_client_config, client_config, fetch_consumer_config, BrokerSslConfig};
use crate::messages::{
    budgeted_payload_bytes, byte_budget_reached, clamp_offset, combined_start_offset, distribute_total_budget,
    effective_max_messages_per_partition, fetch_shard_count, newest_first_start_offset, partition_limits,
    payload_preview_slice,
};

const TCP_PING_TIMEOUT: Duration = Duration::from_secs(3);

/// How long the first bootstrap server gets to answer a reachability probe
/// before the others are tried as well — see `ping_bootstrap`.
///
/// Short enough that a dead first broker barely delays the answer, long
/// enough that a live one is always home first: a broker on the same machine
/// answers in well under a millisecond, and one across a 50ms link in a tenth
/// of this.
const PING_FANOUT_DELAY: Duration = Duration::from_millis(250);

/// How many partitions' watermarks to ask for at once.
///
/// `fetch_watermarks` blocks its calling thread for a full broker round trip,
/// so querying partitions one at a time costs one round trip *each*, end to
/// end. That is invisible against a broker on localhost and brutal against a
/// real one: measured through a 50ms link, reading the watermarks of a
/// 100-partition topic took 6.9 seconds before a single message was fetched —
/// and the Data tab, the Partitions tab, the topic message count and consumer
/// group lag all pay it.
///
/// librdkafka's client is thread-safe and does its I/O on its own threads, so
/// these calls overlap happily; the bound keeps a wide topic from opening a
/// hundred threads to wait on a hundred replies.
const WATERMARK_LOOKUP_CONCURRENCY: usize = 16;

/// Low and high watermarks for every given partition, queried concurrently.
///
/// Fails as a whole if any partition's lookup fails: every caller needs all of
/// them, and a partial answer would silently under-report a topic's contents.
fn watermarks_for_partitions<C>(
    consumer: &BaseConsumer<C>,
    topic: &str,
    partitions: &[i32],
    read_timeout: Duration,
) -> Result<BTreeMap<i32, (i64, i64)>, AppError>
where
    // Generic over the consumer's context so this works for both the plain
    // consumers and the error-capturing ones the pooled clients use. The
    // bound is what makes `&BaseConsumer<C>` shareable across the scoped
    // threads below.
    C: ConsumerContext + 'static,
{
    if partitions.is_empty() {
        return Ok(BTreeMap::new());
    }

    let per_thread = partitions.len().div_ceil(WATERMARK_LOOKUP_CONCURRENCY).max(1);

    std::thread::scope(|scope| {
        let handles: Vec<_> = partitions
            .chunks(per_thread)
            .map(|chunk| {
                scope.spawn(move || {
                    chunk
                        .iter()
                        .map(|&partition| {
                            consumer
                                .fetch_watermarks(topic, partition, read_timeout)
                                .map(|watermarks| (partition, watermarks))
                                // The reason is captured as a string here
                                // rather than wrapped: a `KafkaError` cannot
                                // cross the scope boundary as an
                                // `error_stack` context without losing the
                                // partition it belongs to.
                                .map_err(|err| (partition, err.to_string()))
                        })
                        .collect::<std::result::Result<Vec<_>, (i32, String)>>()
                })
            })
            .collect();

        let mut watermarks = BTreeMap::new();
        for handle in handles {
            match handle.join().expect("watermark lookup thread panicked") {
                Ok(found) => watermarks.extend(found),
                Err((partition, reason)) => {
                    return Err(error_stack::Report::new(AppError::Kafka).attach(format!(
                        "failed to fetch watermarks for {topic}:{partition}: {reason}"
                    )))
                }
            }
        }
        Ok(watermarks)
    })
}

/// Fallback broker read timeout used only by this crate's own unit tests
/// (which call `KafkaClient` methods directly, bypassing the Tauri command
/// layer). Real callers always pass the user's configured General settings >
/// Brokers > Read Timeout value — see `frontend/src/features/settings/useGeneralSettingsStore.ts`
/// and its default, which this mirrors.
#[cfg(test)]
const TEST_READ_TIMEOUT: Duration = Duration::from_millis(10_000);

/// Fallback max message size used only by this crate's own unit tests — see
/// `TEST_READ_TIMEOUT`'s doc comment.
#[cfg(test)]
const TEST_MAX_MESSAGE_SIZE_BYTES: u32 = 1_048_576;

#[async_trait]
pub trait KafkaClient: Send + Sync {
    /// Checks a saved connection (used for the periodic, every-10s status
    /// dot poll in the connection tree — one call per saved connection, for
    /// as long as the app runs). Deliberately a plain TCP reachability
    /// check (see `ping_bootstrap`'s doc comment for the "why" behind that
    /// trade-off), NOT a real librdkafka client probe: creating and
    /// destroying a native Kafka client on this timer, forever, for every
    /// saved connection regardless of whether any of them are actually
    /// "Connected", was a real source of continuous native-resource churn —
    /// harmless in isolated bursts, but compounding into a slow, steady
    /// memory climb over a long-running session (worse on Windows, where
    /// librdkafka's client teardown is reportedly less prompt than on
    /// Unix). The deeper, protocol-level check (real SASL/SSL handshake,
    /// actual errors surfaced) still happens at Connect time via
    /// `test_connection`/the modal's "Test" button — this poll only ever
    /// needs to answer "is the network still there".
    async fn check_status(&self, connection: &Connection) -> Result<ConnectionStatus, AppError>;

    /// Plain TCP reachability check against each host:port in
    /// `bootstrap_servers` (comma-separated) — reports `Reachable` as soon
    /// as any one accepts a connection. Deliberately does not speak the
    /// Kafka wire protocol or apply any security settings: a broker that
    /// only accepts TLS/SASL (as virtually all managed cloud Kafka does)
    /// still has an open TCP port, and "is something listening" is the
    /// question this answers. Backs the ping button next to "Bootstrap
    /// servers" in the New Connection modal's General section.
    async fn ping_bootstrap(&self, bootstrap_servers: &str) -> Result<ConnectionStatus, AppError>;

    /// Tests full connectivity using the in-progress modal's entered
    /// values, before the connection has been saved. Backs the modal's
    /// bottom "Test" button. PLAIN/SCRAM mechanisms with no username/
    /// password entered will surface as an `Err` here rather than
    /// `ConnectionStatus::Unreachable` — this is a real config error, not a
    /// failed probe.
    async fn test_connection(
        &self,
        bootstrap_servers: &str,
        security_protocol: SecurityProtocol,
        sasl_mechanism: Option<SaslMechanism>,
        sasl_username: Option<&str>,
        password: Option<&str>,
        ssl: BrokerSslConfig<'_>,
    ) -> Result<ConnectionStatus, AppError>;

    /// Authenticates a saved connection and keeps the resulting client for
    /// every request that follows. Backs the cluster panel's Connect /
    /// Reconnect.
    ///
    /// Unlike `test_connection` (which probes values the user has typed but
    /// not saved, and throws the client away), this leaves a live,
    /// authenticated connection behind — so listing topics immediately
    /// afterwards is one request on an open socket rather than another full
    /// handshake.
    async fn connect(&self, connection: &Connection) -> Result<ConnectionStatus, AppError>;

    /// Closes and forgets this connection's pooled client. Called when the
    /// user disconnects, edits or deletes a connection, and when the broker
    /// rejects its credentials — anything that makes the pooled connection
    /// wrong or unwanted.
    fn release(&self, connection_id: &str);

    /// Backs the tree's "Brokers" sub-list once a cluster is connected.
    /// `read_timeout` is the user's configured General settings > Brokers >
    /// Read Timeout value.
    async fn list_brokers(&self, connection: &Connection, read_timeout: Duration) -> Result<Vec<BrokerSummary>, AppError>;

    /// Backs the tree's "Topics" sub-list once a cluster is connected.
    async fn list_topics(&self, connection: &Connection, read_timeout: Duration) -> Result<Vec<TopicSummary>, AppError>;

    /// Backs the tree's "Consumers" sub-list once a cluster is connected.
    async fn list_consumer_groups(
        &self,
        connection: &Connection,
        read_timeout: Duration,
    ) -> Result<Vec<ConsumerGroupSummary>, AppError>;

    /// Sums (high watermark - low watermark) across every partition of the
    /// topic. Backs the topic detail panel's Properties > Messages section,
    /// which fetches this lazily only when its Refresh button is clicked —
    /// never on tab open, since this can be an expensive per-partition call
    /// on a topic with many partitions.
    async fn count_topic_messages(&self, connection: &Connection, topic: &str, read_timeout: Duration) -> Result<u64, AppError>;

    /// Backs the topic Data tab's Fetch button. Pulls message metadata (plus
    /// base64 payload — decoded/rendered client-side when a row is
    /// clicked) applying the given filters; an all-`None` filter pulls
    /// everything. Bounded/historical, not a live tail: partition
    /// start/end offsets are resolved once up front (from watermarks, or
    /// from the from/to timestamps via `offsets_for_times`), so messages
    /// produced after the fetch starts are not included.
    ///
    /// When `on_message` is given, each message is also sent on it as soon
    /// as it's polled, in addition to being collected into the returned
    /// result — this lets a caller (the Tauri command layer) stream results
    /// to the UI incrementally instead of waiting for the whole fetch to
    /// finish. The channel is purely a progress feed: its receiver going
    /// away (e.g. the caller stopped listening) does not affect the fetch,
    /// which still runs to completion and returns the full result.
    ///
    /// `max_message_size_bytes` is the user's configured General settings >
    /// Messages > Max Message Size value, applied as librdkafka's
    /// `max.partition.fetch.bytes`.
    ///
    /// The returned `MessageFetchResult::total_matching` is how many
    /// messages satisfy `filter`'s partition/offset/timestamp constraints in
    /// total, uncapped by `max_messages_per_partition`/`max_total_messages`
    /// — lets the frontend tell the user whether more remain beyond what was
    /// actually pulled.
    ///
    /// `cancelled` is checked once per poll slice (~500ms) so a Stop click,
    /// or a topic switch that implies one, can interrupt the fetch instead
    /// of only hiding its result once it eventually finishes — see
    /// `salty_core::FetchCancellations`.
    ///
    /// `max_total_payload_bytes` bounds the fetch by size rather than by
    /// message count — see `byte_budget_reached`. `None` leaves it unbounded.
    /// It counts only payloads the fetch keeps, so a `filter` with
    /// `include_payload` off is never bounded by it.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_messages(
        &self,
        connection: &Connection,
        topic: &str,
        filter: &MessageFilter,
        on_message: Option<mpsc::UnboundedSender<TopicMessage>>,
        read_timeout: Duration,
        max_message_size_bytes: u32,
        max_total_payload_bytes: Option<u64>,
        cancelled: Arc<AtomicBool>,
    ) -> Result<MessageFetchResult, AppError>;

    /// Backs the topic detail panel's Partitions tab: id, leader, replicas,
    /// ISR, and low/high offsets for every partition.
    async fn list_partitions(
        &self,
        connection: &Connection,
        topic: &str,
        read_timeout: Duration,
    ) -> Result<Vec<PartitionSummary>, AppError>;

    /// Backs the topic detail panel's Config tab, via librdkafka's
    /// DescribeConfigs admin API.
    async fn describe_topic_config(
        &self,
        connection: &Connection,
        topic: &str,
        read_timeout: Duration,
    ) -> Result<Vec<ConfigEntry>, AppError>;

    /// Backs the New Connection modal's Detect button: asks the cluster
    /// which metadata mode it runs in, and which version it reports.
    ///
    /// Raw values rather than a saved `Connection`, exactly like
    /// `test_connection` — this runs on what the user has typed and has not
    /// saved yet, so there is no connection id to pool a client under, and
    /// both clients it builds are dropped when it returns.
    ///
    /// An authorization refusal comes back as `Ok` with
    /// `MetadataMode::Unknown` and an explanatory note, **not** as `Err`:
    /// "this principal may not read broker configs" is a fact about the
    /// cluster, the same way an empty `AclListing` is. Only a transport
    /// failure — an unreachable broker, a timeout — is an error.
    ///
    /// Note this is the *opposite* of `authorizer_class`'s policy of
    /// collapsing every failure to `None`, and deliberately so: that one
    /// silently qualifies another request's result and must never turn a
    /// good answer into a bad one, whereas this *is* the user's answer and
    /// has to explain itself.
    #[allow(clippy::too_many_arguments)]
    async fn detect_cluster_version(
        &self,
        bootstrap_servers: &str,
        security_protocol: SecurityProtocol,
        sasl_mechanism: Option<SaslMechanism>,
        sasl_username: Option<&str>,
        password: Option<&str>,
        ssl: BrokerSslConfig<'_>,
        read_timeout: Duration,
    ) -> Result<ClusterVersionReport, AppError>;

    /// Backs the tree's Access Control category and the Access tabs, via
    /// librdkafka's DescribeAcls admin API — reached over raw FFI, because
    /// rdkafka's safe wrapper binds no ACL call at all (see `crate::acl`).
    ///
    /// Returns an `AclListing` rather than a bare `Vec` because two of the
    /// broker's answers are facts about the cluster rather than failures:
    /// no authorizer is configured, or this principal may not read ACLs.
    /// Only a genuine failure — a timeout, a dead socket — comes back as
    /// `Err`.
    async fn describe_acls(
        &self,
        connection: &Connection,
        filter: AclFilter,
        read_timeout: Duration,
    ) -> Result<AclListing, AppError>;

    /// Backs the consumer group detail panel's "Refresh" button. Decodes
    /// each member's partition assignment (see `crate::assignment`), then
    /// fetches committed offsets (via a throwaway consumer scoped to this
    /// group id — never subscribed/polled, so it cannot join the group or
    /// disturb the real consumers' rebalance) and log-end offsets for
    /// exactly those partitions.
    async fn fetch_consumer_group_lag(
        &self,
        connection: &Connection,
        group_id: &str,
        read_timeout: Duration,
    ) -> Result<ConsumerGroupLag, AppError>;

    /// Backs the partition detail panel's Publish tab: writes `records` to one
    /// partition, in order, and reports what happened to each.
    ///
    /// The only write in this trait, and the only one whose caller must have
    /// cleared a gate first — see `salty_core::publish_refusal`, which the
    /// command layer applies before calling this. `records` arrive already
    /// encoded and size-checked by `salty_core::encode_messages`, so this
    /// method neither parses nor validates user input: whatever reaches here is
    /// bytes that have already been agreed to.
    ///
    /// A broker's refusal to authorize the write is reported as
    /// `AppError::Authorization` inside the returned `PublishOutcome`'s
    /// `failure`, not as an `Err` — the outcome has to survive a partial
    /// publish, and "which of my messages landed" is unanswerable otherwise.
    /// See `crate::producer::publish_messages`.
    #[allow(clippy::too_many_arguments)]
    async fn publish_messages(
        &self,
        connection: &Connection,
        topic: &str,
        partition: i32,
        records: &[EncodedRecord],
        max_message_size_bytes: u32,
        write_timeout: Duration,
    ) -> Result<PublishOutcome, AppError>;
}

/// The members of a consumer group, safely.
///
/// librdkafka leaves a group's `members` array NULL when it has none, and
/// rdkafka's `GroupInfo::members()` hands that straight to
/// `slice::from_raw_parts` without checking it — which is undefined
/// behaviour, however zero the length is. A release build gets away with it
/// (nothing is ever read through the pointer), but any build with debug
/// assertions on aborts the process outright on the precondition check, with
/// no unwind and no chance to report anything to the user.
///
/// A group in `Empty` or `Dead` has no members by definition, so there is
/// nothing to ask rdkafka for; in every other state librdkafka has allocated
/// the array and the pointer is real. This matters well beyond the tests: an
/// idle group — one with committed offsets and no consumer currently running
/// — is `Empty`, which is the ordinary resting state of most groups the
/// Consumers panel lists.
fn group_members(group: &GroupInfo) -> &[GroupMemberInfo] {
    match group.state() {
        "Empty" | "Dead" => &[],
        _ => group.members(),
    }
}

/// Collects a message's Kafka headers. Values are base64-encoded, not
/// lossy-UTF-8-decoded — a header value is an arbitrary Kafka byte string,
/// not guaranteed text, and lossy decoding would silently corrupt a binary
/// one.
fn extract_headers(message: &BorrowedMessage) -> Vec<MessageHeader> {
    let Some(headers) = message.headers() else {
        return Vec::new();
    };
    // `HeadersIter` is a plain `Iterator`, not an `ExactSizeIterator`, so
    // `collect` cannot size the vector up front and grows it by reallocating
    // and copying. `Headers::count` is the exact length, and this runs once
    // per message in the fetch loop, so the reservation is worth making
    // explicitly.
    let mut extracted = Vec::with_capacity(headers.count());
    extracted.extend(headers.iter().map(|header| MessageHeader {
        key: header.key.to_string(),
        value_base64: header.value.map(|v| BASE64.encode(v)),
    }));
    extracted
}

/// Captures librdkafka's own detailed failure reason (e.g. "SSL connection
/// closed by peer", "SASL authentication failed") via the `error` callback.
/// `fetch_metadata`'s return value alone can't distinguish these cases — a
/// closed port, a TLS failure, and a bad SASL password all surface as the
/// same generic `BrokerTransportFailure` code — so without this, every
/// failure reason gets collapsed into an identical, unhelpful message.
///
/// Attached to *every* client this module creates, not just the connection
/// probe: the reason string is also what tells an authentication rejection
/// apart from a transport blip (see [`crate::auth::is_auth_failure`]), and
/// that verdict decides whether the connection's circuit breaker trips. A
/// client created without this context can only ever report the generic
/// code, so a password rotated mid-session would look like a network
/// problem and be retried like one.
#[derive(Clone, Default)]
struct ClientErrorContext {
    last_error: Arc<Mutex<Option<String>>>,
}

impl ClientErrorContext {
    /// librdkafka's reason for the most recent failure on this client, if it
    /// reported one through the `error` callback.
    fn last_error(&self) -> Option<String> {
        self.last_error.lock().ok().and_then(|guard| guard.clone())
    }

    fn clear(&self) {
        if let Ok(mut last_error) = self.last_error.lock() {
            *last_error = None;
        }
    }
}

impl ClientContext for ClientErrorContext {
    fn error(&self, _error: rdkafka::error::KafkaError, reason: &str) {
        if let Ok(mut last_error) = self.last_error.lock() {
            *last_error = Some(reason.to_string());
        }
    }
}

impl ConsumerContext for ClientErrorContext {}

/// Every consumer in this module: a `BaseConsumer` that reports librdkafka's
/// failure reasons through [`ClientErrorContext`].
type ObservedConsumer = BaseConsumer<ClientErrorContext>;

/// How long a failed request will spend serving the client's event queue to
/// find out *why* it failed. Only ever spent on the error path.
const ERROR_DRAIN_BUDGET: Duration = Duration::from_millis(250);

/// A consumer together with the context watching it fail.
///
/// Cloneable and cheap (two `Arc`s), so one client can be shared by every
/// request against a connection — see [`RdKafkaClient::metadata_client`].
#[derive(Clone)]
struct ObservedClient {
    consumer: Arc<ObservedConsumer>,
    context: ClientErrorContext,
    /// Serializes ownership of the shared error slot — see [`Self::observed`].
    ///
    /// The tree fires brokers, topics and consumer groups concurrently the
    /// moment a cluster connects, and all three share one pooled client:
    /// one `ClientErrorContext` and one underlying librdkafka consumer.
    /// Without this, their blocking tasks call `begin()`/`poll()` on that
    /// shared state from different threads at the same time. One request's
    /// `begin()` can wipe the reason another just captured, or a slower
    /// request's `drain_error_events()` can read a reason a *different*
    /// concurrent request's failure produced — misattributing, say, a
    /// consumer-group ACL rejection as the reason brokers/topics failed.
    ///
    /// It also keeps `poll()` single-threaded per client, which is what
    /// serves librdkafka's main event queue.
    ///
    /// **Scope is deliberately the `begin()`..`failure()` window, not the
    /// whole request.** This used to be held for the entire request, on the
    /// stated assumption that "every pooled request here is one broker round
    /// trip". That was true of `list_brokers`/`list_topics`/
    /// `list_consumer_groups` and false of the three that matter most:
    /// `count_topic_messages`, `list_partitions` and
    /// `fetch_consumer_group_lag` all walk every partition's watermarks,
    /// which is seconds of work on a wide topic. Holding this across that
    /// froze every other request on the connection for the duration — so
    /// clicking a second topic during a consumer-group lag refresh looked
    /// like the app had hung, and the freeze scaled with the size of someone
    /// else's consumer group.
    ///
    /// The watermark walk needs no exclusive access: it reports failures
    /// per-partition as plain strings and never reads or writes the error
    /// slot (pinned by
    /// `the_watermark_walk_does_not_touch_the_shared_error_slot`), and
    /// `rd_kafka_query_watermark_offsets` uses its own reply queue rather
    /// than the main one `poll()` serves. So it runs outside this lock.
    ///
    /// A `std::sync::Mutex` rather than an async one because every holder is
    /// already inside `spawn_blocking`: the guard has to be taken and
    /// released *within* the blocking closure to be this narrow, which an
    /// async lock cannot do.
    error_slot: Arc<Mutex<()>>,
}

impl ObservedClient {
    fn create(config: &ClientConfig) -> Result<Self, AppError> {
        let context = ClientErrorContext::default();
        match config.create_with_context::<_, ObservedConsumer>(context.clone()) {
            Ok(consumer) => Ok(ObservedClient {
                consumer: Arc::new(consumer),
                context,
                error_slot: Arc::new(Mutex::new(())),
            }),
            Err(err) => {
                // No client exists to drain, so the only reason available is
                // the creation error itself — which does carry librdkafka's
                // text (e.g. "Invalid sasl.username: not set").
                let reason = err.to_string();
                Err(failure_report(&err, &reason, "failed to create kafka consumer"))
            }
        }
    }

    /// Runs one librdkafka call with exclusive ownership of the shared error
    /// slot, so a failure is attributed to the request that caused it.
    ///
    /// This is the only correct way to make a call on a *pooled* client that
    /// needs librdkafka's reason for failing. It bundles the three steps that
    /// have to happen together — clear the slot, make the call, read the slot
    /// back on failure — under one guard, so they cannot be interleaved with
    /// another request's. Calling `begin()`/`failure()` by hand around a
    /// pooled client is the bug this replaces.
    ///
    /// Deliberately narrow: see [`Self::error_slot`] for why anything slow
    /// (the watermark walks) belongs *outside* this.
    fn observed<T>(
        &self,
        what: &str,
        call: impl FnOnce(&ObservedConsumer) -> std::result::Result<T, KafkaError>,
    ) -> Result<T, AppError> {
        // Poisoning only means some other request panicked mid-call; the slot
        // holds one replaceable string, and `begin()` overwrites it anyway.
        let _guard = self.error_slot.lock().unwrap_or_else(|err| err.into_inner());
        self.begin();
        call(&self.consumer).map_err(|err| self.failure(&err, what))
    }

    /// Forgets any reason left over from an earlier request on this client.
    ///
    /// A pooled client outlives the request that created it, so without this
    /// a stale reason from a previous failure could be reported — and
    /// classified — as the current one's cause.
    ///
    /// Callers on a pooled client must hold [`Self::error_slot`] — use
    /// [`Self::observed`], which does.
    fn begin(&self) {
        self.context.clear();
    }

    /// Serves the client's event queue until it yields librdkafka's reason
    /// for a failure, or the budget runs out.
    ///
    /// librdkafka reports failures as *events*, and rdkafka only turns an
    /// event into a `ClientContext::error` call while the client's queue is
    /// being served — which nothing but `poll` does (rdkafka 0.36 sets no
    /// classic `error_cb` at all; see `Client::poll_event`). A client that
    /// only calls `fetch_metadata` therefore never learns why anything
    /// failed: its context stays empty and every failure collapses into the
    /// same generic "Broker transport failure". Polling here is what makes
    /// the reason — and with it the difference between a rejected password
    /// and an unreachable broker — observable at all.
    ///
    /// Safe on a consumer with no `group.id` and no assignment: rdkafka
    /// leaves such a client's main queue in place precisely so it can be
    /// used for metadata and watermarks.
    fn drain_error_events(&self) {
        let deadline = std::time::Instant::now() + ERROR_DRAIN_BUDGET;
        while self.context.last_error().is_none() && std::time::Instant::now() < deadline {
            let _ = self.consumer.poll(Duration::from_millis(10));
        }
    }

    /// Turns a librdkafka failure into a report carrying its real reason,
    /// typed as [`AppError::Authentication`] when the credentials were
    /// rejected and [`AppError::Kafka`] otherwise.
    ///
    /// The distinction is the whole point: `AppError::Authentication` is
    /// what the command layer trips the connection's circuit breaker on, so
    /// a rejected connection stops dialling the broker instead of being
    /// retried like a transient failure.
    fn failure(&self, error: &KafkaError, what: &str) -> error_stack::Report<AppError> {
        self.drain_error_events();
        let reason = self.context.last_error().unwrap_or_else(|| error.to_string());
        failure_report(error, &reason, what)
    }
}

/// Builds the report for a librdkafka failure whose reason is already known.
fn failure_report(error: &KafkaError, reason: &str, what: &str) -> error_stack::Report<AppError> {
    // `.change_context(AppError::Kafka)` alone would demote the `KafkaError`
    // to a non-`Printable` context frame that `format_report`
    // (src-tauri/src/commands/connections.rs) never surfaces to the user —
    // rendering the reason into the attachment is the only way it reaches
    // them instead of just the generic wrapper text.
    let kind = if crate::auth::is_auth_failure(Some(error), Some(reason)) {
        AppError::Authentication
    } else {
        AppError::Kafka
    };
    error_stack::Report::new(kind).attach(format!("{what}: {reason}"))
}

async fn run_probe(config: ClientConfig) -> Result<ConnectionStatus, AppError> {
    tokio::task::spawn_blocking(move || {
        let client = ObservedClient::create(&config)?;
        probe_with(&client, PROBE_TIMEOUT)
    })
    .await
    .change_context(AppError::Kafka)
    .attach("status check task panicked")?
}

/// How long a connection probe waits for the broker to answer. Covers the
/// TCP connection, the TLS and SASL handshakes, and the metadata response.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Asks the broker for metadata, which is the cheapest request that can only
/// succeed once the connection is fully established and authenticated.
/// Goes through `observed` because Connect probes a *pooled* client, which
/// the tree's brokers/topics/consumers requests may already be using: the
/// reason a probe fails is what trips the connection's authentication circuit
/// breaker, so misattributing another request's failure to it would block a
/// connection whose credentials are fine.
fn probe_with(client: &ObservedClient, timeout: Duration) -> Result<ConnectionStatus, AppError> {
    client.observed("failed to reach the cluster", |consumer| consumer.fetch_metadata(None, timeout))?;
    Ok(ConnectionStatus::Reachable)
}

/// A client kept alive for reuse, with the version of the connection it was
/// built from.
struct PooledClient {
    /// The connection's `updated_at` at the time this client was built. A
    /// client built from credentials the user has since edited must never be
    /// reused, and this is what notices — even if nothing thought to
    /// release it.
    updated_at: String,
    client: ObservedClient,
}

/// The real Kafka client, holding one authenticated connection per saved
/// cluster.
///
/// Every request used to build its own client, which meant a TCP connection
/// plus — on a secured cluster — a TLS and SASL handshake before any request
/// could be sent. Connecting alone cost four of them: the probe, then the
/// tree's brokers, topics and consumer groups. The handshake dominated;
/// the request itself was never the slow part.
///
/// Now the probe at Connect leaves its authenticated client behind, and
/// every metadata request after it reuses that connection. librdkafka keeps
/// the socket alive and reconnects on its own if the broker goes away, so
/// the pooled client survives a broker restart without the app doing
/// anything.
///
/// Message fetching deliberately does *not* use this pool: a fetch assigns
/// partitions and polls, which is per-request state, and reusing one
/// consumer across fetches measured 3x slower (see `fetch_messages`).
#[derive(Default)]
pub struct RdKafkaClient {
    metadata_clients: Mutex<HashMap<String, PooledClient>>,
    /// The Config tab's admin clients, pooled for the same reason and on the
    /// same terms as `metadata_clients`.
    ///
    /// A separate map because `AdminClient` is its own rdkafka type and
    /// cannot come out of the consumer pool — which is why this one was
    /// rebuilt per request, and why opening ten topics' Config tabs cost ten
    /// handshakes.
    admin_clients: Mutex<HashMap<String, PooledAdminClient>>,
}

/// The broker config naming the authorizer in force. Empty (not absent)
/// when no authorizer is configured, which is what makes it usable as a
/// definitive "this cluster is unsecured" signal.
const AUTHORIZER_CLASS_CONFIG: &str = "authorizer.class.name";

/// A pooled admin client, versioned by the connection it was built from —
/// see [`PooledClient`], whose contract this mirrors exactly.
struct PooledAdminClient {
    updated_at: String,
    client: Arc<AdminClient<DefaultClientContext>>,
}

impl RdKafkaClient {
    pub fn new() -> Self {
        Self::default()
    }

    /// This connection's pooled client, building and pooling one if there
    /// isn't a current one.
    ///
    /// Holds the pool lock across creation deliberately: the three requests
    /// the tree fires the moment a cluster connects would otherwise each
    /// build their own client for the same connection, which is the
    /// stampede this pool exists to prevent. This lock only covers *which*
    /// client those three requests get — see `ObservedClient::error_slot`
    /// for what keeps their failure reasons from being mixed up once they
    /// start using it concurrently.
    fn metadata_client(&self, connection: &Connection) -> Result<ObservedClient, AppError> {
        let mut pool = self.metadata_clients.lock().unwrap_or_else(|err| err.into_inner());

        if let Some(pooled) = pool.get(&connection.id)
            && pooled.updated_at == connection.updated_at
        {
            return Ok(pooled.client.clone());
        }

        let client = ObservedClient::create(&client_config(connection))?;
        pool.insert(
            connection.id.clone(),
            PooledClient {
                updated_at: connection.updated_at.clone(),
                client: client.clone(),
            },
        );
        Ok(client)
    }

    /// This connection's pooled admin client, building and pooling one if
    /// there isn't a current one.
    ///
    /// Same contract as `metadata_client`, for the same reasons: versioned by
    /// `updated_at` so an edited connection never keeps talking to the broker
    /// with the settings the user just replaced, and the pool lock is held
    /// across creation so two Config tabs opened at once share one handshake
    /// instead of racing to build their own.
    fn admin_client(&self, connection: &Connection) -> Result<Arc<AdminClient<DefaultClientContext>>, AppError> {
        let mut pool = self.admin_clients.lock().unwrap_or_else(|err| err.into_inner());

        if let Some(pooled) = pool.get(&connection.id)
            && pooled.updated_at == connection.updated_at
        {
            return Ok(Arc::clone(&pooled.client));
        }

        let client: Arc<AdminClient<DefaultClientContext>> = Arc::new(
            client_config(connection)
                .create()
                .map_err(|err| failure_report(&err, &err.to_string(), "failed to create kafka admin client"))?,
        );
        pool.insert(
            connection.id.clone(),
            PooledAdminClient {
                updated_at: connection.updated_at.clone(),
                client: Arc::clone(&client),
            },
        );
        Ok(client)
    }

    /// The broker's configured `authorizer.class.name`, or `None` if it
    /// could not be read.
    ///
    /// Used only to interpret an empty ACL listing. `DescribeConfigs` is the
    /// right question to ask because, unlike `DescribeAcls`, librdkafka
    /// propagates its failures — so a principal who may not read broker
    /// configs yields `None` here and the caller reports "cannot determine"
    /// rather than inventing an answer.
    ///
    /// Every failure collapses to `None` on purpose: this is a secondary
    /// probe that qualifies another result, and it must never turn a
    /// perfectly good (if empty) ACL listing into an error.
    async fn authorizer_class(&self, connection: &Connection, read_timeout: Duration) -> Option<String> {
        let client = self.metadata_client(connection).ok()?;
        // Any broker will do — `authorizer.class.name` is a static, per-broker
        // config and a cluster running an authorizer on only some of its
        // brokers is not a configuration this app needs to describe.
        let broker_id = tokio::task::spawn_blocking(move || {
            client
                .observed("failed to fetch broker metadata", |consumer| {
                    consumer.fetch_metadata(None, read_timeout)
                })
                .ok()
                .and_then(|metadata| metadata.brokers().first().map(|broker| broker.id()))
        })
        .await
        .ok()??;

        let admin = self.admin_client(connection).ok()?;
        let options = AdminOptions::new().request_timeout(Some(read_timeout));
        let results = admin
            .describe_configs([&ResourceSpecifier::Broker(broker_id)], &options)
            .await
            .ok()?;

        results
            .into_iter()
            .next()?
            .ok()?
            .entries
            .into_iter()
            .find(|entry| entry.name == AUTHORIZER_CLASS_CONFIG)
            .map(|entry| entry.value.unwrap_or_default())
    }

    /// Retires *both* of a connection's pooled clients.
    ///
    /// Both, always: this runs when credentials are rejected as well as when
    /// the connection is edited or deleted, and an admin client left behind
    /// would keep the Config tab dialling a broker that has already said no.
    fn drop_pooled_client(&self, connection_id: &str) {
        self.metadata_clients
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .remove(connection_id);
        self.admin_clients
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .remove(connection_id);
    }
}

/// The `host:port` part of one bootstrap entry, with any `PROTOCOL://`
/// prefix removed.
///
/// Kafka's own configuration files and `docker-compose` advertise listeners
/// as `PLAINTEXT://localhost:9092`, and librdkafka accepts that form in
/// `bootstrap.servers` — so a connection written that way connects, browses
/// and fetches perfectly well. The reachability ping, though, hands the
/// string to `TcpStream::connect`, which parses `host:port` and nothing else:
/// it read the host as `PLAINTEXT://localhost`, failed to resolve it, and
/// reported the cluster **unreachable**.
///
/// That was not a cosmetic red dot. `useUnreachableDisconnect` ends the
/// session of a connected cluster after two consecutive unreachable polls, so
/// a healthy local broker addressed this way was disconnected out from under
/// the user roughly every twenty seconds, and its dot stayed red afterwards
/// because reachability is polled whether or not anything is connected.
fn host_port(entry: &str) -> &str {
    match entry.split_once("://") {
        Some((_scheme, host_port)) => host_port,
        None => entry,
    }
}

#[async_trait]
impl KafkaClient for RdKafkaClient {
    async fn check_status(&self, connection: &Connection) -> Result<ConnectionStatus, AppError> {
        self.ping_bootstrap(&connection.bootstrap_servers).await
    }

    async fn connect(&self, connection: &Connection) -> Result<ConnectionStatus, AppError> {
        // A connection that fails to authenticate must not stay pooled: the
        // next request would reuse a client the broker has already rejected.
        let client = match self.metadata_client(connection) {
            Ok(client) => client,
            Err(err) => {
                self.drop_pooled_client(&connection.id);
                return Err(err);
            }
        };

        // `probe_with` takes the error slot itself for exactly as long as the
        // probe needs it.
        let result = tokio::task::spawn_blocking(move || probe_with(&client, PROBE_TIMEOUT))
            .await
            .change_context(AppError::Kafka)
            .attach("connect task panicked")?;

        if result.is_err() {
            self.drop_pooled_client(&connection.id);
        }
        result
    }

    fn release(&self, connection_id: &str) {
        self.drop_pooled_client(connection_id);
    }

/// Probes every bootstrap server **at once** and answers with the first
    /// one that connects.
    ///
    /// One at a time — which this used to do — meant a list was only as fast
    /// as the dead entries in front of the live one: each unreachable address
    /// costs the full `TCP_PING_TIMEOUT` before the next is tried, so a
    /// three-broker bootstrap list whose first two are down took 6 seconds to
    /// report a cluster that is up. This runs on the sidebar's per-connection
    /// status poll for every saved connection, so that delay was paid over
    /// and over.
    ///
    /// The race is *staggered*, which is what keeps it from costing more than
    /// it saves. The first entry is probed immediately and the rest only if it
    /// has not answered within [`PING_FANOUT_DELAY`], so the ordinary case —
    /// a healthy cluster whose first broker answers at once — opens exactly
    /// one socket, the same as the sequential version did. Firing all of them
    /// unconditionally (which this briefly did) tripled the sockets a
    /// three-broker cluster's status poll opened, every poll, for every saved
    /// connection: the sort of connection churn brokers meter and firewalls
    /// log, spent on an answer the first socket had already produced.
    async fn ping_bootstrap(&self, bootstrap_servers: &str) -> Result<ConnectionStatus, AppError> {
        let mut attempts = JoinSet::new();
        for (index, addr) in bootstrap_servers
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|entry| host_port(entry).to_string())
            .enumerate()
        {
            attempts.spawn(async move {
                // Everything after the first waits to see whether it is needed
                // at all. A cluster that answers is answered by one socket; a
                // first broker that is down costs this delay once, not one
                // whole TCP_PING_TIMEOUT per dead entry ahead of a live one.
                if index > 0 {
                    tokio::time::sleep(PING_FANOUT_DELAY).await;
                }
                matches!(timeout(TCP_PING_TIMEOUT, TcpStream::connect(&addr)).await, Ok(Ok(_)))
            });
        }

        while let Some(joined) = attempts.join_next().await {
            // `Err` is a panicked or aborted probe task, which is not a
            // reachable broker — it joins the failures rather than ending the
            // race, exactly as a refused connection does.
            if matches!(joined, Ok(true)) {
                attempts.abort_all();
                return Ok(ConnectionStatus::Reachable);
            }
        }

        // Also the answer for a bootstrap string with no addresses in it at
        // all: nothing to probe, so nothing was reached.
        Ok(ConnectionStatus::Unreachable)
    }

    async fn test_connection(
        &self,
        bootstrap_servers: &str,
        security_protocol: SecurityProtocol,
        sasl_mechanism: Option<SaslMechanism>,
        sasl_username: Option<&str>,
        password: Option<&str>,
        ssl: BrokerSslConfig<'_>,
    ) -> Result<ConnectionStatus, AppError> {
        run_probe(build_client_config(
            bootstrap_servers,
            security_protocol,
            sasl_mechanism,
            sasl_username,
            password,
            ssl,
        ))
        .await
    }

    async fn list_brokers(&self, connection: &Connection, read_timeout: Duration) -> Result<Vec<BrokerSummary>, AppError> {
        let client = self.metadata_client(connection)?;
        tokio::task::spawn_blocking(move || {
            let metadata = client.observed("failed to fetch broker metadata", |consumer| {
                consumer.fetch_metadata(None, read_timeout)
            })?;

            Ok(metadata
                .brokers()
                .iter()
                .map(|broker| BrokerSummary {
                    id: broker.id(),
                    host: broker.host().to_string(),
                    port: broker.port(),
                })
                .collect())
        })
        .await
        .change_context(AppError::Kafka)
        .attach("list_brokers task panicked")?
    }

    async fn list_topics(&self, connection: &Connection, read_timeout: Duration) -> Result<Vec<TopicSummary>, AppError> {
        let client = self.metadata_client(connection)?;
        tokio::task::spawn_blocking(move || {
            let metadata = client.observed("failed to fetch topic metadata", |consumer| {
                consumer.fetch_metadata(None, read_timeout)
            })?;

            Ok(metadata
                .topics()
                .iter()
                .map(|topic| TopicSummary {
                    name: topic.name().to_string(),
                    partition_count: topic.partitions().len(),
                })
                .collect())
        })
        .await
        .change_context(AppError::Kafka)
        .attach("list_topics task panicked")?
    }

    async fn list_consumer_groups(
        &self,
        connection: &Connection,
        read_timeout: Duration,
    ) -> Result<Vec<ConsumerGroupSummary>, AppError> {
        let client = self.metadata_client(connection)?;
        tokio::task::spawn_blocking(move || {
            let groups = client.observed("failed to fetch consumer group list", |consumer| {
                consumer.fetch_group_list(None, read_timeout)
            })?;

            Ok(groups
                .groups()
                .iter()
                .map(|group| ConsumerGroupSummary {
                    group_id: group.name().to_string(),
                    state: group.state().to_string(),
                })
                .collect())
        })
        .await
        .change_context(AppError::Kafka)
        .attach("list_consumer_groups task panicked")?
    }

    async fn count_topic_messages(&self, connection: &Connection, topic: &str, read_timeout: Duration) -> Result<u64, AppError> {
        let client = self.metadata_client(connection)?;
        let topic = topic.to_string();
        tokio::task::spawn_blocking(move || {
            let consumer = Arc::clone(&client.consumer);
            let metadata = client.observed(&format!("failed to fetch metadata for topic {topic}"), |consumer| {
                consumer.fetch_metadata(Some(&topic), read_timeout)
            })?;
            let topic_metadata = metadata
                .topics()
                .iter()
                .find(|t| t.name() == topic)
                .ok_or_else(|| error_stack::Report::new(AppError::NotFound))
                .attach_with(|| format!("topic {topic} not found"))?;

            // Outside the `observed` section above: on a wide topic this is
            // the seconds-long part, and it needs no exclusive access — see
            // `ObservedClient::error_slot`.
            let partitions: Vec<i32> = topic_metadata.partitions().iter().map(|p| p.id()).collect();
            let watermarks = watermarks_for_partitions(consumer.as_ref(), &topic, &partitions, read_timeout)?;

            Ok(watermarks.values().map(|&(low, high)| (high - low).max(0) as u64).sum())
        })
        .await
        .change_context(AppError::Kafka)
        .attach("count_topic_messages task panicked")?
    }

    #[allow(clippy::too_many_arguments)]
    async fn fetch_messages(
        &self,
        connection: &Connection,
        topic: &str,
        filter: &MessageFilter,
        on_message: Option<mpsc::UnboundedSender<TopicMessage>>,
        read_timeout: Duration,
        max_message_size_bytes: u32,
        max_total_payload_bytes: Option<u64>,
        cancelled: Arc<AtomicBool>,
    ) -> Result<MessageFetchResult, AppError> {
        let config = fetch_consumer_config(connection, max_message_size_bytes);
        // Identical to `config`; a separate handle only because the closure
        // below needs one to build each extra shard's consumer from.
        let shard_config = config.clone();
        // The topic's partition list comes from the **pooled** metadata
        // client, never from this fetch's own consumer — see the metadata
        // call below for why that is worth a second client.
        let metadata_client = self.metadata_client(connection)?;
        let topic = topic.to_string();
        let filter = filter.clone();
        // Captured here rather than reached for inside the blocking closure,
        // so the deferred teardown at the end of it cannot depend on a
        // runtime context being present on a blocking-pool thread.
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            // A consumer per fetch, deliberately.
            //
            // Pooling them across fetches looks obviously right — the Data
            // tab's per-row "Fetch payload" is a whole fetch for one message,
            // and building a consumer means librdkafka's threads, a socket,
            // and on a SASL/TLS cluster a full handshake. Measured against a
            // real broker over 20 single-row fetches, it was 3x *slower*:
            // ~104ms per row building a fresh consumer, ~346ms reusing one
            // (~551ms before `fetch.wait.max.ms` was lowered below). Parking
            // a consumer between fetches means unassigning and reassigning it,
            // and whatever that costs inside librdkafka dwarfs what building
            // a new one costs. Measure before reaching for this again.
            // Stop has to be able to interrupt the setup, not just the poll
            // loop below. Building a client, fetching metadata and reading
            // every target partition's watermarks is seconds of broker work
            // on a wide topic — long enough for the user to give up on it —
            // and a cancellation that only took effect once the first poll
            // came round left all of it running. Checked between phases
            // rather than inside them: each is a single blocking librdkafka
            // call that cannot be interrupted from here, so between them is
            // as fine-grained as this gets.
            let stopped = || cancelled.load(Ordering::Relaxed);
            if stopped() {
                return Ok(MessageFetchResult::default());
            }

            let client = ObservedClient::create(&config)?;
            let consumer = Arc::clone(&client.consumer);
            if stopped() {
                return Ok(MessageFetchResult::default());
            }

            // Asked of the pooled metadata client rather than the consumer
            // built just above, because the same question costs ~500x more
            // of the one than the other.
            //
            // `fetch_consumer_config` has to set a `group.id`: rdkafka
            // routes `assign()` through librdkafka's consumer-group
            // machinery, and without one it fails outright with "Local:
            // Unknown group". Setting it also starts that machinery, whose
            // first act on a new client is a **group coordinator query** —
            // and a metadata request issued on the same client queues behind
            // it on librdkafka's main queue. This fetch never subscribes,
            // never joins the group and never commits, so that coordinator
            // round trip buys nothing and is paid on every single fetch.
            //
            // `client_config` (what `metadata_client` is built from) sets no
            // `group.id` at all, so that client has no group machinery to
            // wait on — and it is long-lived and already connected.
            //
            // Measured, 100-message browse of a 6-partition topic, five
            // consecutive fetches: **[513, 24, 527, 512, 527] ms** asking
            // this fetch's own consumer, against **[12, 14, 13, 13, 11] ms**
            // asking the pooled one, whose metadata call itself is ~1 ms.
            // The rest of the fetch — watermarks, assign, and polling 100
            // messages — is 11 ms of that, and is left on the fetch's own
            // consumer: `fetch_watermarks` uses its own reply queue rather
            // than the main one (see `ObservedClient::error_slot`), so it
            // never queues behind the coordinator query and is already fast.
            //
            // Not fixed by pooling the *fetch* consumer instead: re-assigning
            // a parked consumer costs ~370 ms a browse, which is what the
            // note above about consumers-per-fetch found and this re-measured.
            let metadata = metadata_client.observed(
                &format!("failed to fetch metadata for topic {topic}"),
                |consumer| consumer.fetch_metadata(Some(&topic), read_timeout),
            )?;
            let topic_metadata = metadata
                .topics()
                .iter()
                .find(|t| t.name() == topic)
                .ok_or_else(|| error_stack::Report::new(AppError::NotFound))
                .attach_with(|| format!("topic {topic} not found"))?;

            let target_partitions: Vec<i32> = match &filter.partitions {
                Some(partitions) => partitions.clone(),
                None => topic_metadata.partitions().iter().map(|p| p.id()).collect(),
            };

            // Queried once per partition, concurrently, and reused. The offset
            // maths below reads each partition's watermarks several times
            // over — as a closure that re-queried, a wide topic paid hundreds
            // of round trips before a single message moved.
            let watermarks_by_partition =
                watermarks_for_partitions(consumer.as_ref(), &topic, &target_partitions, read_timeout)?;
            if stopped() {
                return Ok(MessageFetchResult::default());
            }

            let watermarks = |partition: i32| -> Result<(i64, i64), AppError> {
                watermarks_by_partition
                    .get(&partition)
                    .copied()
                    .ok_or_else(|| error_stack::Report::new(AppError::NotFound))
                    .attach_with(|| format!("no such partition: {topic}:{partition}"))
            };

            // An explicit `offset` and a `from_timestamp_ms` are independent
            // constraints, not alternatives — both apply together (AND) when
            // both are given, via `combined_start_offset`, rather than one
            // silently overriding the other. See its doc comment for why the
            // later (higher) of the two is the correct combination.
            let explicit_start_offsets: Option<BTreeMap<i32, i64>> = filter
                .offset
                .map(|offset| {
                    target_partitions
                        .iter()
                        .map(|&p| watermarks(p).map(|(low, high)| (p, clamp_offset(offset, low, high))))
                        .collect::<Result<BTreeMap<i32, i64>, AppError>>()
                })
                .transpose()?;

            // The fallback is the **high** watermark, not the low one.
            //
            // `offsets_for_times` leaves a partition unresolved in exactly one
            // situation: the requested time is after every message in it. The
            // right start for "From 5pm" in a partition whose newest message
            // is from midday is therefore the end of that partition — no
            // message satisfies the filter, so the fetch reads none.
            //
            // This used to fall back to the low watermark, which is the
            // opposite answer to the same question: a From-time later than a
            // partition's newest message made it read that partition **from
            // the very beginning**, so a filter meant to narrow the fetch to
            // the last few minutes returned the topic's oldest messages
            // instead. On an idle topic — where every partition is past its
            // newest message the moment you ask for anything recent — that
            // was every partition, so From appeared to be ignored entirely.
            //
            // See the `to_timestamp_ms` resolution below, whose own fallback
            // is the high watermark for the mirror-image reason: a To-time
            // after every message means "read to the end", which is what the
            // high watermark already says.
            let from_timestamp_start_offsets: Option<BTreeMap<i32, i64>> = filter
                .from_timestamp_ms
                .map(|from_ms| {
                    resolve_offsets_by_timestamp(&consumer, &topic, &target_partitions, from_ms, read_timeout, |p| {
                        watermarks(p).map(|(_, high)| high)
                    })
                })
                .transpose()?;

            // Where the *filter* puts each partition's start, before any count
            // cap narrows it: the explicit offset / from-timestamp when either
            // is given, the low watermark otherwise. Everything below measures
            // against this — "how many messages match" is a property of the
            // filter, and the count caps then carve a window out of that range
            // rather than defining it.
            let start_is_pinned_by_filter =
                explicit_start_offsets.is_some() || from_timestamp_start_offsets.is_some();
            let filter_start_offsets: BTreeMap<i32, i64> = if start_is_pinned_by_filter {
                target_partitions
                    .iter()
                    .map(|&p| {
                        let explicit = explicit_start_offsets.as_ref().and_then(|m| m.get(&p).copied());
                        let from_ts = from_timestamp_start_offsets.as_ref().and_then(|m| m.get(&p).copied());
                        let start = combined_start_offset(explicit, from_ts)
                            .expect("at least one start-offset source is set for every target partition");
                        (p, start)
                    })
                    .collect()
            } else {
                target_partitions
                    .iter()
                    .map(|&p| watermarks(p).map(|(low, _)| (p, low)))
                    .collect::<Result<_, _>>()?
            };

            let end_offsets: BTreeMap<i32, i64> = if let Some(to_ms) = filter.to_timestamp_ms {
                resolve_offsets_by_timestamp(&consumer, &topic, &target_partitions, to_ms, read_timeout, |p| {
                    watermarks(p).map(|(_, high)| high)
                })?
            } else {
                target_partitions
                    .iter()
                    .map(|&p| watermarks(p).map(|(_, high)| (p, high)))
                    .collect::<Result<_, _>>()?
            };

            // How many messages actually satisfy the partition/offset/
            // timestamp constraints, uncapped by the count limits below — the
            // frontend shows the gap ("100 of 4,812 loaded") so the user can
            // tell more remain beyond what was pulled. Measured from the
            // filter's own start, not from the capped window: measuring from
            // the window made this the same number as the window itself, so
            // "loaded / matching" reported the cap back to the user as though
            // it were the size of the topic.
            let total_matching: u64 = partition_limits(&filter_start_offsets, &end_offsets, None)
                .values()
                .map(|&available| available.max(0) as u64)
                .sum();

            // Two independent limits, both honoured. "Max messages per
            // partition" is a window — never read further back than this in
            // any one partition. "Total max messages" is a budget — how many
            // to actually read, spread over the partitions rather than
            // draining them in id order.
            //
            // `partition_limits` treats `None` as "uncapped", which is right
            // for it as a generic utility and wrong for this caller: an
            // offset- or timestamp-filtered fetch with no count set would
            // otherwise read from its start point to the end of the
            // partition, so the per-partition window always gets a concrete
            // value via `effective_max_messages_per_partition`.
            let windows = partition_limits(
                &filter_start_offsets,
                &end_offsets,
                Some(effective_max_messages_per_partition(filter.max_messages_per_partition)),
            );
            let limits = distribute_total_budget(&windows, filter.max_total_messages);

            // The start offsets to actually assign. When the filter pinned a
            // start, reading begins there. Otherwise the fetch is
            // newest-first, and each partition begins exactly its allocation
            // back from the end — so a budget of 100 across 48 partitions
            // reads ~2 messages per partition instead of reading 100 from
            // each and discarding 4,700 of them. On a topic of multi-megabyte
            // records that difference is the whole fetch.
            let start_offsets: BTreeMap<i32, i64> = if start_is_pinned_by_filter {
                filter_start_offsets
            } else {
                target_partitions
                    .iter()
                    .map(|&p| {
                        watermarks(p).map(|(low, high)| {
                            // `end` rather than the high watermark: with a
                            // to-timestamp filter the newest message in range
                            // is that boundary, not the end of the partition.
                            let end = end_offsets.get(&p).copied().unwrap_or(high);
                            let take = limits.get(&p).copied().unwrap_or(0);
                            (p, newest_first_start_offset(low, end, take))
                        })
                    })
                    .collect::<Result<_, _>>()?
            };

            let working: Vec<i32> =
                limits.iter().filter(|&(_, &limit)| limit > 0).map(|(&partition, _)| partition).collect();

            // One consumer per shard, polled from this one loop.
            //
            // The parallelism being bought here is librdkafka's, not ours:
            // each consumer instance runs its own thread per broker, and that
            // thread is where a fetch response is received *and decompressed*,
            // before any message reaches the queue this loop drains. A single
            // consumer therefore serialises the decompression of every
            // partition it owns, which is most of what a browse of a wide
            // compressed topic spends its time on. See `fetch_shard_count`
            // for the measurements and for what bounds the count.
            //
            // Shard 0 is the consumer already built above for the watermark
            // walk, so a topic narrow enough for one shard builds nothing
            // extra and behaves exactly as it did before.
            let shard_count = fetch_shard_count(working.len(), max_message_size_bytes);
            let mut shards: Vec<ObservedClient> = Vec::with_capacity(shard_count);
            // Built concurrently, because building them is the one part of
            // sharding the user waits for. Creating a consumer is ~6 ms of
            // librdkafka setup that does not overlap with anything else in
            // this closure, so seven of them in series put ~45 ms in front of
            // every fetch — enough to lose the whole benefit on a topic that
            // was fast to begin with. In parallel it is roughly the cost of
            // one.
            //
            // Each shard keeps its own prefetch queue, so the size
            // `shard_config` carries is per consumer — `fetch_shard_count` is
            // what keeps the total of them bounded.
            if shard_count > 1 {
                if stopped() {
                    return Ok(MessageFetchResult::default());
                }
                let built: Vec<Result<ObservedClient, AppError>> = std::thread::scope(|scope| {
                    let handles: Vec<_> = (1..shard_count)
                        .map(|_| scope.spawn(|| ObservedClient::create(&shard_config)))
                        .collect();
                    handles.into_iter().map(|handle| handle.join().expect("shard builder panicked")).collect()
                });
                for shard in built {
                    shards.push(shard?);
                }
            }
            let consumers: Vec<Arc<ObservedConsumer>> = std::iter::once(Arc::clone(&consumer))
                .chain(shards.iter().map(|shard| Arc::clone(&shard.consumer)))
                .collect();

            // Dealt round-robin rather than in contiguous blocks: adjacent
            // partition ids tend to share a leader, so slicing the list into
            // runs would hand one shard several partitions from one broker
            // while another shard sat idle.
            let mut assignments = vec![TopicPartitionList::new(); consumers.len()];
            for (position, &partition) in working.iter().enumerate() {
                let start = start_offsets.get(&partition).copied().unwrap_or(0);
                assignments[position % consumers.len()]
                    .add_partition_offset(&topic, partition, Offset::Offset(start))
                    .change_context(AppError::Kafka)
                    .attach("failed to build partition assignment")?;
            }
            if stopped() {
                return Ok(MessageFetchResult::default());
            }
            for (shard, assignment) in consumers.iter().zip(&assignments) {
                if assignment.count() == 0 {
                    continue;
                }
                shard
                    .assign(assignment)
                    .change_context(AppError::Kafka)
                    .attach("failed to assign partitions")?;
            }

            let total_target: i64 = limits.values().sum();
            // How many assigned partitions still have reading left to do.
            //
            // The fetch is finished when every partition is, and *that* is
            // what ends the poll loop — not `collected.len() == total_target`
            // on its own. `total_target` comes from `high - low`, which
            // counts offsets rather than messages: a transaction's commit
            // marker and a compacted-away record both occupy an offset that
            // no consumer is ever handed. On such a topic the count is
            // unreachable and the loop used to sit out its full ten-second
            // `IDLE_TIMEOUT` on every fetch, having already collected
            // everything there was. See `finished` below for the three ways
            // a partition completes.
            let mut unfinished = limits.values().filter(|&&limit| limit > 0).count();
            let mut finished: HashSet<i32> = HashSet::with_capacity(unfinished);
            let mut remaining = limits;
            // Reserving up front keeps the repeated grow-and-copy off every
            // ordinary fetch. Bounded rather than reserving `total_target`
            // outright: that figure comes from the user's own filter (per
            // partition cap x partitions, or an explicit total budget), so
            // an extreme value would otherwise reserve that many message
            // slots before a single message had been read. Beyond the bound
            // the vector still grows normally.
            const PREALLOCATED_MESSAGES: i64 = 8192;
            let mut collected = Vec::with_capacity(total_target.clamp(0, PREALLOCATED_MESSAGES) as usize);
            const POLL_TIMEOUT: Duration = Duration::from_millis(500);
            const IDLE_TIMEOUT: Duration = Duration::from_secs(10);
            // How long one blocking wait may last. A blocking poll wakes for
            // *its own* consumer's queue and nothing else, so with several
            // shards a long wait is latency: the loop can sit on one quiet
            // shard while another already has messages ready. Sweeping them
            // often costs a few non-blocking polls per millisecond and is far
            // cheaper than that. A single shard has nothing to miss and keeps
            // the full timeout.
            const SHARDED_POLL_SLICE: Duration = Duration::from_millis(5);
            let poll_slice =
                if consumers.len() == 1 { POLL_TIMEOUT } else { SHARDED_POLL_SLICE };
            let mut next_shard = 0usize;
            let mut idle_elapsed = Duration::ZERO;
            let mut last_poll_error: Option<String> = None;
            // Counted only over payloads this fetch actually keeps — see the
            // `include_payload` guard below.
            let mut payload_bytes_read: u64 = 0;
            let mut stopped_at_byte_budget = false;

            while unfinished > 0
                && (collected.len() as i64) < total_target
                && idle_elapsed < IDLE_TIMEOUT
                && !stopped_at_byte_budget
                && !cancelled.load(Ordering::Relaxed)
            {
                // One non-blocking sweep of every shard, then a blocking
                // wait on the next one in turn if the whole sweep was empty.
                // Blocking on a single shard for the full `POLL_TIMEOUT`
                // would let one quiet consumer hold up another that already
                // has messages queued, so the wait is divided between them —
                // and with a single shard this is exactly the blocking poll
                // it has always been.
                let polled = {
                    let mut ready = None;
                    for _ in 0..consumers.len() {
                        let shard = &consumers[next_shard % consumers.len()];
                        next_shard = next_shard.wrapping_add(1);
                        if let Some(message) = shard.poll(Duration::ZERO) {
                            ready = Some(message);
                            break;
                        }
                    }
                    match ready {
                        Some(message) => Some(message),
                        None => {
                            let shard = &consumers[next_shard % consumers.len()];
                            next_shard = next_shard.wrapping_add(1);
                            shard.poll(poll_slice)
                        }
                    }
                };
                match polled {
                    Some(Ok(borrowed)) => {
                        idle_elapsed = Duration::ZERO;
                        let partition = borrowed.partition();
                        // One map lookup per message rather than two (a
                        // `get` to read the budget, then an `insert` to
                        // write it back): the same borrow that reads it
                        // decrements it in place below. A partition with no
                        // entry is one this fetch never assigned, and is
                        // skipped exactly as an exhausted budget is.
                        let Some(budget) = remaining.get_mut(&partition) else {
                            continue;
                        };
                        if *budget <= 0 {
                            continue;
                        }
                        // Past the end of what the filter asked for. With no
                        // to-timestamp filter this is the high watermark and
                        // so never triggers, but a to-timestamp fetch has a
                        // real end offset part-way down the partition — and
                        // its budget alone cannot be trusted to stop there,
                        // for the same reason `total_target` cannot be
                        // trusted to end the loop: any offset in the range
                        // that carries no message leaves budget over, and the
                        // messages that budget then admits are the ones after
                        // the To time the user set.
                        if let Some(&end) = end_offsets.get(&partition)
                            && borrowed.offset() >= end
                        {
                            if finished.insert(partition) {
                                unfinished -= 1;
                            }
                            continue;
                        }
                        let payload = borrowed.payload().unwrap_or(&[]);
                        payload_bytes_read += budgeted_payload_bytes(payload.len(), filter.include_payload);
                        let message = TopicMessage {
                            partition,
                            offset: borrowed.offset(),
                            timestamp_ms: borrowed.timestamp().to_millis(),
                            key_base64: borrowed.key().map(|k| BASE64.encode(k)),
                            // Only ever the slice the caller asked for. This
                            // is the difference between a 1,000-row fetch of
                            // 3 MB records costing a few MB of base64 and
                            // costing ~4 GB of it — twice over, once streamed
                            // and once in this result.
                            payload_base64: filter
                                .include_payload
                                .then(|| BASE64.encode(payload_preview_slice(payload, filter.max_payload_preview_bytes))),
                            // Sent whether or not the payload itself is,
                            // so a row can report a message's real size and
                            // the viewer can tell a cut preview from a whole
                            // payload that happened to be short.
                            payload_size_bytes: borrowed.payload().map(|p| p.len() as u64),
                            headers: extract_headers(&borrowed),
                        };
                        if let Some(sender) = &on_message {
                            let _ = sender.send(message.clone());
                        }
                        collected.push(message);
                        *budget -= 1;
                        if *budget <= 0 && finished.insert(partition) {
                            unfinished -= 1;
                        }
                        // After taking the message, never before — see
                        // `byte_budget_reached`.
                        stopped_at_byte_budget = byte_budget_reached(payload_bytes_read, max_total_payload_bytes);
                    }
                    // Reaching the end of a partition is how a fetch finishes,
                    // not something that went wrong with it. `enable.partition
                    // .eof` is set precisely so this arrives (see
                    // `fetch_consumer_config`), so it neither counts towards
                    // the idle timeout nor becomes a `poll_error` the Data tab
                    // would show the user as a failed read.
                    Some(Err(KafkaError::PartitionEOF(partition))) => {
                        if remaining.contains_key(&partition) && finished.insert(partition) {
                            unfinished -= 1;
                        }
                    }
                    Some(Err(err)) => {
                        idle_elapsed += poll_slice;
                        last_poll_error = Some(describe_poll_error(&err));
                    }
                    None => {
                        idle_elapsed += poll_slice;
                    }
                }
            }

            let result = MessageFetchResult {
                messages: collected,
                total_matching,
                poll_error: last_poll_error,
                stopped_at_byte_budget,
                payload_bytes_read,
            };

            // Closing this consumer costs a flat **99 ms**, and the user is
            // not waiting for any of it.
            //
            // It is the same `group.id` tax as the metadata call above, at
            // the other end of the consumer's life: `assign()` forces a
            // group id, a group id starts librdkafka's consumer-group
            // machinery, and closing that machinery down is a fixed cost
            // whether or not the group was ever joined. Measured over five
            // create-then-drop cycles: **99, 99, 99, 99, 99 ms** with a
            // group id set against **0, 0, 0, 0, 0 ms** without one — so it
            // is the group, not the socket or the threads.
            //
            // Nothing in `result` refers to this consumer: the messages are
            // owned `TopicMessage`s, already copied out of librdkafka's
            // buffers. So the close is moved onto the blocking pool and the
            // fetch returns without it, taking a 100-message browse from
            // ~111 ms to ~12 ms.
            //
            // The early returns above (a Stop caught between phases) still
            // close inline. They are not worth the same treatment: they run
            // when the user has already given up on this fetch, so there is
            // no latency there to save.
            // Every shard, not just the first: each one carries the same
            // ~99 ms group-machinery close described above.
            let teardown = (client, consumer, consumers, shards);
            runtime.spawn_blocking(move || drop(teardown));

            Ok(result)
        })
        .await
        .change_context(AppError::Kafka)
        .attach("fetch_messages task panicked")?
    }

    async fn list_partitions(
        &self,
        connection: &Connection,
        topic: &str,
        read_timeout: Duration,
    ) -> Result<Vec<PartitionSummary>, AppError> {
        let client = self.metadata_client(connection)?;
        let topic = topic.to_string();
        tokio::task::spawn_blocking(move || {
            let consumer = Arc::clone(&client.consumer);
            let metadata = client.observed(&format!("failed to fetch metadata for topic {topic}"), |consumer| {
                consumer.fetch_metadata(Some(&topic), read_timeout)
            })?;
            let topic_metadata = metadata
                .topics()
                .iter()
                .find(|t| t.name() == topic)
                .ok_or_else(|| error_stack::Report::new(AppError::NotFound))
                .attach_with(|| format!("topic {topic} not found"))?;

            // Outside the `observed` section — see `count_topic_messages`.
            let partition_ids: Vec<i32> = topic_metadata.partitions().iter().map(|p| p.id()).collect();
            let watermarks = watermarks_for_partitions(consumer.as_ref(), &topic, &partition_ids, read_timeout)?;

            Ok(topic_metadata
                .partitions()
                .iter()
                .map(|partition| {
                    let (low, high) = watermarks.get(&partition.id()).copied().unwrap_or((0, 0));
                    PartitionSummary {
                        id: partition.id(),
                        leader: partition.leader(),
                        replicas: partition.replicas().to_vec(),
                        isr: partition.isr().to_vec(),
                        low_offset: low,
                        high_offset: high,
                    }
                })
                .collect())
        })
        .await
        .change_context(AppError::Kafka)
        .attach("list_partitions task panicked")?
    }

    async fn describe_topic_config(
        &self,
        connection: &Connection,
        topic: &str,
        read_timeout: Duration,
    ) -> Result<Vec<ConfigEntry>, AppError> {
        // Pooled in its own map rather than built here: the admin client is
        // its own type, so it can't come from the consumer pool, and there is
        // no queue for `drain_error_events` to serve — an admin failure is
        // classified from its error code alone. See `admin_client`.
        let admin = self.admin_client(connection)?;

        let specifier = ResourceSpecifier::Topic(topic);
        let options = AdminOptions::new().request_timeout(Some(read_timeout));
        let results = admin.describe_configs([&specifier], &options).await.map_err(|err| {
            failure_report(&err, &err.to_string(), &format!("failed to describe config for topic {topic}"))
        })?;

        let resource_result = results
            .into_iter()
            .next()
            .ok_or_else(|| error_stack::Report::new(AppError::Kafka))
            .attach_with(|| format!("no config result returned for topic {topic}"))?;
        let resource = resource_result
            .change_context(AppError::Kafka)
            .attach_with(|| format!("kafka rejected describe-config for topic {topic}"))?;

        Ok(resource
            .entries
            .into_iter()
            .map(|entry| ConfigEntry { name: entry.name, value: entry.value })
            .collect())
    }

    #[allow(clippy::too_many_arguments)]
    async fn detect_cluster_version(
        &self,
        bootstrap_servers: &str,
        security_protocol: SecurityProtocol,
        sasl_mechanism: Option<SaslMechanism>,
        sasl_username: Option<&str>,
        password: Option<&str>,
        ssl: BrokerSslConfig<'_>,
        read_timeout: Duration,
    ) -> Result<ClusterVersionReport, AppError> {
        let config = build_client_config(
            bootstrap_servers,
            security_protocol,
            sasl_mechanism,
            sasl_username,
            password,
            ssl,
        );

        // Any broker will do: both configs read below are per-node but
        // static, and a cluster whose nodes disagree about whether they run
        // KRaft is not a configuration this app needs to describe. Same
        // reasoning as `authorizer_class`.
        let consumer_config = config.clone();
        let broker_id = tokio::task::spawn_blocking(move || {
            let client = ObservedClient::create(&consumer_config)?;
            let metadata = client.observed("failed to reach the cluster", |consumer| {
                consumer.fetch_metadata(None, read_timeout)
            })?;
            metadata
                .brokers()
                .first()
                .map(|broker| broker.id())
                .ok_or_else(|| error_stack::Report::new(AppError::Kafka))
                .attach("the cluster returned no brokers")
        })
        .await
        .change_context(AppError::Kafka)
        .attach("version detection task panicked")??;

        let admin: AdminClient<DefaultClientContext> = config
            .create()
            .map_err(|err| failure_report(&err, &err.to_string(), "failed to create kafka admin client"))?;
        let options = AdminOptions::new().request_timeout(Some(read_timeout));
        let results = admin
            .describe_configs([&ResourceSpecifier::Broker(broker_id)], &options)
            .await
            .map_err(|err| failure_report(&err, &err.to_string(), "failed to describe broker config"))?;

        // A per-resource error collapses to no entries, which
        // `cluster_version_report` reports as Unknown with a note — see this
        // method's doc comment for why that is not an `Err`.
        //
        // Note the error *code* is discarded here, so this is not purely the
        // authorization case: a resource-level failure that is not a refusal
        // — the broker picked above dying between the metadata fetch and this
        // call — lands on Unknown too. That is the same ambiguity
        // `AclAvailability` documents, minus the second question:
        // `authorizer_class` can recover ACL availability from
        // `authorizer.class.name`, and nothing comparable distinguishes
        // "refused" from "that broker went away". Which is why the note tells
        // the user to choose the version manually rather than naming a cause
        // as fact.
        let entries = results
            .into_iter()
            .next()
            .and_then(|resource| resource.ok())
            .map(|resource| resource.entries)
            .unwrap_or_default();

        let value_of = |name: &str| {
            entries
                .iter()
                .find(|entry| entry.name == name)
                .map(|entry| entry.value.clone().unwrap_or_default())
        };

        Ok(cluster_version_report(
            value_of(PROCESS_ROLES_CONFIG).as_deref(),
            value_of(INTER_BROKER_PROTOCOL_VERSION_CONFIG).as_deref(),
        ))
    }

    async fn describe_acls(
        &self,
        connection: &Connection,
        filter: AclFilter,
        read_timeout: Duration,
    ) -> Result<AclListing, AppError> {
        // The same pooled admin client the Config tab uses: an ACL listing
        // is another DescribeConfigs-shaped admin round trip, and opening
        // Access Control right after a Config tab should not cost a second
        // handshake. See `admin_client`.
        let admin = self.admin_client(connection)?;
        let listing = crate::acl::describe_acls(admin, filter, read_timeout).await?;

        // A listing that actually carries bindings needs no interpreting: the
        // broker answered and we can see the answer.
        if !listing.bindings.is_empty() {
            return Ok(listing);
        }

        // An *empty* listing is the ambiguous one, and librdkafka will not
        // tell us which kind it is — it drops the DescribeAcls error code
        // (see `crate::acl`). So ask the broker something it does answer
        // honestly: whether it is running an authorizer at all. One extra
        // round trip, and only in the empty case.
        let authorizer = self.authorizer_class(connection, read_timeout).await;
        Ok(AclListing {
            availability: AclAvailability::for_empty_listing(authorizer.as_deref()),
            ..listing
        })
    }

    async fn fetch_consumer_group_lag(
        &self,
        connection: &Connection,
        group_id: &str,
        read_timeout: Duration,
    ) -> Result<ConsumerGroupLag, AppError> {
        let client = self.metadata_client(connection)?;
        let mut group_config = client_config(connection);
        let group_id = group_id.to_string();
        tokio::task::spawn_blocking(move || {
            let consumer = Arc::clone(&client.consumer);
            let groups = client.observed(&format!("failed to fetch group list for {group_id}"), |consumer| {
                consumer.fetch_group_list(Some(&group_id), read_timeout)
            })?;
            let group = groups
                .groups()
                .iter()
                .find(|g| g.name() == group_id)
                .ok_or_else(|| error_stack::Report::new(AppError::NotFound))
                .attach_with(|| format!("group {group_id} not found"))?;

            let mut owners: HashMap<(String, i32), (String, String)> = HashMap::new();
            let mut decode_failures = 0usize;
            let mut decode_attempts = 0usize;
            for member in group_members(group) {
                if group.protocol_type() != "consumer" {
                    continue;
                }
                let Some(assignment_bytes) = member.assignment() else {
                    continue;
                };
                decode_attempts += 1;
                match decode_consumer_protocol_assignment(assignment_bytes) {
                    Ok(partitions) => {
                        for (topic, partition) in partitions {
                            owners.insert(
                                (topic, partition),
                                (member.client_id().to_string(), member.client_host().to_string()),
                            );
                        }
                    }
                    Err(_) => decode_failures += 1,
                }
            }

            if decode_attempts > 0 && decode_failures == decode_attempts {
                return Err(error_stack::Report::new(AppError::Kafka)).attach_with(|| {
                    format!("could not determine partition assignment for group {group_id}")
                });
            }

            if owners.is_empty() {
                return Ok(ConsumerGroupLag {
                    state: group.state().to_string(),
                    partitions: Vec::new(),
                });
            }

            let mut tpl = TopicPartitionList::new();
            for (topic, partition) in owners.keys() {
                tpl.add_partition(topic, *partition);
            }

            group_config.set("group.id", &group_id);
            // Not pooled: this one carries a `group.id`, which changes what
            // the client is, and it exists only for this request.
            let group_client = ObservedClient::create(&group_config)?;
            let committed = group_client
                .consumer
                .committed_offsets(tpl, read_timeout)
                .map_err(|err| {
                    group_client.failure(&err, &format!("failed to fetch committed offsets for {group_id}"))
                })?;

            // Grouped by topic and queried concurrently within each, rather
            // than one blocking round trip per committed partition as the
            // loop below walks them — a group consuming a 100-partition topic
            // spent seconds here for a lag table.
            //
            // Outside the `observed` section, so those seconds no longer
            // block every other request on this connection — see
            // `ObservedClient::error_slot`.
            let mut partitions_by_topic: BTreeMap<String, Vec<i32>> = BTreeMap::new();
            for element in committed.elements() {
                partitions_by_topic.entry(element.topic().to_string()).or_default().push(element.partition());
            }
            let mut watermarks: HashMap<(String, i32), (i64, i64)> = HashMap::new();
            for (topic, topic_partitions) in &partitions_by_topic {
                for (partition, marks) in
                    watermarks_for_partitions(consumer.as_ref(), topic, topic_partitions, read_timeout)?
                {
                    watermarks.insert((topic.clone(), partition), marks);
                }
            }

            let mut partitions = Vec::new();
            for element in committed.elements() {
                let topic = element.topic().to_string();
                let partition = element.partition();
                let current_offset = element.offset().to_raw().filter(|&o| o >= 0);

                let (_low, high) = watermarks.get(&(topic.clone(), partition)).copied().unwrap_or((0, 0));

                let lag = current_offset.map(|current| (high - current).max(0));
                let (client_id, client_host) = owners
                    .get(&(topic.clone(), partition))
                    .cloned()
                    .map(|(id, host)| (Some(id), Some(host)))
                    .unwrap_or((None, None));

                partitions.push(PartitionLag {
                    topic,
                    partition,
                    current_offset,
                    log_end_offset: high,
                    lag,
                    client_id,
                    client_host,
                });
            }

            Ok(ConsumerGroupLag { state: group.state().to_string(), partitions })
        })
        .await
        .change_context(AppError::Kafka)
        .attach("fetch_consumer_group_lag task panicked")?
    }

    async fn publish_messages(
        &self,
        connection: &Connection,
        topic: &str,
        partition: i32,
        records: &[EncodedRecord],
        max_message_size_bytes: u32,
        write_timeout: Duration,
    ) -> Result<PublishOutcome, AppError> {
        // Deliberately *not* routed through `metadata_client`/`admin_client`'s
        // pool. Every other client here is long-lived because it is read-only
        // and reused constantly; a producer is neither. Building one per publish
        // means no write-capable client exists on this machine except while an
        // authorized publish is actually in flight, and the handshake it costs
        // is paid once per deliberate human action rather than per request.
        crate::producer::publish_messages(
            connection,
            topic,
            partition,
            records,
            max_message_size_bytes,
            write_timeout,
        )
        .await
    }
}

/// Turns a poll failure into a line that explains itself.
///
/// `NotImplemented` is what librdkafka reports for a batch it has no code path
/// to decompress, and rdkafka-rust throws away the detail: the error op
/// librdkafka enqueues carries a string naming the offending codec
/// (`rd_kafka_event_error_string`), but `handle_error_event` keeps only the
/// error *code*, so all that reaches us is "Message consumption error:
/// NotImplemented (Local: Not implemented)". Every bug report about this
/// arrived as exactly that sentence, which cannot distinguish "this build was
/// compiled without the codec" — the cause every previous time — from anything
/// else sharing the code.
///
/// So attach what this binary can actually decode, read out of librdkafka
/// itself (see [`crate::build_info`]). The next report then says which case it
/// is without a round trip.
fn describe_poll_error(err: &KafkaError) -> String {
    let message = err.to_string();

    let code = match err {
        KafkaError::MessageConsumption(code) | KafkaError::MessageConsumptionFatal(code) => *code,
        _ => return message,
    };
    if code != RDKafkaErrorCode::NotImplemented {
        return message;
    }

    let features = crate::build_info::builtin_features();
    match crate::build_info::missing_required_features().as_slice() {
        [] => format!(
            "{message} - this build decodes {features}, so the batch is not \
             simply using a codec that was left out of the build"
        ),
        missing => format!(
            "{message} - this build of librdkafka is missing {}, and a topic \
             compressed with one of those cannot be read at all. Compiled \
             with: {features}",
            missing.join(", "),
        ),
    }
}

/// Resolves each target partition's offset at `timestamp_ms` via
/// `offsets_for_times`, falling back to `fallback` (a watermark lookup) for
/// any partition librdkafka couldn't resolve to a concrete offset (e.g. the
/// timestamp is after every message in the partition).
fn resolve_offsets_by_timestamp(
    consumer: &ObservedConsumer,
    topic: &str,
    partitions: &[i32],
    timestamp_ms: i64,
    read_timeout: Duration,
    fallback: impl Fn(i32) -> Result<i64, AppError>,
) -> Result<BTreeMap<i32, i64>, AppError> {
    let mut request = TopicPartitionList::new();
    for &partition in partitions {
        request
            .add_partition_offset(topic, partition, Offset::Offset(timestamp_ms))
            .change_context(AppError::Kafka)
            .attach("failed to build offsets_for_times request")?;
    }

    let resolved = consumer
        .offsets_for_times(request, read_timeout)
        .change_context(AppError::Kafka)
        .attach("failed to resolve timestamp to offsets")?;

    let mut result = BTreeMap::new();
    for partition in partitions {
        let raw_offset = resolved
            .elements_for_topic(topic)
            .into_iter()
            .find(|elem| elem.partition() == *partition)
            .and_then(|elem| elem.offset().to_raw())
            .filter(|&offset| offset >= 0);

        let offset = match raw_offset {
            Some(offset) => offset,
            None => fallback(*partition)?,
        };
        result.insert(*partition, offset);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pooled_test_connection() -> Connection {
        Connection {
            id: "conn-1".into(),
            name: "test".into(),
            // Creating a librdkafka client does not connect, so these tests
            // need no broker.
            bootstrap_servers: "localhost:9092".into(),
            kafka_version: "3.7".into(),
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
            allow_publishing: false,
            created_at: "now".into(),
            updated_at: "2026-08-27T00:00:00Z".into(),
        }
    }

    /// The point of the pool: a second request against the same connection
    /// reuses the first one's client — and therefore its already-established,
    /// already-authenticated connection — instead of paying for another
    /// handshake.
    #[test]
    fn a_second_request_for_the_same_connection_reuses_one_client() {
        let kafka = RdKafkaClient::new();
        let connection = pooled_test_connection();

        let first = kafka.metadata_client(&connection).expect("first client");
        let second = kafka.metadata_client(&connection).expect("second client");

        assert!(Arc::ptr_eq(&first.consumer, &second.consumer), "expected one pooled client, got two");
    }

    #[test]
    fn different_connections_get_their_own_clients() {
        let kafka = RdKafkaClient::new();
        let one = pooled_test_connection();
        let mut two = pooled_test_connection();
        two.id = "conn-2".into();

        let first = kafka.metadata_client(&one).expect("first client");
        let second = kafka.metadata_client(&two).expect("second client");

        assert!(!Arc::ptr_eq(&first.consumer, &second.consumer));
    }

    /// Regression test for the pooled-client race: three tree requests
    /// (brokers/topics/consumer groups) share one `ObservedClient`, and
    /// before the error slot was serialized, one request's `begin()` could
    /// clear a reason another concurrent request was mid-read of, or vice
    /// versa — misattributing one request's failure as another's.
    #[test]
    fn a_second_request_waits_for_the_first_to_finish_with_the_error_slot() {
        let client = ObservedClient::create(&client_config(&pooled_test_connection())).expect("client");
        let events: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(std::sync::Barrier::new(2));

        std::thread::scope(|scope| {
            let client_a = client.clone();
            let events_a = Arc::clone(&events);
            let started_a = Arc::clone(&started);
            scope.spawn(move || {
                let _: Result<(), AppError> = client_a.observed("a", |_| {
                    events_a.lock().unwrap().push("a-start");
                    // Only once A owns the slot is B released to contend for
                    // it, so the assertion tests the lock rather than which
                    // thread happened to start first.
                    started_a.wait();
                    std::thread::sleep(Duration::from_millis(20));
                    events_a.lock().unwrap().push("a-end");
                    Ok(())
                });
            });

            let client_b = client.clone();
            let events_b = Arc::clone(&events);
            let started_b = Arc::clone(&started);
            scope.spawn(move || {
                started_b.wait();
                let _: Result<(), AppError> = client_b.observed("b", |_| {
                    events_b.lock().unwrap().push("b-start");
                    Ok(())
                });
            });
        });

        // b-start must never land between a-start and a-end — that ordering
        // is exactly the "read another request's mid-flight state" race.
        assert_eq!(*events.lock().unwrap(), vec!["a-start", "a-end", "b-start"]);
    }

    /// An edited connection must never keep talking to the broker with the
    /// settings the user just replaced — even if nothing thought to release
    /// the old client.
    #[test]
    fn editing_a_connection_retires_the_client_built_from_the_old_settings() {
        let kafka = RdKafkaClient::new();
        let connection = pooled_test_connection();
        let before = kafka.metadata_client(&connection).expect("first client");

        let mut edited = connection.clone();
        edited.bootstrap_servers = "otherhost:9092".into();
        edited.updated_at = "2026-08-27T01:00:00Z".into();
        let after = kafka.metadata_client(&edited).expect("client after the edit");

        assert!(!Arc::ptr_eq(&before.consumer, &after.consumer));
    }

    #[test]
    fn releasing_a_connection_drops_its_pooled_client() {
        let kafka = RdKafkaClient::new();
        let connection = pooled_test_connection();
        let before = kafka.metadata_client(&connection).expect("first client");

        kafka.release(&connection.id);
        let after = kafka.metadata_client(&connection).expect("client after release");

        assert!(!Arc::ptr_eq(&before.consumer, &after.consumer));
    }

    /// The Config tab used to pay a full TCP + TLS + SASL handshake every
    /// time it was opened, because `describe_topic_config` built a fresh
    /// `AdminClient` per call. Clicking through ten topics' Config tabs was
    /// ten handshakes.
    #[test]
    fn an_admin_client_is_pooled_and_reused_across_config_requests() {
        let kafka = RdKafkaClient::new();
        let connection = pooled_test_connection();

        let first = kafka.admin_client(&connection).expect("first admin client");
        let second = kafka.admin_client(&connection).expect("second admin client");

        assert!(Arc::ptr_eq(&first, &second), "expected the pooled admin client, not a rebuilt one");
    }

    #[test]
    fn admin_clients_are_not_shared_between_connections() {
        let kafka = RdKafkaClient::new();
        let one = pooled_test_connection();
        let mut two = pooled_test_connection();
        two.id = "conn-2".into();

        let first = kafka.admin_client(&one).expect("first admin client");
        let second = kafka.admin_client(&two).expect("second admin client");

        assert!(!Arc::ptr_eq(&first, &second));
    }

    /// Same contract as the metadata pool: an edited connection must never
    /// keep talking to the broker with the settings the user just replaced.
    #[test]
    fn editing_a_connection_retires_its_pooled_admin_client() {
        let kafka = RdKafkaClient::new();
        let connection = pooled_test_connection();
        let before = kafka.admin_client(&connection).expect("first admin client");

        let mut edited = connection.clone();
        edited.bootstrap_servers = "otherhost:9092".into();
        edited.updated_at = "2026-08-27T01:00:00Z".into();
        let after = kafka.admin_client(&edited).expect("admin client after the edit");

        assert!(!Arc::ptr_eq(&before, &after));
    }

    /// `release` is called when credentials are rejected, so it has to drop
    /// *both* pools — leaving a rejected admin client behind would keep the
    /// Config tab dialling a broker that has already said no.
    #[test]
    fn releasing_a_connection_drops_its_admin_client_too() {
        let kafka = RdKafkaClient::new();
        let connection = pooled_test_connection();
        let before = kafka.admin_client(&connection).expect("first admin client");

        kafka.release(&connection.id);
        let after = kafka.admin_client(&connection).expect("admin client after release");

        assert!(!Arc::ptr_eq(&before, &after));
    }

    /// The reason `observed` exists: two requests sharing one pooled client
    /// share one error slot, so one's `begin()` could wipe a reason the other
    /// had just captured, and `drain_error_events` could report a reason
    /// belonging to a different request entirely.
    #[test]
    fn observed_sections_never_overlap_on_one_pooled_client() {
        use std::sync::atomic::{AtomicBool, AtomicUsize};

        let kafka = RdKafkaClient::new();
        let client = kafka.metadata_client(&pooled_test_connection()).expect("client");
        let inside = Arc::new(AtomicBool::new(false));
        let overlaps = Arc::new(AtomicUsize::new(0));

        std::thread::scope(|scope| {
            for _ in 0..8 {
                let client = client.clone();
                let inside = Arc::clone(&inside);
                let overlaps = Arc::clone(&overlaps);
                scope.spawn(move || {
                    for _ in 0..50 {
                        let _: Result<(), AppError> = client.observed("test", |_| {
                            if inside.swap(true, Ordering::SeqCst) {
                                overlaps.fetch_add(1, Ordering::SeqCst);
                            }
                            std::thread::yield_now();
                            inside.store(false, Ordering::SeqCst);
                            Ok(())
                        });
                    }
                });
            }
        });

        assert_eq!(overlaps.load(Ordering::SeqCst), 0, "two requests owned the error slot at once");
    }

    /// The invariant that makes narrowing the lock safe.
    ///
    /// The watermark walk is the seconds-long part of `count_topic_messages`,
    /// `list_partitions` and consumer-group lag. It reports failures
    /// per-partition as plain strings rather than through the shared error
    /// slot, so it needs no exclusive access — which is why it can run
    /// outside the `observed` section instead of blocking every other request
    /// on the same connection for its whole duration.
    ///
    /// If someone ever routes it through `begin()`/`failure()`, this test
    /// fails and says why that is not a free change.
    #[test]
    fn the_watermark_walk_does_not_touch_the_shared_error_slot() {
        let kafka = RdKafkaClient::new();
        let client = kafka.metadata_client(&pooled_test_connection()).expect("client");
        client.context.error(KafkaError::Canceled, "a reason from another request");

        // Empty partition list: returns without a broker round trip, but
        // through the same function every real caller uses.
        let found = watermarks_for_partitions(client.consumer.as_ref(), "topic", &[], TEST_READ_TIMEOUT)
            .expect("an empty partition list needs no broker");

        assert!(found.is_empty());
        assert_eq!(
            client.context.last_error().as_deref(),
            Some("a reason from another request"),
            "the watermark walk must not clear or overwrite another request's reason"
        );
    }

    #[test]
    fn a_pooled_client_forgets_an_earlier_requests_failure_reason() {
        // The context outlives the request, so a reason left behind by an
        // earlier failure must not be read as this request's cause.
        let kafka = RdKafkaClient::new();
        let client = kafka.metadata_client(&pooled_test_connection()).expect("client");
        client.context.error(KafkaError::Canceled, "Authentication failed");
        assert!(client.context.last_error().is_some());

        client.begin();

        assert_eq!(client.context.last_error(), None);
    }

    /// The report that started this: a bare `NotImplemented` says nothing
    /// about *why*, and the answer is a compile-time property of the binary,
    /// so the binary should be the one to state it.
    #[test]
    fn a_not_implemented_poll_error_reports_what_this_build_can_decode() {
        let described = describe_poll_error(&KafkaError::MessageConsumption(
            RDKafkaErrorCode::NotImplemented,
        ));

        assert!(
            described.starts_with("Message consumption error"),
            "the original error must survive, got {described:?}"
        );
        assert!(
            described.contains(&crate::build_info::builtin_features()),
            "the codec list librdkafka reports must be in the message, got {described:?}"
        );
    }

    /// Fatal and non-fatal arrive as different variants of the same code, and
    /// a user hitting the fatal one needs the same explanation.
    #[test]
    fn the_fatal_variant_is_explained_the_same_way() {
        let described = describe_poll_error(&KafkaError::MessageConsumptionFatal(
            RDKafkaErrorCode::NotImplemented,
        ));

        assert!(described.contains(&crate::build_info::builtin_features()));
    }

    /// Only `NotImplemented` is about codecs. Annotating every poll failure
    /// with a codec list would bury the actual reason for the common ones.
    #[test]
    fn other_poll_errors_are_left_exactly_as_rdkafka_worded_them() {
        let err = KafkaError::MessageConsumption(RDKafkaErrorCode::UnknownTopicOrPartition);

        assert_eq!(describe_poll_error(&err), err.to_string());
    }

    fn sample_connection() -> Connection {
        Connection {
            id: "1".into(),
            name: "test".into(),
            bootstrap_servers: "127.0.0.1:1".into(),
            kafka_version: "3.7".into(),
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
            allow_publishing: false,
            created_at: "now".into(),
            updated_at: "now".into(),
        }
    }

    #[tokio::test]
    async fn check_status_reports_unreachable_for_a_closed_port_without_creating_a_kafka_client() {
        // Regression test: check_status used to create+destroy a real
        // librdkafka client on every call (via run_probe), and this is the
        // periodic every-10s status-dot poll — one such cycle per saved
        // connection, forever, for as long as the app runs, entirely
        // independent of whether the user ever actually "Connects". That
        // continuous native-client churn was a plausible source of the
        // slow memory growth reported on long-running Windows sessions.
        // check_status must now be a plain TCP check (like ping_bootstrap)
        // and therefore never produce a hard Err for a merely-closed port.
        let client = RdKafkaClient::new();
        let status = client.check_status(&sample_connection()).await.unwrap();
        assert_eq!(status, ConnectionStatus::Unreachable);
    }

    #[tokio::test]
    async fn ping_bootstrap_reports_unreachable_for_a_closed_port() {
        let client = RdKafkaClient::new();
        let status = client.ping_bootstrap("127.0.0.1:1").await.unwrap();
        assert_eq!(status, ConnectionStatus::Unreachable);
    }

    // Reproduces the reported bug: a broker that only accepts TLS/SASL (as
    // virtually all managed cloud Kafka does) still has an open TCP port —
    // it just won't speak plaintext Kafka wire protocol on it. Ping should
    // answer "is something listening", not "can I complete an unauthenticated
    // plaintext Kafka handshake", so it must not depend on rdkafka's
    // fetch_metadata at all. This listener accepts a connection and then
    // does nothing — never sending anything a real Kafka client would
    // recognize — simulating exactly that TLS-only-broker case.
    #[tokio::test]
    async fn ping_bootstrap_reports_reachable_for_a_listener_that_speaks_no_kafka_protocol() {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let client = RdKafkaClient::new();
        let status = client
            .ping_bootstrap(&format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        assert_eq!(status, ConnectionStatus::Reachable);
    }

    #[tokio::test]
    async fn ping_bootstrap_reports_reachable_when_any_of_several_servers_is_up() {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let client = RdKafkaClient::new();
        // First entry (port 1) is closed; second is the live listener above.
        let status = client
            .ping_bootstrap(&format!("127.0.0.1:1,127.0.0.1:{port}"))
            .await
            .unwrap();
        assert_eq!(status, ConnectionStatus::Reachable);
    }

    /// The point of probing concurrently: dead entries in front of the live
    /// one must not each cost their own `TCP_PING_TIMEOUT` before it is
    /// tried. The addresses below are TEST-NET-3 (RFC 5737) — reserved for
    /// documentation and routed nowhere — so on a networked machine each one
    /// hangs for the full timeout. Probed one at a time, as this used to be,
    /// reaching the live listener behind them takes 3 x TCP_PING_TIMEOUT;
    /// probed together it takes one.
    #[tokio::test]
    async fn ping_bootstrap_answers_without_waiting_out_every_dead_server_in_turn() {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let client = RdKafkaClient::new();
        let servers =
            format!("203.0.113.1:9092,203.0.113.2:9092,203.0.113.3:9092,127.0.0.1:{port}");

        let started = std::time::Instant::now();
        let status = client.ping_bootstrap(&servers).await.unwrap();
        let elapsed = started.elapsed();

        assert_eq!(status, ConnectionStatus::Reachable);
        assert!(
            elapsed < TCP_PING_TIMEOUT * 2,
            "took {elapsed:?}, which means the dead servers were probed one after another"
        );
    }

    /// Every probe failing is the only way to `Unreachable` — and the loop
    /// has to end rather than hang once they all have.
    #[tokio::test]
    async fn ping_bootstrap_reports_unreachable_when_every_server_is_closed() {
        let client = RdKafkaClient::new();
        let status = client
            .ping_bootstrap("127.0.0.1:1,127.0.0.1:2,127.0.0.1:3")
            .await
            .unwrap();
        assert_eq!(status, ConnectionStatus::Unreachable);
    }

    /// A bootstrap string with nothing probeable in it (empty, or just
    /// separators) must answer rather than fall through to something else.
    #[tokio::test]
    async fn ping_bootstrap_reports_unreachable_for_a_bootstrap_list_with_no_addresses() {
        let client = RdKafkaClient::new();
        assert_eq!(client.ping_bootstrap("").await.unwrap(), ConnectionStatus::Unreachable);
        assert_eq!(client.ping_bootstrap(" , ,").await.unwrap(), ConnectionStatus::Unreachable);
    }

    #[test]
    fn host_port_strips_the_protocol_prefix_kafka_configs_are_written_with() {
        assert_eq!(host_port("PLAINTEXT://localhost:9092"), "localhost:9092");
        assert_eq!(host_port("SASL_SSL://broker.example.com:9093"), "broker.example.com:9093");
        // Lowercase and unknown schemes alike: anything before "://" is the
        // scheme, because a host name cannot contain it.
        assert_eq!(host_port("ssl://10.0.0.1:9094"), "10.0.0.1:9094");
    }

    #[test]
    fn host_port_leaves_a_plain_host_port_alone() {
        assert_eq!(host_port("localhost:9092"), "localhost:9092");
        assert_eq!(host_port("10.0.0.1:9092"), "10.0.0.1:9092");
        // An IPv6 literal carries no scheme and must survive untouched.
        assert_eq!(host_port("[::1]:9092"), "[::1]:9092");
    }

    /// The bug this fixes: a local broker addressed the way Kafka's own
    /// configs and docker-compose files write it was reported unreachable
    /// even while it was listening — which showed as a red dot on a healthy
    /// cluster and, worse, had `useUnreachableDisconnect` end the session
    /// after two polls.
    #[tokio::test]
    async fn ping_bootstrap_reaches_a_server_written_with_a_protocol_prefix() {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let client = RdKafkaClient::new();
        let status = client
            .ping_bootstrap(&format!("PLAINTEXT://127.0.0.1:{port}"))
            .await
            .unwrap();

        assert_eq!(status, ConnectionStatus::Reachable);
    }

    /// A mixed list is the realistic case for a cluster copied out of a
    /// broker config: some entries carry the prefix, some do not.
    #[tokio::test]
    async fn ping_bootstrap_handles_a_list_mixing_prefixed_and_bare_servers() {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let client = RdKafkaClient::new();
        let status = client
            .ping_bootstrap(&format!("PLAINTEXT://127.0.0.1:1,127.0.0.1:{port}"))
            .await
            .unwrap();

        assert_eq!(status, ConnectionStatus::Reachable);
    }

    /// Binds `count` listeners that count what they accept, for the socket
    /// footprint tests below.
    async fn counting_listeners(count: usize) -> (Vec<String>, Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::atomic::Ordering as AtomicOrdering;
        use tokio::net::TcpListener;

        let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut addresses = Vec::new();
        for _ in 0..count {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addresses.push(listener.local_addr().unwrap().to_string());
            let accepted = Arc::clone(&accepted);
            tokio::spawn(async move {
                while listener.accept().await.is_ok() {
                    accepted.fetch_add(1, AtomicOrdering::Relaxed);
                }
            });
        }
        (addresses, accepted)
    }

    /// The status poll runs for every saved connection, every 10s connected
    /// and every 60s idle, forever. Probing every bootstrap entry when the
    /// first one answers multiplies that by the size of the list — connection
    /// churn brokers meter and firewalls log, for an answer already in hand.
    #[tokio::test]
    async fn a_reachable_cluster_costs_exactly_one_socket_per_poll() {
        use std::sync::atomic::Ordering as AtomicOrdering;

        let (addresses, accepted) = counting_listeners(3).await;
        let client = RdKafkaClient::new();

        let status = client.ping_bootstrap(&addresses.join(",")).await.unwrap();
        // Long enough for any un-staggered fan-out to have landed.
        tokio::time::sleep(Duration::from_millis(400)).await;

        assert_eq!(status, ConnectionStatus::Reachable);
        assert_eq!(
            accepted.load(AtomicOrdering::Relaxed),
            1,
            "a healthy cluster's poll must open one socket, not one per bootstrap entry"
        );
    }

    /// The other half of the trade: when the first entry is *not* answering,
    /// the rest still get tried — quickly — rather than each waiting out its
    /// own timeout in turn.
    #[tokio::test]
    async fn a_dead_first_broker_still_answers_from_the_others_promptly() {
        use std::sync::atomic::Ordering as AtomicOrdering;

        let (addresses, accepted) = counting_listeners(1).await;
        let client = RdKafkaClient::new();
        // Two entries that hang (TEST-NET-3, routed nowhere) ahead of the live one.
        let servers = format!("203.0.113.1:9092,203.0.113.2:9092,{}", addresses[0]);

        let started = std::time::Instant::now();
        let status = client.ping_bootstrap(&servers).await.unwrap();
        let elapsed = started.elapsed();

        assert_eq!(status, ConnectionStatus::Reachable);
        assert_eq!(accepted.load(AtomicOrdering::Relaxed), 1);
        assert!(
            elapsed < TCP_PING_TIMEOUT,
            "took {elapsed:?} — the dead entries were waited out rather than overlapped"
        );
    }

    #[tokio::test]
    async fn ping_bootstrap_reports_unreachable_for_an_unresolvable_host() {
        let client = RdKafkaClient::new();
        let status = client
            .ping_bootstrap("this-host-does-not-resolve.invalid:9092")
            .await
            .unwrap();
        assert_eq!(status, ConnectionStatus::Unreachable);
    }

    /// A closed port must never be mistaken for rejected credentials: the
    /// command layer trips a connection's circuit breaker on
    /// `AppError::Authentication`, so a false positive here would lock a
    /// user out of a cluster that is merely down.
    #[tokio::test]
    async fn test_connection_reports_an_unreachable_port_as_a_kafka_error_not_an_auth_failure() {
        let client = RdKafkaClient::new();
        let result = client
            .test_connection(
                "127.0.0.1:1",
                SecurityProtocol::Plaintext,
                None,
                None,
                None,
                BrokerSslConfig::default(),
            )
            .await;

        let report = result.expect_err("a closed port should fail");
        assert!(
            !matches!(report.current_context(), AppError::Authentication),
            "expected a transport-level error, got {report:?}"
        );
    }

    #[tokio::test]
    async fn test_connection_reports_a_real_error_for_a_closed_port() {
        let client = RdKafkaClient::new();
        let result = client
            .test_connection(
                "127.0.0.1:1",
                SecurityProtocol::Plaintext,
                None,
                None,
                None,
                BrokerSslConfig::default(),
            )
            .await;
        assert!(result.is_err(), "expected a real error, got {result:?}");
    }

    #[tokio::test]
    async fn list_brokers_errors_for_a_closed_port() {
        let client = RdKafkaClient::new();
        let result = client.list_brokers(&sample_connection(), TEST_READ_TIMEOUT).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn list_topics_errors_for_a_closed_port() {
        let client = RdKafkaClient::new();
        let result = client.list_topics(&sample_connection(), TEST_READ_TIMEOUT).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn list_consumer_groups_errors_for_a_closed_port() {
        let client = RdKafkaClient::new();
        let result = client.list_consumer_groups(&sample_connection(), TEST_READ_TIMEOUT).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn count_topic_messages_errors_for_a_closed_port() {
        let client = RdKafkaClient::new();
        let result = client
            .count_topic_messages(&sample_connection(), "orders", TEST_READ_TIMEOUT)
            .await;
        assert!(result.is_err());
    }

    /// Stop has to reach the broker phases too, not just the poll loop.
    ///
    /// Before a single message can be polled, a fetch builds a client, asks
    /// for topic metadata, and queries watermarks for every target partition
    /// — on a wide topic against a slow cluster that is seconds of broker
    /// work, and none of it used to look at the cancellation flag. Pressing
    /// Stop during it did nothing at all until the setup finished.
    ///
    /// Proven without a broker: `sample_connection` points at a closed port,
    /// so a fetch that reaches the metadata call can only fail. Coming back
    /// `Ok` with nothing is therefore proof it stopped before dialling.
    #[tokio::test]
    async fn fetch_messages_already_cancelled_returns_immediately_without_touching_the_broker() {
        let client = RdKafkaClient::new();
        let result = client
            .fetch_messages(
                &sample_connection(),
                "orders",
                &MessageFilter::default(),
                None,
                TEST_READ_TIMEOUT,
                TEST_MAX_MESSAGE_SIZE_BYTES,
                None,
                Arc::new(AtomicBool::new(true)),
            )
            .await;

        let result = result.expect("a cancelled fetch is not a failure");
        assert!(result.messages.is_empty());
        assert_eq!(result.total_matching, 0);
    }

    #[tokio::test]
    async fn fetch_messages_errors_for_a_closed_port() {
        let client = RdKafkaClient::new();
        let result = client
            .fetch_messages(
                &sample_connection(),
                "orders",
                &MessageFilter::default(),
                None,
                TEST_READ_TIMEOUT,
                TEST_MAX_MESSAGE_SIZE_BYTES,
                None,
                Arc::new(AtomicBool::new(false)),
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn fetch_messages_surfaces_the_real_metadata_fetch_failure_reason() {
        // Regression test: `.change_context(AppError::Kafka)` alone silently
        // drops the underlying `KafkaError`'s message (it becomes a
        // non-`Printable` context frame `format_report` never walks), so a
        // metadata-fetch failure used to reach the user as just "failed to
        // fetch metadata for topic orders" with no indication of *why* —
        // useless for diagnosing an intermittent broker/network issue.
        let client = RdKafkaClient::new();
        let report = client
            .fetch_messages(
                &sample_connection(),
                "orders",
                &MessageFilter::default(),
                None,
                TEST_READ_TIMEOUT,
                TEST_MAX_MESSAGE_SIZE_BYTES,
                None,
                Arc::new(AtomicBool::new(false)),
            )
            .await
            .expect_err("expected a metadata-fetch failure against a closed port");
        let printable = printable_attachments(&report).join(" | ").to_lowercase();
        assert!(
            printable.contains("transport") || printable.contains("timed out") || printable.contains("connect"),
            "expected the real librdkafka failure reason in a Printable attachment, got: {printable:?}"
        );
    }

    #[tokio::test]
    async fn fetch_messages_streams_each_message_on_the_given_channel_as_it_arrives() {
        // A closed-port fetch fails before ever polling a message, so this
        // only proves the sender is accepted and the channel closes cleanly
        // (no hang) when the fetch errors out early — full delivery-of-real-
        // messages behavior needs a live broker and isn't covered by this
        // unit test suite (see the `_errors_for_a_closed_port` tests' doc
        // comments for why: no broker fixture in this crate).
        let client = RdKafkaClient::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let result = client
            .fetch_messages(
                &sample_connection(),
                "orders",
                &MessageFilter::default(),
                Some(tx),
                TEST_READ_TIMEOUT,
                TEST_MAX_MESSAGE_SIZE_BYTES,
                None,
                Arc::new(AtomicBool::new(false)),
            )
            .await;
        assert!(result.is_err());
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn list_partitions_errors_for_a_closed_port() {
        let client = RdKafkaClient::new();
        let result = client.list_partitions(&sample_connection(), "orders", TEST_READ_TIMEOUT).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn describe_topic_config_errors_for_a_closed_port() {
        let client = RdKafkaClient::new();
        let result = client.describe_topic_config(&sample_connection(), "orders", TEST_READ_TIMEOUT).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn detect_cluster_version_errors_for_a_closed_port() {
        // A transport failure is an error. An *authorization* refusal is not
        // — that comes back as Ok with MetadataMode::Unknown, which needs a
        // broker to exercise (see tests/cluster_mode_detect.rs).
        let client = RdKafkaClient::new();
        let result = client
            .detect_cluster_version(
                "127.0.0.1:1",
                SecurityProtocol::Plaintext,
                None,
                None,
                None,
                BrokerSslConfig::default(),
                Duration::from_millis(500),
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn fetch_consumer_group_lag_errors_for_a_closed_port() {
        let client = RdKafkaClient::new();
        let result = client
            .fetch_consumer_group_lag(&sample_connection(), "billing-service", TEST_READ_TIMEOUT)
            .await;
        assert!(result.is_err());
    }

    // Mirrors `format_report` in `src-tauri/src/commands/connections.rs` —
    // that's the ONLY thing that actually reaches the frontend
    // (`CommandError.message`), and it walks exclusively `Printable`
    // attachments, ignoring everything else in the report (including
    // whatever `format!("{:?}", report)` would show, which is much more
    // verbose and can make a bug look fixed when it isn't).
    fn printable_attachments(report: &error_stack::Report<AppError>) -> Vec<String> {
        use error_stack::{AttachmentKind, FrameKind};
        report
            .frames()
            .filter_map(|frame| match frame.kind() {
                FrameKind::Attachment(AttachmentKind::Printable(printable)) => Some(printable.to_string()),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn test_connection_surfaces_a_config_error_for_sasl_mechanisms_missing_a_username() {
        // PLAIN/SCRAM mechanisms need sasl.username — if the Authentication
        // tab's Username field was left blank, librdkafka refuses to even
        // build a client, which is surfaced as an error rather than
        // misreported as "unreachable". The user-visible message must
        // contain librdkafka's actual reason, not just the generic "failed
        // to create kafka consumer" wrapper text — that alone isn't
        // actionable for someone staring at an error dialog.
        let client = RdKafkaClient::new();
        let result = client
            .test_connection(
                "127.0.0.1:1",
                SecurityProtocol::SaslPlaintext,
                Some(SaslMechanism::Plain),
                None,
                None,
                BrokerSslConfig::default(),
            )
            .await;
        let report = result.expect_err("expected a config error");
        let printable = printable_attachments(&report).join(" | ").to_lowercase();
        assert!(
            printable.contains("sasl") || printable.contains("username"),
            "expected the real librdkafka reason in a user-visible (Printable) attachment, got: {printable:?}"
        );
    }

    #[tokio::test]
    async fn test_connection_reaches_the_probe_stage_once_a_username_is_given() {
        // Distinguishes this from `..._missing_a_username` above: both now
        // return `Err`, but this one must fail *during the connection
        // attempt* (client creation succeeds), not during config
        // validation — the error message should reflect a probe failure,
        // not "failed to create kafka consumer".
        let client = RdKafkaClient::new();
        let result = client
            .test_connection(
                "127.0.0.1:1",
                SecurityProtocol::SaslPlaintext,
                Some(SaslMechanism::Plain),
                Some("kafka-user"),
                Some("hunter2"),
                BrokerSslConfig::default(),
            )
            .await;
        let err = result.expect_err("expected a real error, but the probe unexpectedly succeeded");
        let message = format!("{err:?}");
        assert!(
            !message.contains("failed to create kafka consumer"),
            "expected a probe-stage failure, but client creation itself failed: {message}"
        );
    }
}

