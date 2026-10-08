use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use error_stack::ResultExt;
use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
use rdkafka::client::{ClientContext, DefaultClientContext};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::groups::{GroupInfo, GroupMemberInfo};
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::{ClientConfig, Message};
use salty_core::Result;
use salty_core::{
    AclAvailability, AclFilter, AclListing, AppError, BrokerSummary, ClusterVersionReport,
    ConfigEntry, Connection, ConnectionStatus, ConsumerGroupLag, ConsumerGroupSummary,
    EncodedRecord, INTER_BROKER_PROTOCOL_VERSION_CONFIG, LIVENESS_GRACE_MS, LivenessTracker,
    MessageFetchResult, MessageFilter, MessageHeader, PROCESS_ROLES_CONFIG, PartitionLag,
    PartitionMessageCount, PartitionSummary, PublishOutcome, STATS_INTERVAL_MS, SaslMechanism,
    SecurityProtocol, TopicMessage, TopicSummary, broker_state_is_up, cluster_version_report,
    messages_in_range,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::assignment::decode_consumer_protocol_assignment;
use crate::config::{BrokerSslConfig, build_client_config, client_config, fetch_consumer_config};
use crate::messages::{
    budgeted_payload_bytes, byte_budget_reached, clamp_offset, combined_start_offset,
    distribute_total_budget, effective_max_messages_per_partition, fetch_shard_count,
    newest_first_start_offset, partition_limits, payload_preview_slice,
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

    let per_thread = partitions
        .len()
        .div_ceil(WATERMARK_LOOKUP_CONCURRENCY)
        .max(1);

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
                    )));
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
    async fn list_brokers(
        &self,
        connection: &Connection,
        read_timeout: Duration,
    ) -> Result<Vec<BrokerSummary>, AppError>;

    /// Backs the tree's "Topics" sub-list once a cluster is connected.
    async fn list_topics(
        &self,
        connection: &Connection,
        read_timeout: Duration,
    ) -> Result<Vec<TopicSummary>, AppError>;

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
    async fn count_topic_messages(
        &self,
        connection: &Connection,
        topic: &str,
        read_timeout: Duration,
    ) -> Result<u64, AppError>;

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

    /// Backs the topic Metrics tab's partition-skew chart when a From/To
    /// window is set: how many messages each partition holds between the two
    /// times. Each bound is resolved to an offset with `offsets_for_times` —
    /// the same resolution the Data tab's From/To filters use, so the two
    /// agree — and the count is the offset distance, never a scan of the
    /// messages. A missing bound means the start or end of the log.
    ///
    /// Approximate by nature: `offsets_for_times` assumes timestamps rise with
    /// offset, which a producer-assigned (CreateTime) timestamp need not.
    async fn count_partition_messages(
        &self,
        connection: &Connection,
        topic: &str,
        from_timestamp_ms: Option<i64>,
        to_timestamp_ms: Option<i64>,
        read_timeout: Duration,
    ) -> Result<Vec<PartitionMessageCount>, AppError>;

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
#[derive(Clone)]
struct ClientErrorContext {
    last_error: Arc<Mutex<Option<String>>>,
    /// What librdkafka's statistics and our own successful requests say about
    /// whether any broker is up — see [`salty_core::LivenessTracker`].
    liveness: Arc<Mutex<LivenessTracker>>,
    /// When a request on this client last got an answer, in [`now_ms`] terms.
    ///
    /// Deliberately separate from `liveness`, which statistics also feed: a
    /// connection that a NAT or load balancer has silently dropped still looks
    /// `UP` in every statistics report, so statistics cannot say whether the
    /// connection has actually been *used* lately. Only a completed request
    /// can, and this is what [`ObservedClient::idle_for_at_least`] reads.
    last_success_ms: Arc<AtomicU64>,
    /// How many statistics reports and errors this client's callbacks have
    /// served. Lets a drain tell "the queue is empty" from "that poll consumed
    /// an event" — `BaseConsumer::poll` returns `None` for both.
    events_served: Arc<AtomicU64>,
}

impl Default for ClientErrorContext {
    fn default() -> Self {
        Self {
            last_error: Arc::default(),
            liveness: Arc::new(Mutex::new(LivenessTracker::new(now_ms()))),
            last_success_ms: Arc::new(AtomicU64::new(now_ms())),
            events_served: Arc::new(AtomicU64::new(0)),
        }
    }
}

/// Wall-clock milliseconds since the Unix epoch — the same scale as
/// librdkafka's statistics `time` field, so both feed one tracker.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
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

    fn note_broker_up(&self, at_ms: u64) {
        if let Ok(mut liveness) = self.liveness.lock() {
            liveness.observe(at_ms, true);
        }
        self.last_success_ms.store(at_ms, Ordering::Relaxed);
    }

    /// Makes the last answered request look `by_ms` older.
    #[cfg(test)]
    fn age_last_success(&self, by_ms: u64) {
        let current = self.last_success_ms.load(Ordering::Relaxed);
        self.last_success_ms
            .store(current.saturating_sub(by_ms), Ordering::Relaxed);
    }
}

impl ClientContext for ClientErrorContext {
    /// Delivered only while the client's queue is being served (see
    /// [`ObservedClient::liveness_status`]), and only when the client was
    /// built with `statistics.interval.ms` — the pooled metadata client is.
    fn stats(&self, statistics: rdkafka::Statistics) {
        self.events_served.fetch_add(1, Ordering::Relaxed);
        let any_up = statistics
            .brokers
            .values()
            .any(|broker| broker_state_is_up(&broker.state));
        if let Ok(mut liveness) = self.liveness.lock() {
            // The report's own timestamp, not the time it was read: a backlog
            // drained in one go must not make an old sighting look fresh.
            liveness.observe((statistics.time.max(0) as u64).saturating_mul(1000), any_up);
        }
    }

    fn error(&self, _error: rdkafka::error::KafkaError, reason: &str) {
        self.events_served.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut last_error) = self.last_error.lock() {
            *last_error = Some(reason.to_string());
        }
    }
}

impl ConsumerContext for ClientErrorContext {}

/// Every consumer in this module: a `BaseConsumer` that reports librdkafka's
/// failure reasons through [`ClientErrorContext`].
type ObservedConsumer = BaseConsumer<ClientErrorContext>;

/// The most events one liveness read will serve. Statistics arrive once per
/// [`salty_core::STATS_INTERVAL_MS`], so even a window left in the background
/// for a long while has a bounded backlog worth clearing.
const DRAIN_EVENT_LIMIT: usize = 4096;

/// Polls in a row that must serve nothing before the queue counts as empty.
/// More than one because librdkafka also queues events no callback counts
/// (log lines), which are consumed silently between reports.
const DRAIN_EMPTY_POLLS_TO_STOP: usize = 4;

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
                Err(failure_report(
                    &err,
                    &reason,
                    "failed to create kafka consumer",
                ))
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
        let _guard = self
            .error_slot
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        self.begin();
        let result = call(&self.consumer).map_err(|err| self.failure(&err, what));
        // A request that got an answer is proof a broker is up, so real
        // traffic keeps the liveness verdict current between statistics.
        if result.is_ok() {
            self.context.note_broker_up(now_ms());
        }
        result
    }

    /// Whether no request on this client has been answered for `duration`.
    ///
    /// A client that has sat unused this long may be on a connection that
    /// something in between has silently dropped — see
    /// [`STALE_PROBE_TIMEOUT`] — so it is checked before a listing relies on it.
    fn idle_for_at_least(&self, duration: Duration) -> bool {
        let last = self.context.last_success_ms.load(Ordering::Relaxed);
        now_ms().saturating_sub(last) >= duration.as_millis() as u64
    }

    /// What this client's own connections say about the cluster, without
    /// sending the broker anything.
    ///
    /// librdkafka reports broker state through the statistics callback, which
    /// rdkafka only invokes while the client's queue is being served — so this
    /// serves it (non-blocking) before reading the verdict. Takes the error
    /// slot for that, for the same reason `observed` does: serving the queue
    /// also runs the error callback.
    fn liveness_status(&self, grace_ms: u64) -> ConnectionStatus {
        // Never waits for the error slot. `observed` holds it for the whole of
        // a request — up to the read timeout — so blocking here would stall the
        // status check exactly when the cluster has stopped answering. If a
        // request is in flight the queue is simply served next time: the
        // reports wait, and the request is itself proof of what it finds.
        let guard = match self.error_slot.try_lock() {
            Ok(guard) => Some(guard),
            Err(std::sync::TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => None,
        };
        if guard.is_some() {
            self.serve_queued_events();
        }
        let now = now_ms();
        self.context
            .liveness
            .lock()
            .map(|liveness| liveness.status(now, grace_ms))
            .unwrap_or(ConnectionStatus::Unknown)
    }

    /// Serves everything queued on the client: statistics (one per interval
    /// while nothing polls) and errors.
    ///
    /// `poll(ZERO)` serves **one** event and returns `None` whether it consumed
    /// a statistics report or found the queue empty — so stopping at the first
    /// `None` served one report per call. With reports every 5 s and a check
    /// every 10 s the backlog then grew by one each time, the verdict was read
    /// off ever-older sightings, and a healthy idle cluster turned `Unreachable`
    /// within 30 s. The callbacks count what they serve, so the drain stops only
    /// after several polls in a row that served nothing.
    fn serve_queued_events(&self) {
        let mut empty_polls = 0;
        for _ in 0..DRAIN_EVENT_LIMIT {
            let served_before = self.context.events_served.load(Ordering::Relaxed);
            let returned_something = self.consumer.poll(Duration::ZERO).is_some();
            let served = self.context.events_served.load(Ordering::Relaxed) != served_before;
            if returned_something || served {
                empty_polls = 0;
            } else {
                empty_polls += 1;
                if empty_polls >= DRAIN_EMPTY_POLLS_TO_STOP {
                    break;
                }
            }
        }
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
        let reason = self
            .context
            .last_error()
            .unwrap_or_else(|| error.to_string());
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
    client.observed("failed to reach the cluster", |consumer| {
        consumer.fetch_metadata(None, timeout)
    })?;
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
    /// When a request or a status check last asked for this client, in
    /// [`now_ms`] terms. What idle expiry measures — see
    /// [`RdKafkaClient::reap_unused_at`].
    last_used_ms: u64,
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
    // `Arc`ed so the idle-expiry task can hold a `Weak` to each pool: it must
    // reach them without keeping them alive after this client is dropped.
    metadata_clients: Arc<Mutex<HashMap<String, PooledClient>>>,
    /// The Config tab's admin clients, pooled for the same reason and on the
    /// same terms as `metadata_clients`.
    ///
    /// A separate map because `AdminClient` is its own rdkafka type and
    /// cannot come out of the consumer pool — which is why this one was
    /// rebuilt per request, and why opening ten topics' Config tabs cost ten
    /// handshakes.
    admin_clients: Arc<Mutex<HashMap<String, PooledAdminClient>>>,
    /// Overrides [`IDLE_CLIENT_TTL`] and [`IDLE_SWEEP_INTERVAL`]; `None` in
    /// the app.
    idle_expiry: Option<(Duration, Duration)>,
    /// Set once the expiry task has been spawned, so it is started exactly
    /// once, on first use, from inside whatever runtime the app is running.
    sweeper_started: AtomicBool,
    /// Overrides [`LIVENESS_GRACE_MS`]; `None` in the app. Exists so a test
    /// can see a client given up on without waiting out the real grace period.
    liveness_grace: Option<Duration>,
}

/// How long a pooled client may go unused before it is closed.
///
/// A connected cluster is asked for by the status poll every few seconds, so
/// this never touches a live session. It exists for the client nobody is
/// asking for: one a late request recreated after the user disconnected,
/// which would otherwise sit there for the life of the app — and, if the
/// broker rejects its credentials, keep re-dialling it every 30 seconds
/// (librdkafka has no "give up" setting).
const IDLE_CLIENT_TTL: Duration = Duration::from_secs(5 * 60);

/// How often the expiry task looks. A fraction of the TTL, so a client is
/// closed soon after it becomes eligible rather than up to a whole TTL late.
const IDLE_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// Builds a pooled metadata client for `connection`.
///
/// Statistics are what let the pooled client report on the cluster without
/// asking it anything — see `check_status`. Set here, not in `client_config`,
/// because that builds every fetch and probe client too, and they are
/// short-lived and never read the report.
fn build_metadata_client(connection: &Connection) -> Result<ObservedClient, AppError> {
    let mut config = client_config(connection);
    config.set("statistics.interval.ms", STATS_INTERVAL_MS.to_string());
    ObservedClient::create(&config)
}

/// Swaps `connection`'s pooled metadata client for a freshly built one and
/// returns it, so a client found to be on a dead connection is not handed to
/// the next request as well.
///
/// The old client is closed after the pool lock is released: closing one
/// tears down its connections and can block.
fn replace_pooled_metadata_client(
    pool: &Mutex<HashMap<String, PooledClient>>,
    connection: &Connection,
) -> Result<ObservedClient, AppError> {
    let client = build_metadata_client(connection)?;
    let replaced = pool.lock().unwrap_or_else(|err| err.into_inner()).insert(
        connection.id.clone(),
        PooledClient {
            updated_at: connection.updated_at.clone(),
            client: client.clone(),
            last_used_ms: now_ms(),
        },
    );
    drop(replaced);
    Ok(client)
}

/// How long the first, cheap question to a pooled client may take before the
/// connection is assumed dead.
///
/// A healthy pooled client answers a single-topic metadata request in about a
/// millisecond — measured at 11–14 ms end to end — so three seconds is a very
/// generous allowance for a live connection and a short wait for a dead one.
/// The alternative is the caller's whole read timeout: a NAT, load balancer or
/// firewall that has forgotten an idle flow gives no sign of it, so the pooled
/// client believes its connection is up and a request on it simply waits.
const STALE_PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// One try against a pooled client, and whether it failed by timing out — the
/// only failure that says the connection itself may be dead.
struct StaleAttempt {
    error: error_stack::Report<AppError>,
    timed_out: bool,
}

/// Runs `attempt` against a pooled client, and if it times out replaces the
/// client and tries once more with the rest of the budget.
///
/// The first try gets only [`STALE_PROBE_TIMEOUT`]; the retry gets whatever is
/// left of `read_timeout`, so the whole call still ends by then and a cluster
/// that is genuinely down is reported no later than it was before. Anything
/// other than a timeout is returned as is: a rejected credential or a missing
/// topic is the cluster answering, and a fresh client would be told the same.
///
/// Generic over the client so the policy is testable without a broker.
fn with_stale_connection_retry<C, T>(
    client: C,
    read_timeout: Duration,
    replace: impl FnOnce() -> Result<C, AppError>,
    mut attempt: impl FnMut(&C, Duration) -> std::result::Result<T, StaleAttempt>,
) -> Result<T, AppError> {
    let probe = STALE_PROBE_TIMEOUT.min(read_timeout);
    match attempt(&client, probe) {
        Ok(value) => Ok(value),
        Err(first) if first.timed_out && read_timeout > probe => {
            let fresh = replace()?;
            attempt(&fresh, read_timeout - probe).map_err(|second| second.error)
        }
        Err(first) => Err(first.error),
    }
}

/// How long a pooled client may go without an answered request before the next
/// listing checks it is still on a live connection.
///
/// Short enough to be well inside the idle timeouts of the usual culprits
/// (NATs, load balancers and firewalls run from tens of seconds to minutes),
/// and cheap to be wrong about: the check is one single-topic metadata request
/// on a connection that is already open.
const STALE_IDLE_AFTER: Duration = Duration::from_secs(20);

/// A topic name no cluster will have. Asking for it is the smallest metadata
/// request there is, and "unknown topic" is an answer — which is all a
/// liveness check needs. `allow.auto.create.topics` is off on every client
/// here, so the request cannot create it.
const LIVENESS_PROBE_TOPIC: &str = "__salty_liveness_probe";

/// Makes sure a pooled client that has been idle is still on a live
/// connection, replacing it if not, before a request that may legitimately take
/// a long time relies on it.
///
/// The fetch path uses its own metadata call as the probe, because that call is
/// always tiny. A listing cannot: all topics on a large cluster can honestly
/// take longer than [`STALE_PROBE_TIMEOUT`], and would be wrongly condemned. So
/// this asks a tiny question first, and only when `idle`.
///
/// A probe that comes back with an *error* (a rejected credential, a refused
/// request) proves the connection is alive, so the client stays and the real
/// request reports its own error. A cluster that fails the probe twice is
/// reported here, within `read_timeout`, rather than after a further full wait.
fn ensure_live_client<C: Clone>(
    client: C,
    idle: bool,
    read_timeout: Duration,
    replace: impl FnOnce() -> Result<C, AppError>,
    mut probe: impl FnMut(&C, Duration) -> std::result::Result<(), StaleAttempt>,
) -> Result<C, AppError> {
    if !idle {
        return Ok(client);
    }
    with_stale_connection_retry(
        client,
        read_timeout,
        replace,
        |candidate, timeout| match probe(candidate, timeout) {
            Ok(()) => Ok(candidate.clone()),
            Err(attempt) if attempt.timed_out => Err(attempt),
            Err(_) => Ok(candidate.clone()),
        },
    )
}

/// See [`RdKafkaClient::reap_unused_at`], which is this over its own pools.
fn reap_pools(
    metadata_clients: &Mutex<HashMap<String, PooledClient>>,
    admin_clients: &Mutex<HashMap<String, PooledAdminClient>>,
    now_ms: u64,
    ttl: Duration,
) -> usize {
    let ttl_ms = ttl.as_millis() as u64;
    let expired = |last_used_ms: u64| now_ms.saturating_sub(last_used_ms) >= ttl_ms;

    // Removed under the lock, dropped after it: closing a librdkafka client can
    // block (it tears down its connections, and an admin client joins its
    // polling thread), and a request wanting its own client must not queue
    // behind that.
    let mut dropped_metadata = Vec::new();
    {
        let mut pool = metadata_clients
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let stale: Vec<String> = pool
            .iter()
            .filter(|(_, pooled)| expired(pooled.last_used_ms))
            .map(|(id, _)| id.clone())
            .collect();
        for id in stale {
            dropped_metadata.extend(pool.remove(&id));
        }
    }
    let mut dropped_admin = Vec::new();
    {
        let mut pool = admin_clients.lock().unwrap_or_else(|err| err.into_inner());
        let stale: Vec<String> = pool
            .iter()
            .filter(|(_, pooled)| expired(pooled.last_used_ms))
            .map(|(id, _)| id.clone())
            .collect();
        for id in stale {
            dropped_admin.extend(pool.remove(&id));
        }
    }
    dropped_metadata.len() + dropped_admin.len()
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
    /// See [`PooledClient::last_used_ms`].
    last_used_ms: u64,
}

impl RdKafkaClient {
    pub fn new() -> Self {
        Self::default()
    }

    /// A client that gives up on a silent cluster after `grace` rather than
    /// [`LIVENESS_GRACE_MS`].
    pub fn with_liveness_grace(mut self, grace: Duration) -> Self {
        self.liveness_grace = Some(grace);
        self
    }

    /// A client that closes unused pooled clients after `ttl`, checking every
    /// `interval`, rather than after [`IDLE_CLIENT_TTL`] / every
    /// [`IDLE_SWEEP_INTERVAL`].
    pub fn with_idle_expiry(mut self, ttl: Duration, interval: Duration) -> Self {
        self.idle_expiry = Some((ttl, interval));
        self
    }

    /// Closes every pooled client — metadata or admin — that has not been
    /// used since `ttl` before `now_ms`, and returns how many went.
    ///
    /// The clients are dropped *after* the pool locks are released: closing a
    /// librdkafka client can block (it tears down its connections, and an
    /// admin client joins its polling thread), and a request wanting its own
    /// client must not queue behind that.
    #[cfg(test)]
    fn reap_unused_at(&self, now_ms: u64, ttl: Duration) -> usize {
        reap_pools(&self.metadata_clients, &self.admin_clients, now_ms, ttl)
    }

    /// Starts the task that applies [`Self::reap_unused_at`] on a timer, the
    /// first time a client is pooled. Does nothing without a tokio runtime
    /// (plain unit tests that pool a client synchronously), and tries again
    /// next time rather than giving up for good.
    ///
    /// The task holds only `Weak` references to the pools, so it ends when the
    /// client that owns them is dropped.
    fn ensure_sweeper(&self) {
        if self.sweeper_started.load(Ordering::Acquire) {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        if self.sweeper_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let (ttl, interval) = self
            .idle_expiry
            .unwrap_or((IDLE_CLIENT_TTL, IDLE_SWEEP_INTERVAL));
        let metadata = Arc::downgrade(&self.metadata_clients);
        let admin = Arc::downgrade(&self.admin_clients);
        runtime.spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let (Some(metadata), Some(admin)) = (metadata.upgrade(), admin.upgrade()) else {
                    return;
                };
                // Closing a client blocks, so keep it off the async workers.
                let _ = tokio::task::spawn_blocking(move || {
                    reap_pools(&metadata, &admin, now_ms(), ttl)
                })
                .await;
            }
        });
    }

    fn liveness_grace_ms(&self) -> u64 {
        self.liveness_grace
            .map_or(LIVENESS_GRACE_MS, |grace| grace.as_millis() as u64)
    }

    /// [`Self::metadata_client`], checked first if it has been idle.
    ///
    /// For the tree's listings, which can take as long as the cluster needs
    /// and so cannot detect a dead connection by timing out themselves. See
    /// [`ensure_live_client`].
    async fn live_metadata_client(
        &self,
        connection: &Connection,
        read_timeout: Duration,
    ) -> Result<ObservedClient, AppError> {
        let client = self.metadata_client(connection)?;
        if !client.idle_for_at_least(STALE_IDLE_AFTER) {
            return Ok(client);
        }
        let pool = Arc::clone(&self.metadata_clients);
        let connection = connection.clone();
        tokio::task::spawn_blocking(move || {
            ensure_live_client(
                client,
                true,
                read_timeout,
                || replace_pooled_metadata_client(&pool, &connection),
                |candidate, timeout| {
                    let mut timed_out = false;
                    candidate
                        .observed("failed to check the connection", |consumer| {
                            let result =
                                consumer.fetch_metadata(Some(LIVENESS_PROBE_TOPIC), timeout);
                            timed_out = matches!(
                                &result,
                                Err(KafkaError::MetadataFetch(
                                    RDKafkaErrorCode::OperationTimedOut
                                ))
                            );
                            result
                        })
                        .map(|_| ())
                        .map_err(|error| StaleAttempt { error, timed_out })
                },
            )
        })
        .await
        .change_context(AppError::Kafka)
        .attach("connection check task panicked")?
    }

    /// This connection's pooled client if there is a current one — and
    /// *only* if: unlike [`Self::metadata_client`] it never builds one, since
    /// a status check must not be what opens a connection to the cluster.
    fn existing_metadata_client(&self, connection: &Connection) -> Option<ObservedClient> {
        let mut pool = self
            .metadata_clients
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        pool.get_mut(&connection.id)
            .filter(|pooled| pooled.updated_at == connection.updated_at)
            .map(|pooled| {
                // Being asked for — by the status poll — is what keeps a
                // connected cluster's client from expiring.
                pooled.last_used_ms = now_ms();
                pooled.client.clone()
            })
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
        let mut pool = self
            .metadata_clients
            .lock()
            .unwrap_or_else(|err| err.into_inner());

        self.ensure_sweeper();
        if let Some(pooled) = pool.get_mut(&connection.id)
            && pooled.updated_at == connection.updated_at
        {
            pooled.last_used_ms = now_ms();
            return Ok(pooled.client.clone());
        }

        let client = build_metadata_client(connection)?;
        pool.insert(
            connection.id.clone(),
            PooledClient {
                updated_at: connection.updated_at.clone(),
                client: client.clone(),
                last_used_ms: now_ms(),
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
    fn admin_client(
        &self,
        connection: &Connection,
    ) -> Result<Arc<AdminClient<DefaultClientContext>>, AppError> {
        let mut pool = self
            .admin_clients
            .lock()
            .unwrap_or_else(|err| err.into_inner());

        self.ensure_sweeper();
        if let Some(pooled) = pool.get_mut(&connection.id)
            && pooled.updated_at == connection.updated_at
        {
            pooled.last_used_ms = now_ms();
            return Ok(Arc::clone(&pooled.client));
        }

        let client: Arc<AdminClient<DefaultClientContext>> =
            Arc::new(client_config(connection).create().map_err(|err| {
                failure_report(
                    &err,
                    &err.to_string(),
                    "failed to create kafka admin client",
                )
            })?);
        pool.insert(
            connection.id.clone(),
            PooledAdminClient {
                updated_at: connection.updated_at.clone(),
                client: Arc::clone(&client),
                last_used_ms: now_ms(),
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
    async fn authorizer_class(
        &self,
        connection: &Connection,
        read_timeout: Duration,
    ) -> Option<String> {
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
        // A connected cluster already has a client holding its connections
        // open; its own account of them answers this for free. Dialling the
        // bootstrap port instead — which is what every status poll used to do
        // — is a fresh socket on the broker per call, per user.
        if let Some(client) = self.existing_metadata_client(connection) {
            let grace_ms = self.liveness_grace_ms();
            return tokio::task::spawn_blocking(move || client.liveness_status(grace_ms))
                .await
                .change_context(AppError::Kafka)
                .attach("liveness check task panicked");
        }
        // No client yet (not connected, or it was just dropped): fall back to
        // the plain TCP check, which is the only thing that can answer.
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
                matches!(
                    timeout(TCP_PING_TIMEOUT, TcpStream::connect(&addr)).await,
                    Ok(Ok(_))
                )
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

    async fn list_brokers(
        &self,
        connection: &Connection,
        read_timeout: Duration,
    ) -> Result<Vec<BrokerSummary>, AppError> {
        let client = self.live_metadata_client(connection, read_timeout).await?;
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

    async fn list_topics(
        &self,
        connection: &Connection,
        read_timeout: Duration,
    ) -> Result<Vec<TopicSummary>, AppError> {
        let client = self.live_metadata_client(connection, read_timeout).await?;
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
        let client = self.live_metadata_client(connection, read_timeout).await?;
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

    async fn count_topic_messages(
        &self,
        connection: &Connection,
        topic: &str,
        read_timeout: Duration,
    ) -> Result<u64, AppError> {
        let client = self.live_metadata_client(connection, read_timeout).await?;
        let topic = topic.to_string();
        tokio::task::spawn_blocking(move || {
            let consumer = Arc::clone(&client.consumer);
            let metadata = client.observed(
                &format!("failed to fetch metadata for topic {topic}"),
                |consumer| consumer.fetch_metadata(Some(&topic), read_timeout),
            )?;
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
            let watermarks =
                watermarks_for_partitions(consumer.as_ref(), &topic, &partitions, read_timeout)?;

            Ok(watermarks
                .values()
                .map(|&(low, high)| (high - low).max(0) as u64)
                .sum())
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
        // What replacing that client takes, should it turn out to be on a dead
        // connection.
        let metadata_pool = Arc::clone(&self.metadata_clients);
        let pooled_connection = connection.clone();
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
            //
            // **A long-lived client can be on a dead connection.** After a
            // while idle, a NAT, load balancer or firewall may have forgotten
            // it without telling either end, and the call below would then
            // wait out the whole read timeout and fail — on the very first
            // click after a pause, and again on the next until librdkafka
            // finally gives up on the socket. So it is asked with a short
            // deadline (it normally takes a millisecond), and a timeout
            // replaces the client and asks once more with the rest of the
            // budget. See [`with_stale_connection_retry`].
            let metadata = with_stale_connection_retry(
                metadata_client,
                read_timeout,
                || replace_pooled_metadata_client(&metadata_pool, &pooled_connection),
                |client, timeout| {
                    let mut timed_out = false;
                    client
                        .observed(
                            &format!("failed to fetch metadata for topic {topic}"),
                            |consumer| {
                                let result = consumer.fetch_metadata(Some(&topic), timeout);
                                timed_out = matches!(
                                    &result,
                                    Err(KafkaError::MetadataFetch(
                                        RDKafkaErrorCode::OperationTimedOut
                                    ))
                                );
                                result
                            },
                        )
                        .map_err(|error| StaleAttempt { error, timed_out })
                },
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
            let watermarks_by_partition = watermarks_for_partitions(
                consumer.as_ref(),
                &topic,
                &target_partitions,
                read_timeout,
            )?;
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
                        .map(|&p| {
                            watermarks(p).map(|(low, high)| (p, clamp_offset(offset, low, high)))
                        })
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
                    resolve_offsets_by_timestamp(
                        &consumer,
                        &topic,
                        &target_partitions,
                        from_ms,
                        read_timeout,
                        |p| watermarks(p).map(|(_, high)| high),
                    )
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
                        let explicit = explicit_start_offsets
                            .as_ref()
                            .and_then(|m| m.get(&p).copied());
                        let from_ts = from_timestamp_start_offsets
                            .as_ref()
                            .and_then(|m| m.get(&p).copied());
                        let start = combined_start_offset(explicit, from_ts).expect(
                            "at least one start-offset source is set for every target partition",
                        );
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
                resolve_offsets_by_timestamp(
                    &consumer,
                    &topic,
                    &target_partitions,
                    to_ms,
                    read_timeout,
                    |p| watermarks(p).map(|(_, high)| high),
                )?
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
                Some(effective_max_messages_per_partition(
                    filter.max_messages_per_partition,
                )),
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

            let working: Vec<i32> = limits
                .iter()
                .filter(|&(_, &limit)| limit > 0)
                .map(|(&partition, _)| partition)
                .collect();

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
                    handles
                        .into_iter()
                        .map(|handle| handle.join().expect("shard builder panicked"))
                        .collect()
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
            let mut collected =
                Vec::with_capacity(total_target.clamp(0, PREALLOCATED_MESSAGES) as usize);
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
            let poll_slice = if consumers.len() == 1 {
                POLL_TIMEOUT
            } else {
                SHARDED_POLL_SLICE
            };
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
                        payload_bytes_read +=
                            budgeted_payload_bytes(payload.len(), filter.include_payload);
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
                            payload_base64: filter.include_payload.then(|| {
                                BASE64.encode(payload_preview_slice(
                                    payload,
                                    filter.max_payload_preview_bytes,
                                ))
                            }),
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
                        stopped_at_byte_budget =
                            byte_budget_reached(payload_bytes_read, max_total_payload_bytes);
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
        let client = self.live_metadata_client(connection, read_timeout).await?;
        let topic = topic.to_string();
        tokio::task::spawn_blocking(move || {
            let consumer = Arc::clone(&client.consumer);
            let metadata = client.observed(
                &format!("failed to fetch metadata for topic {topic}"),
                |consumer| consumer.fetch_metadata(Some(&topic), read_timeout),
            )?;
            let topic_metadata = metadata
                .topics()
                .iter()
                .find(|t| t.name() == topic)
                .ok_or_else(|| error_stack::Report::new(AppError::NotFound))
                .attach_with(|| format!("topic {topic} not found"))?;

            // Outside the `observed` section — see `count_topic_messages`.
            let partition_ids: Vec<i32> =
                topic_metadata.partitions().iter().map(|p| p.id()).collect();
            let watermarks =
                watermarks_for_partitions(consumer.as_ref(), &topic, &partition_ids, read_timeout)?;

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

    async fn count_partition_messages(
        &self,
        connection: &Connection,
        topic: &str,
        from_timestamp_ms: Option<i64>,
        to_timestamp_ms: Option<i64>,
        read_timeout: Duration,
    ) -> Result<Vec<PartitionMessageCount>, AppError> {
        let client = self.live_metadata_client(connection, read_timeout).await?;
        let topic = topic.to_string();
        tokio::task::spawn_blocking(move || {
            let consumer = Arc::clone(&client.consumer);
            let metadata = client.observed(
                &format!("failed to fetch metadata for topic {topic}"),
                |consumer| consumer.fetch_metadata(Some(&topic), read_timeout),
            )?;
            let topic_metadata = metadata
                .topics()
                .iter()
                .find(|t| t.name() == topic)
                .ok_or_else(|| error_stack::Report::new(AppError::NotFound))
                .attach_with(|| format!("topic {topic} not found"))?;

            // Outside the `observed` section — see `count_topic_messages`.
            let partition_ids: Vec<i32> =
                topic_metadata.partitions().iter().map(|p| p.id()).collect();
            let watermarks =
                watermarks_for_partitions(consumer.as_ref(), &topic, &partition_ids, read_timeout)?;
            let high_of = |p: i32| {
                watermarks.get(&p).map(|&(_, high)| high).ok_or_else(|| {
                    error_stack::Report::new(AppError::NotFound)
                        .attach(format!("no such partition: {topic}:{p}"))
                })
            };

            // A From after every message resolves to nothing, so it falls back
            // to the high watermark (an empty window), and a To after every
            // message to the high watermark too (read to the end) — the same
            // fallbacks as the Data tab's fetch, for the same reasons.
            let starts = from_timestamp_ms
                .map(|ms| {
                    resolve_offsets_by_timestamp(
                        &consumer,
                        &topic,
                        &partition_ids,
                        ms,
                        read_timeout,
                        high_of,
                    )
                })
                .transpose()?;
            let ends = to_timestamp_ms
                .map(|ms| {
                    resolve_offsets_by_timestamp(
                        &consumer,
                        &topic,
                        &partition_ids,
                        ms,
                        read_timeout,
                        high_of,
                    )
                })
                .transpose()?;

            let mut counts: Vec<PartitionMessageCount> = partition_ids
                .iter()
                .map(|&partition| {
                    let (low, high) = watermarks.get(&partition).copied().unwrap_or((0, 0));
                    let start = starts
                        .as_ref()
                        .and_then(|m| m.get(&partition))
                        .copied()
                        .unwrap_or(low);
                    let end = ends
                        .as_ref()
                        .and_then(|m| m.get(&partition))
                        .copied()
                        .unwrap_or(high);
                    PartitionMessageCount {
                        partition,
                        messages: messages_in_range(low, high, start, end),
                    }
                })
                .collect();
            counts.sort_by_key(|c| c.partition);
            Ok(counts)
        })
        .await
        .change_context(AppError::Kafka)
        .attach("count_partition_messages task panicked")?
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
        let results = admin
            .describe_configs([&specifier], &options)
            .await
            .map_err(|err| {
                failure_report(
                    &err,
                    &err.to_string(),
                    &format!("failed to describe config for topic {topic}"),
                )
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
            .map(|entry| ConfigEntry {
                name: entry.name,
                value: entry.value,
            })
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

        let admin: AdminClient<DefaultClientContext> = config.create().map_err(|err| {
            failure_report(
                &err,
                &err.to_string(),
                "failed to create kafka admin client",
            )
        })?;
        let options = AdminOptions::new().request_timeout(Some(read_timeout));
        let results = admin
            .describe_configs([&ResourceSpecifier::Broker(broker_id)], &options)
            .await
            .map_err(|err| {
                failure_report(&err, &err.to_string(), "failed to describe broker config")
            })?;

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

        // `and_then`, not `map(... .unwrap_or_default())`: rdkafka reports a
        // `value` of `None` when librdkafka hands back a null pointer, which
        // the wire format uses for configs the broker marks `is_sensitive`
        // and redacts. Defaulting that to `""` would be read as the *empty*
        // `process.roles` a ZooKeeper-mode broker sends, so a redacted value
        // would make Detect claim ZooKeeper on a KRaft cluster. Collapsing it
        // to `None` instead reports `Unknown`, which is the honest answer and
        // the safe direction to be wrong in. Neither config read here is
        // sensitive today, so this is a guard rather than a live fix.
        let value_of = |name: &str| {
            entries
                .iter()
                .find(|entry| entry.name == name)
                .and_then(|entry| entry.value.clone())
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
        let client = self.live_metadata_client(connection, read_timeout).await?;
        let mut group_config = client_config(connection);
        let group_id = group_id.to_string();
        tokio::task::spawn_blocking(move || {
            let consumer = Arc::clone(&client.consumer);
            let groups = client.observed(
                &format!("failed to fetch group list for {group_id}"),
                |consumer| consumer.fetch_group_list(Some(&group_id), read_timeout),
            )?;
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
                                (
                                    member.client_id().to_string(),
                                    member.client_host().to_string(),
                                ),
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
                    group_client.failure(
                        &err,
                        &format!("failed to fetch committed offsets for {group_id}"),
                    )
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
                partitions_by_topic
                    .entry(element.topic().to_string())
                    .or_default()
                    .push(element.partition());
            }
            let mut watermarks: HashMap<(String, i32), (i64, i64)> = HashMap::new();
            for (topic, topic_partitions) in &partitions_by_topic {
                for (partition, marks) in watermarks_for_partitions(
                    consumer.as_ref(),
                    topic,
                    topic_partitions,
                    read_timeout,
                )? {
                    watermarks.insert((topic.clone(), partition), marks);
                }
            }

            let mut partitions = Vec::new();
            for element in committed.elements() {
                let topic = element.topic().to_string();
                let partition = element.partition();
                let current_offset = element.offset().to_raw().filter(|&o| o >= 0);

                let (_low, high) = watermarks
                    .get(&(topic.clone(), partition))
                    .copied()
                    .unwrap_or((0, 0));

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

            Ok(ConsumerGroupLag {
                state: group.state().to_string(),
                partitions,
            })
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

        assert!(
            Arc::ptr_eq(&first.consumer, &second.consumer),
            "expected one pooled client, got two"
        );
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
        let client =
            ObservedClient::create(&client_config(&pooled_test_connection())).expect("client");
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
        let after = kafka
            .metadata_client(&edited)
            .expect("client after the edit");

        assert!(!Arc::ptr_eq(&before.consumer, &after.consumer));
    }

    #[test]
    fn releasing_a_connection_drops_its_pooled_client() {
        let kafka = RdKafkaClient::new();
        let connection = pooled_test_connection();
        let before = kafka.metadata_client(&connection).expect("first client");

        kafka.release(&connection.id);
        let after = kafka
            .metadata_client(&connection)
            .expect("client after release");

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
        let second = kafka
            .admin_client(&connection)
            .expect("second admin client");

        assert!(
            Arc::ptr_eq(&first, &second),
            "expected the pooled admin client, not a rebuilt one"
        );
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
        let after = kafka
            .admin_client(&edited)
            .expect("admin client after the edit");

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
        let after = kafka
            .admin_client(&connection)
            .expect("admin client after release");

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
        let client = kafka
            .metadata_client(&pooled_test_connection())
            .expect("client");
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

        assert_eq!(
            overlaps.load(Ordering::SeqCst),
            0,
            "two requests owned the error slot at once"
        );
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
        let client = kafka
            .metadata_client(&pooled_test_connection())
            .expect("client");
        client
            .context
            .error(KafkaError::Canceled, "a reason from another request");

        // Empty partition list: returns without a broker round trip, but
        // through the same function every real caller uses.
        let found =
            watermarks_for_partitions(client.consumer.as_ref(), "topic", &[], TEST_READ_TIMEOUT)
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
        let client = kafka
            .metadata_client(&pooled_test_connection())
            .expect("client");
        client
            .context
            .error(KafkaError::Canceled, "Authentication failed");
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

    // The pooled client's own view of the cluster answers the status poll, so
    // the poll costs the broker nothing. Proven by the *difference* from the
    // TCP fallback: against a closed port the fallback says Unreachable at
    // once, while a client that has only just been created has not yet had time
    // to connect and must say Unknown.
    #[tokio::test]
    async fn check_status_reads_a_pooled_clients_liveness_instead_of_dialling() {
        let client = RdKafkaClient::new();
        let connection = sample_connection();
        client.metadata_client(&connection).unwrap();

        let status = client.check_status(&connection).await.unwrap();

        assert_eq!(status, ConnectionStatus::Unknown);
    }

    #[tokio::test]
    async fn a_pooled_client_that_never_reaches_a_broker_is_unreachable_after_the_grace_period() {
        let client = RdKafkaClient::new().with_liveness_grace(Duration::from_millis(200));
        let connection = sample_connection();
        client.metadata_client(&connection).unwrap();

        tokio::time::sleep(Duration::from_millis(300)).await;

        assert_eq!(
            client.check_status(&connection).await.unwrap(),
            ConnectionStatus::Unreachable
        );
    }

    // A request that succeeds is proof a broker is up, so real traffic keeps
    // the verdict Reachable without waiting for the next statistics report.
    #[test]
    fn a_successful_request_marks_the_client_reachable() {
        let observed = ObservedClient::create(&client_config(&sample_connection())).unwrap();
        let _ = observed.observed("noop", |_consumer| Ok::<(), KafkaError>(()));
        assert_eq!(
            observed.liveness_status(salty_core::LIVENESS_GRACE_MS),
            ConnectionStatus::Reachable
        );
    }

    #[test]
    fn a_failed_request_does_not_mark_the_client_reachable() {
        let observed = ObservedClient::create(&client_config(&sample_connection())).unwrap();
        let _ = observed.observed("noop", |_consumer| Err::<(), _>(KafkaError::Canceled));
        assert_ne!(
            observed.liveness_status(salty_core::LIVENESS_GRACE_MS),
            ConnectionStatus::Reachable
        );
    }

    #[tokio::test]
    async fn a_connection_with_no_pooled_client_still_falls_back_to_the_tcp_check() {
        let client = RdKafkaClient::new();
        // Nothing pooled for this connection: the old behaviour, untouched.
        assert_eq!(
            client.check_status(&sample_connection()).await.unwrap(),
            ConnectionStatus::Unreachable
        );
    }

    // --- Stale pooled connections ------------------------------------------
    //
    // A NAT, load balancer or firewall can forget an idle flow without telling
    // either end. The pooled client then still believes its connection is up,
    // and a request on it waits out the whole read timeout before failing.

    fn timed_out<T>() -> std::result::Result<T, StaleAttempt> {
        Err(StaleAttempt {
            error: error_stack::Report::new(AppError::Kafka).attach("timed out"),
            timed_out: true,
        })
    }

    #[test]
    fn a_healthy_pooled_client_is_asked_once_and_never_replaced() {
        let mut asked = Vec::new();
        let result = with_stale_connection_retry(
            "old",
            Duration::from_secs(15),
            || panic!("a healthy client must not be replaced"),
            |client, timeout| {
                asked.push((*client, timeout));
                Ok::<_, StaleAttempt>(42)
            },
        );

        assert_eq!(result.unwrap(), 42);
        assert_eq!(asked, vec![("old", STALE_PROBE_TIMEOUT)]);
    }

    #[test]
    fn a_timeout_replaces_the_client_and_retries_within_the_same_overall_budget() {
        let mut asked = Vec::new();
        let result = with_stale_connection_retry(
            "old",
            Duration::from_secs(15),
            || Ok("fresh"),
            |client, timeout| {
                asked.push((*client, timeout));
                if *client == "old" { timed_out() } else { Ok(7) }
            },
        );

        assert_eq!(result.unwrap(), 7);
        // Probe, then the rest of the budget: the whole thing never takes
        // longer than the read timeout the user configured.
        assert_eq!(
            asked,
            vec![
                ("old", STALE_PROBE_TIMEOUT),
                ("fresh", Duration::from_secs(15) - STALE_PROBE_TIMEOUT)
            ]
        );
    }

    #[test]
    fn a_cluster_that_is_really_down_still_fails_after_one_retry() {
        let mut attempts = 0;
        let result = with_stale_connection_retry(
            "old",
            Duration::from_secs(15),
            || Ok("fresh"),
            |_, _| {
                attempts += 1;
                timed_out::<u8>()
            },
        );

        assert!(result.is_err());
        assert_eq!(attempts, 2, "one probe and one retry, not a loop");
    }

    // Only a timeout says the connection may be dead. A refusal or a missing
    // topic is the cluster answering, and a fresh client would answer the same.
    #[test]
    fn a_failure_that_is_not_a_timeout_is_not_retried() {
        let mut attempts = 0;
        let result = with_stale_connection_retry(
            "old",
            Duration::from_secs(15),
            || panic!("only a timeout replaces the client"),
            |_, _| {
                attempts += 1;
                Err::<u8, _>(StaleAttempt {
                    error: error_stack::Report::new(AppError::Authentication).attach("rejected"),
                    timed_out: false,
                })
            },
        );

        assert!(matches!(
            result.unwrap_err().current_context(),
            AppError::Authentication
        ));
        assert_eq!(attempts, 1);
    }

    // With no room for a short probe the single attempt gets the whole budget,
    // exactly as before.
    #[test]
    fn a_read_timeout_no_longer_than_the_probe_is_a_single_attempt() {
        let mut asked = Vec::new();
        let result = with_stale_connection_retry(
            "old",
            STALE_PROBE_TIMEOUT,
            || panic!("nothing to retry with"),
            |_, timeout| {
                asked.push(timeout);
                timed_out::<u8>()
            },
        );

        assert!(result.is_err());
        assert_eq!(asked, vec![STALE_PROBE_TIMEOUT]);
    }

    #[test]
    fn a_failure_to_build_the_replacement_is_reported() {
        let result = with_stale_connection_retry(
            "old",
            Duration::from_secs(15),
            || Err(error_stack::Report::new(AppError::Kafka).attach("cannot build")),
            |_, _| timed_out::<u8>(),
        );

        assert!(result.is_err());
    }

    #[test]
    fn replacing_a_pooled_client_swaps_it_for_everyone_who_asks_next() {
        let kafka = RdKafkaClient::new();
        let connection = pooled_test_connection();
        let stale = kafka.metadata_client(&connection).expect("client");

        let fresh = replace_pooled_metadata_client(&kafka.metadata_clients, &connection)
            .expect("replacement");

        assert!(!Arc::ptr_eq(&stale.consumer, &fresh.consumer));
        let next = kafka.metadata_client(&connection).expect("next request");
        assert!(
            Arc::ptr_eq(&fresh.consumer, &next.consumer),
            "the pool must now hold the replacement"
        );
    }

    // A replacement is a new client for the *same* settings, not a way to
    // resurrect one built from credentials the user has since edited.
    #[test]
    fn a_replacement_is_built_from_the_connection_it_was_asked_for() {
        let kafka = RdKafkaClient::new();
        let mut connection = pooled_test_connection();
        kafka.metadata_client(&connection).expect("client");
        connection.updated_at = "later".into();

        replace_pooled_metadata_client(&kafka.metadata_clients, &connection).expect("replacement");

        assert!(kafka.existing_metadata_client(&connection).is_some());
    }

    // --- Validating an idle pooled client before a listing -------------------
    //
    // The fetch path's own call doubles as the probe. A listing cannot: asking
    // for every topic on a big cluster can legitimately take longer than the
    // probe deadline, so it would wrongly condemn a healthy client. Those get a
    // tiny request first, and only when the client has been idle.

    fn probe_timed_out() -> std::result::Result<(), StaleAttempt> {
        Err(StaleAttempt {
            error: error_stack::Report::new(AppError::Kafka).attach("timed out"),
            timed_out: true,
        })
    }

    #[test]
    fn a_client_used_a_moment_ago_is_not_probed_at_all() {
        let result = ensure_live_client(
            "client",
            false,
            Duration::from_secs(15),
            || panic!("nothing to replace"),
            |_, _| panic!("no probe for a recently used client"),
        );

        assert_eq!(result.unwrap(), "client");
    }

    #[test]
    fn an_idle_client_that_answers_the_probe_is_kept() {
        let result = ensure_live_client(
            "client",
            true,
            Duration::from_secs(15),
            || panic!("a live client must not be replaced"),
            |_, timeout| {
                assert_eq!(timeout, STALE_PROBE_TIMEOUT);
                Ok(())
            },
        );

        assert_eq!(result.unwrap(), "client");
    }

    #[test]
    fn an_idle_client_that_times_out_is_replaced_before_the_real_request() {
        let result = ensure_live_client(
            "old",
            true,
            Duration::from_secs(15),
            || Ok("fresh"),
            |client, _| {
                if *client == "old" {
                    probe_timed_out()
                } else {
                    Ok(())
                }
            },
        );

        assert_eq!(result.unwrap(), "fresh");
    }

    // A really-down cluster must still be reported within the read timeout, not
    // after a probe, a retry *and* the listing's own full wait.
    #[test]
    fn a_cluster_that_fails_the_probe_twice_is_an_error_not_a_second_full_wait() {
        let result = ensure_live_client(
            "old",
            true,
            Duration::from_secs(15),
            || Ok("fresh"),
            |_, _| probe_timed_out(),
        );

        assert!(result.is_err());
    }

    // The probe answered, just not with good news (a rejected credential, a
    // refused metadata request). The connection is alive; let the real request
    // report its own error rather than this one.
    #[test]
    fn a_probe_that_gets_an_error_answer_leaves_the_client_in_place() {
        let result = ensure_live_client(
            "client",
            true,
            Duration::from_secs(15),
            || panic!("the connection is alive, so it must not be replaced"),
            |_, _| {
                Err(StaleAttempt {
                    error: error_stack::Report::new(AppError::Authentication).attach("rejected"),
                    timed_out: false,
                })
            },
        );

        assert_eq!(result.unwrap(), "client");
    }

    #[test]
    fn a_new_client_has_not_been_idle() {
        let observed = ObservedClient::create(&client_config(&sample_connection())).unwrap();
        assert!(!observed.idle_for_at_least(Duration::from_secs(20)));
    }

    #[test]
    fn a_client_unused_for_a_while_is_idle() {
        let observed = ObservedClient::create(&client_config(&sample_connection())).unwrap();
        observed.context.age_last_success(60_000);
        assert!(observed.idle_for_at_least(Duration::from_secs(20)));
    }

    #[test]
    fn a_successful_request_resets_the_idle_clock() {
        let observed = ObservedClient::create(&client_config(&sample_connection())).unwrap();
        observed.context.age_last_success(60_000);
        let _ = observed.observed("noop", |_| Ok::<(), KafkaError>(()));
        assert!(!observed.idle_for_at_least(Duration::from_secs(20)));
    }

    // The status poll and statistics must NOT count: a connection a NAT has
    // silently dropped still looks up in both.
    #[test]
    fn a_failed_request_does_not_reset_the_idle_clock() {
        let observed = ObservedClient::create(&client_config(&sample_connection())).unwrap();
        observed.context.age_last_success(60_000);
        let _ = observed.observed("noop", |_| Err::<(), _>(KafkaError::Canceled));
        assert!(observed.idle_for_at_least(Duration::from_secs(20)));
    }

    // The status check is meant to be instant and free. `observed` holds the
    // error slot for the whole of a request — up to the read timeout — so a
    // check that waited for it would stall exactly when the cluster is down.
    #[test]
    fn the_liveness_read_does_not_wait_for_a_request_in_flight() {
        let observed = ObservedClient::create(&client_config(&sample_connection())).unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();

        let holder = {
            let observed = observed.clone();
            std::thread::spawn(move || {
                let _: Result<(), AppError> = observed.observed("slow request", |_| {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(())
                });
            })
        };
        started_rx.recv().unwrap();

        let began = std::time::Instant::now();
        let _ = observed.liveness_status(salty_core::LIVENESS_GRACE_MS);
        let waited = began.elapsed();

        release_tx.send(()).unwrap();
        holder.join().unwrap();
        assert!(
            waited < Duration::from_millis(500),
            "the status read blocked for {waited:?}"
        );
    }

    // --- Idle expiry of pooled clients -------------------------------------
    //
    // A pooled client nobody touches is closed. A connected cluster is touched
    // by the status poll every few seconds, so it stays; a client left behind
    // after a disconnect (a late request recreating it) is never asked for
    // again and goes — which matters because librdkafka retries rejected
    // credentials in the background for as long as the client lives.

    impl RdKafkaClient {
        /// Makes every pooled metadata client look `by_ms` older.
        fn age_pooled_clients(&self, by_ms: u64) {
            for pooled in self.metadata_clients.lock().unwrap().values_mut() {
                pooled.last_used_ms = pooled.last_used_ms.saturating_sub(by_ms);
            }
        }

        fn age_pooled_admin_clients(&self, by_ms: u64) {
            for pooled in self.admin_clients.lock().unwrap().values_mut() {
                pooled.last_used_ms = pooled.last_used_ms.saturating_sub(by_ms);
            }
        }
    }

    const TEN_MINUTES_MS: u64 = 10 * 60 * 1000;
    const FIVE_MINUTES: Duration = Duration::from_secs(5 * 60);

    #[test]
    fn a_pooled_client_nobody_has_used_for_the_ttl_is_closed() {
        let kafka = RdKafkaClient::new();
        kafka
            .metadata_client(&pooled_test_connection())
            .expect("client");

        let removed = kafka.reap_unused_at(now_ms() + TEN_MINUTES_MS, FIVE_MINUTES);

        assert_eq!(removed, 1);
        assert!(
            kafka
                .existing_metadata_client(&pooled_test_connection())
                .is_none()
        );
    }

    #[test]
    fn a_recently_used_pooled_client_is_kept() {
        let kafka = RdKafkaClient::new();
        kafka
            .metadata_client(&pooled_test_connection())
            .expect("client");

        let removed = kafka.reap_unused_at(now_ms() + 1_000, FIVE_MINUTES);

        assert_eq!(removed, 0);
        assert!(
            kafka
                .existing_metadata_client(&pooled_test_connection())
                .is_some()
        );
    }

    // The status poll is how a connected cluster says "I am still wanted".
    #[tokio::test]
    async fn a_status_check_counts_as_use() {
        let kafka = RdKafkaClient::new();
        let connection = pooled_test_connection();
        kafka.metadata_client(&connection).expect("client");
        // Age the entry, then touch it the way the poll does.
        kafka.age_pooled_clients(TEN_MINUTES_MS);
        kafka.check_status(&connection).await.unwrap();

        assert_eq!(kafka.reap_unused_at(now_ms(), FIVE_MINUTES), 0);
    }

    #[test]
    fn a_request_counts_as_use() {
        let kafka = RdKafkaClient::new();
        let connection = pooled_test_connection();
        kafka.metadata_client(&connection).expect("client");
        kafka.age_pooled_clients(TEN_MINUTES_MS);
        kafka.metadata_client(&connection).expect("client again");

        assert_eq!(kafka.reap_unused_at(now_ms(), FIVE_MINUTES), 0);
    }

    // The admin client is created on demand for the Config and ACL tabs and is
    // not polled, so it expires on its own clock rather than riding on the
    // metadata client's.
    #[test]
    fn the_admin_client_expires_independently() {
        let kafka = RdKafkaClient::new();
        let connection = pooled_test_connection();
        kafka.metadata_client(&connection).expect("client");
        kafka.admin_client(&connection).expect("admin client");
        kafka.age_pooled_admin_clients(TEN_MINUTES_MS);

        let removed = kafka.reap_unused_at(now_ms(), FIVE_MINUTES);

        assert_eq!(removed, 1);
        assert!(kafka.existing_metadata_client(&connection).is_some());
        assert!(kafka.admin_clients.lock().unwrap().is_empty());
    }

    // Expiry must not strand the next request: it rebuilds lazily, exactly as
    // after a release.
    #[test]
    fn a_request_after_expiry_builds_a_fresh_client() {
        let kafka = RdKafkaClient::new();
        let connection = pooled_test_connection();
        let before = kafka.metadata_client(&connection).expect("client");
        kafka.reap_unused_at(now_ms() + TEN_MINUTES_MS, FIVE_MINUTES);

        let after = kafka
            .metadata_client(&connection)
            .expect("client after expiry");

        assert!(!Arc::ptr_eq(&before.consumer, &after.consumer));
    }

    #[tokio::test]
    async fn the_background_task_closes_an_abandoned_client_by_itself() {
        let kafka = RdKafkaClient::new()
            .with_idle_expiry(Duration::from_millis(150), Duration::from_millis(40));
        kafka
            .metadata_client(&pooled_test_connection())
            .expect("client");
        assert!(
            kafka
                .existing_metadata_client(&pooled_test_connection())
                .is_some()
        );

        tokio::time::sleep(Duration::from_millis(600)).await;

        assert!(kafka.metadata_clients.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn the_background_task_leaves_a_client_that_keeps_being_used() {
        let kafka = RdKafkaClient::new()
            .with_idle_expiry(Duration::from_millis(300), Duration::from_millis(40));
        let connection = pooled_test_connection();
        kafka.metadata_client(&connection).expect("client");

        for _ in 0..8 {
            tokio::time::sleep(Duration::from_millis(80)).await;
            kafka.check_status(&connection).await.unwrap();
        }

        assert!(!kafka.metadata_clients.lock().unwrap().is_empty());
    }

    // Without this the task would keep the pools alive for the life of the
    // process after the client that owns them is gone.
    #[tokio::test]
    async fn the_background_task_stops_when_the_client_is_dropped() {
        let kafka = RdKafkaClient::new()
            .with_idle_expiry(Duration::from_secs(60), Duration::from_millis(20));
        kafka
            .metadata_client(&pooled_test_connection())
            .expect("client");
        let pool = Arc::downgrade(&kafka.metadata_clients);
        drop(kafka);

        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(pool.upgrade().is_none());
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
        assert_eq!(
            client.ping_bootstrap("").await.unwrap(),
            ConnectionStatus::Unreachable
        );
        assert_eq!(
            client.ping_bootstrap(" , ,").await.unwrap(),
            ConnectionStatus::Unreachable
        );
    }

    #[test]
    fn host_port_strips_the_protocol_prefix_kafka_configs_are_written_with() {
        assert_eq!(host_port("PLAINTEXT://localhost:9092"), "localhost:9092");
        assert_eq!(
            host_port("SASL_SSL://broker.example.com:9093"),
            "broker.example.com:9093"
        );
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
    async fn counting_listeners(
        count: usize,
    ) -> (Vec<String>, Arc<std::sync::atomic::AtomicUsize>) {
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
        let result = client
            .list_brokers(&sample_connection(), TEST_READ_TIMEOUT)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn list_topics_errors_for_a_closed_port() {
        let client = RdKafkaClient::new();
        let result = client
            .list_topics(&sample_connection(), TEST_READ_TIMEOUT)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn list_consumer_groups_errors_for_a_closed_port() {
        let client = RdKafkaClient::new();
        let result = client
            .list_consumer_groups(&sample_connection(), TEST_READ_TIMEOUT)
            .await;
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
            printable.contains("transport")
                || printable.contains("timed out")
                || printable.contains("connect"),
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
        let result = client
            .list_partitions(&sample_connection(), "orders", TEST_READ_TIMEOUT)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn count_partition_messages_errors_for_a_closed_port() {
        let client = RdKafkaClient::new();
        let result = client
            .count_partition_messages(
                &sample_connection(),
                "orders",
                Some(1),
                Some(2),
                TEST_READ_TIMEOUT,
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn describe_topic_config_errors_for_a_closed_port() {
        let client = RdKafkaClient::new();
        let result = client
            .describe_topic_config(&sample_connection(), "orders", TEST_READ_TIMEOUT)
            .await;
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
                FrameKind::Attachment(AttachmentKind::Printable(printable)) => {
                    Some(printable.to_string())
                }
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
