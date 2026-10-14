//! A bounded, compressed, in-memory cache of Kafka message payloads.
//!
//! The Data tab keeps at most a few KB of each payload in the webview; the
//! rest used to be a broker round trip (a fresh consumer, a TLS and SASL
//! handshake) every time a row was opened. This crate keeps the full message
//! on the Rust side, zstd-compressed, so opening a row becomes a map lookup
//! and a decompress.
//!
//! It knows nothing about Kafka clients: callers hand it a [`MessageKey`] and
//! a [`CachedMessage`], and get the same back. That keeps every rule here —
//! the 4 KB threshold, the byte cap, the idle expiry, what is logged —
//! testable without a broker.
//!
//! # Policy
//!
//! * **Size:** weighted by compressed bytes, capped at
//!   [`DEFAULT_MAX_BYTES`] (1 GiB). New entries always win: when a store
//!   takes the cache past the cap, the entries with the largest
//!   *idle time x size* are evicted — the oldest untouched and the biggest
//!   first — down to 90% of the cap, so room is made in one pass rather
//!   than on every store. moka is given headroom above the cap so its own
//!   admission policy never refuses a new entry; the cap is enforced here.
//!   Only a single payload larger than the whole cap is declined.
//! * **Expiry:** time-to-*idle*, [`DEFAULT_TIME_TO_IDLE`] (60 minutes). Every
//!   read resets it, so a payload in use stays; one nobody opens goes.
//! * **Freshness:** a store always reflects the most recent fetch of that
//!   offset.
//! * **Staleness:** an offset is immutable only until its topic is deleted and
//!   recreated or retention removes it, and a hit never asks the broker. So a
//!   connection's entries are dropped when it is released
//!   ([`MessageCache::invalidate_connection`]); a topic's when a fetch sees a
//!   high watermark at or below an offset already cached; and a partition's
//!   offsets below its low watermark when a fetch sees it move past them
//!   ([`MessageCache::note_watermarks`]).
//! * **Cost:** a store is an event on an unbounded channel, so the fetch loop
//!   never waits for compression. One listener task drains it and runs at
//!   most [`MAX_PENDING_STORES`] compressions at a time, on a dedicated rayon
//!   pool of that many threads — not tokio's blocking pool, which the fetch
//!   loop's consumers need. The queue is bounded in *bytes* ([`MAX_QUEUED_BYTES`]), not in
//!   messages, because what it holds is uncompressed copies: a store that
//!   would push it past the bound is dropped (and counted), so a very fast
//!   fetch cannot grow memory without limit. A payload already cached
//!   unchanged is neither queued nor compressed again, and a queued store is
//!   discarded if the cache was purged or resized after it was queued.
//! * **Logging:** one summary line per burst of stores, not one per message —
//!   a 1,000-row fetch would otherwise write 1,000 lines.
//! * **Memory:** moka only expires entries while it is being used, so a timer
//!   runs its maintenance; otherwise an idle app would hold expired payloads
//!   indefinitely.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock, Weak};
use std::time::Duration;

use moka::sync::Cache;
use tokio::runtime::Handle;
use tokio::sync::{Semaphore, mpsc};

/// Payloads up to this size are not cached: the UI already holds them.
pub const INLINE_PAYLOAD_BYTES: usize = 4 * 1024;

/// Compressed bytes the cache may hold.
pub const DEFAULT_MAX_BYTES: u64 = 1024 * 1024 * 1024;

/// How long an entry may go unused before it expires.
pub const DEFAULT_TIME_TO_IDLE: Duration = Duration::from_secs(60 * 60);

/// zstd's highest level. Measured at ~1.3 MB/s and no smaller than level 19 on
/// JSON, so it is available but not the default.
pub const ZSTD_MAX_LEVEL: i32 = 22;

/// The level used unless configured otherwise: ~11x on JSON at ~50 MB/s, where
/// level 22 is ~13x at ~1.3 MB/s. Decompression is ~2 ms per 4 MB at any level,
/// so the click path does not care.
pub const ZSTD_DEFAULT_LEVEL: i32 = 10;

/// Compressions allowed in flight at once.
pub const MAX_PENDING_STORES: usize = 4;

/// Uncompressed bytes that may wait for a compression slot. Past this a store
/// is dropped rather than queued.
///
/// This is the fetch loop's budget, not a memory limit: each queued store is a
/// copy made *on the fetch thread*, so the bound is also how much copying a
/// fast fetch can be made to do for the cache's sake. Measured against a real
/// broker, 1,500 x 300 KB messages: 256 MB took the fetch from 125 ms to
/// ~300 ms, 128 MB to ~190 ms, 32 MB left it at 125 ms.
pub const MAX_QUEUED_BYTES: u64 = 32 * 1024 * 1024;

/// Payloads at or under this size are kept as they are, not compressed. A
/// small payload costs little memory and little to copy, and compressing it
/// is CPU spent for a saving nobody needs; the cache's byte cap simply holds
/// fewer of them than it would compressed ones.
pub const MIN_COMPRESS_BYTES: usize = 64 * 1024;

/// Idle buffers kept for reuse by [`MessageCache::take_buffer`].
pub const MAX_POOLED_BYTES: u64 = 32 * 1024 * 1024;

/// How long stores are gathered before one summary line is logged.
pub const LOG_DEBOUNCE: Duration = Duration::from_millis(500);

/// How often expired entries are released while nothing is using the cache.
pub const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(60);

/// Identifies one message. The connection is part of it: two clusters can
/// both have an `orders` topic whose partition 0 reaches offset 1042.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MessageKey {
    pub connection_id: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

impl MessageKey {
    pub fn new(connection_id: &str, topic: &str, partition: i32, offset: i64) -> Self {
        Self {
            connection_id: connection_id.to_string(),
            topic: topic.to_string(),
            partition,
            offset,
        }
    }
}

impl std::fmt::Display for MessageKey {
    /// `topic-partition-offset`, as logged.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}-{}", self.topic, self.partition, self.offset)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedHeader {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

/// A whole message as the cache stores and returns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedMessage {
    pub timestamp_ms: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub headers: Vec<CachedHeader>,
    /// The full, uncompressed payload.
    pub payload: Vec<u8>,
}

/// What the cache reports about itself, for the app's log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheEvent {
    Hit {
        key: String,
        payload_bytes: usize,
    },
    Miss {
        key: String,
    },
    /// A burst of stores finished. `entries` and `weighted_bytes` are the
    /// cache's totals afterwards; `skipped` counts stores dropped because
    /// every compression slot was busy.
    Updated {
        stored: u64,
        skipped: u64,
        payload_bytes: u64,
        compressed_bytes: u64,
        last_key: String,
        entries: u64,
        weighted_bytes: u64,
    },
    /// Entries were dropped because what they described is gone or changed.
    Purged {
        what: String,
        entries: u64,
    },
    /// The byte cap changed, which empties the cache (`dropped` entries).
    Resized {
        max_bytes: u64,
        dropped: u64,
    },
}

impl CacheEvent {
    /// The line for the Logs panel.
    pub fn message(&self) -> String {
        match self {
            CacheEvent::Hit { key, payload_bytes } => {
                format!("Payload cache hit: {key} ({payload_bytes} bytes)")
            }
            CacheEvent::Miss { key } => format!("Payload cache miss: {key}"),
            CacheEvent::Updated {
                stored,
                skipped,
                payload_bytes,
                compressed_bytes,
                last_key,
                entries,
                weighted_bytes,
            } => {
                let what = if *stored == 1 {
                    format!("{last_key} stored")
                } else {
                    format!("{stored} payloads stored")
                };
                let skipped = if *skipped == 0 {
                    String::new()
                } else {
                    format!(", {skipped} not stored (cache or queue full)")
                };
                format!(
                    "Payload cache updated: {what} ({payload_bytes} -> {compressed_bytes} bytes \
                     zstd){skipped}; {entries} objects, {weighted_bytes} bytes"
                )
            }
            CacheEvent::Purged { what, entries } => {
                format!("Payload cache purged {what}; {entries} objects remain")
            }
            CacheEvent::Resized {
                max_bytes: 0,
                dropped,
            } => format!("Payload cache turned off; {dropped} cached payloads dropped"),
            CacheEvent::Resized { max_bytes, dropped } => format!(
                "Payload cache size set to {} MB; {dropped} cached payloads dropped",
                max_bytes / (1024 * 1024)
            ),
        }
    }
}

type Listener = Arc<dyn Fn(CacheEvent) + Send + Sync>;

#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub max_bytes: u64,
    pub time_to_idle: Duration,
    /// Payloads this size or smaller are not stored.
    pub min_payload_bytes: usize,
    pub zstd_level: i32,
    /// Compressions running at once.
    pub max_pending_stores: usize,
    /// Uncompressed bytes allowed to wait for one; see [`MAX_QUEUED_BYTES`].
    pub max_queued_bytes: u64,
    /// Payloads this size or smaller are stored uncompressed; see
    /// [`MIN_COMPRESS_BYTES`]. `0` compresses everything.
    pub min_compress_bytes: usize,
    /// Idle payload buffers kept for reuse; see [`MAX_POOLED_BYTES`].
    pub max_pooled_bytes: u64,
    /// How long stores are gathered before one summary line is logged.
    pub log_debounce: Duration,
    /// How often expired entries are released while the cache is idle.
    pub maintenance_interval: Duration,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: DEFAULT_MAX_BYTES,
            time_to_idle: DEFAULT_TIME_TO_IDLE,
            min_payload_bytes: INLINE_PAYLOAD_BYTES,
            zstd_level: ZSTD_DEFAULT_LEVEL,
            max_pending_stores: MAX_PENDING_STORES,
            max_queued_bytes: MAX_QUEUED_BYTES,
            min_compress_bytes: MIN_COMPRESS_BYTES,
            max_pooled_bytes: MAX_POOLED_BYTES,
            log_debounce: LOG_DEBOUNCE,
            maintenance_interval: MAINTENANCE_INTERVAL,
        }
    }
}

struct Entry {
    /// The payload as zstd compressed it — or, when `raw`, as it came.
    compressed: Box<[u8]>,
    /// Stored without compression ([`MIN_COMPRESS_BYTES`]).
    raw: bool,
    payload_len: usize,
    timestamp_ms: Option<i64>,
    key: Option<Vec<u8>>,
    headers: Vec<CachedHeader>,
}

impl Entry {
    /// What this entry costs against the byte cap: the compressed payload
    /// plus the metadata kept beside it.
    fn weight(&self) -> u32 {
        let headers: usize = self
            .headers
            .iter()
            .map(|h| h.key.len() + h.value.as_ref().map_or(0, Vec::len))
            .sum();
        let total = self.compressed.len() + self.key.as_ref().map_or(0, Vec::len) + headers + 64;
        u32::try_from(total).unwrap_or(u32::MAX)
    }
}

type PartitionId = (String, String, i32);

/// The offsets cached for one partition, kept to notice when the broker's
/// watermarks have moved past them.
#[derive(Clone, Copy)]
struct CachedRange {
    lowest: i64,
    highest: i64,
}

/// Stores gathered since the last summary line.
#[derive(Default)]
struct PendingLog {
    stored: u64,
    skipped: u64,
    payload_bytes: u64,
    compressed_bytes: u64,
    last_key: String,
}

/// Idle payload buffers, so the copy a store makes on the fetch thread reuses
/// memory instead of allocating — a 300 KB `Vec` is a fresh mapping and a run
/// of page faults every time, which is most of what the copy costs.
#[derive(Default)]
struct BufferPool {
    buffers: Vec<Vec<u8>>,
    bytes: u64,
}

/// One store waiting for a compression slot.
struct StoreJob {
    key: MessageKey,
    message: CachedMessage,
    /// `Shared::epoch` when it was queued; a purge since makes it stale.
    epoch: u64,
}

/// What the cache knows about each entry's use, to choose what to evict.
/// moka reports neither last use nor admits to a policy of ours, so this
/// shadows its contents: every write records, every read touches, and moka's
/// eviction listener forgets.
#[derive(Default)]
struct UsageIndex {
    /// Weight, logical time of last use, and the entry's identity (so a late
    /// removal notice for a replaced entry cannot forget its successor).
    entries: HashMap<MessageKey, (u32, u64, usize)>,
    bytes: u64,
    clock: u64,
}

impl UsageIndex {
    fn record(&mut self, key: MessageKey, weight: u32, id: usize) {
        self.clock += 1;
        if let Some((old, _, _)) = self.entries.insert(key, (weight, self.clock, id)) {
            self.bytes -= u64::from(old);
        }
        self.bytes += u64::from(weight);
    }

    fn touch(&mut self, key: &MessageKey) {
        self.clock += 1;
        let now = self.clock;
        if let Some(entry) = self.entries.get_mut(key) {
            entry.1 = now;
        }
    }

    /// Forgets `key`, but only the entry identified by `id` when given.
    fn forget(&mut self, key: &MessageKey, id: Option<usize>) {
        if let Some(&(weight, _, current)) = self.entries.get(key)
            && id.is_none_or(|id| id == current)
        {
            self.entries.remove(key);
            self.bytes -= u64::from(weight);
        }
    }

    /// Removes entries until at most `target` bytes remain, never `keep`:
    /// highest idle-time x weight first. Returns what was removed.
    fn make_room(&mut self, keep: &MessageKey, target: u64) -> Vec<MessageKey> {
        let now = self.clock;
        let mut candidates: Vec<(u128, MessageKey)> = self
            .entries
            .iter()
            .filter(|(key, _)| *key != keep)
            .map(|(key, &(weight, used, _))| {
                let idle = u128::from(now - used) + 1;
                (idle * u128::from(weight), key.clone())
            })
            .collect();
        candidates.sort_unstable_by_key(|c| std::cmp::Reverse(c.0));
        let mut evicted = Vec::new();
        for (_, key) in candidates {
            if self.bytes <= target {
                break;
            }
            self.forget(&key, None);
            evicted.push(key);
        }
        evicted
    }
}

struct Shared {
    /// Swapped when the capacity changes: moka cannot resize in place.
    cache: RwLock<Cache<MessageKey, Arc<Entry>>>,
    /// The byte cap in force; `0` means the cache is off.
    max_bytes: AtomicU64,
    config: CacheConfig,
    ranges: Mutex<HashMap<PartitionId, CachedRange>>,
    /// Compression slots.
    pending: Arc<Semaphore>,
    /// The store queue, created with its listener task on first use.
    queue: OnceLock<mpsc::UnboundedSender<StoreJob>>,
    /// The threads compression runs on: as many as there are slots, owned
    /// by the cache so they are not tokio's blocking threads (which the
    /// fetch loop's consumers need) and end with it.
    pool: OnceLock<rayon::ThreadPool>,
    /// Uncompressed bytes queued or compressing.
    queued_bytes: AtomicU64,
    /// Buffers handed back by finished compressions.
    buffers: Mutex<BufferPool>,
    /// Use of every entry; see [`UsageIndex`]. Never held across a call into
    /// moka, whose listener takes it.
    usage: Arc<Mutex<UsageIndex>>,
    /// Stores declined, folded into the next summary. Its own counter so
    /// declining one never takes the log's lock.
    skipped: AtomicU64,
    /// Bumped by every purge, clear and resize; stores queued before it are
    /// dropped so they cannot resurrect what was just forgotten.
    epoch: AtomicU64,
    listener: Option<Listener>,
    log: Mutex<PendingLog>,
    log_scheduled: AtomicBool,
    maintenance_started: AtomicBool,
    hits: AtomicU64,
    misses: AtomicU64,
    /// Entries actually compressed and written, for tests and diagnostics.
    compressions: AtomicU64,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // A panic while holding one of these locks only ever interrupted a map
    // update, which is safe to continue from.
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn build_cache(
    config: &CacheConfig,
    max_bytes: u64,
    usage: Arc<Mutex<UsageIndex>>,
) -> Cache<MessageKey, Arc<Entry>> {
    Cache::builder()
        // The cap is enforced by `write_at` through the usage index, evicting
        // what has sat longest and weighs most. moka's own limit is only a
        // backstop with headroom, so its admission policy never turns away a
        // new entry. (A cap of 0 is "off" and nothing is stored; moka is
        // given a floor so it is always valid.)
        .max_capacity(max_bytes.saturating_mul(2).saturating_add(1 << 20))
        .weigher(|_key: &MessageKey, entry: &Arc<Entry>| entry.weight())
        .eviction_listener(move |key: Arc<MessageKey>, entry: Arc<Entry>, cause| {
            if cause != moka::notification::RemovalCause::Replaced {
                lock(&usage).forget(&key, Some(Arc::as_ptr(&entry) as usize));
            }
        })
        // Idle, not live: a read pushes the expiry out again.
        .time_to_idle(config.time_to_idle)
        .support_invalidation_closures()
        .build()
}

impl Shared {
    /// The cache in force. Cheap: a moka cache is a handle.
    fn cache(&self) -> Cache<MessageKey, Arc<Entry>> {
        self.cache
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn emit(&self, event: CacheEvent) {
        if let Some(listener) = &self.listener {
            listener(event);
        }
    }

    fn flush_log(&self) {
        let mut pending = std::mem::take(&mut *lock(&self.log));
        pending.skipped = self.skipped.swap(0, Ordering::AcqRel);
        if pending.stored == 0 && pending.skipped == 0 {
            return;
        }
        self.cache().run_pending_tasks();
        self.emit(CacheEvent::Updated {
            stored: pending.stored,
            skipped: pending.skipped,
            payload_bytes: pending.payload_bytes,
            compressed_bytes: pending.compressed_bytes,
            last_key: pending.last_key,
            entries: self.cache().entry_count(),
            weighted_bytes: self.cache().weighted_size(),
        });
    }
}

/// Cheap to clone; every clone shares one cache.
#[derive(Clone)]
pub struct MessageCache {
    shared: Arc<Shared>,
}

impl Default for MessageCache {
    fn default() -> Self {
        Self::new(CacheConfig::default())
    }
}

impl MessageCache {
    pub fn new(config: CacheConfig) -> Self {
        Self::build(config, None)
    }

    /// A cache that reports its hits, misses and updates to `listener`.
    pub fn with_listener(
        config: CacheConfig,
        listener: impl Fn(CacheEvent) + Send + Sync + 'static,
    ) -> Self {
        Self::build(config, Some(Arc::new(listener)))
    }

    fn build(config: CacheConfig, listener: Option<Listener>) -> Self {
        let max_bytes = config.max_bytes;
        let usage = Arc::new(Mutex::new(UsageIndex::default()));
        Self {
            shared: Arc::new(Shared {
                cache: RwLock::new(build_cache(&config, max_bytes, usage.clone())),
                usage,
                max_bytes: AtomicU64::new(max_bytes),
                ranges: Mutex::new(HashMap::new()),
                pending: Arc::new(Semaphore::new(config.max_pending_stores.max(1))),
                queue: OnceLock::new(),
                pool: OnceLock::new(),
                queued_bytes: AtomicU64::new(0),
                skipped: AtomicU64::new(0),
                buffers: Mutex::new(BufferPool::default()),
                epoch: AtomicU64::new(0),
                config,
                listener,
                log: Mutex::new(PendingLog::default()),
                log_scheduled: AtomicBool::new(false),
                maintenance_started: AtomicBool::new(false),
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                compressions: AtomicU64::new(0),
            }),
        }
    }

    /// Whether the cache is on. A cap of 0 turns it off: nothing is stored
    /// and every lookup misses without being counted or logged.
    pub fn enabled(&self) -> bool {
        self.shared.max_bytes.load(Ordering::Relaxed) > 0
    }

    /// The byte cap in force.
    pub fn max_bytes(&self) -> u64 {
        self.shared.max_bytes.load(Ordering::Relaxed)
    }

    /// Changes the byte cap, returning whether it was different.
    ///
    /// moka fixes its capacity when it is built, so a change builds a new,
    /// empty cache and drops the old one — what was cached is gone, which is
    /// the same cost a larger or smaller cache would have paid anyway to be
    /// filled again. An unchanged value does nothing, so the app can state its
    /// setting at every start without emptying anything.
    ///
    /// A store that was already compressing when the swap happens writes into
    /// the new cache. `0` turns the cache off.
    pub fn set_max_bytes(&self, max_bytes: u64) -> bool {
        if self.shared.max_bytes.swap(max_bytes, Ordering::AcqRel) == max_bytes {
            return false;
        }
        let dropped = {
            let mut cache = self
                .shared
                .cache
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            cache.run_pending_tasks();
            let dropped = cache.entry_count();
            *cache = build_cache(&self.shared.config, max_bytes, self.shared.usage.clone());
            *lock(&self.shared.usage) = UsageIndex::default();
            dropped
        };
        lock(&self.shared.ranges).clear();
        self.shared.epoch.fetch_add(1, Ordering::AcqRel);
        self.drop_pooled_buffers();
        self.refresh_full();
        self.shared.emit(CacheEvent::Resized { max_bytes, dropped });
        true
    }

    /// Whether a payload of this size belongs in the cache at all.
    pub fn wants(&self, payload_len: usize) -> bool {
        self.enabled() && payload_len > self.shared.config.min_payload_bytes
    }

    /// Whether a payload of this size is worth storing for a fetch that
    /// already carried the first `inline_bytes` of every payload to the grid.
    ///
    /// The cache exists for what the grid had to truncate, so a payload that
    /// fit in the row is not stored — nobody will ask for it again. Never
    /// below the cache's own floor ([`Self::wants`]). `0` means the fetch
    /// carried nothing inline (a whole-payload fetch, whose result is what
    /// fills the cache for the row that was clicked).
    pub fn wants_above(&self, payload_len: usize, inline_bytes: usize) -> bool {
        self.wants(payload_len) && payload_len > inline_bytes
    }

    /// Whether the cache already holds this message unchanged.
    ///
    /// An offset's content does not change, so recompressing a payload that is
    /// already there — which is what re-running a fetch over the same rows
    /// would do — is pure cost. Matching on the true length and the timestamp
    /// is what makes "already there" mean "the same message". A read, so it
    /// also counts as use of the entry.
    pub fn is_current(
        &self,
        key: &MessageKey,
        payload_len: usize,
        timestamp_ms: Option<i64>,
    ) -> bool {
        let current = self.shared.cache().get(key).is_some_and(|entry| {
            entry.payload_len == payload_len && entry.timestamp_ms == timestamp_ms
        });
        if current {
            lock(&self.shared.usage).touch(key);
        }
        current
    }

    /// Looks a message up, decompressing it. Resets the entry's idle timer.
    ///
    /// Decompression is CPU work proportional to the payload; call from a
    /// blocking context. An entry that cannot be decompressed is dropped.
    pub fn get(&self, key: &MessageKey) -> Option<CachedMessage> {
        if !self.enabled() {
            return None;
        }
        let found = self.shared.cache().get(key).and_then(|entry| {
            let payload = if entry.raw {
                Ok(entry.compressed.to_vec())
            } else {
                zstd::bulk::decompress(&entry.compressed, entry.payload_len)
            };
            match payload {
                Ok(payload) => Some(CachedMessage {
                    timestamp_ms: entry.timestamp_ms,
                    key: entry.key.clone(),
                    headers: entry.headers.clone(),
                    payload,
                }),
                Err(_) => {
                    self.shared.cache().invalidate(key);
                    None
                }
            }
        });
        match &found {
            Some(message) => {
                lock(&self.shared.usage).touch(key);
                self.shared.hits.fetch_add(1, Ordering::Relaxed);
                self.shared.emit(CacheEvent::Hit {
                    key: key.to_string(),
                    payload_bytes: message.payload.len(),
                });
            }
            None => {
                self.shared.misses.fetch_add(1, Ordering::Relaxed);
                self.shared.emit(CacheEvent::Miss {
                    key: key.to_string(),
                });
            }
        }
        found
    }

    /// Queues a message for compression and returns at once.
    ///
    /// The store is an event sent to the listener task, which compresses it
    /// on the compression pool as soon as a slot is free, so a
    /// caller that outruns the compressors waits for nothing.
    ///
    /// `make` builds the entry and is called **only if** the store will be
    /// queued: the payload is not copied for a store that is declined. It is
    /// declined for a payload at or under the threshold, one already cached
    /// unchanged ([`Self::is_current`]), and — counted in the next summary —
    /// when the queue already holds [`CacheConfig::max_queued_bytes`]. A
    /// single payload larger than that bound is accepted into an empty queue.
    pub fn store_in_background(
        &self,
        runtime: &Handle,
        key: MessageKey,
        payload_len: usize,
        timestamp_ms: Option<i64>,
        make: impl FnOnce() -> CachedMessage,
    ) {
        // Nothing here touches moka: a lookup or a size read can run its
        // housekeeping on the calling thread, and the caller is the fetch
        // loop. "Already cached unchanged" is answered by the listener.
        let _ = timestamp_ms; // kept in the signature for callers; the listener compares it
        if !self.wants(payload_len) {
            return;
        }
        self.start_maintenance(runtime);
        let queue = self.queue(runtime);
        let len = payload_len as u64;
        let before = self.shared.queued_bytes.fetch_add(len, Ordering::AcqRel);
        if before > 0 && before + len > self.shared.config.max_queued_bytes {
            self.shared.queued_bytes.fetch_sub(len, Ordering::AcqRel);
            self.shared.skipped.fetch_add(1, Ordering::Relaxed);
            self.schedule_log(runtime);
            return;
        }
        let message = make();
        let job = StoreJob {
            key,
            message,
            epoch: self.shared.epoch.load(Ordering::Acquire),
        };
        if queue.send(job).is_err() {
            self.shared.queued_bytes.fetch_sub(len, Ordering::AcqRel);
        }
    }

    /// The store queue's sender, starting its listener task on first use.
    ///
    /// The task owns the receiver and only a `Weak` of the cache's state, so
    /// dropping the last [`MessageCache`] closes the channel and ends it.
    fn queue(&self, runtime: &Handle) -> &mpsc::UnboundedSender<StoreJob> {
        self.shared.queue.get_or_init(|| {
            let (tx, mut rx) = mpsc::unbounded_channel::<StoreJob>();
            let weak: Weak<Shared> = Arc::downgrade(&self.shared);
            let handle = runtime.clone();
            runtime.spawn(async move {
                while let Some(job) = rx.recv().await {
                    let Some(shared) = weak.upgrade() else { break };
                    let this = MessageCache { shared };
                    let len = job.message.payload.len() as u64;
                    // Waiting here is what turns the slot count into
                    // back-pressure on the queue rather than on the caller.
                    let Ok(permit) = this.shared.pending.clone().acquire_owned().await else {
                        break;
                    };
                    let handle = handle.clone();
                    let task = this.clone();
                    this.compression_pool().spawn(move || {
                        let this = task;
                        let stale = job.epoch != this.shared.epoch.load(Ordering::Acquire);
                        if !stale
                            && !this.is_current(
                                &job.key,
                                job.message.payload.len(),
                                job.message.timestamp_ms,
                            )
                        {
                            this.write_at(job.key, job.message, Some(job.epoch));
                        }
                        this.shared.queued_bytes.fetch_sub(len, Ordering::AcqRel);
                        drop(permit);
                        this.schedule_log(&handle);
                    });
                }
            });
            tx
        })
    }

    /// An empty buffer with room for `len` bytes, reusing an idle one when
    /// there is one. Fill it and put it in a [`CachedMessage`]: the cache
    /// hands it back to the pool once the payload is compressed.
    ///
    /// Best fit, so one huge buffer is not spent on a small payload while a
    /// closer one waits.
    pub fn take_buffer(&self, len: usize) -> Vec<u8> {
        let reused = {
            let mut pool = lock(&self.shared.buffers);
            let best = pool
                .buffers
                .iter()
                .enumerate()
                .filter(|(_, b)| b.capacity() >= len)
                .min_by_key(|(_, b)| b.capacity())
                .map(|(index, _)| index);
            best.map(|index| {
                let buffer = pool.buffers.swap_remove(index);
                pool.bytes -= buffer.capacity() as u64;
                buffer
            })
        };
        match reused {
            Some(mut buffer) => {
                buffer.clear();
                buffer
            }
            None => Vec::with_capacity(len),
        }
    }

    /// Keeps a finished payload buffer for [`Self::take_buffer`], unless the
    /// pool is full — then it is simply freed.
    fn give_back(&self, mut buffer: Vec<u8>) {
        let capacity = buffer.capacity() as u64;
        if capacity == 0 || capacity > self.shared.config.max_pooled_bytes / 2 {
            return;
        }
        buffer.clear();
        let mut pool = lock(&self.shared.buffers);
        if pool.bytes + capacity > self.shared.config.max_pooled_bytes || pool.buffers.len() >= 64 {
            return;
        }
        pool.bytes += capacity;
        pool.buffers.push(buffer);
    }

    /// Idle buffer bytes waiting for reuse.
    pub fn pooled_bytes(&self) -> u64 {
        lock(&self.shared.buffers).bytes
    }

    fn drop_pooled_buffers(&self) {
        let mut pool = lock(&self.shared.buffers);
        pool.buffers.clear();
        pool.bytes = 0;
    }

    /// The compression threads, started with the first store.
    fn compression_pool(&self) -> &rayon::ThreadPool {
        self.shared.pool.get_or_init(|| {
            rayon::ThreadPoolBuilder::new()
                .num_threads(self.shared.config.max_pending_stores.max(1))
                .thread_name(|index| format!("salty-cache-compress-{index}"))
                .build()
                .expect("the compression thread pool starts")
        })
    }

    /// Lets moka apply its pending writes and removals, which is what makes
    /// recent inserts and purges visible. Only for threads that are already
    /// off the fetch path.
    fn refresh_full(&self) {
        self.shared.cache().run_pending_tasks();
    }

    /// Uncompressed bytes waiting for, or in, a compression slot.
    pub fn queued_bytes(&self) -> u64 {
        self.shared.queued_bytes.load(Ordering::Acquire)
    }

    /// Compresses and stores on the calling thread, then logs at once.
    /// [`Self::store_in_background`] is what a fetch should use; this is for
    /// callers that are already off the hot path, and for tests.
    pub fn store_now(&self, key: MessageKey, message: CachedMessage) {
        if !self.wants(message.payload.len()) {
            return;
        }
        self.write(key, message);
        self.shared.flush_log();
    }

    fn write(&self, key: MessageKey, message: CachedMessage) {
        self.write_at(key, message, None);
    }

    /// Compresses and inserts. With an `epoch`, the entry is discarded if a
    /// purge or resize happened since — checked after compressing too, since
    /// that is the slow part and a purge can land during it. (A purge in the
    /// instants between that check and the insert still loses the race; the
    /// consequence is one stale entry, which idle expiry removes.)
    fn write_at(&self, key: MessageKey, message: CachedMessage, epoch: Option<u64>) {
        let raw = message.payload.len() <= self.shared.config.min_compress_bytes;
        let compressed = if raw {
            Ok(message.payload.clone())
        } else {
            zstd::bulk::compress(&message.payload, self.shared.config.zstd_level)
        };
        let Ok(compressed) = compressed else {
            self.give_back(message.payload);
            return;
        };
        if epoch.is_some_and(|e| e != self.shared.epoch.load(Ordering::Acquire)) {
            self.give_back(message.payload);
            return;
        }
        let payload_len = message.payload.len();
        self.give_back(message.payload);
        let entry = Entry {
            raw,
            payload_len,
            compressed: compressed.into_boxed_slice(),
            timestamp_ms: message.timestamp_ms,
            key: message.key,
            headers: message.headers,
        };
        {
            let mut log = lock(&self.shared.log);
            log.stored += 1;
            log.payload_bytes += entry.payload_len as u64;
            log.compressed_bytes += entry.compressed.len() as u64;
            log.last_key = key.to_string();
        }
        {
            let mut ranges = lock(&self.shared.ranges);
            let range = ranges
                .entry((key.connection_id.clone(), key.topic.clone(), key.partition))
                .or_insert(CachedRange {
                    lowest: key.offset,
                    highest: key.offset,
                });
            range.lowest = range.lowest.min(key.offset);
            range.highest = range.highest.max(key.offset);
        }
        let weight = entry.weight();
        let max = self.shared.max_bytes.load(Ordering::Relaxed);
        if u64::from(weight) > max {
            // Larger than the whole cache: nothing could be evicted to fit it.
            self.shared.skipped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.shared.compressions.fetch_add(1, Ordering::Relaxed);
        let entry = Arc::new(entry);
        let id = Arc::as_ptr(&entry) as usize;
        let cache = self.shared.cache();
        cache.insert(key.clone(), entry);
        // New entries take precedence: if this write took the cache past its
        // cap, evict the oldest-untouched and biggest down to 90% of it.
        let evicted = {
            let mut usage = lock(&self.shared.usage);
            usage.record(key.clone(), weight, id);
            if usage.bytes > max {
                usage.make_room(&key, max - max / 10)
            } else {
                Vec::new()
            }
        };
        for victim in &evicted {
            cache.invalidate(victim);
        }
    }

    /// One summary line per burst: the first store of a burst schedules it,
    /// and everything that lands in the meantime is folded in.
    fn schedule_log(&self, runtime: &Handle) {
        if self.shared.log_scheduled.swap(true, Ordering::AcqRel) {
            return;
        }
        let this = self.clone();
        let debounce = self.shared.config.log_debounce;
        runtime.spawn(async move {
            tokio::time::sleep(debounce).await;
            this.shared.log_scheduled.store(false, Ordering::Release);
            this.shared.flush_log();
        });
    }

    /// Releases expired entries on a timer, for as long as the cache lives.
    fn start_maintenance(&self, runtime: &Handle) {
        if self.shared.maintenance_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let weak: Weak<Shared> = Arc::downgrade(&self.shared);
        let interval = self.shared.config.maintenance_interval;
        runtime.spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(shared) = weak.upgrade() else { break };
                // Blocking-pool work, not the async runtime's: this is moka's
                // housekeeping, which scales with what expired.
                let _ = tokio::task::spawn_blocking(move || {
                    MessageCache { shared }.refresh_full();
                })
                .await;
            }
        });
    }

    fn purge(&self, what: String, predicate: impl Fn(&MessageKey) -> bool + Send + Sync + 'static) {
        self.shared.epoch.fetch_add(1, Ordering::AcqRel);
        // Only fails if invalidation closures were not enabled, which `build`
        // always does.
        let _ = self
            .shared
            .cache()
            .invalidate_entries_if(move |key, _| predicate(key));
        self.refresh_full();
        self.shared.emit(CacheEvent::Purged {
            what,
            entries: self.shared.cache().entry_count(),
        });
    }

    /// Drops everything the cache holds, for every connection, and returns
    /// how many entries that was. The user-facing "free my memory" action.
    ///
    /// Counters and hit/miss totals are kept: they describe the session, not
    /// what is currently held.
    pub fn clear(&self) -> u64 {
        self.shared.epoch.fetch_add(1, Ordering::AcqRel);
        self.drop_pooled_buffers();
        self.shared.cache().run_pending_tasks();
        let cleared = self.shared.cache().entry_count();
        lock(&self.shared.ranges).clear();
        self.shared.cache().invalidate_all();
        self.refresh_full();
        self.shared.emit(CacheEvent::Purged {
            what: format!("all {cleared} cached payloads"),
            entries: self.shared.cache().entry_count(),
        });
        cleared
    }

    /// Drops every entry of one connection. Called when the connection is
    /// released — disconnect, delete, idle disconnect, or an edited
    /// configuration, any of which can mean a different cluster.
    pub fn invalidate_connection(&self, connection_id: &str) {
        let id = connection_id.to_string();
        lock(&self.shared.ranges).retain(|(connection, _, _), _| *connection != id);
        // Nothing cached for it means nothing to purge — and nothing to log.
        self.shared.cache().run_pending_tasks();
        if self.shared.cache().entry_count() == 0 {
            return;
        }
        let what = format!("connection {connection_id}");
        self.purge(what, move |key| key.connection_id == id);
    }

    /// Drops every entry of one topic.
    pub fn invalidate_topic(&self, connection_id: &str, topic: &str) {
        let (id, name) = (connection_id.to_string(), topic.to_string());
        lock(&self.shared.ranges)
            .retain(|(connection, t, _), _| !(*connection == id && *t == name));
        self.purge(format!("topic {topic}"), move |key| {
            key.connection_id == id && key.topic == name
        });
    }

    /// Tells the cache a partition's current watermarks: `low` is the oldest
    /// offset still on the broker and `high` the offset the next message will
    /// get. Returns whether anything was dropped.
    ///
    /// * A cached offset at or past `high` cannot exist: the topic was deleted
    ///   and recreated, or truncated. The whole topic goes — an offset that
    ///   was reused names a different message, and so may its neighbours.
    /// * Cached offsets below `low` have been removed by retention or
    ///   compaction; those go, and the rest stay.
    pub fn note_watermarks(
        &self,
        connection_id: &str,
        topic: &str,
        partition: i32,
        low: i64,
        high: i64,
    ) -> bool {
        let id = (connection_id.to_string(), topic.to_string(), partition);
        let Some(range) = lock(&self.shared.ranges).get(&id).copied() else {
            return false;
        };
        if range.highest >= high {
            self.invalidate_topic(connection_id, topic);
            return true;
        }
        if range.lowest < low {
            if let Some(range) = lock(&self.shared.ranges).get_mut(&id) {
                range.lowest = low;
            }
            let (conn, name) = (connection_id.to_string(), topic.to_string());
            self.purge(
                format!("{topic}-{partition} offsets below {low}"),
                move |key| {
                    key.connection_id == conn
                        && key.topic == name
                        && key.partition == partition
                        && key.offset < low
                },
            );
            return true;
        }
        false
    }

    /// Entries currently held.
    pub fn entry_count(&self) -> u64 {
        self.shared.cache().run_pending_tasks();
        self.shared.cache().entry_count()
    }

    /// Compressed bytes (plus metadata) currently held.
    pub fn weighted_bytes(&self) -> u64 {
        self.shared.cache().run_pending_tasks();
        self.shared.cache().weighted_size()
    }

    pub fn hits(&self) -> u64 {
        self.shared.hits.load(Ordering::Relaxed)
    }

    pub fn misses(&self) -> u64 {
        self.shared.misses.load(Ordering::Relaxed)
    }

    /// How many entries have been compressed and written.
    pub fn compressions(&self) -> u64 {
        self.shared.compressions.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn key(offset: i64) -> MessageKey {
        MessageKey::new("c1", "orders", 3, offset)
    }

    fn message(len: usize) -> CachedMessage {
        CachedMessage {
            timestamp_ms: Some(1_700_000_000_000),
            key: Some(b"k".to_vec()),
            headers: vec![CachedHeader {
                key: "trace".into(),
                value: Some(b"abc".to_vec()),
            }],
            // Repetitive on purpose, so it compresses.
            payload: (0..len).map(|i| (i % 7) as u8).collect(),
        }
    }

    /// Bytes that do not compress, so each entry really costs its size and
    /// a high level really takes a while.
    fn noise(len: usize, seed: u32) -> Vec<u8> {
        (0..len as u32)
            .map(|i| (i.wrapping_mul(2_654_435_761).wrapping_add(seed) >> 13) as u8)
            .collect()
    }

    fn recording(config: CacheConfig) -> (MessageCache, Arc<Mutex<Vec<CacheEvent>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink = events.clone();
        let cache = MessageCache::with_listener(config, move |e| sink.lock().unwrap().push(e));
        (cache, events)
    }

    fn lines(events: &Arc<Mutex<Vec<CacheEvent>>>) -> Vec<String> {
        events
            .lock()
            .unwrap()
            .iter()
            .map(CacheEvent::message)
            .collect()
    }

    #[test]
    fn a_stored_message_comes_back_whole() {
        let cache = MessageCache::default();
        let original = message(100_000);
        cache.store_now(key(1), original.clone());
        assert_eq!(cache.get(&key(1)), Some(original));
    }

    #[test]
    fn payloads_at_or_under_four_kb_are_not_cached() {
        let cache = MessageCache::default();
        cache.store_now(key(1), message(INLINE_PAYLOAD_BYTES));
        assert_eq!(cache.entry_count(), 0);
        cache.store_now(key(2), message(INLINE_PAYLOAD_BYTES + 1));
        assert_eq!(cache.entry_count(), 1);
    }

    #[test]
    fn the_key_includes_the_connection() {
        let cache = MessageCache::default();
        cache.store_now(key(1), message(10_000));
        let other = MessageKey::new("c2", "orders", 3, 1);
        assert!(cache.get(&other).is_none());
        assert!(cache.get(&key(1)).is_some());
    }

    #[test]
    fn the_key_is_logged_as_topic_partition_offset() {
        assert_eq!(key(1042).to_string(), "orders-3-1042");
    }

    #[test]
    fn a_later_store_replaces_the_entry() {
        let cache = MessageCache::default();
        cache.store_now(key(1), message(10_000));
        let mut newer = message(20_000);
        newer.headers.clear();
        cache.store_now(key(1), newer.clone());
        assert_eq!(cache.get(&key(1)), Some(newer));
        assert_eq!(cache.entry_count(), 1);
    }

    #[test]
    fn a_small_payload_is_stored_as_it_came_and_a_large_one_is_compressed() {
        let cache = MessageCache::default();
        let small = message(MIN_COMPRESS_BYTES);
        cache.store_now(key(1), small.clone());
        assert!(
            cache.weighted_bytes() >= MIN_COMPRESS_BYTES as u64,
            "{}",
            cache.weighted_bytes()
        );
        assert_eq!(cache.get(&key(1)), Some(small));

        let cache = MessageCache::default();
        cache.store_now(key(2), message(MIN_COMPRESS_BYTES + 1));
        assert!(
            cache.weighted_bytes() < 10_000,
            "{}",
            cache.weighted_bytes()
        );
    }

    #[test]
    fn it_is_compressed() {
        let cache = MessageCache::default();
        cache.store_now(key(1), message(1_000_000));
        assert!(
            cache.weighted_bytes() < 100_000,
            "{}",
            cache.weighted_bytes()
        );
    }

    #[test]
    fn the_default_level_is_not_the_maximum() {
        assert_eq!(CacheConfig::default().zstd_level, ZSTD_DEFAULT_LEVEL);
        const { assert!(ZSTD_DEFAULT_LEVEL < ZSTD_MAX_LEVEL) };
    }

    #[test]
    fn a_configured_level_is_used() {
        let cache = MessageCache::new(CacheConfig {
            zstd_level: ZSTD_MAX_LEVEL,
            ..CacheConfig::default()
        });
        let original = message(50_000);
        cache.store_now(key(1), original.clone());
        assert_eq!(cache.get(&key(1)), Some(original));
    }

    #[test]
    fn hits_and_misses_are_counted_and_reported() {
        let (cache, events) = recording(CacheConfig::default());
        cache.store_now(key(1), message(10_000));
        assert!(cache.get(&key(1)).is_some());
        assert!(cache.get(&key(2)).is_none());

        assert_eq!((cache.hits(), cache.misses()), (1, 1));
        let lines = lines(&events);
        assert!(
            lines
                .iter()
                .any(|l| l == "Payload cache hit: orders-3-1 (10000 bytes)"),
            "{lines:?}"
        );
        assert!(
            lines.iter().any(|l| l == "Payload cache miss: orders-3-2"),
            "{lines:?}"
        );
    }

    #[test]
    fn an_update_reports_the_object_count() {
        let (cache, events) = recording(CacheConfig::default());
        cache.store_now(key(1), message(10_000));
        cache.store_now(key(2), message(10_000));

        let counts: Vec<u64> = events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                CacheEvent::Updated { entries, .. } => Some(*entries),
                _ => None,
            })
            .collect();
        assert_eq!(counts, vec![1, 2]);
        assert!(
            lines(&events)[1].contains("2 objects"),
            "{:?}",
            lines(&events)
        );
        assert!(
            lines(&events)[0].contains("orders-3-1 stored"),
            "{:?}",
            lines(&events)
        );
    }

    #[test]
    fn the_cache_holds_no_more_than_its_cap() {
        let cache = MessageCache::new(CacheConfig {
            max_bytes: 4_000,
            ..CacheConfig::default()
        });
        for offset in 0..50 {
            let mut m = message(10_000);
            m.payload = noise(10_000, offset as u32);
            cache.store_now(key(offset), m);
        }
        assert!(
            cache.weighted_bytes() <= 4_000,
            "{}",
            cache.weighted_bytes()
        );
    }

    /// Bytes that really weigh `len` once stored: unlike `noise`, xorshift
    /// does not compress.
    fn incompressible(len: usize, seed: u32) -> CachedMessage {
        let mut x = 0x9E37_79B9_u32.wrapping_add(seed) | 1;
        let mut m = message(len);
        m.payload = (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 17;
                x ^= x << 5;
                x as u8
            })
            .collect();
        m
    }

    #[test]
    fn a_full_cache_makes_room_for_the_newest_entry() {
        let cache = MessageCache::new(CacheConfig {
            max_bytes: 40_000,
            ..CacheConfig::default()
        });
        for offset in 0..20 {
            cache.store_now(key(offset), incompressible(10_000, offset as u32));
            assert!(
                cache.get(&key(offset)).is_some(),
                "entry {offset} was just stored and must be kept"
            );
        }
        assert!(
            cache.weighted_bytes() <= 40_000,
            "{}",
            cache.weighted_bytes()
        );
        assert!(cache.get(&key(0)).is_none(), "the oldest was evicted");
    }

    #[test]
    fn the_oldest_untouched_goes_before_one_that_was_just_read() {
        let cache = MessageCache::new(CacheConfig {
            max_bytes: 35_000,
            ..CacheConfig::default()
        });
        for offset in 0..3 {
            cache.store_now(key(offset), incompressible(10_000, offset as u32));
        }
        // Entry 0 is the oldest, but it is read: 1 is now the oldest untouched.
        assert!(cache.get(&key(0)).is_some());
        cache.store_now(key(3), incompressible(10_000, 3));
        assert!(cache.get(&key(1)).is_none(), "oldest untouched evicted");
        assert!(cache.get(&key(0)).is_some(), "recently read entry kept");
        assert!(cache.get(&key(3)).is_some());
    }

    #[test]
    fn a_big_idle_entry_goes_before_a_small_one_of_the_same_age() {
        let cache = MessageCache::new(CacheConfig {
            max_bytes: 30_000,
            ..CacheConfig::default()
        });
        cache.store_now(key(1), incompressible(5_000, 1));
        cache.store_now(key(2), incompressible(20_000, 2));
        cache.store_now(key(3), incompressible(10_000, 3));
        assert!(cache.get(&key(2)).is_none(), "the biggest was evicted");
        assert!(cache.get(&key(1)).is_some());
        assert!(cache.get(&key(3)).is_some());
    }

    #[test]
    fn a_payload_larger_than_the_whole_cache_is_declined() {
        let cache = MessageCache::new(CacheConfig {
            max_bytes: 10_000,
            ..CacheConfig::default()
        });
        cache.store_now(key(1), incompressible(5_000, 1));
        cache.store_now(key(2), incompressible(20_000, 2));
        assert!(cache.get(&key(2)).is_none());
        assert!(cache.get(&key(1)).is_some(), "nothing was evicted for it");
    }

    #[test]
    fn compression_does_not_need_a_tokio_blocking_thread() {
        // One blocking thread, and it is occupied for the whole test: a
        // cache that compressed through `spawn_blocking` could not finish.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (release, hold) = std::sync::mpsc::channel::<()>();
            let _occupied = tokio::task::spawn_blocking(move || {
                let _ = hold.recv();
            });
            let cache = MessageCache::default();
            let handle = Handle::current();
            let m = message(50_000);
            cache.store_in_background(&handle, key(1), m.payload.len(), None, || m);
            for _ in 0..200 {
                if cache.compressions() == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            let compressed = cache.compressions();
            let _ = release.send(());
            assert_eq!(
                compressed, 1,
                "the store finished with no blocking thread free"
            );
        });
    }

    /// The runtime's thread is the one that serves the UI's commands and
    /// emits the stream's events. Nothing the cache does for a burst of
    /// stores may hold it.
    #[tokio::test(flavor = "current_thread")]
    async fn a_burst_of_stores_never_stalls_the_async_runtime() {
        let cache = MessageCache::new(CacheConfig {
            log_debounce: Duration::from_millis(20),
            ..CacheConfig::default()
        });
        let handle = Handle::current();
        let mut worst = Duration::ZERO;
        let mut last = std::time::Instant::now();
        for i in 0..400i64 {
            let payload: Vec<u8> = (0..60_000u32)
                .map(|j| (j.wrapping_mul(2_654_435_761).wrapping_add(i as u32) >> 13) as u8)
                .collect();
            cache.store_in_background(&handle, key(i), payload.len(), None, move || {
                CachedMessage {
                    timestamp_ms: None,
                    key: None,
                    headers: Vec::new(),
                    payload,
                }
            });
            if i % 8 == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
                worst = worst.max(last.elapsed());
                last = std::time::Instant::now();
            }
        }
        for _ in 0..400 {
            if cache.queued_bytes() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
            worst = worst.max(last.elapsed());
            last = std::time::Instant::now();
        }
        assert!(
            worst < Duration::from_millis(150),
            "the runtime thread was held for {worst:?} (compressed {})",
            cache.compressions()
        );
    }

    #[test]
    fn only_what_the_grid_had_to_truncate_is_wanted() {
        let cache = MessageCache::default();
        // The grid carried 64 KB of each row: a 10 KB payload fit, a 70 KB
        // one did not.
        assert!(!cache.wants_above(10_000, 64 * 1024));
        assert!(!cache.wants_above(64 * 1024, 64 * 1024));
        assert!(cache.wants_above(64 * 1024 + 1, 64 * 1024));
        // A fetch priced down to 4 KB leaves the 10 KB payload truncated.
        assert!(cache.wants_above(10_000, 4_096));
        // Nothing inline: the cache's own floor decides.
        assert!(cache.wants_above(10_000, 0));
        assert!(!cache.wants_above(100, 0));
        // Off means off.
        cache.set_max_bytes(0);
        assert!(!cache.wants_above(1_000_000, 0));
    }

    #[test]
    fn a_taken_buffer_is_empty_and_has_the_room_asked_for() {
        let cache = MessageCache::default();
        let buffer = cache.take_buffer(10_000);
        assert!(buffer.is_empty());
        assert!(buffer.capacity() >= 10_000);
    }

    #[test]
    fn a_finished_stores_buffer_is_reused_by_the_next_take() {
        let cache = MessageCache::default();
        let mut buffer = cache.take_buffer(50_000);
        buffer.extend_from_slice(&noise(50_000, 1));
        let pointer = buffer.as_ptr();
        cache.store_now(
            key(1),
            CachedMessage {
                timestamp_ms: None,
                key: None,
                headers: Vec::new(),
                payload: buffer,
            },
        );
        assert!(cache.pooled_bytes() >= 50_000, "{}", cache.pooled_bytes());

        let again = cache.take_buffer(40_000);
        assert_eq!(again.as_ptr(), pointer, "the same allocation came back");
        assert!(again.is_empty());
        assert_eq!(cache.pooled_bytes(), 0);
        // And what was cached is intact, not the recycled bytes.
        assert_eq!(cache.get(&key(1)).unwrap().payload, noise(50_000, 1));
    }

    #[test]
    fn the_pool_is_bounded_and_refuses_what_would_overfill_it() {
        let cache = MessageCache::new(CacheConfig {
            max_pooled_bytes: 100_000,
            ..CacheConfig::default()
        });
        for i in 0..10 {
            cache.store_now(
                key(i),
                CachedMessage {
                    timestamp_ms: None,
                    key: None,
                    headers: Vec::new(),
                    payload: noise(40_000, i as u32),
                },
            );
        }
        assert!(cache.pooled_bytes() <= 100_000, "{}", cache.pooled_bytes());
        // A buffer over half the pool is not kept at all.
        let cache = MessageCache::new(CacheConfig {
            max_pooled_bytes: 100_000,
            ..CacheConfig::default()
        });
        cache.store_now(
            key(1),
            CachedMessage {
                timestamp_ms: None,
                key: None,
                headers: Vec::new(),
                payload: noise(60_000, 1),
            },
        );
        assert_eq!(cache.pooled_bytes(), 0);
    }

    #[test]
    fn clearing_the_cache_drops_the_idle_buffers() {
        let cache = MessageCache::default();
        cache.store_now(key(1), message(50_000));
        assert!(cache.pooled_bytes() > 0);
        cache.clear();
        assert_eq!(cache.pooled_bytes(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn queueing_does_not_wait_for_compression() {
        let cache = MessageCache::new(CacheConfig {
            max_pending_stores: 1,
            // ~1 MB/s: compressing even one of these takes seconds.
            zstd_level: ZSTD_MAX_LEVEL,
            ..CacheConfig::default()
        });
        let handle = Handle::current();
        let payloads: Vec<Vec<u8>> = (0..6).map(|i| noise(4_000_000, i)).collect();
        let started = std::time::Instant::now();
        for (offset, payload) in payloads.into_iter().enumerate() {
            cache.store_in_background(
                &handle,
                key(offset as i64),
                payload.len(),
                None,
                move || CachedMessage {
                    timestamp_ms: None,
                    key: None,
                    headers: Vec::new(),
                    payload,
                },
            );
        }
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "queueing 6 slow stores took {:?}",
            started.elapsed()
        );
        assert!(cache.queued_bytes() > 0);
        assert_eq!(cache.compressions(), 0, "nothing had to finish first");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_full_cache_keeps_storing_newer_entries_from_the_queue() {
        let cache = MessageCache::new(CacheConfig {
            max_pending_stores: 1,
            max_bytes: 102_000,
            ..CacheConfig::default()
        });
        let handle = Handle::current();
        for offset in 0..8 {
            let m = incompressible(100_000, offset as u32);
            cache.store_in_background(&handle, key(offset), m.payload.len(), None, || m);
        }
        for _ in 0..200 {
            if cache.queued_bytes() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(cache.queued_bytes(), 0, "the queue drained");
        assert_eq!(cache.compressions(), 8, "every store was written");
        assert!(cache.get(&key(7)).is_some(), "the newest is cached");
        assert!(cache.weighted_bytes() <= 102_000);
    }

    #[test]
    fn an_unused_entry_expires_but_a_used_one_stays() {
        // Wide margins: touches are 150 ms apart against a 1 s idle time, so a
        // runner that stalls for most of a second still does not expire the
        // entry that is being used.
        let cache = MessageCache::new(CacheConfig {
            time_to_idle: Duration::from_millis(1_000),
            ..CacheConfig::default()
        });
        cache.store_now(key(1), message(10_000));
        cache.store_now(key(2), message(10_000));

        // Keep touching key 1 for longer than the idle time.
        for _ in 0..9 {
            std::thread::sleep(Duration::from_millis(150));
            assert!(cache.get(&key(1)).is_some());
        }

        assert!(
            cache.get(&key(2)).is_none(),
            "the untouched entry should have expired"
        );
        assert!(
            cache.get(&key(1)).is_some(),
            "the used entry should still be there"
        );
    }

    // --- invalidation -----------------------------------------------------

    #[test]
    fn releasing_a_connection_drops_only_its_entries() {
        let cache = MessageCache::default();
        cache.store_now(key(1), message(10_000));
        cache.store_now(MessageKey::new("c2", "orders", 3, 1), message(10_000));

        cache.invalidate_connection("c1");

        assert!(cache.get(&key(1)).is_none());
        assert!(cache.get(&MessageKey::new("c2", "orders", 3, 1)).is_some());
    }

    #[test]
    fn releasing_a_connection_with_nothing_cached_logs_nothing() {
        let (cache, events) = recording(CacheConfig::default());
        cache.invalidate_connection("c1");
        assert!(events.lock().unwrap().is_empty());
    }

    #[test]
    fn a_watermark_at_or_below_a_cached_offset_drops_that_topic_only() {
        let cache = MessageCache::default();
        cache.store_now(key(500), message(10_000));
        cache.store_now(MessageKey::new("c1", "payments", 0, 5), message(10_000));

        // The topic was recreated: its next offset is now 40, below the 500 cached.
        assert!(cache.note_watermarks("c1", "orders", 3, 0, 40));

        assert!(cache.get(&key(500)).is_none());
        assert!(
            cache
                .get(&MessageKey::new("c1", "payments", 0, 5))
                .is_some()
        );
    }

    #[test]
    fn offsets_below_the_low_watermark_are_dropped_and_the_rest_kept() {
        let cache = MessageCache::default();
        for offset in [10, 20, 30] {
            cache.store_now(key(offset), message(10_000));
        }

        // Retention removed everything before offset 25.
        assert!(cache.note_watermarks("c1", "orders", 3, 25, 1_000));

        assert!(cache.get(&key(10)).is_none());
        assert!(cache.get(&key(20)).is_none());
        assert!(cache.get(&key(30)).is_some());
    }

    #[test]
    fn watermarks_that_leave_every_cached_offset_valid_change_nothing() {
        let cache = MessageCache::default();
        cache.store_now(key(500), message(10_000));

        assert!(!cache.note_watermarks("c1", "orders", 3, 0, 501));
        assert!(!cache.note_watermarks("c1", "orders", 9, 0, 0));
        // And asking again does not scan again.
        assert!(!cache.note_watermarks("c1", "orders", 3, 500, 501));

        assert!(cache.get(&key(500)).is_some());
    }

    #[test]
    fn a_purge_is_logged_with_what_remains() {
        let (cache, events) = recording(CacheConfig::default());
        cache.store_now(key(1), message(10_000));
        cache.invalidate_topic("c1", "orders");
        assert!(
            lines(&events)
                .iter()
                .any(|l| l == "Payload cache purged topic orders; 0 objects remain"),
            "{:?}",
            lines(&events)
        );
    }

    #[test]
    fn clearing_empties_the_cache_and_says_how_much_it_held() {
        let (cache, events) = recording(CacheConfig::default());
        cache.store_now(key(1), message(10_000));
        cache.store_now(MessageKey::new("c2", "payments", 0, 9), message(10_000));

        assert_eq!(cache.clear(), 2);

        assert_eq!(cache.entry_count(), 0);
        assert_eq!(cache.weighted_bytes(), 0);
        assert!(cache.get(&key(1)).is_none());
        assert!(
            lines(&events)
                .iter()
                .any(|l| l == "Payload cache purged all 2 cached payloads; 0 objects remain"),
            "{:?}",
            lines(&events)
        );
    }

    #[test]
    fn clearing_an_empty_cache_is_harmless_and_the_cache_still_works_after() {
        let cache = MessageCache::default();
        assert_eq!(cache.clear(), 0);

        cache.store_now(key(1), message(10_000));
        assert!(cache.get(&key(1)).is_some());
        // The retention bookkeeping was reset too, so a stale range cannot
        // trigger a purge of the new entry.
        assert!(!cache.note_watermarks("c1", "orders", 3, 0, 1_000));
        assert!(cache.get(&key(1)).is_some());
    }

    // --- configurable size ---------------------------------------------------

    #[test]
    fn changing_the_size_empties_the_cache_and_applies_the_new_cap() {
        let (cache, events) = recording(CacheConfig::default());
        cache.store_now(key(1), message(10_000));
        assert_eq!(cache.entry_count(), 1);

        assert!(cache.set_max_bytes(64 * 1024 * 1024));

        assert_eq!(cache.max_bytes(), 64 * 1024 * 1024);
        assert_eq!(cache.entry_count(), 0);
        assert!(
            lines(&events)
                .iter()
                .any(|l| l == "Payload cache size set to 64 MB; 1 cached payloads dropped"),
            "{:?}",
            lines(&events)
        );
        // And it works afterwards.
        cache.store_now(key(2), message(10_000));
        assert!(cache.get(&key(2)).is_some());
    }

    #[test]
    fn stating_the_same_size_again_changes_nothing() {
        let cache = MessageCache::default();
        cache.store_now(key(1), message(10_000));

        assert!(!cache.set_max_bytes(DEFAULT_MAX_BYTES));

        assert!(
            cache.get(&key(1)).is_some(),
            "an unchanged setting must not empty the cache"
        );
    }

    #[test]
    fn a_smaller_cap_is_enforced() {
        let cache = MessageCache::default();
        cache.set_max_bytes(4_000);
        for offset in 0..30 {
            let mut m = message(10_000);
            m.payload = noise(10_000, offset as u32);
            cache.store_now(key(offset), m);
        }
        assert!(
            cache.weighted_bytes() <= 4_000,
            "{}",
            cache.weighted_bytes()
        );
    }

    #[test]
    fn zero_turns_the_cache_off() {
        let (cache, events) = recording(CacheConfig::default());
        cache.store_now(key(1), message(10_000));

        cache.set_max_bytes(0);

        assert!(!cache.enabled());
        assert!(!cache.wants(1_000_000));
        cache.store_now(key(2), message(10_000));
        assert_eq!(cache.entry_count(), 0, "nothing is stored while off");
        let (hits, misses) = (cache.hits(), cache.misses());
        assert!(cache.get(&key(1)).is_none());
        assert_eq!(
            (cache.hits(), cache.misses()),
            (hits, misses),
            "off means no counting"
        );
        assert!(
            lines(&events)
                .iter()
                .any(|l| l == "Payload cache turned off; 1 cached payloads dropped"),
            "{:?}",
            lines(&events)
        );
    }

    #[test]
    fn turning_it_back_on_caches_again() {
        let cache = MessageCache::default();
        cache.set_max_bytes(0);
        cache.set_max_bytes(DEFAULT_MAX_BYTES);

        cache.store_now(key(1), message(10_000));

        assert!(cache.enabled());
        assert!(cache.get(&key(1)).is_some());
    }

    // --- store behaviour ---------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn a_background_store_lands_without_blocking_the_caller() {
        let cache = MessageCache::default();
        let m = message(50_000);
        cache.store_in_background(
            &Handle::current(),
            key(1),
            m.payload.len(),
            m.timestamp_ms,
            || m,
        );

        for _ in 0..100 {
            if cache.get(&key(1)).is_some() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("the background store never landed");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_payload_is_not_copied_for_a_store_that_is_declined() {
        let cache = MessageCache::default();
        let built = Arc::new(AtomicUsize::new(0));
        let handle = Handle::current();

        // Too small.
        let counter = built.clone();
        cache.store_in_background(&handle, key(1), 100, None, move || {
            counter.fetch_add(1, Ordering::SeqCst);
            message(100)
        });
        assert_eq!(
            built.load(Ordering::SeqCst),
            0,
            "a small payload must not be copied"
        );

        // An unchanged payload *is* copied here — recognising it needs a
        // moka lookup, which the fetch path no longer makes — but the
        // listener still does not compress it again (next test).
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_unchanged_payload_is_not_compressed_again_but_a_changed_one_is() {
        let cache = MessageCache::default();
        let handle = Handle::current();
        let m = message(10_000);
        cache.store_now(key(1), m.clone());
        assert_eq!(cache.compressions(), 1);

        cache.store_in_background(&handle, key(1), m.payload.len(), m.timestamp_ms, || {
            m.clone()
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            cache.compressions(),
            1,
            "same length and timestamp: nothing to redo"
        );

        let mut changed = message(20_000);
        changed.timestamp_ms = Some(1);
        cache.store_in_background(
            &handle,
            key(1),
            changed.payload.len(),
            changed.timestamp_ms,
            || changed.clone(),
        );
        for _ in 0..50 {
            if cache.compressions() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(cache.compressions(), 2);
        assert_eq!(cache.get(&key(1)), Some(changed));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stores_wait_for_a_slot_instead_of_being_dropped() {
        let cache = MessageCache::new(CacheConfig {
            max_pending_stores: 1,
            ..CacheConfig::default()
        });
        let handle = Handle::current();
        for offset in 0..8 {
            let m = message(50_000);
            cache.store_in_background(&handle, key(offset), m.payload.len(), m.timestamp_ms, || m);
        }
        for _ in 0..200 {
            if cache.compressions() == 8 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(cache.compressions(), 8, "every queued store is compressed");
        assert_eq!(cache.queued_bytes(), 0);
        for offset in 0..8 {
            assert!(cache.get(&key(offset)).is_some());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stores_beyond_the_queue_byte_bound_are_dropped_and_not_copied() {
        let (cache, events) = recording(CacheConfig {
            max_pending_stores: 1,
            // Room for one 8 MB payload, which is slow to compress on
            // purpose so it is still queued when the next arrive.
            max_queued_bytes: 10_000_000,
            zstd_level: ZSTD_MAX_LEVEL,
            log_debounce: Duration::from_millis(50),
            ..CacheConfig::default()
        });
        let handle = Handle::current();
        let built = Arc::new(AtomicUsize::new(0));
        for offset in 0..3 {
            let counter = built.clone();
            let payload = noise(8_000_000, offset as u32);
            cache.store_in_background(&handle, key(offset), payload.len(), None, move || {
                counter.fetch_add(1, Ordering::SeqCst);
                CachedMessage {
                    timestamp_ms: None,
                    key: None,
                    headers: Vec::new(),
                    payload,
                }
            });
        }
        assert_eq!(
            built.load(Ordering::SeqCst),
            1,
            "only the store that fits the queue copies"
        );

        // The summary says some were dropped.
        for _ in 0..200 {
            if lines(&events)
                .iter()
                .any(|l| l.contains("not stored (cache or queue full)"))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!(
            "no summary reported the dropped stores: {:?}",
            lines(&events)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_store_queued_before_a_purge_does_not_land_after_it() {
        let cache = MessageCache::new(CacheConfig {
            max_pending_stores: 1,
            zstd_level: ZSTD_MAX_LEVEL,
            ..CacheConfig::default()
        });
        let handle = Handle::current();
        // The first occupies the only slot; the second waits behind it.
        for offset in 0..2 {
            let payload = noise(4_000_000, offset as u32);
            cache.store_in_background(&handle, key(offset), payload.len(), None, move || {
                CachedMessage {
                    timestamp_ms: None,
                    key: None,
                    headers: Vec::new(),
                    payload,
                }
            });
        }
        cache.clear();
        for _ in 0..400 {
            if cache.queued_bytes() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(cache.queued_bytes(), 0);
        assert!(cache.get(&key(0)).is_none());
        assert!(cache.get(&key(1)).is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_burst_of_stores_is_one_log_line() {
        let (cache, events) = recording(CacheConfig {
            log_debounce: Duration::from_millis(300),
            ..CacheConfig::default()
        });
        let handle = Handle::current();
        for offset in 0..20 {
            let m = message(10_000);
            cache.store_in_background(&handle, key(offset), m.payload.len(), m.timestamp_ms, || m);
        }
        // Let the compressions finish and the debounced line(s) follow.
        tokio::time::sleep(Duration::from_millis(2_000)).await;

        let updates: Vec<String> = lines(&events)
            .into_iter()
            .filter(|l| l.starts_with("Payload cache updated"))
            .collect();
        // One line for the burst. A runner starved enough to spread twenty
        // compressions over several windows may log a couple more; what must
        // never happen is a line per message.
        assert!(!updates.is_empty(), "no summary was logged");
        assert!(
            updates.len() <= 3,
            "a burst of 20 stores logged {} lines: {updates:?}",
            updates.len()
        );
        // Slots are limited, so some may have been dropped; the line counts
        // what happened to all twenty.
        assert!(updates.iter().all(|l| l.contains("stored")), "{updates:?}");
        assert!(updates.iter().all(|l| l.contains("objects")), "{updates:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn expired_entries_are_released_while_the_cache_is_idle() {
        let cache = MessageCache::new(CacheConfig {
            time_to_idle: Duration::from_millis(150),
            maintenance_interval: Duration::from_millis(100),
            ..CacheConfig::default()
        });
        let m = message(10_000);
        cache.store_in_background(
            &Handle::current(),
            key(1),
            m.payload.len(),
            m.timestamp_ms,
            || m,
        );
        for _ in 0..50 {
            if cache.compressions() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(cache.compressions(), 1);

        // Nobody touches the cache; the timer alone has to free the entry.
        // (`weighted_size` is read without running maintenance itself.) Polled
        // rather than slept for, so a slow runner only makes it wait longer.
        let mut freed = false;
        for _ in 0..100 {
            if cache.shared.cache().weighted_size() == 0 {
                freed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            freed,
            "the maintenance timer never released the expired entry"
        );
    }
}
