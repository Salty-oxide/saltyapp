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
//!   [`DEFAULT_MAX_BYTES`] (1 GiB). Past the cap moka's own policy decides —
//!   TinyLFU admission with LRU eviction — so a new entry may be refused
//!   rather than displace something more valuable, and the app then behaves
//!   as it did without a cache.
//! * **Expiry:** time-to-*idle*, [`DEFAULT_TIME_TO_IDLE`] (15 minutes). Every
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
//! * **Cost:** compression runs off the caller's thread, at most
//!   [`MAX_PENDING_STORES`] at a time. A store that finds them all busy is
//!   dropped rather than queued — queueing would hold a copy of every
//!   payload in a fast fetch, which is the memory the cap exists to bound. The
//!   copy of the payload is only made once a slot is free, and a payload
//!   already cached unchanged is not compressed again.
//! * **Logging:** one summary line per burst of stores, not one per message —
//!   a 1,000-row fetch would otherwise write 1,000 lines.
//! * **Memory:** moka only expires entries while it is being used, so a timer
//!   runs its maintenance; otherwise an idle app would hold expired payloads
//!   indefinitely.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, Weak};
use std::time::Duration;

use moka::sync::Cache;
use tokio::runtime::Handle;
use tokio::sync::Semaphore;

/// Payloads up to this size are not cached: the UI already holds them.
pub const INLINE_PAYLOAD_BYTES: usize = 4 * 1024;

/// Compressed bytes the cache may hold.
pub const DEFAULT_MAX_BYTES: u64 = 1024 * 1024 * 1024;

/// How long an entry may go unused before it expires.
pub const DEFAULT_TIME_TO_IDLE: Duration = Duration::from_secs(15 * 60);

/// zstd's highest level. Measured at ~1.3 MB/s and no smaller than level 19 on
/// JSON, so it is available but not the default.
pub const ZSTD_MAX_LEVEL: i32 = 22;

/// The level used unless configured otherwise: ~11x on JSON at ~50 MB/s, where
/// level 22 is ~13x at ~1.3 MB/s. Decompression is ~2 ms per 4 MB at any level,
/// so the click path does not care.
pub const ZSTD_DEFAULT_LEVEL: i32 = 10;

/// Compressions allowed in flight at once.
pub const MAX_PENDING_STORES: usize = 4;

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
                    format!(", {skipped} not stored (compressor busy)")
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
    pub max_pending_stores: usize,
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
            log_debounce: LOG_DEBOUNCE,
            maintenance_interval: MAINTENANCE_INTERVAL,
        }
    }
}

struct Entry {
    compressed: Box<[u8]>,
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

struct Shared {
    /// Swapped when the capacity changes: moka cannot resize in place.
    cache: RwLock<Cache<MessageKey, Arc<Entry>>>,
    /// The byte cap in force; `0` means the cache is off.
    max_bytes: AtomicU64,
    config: CacheConfig,
    ranges: Mutex<HashMap<PartitionId, CachedRange>>,
    pending: Arc<Semaphore>,
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

fn build_cache(config: &CacheConfig, max_bytes: u64) -> Cache<MessageKey, Arc<Entry>> {
    Cache::builder()
        // A cap of 0 is "off" and never reaches the weigher (nothing is
        // stored); moka itself is given a floor so it is always valid.
        .max_capacity(max_bytes.max(1))
        .weigher(|_key: &MessageKey, entry: &Arc<Entry>| entry.weight())
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
        let pending = std::mem::take(&mut *lock(&self.log));
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
        Self {
            shared: Arc::new(Shared {
                cache: RwLock::new(build_cache(&config, max_bytes)),
                max_bytes: AtomicU64::new(max_bytes),
                ranges: Mutex::new(HashMap::new()),
                pending: Arc::new(Semaphore::new(config.max_pending_stores.max(1))),
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
            *cache = build_cache(&self.shared.config, max_bytes);
            dropped
        };
        lock(&self.shared.ranges).clear();
        self.shared.emit(CacheEvent::Resized { max_bytes, dropped });
        true
    }

    /// Whether a payload of this size belongs in the cache at all.
    pub fn wants(&self, payload_len: usize) -> bool {
        self.enabled() && payload_len > self.shared.config.min_payload_bytes
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
        self.shared.cache().get(key).is_some_and(|entry| {
            entry.payload_len == payload_len && entry.timestamp_ms == timestamp_ms
        })
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
            match zstd::bulk::decompress(&entry.compressed, entry.payload_len) {
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

    /// Stores a message — compressing on a blocking thread of `runtime` — and
    /// returns at once.
    ///
    /// `make` builds the entry and is called **only if** the store will go
    /// ahead: the payload is not copied for a store that is declined. It is
    /// declined for a payload at or under the threshold, one already cached
    /// unchanged ([`Self::is_current`]), and — counted in the next summary —
    /// when every compression slot is busy.
    pub fn store_in_background(
        &self,
        runtime: &Handle,
        key: MessageKey,
        payload_len: usize,
        timestamp_ms: Option<i64>,
        make: impl FnOnce() -> CachedMessage,
    ) {
        if !self.wants(payload_len) || self.is_current(&key, payload_len, timestamp_ms) {
            return;
        }
        self.start_maintenance(runtime);
        let Ok(permit) = self.shared.pending.clone().try_acquire_owned() else {
            lock(&self.shared.log).skipped += 1;
            self.schedule_log(runtime);
            return;
        };
        let message = make();
        let this = self.clone();
        let handle = runtime.clone();
        runtime.spawn_blocking(move || {
            this.write(key, message);
            drop(permit);
            this.schedule_log(&handle);
        });
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
        let Ok(compressed) = zstd::bulk::compress(&message.payload, self.shared.config.zstd_level)
        else {
            return;
        };
        let entry = Entry {
            payload_len: message.payload.len(),
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
        self.shared.compressions.fetch_add(1, Ordering::Relaxed);
        self.shared.cache().insert(key, Arc::new(entry));
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
                shared.cache().run_pending_tasks();
            }
        });
    }

    fn purge(&self, what: String, predicate: impl Fn(&MessageKey) -> bool + Send + Sync + 'static) {
        // Only fails if invalidation closures were not enabled, which `build`
        // always does.
        let _ = self
            .shared
            .cache()
            .invalidate_entries_if(move |key, _| predicate(key));
        self.shared.cache().run_pending_tasks();
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
        self.shared.cache().run_pending_tasks();
        let cleared = self.shared.cache().entry_count();
        lock(&self.shared.ranges).clear();
        self.shared.cache().invalidate_all();
        self.shared.cache().run_pending_tasks();
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

        // Already cached, unchanged.
        let m = message(10_000);
        cache.store_now(key(2), m.clone());
        let counter = built.clone();
        cache.store_in_background(
            &handle,
            key(2),
            m.payload.len(),
            m.timestamp_ms,
            move || {
                counter.fetch_add(1, Ordering::SeqCst);
                m
            },
        );
        assert_eq!(
            built.load(Ordering::SeqCst),
            0,
            "an unchanged payload must not be copied"
        );
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
    async fn stores_beyond_the_pending_limit_are_dropped_and_not_copied() {
        let (cache, events) = recording(CacheConfig {
            max_pending_stores: 1,
            // Slow on purpose, so the permit is still held when the next
            // store arrives.
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
            "only the store that got a slot copies"
        );

        // The summary says some were dropped.
        for _ in 0..200 {
            if lines(&events)
                .iter()
                .any(|l| l.contains("not stored (compressor busy)"))
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
