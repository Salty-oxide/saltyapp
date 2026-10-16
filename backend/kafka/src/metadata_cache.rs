//! A time-bounded cache of the cluster data the tree and the Metrics views are
//! built from: the broker, topic and consumer group listings, the ACL listings,
//! and per-topic partition layout (with watermarks) and config.
//!
//! Both are two broker round trips per topic, the cluster Metrics tab reads
//! every topic's, and neither changes often — so a read is served from here
//! for [`METADATA_TTL`] unless the caller asks for a refresh. It lives beside
//! the payload cache on `RdKafkaClient`, is dropped with a connection's
//! session ([`MetadataCache::invalidate_connection`]), and only ever stores a
//! *successful* read.
//!
//! The clock is a parameter so every rule is tested without sleeping.
//! Pure: knows nothing of Kafka clients.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use salty_core::{
    AclListing, BrokerSummary, Cached, ConfigEntry, ConsumerGroupSummary, PartitionSummary,
    TopicSummary,
};

/// How long an entry is served without asking the broker: 24 hours.
pub const METADATA_TTL_MS: u64 = 24 * 60 * 60 * 1_000;

/// `(connection id, topic)`.
type Key = (String, String);

/// A connection's cache generation as of the moment a broker read began.
///
/// A read takes one *before* it asks the broker and hands it back when it
/// stores the answer; the store is dropped if the connection was purged in
/// between. Without it, a read in flight when the session ended (Disconnect,
/// an edit, a tripped breaker) would land after `invalidate_connection` and
/// serve the old cluster's data for the whole TTL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Session(Option<u64>);

impl Session {
    /// Stores unconditionally — for seeding the cache and for tests.
    pub const ANY: Session = Session(None);
}

struct Store<T> {
    entries: HashMap<Key, Cached<T>>,
}

impl<T: Clone> Store<T> {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    fn drop_connection(&mut self, connection_id: &str) {
        self.entries.retain(|(id, _), _| id != connection_id);
    }

    fn get(&mut self, key: &Key, now_ms: u64) -> Option<Cached<T>> {
        let entry = self.entries.get(key)?;
        let expired = now_ms.saturating_sub(entry.fetched_at_ms) >= METADATA_TTL_MS;
        if expired {
            self.entries.remove(key);
            return None;
        }
        self.entries.get(key).cloned()
    }
}

#[derive(Default)]
pub struct MetadataCache {
    inner: Mutex<Option<Inner>>,
}

/// Cluster-wide data is keyed by connection alone: its scope is [`CLUSTER`].
const CLUSTER: &str = "";

struct Inner {
    /// Bumped by every purge of a connection (and by `clear` for all of them,
    /// via `epoch`), so a read that began earlier can tell it is stale.
    generations: HashMap<String, u64>,
    epoch: u64,
    partitions: Store<Vec<PartitionSummary>>,
    configs: Store<Vec<ConfigEntry>>,
    brokers: Store<Vec<BrokerSummary>>,
    topics: Store<Vec<TopicSummary>>,
    groups: Store<Vec<ConsumerGroupSummary>>,
    acls: Store<AclListing>,
    /// Keyed `(connection, "<resource type>:<name>")`.
    resource_acls: Store<AclListing>,
}

impl Inner {
    fn generation(&self, connection_id: &str) -> u64 {
        // `epoch` only grows, as does each connection's own count, so their
        // sum changes whenever either is bumped.
        self.epoch + self.generations.get(connection_id).copied().unwrap_or(0)
    }

    fn len(&self) -> usize {
        self.partitions.entries.len()
            + self.configs.entries.len()
            + self.brokers.entries.len()
            + self.topics.entries.len()
            + self.groups.entries.len()
            + self.acls.entries.len()
            + self.resource_acls.entries.len()
    }
}

impl MetadataCache {
    fn lock(&self) -> MutexGuard<'_, Option<Inner>> {
        // A panic while holding the lock cannot leave a half-written entry
        // (each mutation is one map operation), so a poisoned lock is safe to
        // use — and the pooled clients recover from poisoning the same way.
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn with<R>(&self, f: impl FnOnce(&mut Inner) -> R) -> R {
        let mut guard = self.lock();
        let inner = guard.get_or_insert_with(|| Inner {
            generations: HashMap::new(),
            epoch: 0,
            partitions: Store::new(),
            configs: Store::new(),
            brokers: Store::new(),
            topics: Store::new(),
            groups: Store::new(),
            acls: Store::new(),
            resource_acls: Store::new(),
        });
        f(inner)
    }

    pub fn partitions(
        &self,
        connection_id: &str,
        topic: &str,
        now_ms: u64,
    ) -> Option<Cached<Vec<PartitionSummary>>> {
        self.with(|c| c.partitions.get(&key(connection_id, topic), now_ms))
    }

    pub fn store_partitions(
        &self,
        connection_id: &str,
        topic: &str,
        value: Vec<PartitionSummary>,
        now_ms: u64,
        session: Session,
    ) -> Cached<Vec<PartitionSummary>> {
        self.write(
            |c| &mut c.partitions,
            connection_id,
            topic,
            value,
            now_ms,
            session,
        )
    }

    pub fn config(
        &self,
        connection_id: &str,
        topic: &str,
        now_ms: u64,
    ) -> Option<Cached<Vec<ConfigEntry>>> {
        self.with(|c| c.configs.get(&key(connection_id, topic), now_ms))
    }

    pub fn store_config(
        &self,
        connection_id: &str,
        topic: &str,
        value: Vec<ConfigEntry>,
        now_ms: u64,
        session: Session,
    ) -> Cached<Vec<ConfigEntry>> {
        self.write(
            |c| &mut c.configs,
            connection_id,
            topic,
            value,
            now_ms,
            session,
        )
    }

    fn read<T: Clone>(
        &self,
        pick: fn(&mut Inner) -> &mut Store<T>,
        connection_id: &str,
        scope: &str,
        now_ms: u64,
    ) -> Option<Cached<T>> {
        self.with(|c| pick(c).get(&key(connection_id, scope), now_ms))
    }

    fn write<T: Clone>(
        &self,
        pick: fn(&mut Inner) -> &mut Store<T>,
        connection_id: &str,
        scope: &str,
        value: T,
        now_ms: u64,
        session: Session,
    ) -> Cached<T> {
        let cached = Cached {
            value,
            fetched_at_ms: now_ms,
        };
        self.with(|c| {
            // A purge since the read began means its answer describes a
            // session that is over: hand it back, don't keep it.
            let current = session
                .0
                .is_none_or(|began| began == c.generation(connection_id));
            if current {
                pick(c)
                    .entries
                    .insert(key(connection_id, scope), cached.clone());
            }
        });
        cached
    }

    /// Call before asking the broker; pass the result to the matching
    /// `store_*`. See [`Session`].
    pub fn session(&self, connection_id: &str) -> Session {
        Session(Some(self.with(|c| c.generation(connection_id))))
    }

    pub fn brokers(&self, connection_id: &str, now_ms: u64) -> Option<Cached<Vec<BrokerSummary>>> {
        self.read(|c| &mut c.brokers, connection_id, CLUSTER, now_ms)
    }

    pub fn store_brokers(
        &self,
        connection_id: &str,
        value: Vec<BrokerSummary>,
        now_ms: u64,
        session: Session,
    ) -> Cached<Vec<BrokerSummary>> {
        self.write(
            |c| &mut c.brokers,
            connection_id,
            CLUSTER,
            value,
            now_ms,
            session,
        )
    }

    pub fn topics(&self, connection_id: &str, now_ms: u64) -> Option<Cached<Vec<TopicSummary>>> {
        self.read(|c| &mut c.topics, connection_id, CLUSTER, now_ms)
    }

    pub fn store_topics(
        &self,
        connection_id: &str,
        value: Vec<TopicSummary>,
        now_ms: u64,
        session: Session,
    ) -> Cached<Vec<TopicSummary>> {
        self.write(
            |c| &mut c.topics,
            connection_id,
            CLUSTER,
            value,
            now_ms,
            session,
        )
    }

    pub fn consumer_groups(
        &self,
        connection_id: &str,
        now_ms: u64,
    ) -> Option<Cached<Vec<ConsumerGroupSummary>>> {
        self.read(|c| &mut c.groups, connection_id, CLUSTER, now_ms)
    }

    pub fn store_consumer_groups(
        &self,
        connection_id: &str,
        value: Vec<ConsumerGroupSummary>,
        now_ms: u64,
        session: Session,
    ) -> Cached<Vec<ConsumerGroupSummary>> {
        self.write(
            |c| &mut c.groups,
            connection_id,
            CLUSTER,
            value,
            now_ms,
            session,
        )
    }

    /// The cluster-wide ACL listing.
    pub fn acls(&self, connection_id: &str, now_ms: u64) -> Option<Cached<AclListing>> {
        self.read(|c| &mut c.acls, connection_id, CLUSTER, now_ms)
    }

    pub fn store_acls(
        &self,
        connection_id: &str,
        value: AclListing,
        now_ms: u64,
        session: Session,
    ) -> Cached<AclListing> {
        self.write(
            |c| &mut c.acls,
            connection_id,
            CLUSTER,
            value,
            now_ms,
            session,
        )
    }

    /// The ACLs governing one resource. `resource` is whatever the caller uses
    /// to name it (type and name), opaque here.
    pub fn resource_acls(
        &self,
        connection_id: &str,
        resource: &str,
        now_ms: u64,
    ) -> Option<Cached<AclListing>> {
        self.read(|c| &mut c.resource_acls, connection_id, resource, now_ms)
    }

    pub fn store_resource_acls(
        &self,
        connection_id: &str,
        resource: &str,
        value: AclListing,
        now_ms: u64,
        session: Session,
    ) -> Cached<AclListing> {
        self.write(
            |c| &mut c.resource_acls,
            connection_id,
            resource,
            value,
            now_ms,
            session,
        )
    }

    /// Drops everything held for one connection — its session ended, so the
    /// cluster it described may not be the one it reconnects to.
    pub fn invalidate_connection(&self, connection_id: &str) {
        self.with(|c| {
            *c.generations.entry(connection_id.to_string()).or_insert(0) += 1;
            c.partitions.drop_connection(connection_id);
            c.configs.drop_connection(connection_id);
            c.brokers.drop_connection(connection_id);
            c.topics.drop_connection(connection_id);
            c.groups.drop_connection(connection_id);
            c.acls.drop_connection(connection_id);
            c.resource_acls.drop_connection(connection_id);
        });
    }

    /// Drops everything, for every connection; returns how many entries went.
    pub fn clear(&self) -> usize {
        self.with(|c| {
            c.epoch += 1;
            let n = c.len();
            c.partitions.entries.clear();
            c.configs.entries.clear();
            c.brokers.entries.clear();
            c.topics.entries.clear();
            c.groups.entries.clear();
            c.acls.entries.clear();
            c.resource_acls.entries.clear();
            n
        })
    }

    pub fn len(&self) -> usize {
        self.with(|c| c.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn key(connection_id: &str, topic: &str) -> Key {
    (connection_id.to_string(), topic.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partition(id: i32) -> PartitionSummary {
        PartitionSummary {
            id,
            leader: 1,
            replicas: vec![1],
            isr: vec![1],
            low_offset: 0,
            high_offset: 10,
        }
    }

    fn entry(name: &str) -> ConfigEntry {
        ConfigEntry {
            name: name.into(),
            value: Some("1".into()),
        }
    }

    #[test]
    fn ttl_is_24_hours() {
        assert_eq!(METADATA_TTL_MS, 86_400_000);
    }

    #[test]
    fn a_miss_before_anything_is_stored() {
        let cache = MetadataCache::default();
        assert!(cache.partitions("c", "t", 0).is_none());
        assert!(cache.config("c", "t", 0).is_none());
    }

    #[test]
    fn serves_what_was_stored_with_its_fetch_time_until_the_ttl() {
        let cache = MetadataCache::default();
        cache.store_partitions("c", "t", vec![partition(0)], 1_000, Session::ANY);
        let hit = cache
            .partitions("c", "t", 1_000 + METADATA_TTL_MS - 1)
            .unwrap();
        assert_eq!(hit.fetched_at_ms, 1_000);
        assert_eq!(hit.value, vec![partition(0)]);
    }

    #[test]
    fn expires_at_the_ttl_and_forgets_the_entry() {
        let cache = MetadataCache::default();
        cache.store_config("c", "t", vec![entry("a")], 1_000, Session::ANY);
        assert!(cache.config("c", "t", 1_000 + METADATA_TTL_MS).is_none());
        assert!(cache.is_empty());
    }

    #[test]
    fn keeps_connections_and_topics_apart() {
        let cache = MetadataCache::default();
        cache.store_partitions("c1", "t", vec![partition(1)], 0, Session::ANY);
        cache.store_partitions("c2", "t", vec![partition(2)], 0, Session::ANY);
        cache.store_partitions("c1", "u", vec![partition(3)], 0, Session::ANY);
        assert_eq!(cache.partitions("c1", "t", 1).unwrap().value[0].id, 1);
        assert_eq!(cache.partitions("c2", "t", 1).unwrap().value[0].id, 2);
        assert_eq!(cache.partitions("c1", "u", 1).unwrap().value[0].id, 3);
    }

    #[test]
    fn a_new_store_replaces_the_old_one_and_its_time() {
        let cache = MetadataCache::default();
        cache.store_partitions("c", "t", vec![partition(0)], 0, Session::ANY);
        cache.store_partitions("c", "t", vec![partition(9)], 500, Session::ANY);
        let hit = cache.partitions("c", "t", 600).unwrap();
        assert_eq!((hit.value[0].id, hit.fetched_at_ms), (9, 500));
    }

    #[test]
    fn invalidating_a_connection_drops_both_kinds_and_only_that_connection() {
        let cache = MetadataCache::default();
        cache.store_partitions("c1", "t", vec![partition(0)], 0, Session::ANY);
        cache.store_config("c1", "t", vec![entry("a")], 0, Session::ANY);
        cache.store_config("c2", "t", vec![entry("b")], 0, Session::ANY);

        cache.invalidate_connection("c1");

        assert!(cache.partitions("c1", "t", 1).is_none());
        assert!(cache.config("c1", "t", 1).is_none());
        assert!(cache.config("c2", "t", 1).is_some());
    }

    #[test]
    fn clear_empties_everything_and_counts_it() {
        let cache = MetadataCache::default();
        cache.store_partitions("c1", "t", vec![], 0, Session::ANY);
        cache.store_config("c2", "t", vec![], 0, Session::ANY);
        assert_eq!(cache.clear(), 2);
        assert!(cache.is_empty());
    }

    fn listing() -> AclListing {
        AclListing::no_authorizer()
    }

    #[test]
    fn cluster_listings_are_served_stored_and_expire_like_the_rest() {
        let cache = MetadataCache::default();
        cache.store_brokers(
            "c",
            vec![BrokerSummary {
                id: 1,
                host: "h".into(),
                port: 9092,
            }],
            10,
            Session::ANY,
        );
        cache.store_topics(
            "c",
            vec![TopicSummary {
                name: "t".into(),
                partition_count: 3,
            }],
            10,
            Session::ANY,
        );
        cache.store_consumer_groups(
            "c",
            vec![ConsumerGroupSummary {
                group_id: "g".into(),
                state: "Stable".into(),
            }],
            10,
            Session::ANY,
        );

        assert_eq!(cache.brokers("c", 11).unwrap().fetched_at_ms, 10);
        assert_eq!(cache.topics("c", 11).unwrap().value[0].name, "t");
        assert_eq!(
            cache.consumer_groups("c", 11).unwrap().value[0].group_id,
            "g"
        );
        assert!(cache.brokers("other", 11).is_none());

        let later = 10 + METADATA_TTL_MS;
        assert!(cache.brokers("c", later).is_none());
        assert!(cache.topics("c", later).is_none());
        assert!(cache.consumer_groups("c", later).is_none());
    }

    #[test]
    fn acl_listings_are_kept_per_connection_and_per_resource() {
        let cache = MetadataCache::default();
        cache.store_acls("c", listing(), 1, Session::ANY);
        cache.store_resource_acls("c", "topic:orders", listing(), 2, Session::ANY);

        assert_eq!(cache.acls("c", 3).unwrap().fetched_at_ms, 1);
        assert_eq!(
            cache
                .resource_acls("c", "topic:orders", 3)
                .unwrap()
                .fetched_at_ms,
            2
        );
        assert!(cache.resource_acls("c", "topic:other", 3).is_none());
        assert!(cache.resource_acls("c", "group:orders", 3).is_none());
    }

    #[test]
    fn invalidating_a_connection_drops_every_kind() {
        let cache = MetadataCache::default();
        cache.store_brokers("c1", vec![], 0, Session::ANY);
        cache.store_topics("c1", vec![], 0, Session::ANY);
        cache.store_consumer_groups("c1", vec![], 0, Session::ANY);
        cache.store_acls("c1", listing(), 0, Session::ANY);
        cache.store_resource_acls("c1", "topic:t", listing(), 0, Session::ANY);
        cache.store_topics("c2", vec![], 0, Session::ANY);

        cache.invalidate_connection("c1");

        assert_eq!(cache.len(), 1);
        assert!(cache.topics("c2", 1).is_some());
    }

    #[test]
    fn a_read_that_began_before_a_purge_is_not_stored_after_it() {
        let cache = MetadataCache::default();
        let session = cache.session("c");

        // The session ends while the broker read is still in flight...
        cache.invalidate_connection("c");
        // ...and its result arrives afterwards.
        let returned = cache.store_topics("c", vec![], 5, session);

        // The caller still gets its answer, but nothing outlives the session.
        assert_eq!(returned.fetched_at_ms, 5);
        assert!(cache.topics("c", 6).is_none());
    }

    #[test]
    fn a_read_that_began_after_a_purge_is_stored() {
        let cache = MetadataCache::default();
        cache.invalidate_connection("c");
        let session = cache.session("c");

        cache.store_topics("c", vec![], 5, session);

        assert!(cache.topics("c", 6).is_some());
    }

    #[test]
    fn a_purge_of_one_connection_does_not_discard_another_connections_read() {
        let cache = MetadataCache::default();
        let session = cache.session("c2");

        cache.invalidate_connection("c1");
        cache.store_brokers("c2", vec![], 1, session);

        assert!(cache.brokers("c2", 2).is_some());
    }

    #[test]
    fn clearing_everything_also_discards_reads_in_flight() {
        let cache = MetadataCache::default();
        let session = cache.session("c");

        cache.clear();
        cache.store_partitions("c", "t", vec![], 1, session);

        assert!(cache.partitions("c", "t", 2).is_none());
    }
}
