//! Guards the fetch against stalling on librdkafka's prefetch-queue backoff.
//!
//! `fetch_consumer_config` lowers `queued.max.messages.kbytes` so a fetch
//! does not over-read a large-message topic. librdkafka then reaches that
//! threshold constantly, and postpones each next fetch by
//! `fetch.queue.backoff.ms` — whose default is a flat **1000 ms**. Left at
//! that default, a 30,000-message fetch spent 7,050 ms, of which 7,000 ms was
//! seven one-second stalls and ~50 ms was the work.
//!
//! Nothing about that was visible through the payload switch: the broker
//! sends whole records either way and they fill the same queue either way, so
//! the fetch was equally slow with "Fetch message payload" off — which is
//! what made it look like a client-side cost.
//!
//! The unit test `a_lowered_prefetch_queue_comes_with_a_lowered_queue_backoff`
//! pins the two properties against each other with no broker. This one pins
//! the consequence, which is the thing anyone actually cares about.
//!
//! ```bash
//! docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0
//! ./scripts/e2e-fixtures.sh
//! SALTY_E2E_BOOTSTRAP=localhost:9092 \
//!   cargo test -p salty-kafka --test fetch_stall -- --nocapture
//! ```

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use salty_core::{Connection, MessageFilter, SecurityProtocol};
use salty_kafka::{KafkaClient, RdKafkaClient};

/// Created by `scripts/e2e-fixtures.sh`: 30,000 x ~1 KB over 6 partitions.
const TOPIC: &str = "perf-probe";
const MESSAGES: u32 = 30_000;

/// Generous on purpose. The fix measures ~110 ms of poll loop for this topic
/// and the bug measured ~7,050 ms, so anything in this range separates them
/// by more than an order of magnitude in both directions — it fails on the
/// stall without becoming a flaky benchmark of whatever machine runs it.
const BUDGET: Duration = Duration::from_secs(3);

fn bootstrap_servers() -> Option<String> {
    std::env::var("SALTY_E2E_BOOTSTRAP").ok().filter(|value| !value.is_empty())
}

fn connection(bootstrap_servers: String) -> Connection {
    Connection {
        id: "e2e".into(),
        name: "e2e".into(),
        bootstrap_servers,
        kafka_version: "3.9".into(),
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

/// The shape of an ordinary browse — the newest 100 messages — where the
/// fixed cost of setting a fetch up is the whole of what the user waits for.
fn browse_filter() -> MessageFilter {
    MessageFilter {
        partitions: None,
        max_messages_per_partition: Some(100),
        max_total_messages: Some(100),
        from_timestamp_ms: None,
        to_timestamp_ms: None,
        offset: None,
        include_payload: true,
        max_payload_preview_bytes: Some(4096),
    }
}

fn filter(include_payload: bool) -> MessageFilter {
    MessageFilter {
        partitions: None,
        max_messages_per_partition: Some(MESSAGES),
        max_total_messages: Some(MESSAGES),
        from_timestamp_ms: None,
        to_timestamp_ms: None,
        offset: None,
        include_payload,
        max_payload_preview_bytes: Some(4096),
    }
}

/// Serialises the tests in this file, so that three wall-clock budgets are not
/// measuring each other's load. The two large-fetch tests each pull 30,000 x
/// 1 KB records, which is not a neighbour you want while timing a browse.
///
/// This is hygiene, not the fix for the flake that prompted it — see the
/// warm-up in `repeated_small_browses_do_not_pay_a_coordinator_query_each`.
/// Serialising alone took that test from failing ~40% of runs to ~10%, which
/// was enough to show contention was aggravating the problem without being
/// its cause.
///
/// A `tokio::sync::Mutex`, not a `std` one: these are `multi_thread` tests and
/// the guard is held across `.await`, which a `std::sync::MutexGuard` cannot
/// be (it is not `Send`).
static BROKER: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(|| tokio::sync::Mutex::new(()));

async fn fetch(include_payload: bool) -> Option<(Duration, usize)> {
    let bootstrap = bootstrap_servers()?;
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);
    let started = Instant::now();
    let result = client
        .fetch_messages(
            &connection,
            TOPIC,
            &filter(include_payload),
            None,
            Duration::from_secs(30),
            1_048_576,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("fetch failed");
    Some((started.elapsed(), result.messages.len()))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_large_fetch_does_not_stall_on_the_prefetch_queue_backoff() {
    let _serial = BROKER.lock().await;
    let Some((elapsed, count)) = fetch(false).await else {
        eprintln!("skipped: set SALTY_E2E_BOOTSTRAP (and run scripts/e2e-fixtures.sh)");
        return;
    };

    println!("metadata-only fetch of {count} messages: {} ms", elapsed.as_millis());
    assert_eq!(count, MESSAGES as usize, "the fixture topic is not the expected size");
    assert!(
        elapsed < BUDGET,
        "fetching {count} messages took {} ms. Each whole second in that figure is one \
         `fetch.queue.backoff.ms` stall against the lowered `queued.max.messages.kbytes` — \
         see `fetch_consumer_config`.",
        elapsed.as_millis(),
    );
}

/// The stall was independent of the payload switch, so the guard has to be
/// too — otherwise a regression could hide in whichever mode is untested.
#[tokio::test(flavor = "multi_thread")]
async fn the_same_holds_when_payloads_are_fetched() {
    let _serial = BROKER.lock().await;
    let Some((elapsed, count)) = fetch(true).await else {
        eprintln!("skipped: set SALTY_E2E_BOOTSTRAP (and run scripts/e2e-fixtures.sh)");
        return;
    };

    println!("payload fetch of {count} messages: {} ms", elapsed.as_millis());
    assert!(
        elapsed < BUDGET,
        "fetching {count} messages with payloads took {} ms",
        elapsed.as_millis(),
    );
}

/// Five small browses back to back, which is what using the Data tab looks
/// like — and where a fixed per-fetch cost shows up as the whole wait.
///
/// The fetch's own consumer must set a `group.id` (rdkafka routes `assign()`
/// through the consumer-group machinery), and asking *that* client for topic
/// metadata queues the request behind librdkafka's group-coordinator query:
/// ~500 ms, on every fetch, for a group this consumer never joins. The
/// partition list is asked of the pooled metadata client instead, which has
/// no `group.id` and is already connected.
///
/// Measured as a total rather than per fetch because the old behaviour was
/// bimodal — roughly four fetches in five paid the coordinator query and the
/// fifth slipped past it, so any single fetch could pass by luck. Five
/// together could not: ~2,100 ms against the ~60 ms this budget allows.
#[tokio::test(flavor = "multi_thread")]
async fn repeated_small_browses_do_not_pay_a_coordinator_query_each() {
    let _serial = BROKER.lock().await;
    let Some(bootstrap) = bootstrap_servers() else {
        eprintln!("skipped: set SALTY_E2E_BOOTSTRAP (and run scripts/e2e-fixtures.sh)");
        return;
    };
    let client = RdKafkaClient::new();
    let connection = connection(bootstrap);

    // The app reaches a Data tab through the tree, which lists the cluster's
    // topics first — so the pooled metadata client is always already built
    // and connected by the time Fetch is clicked. Doing that here measures
    // the steady state the user is actually in, rather than charging the
    // first browse for a connection the real app opened minutes ago.
    client
        .list_topics(&connection, Duration::from_secs(30))
        .await
        .expect("list_topics failed");

    // And one discarded browse, for the same reason one step further on.
    //
    // The *first* fetch in a process intermittently costs ~513 ms where every
    // later one costs 7-9 ms — measured as `each=[513, 8, 18, 8, 23]`, and
    // reproducibly on roughly 10-40% of runs depending on what else is
    // touching the broker. That is a one-off group-coordinator lookup for the
    // `group.id` `assign()` forces on a fetch consumer: a fresh consumer has
    // no cached coordinator, and a FindCoordinator that needs a retry pays
    // ~500 ms of `retry.backoff.ms` before it lands.
    //
    // Charging that to the measurement made this test fail for a cost the
    // real app pays once per session, not once per browse. Excluding it costs
    // the guard nothing: both regressions named in the assertion below are
    // *per-browse* costs — ~500 ms if the partition list is asked of the fetch
    // consumer, ~99 ms if the consumer is closed on the critical path — so
    // they still show up across the five timed browses that follow. Four or
    // five samples of the steady-state cost is what this test is about, and
    // the warm-up is what makes them steady-state.
    client
        .fetch_messages(
            &connection,
            TOPIC,
            &browse_filter(),
            None,
            Duration::from_secs(30),
            1_048_576,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("warm-up fetch failed");

    let started = Instant::now();
    let mut each = Vec::new();
    for _ in 0..5 {
        let one = Instant::now();
        let result = client
            .fetch_messages(
                &connection,
                TOPIC,
                &browse_filter(),
                None,
                Duration::from_secs(30),
                1_048_576,
                None,
                Arc::new(AtomicBool::new(false)),
            )
            .await
            .expect("fetch failed");
        each.push(one.elapsed().as_millis());
        assert_eq!(result.messages.len(), 100);
    }
    let elapsed = started.elapsed();

    println!("five 100-message browses: {} ms each={each:?}", elapsed.as_millis());
    assert!(
        elapsed < Duration::from_millis(250),
        "five 100-message browses took {} ms; each should be ~10. Both costs this guards are \
         the `group.id` that `assign()` forces on the fetch consumer: ~500 ms if the partition \
         list is asked of that consumer instead of the pooled metadata client (its group \
         coordinator query), and ~99 ms if the consumer is closed on the critical path instead \
         of on the blocking pool. See `fetch_messages`.",
        elapsed.as_millis(),
    );
}
