#!/usr/bin/env bash
#
# Fills a local broker with everything `backend/kafka/tests/*` reads.
#
# The existing PowerShell fixture scripts cover the compression and
# large-message topics on Windows; this is the portable equivalent, and it
# also creates the topics `cluster_reads.rs` needs — which the PowerShell
# ones predate.
#
#   docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0
#   ./scripts/e2e-fixtures.sh
#   SALTY_E2E_BOOTSTRAP=localhost:9092 cargo test -p salty-kafka
#
# Every step is idempotent, so re-running it against a broker that already
# has the fixtures is harmless.
set -euo pipefail

CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
K="/opt/kafka/bin"

kexec() { docker exec "$CONTAINER" "$@"; }
kexec_i() { docker exec -i "$CONTAINER" "$@"; }

topic() {
  reset_if_stale "$1"
  kexec "$K/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1 >/dev/null
}

# Deletes a topic that still has offsets but no longer has any messages, so it
# comes back numbering from zero.
#
# Re-seeding alone is not enough after retention has expired a topic's
# segments. The offsets do not rewind: produce 60 records into an `e2e-basic`
# whose partitions have advanced to 16/23/21 and the data lands at 16..76, so
# every partition's *earliest* readable offset is now well past zero. Several
# tests are written against the offsets a freshly-created topic gives —
# `cluster_reads.rs`'s `an_offset_filter_starts_each_partition_at_that_offset`
# asks for `offset: Some(5)` and expects it to land inside the data, but 5 is
# below every partition's base, so the filter clamps up to the watermark,
# returns the whole topic, and the test fails on `from_five.len() < all.len()`.
#
# Deleting first restores what a fresh `docker run` would have given. Only
# ever applied to a topic that holds nothing, so no fixture data is discarded.
reset_if_stale() {
  local latest
  latest=$(offsets_at "$1" -1)
  if [ "$latest" -gt 0 ] && [ "$(messages_in "$1")" -eq 0 ]; then
    echo "    (recreating $1: $latest offsets but no messages left — retention expired it)"
    kexec "$K/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$1" >/dev/null 2>&1 || true
    # Deletion is asynchronous; wait for the name to disappear before the
    # create below races it and gets refused.
    for _ in $(seq 1 30); do
      kexec "$K/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list 2>/dev/null \
        | grep -qx "$1" || break
      sleep 1
    done
  fi
}

# Retrievable messages across a topic's partitions; 0 for a topic with none.
#
# Creating a topic is idempotent (`--if-not-exists`) but *producing* to one is
# not, and only the two expensive perf fixtures below used to check. Re-running
# the script therefore appended a second copy of every cheap fixture — 60
# records in `e2e-basic` became 120, and `cluster_reads.rs`, which asserts on
# exact contents, failed against a broker that had merely been seeded twice.
# That is a confusing way to spend an afternoon, so every produce step now
# gates on this.
#
# **Latest minus earliest, not the sum of latest.** A broker left running for
# days expires its own segments: once retention deletes them the earliest
# offset advances to meet the latest, so a topic holding *nothing* still
# reports a high watermark of 60. Summing latest alone therefore made this
# script unable to re-seed a broker it had seeded a week earlier — the gate
# saw 60, skipped every produce step, and every test reading the topic failed
# on "fixture topic is empty" with no way to recover but deleting the
# container. Subtracting earliest is what makes re-seeding actually idempotent
# in both directions.
messages_in() {
  echo $(( $(offsets_at "$1" -1) - $(offsets_at "$1" -2) ))
}

# Summed offsets across a topic's partitions at `-1` (latest) or `-2`
# (earliest); `0` for a topic that does not exist.
#
# The `|| true` matters more than it looks: this script runs under `set -euo
# pipefail`, and `kafka-get-offsets.sh` exits non-zero for an unknown topic.
# Without it, the very first fixture lookup on a fresh broker takes the whole
# script down — silently, since the failing command's own stderr is discarded.
offsets_at() {
  { kexec "$K/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic "$1" --time "$2" 2>/dev/null || true; } \
    | awk -F: '{ total += $3 } END { print total + 0 }'
}

echo "==> waiting for the broker"
# The loop used to fall through when the broker never answered, leaving the
# first real command to fail instead — under `set -e` that surfaced as a bare
# "No such container" or a topic-creation error, naming neither the broker nor
# the wait. On a loaded CI runner a cold Kafka start is exactly when this
# happens, so it says so, and shows the broker's own last words.
broker_ready=0
for _ in $(seq 1 60); do
  if kexec "$K/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
    broker_ready=1
    break
  fi
  sleep 1
done
if [ "$broker_ready" -ne 1 ]; then
  echo "the broker at $BOOTSTRAP did not answer within 60s" >&2
  echo "--- last 50 lines of \`docker logs $CONTAINER\` ---" >&2
  docker logs --tail 50 "$CONTAINER" >&2 || echo "(container $CONTAINER does not exist)" >&2
  exit 1
fi

# --- compression_codecs.rs -------------------------------------------------
# Produced by Kafka's own Java console producer, so what lands on disk does
# not depend on which codecs the librdkafka build under test supports — that
# independence is the whole point of the test.
echo "==> compression fixtures (c-gzip, c-snappy, c-lz4, c-zstd)"
for codec in gzip snappy lz4 zstd; do
  topic "c-$codec" 1
  if [ "$(messages_in "c-$codec")" -eq 0 ]; then
    seq 1 20 | sed 's/^/msg-/' \
      | kexec_i "$K/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" \
          --topic "c-$codec" --compression-codec "$codec" >/dev/null 2>&1
  fi
done

# --- payload_budget.rs / fetch_budget.rs -----------------------------------
# One 512 KB record per line: the console producer splits on newlines, so a
# file of concatenated records with no separator is sent as one oversized
# message and rejected wholesale.
echo "==> large-message fixtures (big-msgs, big-2mb)"
topic big-msgs 1
topic big-2mb 3
if [ "$(messages_in big-msgs)" -eq 0 ] || [ "$(messages_in big-2mb)" -eq 0 ]; then
  kexec sh -c 'head -c 524288 /dev/zero | tr "\0" x > /tmp/big.txt; echo >> /tmp/big.txt'
  kexec sh -c "for i in \$(seq 1 20); do cat /tmp/big.txt; done > /tmp/big20.txt"
  kexec sh -c "for i in \$(seq 1 30); do cat /tmp/big.txt; done > /tmp/big30.txt"
fi
if [ "$(messages_in big-msgs)" -eq 0 ]; then
  kexec sh -c "$K/kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic big-msgs < /tmp/big20.txt" >/dev/null 2>&1
fi
if [ "$(messages_in big-2mb)" -eq 0 ]; then
  kexec sh -c "$K/kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic big-2mb  < /tmp/big30.txt" >/dev/null 2>&1
fi

# --- fetch_stall.rs --------------------------------------------------------
# Enough messages, over enough partitions, that a fetch of the whole topic
# crosses librdkafka's prefetch-queue threshold many times over — which is
# the only way the one-second-per-crossing stall that test guards against
# becomes visible. A few hundred small messages never reach the threshold at
# all and the test would pass against the bug.
echo "==> stall fixture (perf-probe: 30,000 x 1 KB over 6 partitions)"
topic perf-probe 6
if [ "$(messages_in perf-probe)" -lt 30000 ]; then
  kexec sh -c 'head -c 1000 /dev/zero | tr "\0" x > /tmp/kb.txt; echo >> /tmp/kb.txt'
  kexec sh -c 'rm -f /tmp/perf.txt; for i in $(seq 1 30000); do cat /tmp/kb.txt; done > /tmp/perf.txt'
  kexec sh -c "$K/kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic perf-probe \
    --batch-size 65536 < /tmp/perf.txt" >/dev/null 2>&1
fi

# --- fetch_completion.rs ---------------------------------------------------
# A topic whose offsets are NOT all readable messages. Every transaction
# writes a commit marker, which occupies an offset that the broker never
# delivers to a consumer — so `high - low` over-counts what a fetch can
# actually collect, and the newest offset in a partition is typically a
# marker rather than a record.
#
# That gap is the whole point of the fixture: a fetch that decides it is
# finished by counting messages up to `high - low` can never reach that
# number here, and falls back on its idle timeout instead.
echo "==> completion fixture (perf-txn: 10,000 transactional records + commit markers)"
topic perf-txn 6
if [ "$(messages_in perf-txn)" -lt 10000 ]; then
  kexec sh -c 'head -c 1000 /dev/zero | tr "\0" y > /tmp/txn-kb.txt; echo >> /tmp/txn-kb.txt'
  kexec "$K/kafka-producer-perf-test.sh" --topic perf-txn --num-records 10000 \
    --payload-file /tmp/txn-kb.txt --throughput -1 \
    --transactional-id e2e-fixture-txn --transaction-duration-ms 50 \
    --producer-props "bootstrap.servers=$BOOTSTRAP" batch.size=65536 >/dev/null 2>&1
fi

# --- cluster_reads.rs ------------------------------------------------------
echo "==> cluster fixtures (e2e-basic, e2e-headers)"
topic e2e-basic 3
if [ "$(messages_in e2e-basic)" -eq 0 ]; then
  for i in $(seq 1 60); do echo "k$i:value-$i"; done \
    | kexec_i "$K/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic e2e-basic \
        --property parse.key=true --property key.separator=: >/dev/null 2>&1
fi

topic e2e-headers 1
if [ "$(messages_in e2e-headers)" -eq 0 ]; then
  printf 'trace-id:abc123,content-type:application/json\tkey-1:body-1\ntrace-id:def456,content-type:text/plain\tkey-2:body-2\n' \
    | kexec_i "$K/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic e2e-headers \
        --property parse.headers=true --property parse.key=true --property key.separator=: >/dev/null 2>&1
fi

# Two consumer groups, because the lag path behaves differently for each:
# `e2e-group` is left idle (Kafka reports it `Empty`, members array NULL) and
# `e2e-live` keeps a consumer running so its members carry the partition
# assignments that lag rows are derived from.
echo "==> consumer groups (e2e-group idle, e2e-live with a live member)"
kexec "$K/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" --topic e2e-basic \
  --group e2e-group --from-beginning --max-messages 30 --timeout-ms 15000 >/dev/null 2>&1 || true

if ! kexec "$K/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" --describe --group e2e-live 2>/dev/null \
     | grep -q console-consumer; then
  docker exec -d "$CONTAINER" "$K/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" \
    --topic e2e-basic --group e2e-live --from-beginning
  # The group only becomes Stable once the member has joined and been assigned.
  sleep 12
fi

# --- publish_roundtrip.rs ---------------------------------------------------
# Deliberately left empty: `publish_roundtrip.rs` fills it itself, which is the
# point of it. Three partitions so a publish can prove it reached the partition
# it was aimed at rather than the one a key happened to hash to.
echo "==> publish target (e2e-publish, empty)"
topic e2e-publish 3

echo
echo "==> topics"
kexec "$K/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list
