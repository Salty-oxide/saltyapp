#!/usr/bin/env bash
#
# Brings up the ksqlDB server that `backend/ksql/tests/live_ksqldb.rs` needs,
# and registers a stream over the ordinary e2e broker's `e2e-basic` topic.
#
#   ./scripts/e2e-fixtures.sh          # the broker and its topics first
#   ./scripts/e2e-ksql-fixtures.sh
#   SALTY_E2E_KSQL_URL=http://localhost:8088 \
#     cargo test -p salty-ksql --test live_ksqldb
#
# ksqlDB cannot query a raw topic — it needs a STREAM registered over one —
# which is why this script exists at all rather than the tests pointing
# straight at a topic.
#
# Every step is idempotent: the container is reused when already running, and
# the stream is created with IF NOT EXISTS.
#
# Tear down with:
#
#   docker rm -f ksqldb
set -euo pipefail

CONTAINER="${KSQL_CONTAINER:-ksqldb}"
KSQL_URL="${KSQL_URL:-http://localhost:8088}"
BOOTSTRAP="${KSQL_KAFKA_BOOTSTRAP:-localhost:9092}"
IMAGE="${KSQL_IMAGE:-confluentinc/ksqldb-server:0.29.0}"
TOPIC="${KSQL_TOPIC:-e2e-basic}"
STREAM="${KSQL_STREAM:-E2E_BASIC_STREAM}"

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  echo "==> starting $CONTAINER on $KSQL_URL"
  # `--network host` because the broker advertises `localhost:9092`: from
  # inside its own network namespace a second container would resolve that to
  # itself and never find Kafka at all.
  docker run -d --name "$CONTAINER" --network host \
    -e KSQL_LISTENERS=http://0.0.0.0:8088 \
    -e KSQL_BOOTSTRAP_SERVERS="$BOOTSTRAP" \
    -e KSQL_KSQL_SERVICE_ID=salty_e2e_ \
    -e KSQL_KSQL_STREAMS_AUTO_OFFSET_RESET=earliest \
    "$IMAGE" >/dev/null
fi

# A JVM that also waits on Kafka; slower to become ready than the broker is.
# Fails loudly rather than falling through — see the matching note in
# scripts/e2e-fixtures.sh.
echo "==> waiting for ksqlDB"
ksql_ready=0
for _ in $(seq 1 120); do
  if curl -fsS -m 3 "$KSQL_URL/info" 2>/dev/null | grep -q '"serverStatus":"RUNNING"'; then
    ksql_ready=1
    break
  fi
  sleep 1
done
if [ "$ksql_ready" -ne 1 ]; then
  echo "ksqlDB at $KSQL_URL did not report RUNNING within 120s" >&2
  echo "--- last 50 lines of \`docker logs $CONTAINER\` ---" >&2
  docker logs --tail 50 "$CONTAINER" >&2 || echo "(container $CONTAINER does not exist)" >&2
  exit 1
fi

run_ksql() {
  curl -fsS -m 30 -X POST "$KSQL_URL/ksql" \
    -H 'Content-Type: application/vnd.ksql.v1+json' \
    -d "$(printf '{"ksql":%s,"streamsProperties":{}}' "$(printf '%s' "$1" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')")"
}

# The topic carries `k<N>:value-<N>` pairs as plain text — see
# scripts/e2e-fixtures.sh — so the stream declares a single VARCHAR value.
echo "==> stream $STREAM over $TOPIC"
run_ksql "CREATE STREAM IF NOT EXISTS $STREAM (MESSAGE VARCHAR) WITH (KAFKA_TOPIC='$TOPIC', VALUE_FORMAT='KAFKA');" >/dev/null

echo "==> current streams"
run_ksql "LIST STREAMS;" | python3 -c 'import json,sys; [print(" ", s["name"], "->", s["topic"]) for e in json.load(sys.stdin) for s in e.get("streams",[])]'

cat <<EOS

Ready. Run the live tests with:

  SALTY_E2E_KSQL_URL=$KSQL_URL \\
    cargo test -p salty-ksql --test live_ksqldb
EOS
