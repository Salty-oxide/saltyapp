#!/usr/bin/env bash
#
# Brings up the SASL + ACL broker that `backend/kafka/tests/publish_authorization.rs`
# needs, and grants the ACLs that make its three principals different.
#
# The ordinary e2e broker (`scripts/e2e-fixtures.sh`) has no authorizer, so on it
# every principal may write to everything — which is exactly the wrong cluster on
# which to test that a read-only user cannot publish. This one denies by default.
#
#   ./scripts/e2e-acl-fixtures.sh
#   SALTY_E2E_ACL_BOOTSTRAP=localhost:9192 \
#     cargo test -p salty-kafka --test publish_authorization
#
# Every step is idempotent, so re-running against an already-configured broker
# is harmless. Tear down with:
#
#   docker rm -f kafka-acl
set -euo pipefail

CONTAINER="${KAFKA_ACL_CONTAINER:-kafka-acl}"
HOST_PORT="${KAFKA_ACL_PORT:-9192}"
IMAGE="${KAFKA_ACL_IMAGE:-apache/kafka:3.9.0}"
TOPIC="${KAFKA_ACL_TOPIC:-e2e-acl-publish}"
# The prefix the PREFIXED ACL below is granted on. Nothing needs to exist with
# this name: a prefixed binding governs topics that do not exist yet, which is
# exactly the property the UI surfaces.
PREFIX="${KAFKA_ACL_PREFIX:-e2e-acl-prefixed-}"
CONFIG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../build-support/e2e-acl" && pwd)"
K=/opt/kafka/bin
# The same address inside the container as outside — see the listener comments
# in build-support/e2e-acl/server.properties.
INTERNAL=localhost:9192

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  echo "==> starting $CONTAINER on localhost:$HOST_PORT"
  docker run -d --name "$CONTAINER" \
    -p "$HOST_PORT:9192" \
    -v "$CONFIG_DIR/server.properties:/mnt/shared/config/server.properties:ro" \
    "$IMAGE" >/dev/null
fi

# The admin client needs credentials of its own — every listener on this broker
# requires SASL, including the one the CLI tools use.
docker exec -i "$CONTAINER" sh -c 'cat > /tmp/admin.properties' <<'PROPS'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";
PROPS

echo "==> waiting for the broker"
# Fails loudly rather than falling through — see the matching note in
# scripts/e2e-fixtures.sh. This broker is slower to start than the plain one
# (SASL plus the authorizer), which is why it waits longer.
broker_ready=0
for _ in $(seq 1 90); do
  if docker exec "$CONTAINER" "$K/kafka-topics.sh" --bootstrap-server "$INTERNAL" \
       --command-config /tmp/admin.properties --list >/dev/null 2>&1; then
    broker_ready=1
    break
  fi
  sleep 1
done
if [ "$broker_ready" -ne 1 ]; then
  echo "the broker at $INTERNAL did not answer within 90s" >&2
  echo "--- last 50 lines of \`docker logs $CONTAINER\` ---" >&2
  docker logs --tail 50 "$CONTAINER" >&2 || echo "(container $CONTAINER does not exist)" >&2
  exit 1
fi

echo "==> topic $TOPIC"
docker exec "$CONTAINER" "$K/kafka-topics.sh" --bootstrap-server "$INTERNAL" \
  --command-config /tmp/admin.properties \
  --create --if-not-exists --topic "$TOPIC" --partitions 3 --replication-factor 1 >/dev/null

acl() {
  docker exec "$CONTAINER" "$K/kafka-acls.sh" --bootstrap-server "$INTERNAL" \
    --command-config /tmp/admin.properties --add "$@" >/dev/null
}

# `writer` may do everything this feature needs: describe the topic (so the
# client can fetch metadata for it), read it, and write to it.
echo "==> ACLs for User:writer (Describe, Read, Write on $TOPIC)"
acl --allow-principal User:writer --operation Describe --operation Read --operation Write --topic "$TOPIC"
acl --allow-principal User:writer --operation Read --group '*'

# `reader` may look and read, and nothing else. No Write ACL is granted here,
# and that omission is what the test asserts on — so do not add one.
echo "==> ACLs for User:reader (Describe, Read on $TOPIC — deliberately no Write)"
acl --allow-principal User:reader --operation Describe --operation Read --topic "$TOPIC"
acl --allow-principal User:reader --operation Read --group '*'

# A PREFIXED grant, so `backend/kafka/tests/acl_describe.rs` can show that a
# DescribeAcls filter in MATCH mode resolves a prefix against a concrete topic
# name. The app relies on the broker to do that resolution rather than matching
# patterns itself, so it needs a prefixed ACL to resolve. `--resource-pattern-type
# prefixed` is what makes this a prefix rather than a literal topic called
# "e2e-acl-prefixed-".
echo "==> PREFIXED ACL for User:writer (Write on $PREFIX*)"
acl --allow-principal User:writer --operation Write \
  --topic "$PREFIX" --resource-pattern-type prefixed

echo "==> current ACLs"
docker exec "$CONTAINER" "$K/kafka-acls.sh" --bootstrap-server "$INTERNAL" \
  --command-config /tmp/admin.properties --list --topic "$TOPIC"

cat <<EOF

Ready. Run the authorization tests with:

  SALTY_E2E_ACL_BOOTSTRAP=localhost:$HOST_PORT \\
    cargo test -p salty-kafka --test publish_authorization
EOF
