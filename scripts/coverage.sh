#!/usr/bin/env bash
#
# Produces the two LCOV reports `sonar-project.properties` imports:
#
#   coverage/frontend.lcov   Vitest (v8 provider) over frontend/src
#   coverage/backend.lcov    cargo-llvm-cov over the backend/* crates
#
# Run this before the Sonar scanner. The scanner does not measure coverage
# itself — with no reports present it reports 0%, which reads as a total
# regression rather than a missing step.
#
#   ./scripts/coverage.sh
#   sonar-scanner
#
# The backend's e2e tests (`backend/kafka/tests/*`) need a real broker and
# skip themselves without one — but they are what covers `client.rs`'s
# rdkafka paths, which is most of the file. Export SALTY_E2E_BOOTSTRAP
# to include them:
#
#   docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0
#   ./scripts/e2e-fixtures.sh
#   SALTY_E2E_BOOTSTRAP=localhost:9092 ./scripts/coverage.sh
#
# `src-tauri` is deliberately not measured: it needs a desktop toolchain to
# build and a running Tauri app to invoke, so it is excluded from the
# coverage ratio in sonar-project.properties too.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p coverage

echo "==> frontend (vitest + v8)"
npm --prefix frontend run test:coverage

# Vitest writes paths relative to `frontend/` ("SF:src/App.tsx"). The scanner
# resolves them against the repo root, where that path does not exist, and
# silently reports no coverage for every file. Rewrite them once here rather
# than teaching every consumer about the offset.
sed 's|^SF:src/|SF:frontend/src/|' frontend/coverage/lcov.info > coverage/frontend.lcov
echo "    -> coverage/frontend.lcov"

echo "==> backend (cargo-llvm-cov)"
if ! cargo llvm-cov --version >/dev/null 2>&1; then
  echo "cargo-llvm-cov is not installed. Install it with:" >&2
  echo "  cargo install cargo-llvm-cov --locked && rustup component add llvm-tools-preview" >&2
  exit 1
fi

cargo llvm-cov \
  --lcov --output-path coverage/backend.lcov \
  -p salty-core \
  -p salty-db \
  -p salty-avro \
  -p salty-protobuf \
  -p salty-schema-registry \
  -p salty-kafka \
  -p salty-ksql
echo "    -> coverage/backend.lcov"

echo
echo "==> summary"
cargo llvm-cov report --summary-only \
  -p salty-core \
  -p salty-db \
  -p salty-avro \
  -p salty-protobuf \
  -p salty-schema-registry \
  -p salty-kafka \
  -p salty-ksql
