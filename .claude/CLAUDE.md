# Salty

@../AGENTS.md

A desktop Kafka client built with Tauri v2 (Rust backend, React/TypeScript frontend).

Renamed from "Offset Explorer Oxide" in v1.0.0. The Rust crates are
`salty-*` (`salty_core`, `salty_kafka`, …), the bundle identifier is
`dev.salty.app`, and the e2e gates are `SALTY_E2E_*`.

## Structure

- `src-tauri/` — Tauri app shell and command layer (`src-tauri/src/commands/`), config in `src-tauri/tauri.conf.json`
- `backend/core` — shared domain types (connections, clusters, messages)
- `backend/kafka` — Kafka client, built on `rdkafka`/librdkafka
- `backend/db` — SQLite-backed local storage (connections, tabs, saved schemas, secrets — see Conventions)
- `backend/avro` — Avro payload decoding
- `backend/protobuf` — Protobuf payload decoding
- `backend/schema-registry` — Confluent Schema Registry client
- `frontend/` — React + TypeScript + Vite app, organized by feature under `frontend/src/features/`
- `.github/workflows/release.yml` — CI: builds and releases installers on push to `main`

## Commands

```bash
npm run dev              # tauri dev — hot-reload app shell + Vite frontend
npm run build             # tauri build — produces platform installers
npm --prefix frontend test  # frontend unit tests (Vitest)
cargo test                 # backend unit tests
npm run coverage           # both LCOV reports into coverage/, for SonarQube
```

## Conventions

- Rust workspace defined at the repo root `Cargo.toml`; `src-tauri` is a member, not its own workspace root.
- The Rust toolchain is pinned in `rust-toolchain.toml` (1.98.1) and the CI
  workflow pins the same version on its `dtolnay/rust-toolchain@` ref — keep
  the two in step. It used to be `@stable`, which meant the compiler building a
  release depended on the day it ran.
- Every `backend/*` crate is on **edition 2024**; `src-tauri` is deliberately
  still on 2021 because it cannot be compiled here, and the 2024 migration
  needs `cargo fix --edition` run somewhere it builds. Editions are per-crate
  and mix freely.
- `[profile.release]` is `lto = "fat"` + `codegen-units = 1` + `strip =
  "debuginfo"`, which is the practical optimum. `panic = "abort"` is
  deliberately **not** set (see the profile comment: `spawn_blocking` turns
  panics back into errors and the pooled-client mutexes recover from
  poisoning), and `-C target-cpu=native` would be wrong for a binary shipped
  to other people's machines.
- Secrets (SASL password, schema registry credentials, keystore/truststore passwords) are stored as plaintext columns on `connections` and returned to the frontend as part of `Connection`. This was previously OS-keychain-backed (`kafkaoxide-secrets`, since removed); that approach was abandoned after keychain writes proved unreliable on Windows (Credential Manager silently failing for some users, with no working fallback for SASL-authenticated connections). Export (`connections_export`) still deliberately excludes every secret via `PortableConnection`'s field list.
- **`kafka_version` drives no librdkafka property.** It is stored on
  `connections`, carried in `PortableConnection`, and shown in the modal's
  dropdown — and `backend/kafka/src/config.rs` never reads it. Its one
  behavioural use is `isKRaftOnly` in `frontend/src/lib/tauri.ts`, which hides
  the ZooKeeper section and makes `toNewConnection` null the ZooKeeper columns
  from 4.0 on, since Kafka 4.0 removed ZooKeeper (3.9 is the last version that
  can run it). There is deliberately nothing to configure from it: every
  offered version is 0.11+, so ApiVersions negotiation settles the protocol,
  and **no client config can point at a KRaft controller quorum** — clients
  only ever talk to `bootstrap.servers`, `bootstrap.controllers` (KIP-919) is
  Java-admin-only, and librdkafka binds nothing for it. The list also drops
  **2.9**, which Kafka never released; `PropertiesTab` appends any
  stored-but-unlisted version as its own option, because `Dropdown` falls back
  to `options[0]` and would otherwise display `0.11` for a row still storing
  `2.9` — with Update disabled, since the draft never diverged.
- **Vitest does not type-check, so a green frontend suite says nothing about
  whether the frontend compiles.** `npm --prefix frontend run build` (`tsc &&
  vite build`) is the only gate on type errors. This is not theoretical: a
  `Pick<UseMutationResult<…>, "isSuccess" | "data" | …>` loses the
  discriminated-union correlation between the two, so `data` stays possibly
  `undefined` inside an `if (mutation.isSuccess)` branch — `DetectResult`
  needed an assertion for exactly that, while `PingResult` gets away with the
  same `Pick` only because it compares `data` rather than dereferencing it.
- Publishing is gated in four places, and the broker is the only authority
  among them: a per-connection `allow_publishing` column (`DEFAULT 0`, so every
  connection starts unable to publish), a connected-cluster check, a cached
  per-(connection, topic) broker denial, and `encode_messages` validation — all
  four applied by `src-tauri/src/commands/publish.rs` before a producer exists.
  `allow_publishing` is deliberately **excluded from `PortableConnection`**, so
  an exported connections file can never arrive with publishing pre-enabled;
  that is the same reasoning that keeps secrets out of exports. The gate order
  itself lives in `salty_core::publish_refusal` rather than the command, so
  it can be tested where `src-tauri` cannot be built.
- `AppError::Authorization` is distinct from `AppError::Authentication` on
  purpose: a `TOPIC_AUTHORIZATION_FAILED` must not feed the credential circuit
  breaker, or one refused publish would take an otherwise-working cluster
  offline inside the app. A rejected *password* during a publish still does feed
  it — which needs `backend/kafka/src/producer.rs`'s `ProducerErrorContext`,
  because on the produce path a wrong password arrives as a bare
  `MessageTimedOut` and only librdkafka's `error` callback names the cause.
- `rdkafka`'s **safe wrapper** has no ACL or `DescribeTopics` support in any
  published version (checked against 0.39.0), so there is no way to ask a
  broker "may I write here?" before trying. Publishing therefore relies on the
  produce attempt itself being the authorization check — nothing is written
  when it is refused — plus the app-side gates above. `DescribeTopics` is
  genuinely absent; **ACLs are not**. `rdkafka` re-exports rdkafka-sys
  wholesale (`pub use rdkafka_sys::{bindings, helpers, types};`, `lib.rs:275`)
  and rdkafka-sys 4.10.0+2.12.1 binds the full C ACL API, reachable from the
  existing pooled `AdminClient` via `AdminClient::inner().native_ptr()`. That
  is what `backend/kafka/src/acl.rs` uses, and it is the only `unsafe` in the
  codebase — keep it that way.
- **librdkafka discards the `DescribeAcls` error code**, so an ACL listing
  cannot tell you why it is empty. `rd_kafka_DescribeAclsResponse_parse`
  (2.12.1) reads the response's `error_code`, uses it only to reassign a local
  `errstr` pointer, and returns `RD_KAFKA_RESP_ERR_NO_ERROR` unconditionally —
  so `CLUSTER_AUTHORIZATION_FAILED` and `SECURITY_DISABLED` both arrive as a
  *successful, empty* result, indistinguishable from a cluster with no ACLs
  defined. Since those three mean opposite things, `salty_core`'s
  `AclAvailability` recovers what it can from the broker's own
  `authorizer.class.name` (read via `DescribeConfigs`, which *does* propagate
  its errors) and reports `Indeterminate` rather than guessing the rest. Do not
  "simplify" this back to reading the error code; `backend/kafka/tests/
  acl_describe.rs` fails against a real broker if you do.
- **The Properties tab has no Detect button, and `connection_detect_version` is gone from `src-tauri`** (removed in 1.1.5; the version is picked from the dropdown). `salty_kafka`'s `detect_cluster_version` and `salty_core::cluster_mode` are deliberately still there with their tests — only the command layer was dropped, because `src-tauri` cannot be compiled here and a backend-wide removal would be unverifiable. Delete them together if nothing needs them.
- **`detect_cluster_version` returns `Ok` on an authorization refusal, and
  that is the opposite of `authorizer_class` right beside it.** Both read
  broker config via `DescribeConfigs`, and a principal without that
  permission gets an empty *successful* result either way. `authorizer_class`
  collapses every failure to `None` because it silently qualifies an ACL
  listing and must never turn a good answer into a bad one. Detect's result
  *is* the user's answer, so an empty response becomes
  `MetadataMode::Unknown` carrying a note, and only a transport failure is an
  `Err`. Both paths are pinned against real brokers:
  `backend/kafka/tests/cluster_mode_detect.rs` for the KRaft success path, and
  `cluster_mode_authorization.rs` for the refusal — the latter gated on
  `SALTY_E2E_ACL_BOOTSTRAP`, because on the ordinary e2e broker (no
  authorizer) `reader` reads configs happily and the test would pass for the
  wrong reason, the same trap `publish_authorization.rs` documents. The
  derivation rules themselves live in `salty_core::cluster_mode`, not
  `salty-kafka`, so every branch is unit-testable without a broker — same
  reason as `publish_refusal` and `acl_effective`. **Known limitation:**
  `.and_then(|resource| resource.ok())` discards the per-resource error code,
  so a resource-level failure that is *not* a refusal — the chosen broker
  dying between the metadata fetch and the `DescribeConfigs` call — also
  reports `Unknown`. That is the same ambiguity above, minus the second
  question: nothing comparable to `authorizer.class.name` distinguishes
  "refused" from "that broker went away", which is why the note says to choose
  the version manually rather than naming a cause as fact.
- ACL **pattern matching is the broker's job, not ours.** A `DescribeAcls`
  filter sent with `RD_KAFKA_RESOURCE_PATTERN_MATCH` makes the broker resolve
  which literal, prefixed and wildcard patterns govern a given resource name,
  so `salty_core::acl_effective` never matches patterns — it only applies
  Kafka's precedence and implication rules. Note those rules are **asymmetric**:
  the implication expansion (`Read`/`Write`/`Delete`/`Alter` ⇒ `Describe`,
  `AlterConfigs` ⇒ `DescribeConfigs`) applies when looking for an *allow* and
  never to a *deny*, so a `Deny Read` does not deny `Describe`.
- **A connected cluster's status dot is answered by the pooled client, not by
  dialling the broker.** `RdKafkaClient::check_status` reads librdkafka's
  statistics (`statistics.interval.ms`, set only on the pooled metadata client)
  through `ObservedClient::liveness_status` — an IPC call and **no traffic to the
  cluster** — and only falls back to the old TCP connect when no pooled client
  exists (it never builds one for the purpose). The verdict is a *duration*, in
  `salty_core::LivenessTracker`: measured against a real broker, a broker's
  idle-close (`connections.max.idle.ms`) and a dead broker look identical for the
  first seconds (`AllBrokersDown`, state `INIT`), and only the length of the gap
  tells them apart, so "no broker `UP` for `LIVENESS_GRACE_MS`" is unreachable
  and anything shorter is `Unknown`. A successful pooled request also counts as
  an `UP` sighting. Statistics are only delivered while the queue is served, so
  `liveness_status` polls it (bounded, and it never waits on the error slot — a
  slow request holds that for its whole timeout); that is why the interval is a
  slow 5 s — an unpolled client queues one report per interval. **`poll(ZERO)`
  serves one event and returns `None` whether it consumed a report or found the
  queue empty**, so the drain counts what the callbacks served
  (`events_served`) instead of stopping at the first `None`; stopping there
  passed every test polling faster than the reports arrive and disconnected a
  healthy idle cluster about 35 s after Connect at the app's real 10 s cadence
  (`an_idle_connected_cluster_stays_reachable_at_the_apps_polling_cadence`). The frontend skips the call
  entirely when a cluster-data query just succeeded
  (`connectionStatusPolling.ts`) and re-probes at once when one fails.
- **Pooled Kafka clients expire when nobody asks for them.** `RdKafkaClient`
  stamps each pooled metadata/admin client with `last_used_ms` (a request, or the
  status poll's `existing_metadata_client`, counts as use) and a task started on
  first pooling closes any unused for `IDLE_CLIENT_TTL` (5 min, swept every 30 s;
  it holds only `Weak` references, so it ends with the client). It exists because
  nothing in the backend checks `is_connected` before building a client — a late
  request after Disconnect recreates one — and librdkafka re-dials rejected
  credentials every 30 s for as long as the client lives (measured: failed logins
  at 0, 2, 11, 37 s … on a SASL broker). A connected cluster is polled every 10 s
  so is never reaped; a hidden window stops polling and its client closes after
  the TTL, rebuilding lazily on return. Deliberately **not** done instead: making
  every request refuse to build a client unless Connect ran — `connection_update`
  releases the client but leaves the cluster registered as connected, relying on
  that lazy rebuild, and a dozen e2e files list topics without connecting.
- **The fetch path asks the pooled client for the topic's partition list with a
  3 s deadline, and replaces the client if that times out** (`with_stale_connection_retry`,
  `STALE_PROBE_TIMEOUT`). That call deliberately goes to the long-lived pooled client
  (~12 ms) rather than the fetch's own consumer (~500 ms, queued behind a group
  coordinator query). The cost of that choice: after a pause a NAT, load balancer or
  firewall can silently forget the pooled client's flow, so the client believes it is
  connected and the call waits out the whole read timeout — "click a message after
  being idle for a couple of minutes does nothing; a second try works". Reproduced
  with a proxy that goes silent on idle flows: 15.4 s then failure, twice in a row;
  now 3.8 s then success. Only a *timeout* triggers the replacement, and the retry
  gets the rest of the same budget, so a cluster that is really down is reported no
  later than before. The tree's listings (brokers, topics, consumer groups,
  partitions, message count, group lag) cannot use their own call as the probe — all
  topics on a big cluster can honestly take longer than 3 s — so `live_metadata_client`
  first sends a one-topic metadata request (`LIVENESS_PROBE_TOPIC`) to a client whose
  last *answered* request is over `STALE_IDLE_AFTER` (20 s) old. That clock is
  `last_success_ms`, deliberately not the liveness tracker or the status poll: a
  connection a NAT has dropped still looks `UP` in every statistics report. Measured
  through the same silent-drop proxy: every listing 15.3 s then failure, twice in a row,
  before; 3.8 s then success, then ~2 ms, after.
- `rdkafka` uses librdkafka's default vendored build (`configure && make`) on macOS/Linux, and the `cmake-build` feature (CMake + MSVC) on Windows — see `backend/kafka/Cargo.toml`.
- **The fetch path uses `BaseConsumer` inside `spawn_blocking`, not
  `StreamConsumer`, and that is deliberate.** rdkafka's `tokio` feature *is*
  on (it is in rdkafka's `default`) and the publish path does use it, via
  `FutureProducer::send(..).await`. `StreamConsumer` would wrap the same
  librdkafka queue the `BaseConsumer` already polls — it changes which thread
  waits, not how fast bytes arrive. Every measured fetch win here came from
  librdkafka *config* or from the loop's own strategy instead:
  `fetch.queue.backoff.ms`, dropping the `group.id` coordinator query,
  partition-EOF completion, the 8 MB queue floor, and per-partition
  decompression sharding. The loop in `client.rs` also multiplexes several
  shard consumers with a non-blocking sweep plus a 5 ms blocking slice, which
  is the one thing `StreamConsumer` would otherwise buy, and it carries the
  cancellation / byte-budget / idle-timeout logic inline.
- `package-lock.json` and `Cargo.lock` are both gitignored — installs are not lockfile-pinned.
- Coverage is enforced at 80% (lines/statements/functions/branches) by
  `frontend/vitest.config.ts`'s `thresholds`, so `npm --prefix frontend run test:coverage`
  fails rather than merely reports when it slips. There is no equivalent gate on
  the Rust side — `scripts/coverage.sh` prints the per-crate table instead.
- `scripts/coverage.sh` produces the two LCOV files `sonar-project.properties`
  imports. `src-tauri` is excluded from both the `cargo llvm-cov` run and the
  Sonar coverage ratio: its functions are `#[tauri::command]` wrappers that need
  a running Tauri app (and a desktop toolchain) to invoke, and the logic they
  wrap lives in `backend/*` where it is covered.
- `scripts/e2e-acl-fixtures.sh` brings up a *second* broker (SASL/PLAIN +
  `StandardAuthorizer` + `allow.everyone.if.no.acl.found=false`, config in
  `build-support/e2e-acl/server.properties`) with three principals: `admin`
  (super), `writer` (Describe+Read+Write) and `reader` (Describe+Read only).
  `backend/kafka/tests/publish_authorization.rs` is gated on
  `SALTY_E2E_ACL_BOOTSTRAP` and is the only test that can show a read-only
  principal being refused — on the ordinary e2e broker, which has no authorizer,
  `reader` would publish happily and the test would pass for the wrong reason.
- Most of `backend/kafka/src/client.rs` is only reachable with a real broker.
  `scripts/e2e-fixtures.sh` sets one up (`docker run -d --name kafka -p 9092:9092
  apache/kafka:3.9.0` first); without `SALTY_E2E_BOOTSTRAP` the e2e tests
  skip themselves and that file drops from ~94% to ~62%.
- The message payload panel (`MessagePayloadViewer`) is the one place besides
  `connections_export` that writes a user file: **Save** writes the value as
  the selected format renders it, through `commands::system::payload_save`
  after the frontend has resolved a path via the native save dialog. It takes
  base64 rather than a byte vector because Tauri's IPC is JSON. A second
  button, **Download** (the payload's original bytes, in both this panel and
  `JsonViewerTabPanel`), was **deliberately removed** — Save is the only way
  out now, so don't reintroduce it; `payload_save` still writes bytes rather
  than a string only because the frontend base64-encodes the rendered text
  itself. Its chosen format and its placement (beside the middle pane or
  docked under it) live in
  `useMessageViewerPrefsStore`, which keeps a per-tab choice *and* an
  app-wide last-chosen default in localStorage.
- Protobuf decoding uses `protox` (compiles `.proto` *source text* to a
  descriptor set in pure Rust) plus `prost-reflect` (`DynamicMessage`). That
  pair is the only route that works here: every alternative wants the schema
  at compile time, and these schemas are pasted by the user or fetched from a
  registry at run time. It also means no `protoc` binary to ship and locate on
  three platforms. `Compiler::include_imports(true)` is **required** — without
  it `file_descriptor_set()` omits the transitive imports and any schema using
  a well-known type (`Timestamp`, `Duration`) fails to load.
- Protobuf's framing differs from Avro's in two ways the commands encode: the
  Confluent header is stripped on *every* path (a manual schema does not mean
  "decode the payload whole", as it does for Avro), and there is no refusal —
  with no schema anywhere the wire format still yields field numbers, so
  `salty_protobuf::wire::decode_raw` renders those and the result carries
  `source: "none"` so the UI can say the keys are numbers, not names. Note the
  Confluent message-index shorthand: an all-zero path is a single `0` byte,
  not a length-prefixed array.
- AG Grid is **two thirds of the frontend bundle** and is reached from exactly
  two tab panels, so both load through `features/connections/gridTabs.tsx`
  (`React.lazy` + `Suspense`) rather than being imported directly. Import
  `DataTab`/`PartitionReplicasTab` from there, not from their own modules, or
  AG Grid lands back in the initial chunk. Measured: initial bundle 1545 kB ->
  415 kB, startup 111 ms -> 43 ms. A test that renders one of those panels has
  to `findBy` its contents, not `getBy` — the tab body now resolves
  asynchronously.
- **Every node in the JSON tree starts expanded**, and there is no Expand all
  button — both were removed in favour of the tree simply opening. The
  auto-collapse rule (`shouldAutoExpand`, a 1,000-line budget) existed only
  because rendering meant a DOM element per line; virtualization removed the
  reason, so `jsonTreeExpansion.ts` is gone and `jsonTreeLines.ts` keeps just a
  map of the containers the reader clicked shut. `XmlTreeView` has always
  opened every node (`useState(true)`) and is unchanged. The cost of the
  default: a flatten now walks the whole document, so collapsing one node in a
  4 MB payload rebuilds ~300k rows.
- **`JsonTreeView` is virtualized and must stay that way.** It flattens the
  document to a list of lines (`jsonTreeLines.ts`) and renders it through
  `react-window`'s `List`, so the DOM holds a screenful of rows however much is
  expanded. Before that, Expand all on a 4 MB payload mounted ~1.5 million
  elements: measured in Chromium, 1 MB blocked the main thread for 6.6 s and
  4 MB crashed the renderer. Three consequences to keep in mind: the line
  numbers come from the row's index, **not** a CSS counter (a counter restarts
  at 1 on every screen, and the counter rule is still there for the XML tree,
  scoped `:not(.json-tree-body--virtual)`); the list needs a *definite height*
  to fill, which is why `.message-payload-scroll` hands its scrolling over via
  `--tree` for the three tree formats; and the document's width is computed
  from one measured character (`.json-tree-measure`) rather than from
  `width: max-content`, which can only ever see the rows currently on screen.
  A test that asserts a deep row is in the DOM will fail — assert the
  expansion state instead (an absent Expand arrow, a disabled Expand all).
- **`XmlTreeView` is *not* virtualized, and expands every node on mount**
  (`useState(true)`), so it has the freeze `JsonTreeView` no longer has:
  measured 5,000 elements -> 688 ms, 20,000 -> 2.25 s, linear. It shares
  `.json-tree*` classes with the JSON tree, so scope changes to those away from
  it (`--virtual` modifiers) unless you mean to hit both.
- **The pane divider is the only separator between panes.** `.resizable-pane--right`
  and `--bottom` used to also carry a border on the edge the divider sits
  against, and the payload pane's `.json-tree-body` drew its own box 8px
  further in — three parallel 1px rules in the same colour. `ResizableShell`
  always renders a divider alongside the pane, and only the divider's line
  highlights when grabbed, so it is the one that stays.
- **The viewer's fetched payload lives in `useMessageViewerStore.byTab`, not in
  the query cache.** `MessagePayloadViewer` is rendered `key={activeTabId}`, so
  every top-level tab switch destroys it — and `useFullPayload`'s `gcTime: 0`
  dropped the result with it, re-fetching the same message from the broker on
  every return to the tab. Held per tab it survives the remount and is still
  cleared by "Clear memory" and by deleting the connection.
- Only `ResourceCategory` (Brokers, Consumers) virtualizes its list, via
  `react-window`'s `List` past a 50-item threshold. **Topics is a separate,
  deliberately non-virtualized `TopicCategory`** — so a change to the
  virtualized path cannot be verified by opening Topics, which is the obvious
  thing to try and shows the plain `<ul>` branch instead.
- **Frontend `localStorage` keys keep the `kafkaoxide.` prefix** (theme, font
  settings, panel heights, pane widths, message-viewer prefs) even though
  everything else is `salty`. Renaming them is strictly worse than leaving
  them: a key that survives the identifier change carries the user's settings
  forward, and one that doesn't is lost either way — renaming only guarantees
  the first case is lost too. The app data *database* is a different story and
  is migrated for real; see `salty_core::adopt_legacy_app_data`.
- **`backend/db/migrations/*.sql` must never be edited**, not even a comment:
  `sqlx::migrate!` checksums applied migrations, so a changed byte makes every
  existing install fail to start. `0004_topic_schemas.sql` still says
  `kafkaoxide_db::init_pool` for exactly this reason.
- The connections export file's version field serialises as
  `saltyConnectionsVersion` but carries `#[serde(alias =
  "kafkaoxideConnectionsVersion")]`, because it is the first thing
  `ConnectionExportFile::parse` checks — without the alias every file exported
  before the rename would be rejected as invalid.
- The topic detail panel's tabs are **Data, Meta Data, Partitions, Schema,
  Config**, in that order, with Data as the default — first tab and landing tab
  deliberately the same. "Meta Data" was "Properties" (it holds the topic's
  name and a message count, not settings); Config is last because it is the one
  read-only tab, showing the broker's own settings for the topic via
  `useTopicConfig` -> `connection_describe_topic_config`.
- The connection modal / cluster panel tabs are **Properties, Security &
  Authentication, Schema**. Security and Authentication were merged because
  the protocol chosen in one decides whether the other is needed at all; the
  two halves are `SecuritySection` and `AuthenticationSection`, composed in
  that order by `SecurityAuthenticationTab`, each keeping its own `disabled`
  fieldset. "Schema" was "Advanced" — every field on it is a Schema Registry
  setting.
- Frontend feature folders pair each component/store with its test file (e.g. `useTabsStore.ts` + `useTabsStore.test.ts`) rather than a separate `__tests__` tree.
