import { invoke as tauriInvoke } from "@tauri-apps/api/core";
import { useGeneralSettingsStore } from "../features/settings/useGeneralSettingsStore";

/**
 * Tauri's `invoke` rejects with whatever the Rust command's `Err` payload
 * deserializes to — for this app's `CommandError { message: String }`,
 * that's a plain object `{ message: "..." }`, not a JS `Error` instance.
 * Every catch block across this app that checks `err instanceof Error` (to
 * show the real backend error message instead of a generic fallback) was
 * silently failing that check and falling back to the generic text, no
 * matter how informative the actual Rust-side error was. Normalizing once
 * here means every `invoke<T>(...)` call below keeps its existing call
 * signature, and every caller's `instanceof Error` check now actually works.
 */
async function invoke<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  try {
    return await tauriInvoke<T>(cmd, args);
  } catch (err) {
    if (err instanceof Error) throw err;
    const message =
      typeof err === "object" && err !== null && "message" in err && typeof (err as { message: unknown }).message === "string"
        ? (err as { message: string }).message
        : String(err);
    throw new Error(message);
  }
}

export type SecurityProtocol = "PLAINTEXT" | "SSL" | "SASL_PLAINTEXT" | "SASL_SSL";
export type SaslMechanism = "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512";
export type ConnectionStatus = "UNKNOWN" | "REACHABLE" | "UNREACHABLE";

/** Values for the General section's "Kafka cluster version" dropdown. */
export const KAFKA_VERSIONS = [
  "0.11",
  "1.0",
  "1.1",
  "2.0",
  "2.1",
  "2.2",
  "2.3",
  "2.4",
  "2.5",
  "2.6",
  "2.7",
  "2.8",
  "3.0",
  "3.1",
  "3.2",
  "3.3",
  "3.4",
  "3.5",
  "3.6",
  "3.7",
  "3.8",
  "3.9",
  "4.0",
  "4.1",
  "4.2",
  "4.3",
] as const;

/**
 * Whether this version's clusters can only be KRaft — Kafka 4.0 removed
 * ZooKeeper outright, so the modal's ZooKeeper section is meaningless from
 * there on, and a 4.x connection must not persist ZooKeeper settings.
 *
 * Note what this is *not* used for: `kafkaVersion` drives no librdkafka
 * property anywhere in the app (see `backend/kafka/src/config.rs`, which
 * never reads it). It is stored metadata, and this predicate is the one
 * place it changes behaviour.
 *
 * An unparseable or unlisted version returns `false` on purpose. "Unknown"
 * has to mean "hide nothing" — a row written by an older build, or a
 * version this build does not offer, must not lose its ZooKeeper settings
 * because the string could not be read.
 */
export function isKRaftOnly(version: string): boolean {
  const major = Number.parseInt(version, 10);
  return Number.isFinite(major) && major >= 4;
}

/** `MetadataMode` in `salty_core::cluster_mode`, as camelCase JSON. */
export type MetadataMode = "kraft" | "zookeeper" | "unknown";

/**
 * What the Properties tab's Detect button learned from the cluster.
 *
 * Every field is nullable because this comes from a broker's answer to
 * `DescribeConfigs`, and a principal who may not read broker configs gets
 * an empty *successful* result rather than a refusal — so "could not tell"
 * arrives as a `mode` of `"unknown"` plus a `note`, not as a rejection.
 */
export interface ClusterVersionReport {
  mode: MetadataMode;
  processRoles: string | null;
  interBrokerProtocolVersion: string | null;
  /** The dropdown value to apply — a suggestion, which the user can override. */
  suggestedVersion: string | null;
  note: string | null;
}

export type SchemaFormat = "avro" | "protobuf";

export interface Connection {
  id: string;
  name: string;
  bootstrapServers: string;
  kafkaVersion: string;
  zookeeperEnabled: boolean;
  zookeeperHost: string | null;
  zookeeperPort: number | null;
  zookeeperChrootPath: string | null;
  securityProtocol: SecurityProtocol;
  saslMechanism: SaslMechanism | null;
  saslUsername: string | null;
  saslPassword: string | null;
  saslOauthUrl: string | null;
  schemaRegistryEndpoint: string | null;
  schemaRegistryBasicAuthCredentials: string | null;
  /** Base URL of this cluster's ksqlDB server. Absent for the many clusters that run none. */
  ksqldbEndpoint: string | null;
  /** `user:password`, the same shape the schema registry's credentials use. */
  ksqldbBasicAuthCredentials: string | null;
  schemaRegistryTrustStoreLocation: string | null;
  schemaRegistryTrustStorePassword: string | null;
  schemaRegistryKeystoreLocation: string | null;
  schemaRegistryKeystorePassword: string | null;
  schemaRegistryKeystoreKeyPassword: string | null;
  sslTruststoreLocation: string | null;
  sslTruststorePassword: string | null;
  sslKeystoreLocation: string | null;
  sslKeystorePassword: string | null;
  sslKeystoreKeyPassword: string | null;
  /**
   * Whether this connection may publish messages at all. Defaults to `false`
   * for every connection — see the Rust `Connection::allow_publishing`.
   *
   * The Publish tab reads this to explain itself before the user types
   * anything, but it is *not* what enforces it: the Rust command re-reads the
   * column from SQLite on every publish, so this field is a hint and the
   * backend is the gate.
   */
  allowPublishing: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface NewConnection {
  name: string;
  bootstrapServers: string;
  kafkaVersion: string;
  zookeeperEnabled: boolean;
  zookeeperHost: string | null;
  zookeeperPort: number | null;
  zookeeperChrootPath: string | null;
  securityProtocol: SecurityProtocol;
  saslMechanism: SaslMechanism | null;
  saslUsername: string | null;
  saslPassword: string | null;
  saslOauthUrl: string | null;
  schemaRegistryEndpoint: string | null;
  schemaRegistryBasicAuthCredentials: string | null;
  /** Base URL of this cluster's ksqlDB server. Absent for the many clusters that run none. */
  ksqldbEndpoint: string | null;
  /** `user:password`, the same shape the schema registry's credentials use. */
  ksqldbBasicAuthCredentials: string | null;
  schemaRegistryTrustStoreLocation: string | null;
  schemaRegistryTrustStorePassword: string | null;
  schemaRegistryKeystoreLocation: string | null;
  schemaRegistryKeystorePassword: string | null;
  schemaRegistryKeystoreKeyPassword: string | null;
  sslTruststoreLocation: string | null;
  sslTruststorePassword: string | null;
  sslKeystoreLocation: string | null;
  sslKeystorePassword: string | null;
  sslKeystoreKeyPassword: string | null;
  allowPublishing: boolean;
}

export interface Tab {
  id: string;
  name: string;
  position: number;
}

export interface BrokerSummary {
  id: number;
  host: string;
  port: number;
}

export interface TopicSummary {
  name: string;
  partitionCount: number;
}

export interface ConsumerGroupSummary {
  groupId: string;
  state: string;
}

export interface PartitionSummary {
  id: number;
  leader: number;
  replicas: number[];
  isr: number[];
  lowOffset: number;
  highOffset: number;
}

export interface ConfigEntry {
  name: string;
  value: string | null;
}

export interface PartitionLag {
  topic: string;
  partition: number;
  currentOffset: number | null;
  logEndOffset: number;
  lag: number | null;
  clientId: string | null;
  clientHost: string | null;
}

export interface ConsumerGroupLag {
  state: string;
  partitions: PartitionLag[];
}

/** All filter fields optional — an all-undefined filter pulls every message. `includePayload` defaults to false (metadata-only). */
export interface MessageFilter {
  partitions: number[] | null;
  maxMessagesPerPartition: number | null;
  maxTotalMessages: number | null;
  fromTimestampMs: number | null;
  toTimestampMs: number | null;
  offset: number | null;
  includePayload: boolean;
  /**
   * How many payload bytes to carry back per message; `null` means the whole
   * payload.
   *
   * The Data tab's grid fetch always sets this to `VALUE_PREVIEW_BYTES` — it
   * renders one line per row, so shipping whole payloads for it cost ~4 GB
   * of base64 on a 1,000-row fetch
   * of 3 MB records (once streamed, once in the result) to display a few
   * hundred KB of text, and the webview died holding it. Only the
   * single-message fetch behind the payload viewer passes `null`.
   */
  maxPayloadPreviewBytes: number | null;
}

export interface MessageHeader {
  key: string;
  /** Base64-encoded — a header value is an arbitrary Kafka byte string, not guaranteed text. Decode with `base64ToBytes`/`bytesToText` from `payloadDecoding.ts` to display. */
  valueBase64: string | null;
}

export interface TopicMessage {
  partition: number;
  offset: number;
  timestampMs: number | null;
  /** Base64-encoded — a Kafka message key is an arbitrary byte string, not guaranteed text. Decode with `base64ToBytes`/`bytesToText` from `payloadDecoding.ts` to display. */
  keyBase64: string | null;
  /** null unless the fetch's `includePayload` filter was set, and truncated to its `maxPayloadPreviewBytes` when that was set — use `isPayloadTruncated` before treating this as the whole message. */
  payloadBase64: string | null;
  /** The payload's true size in bytes, however much of it `payloadBase64` carries. Populated whenever the message has a payload, even on a metadata-only fetch. */
  payloadSizeBytes: number | null;
  /** Always populated regardless of `includePayload` — headers are cheap metadata, not the payload itself. */
  headers: MessageHeader[];
}

/** Payload of the `"messages-batch"` event, emitted once per message as `connection_fetch_messages` streams results. `requestId` must be checked against the id passed into that call — a stale fetch (superseded by a newer one, or one the user hit Stop on) keeps emitting until its backend task finishes, so a listener must ignore events for any other request. */
/**
 * A batch of streamed rows from an in-flight fetch.
 *
 * One event carries many messages: it used to carry exactly one, which cost a
 * separate IPC hop and a separate serialization per row for a listener that
 * buffers them and repaints ten times a second anyway.
 */
export interface MessagesBatchEvent {
  requestId: string;
  messages: TopicMessage[];
}

/** `totalMatching` is how many messages satisfy the fetch's partition/offset/timestamp filter in total, uncapped by "max messages per partition"/"total max messages" — `messages.length` can be smaller than this when those caps trimmed the result, telling the Data tab more remain beyond what was actually loaded. */
export interface MessageFetchResult {
  messages: TopicMessage[];
  totalMatching: number;
  pollError?: string | null;
  /** True when the fetch stopped because it had read `maxTotalFetchBytes` of message payloads rather than because it satisfied the filter — the row count alone can't distinguish the two, and on a topic of large messages this is the cap that actually bites. */
  stoppedAtByteBudget?: boolean;
  /** Payload bytes read from the broker during this fetch, before any preview truncation. */
  payloadBytesRead?: number;
}


/** How an entered key, value or header value becomes bytes. Mirrors the Rust `PayloadEncoding`. */
export type PayloadEncoding = "null" | "text" | "json" | "base64";

/** One entered field: what the user chose, and what they typed. */
export interface PublishField {
  encoding: PayloadEncoding;
  text: string;
}

export interface PublishHeaderInput {
  key: string;
  value: PublishField;
}

/**
 * One message to publish, as entered. Deliberately carries the *text* and the
 * declared encoding rather than bytes: `encode_messages` in Rust is the only
 * thing that turns one into the other, so a malformed payload is refused in one
 * place regardless of how the command was called.
 */
export interface NewPublishMessage {
  key: PublishField;
  value: PublishField;
  headers: PublishHeaderInput[];
}

export interface DeliveredRecord {
  /** Zero-based index into the submitted message list. */
  index: number;
  partition: number;
  offset: number;
}

/**
 * Why a publish stopped. `authorization` is the one the UI treats specially: it
 * means this principal may not write to the topic, so retrying is pointless and
 * the tab disables itself.
 */
export type PublishFailureKind = "authorization" | "authentication" | "validation" | "transient";

export interface PublishFailure {
  index: number;
  kind: PublishFailureKind;
  reason: string;
}

/** What a publish did: what landed, what failed, and what was never tried. */
export interface PublishOutcome {
  delivered: DeliveredRecord[];
  failure: PublishFailure | null;
  notAttempted: number;
}

/** Where the schema a protobuf payload was decoded with came from — `"none"` means field numbers, not names. */
export type ProtobufSchemaSource = "manual" | "registry" | "none";

/**
 * A decoded protobuf payload.
 *
 * Richer than `decodeAvro`'s bare value because protobuf can be decoded with
 * *no* schema at all: the wire format carries field numbers and types, so a
 * payload always renders as something. `source` is what lets the UI say which
 * of the two happened — "these are your schema's field names" and "these are
 * numbers I read off the bytes" must not look alike.
 */
export interface ProtobufDecodeResult {
  value: unknown;
  source: ProtobufSchemaSource;
  /** The fully-qualified message name, when a schema named one. */
  messageType: string | null;
}

export interface ImportSummary {
  imported: number;
  skipped: number;
}

/** What kind of resource an ACL binding governs. Mirrors `salty_core::ResourceType`. */
export type AclResourceType = "unknown" | "any" | "topic" | "group" | "broker" | "transactionalId";

/**
 * How a binding's resource name is matched.
 *
 * The attribute the UI makes most prominent: `prefixed` is what makes a grant
 * cover resources that do not exist yet.
 */
export type AclPatternType = "unknown" | "any" | "match" | "literal" | "prefixed";

export type AclOperation =
  | "unknown"
  | "any"
  | "all"
  | "read"
  | "write"
  | "create"
  | "delete"
  | "alter"
  | "describe"
  | "clusterAction"
  | "describeConfigs"
  | "alterConfigs"
  | "idempotentWrite";

export type AclPermission = "unknown" | "any" | "deny" | "allow";

/** One ACL binding exactly as the broker reported it. */
export interface AclBinding {
  resourceType: AclResourceType;
  /** Read together with `patternType` — the same string means different things under `literal` and `prefixed`. */
  resourceName: string;
  patternType: AclPatternType;
  /** Kafka's principal string, conventionally `User:alice`. `User:*` is the any-principal wildcard. */
  principal: string;
  host: string;
  operation: AclOperation;
  permission: AclPermission;
}

/**
 * What the ACL listing was actually able to tell us.
 *
 * An empty list is dangerously ambiguous, so the meaning travels with it:
 *
 * - `noAuthorizer` — the broker runs no authorizer, so ACLs are not consulted
 *   and every principal may do anything. Empty here means *unrestricted*.
 * - `available` — the broker answered with bindings.
 * - `indeterminate` — nothing came back and the client cannot tell whether
 *   no ACLs are defined or this principal may not read them. librdkafka
 *   discards the broker's error code for DescribeAcls, so this genuinely
 *   cannot be resolved; see the backend's `AclAvailability`.
 */
export type AclAvailability = "noAuthorizer" | "available" | "indeterminate";

export interface AclListing {
  availability: AclAvailability;
  bindings: AclBinding[];
  /** Bindings the broker returned carrying an error of their own — reported rather than allowed to fail the listing. */
  bindingErrors: string[];
}

/**
 * Why a verdict came out the way it did.
 *
 * Carried so the UI can label a tick as *implied* rather than presenting it
 * as a grant somebody wrote — a reader who cannot tell the difference cannot
 * tell which ACL to change.
 */
export type AclVerdictReason =
  | { kind: "explicitDeny" }
  | { kind: "directAllow" }
  | { kind: "impliedAllow"; via: AclOperation }
  | { kind: "defaultDeny" };

export interface AclOperationVerdict {
  operation: AclOperation;
  allowed: boolean;
  reason: AclVerdictReason;
  /** The deciding binding was written against `User:*`, so revoking it affects everyone. */
  viaWildcardPrincipal: boolean;
}

export interface AclPrincipalAccess {
  principal: string;
  verdicts: AclOperationVerdict[];
}

/**
 * Everything the Access tab needs about one resource.
 *
 * `access` is computed in Rust (`salty_core::resource_access`), not here:
 * Kafka's deny-precedence and implication rules have exactly one
 * implementation, the one with unit tests around it.
 */
export interface AclResourceAccess {
  listing: AclListing;
  access: AclPrincipalAccess[];
}

/** A column in a ksqlDB result, as the server described it. */
export interface KsqlColumn {
  name: string;
  /** ksqlDB's own type name — `STRING`, `BIGINT`, … Carried through so a numeric column can get a numeric filter. */
  kind: string;
}

/** One result row: the JSON values in column order. */
export type KsqlRow = unknown[];

/** The columns a query returned, delivered before any row. */
export interface KsqlHeaderEvent {
  requestId: string;
  columns: KsqlColumn[];
}

/** A batch of result rows. */
export interface KsqlRowsEvent {
  requestId: string;
  rows: KsqlRow[];
}

export interface KsqlQueryOutcome {
  /** True when Stop ended it rather than the server closing the stream. */
  cancelled: boolean;
  rowCount: number;
}

export const api = {
  listConnections: () => invoke<Connection[]>("connection_list"),
  createConnection: (newConnection: NewConnection) =>
    invoke<Connection>("connection_create", { newConnection }),
  updateConnection: (id: string, newConnection: NewConnection) =>
    invoke<Connection>("connection_update", { id, newConnection }),
  deleteConnection: (id: string) => invoke<void>("connection_delete", { id }),
  /** `ids: null` exports every connection; a specific list exports just those. */
  exportConnections: (ids: string[] | null, path: string) =>
    invoke<void>("connections_export", { ids, path }),
  importConnections: (path: string) => invoke<ImportSummary>("connections_import", { path }),
  checkConnectionStatus: (id: string) =>
    invoke<ConnectionStatus>("connection_check_status", { id }),
  pingBootstrapServers: (bootstrapServers: string) =>
    invoke<ConnectionStatus>("connection_ping_bootstrap", { bootstrapServers }),
  pingZookeeper: (host: string, port: number) =>
    invoke<ConnectionStatus>("connection_ping_zookeeper", {
      host,
      port,
      timeoutMs: useGeneralSettingsStore.getState().zookeeperTimeoutMs,
    }),
  testConnection: (newConnection: NewConnection) =>
    invoke<ConnectionStatus>("connection_test", { newConnection }),
  detectClusterVersion: (newConnection: NewConnection) =>
    invoke<ClusterVersionReport>("connection_detect_version", {
      newConnection,
      // A broker read, like listBrokers/listTopics — not the ZooKeeper ping's timeout.
      timeoutMs: useGeneralSettingsStore.getState().brokerReadTimeoutMs,
    }),
  connectConnection: (id: string) => invoke<ConnectionStatus>("connection_connect", { id }),
  disconnectConnection: (id: string) => invoke<void>("connection_disconnect", { id }),
  isConnectionConnected: (id: string) => invoke<boolean>("connection_is_connected", { id }),
  /** Why the backend is refusing this connection's requests without dialling the broker, or null if it isn't. */
  connectionAuthBlockReason: (id: string) => invoke<string | null>("connection_auth_block_reason", { id }),
  listBrokers: (id: string) =>
    invoke<BrokerSummary[]>("connection_list_brokers", {
      id,
      readTimeoutMs: useGeneralSettingsStore.getState().brokerReadTimeoutMs,
    }),
  listTopics: (id: string) =>
    invoke<TopicSummary[]>("connection_list_topics", {
      id,
      readTimeoutMs: useGeneralSettingsStore.getState().brokerReadTimeoutMs,
    }),
  listConsumerGroups: (id: string) =>
    invoke<ConsumerGroupSummary[]>("connection_list_consumer_groups", {
      id,
      readTimeoutMs: useGeneralSettingsStore.getState().brokerReadTimeoutMs,
    }),
  countTopicMessages: (id: string, topic: string) =>
    invoke<number>("connection_count_topic_messages", {
      id,
      topic,
      readTimeoutMs: useGeneralSettingsStore.getState().brokerReadTimeoutMs,
    }),
  /**
   * `streamUpdates` decides how the messages come back, and every caller has
   * to choose:
   *
   * - `true` — rows arrive as `"messages-batch"` events while the fetch runs,
   *   and `MessageFetchResult.messages` carries only what the stream did not
   *   deliver (normally nothing). For the Data tab's Fetch, which paints rows
   *   as they land; without it every message crossed the IPC boundary twice.
   * - `false` — nothing is streamed and the result carries the messages. For
   *   the single-message fetches, whose events no listener wants.
   */
  fetchMessages: (
    id: string,
    topic: string,
    filter: MessageFilter,
    requestId: string,
    streamUpdates: boolean,
  ) =>
    invoke<MessageFetchResult>("connection_fetch_messages", {
      id,
      topic,
      filter,
      requestId,
      readTimeoutMs: useGeneralSettingsStore.getState().brokerReadTimeoutMs,
      maxMessageSizeBytes: useGeneralSettingsStore.getState().maxMessageSizeBytes,
      maxTotalPayloadBytes: useGeneralSettingsStore.getState().maxTotalFetchBytes,
      streamUpdates,
    }),
  /** Interrupts a fetch already in flight — the Data tab's Stop button, and switching topics mid-fetch. A no-op if the request already finished. */
  cancelFetch: (requestId: string) => invoke<void>("connection_cancel_fetch", { requestId }),
  listPartitions: (id: string, topic: string) =>
    invoke<PartitionSummary[]>("connection_list_partitions", {
      id,
      topic,
      readTimeoutMs: useGeneralSettingsStore.getState().brokerReadTimeoutMs,
    }),
  describeTopicConfig: (id: string, topic: string) =>
    invoke<ConfigEntry[]>("connection_describe_topic_config", {
      id,
      topic,
      readTimeoutMs: useGeneralSettingsStore.getState().brokerReadTimeoutMs,
    }),
  /**
   * Runs a ksqlDB statement that returns one response — SHOW, DESCRIBE,
   * EXPLAIN, CREATE STREAM. Streaming queries go through `ksqlQuery`.
   */
  ksqlStatement: (id: string, sql: string) =>
    invoke<{ body: string }>("ksql_statement", { id, sql }),
  /** The stream registered over a topic, if any — ksqlDB cannot SELECT from a raw topic. */
  ksqlStreamForTopic: (id: string, topic: string) =>
    invoke<string | null>("ksql_stream_for_topic", { id, topic }),
  /**
   * Runs a push query. Rows arrive as `"ksql-rows"` events tagged with
   * `requestId`, preceded by one `"ksql-header"` naming the columns — the
   * header cannot wait for this promise, because a live tail never resolves
   * until it is stopped.
   */
  ksqlQuery: (id: string, sql: string, requestId: string) =>
    invoke<KsqlQueryOutcome>("ksql_query", { id, sql, requestId }),
  /** Stops a running query. The Stop button. */
  ksqlCancel: (requestId: string) => invoke<void>("ksql_cancel", { requestId }),
  /** Every ACL the broker will show this principal — backs the tree's Access Control category. */
  listAcls: (id: string) =>
    invoke<AclListing>("acl_list", {
      id,
      readTimeoutMs: useGeneralSettingsStore.getState().brokerReadTimeoutMs,
    }),
  /**
   * Every ACL that *governs* one named resource — backs the Access tabs.
   *
   * A separate broker call rather than a client-side filter of `listAcls`:
   * the backend sends this in MATCH mode so the broker resolves which
   * literal, prefixed and wildcard patterns apply. Matching patterns here
   * would mean reimplementing the authorizer.
   */
  aclsForResource: (id: string, resourceType: AclResourceType, resourceName: string) =>
    invoke<AclResourceAccess>("acl_for_resource", {
      id,
      resourceType,
      resourceName,
      readTimeoutMs: useGeneralSettingsStore.getState().brokerReadTimeoutMs,
    }),
  fetchConsumerGroupLag: (id: string, groupId: string) =>
    invoke<ConsumerGroupLag>("connection_fetch_consumer_group_lag", {
      id,
      groupId,
      readTimeoutMs: useGeneralSettingsStore.getState().brokerReadTimeoutMs,
    }),
  /**
   * Publishes messages to one partition. Rejects when any of the backend's
   * gates refuses (not connected, publishing not allowed for the connection, a
   * cached broker denial, or a message that fails validation) — in which case
   * nothing reached the cluster. Resolves with the outcome when the publish
   * reached the broker, *including* when it failed part-way: the outcome then
   * says which messages landed and at which offsets, which an error could not.
   */
  publishMessages: (id: string, topic: string, partition: number, messages: NewPublishMessage[]) =>
    invoke<PublishOutcome>("connection_publish_messages", {
      id,
      topic,
      partition,
      messages,
      maxMessageSizeBytes: useGeneralSettingsStore.getState().maxMessageSizeBytes,
      writeTimeoutMs: useGeneralSettingsStore.getState().brokerReadTimeoutMs,
    }),
  /** Why publishing to this topic is blocked without asking the broker, or `null` if it isn't. */
  writeDeniedReason: (id: string, topic: string) =>
    invoke<string | null>("connection_write_denied_reason", { id, topic }),
  getTopicSchema: (connectionId: string, topic: string, format: SchemaFormat) =>
    invoke<string | null>("topic_schema_get", { connectionId, topic, format }),
  setTopicSchema: (connectionId: string, topic: string, format: SchemaFormat, schemaText: string) =>
    invoke<void>("topic_schema_set", { connectionId, topic, format, schemaText }),
  deleteTopicSchema: (connectionId: string, topic: string, format: SchemaFormat) =>
    invoke<void>("topic_schema_delete", { connectionId, topic, format }),
  decodeAvro: (connectionId: string, topic: string, payloadBase64: string) =>
    invoke<unknown>("connection_decode_avro", { id: connectionId, topic, payloadBase64 }),
  decodeProtobuf: (connectionId: string, topic: string, payloadBase64: string) =>
    invoke<ProtobufDecodeResult>("connection_decode_protobuf", { id: connectionId, topic, payloadBase64 }),
  listTabs: () => invoke<Tab[]>("tab_list"),
  createTab: (name: string) => invoke<Tab>("tab_create", { name }),
  renameTab: (id: string, name: string) => invoke<void>("tab_rename", { id, name }),
  deleteTab: (id: string) => invoke<void>("tab_delete", { id }),
  reorderTabs: (ids: string[]) => invoke<void>("tab_reorder", { ids }),
  /** Trims the OS-visible working set on Windows (a no-op elsewhere) — see `commands::system::trim_process_memory`'s doc comment for why clearing app-level data alone doesn't shrink what Task Manager reports. */
  trimProcessMemory: () => invoke<void>("trim_process_memory"),
  /**
   * Writes bytes to `path` — the payload viewer's and the JSON tab's Save
   * buttons, after the native save dialog has resolved where.
   *
   * Takes base64 rather than bytes because Tauri's IPC is JSON: a `Uint8Array`
   * crosses it as a decimal array, roughly three times the characters of the
   * base64 the payload is already held as.
   */
  savePayloadFile: (path: string, contentsBase64: string) =>
    invoke<void>("payload_save", { path, contentsBase64 }),
};
