import React from "react";
import ReactDOM from "react-dom/client";
import { App } from "../src/App";
import { useTabDataStore } from "../src/features/workspace/useTabDataStore";
import { useDataTabGridStateStore } from "../src/features/connections/useDataTabGridStateStore";
import { useDataTabFiltersStore } from "../src/features/connections/useDataTabFiltersStore";
import { useTabsStore } from "../src/features/tabs/useTabsStore";
import { useMessageViewerStore } from "../src/features/workspace/useMessageViewerStore";

const conn = {
  id: "conn-1",
  name: "local",
  bootstrapServers: "localhost:9092",
  kafkaVersion: "3.7",
  zookeeperEnabled: false,
  zookeeperHost: null, zookeeperPort: null, zookeeperChrootPath: null,
  securityProtocol: "PLAINTEXT",
  saslMechanism: null, saslUsername: null, saslPassword: null, saslOauthUrl: null,
  schemaRegistryEndpoint: null, schemaRegistryBasicAuthCredentials: null,
  schemaRegistryTrustStoreLocation: null, schemaRegistryTrustStorePassword: null,
  schemaRegistryKeystoreLocation: null, schemaRegistryKeystorePassword: null,
  schemaRegistryKeystoreKeyPassword: null,
  sslTruststoreLocation: null, sslTruststorePassword: null,
  sslKeystoreLocation: null, sslKeystorePassword: null, sslKeystoreKeyPassword: null,
  createdAt: "2026-01-01T00:00:00Z", updatedAt: "2026-01-01T00:00:00Z",
};

const tabs = [
  { id: "tab-1", name: "Tab 1", position: 0 },
  { id: "tab-2", name: "Tab 2", position: 1 },
];

const w = window as never as {
  __handlers: Record<string, (a: Record<string, unknown>) => unknown>;
  __emit: (e: string, p: unknown) => void;
  __fetchLog: unknown[];
};
w.__fetchLog = [];
(window as never as { __stores: () => unknown }).__stores = () => {
  const d = useTabDataStore.getState();
  return {
    activeTabId: useTabsStore.getState().activeTabId,
    rows: Object.fromEntries(Object.entries(d.messagesByTab).map(([k, v]) => [k, v.length])),
    payloadBytes: d.payloadBytesByTab,
    totalMatching: d.totalMatchingByTab,
    gridState: useDataTabGridStateStore.getState().stateByTab,
    forms: useDataTabFiltersStore.getState().formByTab,
    viewer: { active: useMessageViewerStore.getState().activeTabId, byTab: Object.fromEntries(Object.entries(useMessageViewerStore.getState().byTab).map(([k, v]) => [k, v ? v.partitionId + "|" + v.topic + "|" + v.message.partition + ":" + v.message.offset : null])) },
    fetchLog: (window as never as { __fetchLog: unknown[] }).__fetchLog,
  };
};

// A realistic JSON document, so the payload panel's JSON/XML/Raw/Hex/Base64
// views all have something meaningful to render.
const PAYLOAD_JSON = JSON.stringify(
  {
    orderId: "ORD-10042",
    customer: { id: "CUST-7", name: "Ada Lovelace", email: "ada@example.com" },
    items: [
      { sku: "SKU-1", qty: 2, price: 19.99 },
      { sku: "SKU-2", qty: 1, price: 149.0 },
    ],
    total: 188.98,
    currency: "GBP",
    placedAt: "2026-09-18T09:14:22Z",
    notes: null,
    paid: true,
  },
  null,
  2,
);
const PAYLOAD_B64 = btoa(PAYLOAD_JSON);

Object.assign(w.__handlers, {
  connection_list: () => [conn],
  connection_check_status: () => "REACHABLE",
  connection_is_connected: () => true,
  connection_connect: () => null,
  connection_disconnect: () => null,
  connection_auth_block_reason: () => null,
  tab_list: () => tabs,
  tab_create: (a: Record<string, unknown>) => ({ id: "tab-new", name: a.name, position: 9 }),
  tab_delete: () => null,
  tab_rename: () => null,
  tab_reorder: () => null,
  trim_process_memory: () => null,
  connection_cancel_fetch: () => null,
  connection_create: (a: Record<string, unknown>) => ({ ...conn, id: "conn-2", ...(a.newConnection as object) }),
  connection_update: () => conn,
  connection_delete: () => null,
  connections_export: () => null,
  connections_import: () => ({ imported: 1, skipped: 0 }),
  connection_ping_bootstrap: () => "REACHABLE",
  connection_ping_zookeeper: () => "REACHABLE",
  connection_test: () => "REACHABLE",
  // Shape must match `ConsumerGroupLag` in lib/tauri.ts: { state, partitions }.
  connection_fetch_consumer_group_lag: () => ({
    state: "Stable",
    partitions: [
      { topic: "orders", partition: 0, currentOffset: 90, logEndOffset: 100, lag: 10, clientId: "c-1", clientHost: "/127.0.0.1" },
      { topic: "orders", partition: 1, currentOffset: 140, logEndOffset: 140, lag: 0, clientId: "c-1", clientHost: "/127.0.0.1" },
    ],
  }),
  connection_write_denied_reason: () => null,
  connection_publish_messages: () => ({
    delivered: [{ index: 0, partition: 0, offset: 1200 }],
    failure: null,
  }),
  connection_decode_avro: () => ({
    orderId: "ORD-10042",
    customer: { name: "Ada Lovelace" },
    total: 188.98,
  }),
  payload_save: (a: Record<string, unknown>) => {
    (window as never as { __saved: unknown[] }).__saved ??= [];
    (window as never as { __saved: unknown[] }).__saved.push({
      path: a.path,
      contents: atob(a.contentsBase64 as string),
    });
    return null;
  },
  connection_list_topics: () =>
    // Well past ResourceCategory's VIRTUALIZE_THRESHOLD (50), so the
    // react-window path is the one exercised.
    [
      { name: "orders", partitionCount: 2 },
      { name: "payments", partitionCount: 2 },
      ...Array.from({ length: 300 }, (_, i) => ({ name: `topic-${i}`, partitionCount: 1 })),
    ],
  connection_list_brokers: () => [{ id: 1, host: "localhost", port: 9092 }],
  // Past ResourceCategory's VIRTUALIZE_THRESHOLD (50), so the react-window
  // path is the one exercised. (Topics deliberately uses its own
  // non-virtualized TopicCategory.)
  connection_list_consumer_groups: () =>
    Array.from({ length: 300 }, (_, i) => ({ groupId: `group-${i}`, state: "Stable", members: 1 })),
  connection_list_partitions: () => [
    { id: 0, leader: 1, replicas: [1], isr: [1], lowOffset: 0, highOffset: 100 },
    { id: 1, leader: 1, replicas: [1], isr: [1], lowOffset: 0, highOffset: 100 },
  ],
  connection_count_topic_messages: () => 200,
  connection_describe_topic_config: () => [],
  // Per-format, so the Schema tab's Avro/Protobuf switch can be exercised.
  topic_schema_get: (a: Record<string, unknown>) =>
    (window as never as { __schemas: Record<string, string> }).__schemas?.[a.format as string] ?? null,
  topic_schema_set: (a: Record<string, unknown>) => {
    const w = window as never as { __schemas: Record<string, string> };
    w.__schemas ??= {};
    w.__schemas[a.format as string] = a.schemaText as string;
    return null;
  },
  topic_schema_delete: (a: Record<string, unknown>) => {
    const w = window as never as { __schemas: Record<string, string> };
    delete w.__schemas?.[a.format as string];
    return null;
  },
  // Mirrors the real command's return shape. `__protobufSource` switches
  // between the schema-decoded and the field-numbers-only renderings so both
  // UI states can be looked at.
  connection_decode_protobuf: () => {
    const source = (window as never as { __protobufSource?: string }).__protobufSource ?? "manual";
    if (source === "none") {
      return {
        value: { "1": "ORD-10042", "2": 2, "3": true, "5": { "1": "Ada Lovelace" } },
        source: "none",
        messageType: null,
      };
    }
    return {
      value: {
        order_id: "ORD-10042",
        quantity: 2,
        paid: true,
        customer: { name: "Ada Lovelace", email: "ada@example.com" },
        tags: ["priority", "gift"],
      },
      source,
      messageType: "shop.Order",
    };
  },
  connection_fetch_messages: async (a: Record<string, unknown>) => {
    const requestId = a.requestId as string;
    const streamUpdates = a.streamUpdates as boolean;
    w.__fetchLog.push({ topic: a.topic, requestId, streamUpdates });
    const messages = [];
    for (let i = 0; i < 200; i++) {
      messages.push({
        partition: i % 2,
        offset: 1000 + i,
        timestampMs: 1700000000000 + i * 1000,
        keyBase64: btoa("key-" + i),
        payloadBase64: PAYLOAD_B64,
        payloadSizeBytes: 30000,
        headers: [],
      });
    }
    // Mirrors the real backend: batched events while streaming, and a result
    // carrying only what the stream did not deliver.
    let streamed = 0;
    if (streamUpdates) {
      const BATCH = 64;
      for (let i = 0; i < messages.length; i += BATCH) {
        const batch = messages.slice(i, i + BATCH);
        w.__emit("messages-batch", { requestId, messages: batch });
        streamed += batch.length;
      }
    }
    await new Promise((r) => setTimeout(r, 400));
    return {
      messages: messages.slice(streamed),
      totalMatching: 200,
      payloadBytesRead: 200 * 3000,
    };
  },
});

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
