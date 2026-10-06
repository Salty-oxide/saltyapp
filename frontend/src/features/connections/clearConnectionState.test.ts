import { beforeEach, describe, expect, it } from "vitest";
import { QueryClient } from "@tanstack/react-query";
import { useMessageViewerStore } from "../workspace/useMessageViewerStore";
import { useTabDataStore } from "../workspace/useTabDataStore";
import { useWorkspaceSelectionStore } from "../workspace/useWorkspaceSelectionStore";
import { clearConnectionState } from "./clearConnectionState";
import { useDataTabFiltersStore } from "./useDataTabFiltersStore";
import { useDataTabGridStateStore } from "./useDataTabGridStateStore";
import { useKsqlStore, ksqlWorkspaceKey } from "../ksql/useKsqlStore";
import { useTreeUiStore } from "./useTreeUiStore";
import { emptyFilterForm } from "./dataFilters";
import { emptyMessage } from "./publishMessages";
import { usePublishDraftStore } from "./usePublishDraftStore";

const GONE = "conn-gone";
const KEPT = "conn-kept";

const message = {
  partition: 0,
  offset: 1,
  timestampMs: null,
  keyBase64: null,
  payloadBase64: "eA==",
  payloadSizeBytes: 1,
  headers: [],
};

let queryClient: QueryClient;

/** Two tabs' worth of state for two connections, so every assertion can tell "cleared this cluster" from "cleared everything". */
function seedState() {
  useTreeUiStore.setState({
    expanded: {
      [`tab-1:connection:${GONE}`]: true,
      [`tab-1:${GONE}:Topics`]: true,
      [`tab-1:${GONE}:topic:orders`]: true,
      [`tab-2:${GONE}:Brokers`]: true,
      [`tab-1:connection:${KEPT}`]: true,
      [`tab-1:${KEPT}:Topics`]: true,
    },
    searchText: { [`tab-1:${GONE}:Topics`]: "ord", [`tab-1:${KEPT}:Topics`]: "ship" },
    hideEmptyConsumerGroups: { [`tab-1:${GONE}:Consumers`]: true, [`tab-1:${KEPT}:Consumers`]: true },
  });
  useWorkspaceSelectionStore.setState({
    activeTabId: "tab-1",
    selection: { type: "topic", connectionId: GONE, topicName: "orders" },
    byTab: {
      "tab-1": { type: "topic", connectionId: GONE, topicName: "orders" },
      "tab-2": { type: "connection", id: KEPT, name: "kept" },
    },
  });
  useMessageViewerStore.setState({
    activeTabId: "tab-1",
    message,
    connectionId: GONE,
    topic: "orders",
    partitionId: undefined,
    byTab: {
      "tab-1": { message, connectionId: GONE, topic: "orders" },
      "tab-2": { message, connectionId: KEPT, topic: "shipments" },
    },
  });
  useTabDataStore.setState({
    messagesByTab: { [`tab-1:${GONE}:orders:all`]: [message], [`tab-1:${KEPT}:ships:all`]: [message] },
    totalMatchingByTab: { [`tab-1:${GONE}:orders:all`]: 1, [`tab-1:${KEPT}:ships:all`]: 1 },
    payloadBytesByTab: { [`tab-1:${GONE}:orders:all`]: 99, [`tab-1:${KEPT}:ships:all`]: 99 },
  });
  useDataTabFiltersStore.setState({
    formByTab: { [`tab-1:${GONE}:orders:all`]: emptyFilterForm(), [`tab-1:${KEPT}:ships:all`]: emptyFilterForm() },
  });
  useDataTabGridStateStore.setState({
    stateByTab: {
      [`tab-1:${GONE}:orders:all`]: { sortModel: [{ colId: "offset", sort: "desc" as const }], filterModel: {} },
      [`tab-1:${KEPT}:ships:all`]: { sortModel: [{ colId: "offset", sort: "asc" as const }], filterModel: {} },
    },
  });
  useKsqlStore.setState({
    byKey: {
      [ksqlWorkspaceKey("tab-1", GONE, "cluster")]: {
        sql: "SELECT 1;",
        status: "idle",
        columns: [],
        rows: [],
        received: 0,
        nextIndex: 0,
        requestId: null,
        error: null,
      },
      [ksqlWorkspaceKey("tab-1", KEPT, "cluster")]: {
        sql: "SELECT 2;",
        status: "idle",
        columns: [],
        rows: [],
        received: 0,
        nextIndex: 0,
        requestId: null,
        error: null,
      },
    },
  });
  usePublishDraftStore.setState({
    messagesByTab: {
      [`tab-1:${GONE}:orders:0`]: [emptyMessage()],
      [`tab-2:${GONE}:orders:1`]: [emptyMessage()],
      [`tab-1:${KEPT}:ships:0`]: [emptyMessage()],
    },
  });
  for (const id of [GONE, KEPT]) {
    queryClient.setQueryData(["topics", id], [{ name: "orders" }]);
    queryClient.setQueryData(["brokers", id], [{ id: 1 }]);
    queryClient.setQueryData(["consumer-groups", id], []);
    queryClient.setQueryData(["partitions", id, "orders"], []);
    queryClient.setQueryData(["topic-config", id, "orders"], []);
    queryClient.setQueryData(["topic-schema", id, "orders", "avro"], null);
    queryClient.setQueryData(["full-payload", id, "orders", 0, 1], null);
    queryClient.setQueryData(["connection-connected", id], true);
    queryClient.setQueryData(["connection-status", id], "REACHABLE");
    queryClient.setQueryData(["connection-auth-block", id], null);
  }
}

beforeEach(() => {
  queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  seedState();
});

describe("clearConnectionState", () => {
  it("collapses every tree row of the disconnected cluster, in every tab", () => {
    clearConnectionState(queryClient, GONE);

    const { expanded } = useTreeUiStore.getState();
    expect(Object.keys(expanded)).toEqual([`tab-1:connection:${KEPT}`, `tab-1:${KEPT}:Topics`]);
  });

  it("drops the disconnected cluster's category search boxes and consumer-group toggles", () => {
    clearConnectionState(queryClient, GONE);

    expect(useTreeUiStore.getState().searchText).toEqual({ [`tab-1:${KEPT}:Topics`]: "ship" });
    expect(useTreeUiStore.getState().hideEmptyConsumerGroups).toEqual({ [`tab-1:${KEPT}:Consumers`]: true });
  });

  it("clears the middle pane by dropping the selection, in every tab", () => {
    clearConnectionState(queryClient, GONE);

    expect(useWorkspaceSelectionStore.getState().selection).toBeNull();
    expect(useWorkspaceSelectionStore.getState().byTab["tab-1"]).toBeNull();
    // Another cluster's tab keeps what it was showing.
    expect(useWorkspaceSelectionStore.getState().byTab["tab-2"]).toEqual({
      type: "connection",
      id: KEPT,
      name: "kept",
    });
  });

  it("clears the right pane by dropping the viewed message, in every tab", () => {
    clearConnectionState(queryClient, GONE);

    expect(useMessageViewerStore.getState().message).toBeNull();
    expect(useMessageViewerStore.getState().byTab["tab-1"]).toBeNull();
    expect(useMessageViewerStore.getState().byTab["tab-2"]).not.toBeNull();
  });

  it("drops the cluster's fetched rows, byte accounting, filter form and grid arrangement", () => {
    clearConnectionState(queryClient, GONE);

    expect(useTabDataStore.getState().messagesByTab).toEqual({ [`tab-1:${KEPT}:ships:all`]: [message] });
    expect(useTabDataStore.getState().totalMatchingByTab).toEqual({ [`tab-1:${KEPT}:ships:all`]: 1 });
    expect(useTabDataStore.getState().payloadBytesByTab).toEqual({ [`tab-1:${KEPT}:ships:all`]: 99 });
    expect(Object.keys(useDataTabFiltersStore.getState().formByTab)).toEqual([`tab-1:${KEPT}:ships:all`]);
    expect(Object.keys(useDataTabGridStateStore.getState().stateByTab)).toEqual([`tab-1:${KEPT}:ships:all`]);
  });

  // The ksqlDB workspace holds the editor's text and whatever rows a query
  // left behind, both of which describe a cluster the app has stopped talking
  // to.
  it("forgets the disconnected connection's ksqlDB workspaces and keeps the others", () => {
    seedState();

    clearConnectionState(queryClient, GONE);

    expect(Object.keys(useKsqlStore.getState().byKey)).toEqual([
      ksqlWorkspaceKey("tab-1", KEPT, "cluster"),
    ]);
  });

  it("drops the cluster's unsent publish drafts, in every tab, and keeps other clusters'", () => {
    // A draft can only be published to a connected cluster, so one that outlived
    // the session it was composed in could never be sent as-is — and finding a
    // half-written message to a production topic waiting after a reconnect is
    // worse than retyping it.
    clearConnectionState(queryClient, GONE);

    expect(Object.keys(usePublishDraftStore.getState().messagesByTab)).toEqual([
      `tab-1:${KEPT}:ships:0`,
    ]);
  });

  it("removes every cached listing read from the cluster", () => {
    clearConnectionState(queryClient, GONE);

    for (const key of [
      ["topics", GONE],
      ["brokers", GONE],
      ["consumer-groups", GONE],
      ["partitions", GONE, "orders"],
      ["topic-config", GONE, "orders"],
      ["topic-schema", GONE, "orders", "avro"],
      ["full-payload", GONE, "orders", 0, 1],
    ]) {
      expect(queryClient.getQueryData(key), `${key[0]} should have been removed`).toBeUndefined();
    }
  });

  // Removed, not invalidated: an invalidated query refetches the moment
  // anything observes it, which against a cluster that just went away means
  // an immediate round of failing requests.
  it("removes the listings rather than leaving them to refetch against a dead cluster", () => {
    clearConnectionState(queryClient, GONE);

    expect(queryClient.getQueryCache().find({ queryKey: ["topics", GONE] })).toBeUndefined();
  });

  // These describe the connection, not the cluster: the status dot, the
  // auth-failure banner, and the very query whose transition triggers this
  // cleanup. Dropping them would blank the row the user reconnects from.
  it("keeps the queries that describe the connection itself", () => {
    clearConnectionState(queryClient, GONE);

    expect(queryClient.getQueryData(["connection-connected", GONE])).toBe(true);
    expect(queryClient.getQueryData(["connection-status", GONE])).toBe("REACHABLE");
    expect(queryClient.getQueryData(["connection-auth-block", GONE])).toBeNull();
  });

  it("leaves every other cluster's cached data alone", () => {
    clearConnectionState(queryClient, GONE);

    expect(queryClient.getQueryData(["topics", KEPT])).toEqual([{ name: "orders" }]);
    expect(queryClient.getQueryData(["partitions", KEPT, "orders"])).toEqual([]);
  });

  it("is safe to run for a connection nothing is cached for", () => {
    expect(() => clearConnectionState(queryClient, "never-seen")).not.toThrow();
    expect(useTreeUiStore.getState().expanded[`tab-1:connection:${GONE}`]).toBe(true);
  });
});
