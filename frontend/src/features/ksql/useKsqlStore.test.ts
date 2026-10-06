import { beforeEach, describe, expect, it } from "vitest";
import { MAX_RESULT_ROWS } from "./ksqlRows";
import {
  EMPTY_KSQL_WORKSPACE,
  ksqlKeyBelongsTo,
  ksqlWorkspaceKey,
  useKsqlStore,
} from "./useKsqlStore";

const KEY = ksqlWorkspaceKey("tab-1", "conn-1", "cluster");

beforeEach(() => useKsqlStore.setState({ byKey: {} }));

function state(key = KEY) {
  return useKsqlStore.getState().byKey[key];
}

describe("ksqlWorkspaceKey", () => {
  it("keys a workspace by tab, connection and scope", () => {
    expect(ksqlWorkspaceKey("tab-1", "conn-1", "orders")).toBe("tab-1:conn-1:orders");
  });

  it("keeps a key stable when there is no active tab", () => {
    expect(ksqlWorkspaceKey(null, "conn-1", "cluster")).toBe("no-tab:conn-1:cluster");
  });
});

describe("ksqlKeyBelongsTo", () => {
  it("matches on the connection segment, not a substring of the whole key", () => {
    expect(ksqlKeyBelongsTo("tab-1:conn-1:orders", "conn-1")).toBe(true);
    expect(ksqlKeyBelongsTo("tab-1:conn-1:orders", "tab-1")).toBe(false);
    expect(ksqlKeyBelongsTo("tab-1:conn-10:orders", "conn-1")).toBe(false);
  });
});

describe("useKsqlStore", () => {
  it("starts a key it has never seen from the empty workspace", () => {
    expect(state()).toBeUndefined();
    expect(EMPTY_KSQL_WORKSPACE.status).toBe("idle");
  });

  it("keeps the editor's text", () => {
    useKsqlStore.getState().setSql(KEY, "SELECT 1;");

    expect(state().sql).toBe("SELECT 1;");
  });

  it("marks a run as running and records its request id", () => {
    useKsqlStore.getState().start(KEY, "req-1");

    expect(state().status).toBe("running");
    expect(state().requestId).toBe("req-1");
  });

  // Leaving the previous result on screen under a new query's header reads as
  // though the new query returned it.
  it("clears the previous result the moment a new run starts", () => {
    const store = useKsqlStore.getState();
    store.start(KEY, "req-1");
    store.setColumns(KEY, "req-1", [{ name: "A", kind: "STRING" }]);
    store.addRows(KEY, "req-1", [["a"]]);

    store.start(KEY, "req-2");

    expect(state().rows).toEqual([]);
    expect(state().columns).toEqual([]);
    expect(state().received).toBe(0);
    expect(state().error).toBeNull();
  });

  it("records the columns from the header frame", () => {
    const store = useKsqlStore.getState();
    store.start(KEY, "req-1");

    store.setColumns(KEY, "req-1", [{ name: "ID", kind: "STRING" }]);

    expect(state().columns).toEqual([{ name: "ID", kind: "STRING" }]);
  });

  it("appends batches and counts everything received", () => {
    const store = useKsqlStore.getState();
    store.start(KEY, "req-1");

    store.addRows(KEY, "req-1", [["a"], ["b"]]);
    store.addRows(KEY, "req-1", [["c"]]);

    expect(state().rows.map((row) => row.values[0])).toEqual(["a", "b", "c"]);
    expect(state().received).toBe(3);
  });

  // The cap bounds what is kept; `received` still reports what arrived.
  it("caps the rows it keeps while still counting what arrived", () => {
    const store = useKsqlStore.getState();
    store.start(KEY, "req-1");

    store.addRows(KEY, "req-1", Array.from({ length: MAX_RESULT_ROWS + 10 }, (_, i) => [i]));

    expect(state().rows).toHaveLength(MAX_RESULT_ROWS);
    expect(state().received).toBe(MAX_RESULT_ROWS + 10);
  });

  /**
   * The guard that matters. Events are not ordered against the command that
   * started them, so a batch from a replaced query can land after the new one
   * began — and without this it would appear under the new query's columns.
   */
  it("ignores rows belonging to a superseded run", () => {
    const store = useKsqlStore.getState();
    store.start(KEY, "req-1");
    store.start(KEY, "req-2");

    store.addRows(KEY, "req-1", [["stale"]]);

    expect(state().rows).toEqual([]);
    expect(state().received).toBe(0);
  });

  it("ignores a header belonging to a superseded run", () => {
    const store = useKsqlStore.getState();
    store.start(KEY, "req-1");
    store.start(KEY, "req-2");

    store.setColumns(KEY, "req-1", [{ name: "STALE", kind: "STRING" }]);

    expect(state().columns).toEqual([]);
  });

  it("ignores a completion belonging to a superseded run", () => {
    const store = useKsqlStore.getState();
    store.start(KEY, "req-1");
    store.start(KEY, "req-2");

    store.finish(KEY, "req-1", "done");

    expect(state().status).toBe("running");
    expect(state().requestId).toBe("req-2");
  });

  it("distinguishes a stopped run from a finished one", () => {
    const store = useKsqlStore.getState();
    store.start(KEY, "req-1");
    store.finish(KEY, "req-1", "stopped");
    expect(state().status).toBe("stopped");

    store.start(KEY, "req-2");
    store.finish(KEY, "req-2", "done");
    expect(state().status).toBe("done");
  });

  it("releases the request id when a run ends, so Stop has nothing to cancel", () => {
    const store = useKsqlStore.getState();
    store.start(KEY, "req-1");

    store.finish(KEY, "req-1", "done");

    expect(state().requestId).toBeNull();
  });

  it("records a failure and ends the run", () => {
    const store = useKsqlStore.getState();
    store.start(KEY, "req-1");

    store.fail(KEY, "ksqlDB rejected the query");

    expect(state().status).toBe("error");
    expect(state().error).toBe("ksqlDB rejected the query");
    expect(state().requestId).toBeNull();
  });

  it("keeps each tab and topic's workspace separate", () => {
    const store = useKsqlStore.getState();
    const other = ksqlWorkspaceKey("tab-1", "conn-1", "orders");

    store.setSql(KEY, "cluster query");
    store.setSql(other, "topic query");

    expect(state(KEY).sql).toBe("cluster query");
    expect(state(other).sql).toBe("topic query");
  });

  it("forgets only the disconnected connection's workspaces", () => {
    const store = useKsqlStore.getState();
    const gone = ksqlWorkspaceKey("tab-1", "gone", "cluster");
    const kept = ksqlWorkspaceKey("tab-1", "kept", "cluster");
    store.setSql(gone, "a");
    store.setSql(kept, "b");

    store.clearForConnection("gone");

    expect(useKsqlStore.getState().byKey[gone]).toBeUndefined();
    expect(useKsqlStore.getState().byKey[kept]?.sql).toBe("b");
  });
});
