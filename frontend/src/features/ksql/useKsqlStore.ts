import { create } from "zustand";
import { KsqlColumn, KsqlRow } from "../../lib/tauri";
import { appendRows, KsqlGridRow } from "./ksqlRows";

export type KsqlStatus = "idle" | "running" | "stopped" | "done" | "error";

export interface KsqlWorkspaceState {
  /** What is in the editor. */
  sql: string;
  status: KsqlStatus;
  /** Columns from the header frame, empty until one arrives. */
  columns: KsqlColumn[];
  rows: KsqlGridRow[];
  /** Every row the query produced, which exceeds `rows.length` once the cap starts dropping. */
  received: number;
  /** Arrival counter, so a row's identity survives eviction of older ones. */
  nextIndex: number;
  /** The in-flight query's id, and the thing Stop cancels. `null` when nothing is running. */
  requestId: string | null;
  error: string | null;
}

export const EMPTY_KSQL_WORKSPACE: KsqlWorkspaceState = {
  sql: "",
  status: "idle",
  columns: [],
  rows: [],
  received: 0,
  nextIndex: 0,
  requestId: null,
  error: null,
};

interface KsqlStore {
  /**
   * One workspace per key.
   *
   * Keyed rather than held in the component because the middle pane is
   * rendered `key={activeTabId}` in App.tsx, so every top-level tab switch
   * unmounts it — a query's results, and the text of the query itself, would
   * otherwise be thrown away on the way out. The same reasoning as
   * `useDataTabGridStateStore`.
   */
  byKey: Record<string, KsqlWorkspaceState>;
  setSql: (key: string, sql: string) => void;
  /** Marks a run as started, clearing the previous result. */
  start: (key: string, requestId: string) => void;
  /** Columns from the header frame. Ignored when it belongs to a superseded run. */
  setColumns: (key: string, requestId: string, columns: KsqlColumn[]) => void;
  /** Appends a batch. Ignored when it belongs to a superseded run. */
  addRows: (key: string, requestId: string, rows: KsqlRow[]) => void;
  finish: (key: string, requestId: string, status: Extract<KsqlStatus, "stopped" | "done">) => void;
  fail: (key: string, message: string) => void;
  /** Forgets every workspace belonging to a connection — see `clearConnectionState`. */
  clearForConnection: (connectionId: string) => void;
}

/** Workspace keys are `<tabId>:<connectionId>:<scope>`; this matches the connection segment. */
export function ksqlKeyBelongsTo(key: string, connectionId: string): boolean {
  return key.split(":")[1] === connectionId;
}

/** The key a workspace is stored under. `scope` is `cluster` or a topic name. */
export function ksqlWorkspaceKey(tabId: string | null, connectionId: string, scope: string): string {
  return `${tabId ?? "no-tab"}:${connectionId}:${scope}`;
}

export const useKsqlStore = create<KsqlStore>((set) => {
  /**
   * Applies a change only when it belongs to the run currently in flight.
   *
   * Rows and headers arrive as events, which are not ordered against the
   * command that started them: a batch from a query the user has already
   * replaced can land after the new one began. Without this guard those rows
   * would appear under the new query's columns, which is the same class of
   * bug `requestId` already solves for the message stream.
   */
  function forRun(
    key: string,
    requestId: string,
    change: (state: KsqlWorkspaceState) => KsqlWorkspaceState,
  ) {
    set((store) => {
      const current = store.byKey[key];
      if (!current || current.requestId !== requestId) return store;
      return { byKey: { ...store.byKey, [key]: change(current) } };
    });
  }

  function patch(key: string, change: (state: KsqlWorkspaceState) => KsqlWorkspaceState) {
    set((store) => ({
      byKey: { ...store.byKey, [key]: change(store.byKey[key] ?? EMPTY_KSQL_WORKSPACE) },
    }));
  }

  return {
    byKey: {},

    setSql: (key, sql) => patch(key, (state) => ({ ...state, sql })),

    start: (key, requestId) =>
      patch(key, (state) => ({
        ...state,
        // The previous result goes now rather than when the first row lands:
        // leaving it on screen under a new query's header reads as though the
        // new query returned it.
        status: "running",
        columns: [],
        rows: [],
        received: 0,
        nextIndex: 0,
        requestId,
        error: null,
      })),

    setColumns: (key, requestId, columns) => forRun(key, requestId, (state) => ({ ...state, columns })),

    addRows: (key, requestId, rows) =>
      forRun(key, requestId, (state) => {
        const appended = appendRows(state.rows, rows, state.nextIndex);
        return {
          ...state,
          rows: appended.rows,
          nextIndex: appended.nextIndex,
          received: state.received + rows.length,
        };
      }),

    finish: (key, requestId, status) =>
      forRun(key, requestId, (state) => ({ ...state, status, requestId: null })),

    fail: (key, message) =>
      patch(key, (state) => ({
        ...state,
        status: "error",
        requestId: null,
        error: message,
      })),

    clearForConnection: (connectionId) =>
      set((store) => ({
        byKey: Object.fromEntries(
          Object.entries(store.byKey).filter(([key]) => !ksqlKeyBelongsTo(key, connectionId)),
        ),
      })),
  };
});
