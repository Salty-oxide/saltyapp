import { useCallback, useEffect, useRef } from "react";
import { listen } from "@tauri-apps/api/event";
import { api, KsqlHeaderEvent, KsqlRowsEvent } from "../../lib/tauri";
import { KsqlResultsGrid } from "../connections/gridTabs";
import { useTabsStore } from "../tabs/useTabsStore";
import { resultSummary } from "./ksqlRows";
import { EMPTY_KSQL_WORKSPACE, ksqlWorkspaceKey, useKsqlStore } from "./useKsqlStore";

export interface KsqlWorkspaceProps {
  connectionId: string;
  /** `cluster`, or a topic name — what this workspace is scoped to. */
  scope: string;
  /** Prefilled into the editor the first time this workspace is opened. */
  initialSql?: string;
  /** Rendered above the editor: the topic tab's "no stream registered" notice, say. */
  notice?: React.ReactNode;
}

/** Generates a request id. `crypto.randomUUID` is available in the webview and in jsdom. */
function newRequestId(): string {
  return crypto.randomUUID();
}

/**
 * The ksqlDB editor, its Run/Stop pair, and the grid its results land in.
 *
 * Shared by the cluster panel's Query tab and every topic's, which differ only
 * in what they prefill and what they say when there is nothing to query.
 *
 * Rows arrive as events rather than as the command's return value, because a
 * push query does not return until it is stopped — the same reason the Data
 * tab streams its fetches.
 */
export function KsqlWorkspace({ connectionId, scope, initialSql, notice }: KsqlWorkspaceProps) {
  const activeTabId = useTabsStore((s) => s.activeTabId);
  const key = ksqlWorkspaceKey(activeTabId, connectionId, scope);

  const workspace = useKsqlStore((s) => s.byKey[key]) ?? EMPTY_KSQL_WORKSPACE;
  const setSql = useKsqlStore((s) => s.setSql);
  const start = useKsqlStore((s) => s.start);
  const setColumns = useKsqlStore((s) => s.setColumns);
  const addRows = useKsqlStore((s) => s.addRows);
  const finish = useKsqlStore((s) => s.finish);
  const fail = useKsqlStore((s) => s.fail);

  // Prefilled once. Re-applying it on every render would overwrite whatever
  // the user had started typing.
  const prefilledFor = useRef<string | null>(null);
  useEffect(() => {
    if (prefilledFor.current === key) return;
    prefilledFor.current = key;
    if (initialSql && workspace.sql.trim().length === 0) setSql(key, initialSql);
  }, [key, initialSql, workspace.sql, setSql]);

  // Subscribed once and left in place: the listeners are keyed by the event's
  // own `requestId`, and the store discards anything from a superseded run, so
  // there is nothing to re-subscribe when the query changes.
  const storeRef = useRef({ key, setColumns, addRows });
  storeRef.current = { key, setColumns, addRows };
  useEffect(() => {
    const unlisteners = [
      listen<KsqlHeaderEvent>("ksql-header", (event) => {
        const { key, setColumns } = storeRef.current;
        setColumns(key, event.payload.requestId, event.payload.columns);
      }),
      listen<KsqlRowsEvent>("ksql-rows", (event) => {
        const { key, addRows } = storeRef.current;
        addRows(key, event.payload.requestId, event.payload.rows);
      }),
    ];
    return () => {
      unlisteners.forEach((pending) => void pending.then((off) => off()).catch(() => {}));
    };
  }, []);

  const isRunning = workspace.status === "running";

  const handleRun = useCallback(async () => {
    const sql = workspace.sql.trim();
    if (sql.length === 0) return;
    const requestId = newRequestId();
    start(key, requestId);
    try {
      const outcome = await api.ksqlQuery(connectionId, sql, requestId);
      finish(key, requestId, outcome.cancelled ? "stopped" : "done");
    } catch (error) {
      fail(key, error instanceof Error ? error.message : String(error));
    }
  }, [workspace.sql, key, connectionId, start, finish, fail]);

  const handleStop = useCallback(() => {
    if (workspace.requestId) void api.ksqlCancel(workspace.requestId);
  }, [workspace.requestId]);

  return (
    <div className="ksql-workspace">
      {notice}

      <label className="ksql-editor-label">
        ksqlDB query
        {/*
          A plain textarea. Syntax highlighting is the obvious ask and the
          wrong trade: AG Grid already costs two thirds of this bundle, the
          `gridTabs` split exists because parse-and-compile was measured at
          111 ms per launch, and Monaco is larger than AG Grid.
        */}
        <textarea
          className="ksql-editor"
          aria-label="ksqlDB query"
          spellCheck={false}
          rows={4}
          value={workspace.sql}
          onChange={(event) => setSql(key, event.target.value)}
          placeholder="SELECT * FROM MY_STREAM EMIT CHANGES;"
        />
      </label>

      <div className="ksql-controls">
        <button
          type="button"
          aria-label="Run"
          onClick={() => void handleRun()}
          disabled={isRunning || workspace.sql.trim().length === 0}
        >
          ▶ Run
        </button>
        <button type="button" aria-label="Stop" onClick={handleStop} disabled={!isRunning}>
          ■ Stop
        </button>
        <span className="ksql-summary" role="status">
          {resultSummary(workspace.rows.length, workspace.received, workspace.status)}
        </span>
      </div>

      {workspace.error && (
        <p role="alert" className="connection-modal-error">
          {workspace.error}
        </p>
      )}

      {/* Imported from `gridTabs`, never from its own module — see that file. */}
      <KsqlResultsGrid columns={workspace.columns} rows={workspace.rows} />

      {workspace.status === "running" && workspace.columns.length === 0 && (
        <p className="ksql-waiting">Waiting for the first rows…</p>
      )}
    </div>
  );
}
