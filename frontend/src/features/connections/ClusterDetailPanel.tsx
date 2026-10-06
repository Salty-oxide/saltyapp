import { useEffect, useRef, useState } from "react";
import { CONNECTION_TABS, ConnectionTabId, ConnectionTabsView } from "./modal/ConnectionTabsView";
import { KsqlWorkspace } from "../ksql/KsqlWorkspace";
import { ConnectionDraft, connectionToDraft, draftsEqual, toNewConnection } from "./modal/draft";
import { PingResult } from "./modal/PingResult";
import {
  useConnect,
  useConnectionConnected,
  useConnectionsQuery,
  useDisconnect,
  useUpdateConnection,
} from "./useConnections";

export interface ClusterDetailPanelProps {
  connectionId: string;
}

/**
 * The cluster panel's tabs: the connection's own, plus the ksqlDB workspace.
 *
 * Query is last because it is the only one that is not configuration — the
 * others describe how to reach the cluster, this one does something with it.
 */
const CLUSTER_TABS = [...CONNECTION_TABS, { id: "query" as const, label: "Query" }];

export function ClusterDetailPanel({ connectionId }: ClusterDetailPanelProps) {
  const { data: connections } = useConnectionsQuery();
  const connection = connections?.find((c) => c.id === connectionId);
  const { data: isConnected } = useConnectionConnected(connectionId);

  const [activeTab, setActiveTab] = useState<ConnectionTabId>("properties");
  const [draft, setDraft] = useState<ConnectionDraft | null>(null);
  const [originalDraft, setOriginalDraft] = useState<ConnectionDraft | null>(null);
  const [error, setError] = useState<string | null>(null);
  const initializedForRef = useRef<string | null>(null);

  const connect = useConnect(connectionId);
  const disconnect = useDisconnect();
  const updateConnection = useUpdateConnection();

  useEffect(() => {
    if (!connection) return;
    // Only (re-)load the draft once per connectionId, the first time its
    // data becomes available. Later `connections` refetches (e.g. after a
    // successful Update, which invalidates the list) must not clobber
    // whatever the user is currently editing.
    if (initializedForRef.current === connectionId) return;
    const loaded = connectionToDraft(connection);
    setDraft(loaded);
    setOriginalDraft(loaded);
    initializedForRef.current = connectionId;
  }, [connectionId, connection]);

  function updateDraft(patch: Partial<ConnectionDraft>) {
    setDraft((prev) => (prev ? { ...prev, ...patch } : prev));
  }

  async function handleConnect() {
    setError(null);
    try {
      await connect.mutateAsync();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to connect");
    }
  }

  async function handleDisconnect() {
    setError(null);
    try {
      await disconnect.mutateAsync(connectionId);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to disconnect");
    }
  }

  async function handleUpdate() {
    if (!draft) return;
    setError(null);
    try {
      await updateConnection.mutateAsync({ id: connectionId, connection: toNewConnection(draft) });
      setOriginalDraft(draft);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to update connection");
    }
  }

  if (!draft || !originalDraft) {
    return <p>Loading cluster…</p>;
  }

  const isDirty = !draftsEqual(draft, originalDraft);
  const connected = isConnected ?? false;

  return (
    <div className="cluster-detail-panel">
      <header className="cluster-detail-header">
        <h2>{connection?.name ?? draft.name}</h2>
      </header>

      <ConnectionTabsView
        // The connection's own tabs plus a Query workspace. Appended here
        // rather than added to `CONNECTION_TABS` because the New Connection
        // modal shares that list, and a half-written connection has nothing to
        // query.
        tabs={CLUSTER_TABS}
        activeTab={activeTab}
        onTabChange={setActiveTab}
        draft={draft}
        onChange={updateDraft}
        disabled={connected}
      >
        {activeTab === "query" &&
          (connection ? (
            <KsqlWorkspace connectionId={connection.id} scope="cluster" />
          ) : (
            <p className="ksql-notice" role="status">
              Save this connection before running a query against it.
            </p>
          ))}
      </ConnectionTabsView>

      {error && (
        <p role="alert" className="connection-modal-error">
          {error}
        </p>
      )}
      <PingResult
        mutation={connect}
        successMessage="Connected"
        failureMessage="Unable to reach the cluster"
      />

      <footer className="cluster-detail-footer">
        {/*
          One button, two names: "Reconnect" only makes sense when there is
          a live session to replace. On a cluster that is not connected this
          is simply Connect, and labelling it "Reconnect" made the action
          that gets you online look like a recovery step.
        */}
        <button type="button" onClick={handleConnect} disabled={connect.isPending}>
          {connected ? "Reconnect" : "Connect"}
        </button>
        <button type="button" onClick={handleDisconnect} disabled={disconnect.isPending}>
          Disconnect
        </button>
        <button type="button" onClick={handleUpdate} disabled={!isDirty || updateConnection.isPending}>
          Update
        </button>
      </footer>
    </div>
  );
}
