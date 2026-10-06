import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, ConnectionStatus, ImportSummary, NewConnection } from "../../lib/tauri";
import { useWorkspaceSelectionStore } from "../workspace/useWorkspaceSelectionStore";
import { useMessageViewerStore } from "../workspace/useMessageViewerStore";
import { clearConnectionState } from "./clearConnectionState";

export function useConnectionsQuery() {
  return useQuery({ queryKey: ["connections"], queryFn: api.listConnections });
}

export function useCreateConnection() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (newConnection: NewConnection) => api.createConnection(newConnection),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["connections"] }),
  });
}

export function useUpdateConnection() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ id, connection }: { id: string; connection: NewConnection }) => api.updateConnection(id, connection),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["connections"] }),
  });
}

export function useDeleteConnection() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.deleteConnection(id),
    onSuccess: (_data, id) => {
      queryClient.invalidateQueries({ queryKey: ["connections"] });
      // A deleted connection's id can still be sitting in the middle pane's
      // selection or the right pane's viewed message (in this tab or any
      // other) — without this, those panes would keep referencing a
      // connection that no longer exists. Deletion clears at least as much
      // as a disconnect does, so it shares the same routine rather than
      // keeping its own shorter list that has to be remembered separately.
      clearConnectionState(queryClient, id);
    },
  });
}

/** Backs the "Export Connection" context-menu item (`ids: [id]`) and the "Export All" button (`ids: null`). */
export function useExportConnections() {
  return useMutation<void, Error, { ids: string[] | null; path: string }>({
    mutationFn: ({ ids, path }) => api.exportConnections(ids, path),
  });
}

/** Backs the sidebar's "Import" button. */
export function useImportConnections() {
  const queryClient = useQueryClient();
  return useMutation<ImportSummary, Error, string>({
    mutationFn: (path: string) => api.importConnections(path),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["connections"] }),
  });
}

/**
 * How often a *connected* cluster's reachability dot is refreshed. This is
 * the dot doing its job — the user is working against this cluster and wants
 * to know promptly if it goes away.
 */
export const CONNECTED_STATUS_POLL_MS = 10_000;

/**
 * How often an idle (not connected) cluster's dot is refreshed.
 *
 * Every saved connection polls, connected or not, for as long as the app is
 * open. At the connected cadence that is ~8,600 TCP connect/teardowns per
 * saved cluster per day against production broker ports — and a user with
 * twenty saved clusters is generating that against all twenty while actively
 * using none of them. Connection-rate metrics and firewall/load-balancer
 * connection logs both notice, even though nothing is authenticated.
 *
 * For a cluster the user is not connected to, the dot only answers "is
 * something listening", which does not change minute to minute. Backing off
 * costs nothing that matters and cuts the idle footprint six-fold.
 *
 * The *first* check still runs immediately on mount — React Query fetches on
 * mount regardless of interval — so the dot is correct as soon as the tree
 * renders. Only the refresh cadence changes.
 */
export const IDLE_STATUS_POLL_MS = 60_000;

export function statusPollInterval(isConnected: boolean): number {
  return isConnected ? CONNECTED_STATUS_POLL_MS : IDLE_STATUS_POLL_MS;
}

/**
 * The reachability poll behind a connected cluster's status dot.
 *
 * **Only runs while there is a session.** It used to run for every saved
 * connection for as long as the app was open, which meant a TCP
 * connect/teardown against production broker ports every minute, per saved
 * cluster, forever — and once the dot stopped colouring itself from
 * reachability while disconnected (a cluster you are not using being
 * unreachable is not a fault), the only thing that traffic still fed was the
 * wording of a tooltip. Nothing else consumed it: the auto-disconnect below
 * only acts on a live session, and the tree's contents are gated on being
 * connected.
 *
 * So a disconnected cluster is now genuinely silent — no sockets at rest, and
 * none at startup either, since nothing is connected when the app opens.
 * Reachability is answered on demand instead, by the things that already do
 * it: Reconnect, and the New Connection modal's Test and Ping buttons.
 */
export function useConnectionStatus(id: string, isConnected: boolean) {
  return useQuery({
    queryKey: ["connection-status", id],
    queryFn: () => api.checkConnectionStatus(id),
    enabled: isConnected,
    refetchInterval: statusPollInterval(isConnected),
    initialData: "UNKNOWN" as const,
  });
}

/** Backs the ping button next to "Bootstrap servers" in the New Connection modal. */
export function usePingBootstrapServers() {
  return useMutation<ConnectionStatus, Error, string>({
    mutationFn: (bootstrapServers: string) => api.pingBootstrapServers(bootstrapServers),
  });
}

/** Backs the ping button next to "Host" in the New Connection modal's Zookeeper section. */
export function usePingZookeeper() {
  return useMutation<ConnectionStatus, Error, { host: string; port: number }>({
    mutationFn: ({ host, port }) => api.pingZookeeper(host, port),
  });
}

/** Backs the New Connection modal's bottom "Test" button. */
export function useTestConnection() {
  return useMutation<ConnectionStatus, Error, NewConnection>({
    mutationFn: (newConnection: NewConnection) => api.testConnection(newConnection),
  });
}

/**
 * Why this connection's requests are being refused before they reach the
 * broker — the reason the broker gave when it rejected its credentials — or
 * `null` while it isn't blocked.
 *
 * Polled alongside the reachability dot, since the breaker can trip at any
 * time (a rotated password takes effect mid-session, not at connect time).
 */
export function useConnectionAuthBlock(id: string) {
  return useQuery({
    queryKey: ["connection-auth-block", id],
    queryFn: () => api.connectionAuthBlockReason(id),
    refetchInterval: CONNECTED_STATUS_POLL_MS,
    // Reads in-process state in the backend and never touches the network, so
    // the app-wide `shouldRetry` had nothing to ride out here — a failure
    // means the IPC call itself failed, which retrying twice with backoff
    // does not fix.
    retry: false,
    initialData: null,
  });
}

/** Whether the cluster detail panel should treat this connection as connected (gates field-disabling and the tree's Brokers/Topics/Consumers expansion). */
export function useConnectionConnected(id: string) {
  return useQuery({
    queryKey: ["connection-connected", id],
    queryFn: () => api.isConnectionConnected(id),
    // Polled, not just invalidated by whoever changed it. The backend drops a
    // connection on its own when the auth circuit breaker trips
    // (`record_auth_failure` marks it disconnected after two rejections), and
    // nothing in the frontend invalidates this for that. Until it was polled,
    // a cluster the backend had already given up on kept its tree expanded
    // and its panes live until something else happened to refetch.
    //
    // Reads an in-process boolean over IPC — no network, no broker — so this
    // is far cheaper than the reachability dot it rides alongside.
    refetchInterval: CONNECTED_STATUS_POLL_MS,
    initialData: false,
  });
}

/**
 * Backs the cluster detail panel's Connect/Reconnect button (one button,
 * named for whether there is already a session). Takes `id` as a hook
 * argument (rather than at `.mutate()` time) so the mutation key is scoped
 * to this connection — `useIsMutating({ mutationKey: connectMutationKey(id) })`
 * elsewhere (the connection tree's spinner) can then observe it in flight
 * without needing its own reference to this mutation instance.
 */
export function connectMutationKey(id: string) {
  return ["connect", id];
}

export function useConnect(id: string) {
  const queryClient = useQueryClient();
  return useMutation<ConnectionStatus, Error, void>({
    mutationKey: connectMutationKey(id),
    mutationFn: () => api.connectConnection(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["connection-connected", id] });
      queryClient.invalidateQueries({ queryKey: ["connection-status", id] });
      // The cluster listings are fetched once and then held for as long as
      // the app runs (see `CLUSTER_LISTING_OPTIONS`), which is right while a
      // connection stays up and wrong the moment it is re-established: the
      // cluster may have gained or lost topics while it was down, and
      // nothing else would ever ask again. Connecting is the one event that
      // means "whatever we knew about this cluster is from a previous
      // session".
      for (const key of ["brokers", "topics", "consumer-groups"]) {
        queryClient.invalidateQueries({ queryKey: [key, id] });
      }
    },
    // On settled, not on success: a Reconnect that the broker *rejects* is
    // exactly when the breaker's state changes, and the tree needs to say so
    // straight away rather than at the next poll.
    onSettled: () => {
      queryClient.invalidateQueries({ queryKey: ["connection-auth-block", id] });
    },
  });
}

/**
 * Backs the cluster detail panel's and the tree's "Disconnect".
 *
 * The state cleanup is deliberately NOT done here. Disconnecting has four
 * routes — this button, the 120-minute idle timer, the auth circuit breaker
 * tripping in the backend, and the reachability poll finding the cluster gone
 * — and only this one goes through this mutation. All four end with the
 * backend reporting the connection as no longer connected, so the cleanup
 * hangs off *that* transition instead (see `useClusterDisconnectCleanup`) and
 * happens exactly once however the connection dropped.
 */
export function useDisconnect() {
  const queryClient = useQueryClient();
  return useMutation<void, Error, string>({
    mutationFn: (id: string) => api.disconnectConnection(id),
    onSuccess: (_void, id) => {
      queryClient.invalidateQueries({ queryKey: ["connection-connected", id] });
      queryClient.invalidateQueries({ queryKey: ["connection-status", id] });
    },
  });
}
