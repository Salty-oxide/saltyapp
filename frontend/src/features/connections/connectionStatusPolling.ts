import type { QueryClient } from "@tanstack/react-query";
import type { ConnectionStatus } from "../../lib/tauri";
import { CLUSTER_DATA_QUERY_ROOTS } from "./clearConnectionState";

/**
 * Steady cadence of the reachability check for a connected cluster.
 *
 * Cheap to run this often because, for a connected cluster, the backend
 * answers `connection_check_status` from the pooled client's own view of its
 * broker connections (librdkafka's statistics) — an IPC call and no traffic to
 * the cluster. Only a connection with no pooled client falls back to a TCP
 * connect against the bootstrap port. A connected cluster normally has one, but
 * not always: right after an edit it is rebuilt lazily on the next request,
 * and the backend closes one nobody has asked for in five minutes.
 * `hasRecentClusterSuccess` additionally skips the call when real requests
 * have just proven the cluster is there.
 */
export const REACHABILITY_POLL_MS = 10_000;

/**
 * How soon to look again after a probe found the cluster unreachable.
 * `useUnreachableDisconnect` ends the session on the second consecutive
 * failure, so a dead cluster is detected after roughly
 * `REACHABILITY_POLL_MS + UNREACHABLE_RECHECK_MS`, not two steady intervals.
 */
export const UNREACHABLE_RECHECK_MS = 5_000;

export function reachabilityPollInterval(lastStatus: ConnectionStatus | undefined): number {
  return lastStatus === "UNREACHABLE" ? UNREACHABLE_RECHECK_MS : REACHABILITY_POLL_MS;
}

/** Whether `queryKey` is cluster data fetched from the broker for `connectionId`. */
export function isClusterDataKey(queryKey: readonly unknown[], connectionId: string): boolean {
  const [root, id] = queryKey;
  return typeof root === "string" && (CLUSTER_DATA_QUERY_ROOTS as readonly string[]).includes(root) && id === connectionId;
}

/**
 * When the broker last *answered* for a cluster-data query.
 *
 * Most of this data is served by the backend's metadata cache, where a
 * successful query can be a read from memory: its `dataUpdatedAt` says the
 * query ran, not that the cluster is there. Such a value carries
 * `fetchedAtMs`, the moment the broker was actually asked, and that is the
 * proof. Anything without it (a live read) is as fresh as the query.
 */
function brokerAnsweredAt(data: unknown, dataUpdatedAt: number): number {
  if (typeof data === "object" && data !== null && "fetchedAtMs" in data) {
    const fetchedAtMs = (data as { fetchedAtMs: unknown }).fetchedAtMs;
    if (typeof fetchedAtMs === "number") return fetchedAtMs;
  }
  return dataUpdatedAt;
}

/**
 * True when a real request to this cluster has succeeded within `withinMs`
 * and no request to it has failed since — proof of reachability that makes
 * a separate TCP probe redundant.
 *
 * Only the cluster-data roots count (the same allow-list that is dropped on
 * disconnect): `connection-connected` and `connection-auth-block` are
 * in-process reads, and `connection-status` is the probe itself.
 */
export function hasRecentClusterSuccess(
  queryClient: QueryClient,
  connectionId: string,
  withinMs: number,
  now: number = Date.now(),
): boolean {
  let lastSuccess = 0;
  let lastError = 0;
  for (const query of queryClient.getQueryCache().getAll()) {
    if (!isClusterDataKey(query.queryKey, connectionId)) continue;
    const succeededAt = brokerAnsweredAt(query.state.data, query.state.dataUpdatedAt);
    if (succeededAt > lastSuccess) lastSuccess = succeededAt;
    if (query.state.errorUpdatedAt > lastError) lastError = query.state.errorUpdatedAt;
  }
  return lastSuccess > lastError && now - lastSuccess <= withinMs;
}
