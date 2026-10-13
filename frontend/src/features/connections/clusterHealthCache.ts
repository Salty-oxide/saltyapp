import type { ClusterHealthLoad } from "./clusterHealthLoader";

/**
 * How long an analysed cluster is reused. Reading every topic's partitions and
 * config is hundreds of broker round trips on a big cluster, and a tab
 * switched away from and back to should not pay for them twice; a minute is
 * short enough that the figures are still the cluster's, and Refresh always
 * reads again.
 */
export const CLUSTER_HEALTH_MAX_AGE_MS = 60_000;

interface Remembered {
  load: ClusterHealthLoad;
  at: number;
}

const remembered = new Map<string, Remembered>();

export function cachedClusterHealth(connectionId: string, now: number = Date.now()): Remembered | null {
  const entry = remembered.get(connectionId);
  if (!entry) return null;
  if (now - entry.at > CLUSTER_HEALTH_MAX_AGE_MS) {
    remembered.delete(connectionId);
    return null;
  }
  return entry;
}

export function rememberClusterHealth(connectionId: string, load: ClusterHealthLoad, now: number = Date.now()): void {
  remembered.set(connectionId, { load, at: now });
}

/** Dropped with the rest of a cluster's contents when its session ends — see `clearConnectionState`. */
export function forgetClusterHealth(connectionId: string): void {
  remembered.delete(connectionId);
}
