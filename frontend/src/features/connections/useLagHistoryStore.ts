import { create } from "zustand";
import { ConsumerGroupLag } from "../../lib/tauri";

/** One point on the lag-over-time chart: the group's total lag when Refresh last answered. */
export interface LagSample {
  at: number;
  totalLag: number;
}

export const MAX_LAG_SAMPLES = 200;

export function lagSampleFrom(lag: ConsumerGroupLag, at: number): LagSample {
  return { at, totalLag: lag.partitions.reduce((sum, p) => sum + (p.lag ?? 0), 0) };
}

const keyOf = (connectionId: string, groupId: string) => `${connectionId}::${groupId}`;

interface LagHistoryState {
  byGroup: Record<string, LagSample[]>;
  record: (connectionId: string, groupId: string, lag: ConsumerGroupLag, at?: number) => void;
  /** Dropped on disconnect with the rest of the cluster's contents — see `clearConnectionState`. */
  clearForConnection: (connectionId: string) => void;
}

/**
 * Kafka answers "what is the lag now", never "what was it", so the time-based
 * chart is built from the samples this app took itself. They live in memory
 * only: a restart starts a fresh series, which is honest — the app wasn't
 * watching in between.
 */
export const useLagHistoryStore = create<LagHistoryState>((set) => ({
  byGroup: {},
  record: (connectionId, groupId, lag, at = Date.now()) =>
    set((state) => {
      const key = keyOf(connectionId, groupId);
      const next = [...(state.byGroup[key] ?? []), lagSampleFrom(lag, at)].slice(-MAX_LAG_SAMPLES);
      return { byGroup: { ...state.byGroup, [key]: next } };
    }),
  clearForConnection: (connectionId) =>
    set((state) => ({
      byGroup: Object.fromEntries(
        Object.entries(state.byGroup).filter(([key]) => !key.startsWith(`${connectionId}::`)),
      ),
    })),
}));

export function lagHistoryKey(connectionId: string, groupId: string): string {
  return keyOf(connectionId, groupId);
}
