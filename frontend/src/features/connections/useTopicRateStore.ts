import { create } from "zustand";
import {
  EMPTY_RATE_HISTORY,
  pruneOlderThan,
  RETENTION_MS,
  RateSample,
  recordSample,
  TopicRateHistory,
} from "./topicRates";

export const topicRateKey = (connectionId: string, topic: string) => `${connectionId}::${topic}`;

interface TopicRateState {
  byTopic: Record<string, TopicRateHistory>;
  record: (connectionId: string, topic: string, sample: RateSample) => void;
  /** Dropped on disconnect with the rest of the cluster's contents — see `clearConnectionState`. */
  clearForConnection: (connectionId: string) => void;
}

/**
 * Produce and consume rates over time, built from the samples this app takes
 * itself (Kafka has no history to ask for). In memory only, like the
 * lag-over-time series: a restart starts empty, which is honest — the app
 * wasn't watching in between.
 */
export const useTopicRateStore = create<TopicRateState>((set) => ({
  byTopic: {},
  record: (connectionId, topic, sample) =>
    set((state) => {
      const key = topicRateKey(connectionId, topic);
      const current = state.byTopic[key] ?? EMPTY_RATE_HISTORY;
      const next = pruneOlderThan(recordSample(current, sample), sample.at - RETENTION_MS);
      if (next === current) return state;
      return { byTopic: { ...state.byTopic, [key]: next } };
    }),
  clearForConnection: (connectionId) =>
    set((state) => ({
      byTopic: Object.fromEntries(
        Object.entries(state.byTopic).filter(([key]) => !key.startsWith(`${connectionId}::`)),
      ),
    })),
}));
