import { create } from "zustand";
import { EMPTY_TOPIC_SELECTION, TopicSelection } from "./topicMultiSelect";

interface TopicMultiSelectState {
  /** Keyed by the Topics list's tree key, so each tab and connection keeps its own set. */
  byKey: Record<string, TopicSelection>;
  set: (key: string, selection: TopicSelection) => void;
  clear: (key: string) => void;
  /** Drops every list's selection for one connection, in every tab — see `clearConnectionState`. */
  clearForConnection: (connectionId: string) => void;
}

export const useTopicMultiSelectStore = create<TopicMultiSelectState>((set) => ({
  byKey: {},
  set: (key, selection) => set((state) => ({ byKey: { ...state.byKey, [key]: selection } })),
  clear: (key) =>
    set((state) => {
      if (!(key in state.byKey)) return state;
      const { [key]: _removed, ...rest } = state.byKey;
      return { byKey: rest };
    }),
  // Keys are `treeKey(tabId, connectionId, "Topics" | "Consumers")`, so the
  // connection is the second segment.
  clearForConnection: (connectionId) =>
    set((state) => ({
      byKey: Object.fromEntries(
        Object.entries(state.byKey).filter(([key]) => key.split(":")[1] !== connectionId),
      ),
    })),
}));

export { EMPTY_TOPIC_SELECTION };
