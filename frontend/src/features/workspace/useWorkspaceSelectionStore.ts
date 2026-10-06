import { create } from "zustand";

/**
 * What's currently selected in the connection tree and shown in the middle
 * pane. A discriminated union so each selection kind's own identifying
 * fields stay distinct.
 */
export type WorkspaceSelection =
  | { type: "connection"; id: string; name: string }
  | { type: "broker"; connectionId: string; brokerId: number }
  | { type: "topic"; connectionId: string; topicName: string }
  | { type: "partition"; connectionId: string; topicName: string; partitionId: number }
  | { type: "consumerGroup"; connectionId: string; groupId: string }
  | { type: "principal"; connectionId: string; principal: string }
  | null;

interface WorkspaceSelectionState {
  /** The active tab's selection — kept in sync with `byTab[activeTabId]` by `setActiveTab`. */
  selection: WorkspaceSelection;
  activeTabId: string | null;
  /** Per-tab selection cache, so each tab keeps its own independent workspace state. */
  byTab: Record<string, WorkspaceSelection>;
  /** Called whenever the active tab changes, so writes below land in the right tab's slot. */
  setActiveTab: (tabId: string | null) => void;
  selectConnection: (id: string, name: string) => void;
  selectBroker: (connectionId: string, brokerId: number) => void;
  selectTopic: (connectionId: string, topicName: string) => void;
  selectPartition: (connectionId: string, topicName: string, partitionId: number) => void;
  selectConsumerGroup: (connectionId: string, groupId: string) => void;
  selectPrincipal: (connectionId: string, principal: string) => void;
  clearSelection: () => void;
  /** Resets a tab's cached selection back to blank — the Bottom panel's "Clear memory" button. Defaults to the active tab. */
  clearTabMemory: (tabId?: string) => void;
  /** Clears this connection's selection from every tab, not just the active one — called after a connection is deleted, so a stale middle-pane selection can't reference an id that no longer exists. */
  clearForConnection: (connectionId: string) => void;
}

function belongsToConnection(selection: WorkspaceSelection, connectionId: string): boolean {
  if (!selection) return false;
  return selection.type === "connection" ? selection.id === connectionId : selection.connectionId === connectionId;
}

export const useWorkspaceSelectionStore = create<WorkspaceSelectionState>((set, get) => {
  function write(selection: WorkspaceSelection) {
    const tabId = get().activeTabId;
    set((state) => ({
      selection,
      byTab: tabId ? { ...state.byTab, [tabId]: selection } : state.byTab,
    }));
  }

  return {
    selection: null,
    activeTabId: null,
    byTab: {},
    setActiveTab: (tabId) =>
      set((state) => ({ activeTabId: tabId, selection: (tabId ? state.byTab[tabId] : null) ?? null })),
    selectConnection: (id, name) => write({ type: "connection", id, name }),
    selectBroker: (connectionId, brokerId) => write({ type: "broker", connectionId, brokerId }),
    selectTopic: (connectionId, topicName) => write({ type: "topic", connectionId, topicName }),
    selectPartition: (connectionId, topicName, partitionId) =>
      write({ type: "partition", connectionId, topicName, partitionId }),
    selectConsumerGroup: (connectionId, groupId) => write({ type: "consumerGroup", connectionId, groupId }),
    selectPrincipal: (connectionId, principal) => write({ type: "principal", connectionId, principal }),
    clearSelection: () => write(null),
    clearTabMemory: (tabId) => {
      const target = tabId ?? get().activeTabId;
      if (!target) return;
      set((state) => ({
        byTab: { ...state.byTab, [target]: null },
        selection: state.activeTabId === target ? null : state.selection,
      }));
    },
    clearForConnection: (connectionId) => {
      set((state) => {
        const byTab = { ...state.byTab };
        for (const tabId of Object.keys(byTab)) {
          if (belongsToConnection(byTab[tabId], connectionId)) {
            byTab[tabId] = null;
          }
        }
        return {
          byTab,
          selection: belongsToConnection(state.selection, connectionId) ? null : state.selection,
        };
      });
    },
  };
});
