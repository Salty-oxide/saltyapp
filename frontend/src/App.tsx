import { focusManager, QueryClient, QueryClientProvider, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { open, save } from "@tauri-apps/plugin-dialog";
import { onWindowFocusChanged } from "./lib/appWindow";
import { ThemeProvider } from "./features/theme/ThemeProvider";
import { TabBar } from "./features/tabs/TabBar";
import { useTabsStore } from "./features/tabs/useTabsStore";
import { useJsonViewerTabsStore } from "./features/tabs/useJsonViewerTabsStore";
import { JsonViewerTabPanel } from "./features/tabs/JsonViewerTabPanel";
import { ConnectionTree } from "./features/connections/ConnectionTree";
import { ConnectionModal } from "./features/connections/modal/ConnectionModal";
import { ConnectionDraft, connectionToDraft } from "./features/connections/modal/draft";
import { api, Connection } from "./lib/tauri";
import { retryDelay, shouldRetry } from "./lib/queryRetry";
import { ClusterDetailPanel } from "./features/connections/ClusterDetailPanel";
import { BrokerDetailPanel } from "./features/connections/BrokerDetailPanel";
import { TopicDetailPanel } from "./features/connections/TopicDetailPanel";
import { PartitionDetailPanel } from "./features/connections/PartitionDetailPanel";
import { ExportIcon, ImportIcon, PlusIcon } from "./components/AppIcons";
import { MessagePayloadViewer } from "./features/connections/MessagePayloadViewer";
import { ConsumerGroupDetailPanel } from "./features/connections/ConsumerGroupDetailPanel";
import { PrincipalDetailPanel } from "./features/connections/PrincipalDetailPanel";
import { useCreateConnection, useExportConnections, useImportConnections } from "./features/connections/useConnections";
import { BottomPanel } from "./features/bottom-panel/BottomPanel";
import { ResizableShell } from "./features/layout/ResizableShell";
import { useWorkspaceSelectionStore } from "./features/workspace/useWorkspaceSelectionStore";
import { PreferencesProvider } from "./features/settings/PreferencesProvider";
import { IdleTimerProvider } from "./features/idle/IdleTimerProvider";
import { SettingsPanel } from "./features/settings/SettingsPanel";
import { SETTINGS_TAB_ID, useSettingsPanelStore } from "./features/settings/useSettingsPanelStore";
import { useMessageViewerStore } from "./features/workspace/useMessageViewerStore";
import { useMessageViewerPrefsStore } from "./features/workspace/useMessageViewerPrefsStore";
import "./styles/themes.css";
import "./styles/global.css";

// Every query here ends in a Kafka client, a socket and — on a secured
// cluster — a TLS and SASL handshake, so React Query's defaults (three
// retries, immediately, in lockstep across every open app) are broker load
// rather than resilience. `shouldRetry` also refuses to retry rejected
// credentials at all: see `frontend/src/lib/queryRetry.ts`.
/**
 * Exported so tests can reset it between cases. It is deliberately
 * module-level — the app wants one cache for its whole lifetime — which
 * means a test file's cases would otherwise share cached cluster data with
 * each other now that those queries have a stale window.
 */
export const queryClient = new QueryClient({
  defaultOptions: {
    queries: { retry: shouldRetry, retryDelay },
    mutations: { retry: false },
  },
});

function AppShell() {
  const [showModal, setShowModal] = useState(false);
  const [cloneDraft, setCloneDraft] = useState<ConnectionDraft | null>(null);
  const createConnection = useCreateConnection();
  const queryClient = useQueryClient();
  const exportConnections = useExportConnections();
  const importConnections = useImportConnections();

  function handleClone(connection: Connection) {
    setCloneDraft({ ...connectionToDraft(connection), name: `${connection.name} (Copy)` });
    setShowModal(true);
  }

  function closeModal() {
    setShowModal(false);
    setCloneDraft(null);
  }

  async function handleExportAll() {
    const path = await save({
      defaultPath: "salty-connections.json",
      filters: [{ name: "JSON", extensions: ["json"] }],
    });
    if (path) {
      exportConnections.mutate({ ids: null, path });
    }
  }

  async function handleImport() {
    const path = await open({ filters: [{ name: "JSON", extensions: ["json"] }], multiple: false });
    if (path) {
      const { imported, skipped } = await importConnections.mutateAsync(path);
      window.alert(`Imported ${imported} connection${imported === 1 ? "" : "s"}, skipped ${skipped}.`);
    }
  }
  const loadTabs = useTabsStore((s) => s.loadTabs);
  const activeTabId = useTabsStore((s) => s.activeTabId);
  const activeJsonTab = useJsonViewerTabsStore((s) => s.tabs.find((tab) => tab.id === activeTabId));
  const isSettingsActive = activeTabId === SETTINGS_TAB_ID;
  const selection = useWorkspaceSelectionStore((s) => s.selection);
  const setSelectionActiveTab = useWorkspaceSelectionStore((s) => s.setActiveTab);
  const openSettings = useSettingsPanelStore((s) => s.open);
  const hasSelectedMessage = useMessageViewerStore((s) => s.message !== null);
  const setMessageViewerActiveTab = useMessageViewerStore((s) => s.setActiveTab);
  const viewedConnectionId = useMessageViewerStore((s) => s.connectionId);
  const viewedTopic = useMessageViewerStore((s) => s.topic);
  const viewedPartitionId = useMessageViewerStore((s) => s.partitionId);
  const clearViewedMessage = useMessageViewerStore((s) => s.clear);
  // App-wide, not per tab: the payload panel's dock changes the shape of the
  // whole window, so it is a property of the workspace rather than of
  // whichever tab happens to be in front.
  const viewerPlacement = useMessageViewerPrefsStore((s) => s.placement);

  useEffect(() => {
    loadTabs();
  }, [loadTabs]);

  // React Query's queries with `refetchInterval` (the sidebar's per-
  // connection status poll, every 10s) already skip firing while
  // `focusManager` reports the app unfocused — but its default listener
  // relies on the DOM's `visibilitychange`/`window.blur`, which WebView2
  // (Tauri's Windows webview) doesn't reliably fire when the OS window
  // loses focus while minimized/backgrounded. Left unfixed, that poll (one
  // native Kafka client created + destroyed per saved connection, every
  // 10s) never actually pauses on Windows even when the app sits in the
  // background for hours — a very plausible source of the kind of slow,
  // continuous memory growth a native client library's create/destroy
  // churn can produce over a long session. Wiring Tauri's own real
  // OS-level focus event in bypasses the unreliable DOM signal entirely.
  useEffect(() => {
    focusManager.setEventListener((handleFocus) => {
      const unlistenPromise = onWindowFocusChanged(handleFocus);
      return () => {
        unlistenPromise.then((fn) => fn());
      };
    });
  }, []);

  // Each tab keeps its own workspace selection/message-viewer state — the
  // global connection list (which clusters exist/are connected) still comes
  // straight from the backend and isn't tab-scoped.
  useEffect(() => {
    setSelectionActiveTab(activeTabId);
    setMessageViewerActiveTab(activeTabId);
  }, [activeTabId, setSelectionActiveTab, setMessageViewerActiveTab]);

  // The right pane's viewed message must always belong to whatever
  // topic/partition Data tab is currently showing in the middle pane —
  // otherwise switching topics leaves a stale message (and the whole right
  // pane, since it only renders while one is selected) visibly stuck from
  // the topic you just left. This can't live inside DataTab itself: that
  // component isn't even mounted while a non-Data sub-tab (Properties,
  // Partitions, ...) is active, so a clear scoped there silently fails to
  // fire on a topic switch made from one of those. `selection` is the one
  // thing that's always live and always changes on any such switch,
  // regardless of which sub-tab or panel happens to be mounted.
  useEffect(() => {
    if (!viewedConnectionId || !viewedTopic) return;
    const stillShowingViewedData =
      (selection?.type === "topic" &&
        selection.connectionId === viewedConnectionId &&
        selection.topicName === viewedTopic &&
        viewedPartitionId === undefined) ||
      (selection?.type === "partition" &&
        selection.connectionId === viewedConnectionId &&
        selection.topicName === viewedTopic &&
        selection.partitionId === viewedPartitionId);
    if (!stillShowingViewedData) {
      clearViewedMessage();
    }
  }, [selection, viewedConnectionId, viewedTopic, viewedPartitionId, clearViewedMessage]);

  return (
    <div className="app-shell">
      <header className="app-header">
        <TabBar />
        <button type="button" aria-label="Open settings" className="settings-gear" onClick={openSettings}>
          ⚙
        </button>
      </header>
      <div className="app-body">
        <ResizableShell
          left={
            <aside className="app-sidebar">
              {/* Add Cluster is the primary action of this pane — it is what
                  an empty sidebar is waiting for — so it takes the accent
                  fill and a row of its own, with Export/Import as the two
                  secondary actions sharing the row below. Previously all
                  three were unstyled buttons of equal weight, wrapping
                  arbitrarily against the sidebar's width. */}
              <div className="app-sidebar-actions">
                <button
                  type="button"
                  className="sidebar-action sidebar-action--primary"
                  onClick={() => setShowModal(true)}
                >
                  <PlusIcon />
                  Add Cluster
                </button>
                <div className="app-sidebar-actions-row">
                  <button type="button" className="sidebar-action" onClick={handleExportAll}>
                    <ExportIcon />
                    Export All
                  </button>
                  <button type="button" className="sidebar-action" onClick={handleImport}>
                    <ImportIcon />
                    Import
                  </button>
                </div>
              </div>
              {showModal && (
                <ConnectionModal
                  initialDraft={cloneDraft ?? undefined}
                  onAdd={async (connection) => {
                    const created = await createConnection.mutateAsync(connection);
                    // Saving is the part the Add button promises, and it has
                    // now happened — so the modal goes immediately, before the
                    // connect attempt rather than after it.
                    //
                    // Awaiting the connect first (which this used to do) meant
                    // that against a cluster that rejects the credentials, or
                    // one that simply isn't there, Add left the modal sitting
                    // open and inert for as long as the broker probe takes —
                    // up to PROBE_TIMEOUT, 10 seconds, on top of the TLS/SASL
                    // handshake attempts inside it. From the user's side that
                    // is indistinguishable from a modal that refuses to close.
                    closeModal();
                    // A saved connection sitting gray in the sidebar until the
                    // user manually hits Reconnect is a pointless extra step —
                    // connect it right away, in the background. Its outcome
                    // belongs to the tree (status dot, Reconnect button) and
                    // the logs panel, not to a modal that is already gone.
                    void (async () => {
                      try {
                        await api.connectConnection(created.id);
                      } catch {
                        // Swallowed deliberately — see comment above.
                      } finally {
                        queryClient.invalidateQueries({ queryKey: ["connection-connected", created.id] });
                        queryClient.invalidateQueries({ queryKey: ["connection-status", created.id] });
                      }
                    })();
                  }}
                  onCancel={closeModal}
                />
              )}
              <ConnectionTree onClone={handleClone} />
            </aside>
          }
          leftHidden={Boolean(activeJsonTab) || isSettingsActive}
          middle={
            isSettingsActive ? (
              <main className="app-main">
                <SettingsPanel />
              </main>
            ) : (
              <main className="app-main" key={activeTabId ?? "no-tab"}>
                {activeJsonTab ? (
                  <JsonViewerTabPanel tab={activeJsonTab} />
                ) : (
                  <>
                    {selection?.type === "connection" && <ClusterDetailPanel connectionId={selection.id} />}
                    {selection?.type === "broker" && (
                      <BrokerDetailPanel connectionId={selection.connectionId} brokerId={selection.brokerId} />
                    )}
                    {selection?.type === "topic" && (
                      <TopicDetailPanel connectionId={selection.connectionId} topicName={selection.topicName} />
                    )}
                    {selection?.type === "partition" && (
                      <PartitionDetailPanel
                        connectionId={selection.connectionId}
                        topicName={selection.topicName}
                        partitionId={selection.partitionId}
                      />
                    )}
                    {selection?.type === "consumerGroup" && (
                      <ConsumerGroupDetailPanel
                        key={`${selection.connectionId}-${selection.groupId}`}
                        connectionId={selection.connectionId}
                        groupId={selection.groupId}
                      />
                    )}
                    {selection?.type === "principal" && (
                      <PrincipalDetailPanel
                        key={`${selection.connectionId}-${selection.principal}`}
                        connectionId={selection.connectionId}
                        principal={selection.principal}
                      />
                    )}
                    {!selection && <p className="app-main-placeholder">Select a cluster, broker, or topic.</p>}
                  </>
                )}
              </main>
            )
          }
          right={hasSelectedMessage ? <MessagePayloadViewer key={activeTabId ?? "no-tab"} /> : undefined}
          rightPlacement={viewerPlacement}
        />
      </div>
      <BottomPanel />
    </div>
  );
}

export function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider>
        <PreferencesProvider>
          <IdleTimerProvider>
            <AppShell />
          </IdleTimerProvider>
        </PreferencesProvider>
      </ThemeProvider>
    </QueryClientProvider>
  );
}
