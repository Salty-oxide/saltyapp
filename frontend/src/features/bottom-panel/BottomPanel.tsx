import { useQueryClient } from "@tanstack/react-query";
import { useMemo, useSyncExternalStore } from "react";
import { LogsIcon } from "../../components/AppIcons";
import { LogsPanel } from "./LogsPanel";
import { useLogsListener } from "./useLogsListener";
import { useLogsPanelHeight } from "./useLogsPanelHeight";
import { useLogsStore } from "./useLogsStore";
import { api } from "../../lib/tauri";
import { useTabsStore } from "../tabs/useTabsStore";
import { useWorkspaceSelectionStore } from "../workspace/useWorkspaceSelectionStore";
import { useMessageViewerStore } from "../workspace/useMessageViewerStore";
import { tabDataPrefix, totalRetainedPayloadBytes, useTabDataStore } from "../workspace/useTabDataStore";
import { useGeneralSettingsStore } from "../settings/useGeneralSettingsStore";
import { ThemeMenuButton } from "../theme/ThemeMenuButton";
import { retainedPayloadBytes, retainedRowBytes } from "../connections/payloadDecoding";
import { MessageFetchResult } from "../../lib/tauri";

/** Formats a byte count (an estimate — see `retainedRowBytes` — not an exact heap figure) as megabytes for display. */
export function formatTabMemory(bytes: number): string {
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

export function BottomPanel() {
  useLogsListener();
  const isExpanded = useLogsStore((s) => s.isExpanded);
  const toggleExpanded = useLogsStore((s) => s.toggleExpanded);
  const { height: logsHeight, startResizing, resetHeight, isResizing } = useLogsPanelHeight();
  const activeTabId = useTabsStore((s) => s.activeTabId);

  // "Tab memory" is everything the active top-level tab has cached — every
  // topic/partition's fetched Data tab rows it holds (not just whichever
  // one happens to be selected right now), plus any payload loaded into
  // the right pane's message viewer — not anything scoped to the left
  // sidebar's tree.
  //
  // Measured by `retainedRowBytes` rather than by serializing the cache:
  // in the same units the ceiling below is enforced in, and without
  // allocating a copy of everything being measured on every render. See its
  // doc comment — both of those were wrong specifically for a tab holding
  // payloads, which is the only case with payloads to get wrong.
  const messagesByTab = useTabDataStore((s) => s.messagesByTab);
  const tabPrefix = tabDataPrefix(activeTabId);
  const viewedMessage = useMessageViewerStore((s) => (activeTabId ? s.byTab[activeTabId] : undefined) ?? null);
  const cachedBytes = useMemo(() => {
    const viewed = viewedMessage?.message ?? null;
    let total = 0;
    // Clicking a grid row hands the viewer the row object itself, so the
    // message on screen is almost always one of the rows already counted
    // below — one message held once, not two. It is counted separately only
    // when it is genuinely a separate copy: its rows can go without it (an
    // eviction, a disconnect, a re-fetch), and then it is the only thing
    // holding that payload.
    let viewedIsCached = false;
    for (const [key, messages] of Object.entries(messagesByTab)) {
      if (!key.startsWith(tabPrefix)) continue;
      total += retainedRowBytes(messages);
      if (viewed !== null && !viewedIsCached && messages.includes(viewed)) viewedIsCached = true;
    }
    return total + (viewed !== null && !viewedIsCached ? retainedRowBytes([viewed]) : 0);
  }, [messagesByTab, tabPrefix, viewedMessage]);

  // The figure the Max total fetch size ceiling is actually enforced against:
  // payload bytes retained across *every* tab, not just this one. Shown
  // beside the per-tab number because every tab shares one webview process,
  // so "how close is the app to the limit" is a different question from "what
  // is this tab holding" — and until it was displayed, neither the user nor
  // anyone tuning the limit could answer the first one.
  const payloadBytesByTab = useTabDataStore((s) => s.payloadBytesByTab);
  // The payload viewer's open message is held in React Query rather than in a
  // tab cache, and it is the one payload carried whole rather than truncated
  // — so leaving it out understated the figure by exactly the largest single
  // thing the app holds. Bounded to one entry by `useFullPayload`'s
  // `gcTime: 0`; summed rather than assumed to be one, so this stays honest
  // if that ever changes.
  //
  // Subscribed to the cache, not merely read from it: this component has no
  // query of its own, so nothing else would re-render it when a payload is
  // opened or released and the figure sat stale until something unrelated
  // happened to touch the panel. The snapshot is a number, so React only
  // re-renders when the total actually moves, and `base64DecodedLength` is
  // arithmetic on the string's length rather than a decode.
  const queryClient = useQueryClient();
  const openPayloadBytes = useSyncExternalStore(
    (onChange) => queryClient.getQueryCache().subscribe(onChange),
    () =>
      queryClient
        .getQueryCache()
        .getAll()
        .reduce(
          (total, query) =>
            query.queryKey[0] === "full-payload"
              ? total + retainedPayloadBytes((query.state.data as MessageFetchResult | undefined)?.messages ?? [])
              : total,
          0,
        ),
  );
  const retainedBytes = totalRetainedPayloadBytes(payloadBytesByTab) + openPayloadBytes;
  // The open payload counts towards this tab too. It belongs to the tab
  // showing it, it is the one payload carried whole rather than truncated —
  // so the largest single thing that tab holds — and leaving it out had the
  // per-tab figure sitting still while the app-wide one beside it jumped by
  // megabytes on the same click.
  const bytesUsed = cachedBytes + openPayloadBytes;
  const retentionLimit = useGeneralSettingsStore((s) => s.maxTotalFetchBytes);
  const isNearLimit = retainedBytes > retentionLimit * 0.8;

  const clearSelectionMemory = useWorkspaceSelectionStore((s) => s.clearTabMemory);
  const clearMessageMemory = useMessageViewerStore((s) => s.clearTabMemory);
  const clearAllTabData = useTabDataStore((s) => s.clearAllMessagesForTab);

  function handleClearMemory() {
    clearSelectionMemory();
    clearMessageMemory();
    clearAllTabData(activeTabId);
    // Dropping the cached rows/selection above frees that memory correctly,
    // but neither the JS heap nor Rust's own allocator hands freed pages
    // back to the OS on their own — this asks Windows to trim the process's
    // visible working set right now instead of waiting for it to happen
    // (if it ever does). No-op on macOS/Linux — see trimProcessMemory's doc
    // comment. Fire-and-forget: nothing in the UI depends on its result.
    void releaseBackendMemory();
  }

  /**
   * The backend's half of Clear memory: empty its payload cache, *then* trim —
   * the trim can only return pages that have already been freed. A cache that
   * cannot be cleared must not stop the trim, so a failure there is swallowed.
   */
  async function releaseBackendMemory() {
    try {
      await api.clearPayloadCache();
    } catch {
      // Nothing to tell the user: the rows they asked to clear are cleared.
    }
    await api.trimProcessMemory();
  }

  return (
    <div className="bottom-panel">
      {isExpanded && (
        <div
          className={`bottom-panel-resizer${isResizing ? " bottom-panel-resizer--active" : ""}`}
          role="separator"
          aria-orientation="horizontal"
          aria-label="Resize logs panel"
          title="Drag to resize the logs panel — double-click to reset"
          onPointerDown={startResizing}
          onDoubleClick={resetHeight}
        />
      )}
      <div className="bottom-panel-status-strip">
        {/* Icon-only, like the theme button beside it: the strip is the one
            piece of chrome on screen at every moment, and "▸ Logs" spent a
            word and a caret on a control whose glyph and pressed state say
            both. `aria-label` and `title` carry the name that was dropped. */}
        <button
          type="button"
          aria-label="Toggle logs panel"
          aria-expanded={isExpanded}
          title="Logs"
          className={`bottom-panel-toggle bottom-panel-toggle--icon${
            isExpanded ? " bottom-panel-toggle--active" : ""
          }`}
          onClick={toggleExpanded}
        >
          <LogsIcon />
        </button>
        <ThemeMenuButton />
        <div className="bottom-panel-memory">
          <span className="bottom-panel-memory-label">Tab memory: {formatTabMemory(bytesUsed)}</span>
          <span
            className={`bottom-panel-memory-label${isNearLimit ? " bottom-panel-memory-label--warning" : ""}`}
            title="Message payloads held across every tab, against Settings → General → Max total fetch size"
          >
            Payloads (all tabs): {formatTabMemory(retainedBytes)} / {formatTabMemory(retentionLimit)}
          </span>
          <button type="button" aria-label="Clear tab memory" onClick={handleClearMemory}>
            Clear memory
          </button>
        </div>
      </div>
      {isExpanded && (
        <div className="bottom-panel-content" style={{ height: logsHeight }} data-testid="logs-panel-content">
          <LogsPanel />
        </div>
      )}
    </div>
  );
}
