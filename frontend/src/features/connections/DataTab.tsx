import { useEffect, useRef, useState } from "react";
import { AgGridReact } from "ag-grid-react";
import {
  AllCommunityModule,
  ColDef,
  FilterChangedEvent,
  GetRowIdParams,
  GridApi,
  GridReadyEvent,
  ModuleRegistry,
  RowSelectionOptions,
  SortChangedEvent,
  ValueFormatterParams,
  ValueGetterParams,
} from "ag-grid-community";
import { listen } from "@tauri-apps/api/event";
import { MessagesBatchEvent, TopicMessage } from "../../lib/tauri";
import { useTabsStore } from "../tabs/useTabsStore";
import { useMessageViewerStore } from "../workspace/useMessageViewerStore";
import {
  dataTabCacheKey,
  EMPTY_TAB_MESSAGES,
  totalRetainedPayloadBytes,
  useTabDataStore,
} from "../workspace/useTabDataStore";
import { APP_GRID_THEME } from "./agGridTheme";
import {
  emptyFilterForm,
  FilterFormState,
  toMessageFilter,
  validateDateRange,
  validateMaxMessagesPerPartition,
} from "./dataFilters";
import { useDataTabFiltersStore } from "./useDataTabFiltersStore";
import {
  DataTabGridState,
  EMPTY_DATA_TAB_GRID_STATE,
  useDataTabGridStateStore,
} from "./useDataTabGridStateStore";
import {
  base64ToDisplayText,
  decodeValuePreview,
  base64DecodedLength,
  MAX_INLINE_PAYLOAD_BYTES,
  retainedPayloadBytes,
} from "./payloadDecoding";
import { api } from "../../lib/tauri";
import { formatLocalTimestamp, localTimeZoneLabel } from "../../lib/time";
import { useFetchMessages } from "./useClusterResources";
import { useGeneralSettingsStore } from "../settings/useGeneralSettingsStore";
import { useLogsStore } from "../bottom-panel/useLogsStore";
import { ValueCell, ValueCellContext } from "./ValueCell";

ModuleRegistry.registerModules([AllCommunityModule]);

/**
 * The broker's record timestamp, shown in the system's timezone rather than
 * UTC — see `formatLocalTimestamp`. The column still sorts and filters on the
 * raw `timestampMs`, so this is presentation only.
 */
function formatTimestamp(params: ValueFormatterParams<TopicMessage, number | null>): string {
  return formatLocalTimestamp(params.value);
}

/**
 * Previews are cached per message object rather than recomputed: AG Grid
 * calls a `valueGetter` again on every sort, filter and re-render. Keyed
 * weakly, so a row's preview is released with the row itself when a new
 * fetch replaces the tab's messages.
 */
const TIMESTAMP_HEADER = (() => {
  const zone = localTimeZoneLabel();
  return zone ? `Timestamp (${zone})` : "Timestamp";
})();

/**
 * The From/To filter inputs name the same zone as the Timestamp column, for
 * one reason: the boundary a user types is read in the system's timezone
 * (see `parseDate`), so the field and the column it filters have to visibly
 * agree. Saying it on both is what makes "filter from 09:00, get rows from
 * 09:00" something the user can see rather than has to trust.
 */
const FROM_LABEL = TIMESTAMP_HEADER === "Timestamp" ? "From" : `From (${localTimeZoneLabel()})`;
const TO_LABEL = TIMESTAMP_HEADER === "Timestamp" ? "To" : `To (${localTimeZoneLabel()})`;

const valuePreviewCache = new WeakMap<TopicMessage, string>();

/**
 * The Value column's text: a bounded preview of the payload (see
 * `decodeValuePreview`), blank until "Fetch message payload" is checked and a
 * fetch has run. The full payload is decoded only when a row is opened in
 * `MessagePayloadViewer`.
 *
 * Bounded rather than whole-payload: decoding every loaded message in full
 * to fill a grid cell costs ~244ms over 300 x 2 MB messages, and the column
 * only ever shows a cell's worth of it. A row's full value is one click away
 * in the payload viewer.
 */
function messageValueText(message: TopicMessage | undefined): string {
  if (!message) return "";
  const cached = valuePreviewCache.get(message);
  if (cached !== undefined) return cached;

  const preview = decodeValuePreview(message.payloadBase64);
  valuePreviewCache.set(message, preview);
  return preview;
}

/** Decodes a message's key for display — base64 on the wire since a Kafka key is an arbitrary byte string, not guaranteed text. */
function messageKeyText(message: TopicMessage | undefined): string {
  return base64ToDisplayText(message?.keyBase64 ?? null) ?? "";
}

function formatValue(params: ValueGetterParams<TopicMessage>): string {
  return messageValueText(params.data);
}

function formatKey(params: ValueGetterParams<TopicMessage>): string {
  return messageKeyText(params.data);
}

/**
 * How long streamed messages accumulate before being written to the grid.
 *
 * Short enough that rows still appear to arrive continuously, long enough that
 * the arrival rate stops mattering: at any speed the grid re-renders ten times
 * a second rather than once per message.
 */
const STREAM_FLUSH_MS = 100;

const COLUMN_DEFS: ColDef<TopicMessage>[] = [
  { field: "partition", headerName: "Partition", width: 100 },
  { field: "offset", headerName: "Offset", width: 100 },
  {
    field: "timestampMs",
    // The zone is named once in the header instead of on every row — see
    // `localTimeZoneLabel`. Read at module load: a machine does not change
    // timezone mid-session, and a header that re-derived it per render would
    // be doing `Intl` work on every grid repaint for a string that never
    // moves.
    headerName: TIMESTAMP_HEADER,
    valueFormatter: formatTimestamp,
    width: 200,
  },
  { headerName: "Key", valueGetter: formatKey, width: 150 },
  { headerName: "Value", valueGetter: formatValue, cellRenderer: ValueCell, flex: 1 },
];

const DEFAULT_COL_DEF: ColDef<TopicMessage> = {
  sortable: true,
  filter: true,
  resizable: true,
};

/**
 * Single-row selection, purely so the row whose payload is open in the right
 * pane is visibly marked in the grid.
 *
 * `enableClickSelection: false` deliberately takes AG Grid out of the
 * business of deciding what's selected: the selection is driven from
 * `useMessageViewerStore` instead (see `syncGridSelection`), which is the
 * only way the highlight can't drift from what the right pane is actually
 * showing. A click AG Grid would have selected on — the Value column's
 * "Fetch payload" button — is one `onRowClicked` refuses to open, and a
 * selection the store makes on its own — restoring the viewed message after
 * a tab switch — is one no click produced.
 */
const ROW_SELECTION: RowSelectionOptions<TopicMessage> = {
  mode: "singleRow",
  checkboxes: false,
  enableClickSelection: false,
};

/**
 * A row's identity, stable across the re-renders that a streaming fetch and
 * a single-row payload patch both cause. (partition, offset) is unique
 * within a topic by definition.
 *
 * Giving AG Grid `getRowId` is what lets a selected row survive `rowData`
 * being replaced — without it the grid throws away every row node (and the
 * selection with it) each time the streaming flush hands it a new array.
 */
function messageRowId(message: TopicMessage): string {
  return `${message.partition}:${message.offset}`;
}

function getRowId(params: GetRowIdParams<TopicMessage>): string {
  return messageRowId(params.data);
}

/** Marks exactly `rowId` selected — walking the selected nodes rather than every row, so this stays O(selection) on a grid holding tens of thousands of rows. */
function syncGridSelection(gridApi: GridApi<TopicMessage>, rowId: string | null): void {
  for (const node of gridApi.getSelectedNodes()) {
    if (node.id !== rowId) node.setSelected(false);
  }
  if (rowId === null) return;
  const node = gridApi.getRowNode(rowId);
  if (node && !node.isSelected()) node.setSelected(true);
}

/** Re-applies a saved sort/column-filter arrangement to a freshly created (or freshly re-keyed) grid. `defaultState` clears the sort on columns the saved model doesn't mention, so restoring "unsorted" actually unsorts. */
function applyGridState(gridApi: GridApi<TopicMessage>, state: DataTabGridState): void {
  gridApi.applyColumnState({
    state: state.sortModel.map((item, index) => ({ colId: item.colId, sort: item.sort, sortIndex: index })),
    defaultState: { sort: null },
  });
  gridApi.setFilterModel(state.filterModel);
}

export interface DataTabProps {
  connectionId: string;
  topicName: string;
  /** When set, this Data tab is scoped to a single partition — the Partition field is prepopulated and locked to it. */
  partitionId?: number;
}

/**
 * Fetch pulls a bounded snapshot of message metadata applying the filters
 * below (an all-blank filter pulls everything). Stop both discards the
 * result client-side (so late-arriving stream events/the final result never
 * reach the grid) and tells the backend to interrupt the poll loop that's
 * running it, via `connection_cancel_fetch` — see `stopActiveFetch`.
 */
export function DataTab({ connectionId, topicName, partitionId }: DataTabProps) {
  const [isPlaying, setIsPlaying] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const fetchMessages = useFetchMessages();
  const viewMessage = useMessageViewerStore((s) => s.viewMessage);
  const clearViewedMessage = useMessageViewerStore((s) => s.clear);
  const stoppedRef = useRef(false);
  const activeTabId = useTabsStore((s) => s.activeTabId);
  const tabKey = dataTabCacheKey(activeTabId, connectionId, topicName, partitionId);
  // Keyed the same way as the cached messages below, so returning to a
  // topic (or partition) you've already set filters on shows them exactly
  // as you left them, instead of resetting every time this component's
  // props change to a different topic and back.
  const defaultForm = partitionId === undefined ? emptyFilterForm() : { ...emptyFilterForm(), partitions: String(partitionId) };
  const form = useDataTabFiltersStore((s) => s.formByTab[tabKey]) ?? defaultForm;
  const setStoredForm = useDataTabFiltersStore((s) => s.setForm);
  // How the grid is arranged (sort, column filters) is kept under the same
  // key, for the same reason and one more: the middle pane is rendered
  // `key={activeTabId}` in App.tsx, so every top-level tab switch destroys
  // the grid. Held in AG Grid, a sort and a filter were gone by the time the
  // user came back.
  const gridState = useDataTabGridStateStore((s) => s.stateByTab[tabKey]) ?? EMPTY_DATA_TAB_GRID_STATE;
  const patchGridState = useDataTabGridStateStore((s) => s.patchState);
  const gridApiRef = useRef<GridApi<TopicMessage> | null>(null);
  /** Set while `applyGridState` is pushing a saved arrangement into the grid, so the sort/filter events that causes aren't written straight back to the store as if the user had made them. */
  const isRestoringGridStateRef = useRef(false);
  const tabKeyRef = useRef(tabKey);
  tabKeyRef.current = tabKey;
  const messages = useTabDataStore((s) => s.messagesByTab[tabKey] ?? EMPTY_TAB_MESSAGES);
  /** Set when the last Fetch stopped on the byte budget rather than on the filter — see `MessageFetchResult.stoppedAtByteBudget`. Local rather than cached per tab: it describes the fetch that just ran, and a re-fetch always re-decides it. */
  const [byteBudgetBytesRead, setByteBudgetBytesRead] = useState<number | null>(null);
  const setTabMessages = useTabDataStore((s) => s.setTabMessages);
  const clearTabMessages = useTabDataStore((s) => s.clearTabMessages);
  const addTabPayloadBytes = useTabDataStore((s) => s.addTabPayloadBytes);
  /** Set when this view alone holds the whole Max Total Fetch Size, so eviction has nothing left it is allowed to drop — see `enforceRetentionLimit`. */
  const [payloadBudgetSpent, setPayloadBudgetSpent] = useState(false);
  /** Payload bytes this view is currently holding, across its last Fetch and every per-row "Fetch payload" click since. */
  const payloadBytesHeld = useTabDataStore((s) => s.payloadBytesByTab[tabKey] ?? 0);
  /** True when this view's rows were dropped to keep the app inside the ceiling while the user was working elsewhere. */
  const wasEvicted = useTabDataStore((s) => s.evictedTabs[tabKey] ?? false);
  const evictToFit = useTabDataStore((s) => s.evictToFit);
  const touchTab = useTabDataStore((s) => s.touchTab);
  const setTabTotalMatching = useTabDataStore((s) => s.setTabTotalMatching);
  const setTabFetchDurationMs = useTabDataStore((s) => s.setTabFetchDurationMs);
  /** How many messages match the last Fetch's filter in total, uncapped by "max messages per partition"/"total max messages" — `messages.length` can be smaller when those caps trimmed the result. `undefined` before any Fetch has run for this tab. */
  const totalMatching = useTabDataStore((s) => s.totalMatchingByTab[tabKey]);
  const fetchDurationMs = useTabDataStore((s) => s.fetchDurationMsByTab[tabKey]);
  const appendTabMessages = useTabDataStore((s) => s.appendTabMessages);
  /**
   * The stream listener below is subscribed once and must not re-subscribe,
   * so the things it calls are reached through refs kept current on every
   * render rather than through its own (stale) closure.
   */
  const addTabPayloadBytesRef = useRef(addTabPayloadBytes);
  addTabPayloadBytesRef.current = addTabPayloadBytes;
  // Wrapped rather than assigned directly: `enforceRetentionLimit` is
  // declared further down, and the arrow defers reaching it until the
  // listener actually fires.
  const enforceRetentionLimitRef = useRef<() => void>(() => {});
  enforceRetentionLimitRef.current = () => enforceRetentionLimit();
  /** Streamed messages waiting to be written to the store — see the listener below. */
  const streamBufferRef = useRef<TopicMessage[]>([]);
  const streamFlushHandleRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  /**
   * Writes the buffer out now, set by the listener effect below.
   *
   * A completed fetch has to flush rather than wait for the pending timer:
   * the streamed rows *are* the result now (the response no longer repeats
   * them), so discarding the last window — which is what used to happen,
   * harmlessly, when the authoritative result replaced everything — would
   * lose up to a flush interval's worth of rows off the end of every fetch.
   */
  const streamFlushRef = useRef<() => void>(() => {});
  /** Tags the in-flight Fetch's `requestId` so the "messages-batch" listener below can tell its rows apart from a stale/superseded fetch's late-arriving events — see `MessagesBatchEvent`'s doc comment. */
  const activeRequestIdRef = useRef<string | null>(null);

  // The row the right pane is showing, as a grid row id — or null when the
  // pane is closed, or when what it holds came from a different
  // topic/partition than this grid is displaying. Deriving the highlight
  // from the viewer store (rather than from the click that usually causes
  // it) is what keeps "highlighted row" and "payload on the right" the same
  // row in every case, including the ones no click produced: reopening a
  // top-level tab, and the Close button clearing the pane.
  const viewedMessage = useMessageViewerStore((s) => s.message);
  const viewedConnectionId = useMessageViewerStore((s) => s.connectionId);
  const viewedTopic = useMessageViewerStore((s) => s.topic);
  const viewedPartitionId = useMessageViewerStore((s) => s.partitionId);
  const selectedRowId =
    viewedMessage !== null &&
    viewedConnectionId === connectionId &&
    viewedTopic === topicName &&
    viewedPartitionId === partitionId
      ? messageRowId(viewedMessage)
      : null;

  // Rows the grid doesn't have yet can't be selected, so this also runs from
  // `onGridReady` (the grid didn't exist when the effect first ran) and from
  // `onRowDataUpdated` (the row arrived after it). `syncGridSelection` is
  // idempotent, so running it from all three costs nothing.
  useEffect(() => {
    if (gridApiRef.current) syncGridSelection(gridApiRef.current, selectedRowId);
  }, [selectedRowId, messages]);

  // Looking at a view counts as using it, so eviction prefers the ones the
  // user has left behind. Without this, opening a tab and reading it without
  // re-fetching left it looking like the coldest thing in the app.
  useEffect(() => {
    touchTab(tabKey);
  }, [tabKey, touchTab]);

  // Restores the saved arrangement when this tab's grid is replaced by a
  // different one. That happens two ways, and only one of them recreates the
  // grid: a top-level tab switch remounts everything (handled by
  // `onGridReady` below), while switching topic/partition within a tab keeps
  // the same grid and only changes `tabKey` — which is what this covers.
  useEffect(() => {
    if (gridApiRef.current) restoreGridState(gridApiRef.current, tabKey);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tabKey]);

  // Streams rows into the grid as connection_fetch_messages's backend task
  // polls them, instead of leaving the grid empty until the whole fetch
  // finishes. Subscribed once (not per-fetch) and reads the current
  // request/tab via refs, since re-subscribing on every Fetch click would
  // risk a race between the old listener's teardown and a new one's setup.
  //
  // Arrivals are buffered and flushed on a timer rather than written straight
  // through: one store write per message means one grid render per message,
  // and each rebuilds the tab's row array, so a big fetch spends longer
  // painting rows than it does fetching them (20,000 messages: ~1.15s of
  // copying across 20,000 renders, against ~11ms across 100 batched ones).
  // A tenth of a second is below what reads as delay, and turns any arrival
  // rate into at most ten renders a second.
  useEffect(() => {
    const flush = () => {
      streamFlushHandleRef.current = null;
      const buffered = streamBufferRef.current;
      if (buffered.length === 0) return;
      streamBufferRef.current = [];
      appendTabMessages(tabKeyRef.current, buffered);
      // Counted as the rows land, not only when the fetch resolves.
      // `handlePlay` sets the authoritative total from the final result, but
      // it only does so on success — so a fetch the user Stops, or one that
      // errors, used to leave every row it had already streamed into the grid
      // sitting there weighing nothing at all as far as the ceiling was
      // concerned. On a large fetch stopped near the end that was hundreds of
      // megabytes the app was holding and not counting.
      //
      // Enforced here too, so a long-running fetch makes room as it grows
      // rather than only once it finishes. The view being fetched into is
      // protected, so this can only ever evict colder views elsewhere.
      addTabPayloadBytesRef.current(tabKeyRef.current, retainedPayloadBytes(buffered));
      enforceRetentionLimitRef.current();
    };

    streamFlushRef.current = flush;

    const unlisten = listen<MessagesBatchEvent>("messages-batch", (event) => {
      if (stoppedRef.current) return;
      if (event.payload.requestId !== activeRequestIdRef.current) return;
      // One event, many rows — see `MessagesBatchEvent`.
      streamBufferRef.current.push(...event.payload.messages);
      if (streamFlushHandleRef.current === null) {
        streamFlushHandleRef.current = setTimeout(flush, STREAM_FLUSH_MS);
      }
    });

    return () => {
      unlisten.then((fn) => fn());
      if (streamFlushHandleRef.current !== null) clearTimeout(streamFlushHandleRef.current);
    };
  }, [appendTabMessages]);

  // DataTab is reused (not remounted) when switching between topics,
  // partitions, or connections within the same top-level tab — neither
  // App.tsx's <TopicDetailPanel>/<PartitionDetailPanel> nor this component
  // are keyed by topic/partition, only by the top-level tab. The fetch
  // filter form and the grid's arrangement (sort, column filters) are both
  // keyed per-topic above, so they don't need resetting here: a different
  // topic is a different key and therefore starts blank — which is what
  // stops a leftover column filter from a previous topic silently hiding a
  // new one's rows — while coming back to a topic you'd already arranged
  // restores it exactly as you left it.
  //
  // A stale error from a previous fetch has no such key, so it still needs
  // clearing explicitly.
  //
  // The right pane's viewed message needs a reset for the same underlying
  // reason, but is deliberately NOT handled here — this component isn't
  // even mounted while a non-Data sub-tab (e.g. Properties) is active, so a
  // clear scoped to this effect would silently fail to fire when the user
  // switches topics from one of those. That reset lives in App.tsx instead,
  // driven directly off `useWorkspaceSelectionStore`'s `selection`, which
  // always changes on a topic/partition switch regardless of which sub-tab
  // (or component) happens to be mounted at the time.
  useEffect(() => {
    stopActiveFetch();
    setError(null);
    setPayloadBudgetSpent(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [connectionId, topicName, partitionId]);

  // The effect above only covers switches this component survives. Plenty of
  // ordinary actions unmount it outright mid-fetch instead: TopicDetailPanel
  // renders it as `{activeTab === "data" && <DataTab/>}`, so every other
  // sub-tab drops it, as do selecting a broker/partition/consumer group,
  // switching top-level tab, and closing the tab. Each of those used to
  // leave the backend polling the broker for a fetch whose UI was gone and
  // whose request id nothing held any more — unstoppable for as long as it
  // took to finish on its own. Reads the request id from a ref at teardown,
  // so it cancels whatever was in flight at that moment.
  useEffect(() => {
    return () => {
      stopActiveFetch();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function updateForm(patch: Partial<FilterFormState>) {
    setStoredForm(tabKey, { ...form, ...patch });
  }

  /** Pushes a key's saved sort/column filters into the grid, reading the store directly so it always applies the arrangement belonging to the key it's given rather than whichever one this render closed over. */
  function restoreGridState(gridApi: GridApi<TopicMessage>, key: string) {
    isRestoringGridStateRef.current = true;
    try {
      applyGridState(gridApi, useDataTabGridStateStore.getState().stateByTab[key] ?? EMPTY_DATA_TAB_GRID_STATE);
    } finally {
      isRestoringGridStateRef.current = false;
    }
  }

  function handleGridReady(event: GridReadyEvent<TopicMessage>) {
    gridApiRef.current = event.api;
    restoreGridState(event.api, tabKeyRef.current);
    syncGridSelection(event.api, selectedRowId);
  }

  function handleSortChanged(event: SortChangedEvent<TopicMessage>) {
    if (isRestoringGridStateRef.current) return;
    patchGridState(tabKeyRef.current, {
      sortModel: event.api
        .getColumnState()
        .filter((column) => column.sort)
        .sort((a, b) => (a.sortIndex ?? 0) - (b.sortIndex ?? 0))
        .map((column) => ({ colId: column.colId, sort: column.sort as "asc" | "desc" })),
    });
  }

  function handleFilterChanged(event: FilterChangedEvent<TopicMessage>) {
    if (isRestoringGridStateRef.current) return;
    patchGridState(tabKeyRef.current, { filterModel: event.api.getFilterModel() });
  }

  /**
   * Brings the app-wide retained total back inside Max Total Fetch Size by
   * dropping the coldest views' rows.
   *
   * Eviction rather than refusal, because refusing only stops *new* growth:
   * by the time a fetch would be refused the app is already holding whatever
   * other tabs accumulated earlier, and nothing frees it. Every evicted view
   * is one Fetch away from coming back, so trading a round trip for staying
   * inside the ceiling is nearly always the right way round — and it is the
   * webview's own process that dies when it isn't.
   *
   * The view being fetched into is protected, so a fetch can never evict its
   * own results. When that view alone is over the limit there is nothing
   * left to drop, and the notice below says so instead.
   */
  function enforceRetentionLimit() {
    const limit = useGeneralSettingsStore.getState().maxTotalFetchBytes;
    // Timed like every other action the panel reports: eviction walks and
    // rewrites the cached rows of every view it drops, so on a large cache it
    // is not free, and it runs on each flush of a streaming fetch.
    const started = performance.now();
    const evicted = evictToFit(limit, tabKey);
    if (evicted.length > 0) {
      useLogsStore.getState().addEntry({
        timestamp: new Date().toISOString(),
        level: "info",
        message: `Cleared ${evicted.length} cached message view(s) to stay within Max total fetch size in ${Math.round(performance.now() - started)} ms`,
      });
    }
    const remaining = totalRetainedPayloadBytes(useTabDataStore.getState().payloadBytesByTab);
    setPayloadBudgetSpent(remaining > limit);
  }

  async function handlePlay() {
    setError(null);
    const maxMessagesError = validateMaxMessagesPerPartition(form);
    if (maxMessagesError) {
      setError(maxMessagesError);
      return;
    }
    const dateRangeError = validateDateRange(form);
    if (dateRangeError) {
      setError(dateRangeError);
      return;
    }
    setIsPlaying(true);
    // Wall clock, started here rather than around `mutateAsync` alone: what
    // the user is timing is the Fetch they clicked, which includes the
    // streamed rows being painted in, not just the round trip.
    const startedAt = performance.now();
    stoppedRef.current = false;
    discardBufferedStream();
    const requestId = crypto.randomUUID();
    activeRequestIdRef.current = requestId;
    // Clears the previous fetch's total-matching count along with its rows —
    // `setTabMessages(tabKey, [])` alone would leave a stale "matching" total
    // from an earlier, unrelated fetch on screen next to the new one's rows
    // until (if) this fetch resolves successfully.
    clearTabMessages(tabKey);
    setByteBudgetBytesRead(null);
    setPayloadBudgetSpent(false);
    // The grid is about to go blank/loading for a new result set, so a
    // right panel left open from the previous fetch would be showing a row
    // that may no longer exist in it.
    clearViewedMessage();
    // The success path retires the request id as soon as it has the
    // authoritative result, so `finally` can no longer tell "I am still the
    // current fetch" from "I was superseded" by looking at the ref alone.
    // This remembers which of the two it was.
    let retiredHere = false;
    try {
      const result = await fetchMessages.mutateAsync({
        connectionId,
        topic: topicName,
        filter: toMessageFilter(form),
        requestId,
        // Rows are painted as they arrive, from the stream below.
        streamUpdates: true,
      });
      if (!stoppedRef.current && activeRequestIdRef.current === requestId) {
        // The streamed rows are the result. The response used to repeat every
        // message the stream had already delivered, and this replaced the
        // grid's rows with that copy — which is why the buffer was dropped
        // here (its rows were about to be written again, and appending them
        // showed up as "200 loaded of 100 matching", doubled payload byte
        // totals, and duplicate `getRowId`s that cost AG Grid the selected
        // row on the next tab switch).
        //
        // Now the response carries only what the stream did *not* deliver, so
        // the two are disjoint by construction: flush what is buffered, then
        // append the remainder — which is empty on every ordinary fetch, and
        // exactly the missing rows if the forwarding task ended early.
        streamFlushRef.current();
        if (result.messages.length > 0) {
          appendTabMessages(tabKey, result.messages);
          // Same accounting the flush does for streamed rows: what this view
          // is now *holding*, measured off the rows themselves rather than
          // taken from the fetch's `payloadBytesRead`. A fetch reads whole
          // messages and keeps only a bounded slice of each, so the two
          // differ by orders of magnitude on a large-message topic — and it
          // is what is held, not what crossed the wire, that decides whether
          // the webview survives.
          addTabPayloadBytes(tabKey, retainedPayloadBytes(result.messages));
        }
        // Retired only after the flush above, so nothing streamed for this
        // request is dropped on the way out.
        activeRequestIdRef.current = null;
        retiredHere = true;
        setTabTotalMatching(tabKey, result.totalMatching);
        // Recorded on this branch only — the branch that has the
        // authoritative result. A stopped or superseded fetch leaves the
        // view with no timing rather than one that describes a fetch the
        // user cut short.
        setTabFetchDurationMs(tabKey, Math.round(performance.now() - startedAt));
        setByteBudgetBytesRead(result.stoppedAtByteBudget ? (result.payloadBytesRead ?? 0) : null);
        enforceRetentionLimit();
      }
    } catch (err) {
      if (!stoppedRef.current && activeRequestIdRef.current === requestId) {
        setError(err instanceof Error ? err.message : "Failed to fetch messages");
      }
    } finally {
      // Only when this fetch is still the current one. Stop re-enables the
      // Fetch button while this promise is still pending, so a second fetch
      // can already be in flight by the time this settles — and it must not
      // have its buffer discarded, its request id retired, or its spinner
      // turned off by the request it replaced.
      if (retiredHere || activeRequestIdRef.current === requestId) {
        discardBufferedStream();
        activeRequestIdRef.current = null;
        setIsPlaying(false);
      }
    }
  }

  /**
   * Drops anything streamed but not yet written to the store.
   *
   * Buffered messages belong to whichever request was in flight when they
   * arrived: carrying them into the next fetch would paint one filter's rows
   * into another's results, and past a Stop they are rows the user asked not
   * to wait for.
   */
  function discardBufferedStream() {
    streamBufferRef.current = [];
    if (streamFlushHandleRef.current !== null) {
      clearTimeout(streamFlushHandleRef.current);
      streamFlushHandleRef.current = null;
    }
  }

  /**
   * Stops the in-flight fetch, client-side and on the backend, and
   * re-enables the filters. Used by the Stop button directly, and by the
   * topic/partition/connection-switch effect below — DataTab is reused
   * (not remounted) across a tab's topics, so without this a fetch left
   * running when the user switches away keeps polling the old topic's
   * broker for a tab nobody is waiting on anymore. Safe to call when
   * nothing is in flight: `activeRequestIdRef` is only set by a Fetch, and
   * cancelling an already-finished/unknown request id is a no-op on the
   * backend.
   */
  function stopActiveFetch() {
    stoppedRef.current = true;
    discardBufferedStream();
    setIsPlaying(false);
    if (activeRequestIdRef.current) {
      api.cancelFetch(activeRequestIdRef.current).catch(() => {});
    }
  }

  function handleStop() {
    stopActiveFetch();
  }

  /**
   * Fetches just one row's payload (by its exact partition/offset) and
   * patches it into the cached rows — reads the store directly so it always
   * applies on top of the latest cached rows regardless of how long the
   * fetch took.
   *
   * Charged against the same app-wide Max Total Fetch Size a full Fetch is.
   * These bytes land in exactly the same cached rows and cost the webview
   * exactly the same memory; the only difference is that they arrive one
   * click at a time, and each click is its own backend fetch.
   *
   * The click is always allowed — it is one message, and refusing it while
   * the app holds megabytes of colder rows elsewhere would be protecting the
   * wrong thing. The ceiling is restored afterwards by evicting those colder
   * views instead (`enforceRetentionLimit`).
   */
  async function fetchPayloadForRow(row: TopicMessage) {
    const result = await fetchMessages.mutateAsync({
      connectionId,
      topic: topicName,
      filter: {
        partitions: [row.partition],
        maxMessagesPerPartition: 1,
        maxTotalMessages: 1,
        fromTimestampMs: null,
        toTimestampMs: null,
        offset: row.offset,
        includePayload: true,
        // Bounded, but at the whole-row bound rather than the grid cell's:
        // this is one message, so the retention budget the grid fetch
        // divides between hundreds of rows is spent on it alone — and a row
        // filled in here should open in the viewer without needing a second
        // trip to the broker, exactly like a row the grid fetch brought back.
        maxPayloadPreviewBytes: MAX_INLINE_PAYLOAD_BYTES,
      },
      // Own, unmatched request id — this single-row fetch shouldn't be
      // appended to the grid via the "messages-batch" listener above (which
      // only reacts to the main Fetch's activeRequestIdRef); its result is
      // patched into the cached rows directly below instead.
      requestId: crypto.randomUUID(),
      // ...and for the same reason it is not streamed at all: the event would
      // be emitted only to be discarded by that listener.
      streamUpdates: false,
    });
    const updated = result.messages.find((m) => m.partition === row.partition && m.offset === row.offset);
    if (!updated) return;
    const current = useTabDataStore.getState().messagesByTab[tabKey] ?? EMPTY_TAB_MESSAGES;
    // The bytes actually kept for this row — the payload is truncated to
    // `MAX_INLINE_PAYLOAD_BYTES` on the way back, so the message's full size
    // is not what lands in memory. Charged as the *change*, because the
    // update below replaces the row: whatever it was holding is released in
    // the same breath, and charging for the new payload on top of the old one
    // would have the tab counting bytes nothing holds. Zero on the ordinary
    // path (the button only offers itself on a row with no payload at all),
    // and the difference on a row fetched with a preview already in it.
    const replaced = current.find((m) => m.partition === row.partition && m.offset === row.offset);
    addTabPayloadBytes(
      tabKey,
      base64DecodedLength(updated.payloadBase64 ?? "") - base64DecodedLength(replaced?.payloadBase64 ?? ""),
    );
    setTabMessages(
      tabKey,
      current.map((m) => (m.partition === row.partition && m.offset === row.offset ? updated : m)),
    );
    enforceRetentionLimit();
  }

  const gridContext: ValueCellContext = { fetchPayload: fetchPayloadForRow };

  return (
    <div className="data-tab">
      <div className="data-tab-filters">
        <label>
          Max messages per partition
          <input
            inputMode="numeric"
            value={form.maxMessagesPerPartition}
            onChange={(e) => updateForm({ maxMessagesPerPartition: e.target.value })}
            disabled={isPlaying}
          />
        </label>
        <label>
          Total max messages
          <input
            inputMode="numeric"
            value={form.maxTotalMessages}
            onChange={(e) => updateForm({ maxTotalMessages: e.target.value })}
            disabled={isPlaying}
          />
        </label>
        <label>
          Partition
          <input
            value={form.partitions}
            onChange={(e) => updateForm({ partitions: e.target.value })}
            placeholder="e.g. 0, 1, 2"
            disabled={partitionId !== undefined || isPlaying}
          />
        </label>
        <label>
          Offset
          <input
            inputMode="numeric"
            value={form.offset}
            onChange={(e) => updateForm({ offset: e.target.value })}
            placeholder="e.g. 100"
            disabled={isPlaying}
          />
        </label>
        <label>
          {FROM_LABEL}
          <input
            type="datetime-local"
            value={form.fromDate}
            onChange={(e) => updateForm({ fromDate: e.target.value })}
            disabled={isPlaying}
          />
        </label>
        <label>
          {TO_LABEL}
          <input
            type="datetime-local"
            value={form.toDate}
            onChange={(e) => updateForm({ toDate: e.target.value })}
            disabled={isPlaying}
          />
        </label>
      </div>

      <label className="connection-modal-checkbox-label data-tab-include-payload">
        <input
          type="checkbox"
          checked={form.includePayload}
          onChange={(e) => updateForm({ includePayload: e.target.checked })}
          disabled={isPlaying}
        />
        Fetch message payload
      </label>
      <div className="data-tab-controls">
        <button type="button" aria-label="Fetch" onClick={handlePlay} disabled={isPlaying}>
          ▶ Fetch
        </button>
        <button type="button" aria-label="Stop" onClick={handleStop} disabled={!isPlaying}>
          ■ Stop
        </button>
      </div>

      {error && (
        <p role="alert" className="connection-modal-error">
          {error}
        </p>
      )}

      {byteBudgetBytesRead !== null && (
        // A count cap can't express this: on a topic of multi-megabyte
        // records the fetch stops on size long before it stops on the
        // message counts in the form, and without saying so a short result
        // reads as "that's all there is".
        <p role="status" className="data-tab-notice data-tab-notice--warning">
          Stopped after reading {Math.round(byteBudgetBytesRead / (1024 * 1024)).toLocaleString()} MB of messages.
          Narrow the filter, or raise Max total fetch size in Settings → General, to load more.
        </p>
      )}

      {wasEvicted && (
        // Rows vanishing with no explanation is worse than the memory
        // pressure that caused it: the filters and sort are still here, so an
        // empty grid reads as a broken fetch rather than a deliberate one.
        <p role="status" className="data-tab-notice">
          These messages were cleared while you were working elsewhere, to keep the app within Max total fetch size.
          Your filters are unchanged — Fetch again to reload them.
        </p>
      )}

      {payloadBudgetSpent && (
        // Only reachable when this view alone is over the ceiling: everything
        // else has already been evicted and there is nothing left to free.
        <p role="status" className="data-tab-notice data-tab-notice--warning">
          This view alone is holding {Math.round(payloadBytesHeld / (1024 * 1024)).toLocaleString()} MB of message
          payloads — the whole Max total fetch size. Narrow the filter, or raise the limit in Settings → General.
        </p>
      )}

      <p className="data-tab-total-count">
        {/*
          Spelled out rather than "100 / 600000 loaded", which reads as though
          the fetch pulled 600,000 messages. It pulled the number on the left;
          the number on the right is how many exist within the filter, and the
          gap is what tells you more are there. Separators because a bare
          600000 is hard to size at a glance.
        */}
        {messages.length.toLocaleString()} loaded of {(totalMatching ?? messages.length).toLocaleString()} matching
        {/*
          Omitted entirely rather than shown as 0 ms while a fetch is still
          running or after one was stopped: the number is how long the fetch
          that produced these rows took, and there is no such number until
          one finishes.
        */}
        {fetchDurationMs !== undefined && ` in ${fetchDurationMs.toLocaleString()} ms`}
      </p>

      <div className="data-tab-grid" data-testid="message-grid">
        <AgGridReact<TopicMessage>
          theme={APP_GRID_THEME}
          rowData={messages}
          columnDefs={COLUMN_DEFS}
          defaultColDef={DEFAULT_COL_DEF}
          context={gridContext}
          getRowId={getRowId}
          rowSelection={ROW_SELECTION}
          onGridReady={handleGridReady}
          onSortChanged={handleSortChanged}
          onFilterChanged={handleFilterChanged}
          // The grid can only select a row it already holds, and rows arrive
          // after the effect that asks for the selection — a streamed fetch
          // paints them in over seconds, and a reopened tab hands the grid
          // its cached rows a beat after mounting.
          onRowDataUpdated={(event) => syncGridSelection(event.api, selectedRowId)}
          // Only until the first row lands, not for the whole fetch. The
          // backend streams each message to the grid as it polls it (see the
          // "messages-batch" listener above), but AG Grid's loading overlay
          // covers the rows it is streaming in — so the work was done and
          // then hidden, and the user waited for the whole fetch anyway.
          // Once rows exist they stay visible and keep arriving; the Stop
          // button and the growing "N loaded of M matching" count are what
          // show the fetch is still running.
          loading={isPlaying && messages.length === 0}
          overlayNoRowsTemplate="<span class='data-tab-no-rows'>No messages</span>"
          onRowClicked={(event) => {
            // AG Grid's row-click detection runs regardless of stopPropagation
            // on the Value column's "Fetch payload" button, so guard here
            // instead — otherwise clicking it also opens the viewer with
            // whatever (possibly payload-less) row data existed at click
            // time, racing the in-flight per-row fetch.
            const target = event.event?.target;
            if (target instanceof HTMLElement && target.closest("button")) return;
            if (event.data) viewMessage(event.data, connectionId, topicName, partitionId);
          }}
        />
      </div>
    </div>
  );
}
