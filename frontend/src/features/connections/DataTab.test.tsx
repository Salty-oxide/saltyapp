import { beforeEach, describe, expect, it, vi } from "vitest";
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { setInvokeHandlers } from "../../lib/testInvoke";
import { MessageFilter } from "../../lib/tauri";
import { useMessageViewerStore } from "../workspace/useMessageViewerStore";
import { useTabDataStore } from "../workspace/useTabDataStore";
import { useTabsStore } from "../tabs/useTabsStore";
import { useGeneralSettingsStore } from "../settings/useGeneralSettingsStore";
import { useDataTabFiltersStore } from "./useDataTabFiltersStore";
import { useDataTabGridStateStore } from "./useDataTabGridStateStore";
import { MAX_INLINE_PAYLOAD_BYTES, VALUE_PREVIEW_BYTES } from "./payloadDecoding";
import { useLogsStore } from "../bottom-panel/useLogsStore";
import { DataTab } from "./DataTab";

/**
 * The "N loaded of M matching" line, with its trailing " in N ms" stripped.
 *
 * The timing is wall clock, so it is different on every run and cannot be
 * asserted literally — but it is part of the same paragraph's text, which is
 * what `getByText` matches against. Stripping it here keeps every count
 * assertion exact instead of loosening them all to a prefix match.
 */
function countLine(): string {
  const line = document.querySelector(".data-tab-total-count");
  return (line?.textContent ?? "").replace(/ in [\d,]+ ms$/, "");
}


vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));

let capturedMessagesBatchHandler:
  | ((event: { payload: { requestId: string; messages: unknown[] } }) => void)
  | null = null;
vi.mock("@tauri-apps/api/event", () => ({
  listen: vi.fn(
    (_event: string, handler: (event: { payload: { requestId: string; messages: unknown[] } }) => void) => {
      capturedMessagesBatchHandler = handler;
      return Promise.resolve(() => {});
    },
  ),
}));

// Real AG Grid needs DOM measurement (ResizeObserver etc.) jsdom doesn't
// fully provide; this test's job is to verify DataTab passes the right
// rowData/onRowClicked, not to exercise AG Grid's own rendering.
interface MockColDef {
  headerName?: string;
  field?: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  valueGetter?: (params: any) => string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  cellRenderer?: any;
}
interface MockGridProps {
  rowData: unknown[];
  onRowClicked: (event: { data: unknown; event?: { target: unknown } }) => void;
  overlayNoRowsTemplate?: string;
  columnDefs: MockColDef[];
  loading?: boolean;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  context?: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  getRowId?: (params: any) => string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  rowSelection?: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  onGridReady?: (event: { api: any }) => void;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  onSortChanged?: (event: { api: any }) => void;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  onFilterChanged?: (event: { api: any }) => void;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  onRowDataUpdated?: (event: { api: any }) => void;
}
let lastGridProps: MockGridProps | null = null;
vi.mock("ag-grid-react", () => ({
  AgGridReact: (props: MockGridProps) => {
    lastGridProps = props;
    return null;
  },
}));

interface MockRowNode {
  id: string;
  selected: boolean;
  isSelected: () => boolean;
  setSelected: (selected: boolean) => void;
}

/**
 * Just enough of AG Grid's `GridApi` for DataTab's selection and
 * sort/filter-restore code paths — the real grid can't run under jsdom (it
 * needs layout measurement), and these tests are about what DataTab asks the
 * grid to do, not about the grid doing it.
 */
function createMockGridApi(rowIds: string[] = []) {
  const nodes = new Map<string, MockRowNode>();
  for (const id of rowIds) {
    const node: MockRowNode = {
      id,
      selected: false,
      isSelected: () => node.selected,
      setSelected: (selected: boolean) => {
        node.selected = selected;
      },
    };
    nodes.set(id, node);
  }
  return {
    nodes,
    getRowNode: (id: string) => nodes.get(id),
    getSelectedNodes: () => [...nodes.values()].filter((node) => node.selected),
    selectedIds: () =>
      [...nodes.values()].filter((node) => node.selected).map((node) => node.id),
    applyColumnState: vi.fn(),
    setFilterModel: vi.fn(),
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    getColumnState: vi.fn((): any[] => []),
    getFilterModel: vi.fn(() => ({})),
  };
}

function renderWithClient(ui: React.ReactElement) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(<QueryClientProvider client={client}>{ui}</QueryClientProvider>);
}

// Resetting via a helper (rather than a bare `lastGridProps = null` inline)
// avoids TypeScript narrowing lastGridProps to exactly `null` for the rest
// of the enclosing test body.
function resetLastGridProps() {
  lastGridProps = null;
}

beforeEach(() => {
  vi.clearAllMocks();
  resetLastGridProps();
  capturedMessagesBatchHandler = null;
  useMessageViewerStore.setState({ message: null, connectionId: null, topic: null, partitionId: undefined, byTab: {} });
  useTabsStore.setState({ tabs: [], activeTabId: null, error: null });
  useTabDataStore.setState({ messagesByTab: {}, totalMatchingByTab: {}, fetchDurationMsByTab: {}, payloadBytesByTab: {}, lastUsedByTab: {}, evictedTabs: {} });
  useLogsStore.setState({ entries: [] });
  useGeneralSettingsStore.setState({ maxTotalFetchBytes: 536_870_912 });
  useDataTabFiltersStore.setState({ formByTab: {} });
  useDataTabGridStateStore.setState({ stateByTab: {} });
});

describe("DataTab", () => {
  it("renders Fetch and Stop controls, and all six filter inputs", () => {
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    expect(screen.getByRole("button", { name: "Fetch" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Stop" })).toBeInTheDocument();
    expect(screen.getByLabelText("Max messages per partition")).toBeInTheDocument();
    expect(screen.getByLabelText("Total max messages")).toBeInTheDocument();
    expect(screen.getByLabelText("Partition")).toBeInTheDocument();
    expect(screen.getByLabelText(/^From/)).toBeInTheDocument();
    expect(screen.getByLabelText(/^To(\s|$)/)).toBeInTheDocument();
    expect(screen.getByLabelText("Offset")).toBeInTheDocument();
  });

  it("pre-fills and disables the partition filter when partitionId is given", () => {
    renderWithClient(<DataTab connectionId="1" topicName="orders" partitionId={2} />);

    expect(screen.getByLabelText("Partition")).toHaveValue("2");
    expect(screen.getByLabelText("Partition")).toBeDisabled();
  });

  it("updates the partition filter when partitionId changes without the component remounting", () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    const { rerender } = render(
      <QueryClientProvider client={client}>
        <DataTab connectionId="1" topicName="orders" partitionId={0} />
      </QueryClientProvider>,
    );
    expect(screen.getByLabelText("Partition")).toHaveValue("0");

    rerender(
      <QueryClientProvider client={client}>
        <DataTab connectionId="1" topicName="orders" partitionId={1} />
      </QueryClientProvider>,
    );

    expect(screen.getByLabelText("Partition")).toHaveValue("1");
  });

  it("clears the filter fields when switching to a different topic without remounting", async () => {
    const user = userEvent.setup();
    const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    const { rerender } = render(
      <QueryClientProvider client={client}>
        <DataTab connectionId="1" topicName="orders" />
      </QueryClientProvider>,
    );
    await user.clear(screen.getByLabelText("Max messages per partition"));
    await user.type(screen.getByLabelText("Max messages per partition"), "5");

    rerender(
      <QueryClientProvider client={client}>
        <DataTab connectionId="1" topicName="order-created" />
      </QueryClientProvider>,
    );

    // A brand new topic's form starts at the default cap, not blank.
    expect(screen.getByLabelText("Max messages per partition")).toHaveValue("100");
  });

  it("keeps a topic's filter form intact when switching away to a different topic and back", async () => {
    const user = userEvent.setup();
    const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    const { rerender } = render(
      <QueryClientProvider client={client}>
        <DataTab connectionId="1" topicName="orders" />
      </QueryClientProvider>,
    );
    await user.clear(screen.getByLabelText("Max messages per partition"));
    await user.type(screen.getByLabelText("Max messages per partition"), "5");
    await user.type(screen.getByLabelText("Offset"), "100");

    rerender(
      <QueryClientProvider client={client}>
        <DataTab connectionId="1" topicName="order-created" />
      </QueryClientProvider>,
    );
    // A brand new topic's form starts at the default cap, not blank.
    expect(screen.getByLabelText("Max messages per partition")).toHaveValue("100");

    rerender(
      <QueryClientProvider client={client}>
        <DataTab connectionId="1" topicName="orders" />
      </QueryClientProvider>,
    );

    expect(screen.getByLabelText("Max messages per partition")).toHaveValue("5");
    expect(screen.getByLabelText("Offset")).toHaveValue("100");
  });

  it("passes the current partitionId to viewMessage when a row is clicked, so the viewer can tell a topic-wide Data tab apart from one of its partitions'", () => {
    renderWithClient(<DataTab connectionId="1" topicName="orders" partitionId={2} />);
    const message = { partition: 2, offset: 5, timestampMs: null, keyBase64: null, payloadBase64: "eA==" };

    lastGridProps?.onRowClicked({ data: message });

    expect(useMessageViewerStore.getState().partitionId).toBe(2);
  });

  it("leaves the partition filter blank and editable when partitionId is not given", () => {
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    expect(screen.getByLabelText("Partition")).toHaveValue("");
    expect(screen.getByLabelText("Partition")).toBeEnabled();
  });

  it("fetches with the prepopulated partition when partitionId is given and Fetch is clicked", async () => {
    const fetchMessages = vi.fn(() => ({ messages: [], totalMatching: 0 }));
    setInvokeHandlers({ connection_fetch_messages: fetchMessages });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" partitionId={2} />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));

    await waitFor(() =>
      expect(fetchMessages).toHaveBeenCalledWith(
        expect.objectContaining({ filter: expect.objectContaining({ partitions: [2] }) }),
      ),
    );
  });

  it("starts with Stop disabled, since nothing is playing yet", () => {
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    expect(screen.getByRole("button", { name: "Stop" })).toBeDisabled();
  });

  it("shows a 'Fetch message payload' checkbox above Fetch/Stop, unchecked by default", () => {
    const { container } = renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    const checkbox = screen.getByLabelText("Fetch message payload");
    expect(checkbox).not.toBeChecked();
    // Above, not below: the checkbox decides what Fetch will do, so it has
    // to be readable before the button is pressed rather than after.
    const controls = container.querySelector(".data-tab-controls") as HTMLElement;
    expect(checkbox.closest("label")!.compareDocumentPosition(controls)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    );
  });

  it("fetches messages with a default-capped, no-payload filter when Fetch is clicked with no filters touched", async () => {
    const fetchMessages = vi.fn(() => ({ messages: [], totalMatching: 0 }));
    setInvokeHandlers({ connection_fetch_messages: fetchMessages });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));

    await waitFor(() =>
      expect(fetchMessages).toHaveBeenCalledWith({
        id: "1",
        topic: "orders",
        filter: {
          partitions: null,
          maxMessagesPerPartition: 100,
          maxTotalMessages: 100,
          fromTimestampMs: null,
          toTimestampMs: null,
          offset: null,
          includePayload: false,
          maxPayloadPreviewBytes: MAX_INLINE_PAYLOAD_BYTES,
        },
        requestId: expect.any(String),
        readTimeoutMs: 10_000,
        maxMessageSizeBytes: 1_048_576,
        maxTotalPayloadBytes: 536_870_912,
        // The grid's own Fetch paints rows as they arrive.
        streamUpdates: true,
      }),
    );
  });

  it("rejects Fetch with a validation error when Max messages per partition is cleared to blank, without calling the backend", async () => {
    const fetchMessages = vi.fn(() => ({ messages: [], totalMatching: 0 }));
    setInvokeHandlers({ connection_fetch_messages: fetchMessages });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.clear(screen.getByLabelText("Max messages per partition"));
    await user.click(screen.getByRole("button", { name: "Fetch" }));

    expect(await screen.findByRole("alert")).toHaveTextContent('"Max messages per partition" is required');
    expect(fetchMessages).not.toHaveBeenCalled();
  });

  it("sets includePayload true when the checkbox is checked before Fetch is clicked", async () => {
    const fetchMessages = vi.fn(() => ({ messages: [], totalMatching: 0 }));
    setInvokeHandlers({ connection_fetch_messages: fetchMessages });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByLabelText("Fetch message payload"));
    await user.click(screen.getByRole("button", { name: "Fetch" }));

    await waitFor(() =>
      expect(fetchMessages).toHaveBeenCalledWith(
        expect.objectContaining({ filter: expect.objectContaining({ includePayload: true }) }),
      ),
    );
  });

  it("applies entered filters when Fetch is clicked", async () => {
    const fetchMessages = vi.fn(() => ({ messages: [], totalMatching: 0 }));
    setInvokeHandlers({ connection_fetch_messages: fetchMessages });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.clear(screen.getByLabelText("Max messages per partition"));
    await user.type(screen.getByLabelText("Max messages per partition"), "10");
    await user.type(screen.getByLabelText("Partition"), "0,1");
    await user.click(screen.getByRole("button", { name: "Fetch" }));

    await waitFor(() =>
      expect(fetchMessages).toHaveBeenCalledWith(
        expect.objectContaining({
          filter: expect.objectContaining({ maxMessagesPerPartition: 10, partitions: [0, 1] }),
        }),
      ),
    );
  });

  it("applies the offset filter when Fetch is clicked, using the same default per-partition cap as any other fetch", async () => {
    const fetchMessages = vi.fn(() => ({ messages: [], totalMatching: 0 }));
    setInvokeHandlers({ connection_fetch_messages: fetchMessages });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.type(screen.getByLabelText("Offset"), "100");
    await user.click(screen.getByRole("button", { name: "Fetch" }));

    await waitFor(() =>
      expect(fetchMessages).toHaveBeenCalledWith(
        expect.objectContaining({
          filter: expect.objectContaining({ offset: 100, maxMessagesPerPartition: 100 }),
        }),
      ),
    );
  });

  it("respects an explicit Max messages per partition value when offset is also set", async () => {
    const fetchMessages = vi.fn(() => ({ messages: [], totalMatching: 0 }));
    setInvokeHandlers({ connection_fetch_messages: fetchMessages });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.type(screen.getByLabelText("Offset"), "100");
    await user.clear(screen.getByLabelText("Max messages per partition"));
    await user.type(screen.getByLabelText("Max messages per partition"), "20");
    await user.click(screen.getByRole("button", { name: "Fetch" }));

    await waitFor(() =>
      expect(fetchMessages).toHaveBeenCalledWith(
        expect.objectContaining({
          filter: expect.objectContaining({ offset: 100, maxMessagesPerPartition: 20 }),
        }),
      ),
    );
  });

  it("rejects Fetch with a validation error when To is before From, without calling the backend", async () => {
    const fetchMessages = vi.fn(() => ({ messages: [], totalMatching: 0 }));
    setInvokeHandlers({ connection_fetch_messages: fetchMessages });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    fireEvent.change(screen.getByLabelText(/^From/), { target: { value: "2026-01-02T00:00" } });
    fireEvent.change(screen.getByLabelText(/^To(\s|$)/), { target: { value: "2026-01-01T00:00" } });
    await user.click(screen.getByRole("button", { name: "Fetch" }));

    expect(await screen.findByRole("alert")).toHaveTextContent('"To" date must be after "From" date');
    expect(fetchMessages).not.toHaveBeenCalled();
  });

  it("rejects Fetch with a validation error when To equals From, without calling the backend", async () => {
    const fetchMessages = vi.fn(() => ({ messages: [], totalMatching: 0 }));
    setInvokeHandlers({ connection_fetch_messages: fetchMessages });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    fireEvent.change(screen.getByLabelText(/^From/), { target: { value: "2026-01-01T00:00" } });
    fireEvent.change(screen.getByLabelText(/^To(\s|$)/), { target: { value: "2026-01-01T00:00" } });
    await user.click(screen.getByRole("button", { name: "Fetch" }));

    expect(await screen.findByRole("alert")).toHaveTextContent('"To" date must be after "From" date');
    expect(fetchMessages).not.toHaveBeenCalled();
  });

  it("shows the grid's loading state while a fetch is in flight, and clears it once it resolves", async () => {
    let resolveFetch: (result: { messages: unknown[]; totalMatching: number }) => void = () => {};
    const pending = new Promise<{ messages: unknown[]; totalMatching: number }>((resolve) => {
      resolveFetch = resolve;
    });
    setInvokeHandlers({ connection_fetch_messages: () => pending });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    expect(lastGridProps?.loading).toBe(false);

    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(lastGridProps?.loading).toBe(true));

    resolveFetch({ messages: [], totalMatching: 0 });
    await waitFor(() => expect(lastGridProps?.loading).toBe(false));
  });

  it("streams a message onto the grid as soon as a matching messages-batch event arrives, ahead of the fetch resolving", async () => {
    let resolveFetch: (result: { messages: unknown[]; totalMatching: number }) => void = () => {};
    const pending = new Promise<{ messages: unknown[]; totalMatching: number }>((resolve) => {
      resolveFetch = resolve;
    });
    const fetchMessages = vi.fn((_args: { requestId: string }) => pending);
    setInvokeHandlers({ connection_fetch_messages: fetchMessages });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(fetchMessages).toHaveBeenCalled());
    const requestId = fetchMessages.mock.calls[0][0].requestId;

    const streamed = { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null };
    capturedMessagesBatchHandler?.({ payload: { requestId, messages: [streamed] } });

    await waitFor(() => expect(lastGridProps?.rowData).toEqual([streamed]));

    // Streaming the row into the grid is only half of it: AG Grid's loading
    // overlay covers the rows underneath it, so leaving it up for the whole
    // fetch hid every message that had already arrived and made the user
    // wait for the last one regardless.
    expect(lastGridProps?.loading).toBe(false);

    resolveFetch({ messages: [streamed], totalMatching: 1 });
    await waitFor(() => expect(lastGridProps?.loading).toBe(false));
  });

  /**
   * Streamed rows are buffered briefly before being written to the grid, so
   * Stop has to drop whatever is still sitting in that buffer — otherwise a
   * message that arrived just before the click lands in the grid just after
   * it, which is precisely the row the user said they no longer wanted.
   */
  it("drops a streamed message that was still buffered when Stop was clicked", async () => {
    const pending = new Promise<{ messages: unknown[]; totalMatching: number }>(() => {});
    const fetchMessages = vi.fn((_args: { requestId: string }) => pending);
    setInvokeHandlers({ connection_fetch_messages: fetchMessages });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(fetchMessages).toHaveBeenCalled());
    const requestId = fetchMessages.mock.calls[0][0].requestId;

    const streamed = { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null };
    capturedMessagesBatchHandler?.({ payload: { requestId, messages: [streamed] } });
    await user.click(screen.getByRole("button", { name: "Stop" }));

    await new Promise((resolve) => setTimeout(resolve, 250));
    expect(lastGridProps?.rowData).toEqual([]);
  });

  it("disables every filter input, and the payload checkbox, while a fetch is in progress", async () => {
    const pending = new Promise<{ messages: unknown[]; totalMatching: number }>(() => {});
    setInvokeHandlers({ connection_fetch_messages: () => pending });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(screen.getByLabelText("Max messages per partition")).toBeDisabled());

    expect(screen.getByLabelText("Total max messages")).toBeDisabled();
    expect(screen.getByLabelText("Partition")).toBeDisabled();
    expect(screen.getByLabelText("Offset")).toBeDisabled();
    expect(screen.getByLabelText(/^From/)).toBeDisabled();
    expect(screen.getByLabelText(/^To(\s|$)/)).toBeDisabled();
    expect(screen.getByLabelText("Fetch message payload")).toBeDisabled();
  });

  it("re-enables the filters once the fetch finishes", async () => {
    let resolveFetch!: (value: { messages: unknown[]; totalMatching: number }) => void;
    const pending = new Promise<{ messages: unknown[]; totalMatching: number }>((resolve) => {
      resolveFetch = resolve;
    });
    setInvokeHandlers({ connection_fetch_messages: () => pending });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(screen.getByLabelText("Offset")).toBeDisabled());

    resolveFetch({ messages: [], totalMatching: 0 });

    await waitFor(() => expect(screen.getByLabelText("Offset")).not.toBeDisabled());
  });

  it("tells the backend to cancel the in-flight request id when Stop is clicked", async () => {
    const pending = new Promise<{ messages: unknown[]; totalMatching: number }>(() => {});
    const fetchMessages = vi.fn((_args: { requestId: string }) => pending);
    const cancelFetch = vi.fn(() => null);
    setInvokeHandlers({ connection_fetch_messages: fetchMessages, connection_cancel_fetch: cancelFetch });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(fetchMessages).toHaveBeenCalled());
    const requestId = fetchMessages.mock.calls[0][0].requestId;

    await user.click(screen.getByRole("button", { name: "Stop" }));

    expect(cancelFetch).toHaveBeenCalledWith({ requestId });
  });

  /**
   * DataTab is reused (not remounted) across topics within a tab, so
   * switching topics mid-fetch would otherwise leave the old fetch running
   * against the broker for a tab the user has already left — exactly the
   * kind of needless broker load the connection breaker/pooling work this
   * session was about avoiding.
   */
  it("cancels the in-flight fetch and re-enables the filters when switching to a different topic mid-fetch", async () => {
    const pending = new Promise<{ messages: unknown[]; totalMatching: number }>(() => {});
    const fetchMessages = vi.fn((_args: { requestId: string }) => pending);
    const cancelFetch = vi.fn(() => null);
    setInvokeHandlers({ connection_fetch_messages: fetchMessages, connection_cancel_fetch: cancelFetch });
    const user = userEvent.setup();
    const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    const { rerender } = render(
      <QueryClientProvider client={client}>
        <DataTab connectionId="1" topicName="orders" />
      </QueryClientProvider>,
    );

    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(fetchMessages).toHaveBeenCalled());
    const requestId = fetchMessages.mock.calls[0][0].requestId;

    rerender(
      <QueryClientProvider client={client}>
        <DataTab connectionId="1" topicName="order-created" />
      </QueryClientProvider>,
    );

    expect(cancelFetch).toHaveBeenCalledWith({ requestId });
    expect(screen.getByRole("button", { name: "Fetch" })).not.toBeDisabled();
    expect(screen.getByRole("button", { name: "Stop" })).toBeDisabled();
    expect(screen.getByLabelText("Offset")).not.toBeDisabled();
  });

  /**
   * The topic-switch case above only covers a switch this component survives.
   * DataTab is unmounted outright by plenty of ordinary actions mid-fetch —
   * `TopicDetailPanel` renders it as `{activeTab === "data" && <DataTab/>}`,
   * so any other sub-tab drops it, as does selecting a broker, a partition or
   * a consumer group, switching top-level tab, or closing the tab. With no
   * unmount cleanup, every one of those left the backend polling the broker
   * for a fetch whose UI no longer existed and which nothing could ever stop.
   */
  it("cancels the in-flight fetch when the tab is unmounted mid-fetch", async () => {
    const pending = new Promise<{ messages: unknown[]; totalMatching: number }>(() => {});
    const fetchMessages = vi.fn((_args: { requestId: string }) => pending);
    const cancelFetch = vi.fn(() => null);
    setInvokeHandlers({ connection_fetch_messages: fetchMessages, connection_cancel_fetch: cancelFetch });
    const user = userEvent.setup();
    const { unmount } = renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(fetchMessages).toHaveBeenCalled());
    const requestId = fetchMessages.mock.calls[0][0].requestId;

    unmount();

    expect(cancelFetch).toHaveBeenCalledWith({ requestId });
  });

  it("does not cancel anything when unmounted without a fetch ever having run", () => {
    const cancelFetch = vi.fn(() => null);
    setInvokeHandlers({ connection_cancel_fetch: cancelFetch });
    const { unmount } = renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    unmount();

    expect(cancelFetch).not.toHaveBeenCalled();
  });

  it("keeps the loading overlay up while a fetch is running but has produced no rows yet", async () => {
    const pending = new Promise<{ messages: unknown[]; totalMatching: number }>(() => {});
    setInvokeHandlers({ connection_fetch_messages: () => pending });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));

    await waitFor(() => expect(lastGridProps?.loading).toBe(true));
  });

  it("ignores a messages-batch event from a different (stale/superseded) request id", async () => {
    setInvokeHandlers({ connection_fetch_messages: () => ({ messages: [], totalMatching: 0 }) });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(lastGridProps?.loading).toBe(false));

    const stale = { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null };
    capturedMessagesBatchHandler?.({ payload: { requestId: "some-other-request", messages: [stale] } });

    expect(lastGridProps?.rowData).toEqual([]);
  });

  it("tells the grid to show a 'No messages' overlay when there are no rows", () => {
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    expect(lastGridProps?.overlayNoRowsTemplate).toContain("No messages");
  });

  it("passes the fetched messages to the grid as rowData", async () => {
    const messages = [{ partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: "eA==" }];
    setInvokeHandlers({ connection_fetch_messages: () => ({ messages, totalMatching: messages.length }) });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));

    await waitFor(() => expect(lastGridProps?.rowData).toEqual(messages));
  });

  it("includes a Value column that decodes a row's base64 payload", async () => {
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    const valueColumn = lastGridProps?.columnDefs.find((c) => c.headerName === "Value");
    expect(valueColumn).toBeDefined();
    expect(valueColumn?.valueGetter?.({ data: { payloadBase64: "eyJhIjoxfQ==" } })).toBe('{"a":1}');
  });

  it("shows a blank Value for rows with no payload loaded", async () => {
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    const valueColumn = lastGridProps?.columnDefs.find((c) => c.headerName === "Value");
    expect(valueColumn?.valueGetter?.({ data: { payloadBase64: null } })).toBe("");
  });

  it("keeps the fetched messages cached for the tab across an unmount/remount (switching tabs away and back)", async () => {
    const messages = [{ partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: "eA==" }];
    setInvokeHandlers({ connection_fetch_messages: () => ({ messages, totalMatching: messages.length }) });
    const user = userEvent.setup();
    const { unmount } = renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(lastGridProps?.rowData).toEqual(messages));

    unmount();
    resetLastGridProps();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    expect(lastGridProps?.rowData).toEqual(messages);
  });

  it("does not leak one topic's cached rows into a different topic's Data tab in the same top-level tab", async () => {
    const messages = [{ partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: "eA==" }];
    setInvokeHandlers({ connection_fetch_messages: () => ({ messages, totalMatching: messages.length }) });
    const user = userEvent.setup();
    const { unmount } = renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(lastGridProps?.rowData).toEqual(messages));

    unmount();
    resetLastGridProps();
    renderWithClient(<DataTab connectionId="1" topicName="payments" />);

    expect(lastGridProps?.rowData).toEqual([]);
  });

  it("does not leak a topic's cached rows into one of its partitions' Data tab", async () => {
    const messages = [{ partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: "eA==" }];
    setInvokeHandlers({ connection_fetch_messages: () => ({ messages, totalMatching: messages.length }) });
    const user = userEvent.setup();
    const { unmount } = renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(lastGridProps?.rowData).toEqual(messages));

    unmount();
    resetLastGridProps();
    renderWithClient(<DataTab connectionId="1" topicName="orders" partitionId={0} />);

    expect(lastGridProps?.rowData).toEqual([]);
  });

  it("shows an error when fetching fails", async () => {
    setInvokeHandlers({
      connection_fetch_messages: () => {
        throw new Error("Failed to fetch messages");
      },
    });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("Failed to fetch messages");
  });

  it("shows nothing loaded before any fetch has run", () => {
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    expect(countLine()).toBe("0 loaded of 0 matching");
  });

  it("shows every loaded message as matching the total when the fetch wasn't capped", async () => {
    const messages = [
      { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] },
      { partition: 0, offset: 2, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] },
    ];
    setInvokeHandlers({ connection_fetch_messages: () => ({ messages, totalMatching: messages.length }) });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));

    await waitFor(() => expect(countLine()).toBe("2 loaded of 2 matching"));
  });

  /**
   * The raw line, timing included — `countLine` deliberately strips it, so
   * these are the tests that look at it.
   */
  function rawCountLine(): string {
    return document.querySelector(".data-tab-total-count")?.textContent ?? "";
  }

  it("reports how long the fetch took next to the loaded/matching count", async () => {
    const messages = [{ partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] }];
    setInvokeHandlers({ connection_fetch_messages: () => ({ messages, totalMatching: 8000 }) });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));

    await waitFor(() => expect(rawCountLine()).toMatch(/^1 loaded of 8,000 matching in [\d,]+ ms$/));
  });

  it("shows no timing before any fetch has run", () => {
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    expect(rawCountLine()).not.toMatch(/ms/);
  });

  /**
   * The timing describes the fetch that produced the rows on screen. A new
   * fetch clears the rows, so it has to clear the timing with them rather
   * than leave the previous fetch's duration sitting next to a count that
   * is climbing again.
   */
  it("drops the previous fetch's timing as soon as a new fetch starts", async () => {
    let resolveSecond: (result: { messages: unknown[]; totalMatching: number }) => void = () => {};
    const second = new Promise<{ messages: unknown[]; totalMatching: number }>((resolve) => {
      resolveSecond = resolve;
    });
    const fetchMessages = vi
      .fn()
      .mockResolvedValueOnce({
        messages: [
          { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] },
        ],
        totalMatching: 1,
      })
      .mockReturnValueOnce(second);
    setInvokeHandlers({ connection_fetch_messages: fetchMessages });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(rawCountLine()).toMatch(/ms$/));

    await user.click(screen.getByRole("button", { name: "Fetch" }));

    await waitFor(() => expect(rawCountLine()).toBe("0 loaded of 0 matching"));

    resolveSecond({ messages: [], totalMatching: 0 });
  });

  /**
   * Cached with the rows, not held in the component — the middle pane
   * remounts per tab, and the timing has to survive that alongside the
   * count it sits next to.
   */
  it("keeps the timing across an unmount/remount, like the count it sits next to", async () => {
    const messages = [{ partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] }];
    setInvokeHandlers({ connection_fetch_messages: () => ({ messages, totalMatching: 1 }) });
    const user = userEvent.setup();
    const { unmount } = renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(rawCountLine()).toMatch(/ms$/));
    const shown = rawCountLine();

    unmount();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    expect(rawCountLine()).toBe(shown);
  });

  it("shows fewer loaded than total matching when a max-messages cap trimmed the fetch, so the user knows more remain", async () => {
    const messages = [{ partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] }];
    setInvokeHandlers({ connection_fetch_messages: () => ({ messages, totalMatching: 150 }) });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));

    await waitFor(() => expect(countLine()).toBe("1 loaded of 150 matching"));
  });

  it("keeps showing the last fetch's loaded/total count for a tab across an unmount/remount", async () => {
    const messages = [{ partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] }];
    setInvokeHandlers({ connection_fetch_messages: () => ({ messages, totalMatching: 150 }) });
    const user = userEvent.setup();
    const { unmount } = renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(countLine()).toBe("1 loaded of 150 matching"));

    unmount();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    expect(countLine()).toBe("1 loaded of 150 matching");
  });

  it("resets the loaded/matching count when a new fetch starts, instead of keeping the previous fetch's stale total", async () => {
    let resolveSecond: (result: { messages: unknown[]; totalMatching: number }) => void = () => {};
    const second = new Promise<{ messages: unknown[]; totalMatching: number }>((resolve) => {
      resolveSecond = resolve;
    });
    const fetchMessages = vi
      .fn()
      .mockResolvedValueOnce({
        messages: [
          { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] },
        ],
        totalMatching: 6000,
      })
      .mockReturnValueOnce(second);
    setInvokeHandlers({ connection_fetch_messages: fetchMessages });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(countLine()).toBe("1 loaded of 6,000 matching"));

    await user.click(screen.getByRole("button", { name: "Fetch" }));

    // The second fetch hasn't resolved yet, but starting it should already
    // have cleared the first fetch's total rather than leaving "6000"
    // displayed next to a grid that's about to show a different result.
    await waitFor(() => expect(countLine()).toBe("0 loaded of 0 matching"));

    resolveSecond({ messages: [], totalMatching: 0 });
  });

  it("selects a message into the viewer store when a grid row is clicked", () => {
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    const message = { partition: 0, offset: 5, timestampMs: null, keyBase64: null, payloadBase64: "eA==" };

    lastGridProps?.onRowClicked({ data: message });

    expect(useMessageViewerStore.getState().message).toEqual(message);
    expect(useMessageViewerStore.getState().connectionId).toBe("1");
    expect(useMessageViewerStore.getState().topic).toBe("orders");
  });

  // The crash this guards: a 1,000-row fetch of multi-megabyte records shipped
  // ~4 GB of base64 across the IPC boundary (once per streamed message, then
  // again in the fetch result) to fill a grid that shows one line per row, and
  // the webview was killed holding two copies of it.
  it("asks the backend for a bounded payload preview rather than whole payloads", async () => {
    const fetchMessages = vi.fn((_args: { filter: MessageFilter }) => ({ messages: [], totalMatching: 0 }));
    setInvokeHandlers({ connection_fetch_messages: fetchMessages });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));

    await waitFor(() => expect(fetchMessages).toHaveBeenCalled());
    expect(fetchMessages.mock.calls[0][0].filter.maxPayloadPreviewBytes).toBe(MAX_INLINE_PAYLOAD_BYTES);
  });

  it("bounds the per-row payload fetch too, at the bound that lets the viewer open it without refetching", async () => {
    const initial = [
      { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: 9000 },
    ];
    setInvokeHandlers({ connection_fetch_messages: () => ({ messages: initial, totalMatching: 1 }) });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(lastGridProps?.rowData).toEqual(initial));

    const perRowFetch = vi.fn((_args: { filter: MessageFilter }) => ({ messages: [], totalMatching: 0 }));
    setInvokeHandlers({ connection_fetch_messages: perRowFetch });
    await lastGridProps?.context.fetchPayload(initial[0]);

    expect(perRowFetch.mock.calls[0][0].filter.maxPayloadPreviewBytes).toBe(MAX_INLINE_PAYLOAD_BYTES);
  });

  // Counts say nothing about size on a topic of large records, so a fetch can
  // stop for a reason the row count alone cannot express. Silently showing
  // fewer rows would read as "that's all there is".
  it("says so when the fetch stopped because it hit the byte budget", async () => {
    setInvokeHandlers({
      connection_fetch_messages: () => ({
        messages: [],
        totalMatching: 5000,
        stoppedAtByteBudget: true,
        payloadBytesRead: 536_870_912,
      }),
    });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));

    const notice = await screen.findByRole("status");
    expect(notice).toHaveTextContent(/512 MB/);
    expect(notice).toHaveClass("data-tab-notice--warning");
  });

  it("shows no byte-budget notice for a fetch that finished within it", async () => {
    setInvokeHandlers({
      connection_fetch_messages: () => ({ messages: [], totalMatching: 0, stoppedAtByteBudget: false }),
    });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(countLine()).toBe("0 loaded of 0 matching"));

    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });

  it("clears a previous fetch's byte-budget notice when Fetch runs again", async () => {
    setInvokeHandlers({
      connection_fetch_messages: () => ({
        messages: [],
        totalMatching: 5000,
        stoppedAtByteBudget: true,
        payloadBytesRead: 536_870_912,
      }),
    });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await screen.findByRole("status");

    setInvokeHandlers({
      connection_fetch_messages: () => ({ messages: [], totalMatching: 0, stoppedAtByteBudget: false }),
    });
    await user.click(screen.getByRole("button", { name: "Fetch" }));

    await waitFor(() => expect(screen.queryByRole("status")).not.toBeInTheDocument());
  });

  it("clears the viewed message when Fetch runs again, so the right panel doesn't keep showing a row from a superseded fetch", async () => {
    let resolveFetch: (result: { messages: unknown[]; totalMatching: number }) => void = () => {};
    const pending = new Promise<{ messages: unknown[]; totalMatching: number }>((resolve) => {
      resolveFetch = resolve;
    });
    setInvokeHandlers({ connection_fetch_messages: () => pending });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    const message = { partition: 0, offset: 5, timestampMs: null, keyBase64: null, payloadBase64: "eA==" };
    lastGridProps?.onRowClicked({ data: message });
    expect(useMessageViewerStore.getState().message).toEqual(message);

    await user.click(screen.getByRole("button", { name: "Fetch" }));

    expect(useMessageViewerStore.getState().message).toBeNull();

    resolveFetch({ messages: [], totalMatching: 0 });
  });

  it("does not open the viewer when the row click originated from a button (e.g. Fetch payload)", () => {
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    const message = { partition: 0, offset: 5, timestampMs: null, keyBase64: null, payloadBase64: null };
    const button = document.createElement("button");

    lastGridProps?.onRowClicked({ data: message, event: { target: button } });

    expect(useMessageViewerStore.getState().message).toBeNull();
  });

  it("passes a context.fetchPayload that fetches just one row's payload and patches it into the cached rows", async () => {
    const initial = [
      { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null },
      { partition: 1, offset: 2, timestampMs: null, keyBase64: null, payloadBase64: null },
    ];
    setInvokeHandlers({ connection_fetch_messages: () => ({ messages: initial, totalMatching: initial.length }) });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(lastGridProps?.rowData).toEqual(initial));

    const fetchMessages = vi.fn(() => ({
      messages: [{ partition: 0, offset: 1, timestampMs: null, keyBase64: "k", payloadBase64: "eA==" }],
      totalMatching: 1,
    }));
    setInvokeHandlers({ connection_fetch_messages: fetchMessages });
    await lastGridProps?.context.fetchPayload(initial[0]);

    expect(fetchMessages).toHaveBeenCalledWith({
      id: "1",
      topic: "orders",
      filter: {
        partitions: [0],
        maxMessagesPerPartition: 1,
        maxTotalMessages: 1,
        fromTimestampMs: null,
        toTimestampMs: null,
        offset: 1,
        includePayload: true,
        maxPayloadPreviewBytes: MAX_INLINE_PAYLOAD_BYTES,
      },
      requestId: expect.any(String),
      readTimeoutMs: 10_000,
      maxMessageSizeBytes: 1_048_576,
      maxTotalPayloadBytes: 536_870_912,
      // A single-row fetch streams nothing: its events would only be
      // discarded by the listener above, which matches on the grid fetch's
      // request id.
      streamUpdates: false,
    });
    expect(lastGridProps?.rowData).toEqual([
      { partition: 0, offset: 1, timestampMs: null, keyBase64: "k", payloadBase64: "eA==" },
      initial[1],
    ]);
  });
  // --- Selected-row highlight ---------------------------------------------
  //
  // The grid's highlight is driven from the message viewer store rather than
  // from AG Grid's own click-to-select, so that the highlighted row and the
  // payload on the right can never disagree — including in the cases no
  // click produced: reopening a top-level tab, and the viewer's Close
  // button.

  it("gives the grid a row id built from partition and offset, so a selected row survives rowData being replaced", () => {
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    expect(lastGridProps?.getRowId?.({ data: { partition: 3, offset: 42 } })).toBe("3:42");
  });

  it("configures single-row selection with AG Grid's own click-selection off, leaving the store to decide what's selected", () => {
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    expect(lastGridProps?.rowSelection).toEqual({
      mode: "singleRow",
      checkboxes: false,
      enableClickSelection: false,
    });
  });

  it("highlights the row whose payload the right pane is showing when a row is clicked", async () => {
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    const gridApi = createMockGridApi(["0:1", "0:2"]);
    lastGridProps?.onGridReady?.({ api: gridApi });

    lastGridProps?.onRowClicked({
      data: { partition: 0, offset: 2, timestampMs: null, keyBase64: null, payloadBase64: "eA==" },
    });

    await waitFor(() => expect(gridApi.selectedIds()).toEqual(["0:2"]));
  });

  it("moves the highlight rather than accumulating it when a second row is clicked", async () => {
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    const gridApi = createMockGridApi(["0:1", "0:2"]);
    lastGridProps?.onGridReady?.({ api: gridApi });

    lastGridProps?.onRowClicked({ data: { partition: 0, offset: 1, payloadBase64: "eA==" } });
    await waitFor(() => expect(gridApi.selectedIds()).toEqual(["0:1"]));
    lastGridProps?.onRowClicked({ data: { partition: 0, offset: 2, payloadBase64: "eA==" } });

    await waitFor(() => expect(gridApi.selectedIds()).toEqual(["0:2"]));
  });

  it("clears the highlight when the right pane is closed, so no row is marked as being shown", async () => {
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    const gridApi = createMockGridApi(["0:1"]);
    lastGridProps?.onGridReady?.({ api: gridApi });
    lastGridProps?.onRowClicked({ data: { partition: 0, offset: 1, payloadBase64: "eA==" } });
    await waitFor(() => expect(gridApi.selectedIds()).toEqual(["0:1"]));

    act(() => useMessageViewerStore.getState().clear());

    await waitFor(() => expect(gridApi.selectedIds()).toEqual([]));
  });

  // Reopening a top-level tab remounts the grid with the tab's cached rows
  // and its cached viewed message. Without this, the right pane came back
  // showing a payload while the grid below it showed nothing selected.
  it("re-highlights the viewed message's row when a grid is created with one already open in the right pane", () => {
    useMessageViewerStore.setState({
      message: { partition: 0, offset: 7, timestampMs: null, keyBase64: null, payloadBase64: "eA==", payloadSizeBytes: 1, headers: [] },
      connectionId: "1",
      topic: "orders",
      partitionId: undefined,
    });
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    const gridApi = createMockGridApi(["0:7"]);

    lastGridProps?.onGridReady?.({ api: gridApi });

    expect(gridApi.selectedIds()).toEqual(["0:7"]);
  });

  // Rows stream in over the course of a fetch, so the row the right pane is
  // showing frequently doesn't exist yet at the moment the grid is created.
  it("highlights the viewed message's row once it arrives, not only if it was already there", () => {
    useMessageViewerStore.setState({
      message: { partition: 1, offset: 9, timestampMs: null, keyBase64: null, payloadBase64: "eA==", payloadSizeBytes: 1, headers: [] },
      connectionId: "1",
      topic: "orders",
      partitionId: undefined,
    });
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    const gridApi = createMockGridApi();
    lastGridProps?.onGridReady?.({ api: gridApi });
    expect(gridApi.selectedIds()).toEqual([]);

    const arrived = createMockGridApi(["1:9"]);
    lastGridProps?.onRowDataUpdated?.({ api: arrived });

    expect(arrived.selectedIds()).toEqual(["1:9"]);
  });

  // The right pane is shared across topics within a tab, and App.tsx clears
  // it on a topic switch — but the clear lands a render later. Until it does,
  // this grid must not highlight one of its own rows just because the offsets
  // happen to line up with a message from a different topic.
  it("highlights nothing when the right pane's message came from a different topic", () => {
    useMessageViewerStore.setState({
      message: { partition: 0, offset: 7, timestampMs: null, keyBase64: null, payloadBase64: "eA==", payloadSizeBytes: 1, headers: [] },
      connectionId: "1",
      topic: "order-created",
      partitionId: undefined,
    });
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    const gridApi = createMockGridApi(["0:7"]);

    lastGridProps?.onGridReady?.({ api: gridApi });

    expect(gridApi.selectedIds()).toEqual([]);
  });

  // --- Sort and column filters across a top-level tab switch ---------------

  it("saves the grid's sort order, in sort priority order, when the user sorts a column", () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    const gridApi = createMockGridApi();
    gridApi.getColumnState.mockReturnValue([
      { colId: "partition", sort: "asc", sortIndex: 1 },
      { colId: "offset", sort: null, sortIndex: null },
      { colId: "timestampMs", sort: "desc", sortIndex: 0 },
    ]);

    lastGridProps?.onSortChanged?.({ api: gridApi });

    expect(useDataTabGridStateStore.getState().stateByTab["tab-1:1:orders:all"]?.sortModel).toEqual([
      { colId: "timestampMs", sort: "desc" },
      { colId: "partition", sort: "asc" },
    ]);
  });

  it("saves the grid's column filters when the user filters a column", () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    const gridApi = createMockGridApi();
    gridApi.getFilterModel.mockReturnValue({ partition: { filterType: "number", type: "equals", filter: 2 } });

    lastGridProps?.onFilterChanged?.({ api: gridApi });

    expect(useDataTabGridStateStore.getState().stateByTab["tab-1:1:orders:all"]?.filterModel).toEqual({
      partition: { filterType: "number", type: "equals", filter: 2 },
    });
  });

  // Switching top-level tabs unmounts the middle pane outright (App.tsx keys
  // it by the active tab), so the grid that comes back is a brand new one —
  // it has to be told the arrangement the old one had.
  it("re-applies the saved sort and column filters to a grid created after a top-level tab switch", () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    useDataTabGridStateStore.setState({
      stateByTab: {
        "tab-1:1:orders:all": {
          sortModel: [{ colId: "offset", sort: "desc" }],
          filterModel: { partition: { filterType: "number", type: "equals", filter: 2 } },
        },
      },
    });
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    const gridApi = createMockGridApi();

    lastGridProps?.onGridReady?.({ api: gridApi });

    expect(gridApi.applyColumnState).toHaveBeenCalledWith({
      state: [{ colId: "offset", sort: "desc", sortIndex: 0 }],
      defaultState: { sort: null },
    });
    expect(gridApi.setFilterModel).toHaveBeenCalledWith({
      partition: { filterType: "number", type: "equals", filter: 2 },
    });
  });

  // Restoring an arrangement makes the grid emit the same sort/filter events
  // a user would — writing those back would be harmless here but pointless
  // churn, and it's the guard that keeps a restore from being able to
  // rewrite the thing it is restoring from.
  it("does not write the arrangement it is restoring back to the store as if the user had made it", () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    useDataTabGridStateStore.setState({
      stateByTab: {
        "tab-1:1:orders:all": {
          sortModel: [{ colId: "offset", sort: "desc" }],
          filterModel: {},
        },
      },
    });
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    const gridApi = createMockGridApi();
    // A grid mid-restore hasn't taken the new sort on yet, so asking it
    // reports the old (empty) one — exactly the value that would wipe the
    // saved sort if it were written back.
    gridApi.applyColumnState.mockImplementation(() => lastGridProps?.onSortChanged?.({ api: gridApi }));

    lastGridProps?.onGridReady?.({ api: gridApi });

    expect(useDataTabGridStateStore.getState().stateByTab["tab-1:1:orders:all"]?.sortModel).toEqual([
      { colId: "offset", sort: "desc" },
    ]);
  });

  // Switching topic within a tab keeps the same grid alive, so onGridReady
  // never fires again — the arrangement has to be swapped over explicitly.
  it("swaps the arrangement over when the tab switches to a different topic without recreating the grid", async () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    useDataTabGridStateStore.setState({
      stateByTab: {
        "tab-1:1:order-created:all": {
          sortModel: [{ colId: "partition", sort: "asc" }],
          filterModel: {},
        },
      },
    });
    const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    const { rerender } = render(
      <QueryClientProvider client={client}>
        <DataTab connectionId="1" topicName="orders" />
      </QueryClientProvider>,
    );
    const gridApi = createMockGridApi();
    lastGridProps?.onGridReady?.({ api: gridApi });
    gridApi.applyColumnState.mockClear();

    rerender(
      <QueryClientProvider client={client}>
        <DataTab connectionId="1" topicName="order-created" />
      </QueryClientProvider>,
    );

    await waitFor(() =>
      expect(gridApi.applyColumnState).toHaveBeenCalledWith({
        state: [{ colId: "partition", sort: "asc", sortIndex: 0 }],
        defaultState: { sort: null },
      }),
    );
  });
  // --- Max total fetch size accounting -------------------------------------
  //
  // The ceiling is app-wide (every tab shares one webview process) and is
  // enforced by evicting the coldest views, not by refusing new work —
  // refusing stops growth but frees nothing already held elsewhere. What is
  // counted is bytes *retained*, which on a large-message topic is orders of
  // magnitude below the bytes read.

  const TAB_KEY = "tab-1:1:orders:all";
  /** A row holding `retained` bytes of payload, from a message of `size` bytes on the broker. */
  const row = (size: number, retained: number | null) => ({
    partition: 0,
    offset: 1,
    timestampMs: null,
    keyBase64: null,
    payloadBase64: retained === null ? null : btoa("a".repeat(retained)),
    payloadSizeBytes: size,
    headers: [],
  });
  const held = (key = TAB_KEY) => useTabDataStore.getState().payloadBytesByTab[key];

  it("charges the tab nothing when a Fetch ran with the payload checkbox off", async () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    setInvokeHandlers({
      connection_fetch_messages: () => ({
        messages: [row(4_000_000, null)],
        totalMatching: 1,
        payloadBytesRead: 0,
      }),
    });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));

    await waitFor(() => expect(held()).toBe(0));
  });

  // The heart of the change: a browse of 4 MB records keeps only a bounded
  // slice of each, so charging it the 4 MB it read off the broker massively
  // over-counted what the webview was actually holding.
  it("charges what the rows retain, not what the fetch read off the broker", async () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    setInvokeHandlers({
      connection_fetch_messages: () => ({
        messages: [row(4_000_000, 4_096), row(4_000_000, 4_096)],
        totalMatching: 2,
        // What the old accounting used, and what the backend still reports.
        payloadBytesRead: 8_000_000,
      }),
    });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    await user.click(screen.getByLabelText("Fetch message payload"));

    await user.click(screen.getByRole("button", { name: "Fetch" }));

    await waitFor(() => expect(held()).toBe(8_192));
  });

  it("adds only the bytes a lazily fetched row actually keeps", async () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    setInvokeHandlers({
      connection_fetch_messages: () => ({ messages: [row(3_000_000, null)], totalMatching: 1, payloadBytesRead: 0 }),
    });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(held()).toBe(0));

    setInvokeHandlers({
      connection_fetch_messages: () => ({
        messages: [row(3_000_000, 2_048)],
        totalMatching: 1,
        payloadBytesRead: 3_000_000,
      }),
    });
    await lastGridProps?.context.fetchPayload(row(3_000_000, null));

    expect(held()).toBe(2_048);
  });

  // The button that runs this only appears on a row with no payload, so the
  // charge below is normally the whole of what arrives. It is a *replacement*
  // all the same: whatever the row was holding is dropped in the same
  // update, and charging the tab for the new payload without crediting the
  // old one counts bytes nobody holds.
  it("credits the preview a per-row fetch replaces rather than charging for both", async () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    setInvokeHandlers({
      connection_fetch_messages: () => ({
        messages: [row(3_000_000, 1_024)],
        totalMatching: 1,
        payloadBytesRead: 3_000_000,
      }),
    });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    await user.click(screen.getByLabelText("Fetch message payload"));
    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(held()).toBe(1_024));

    setInvokeHandlers({
      connection_fetch_messages: () => ({
        messages: [row(3_000_000, 4_096)],
        totalMatching: 1,
        payloadBytesRead: 3_000_000,
      }),
    });
    await lastGridProps?.context.fetchPayload(row(3_000_000, 1_024));

    expect(held()).toBe(4_096);
  });

  it("keeps adding across per-row fetches of different rows rather than replacing the total", async () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    const rowAt = (offset: number, retained: number | null) => ({ ...row(1_000_000, retained), offset });
    setInvokeHandlers({
      connection_fetch_messages: () => ({
        messages: [rowAt(1, null), rowAt(2, null), rowAt(3, null)],
        totalMatching: 3,
        payloadBytesRead: 0,
      }),
    });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(lastGridProps?.rowData).toHaveLength(3));

    setInvokeHandlers({
      connection_fetch_messages: (args: { filter: MessageFilter }) => ({
        messages: [rowAt(args.filter.offset!, 1_024)],
        totalMatching: 1,
        payloadBytesRead: 1_000_000,
      }),
    });
    await lastGridProps?.context.fetchPayload(rowAt(1, null));
    await lastGridProps?.context.fetchPayload(rowAt(2, null));
    await lastGridProps?.context.fetchPayload(rowAt(3, null));

    expect(held()).toBe(3_072);
  });

  // Eviction, not refusal. The click is one message; refusing it while the
  // app holds megabytes of colder rows in other tabs protects the wrong
  // thing, and frees nothing.
  it("evicts a colder view's rows rather than refusing a per-row payload fetch", async () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    const COLD = "tab-2:1:archive:all";
    useTabDataStore.setState({
      messagesByTab: { [COLD]: [row(9_000, 9_000)] },
      payloadBytesByTab: { [COLD]: 9_000 },
      lastUsedByTab: { [COLD]: 1 },
      totalMatchingByTab: {},
      evictedTabs: {},
    });
    useGeneralSettingsStore.setState({ maxTotalFetchBytes: 10_000 });
    setInvokeHandlers({
      connection_fetch_messages: () => ({ messages: [row(5_000, null)], totalMatching: 1, payloadBytesRead: 0 }),
    });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(lastGridProps?.rowData).toHaveLength(1));

    const perRowFetch = vi.fn(() => ({
      messages: [row(5_000, 5_000)],
      totalMatching: 1,
      payloadBytesRead: 5_000,
    }));
    setInvokeHandlers({ connection_fetch_messages: perRowFetch });
    await lastGridProps?.context.fetchPayload(row(5_000, null));

    // The click went through...
    expect(perRowFetch).toHaveBeenCalledTimes(1);
    expect(held()).toBe(5_000);
    // ...and the cold view in the other tab gave up its rows to fit.
    expect(held(COLD)).toBeUndefined();
    expect(useTabDataStore.getState().messagesByTab[COLD]).toBeUndefined();
    expect(useTabDataStore.getState().evictedTabs[COLD]).toBe(true);
    // ...and said so in the Logs panel, with how long the eviction took —
    // every action the panel reports carries its duration, so the log doubles
    // as a performance record.
    const evictionLog = useLogsStore.getState().entries.find((entry) => /Cleared \d+ cached/.test(entry.message));
    expect(evictionLog?.message).toMatch(/^Cleared 1 cached message view\(s\) to stay within Max total fetch size in \d+ ms$/);
  });

  it("never evicts the view being fetched into", async () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    useGeneralSettingsStore.setState({ maxTotalFetchBytes: 1_000 });
    setInvokeHandlers({
      connection_fetch_messages: () => ({
        messages: [row(9_000, 9_000)],
        totalMatching: 1,
        payloadBytesRead: 9_000,
      }),
    });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    await user.click(screen.getByLabelText("Fetch message payload"));

    await user.click(screen.getByRole("button", { name: "Fetch" }));

    // Over the limit with nothing else to drop: the fetch keeps its own rows
    // rather than discarding the results it just went and got.
    await waitFor(() => expect(held()).toBe(9_000));
    expect(lastGridProps?.rowData).toHaveLength(1);
  });

  it("says so when this view alone is over the whole limit", async () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    useGeneralSettingsStore.setState({ maxTotalFetchBytes: 1_048_576 });
    setInvokeHandlers({
      connection_fetch_messages: () => ({
        messages: [row(4_194_304, 4_194_304)],
        totalMatching: 1,
        payloadBytesRead: 4_194_304,
      }),
    });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    await user.click(screen.getByLabelText("Fetch message payload"));

    await user.click(screen.getByRole("button", { name: "Fetch" }));

    expect(await screen.findByText(/This view alone is holding/i)).toBeInTheDocument();
  });

  // Rows disappearing with the filters still set reads as a broken fetch
  // unless the tab says what happened.
  it("tells a view whose rows were evicted while the user was working elsewhere", async () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    useTabDataStore.setState({ evictedTabs: { [TAB_KEY]: true } });
    setInvokeHandlers({ connection_fetch_messages: () => ({ messages: [], totalMatching: 0 }) });

    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    expect(screen.getByText(/cleared while you were working elsewhere/i)).toBeInTheDocument();
  });

  it("drops the eviction notice once the view is fetched again", async () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    useTabDataStore.setState({ evictedTabs: { [TAB_KEY]: true } });
    setInvokeHandlers({
      connection_fetch_messages: () => ({ messages: [row(100, 100)], totalMatching: 1, payloadBytesRead: 100 }),
    });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("button", { name: "Fetch" }));

    await waitFor(() => expect(screen.queryByText(/cleared while you were working elsewhere/i)).not.toBeInTheDocument());
  });

  // The gap this closes: `handlePlay` only records the authoritative total on
  // success, so rows that had already streamed into the grid weighed nothing
  // as far as the ceiling was concerned if the fetch was Stopped or failed.
  // On a large fetch stopped near the end that was hundreds of megabytes the
  // app was holding and not counting.
  it("counts streamed rows as they arrive, so a Stopped fetch's rows are not held for free", async () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    let resolveFetch: (result: unknown) => void = () => {};
    const pending = new Promise((resolve) => {
      resolveFetch = resolve;
    });
    const fetchMessages = vi.fn((_args: { requestId: string }) => pending);
    setInvokeHandlers({ connection_fetch_messages: fetchMessages });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(fetchMessages).toHaveBeenCalled());
    const requestId = fetchMessages.mock.calls[0][0].requestId;

    capturedMessagesBatchHandler?.({ payload: { requestId, messages: [row(4_000, 4_000)] } });
    capturedMessagesBatchHandler?.({ payload: { requestId, messages: [row(4_000, 4_000)] } });
    await waitFor(() => expect(lastGridProps?.rowData).toHaveLength(2));

    // Still mid-fetch, and already counted.
    expect(held()).toBe(8_000);

    await user.click(screen.getByRole("button", { name: "Stop" }));

    // The rows are still on the grid, so they must still be on the books.
    expect(lastGridProps?.rowData).toHaveLength(2);
    expect(held()).toBe(8_000);
    resolveFetch({ messages: [], totalMatching: 0 });
  });

  // The response no longer repeats what the stream delivered — it carries
  // only the messages the stream did *not* — so a streamed row is written
  // once, by the stream, and counted once.
  it("counts a streamed row once, and does not drop it when the response carries no messages", async () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    let resolveFetch: (result: unknown) => void = () => {};
    const pending = new Promise((resolve) => {
      resolveFetch = resolve;
    });
    const fetchMessages = vi.fn((_args: { requestId: string }) => pending);
    setInvokeHandlers({ connection_fetch_messages: fetchMessages });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(fetchMessages).toHaveBeenCalled());
    const requestId = fetchMessages.mock.calls[0][0].requestId;
    const streamed = row(4_000, 4_000);
    capturedMessagesBatchHandler?.({ payload: { requestId, messages: [streamed] } });
    await waitFor(() => expect(held()).toBe(4_000));

    resolveFetch({ messages: [], totalMatching: 1, payloadBytesRead: 4_000 });

    // The same single row, counted once — not 8,000, and not zero.
    await waitFor(() => expect(countLine()).toBe("1 loaded of 1 matching"));
    expect(lastGridProps?.rowData).toHaveLength(1);
    expect(held()).toBe(4_000);
  });

  // The response settling inside the stream's own 100ms flush window is the
  // ordering the real backend produces: it emits its last "messages-batch"
  // events immediately before the command returns. The buffer used to be
  // *discarded* here, which was harmless only because the response repeated
  // every row. Now that it doesn't, those rows have to be flushed instead —
  // and still not duplicated.
  it("writes rows still buffered when the fetch resolves, exactly once", async () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    let resolveFetch: (result: unknown) => void = () => {};
    const pending = new Promise((resolve) => {
      resolveFetch = resolve;
    });
    const fetchMessages = vi.fn((_args: { requestId: string }) => pending);
    setInvokeHandlers({ connection_fetch_messages: fetchMessages });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(fetchMessages).toHaveBeenCalled());
    const requestId = fetchMessages.mock.calls[0][0].requestId;

    const streamed = row(4_000, 4_000);
    // Streamed and resolved inside the same flush window, so the buffer is
    // still pending when the fetch completes.
    capturedMessagesBatchHandler?.({ payload: { requestId, messages: [streamed] } });
    resolveFetch({ messages: [], totalMatching: 1, payloadBytesRead: 4_000 });

    await waitFor(() => expect(lastGridProps?.rowData).toHaveLength(1));
    // Long enough for a flush timer left running to have fired again.
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 250));
    });

    expect(lastGridProps?.rowData).toHaveLength(1);
    expect(held()).toBe(4_000);
    expect(countLine()).toBe("1 loaded of 1 matching");
  });

  // The response is not always empty: if the forwarding task ends early the
  // backend hands back exactly the messages it did not stream, and those
  // still belong on the grid.
  it("appends the messages the response says the stream did not deliver", async () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    let resolveFetch: (result: unknown) => void = () => {};
    const pending = new Promise((resolve) => {
      resolveFetch = resolve;
    });
    const fetchMessages = vi.fn((_args: { requestId: string }) => pending);
    setInvokeHandlers({ connection_fetch_messages: fetchMessages });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(fetchMessages).toHaveBeenCalled());
    const requestId = fetchMessages.mock.calls[0][0].requestId;

    capturedMessagesBatchHandler?.({ payload: { requestId, messages: [row(4_000, 4_000)] } });
    await waitFor(() => expect(lastGridProps?.rowData).toHaveLength(1));

    resolveFetch({ messages: [row(1_000, 1_000)], totalMatching: 2, payloadBytesRead: 5_000 });

    await waitFor(() => expect(lastGridProps?.rowData).toHaveLength(2));
    // Streamed row plus the tail, each counted once.
    expect(held()).toBe(5_000);
    expect(countLine()).toBe("2 loaded of 2 matching");
  });

  // One event now carries many rows — the whole point of batching them.
  it("writes every row in a batch, not just the first", async () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    const pending = new Promise(() => {});
    const fetchMessages = vi.fn((_args: { requestId: string }) => pending);
    setInvokeHandlers({ connection_fetch_messages: fetchMessages });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(fetchMessages).toHaveBeenCalled());
    const requestId = fetchMessages.mock.calls[0][0].requestId;

    capturedMessagesBatchHandler?.({
      payload: { requestId, messages: [row(1_000, 1_000), row(2_000, 2_000), row(3_000, 3_000)] },
    });

    await waitFor(() => expect(lastGridProps?.rowData).toHaveLength(3));
    expect(held()).toBe(6_000);
  });

  it("starts the view's total over when Fetch is run again", async () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    setInvokeHandlers({
      connection_fetch_messages: () => ({
        messages: [row(3_000, 3_000)],
        totalMatching: 1,
        payloadBytesRead: 3_000,
      }),
    });
    const user = userEvent.setup();
    renderWithClient(<DataTab connectionId="1" topicName="orders" />);
    await user.click(screen.getByLabelText("Fetch message payload"));
    await user.click(screen.getByRole("button", { name: "Fetch" }));
    await waitFor(() => expect(held()).toBe(3_000));

    setInvokeHandlers({
      connection_fetch_messages: () => ({ messages: [row(3_000, null)], totalMatching: 1, payloadBytesRead: 0 }),
    });
    await user.click(screen.getByLabelText("Fetch message payload"));
    await user.click(screen.getByRole("button", { name: "Fetch" }));

    await waitFor(() => expect(held()).toBe(0));
  });
});
