import { beforeEach, describe, expect, it, vi } from "vitest";
import { act, fireEvent, render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import userEvent from "@testing-library/user-event";
import { useLogsStore } from "./useLogsStore";
import { BottomPanel, formatTabMemory } from "./BottomPanel";
import { useTabsStore } from "../tabs/useTabsStore";
import { useWorkspaceSelectionStore } from "../workspace/useWorkspaceSelectionStore";
import { useMessageViewerStore } from "../workspace/useMessageViewerStore";
import { dataTabCacheKey, useTabDataStore } from "../workspace/useTabDataStore";
import { retainedPayloadBytes, retainedRowBytes } from "../connections/payloadDecoding";
import { TopicMessage } from "../../lib/tauri";
import { useThemeStore } from "../theme/useThemeStore";
import { DEFAULT_THEME_ID } from "../theme/themes";

/** The panel reads the payload viewer's open message out of the query cache, so it needs a client. */
function renderPanel() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <BottomPanel />
    </QueryClientProvider>,
  );
}

let capturedHandler: ((event: { payload: unknown }) => void) | null = null;

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));

vi.mock("@tauri-apps/api/event", () => ({
  listen: vi.fn((_event: string, handler: (event: { payload: unknown }) => void) => {
    capturedHandler = handler;
    return Promise.resolve(() => {});
  }),
}));

beforeEach(() => {
  localStorage.clear();
  useLogsStore.setState({ entries: [], isExpanded: false });
  useTabsStore.setState({ tabs: [], activeTabId: null, error: null });
  useWorkspaceSelectionStore.setState({ selection: null, activeTabId: null, byTab: {} });
  useMessageViewerStore.setState({ message: null, activeTabId: null, byTab: {} });
  useTabDataStore.setState({ messagesByTab: {}, totalMatchingByTab: {}, fetchDurationMsByTab: {}, payloadBytesByTab: {}, lastUsedByTab: {}, evictedTabs: {} });
  capturedHandler = null;
});

describe("BottomPanel logs tool", () => {
  it("is collapsed by default", () => {
    renderPanel();
    expect(screen.queryByText("No log entries yet.")).not.toBeInTheDocument();
  });

  it("expands when the toggle icon is clicked", async () => {
    const user = userEvent.setup();
    renderPanel();

    await user.click(screen.getByLabelText("Toggle logs panel"));

    expect(screen.getByText("No log entries yet.")).toBeInTheDocument();
  });

  it("collapses again on a second click", async () => {
    const user = userEvent.setup();
    renderPanel();

    await user.click(screen.getByLabelText("Toggle logs panel"));
    await user.click(screen.getByLabelText("Toggle logs panel"));

    expect(screen.queryByText("No log entries yet.")).not.toBeInTheDocument();
  });

  it("renders a log entry pushed over the tauri event channel once expanded", async () => {
    const user = userEvent.setup();
    renderPanel();
    await user.click(screen.getByLabelText("Toggle logs panel"));

    await vi.waitFor(() => expect(capturedHandler).not.toBeNull());
    capturedHandler!({
      payload: {
        timestamp: "2026-08-18T00:00:00Z",
        level: "info",
        message: 'Created connection "Local Kafka"',
      },
    });

    expect(await screen.findByText('Created connection "Local Kafka"')).toBeInTheDocument();
  });
});

describe("formatTabMemory", () => {
  it("formats a byte count as megabytes with two decimal places", () => {
    expect(formatTabMemory(0)).toBe("0.00 MB");
    expect(formatTabMemory(1024 * 1024)).toBe("1.00 MB");
    expect(formatTabMemory(512 * 1024)).toBe("0.50 MB");
  });
});

describe("BottomPanel tab memory", () => {
  it("shows 0.00 MB when the active tab has nothing cached", () => {
    renderPanel();
    expect(screen.getByText("Tab memory: 0.00 MB")).toBeInTheDocument();
  });

  it("reflects the active tab's cached Data tab rows as a byte-size estimate", () => {
    const cached = [{ partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] }];
    useTabsStore.setState({ activeTabId: "tab-1" });
    useTabDataStore.setState({ messagesByTab: { [dataTabCacheKey("tab-1", "1", "orders")]: cached } });

    renderPanel();

    expect(screen.getByText(`Tab memory: ${formatTabMemory(retainedRowBytes(cached))}`)).toBeInTheDocument();
  });

  it("sums every topic (and partition) the active tab has ever fetched, regardless of which one is currently selected", () => {
    useTabsStore.setState({ activeTabId: "tab-1" });
    useWorkspaceSelectionStore.setState({
      selection: { type: "topic", connectionId: "1", topicName: "orders" },
    });
    const orders = [{ partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] }];
    const payments = [
      { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] },
      { partition: 0, offset: 2, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] },
    ];
    const partitionZero = [{ partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] }];
    useTabDataStore.setState({
      messagesByTab: {
        [dataTabCacheKey("tab-1", "1", "orders")]: orders,
        [dataTabCacheKey("tab-1", "1", "payments")]: payments,
        [dataTabCacheKey("tab-1", "1", "orders", 0)]: partitionZero,
      },
    });

    renderPanel();

    const expectedBytes = retainedRowBytes([...orders, ...payments, ...partitionZero]);
    expect(screen.getByText(`Tab memory: ${formatTabMemory(expectedBytes)}`)).toBeInTheDocument();
  });

  it("does not switch to 0.00 MB when navigating away from the topic that was viewed when it was fetched", () => {
    useTabsStore.setState({ activeTabId: "tab-1" });
    const cached = [{ partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] }];
    useTabDataStore.setState({ messagesByTab: { [dataTabCacheKey("tab-1", "1", "orders")]: cached } });
    // No selection at all — as if the user switched to a different topic
    // (or nothing) after fetching "orders". The old, per-selection
    // implementation would show 0.00 MB here; the fix must not.
    useWorkspaceSelectionStore.setState({ selection: null });

    renderPanel();

    expect(screen.getByText(`Tab memory: ${formatTabMemory(retainedRowBytes(cached))}`)).toBeInTheDocument();
  });

  it("only reflects the active tab's own cached data, not another tab's", () => {
    useTabsStore.setState({ activeTabId: "tab-1" });
    useTabDataStore.setState({
      messagesByTab: {
        [dataTabCacheKey("tab-2", "1", "orders")]: [
          { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] },
        ],
      },
    });

    renderPanel();

    expect(screen.getByText("Tab memory: 0.00 MB")).toBeInTheDocument();
  });

  it("clears every cached topic/partition for the active tab, plus its selection and message viewer, when Clear memory is clicked", async () => {
    useTabsStore.setState({ activeTabId: "tab-1" });
    useWorkspaceSelectionStore.setState({
      activeTabId: "tab-1",
      selection: { type: "topic", connectionId: "1", topicName: "orders" },
      byTab: { "tab-1": { type: "topic", connectionId: "1", topicName: "orders" } },
    });
    useMessageViewerStore.setState({
      activeTabId: "tab-1",
      message: { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] },
      byTab: {
        "tab-1": {
          message: { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] },
          connectionId: "1",
          topic: "orders",
        },
      },
    });
    const ordersKey = dataTabCacheKey("tab-1", "1", "orders");
    const paymentsKey = dataTabCacheKey("tab-1", "1", "payments");
    const otherTabKey = dataTabCacheKey("tab-2", "1", "orders");
    useTabDataStore.setState({
      messagesByTab: {
        [ordersKey]: [{ partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] }],
        [paymentsKey]: [{ partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] }],
        [otherTabKey]: [{ partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] }],
      },
    });
    const user = userEvent.setup();
    renderPanel();

    await user.click(screen.getByLabelText("Clear tab memory"));

    expect(screen.getByText("Tab memory: 0.00 MB")).toBeInTheDocument();
    expect(useMessageViewerStore.getState().message).toBeNull();
    expect(useTabDataStore.getState().messagesByTab[ordersKey]).toBeUndefined();
    expect(useTabDataStore.getState().messagesByTab[paymentsKey]).toBeUndefined();
    expect(useTabDataStore.getState().messagesByTab[otherTabKey]).toBeDefined();
  });

  // Both figures only ever disagreed with reality on a tab holding payloads:
  // with none there was nothing for the double count below to double, and
  // nothing for the units to differ over.
  describe("with payloads fetched", () => {
    const withPayload = (offset: number, bytes: number): TopicMessage => ({
      partition: 0,
      offset,
      timestampMs: null,
      keyBase64: null,
      payloadBase64: btoa("a".repeat(bytes)),
      payloadSizeBytes: bytes,
      headers: [],
    });

    // Clicking a grid row hands the viewer the very object the row cache
    // holds — one message in memory, not two — so adding its size on top of
    // the cache's was counting the selected row twice. Invisible until the
    // rows carried payloads, and then worth up to a whole preview per tab.
    it("does not count the selected row a second time", () => {
      const selected = withPayload(1, 512 * 1024);
      const cached = [withPayload(0, 512 * 1024), selected];
      useTabsStore.setState({ activeTabId: "tab-1" });
      useTabDataStore.setState({ messagesByTab: { [dataTabCacheKey("tab-1", "1", "orders")]: cached } });
      useMessageViewerStore.setState({
        activeTabId: "tab-1",
        message: selected,
        byTab: { "tab-1": { message: selected, connectionId: "1", topic: "orders" } },
      });

      renderPanel();

      expect(screen.getByText(`Tab memory: ${formatTabMemory(retainedRowBytes(cached))}`)).toBeInTheDocument();
    });

    // ...but a message the viewer is still showing after its rows went (an
    // eviction, another connection's disconnect) is a copy nothing else
    // holds, and dropping it from the figure understates the tab.
    it("still counts a viewed message whose cached rows have gone", () => {
      const orphan = withPayload(1, 256 * 1024);
      useTabsStore.setState({ activeTabId: "tab-1" });
      useMessageViewerStore.setState({
        activeTabId: "tab-1",
        message: orphan,
        byTab: { "tab-1": { message: orphan, connectionId: "1", topic: "orders" } },
      });

      renderPanel();

      expect(screen.getByText(`Tab memory: ${formatTabMemory(retainedRowBytes([orphan]))}`)).toBeInTheDocument();
    });

    // The payload viewer's copy is the one payload carried whole rather than
    // truncated — the largest single thing the tab holds, and the app-wide
    // figure has counted it since v0.53.0 while this one did not.
    it("counts the payload viewer's open message", () => {
      useTabsStore.setState({ activeTabId: "tab-1" });
      const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
      client.setQueryData(["full-payload", "1", "orders", 0, 7], {
        messages: [withPayload(7, 2 * 1024 * 1024)],
        totalMatching: 1,
      });

      render(
        <QueryClientProvider client={client}>
          <BottomPanel />
        </QueryClientProvider>,
      );

      expect(screen.getByText("Tab memory: 2.00 MB")).toBeInTheDocument();
    });

    // The two figures describe the same rows, so on a single tab holding
    // nothing else they have to be the same number. Sized by
    // `JSON.stringify`, "Tab memory" counted a payload's base64 characters
    // and the ceiling counted the bytes they carry: a third apart, on every
    // fetch that brought payloads back.
    it("agrees with the app-wide payload figure about the same rows", () => {
      const cached = [withPayload(0, 1024 * 1024)];
      const key = dataTabCacheKey("tab-1", "1", "orders");
      useTabsStore.setState({ activeTabId: "tab-1" });
      useTabDataStore.setState({
        messagesByTab: { [key]: cached },
        payloadBytesByTab: { [key]: retainedPayloadBytes(cached) },
      });

      renderPanel();

      expect(screen.getByText("Tab memory: 1.00 MB")).toBeInTheDocument();
      expect(screen.getByText(/Payloads \(all tabs\): 1\.00 MB/)).toBeInTheDocument();
    });
  });

  it("asks the backend to trim the OS-visible working set when Clear memory is clicked", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    const user = userEvent.setup();
    renderPanel();

    await user.click(screen.getByLabelText("Clear tab memory"));

    expect(invoke).toHaveBeenCalledWith("trim_process_memory", undefined);
  });

  // The backend keeps up to 1 GiB of compressed payloads that no tab's
  // "memory" figure shows; Clear memory is the user's way to release it.
  it("empties the backend's payload cache, and only then trims, when Clear memory is clicked", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    vi.mocked(invoke).mockClear();
    const user = userEvent.setup();
    renderPanel();

    await user.click(screen.getByLabelText("Clear tab memory"));

    const calls = vi.mocked(invoke).mock.calls.map(([command]) => command);
    expect(calls).toContain("clear_payload_cache");
    expect(calls.indexOf("clear_payload_cache")).toBeLessThan(calls.indexOf("trim_process_memory"));
  });

  it("still trims the working set when the cache could not be cleared", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    vi.mocked(invoke).mockClear();
    vi.mocked(invoke).mockImplementation(async (command: string) => {
      if (command === "clear_payload_cache") throw new Error("backend unavailable");
      return undefined;
    });
    const user = userEvent.setup();
    renderPanel();

    await user.click(screen.getByLabelText("Clear tab memory"));

    expect(invoke).toHaveBeenCalledWith("trim_process_memory", undefined);
    vi.mocked(invoke).mockReset();
  });
  // The counter's job is to be the number the ceiling is enforced against, so
  // the one payload carried whole rather than truncated must not be missing
  // from it.
  it("includes the payload viewer's open message in the app-wide total", async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    client.setQueryData(["full-payload", "1", "orders", 0, 7], {
      messages: [
        {
          partition: 0,
          offset: 7,
          timestampMs: null,
          keyBase64: null,
          payloadBase64: btoa("a".repeat(2 * 1024 * 1024)),
          payloadSizeBytes: 2 * 1024 * 1024,
          headers: [],
        },
      ],
      totalMatching: 1,
    });

    render(
      <QueryClientProvider client={client}>
        <BottomPanel />
      </QueryClientProvider>,
    );

    expect(screen.getByText(/Payloads \(all tabs\): 2\.00 MB/)).toBeInTheDocument();
  });

  it("counts tab rows and the open payload together", async () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    useTabDataStore.setState({ payloadBytesByTab: { "tab-1:1:orders:all": 1024 * 1024 } });
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    client.setQueryData(["full-payload", "1", "orders", 0, 7], {
      messages: [
        {
          partition: 0,
          offset: 7,
          timestampMs: null,
          keyBase64: null,
          payloadBase64: btoa("a".repeat(1024 * 1024)),
          payloadSizeBytes: 1024 * 1024,
          headers: [],
        },
      ],
      totalMatching: 1,
    });

    render(
      <QueryClientProvider client={client}>
        <BottomPanel />
      </QueryClientProvider>,
    );

    expect(screen.getByText(/Payloads \(all tabs\): 2\.00 MB/)).toBeInTheDocument();
  });
});

describe("BottomPanel logs panel sizing", () => {
  function pointerEventAt(type: string, clientY: number): Event {
    const event = new Event(type, { bubbles: true });
    Object.defineProperty(event, "clientY", { value: clientY });
    return event;
  }

  it("has no resize handle while the panel is collapsed", () => {
    renderPanel();
    expect(screen.queryByLabelText("Resize logs panel")).not.toBeInTheDocument();
  });

  it("grows the logs panel when its top edge is dragged upwards", async () => {
    const user = userEvent.setup();
    renderPanel();
    await user.click(screen.getByLabelText("Toggle logs panel"));

    const handle = screen.getByLabelText("Resize logs panel");
    act(() => {
      handle.dispatchEvent(pointerEventAt("pointerdown", 500));
    });
    act(() => {
      window.dispatchEvent(pointerEventAt("pointermove", 400));
    });

    expect(screen.getByTestId("logs-panel-content")).toHaveStyle({ height: "260px" });
  });

  it("restores the default height on a double-click of the handle", async () => {
    const user = userEvent.setup();
    renderPanel();
    await user.click(screen.getByLabelText("Toggle logs panel"));

    const handle = screen.getByLabelText("Resize logs panel");
    act(() => {
      handle.dispatchEvent(pointerEventAt("pointerdown", 500));
    });
    act(() => {
      window.dispatchEvent(pointerEventAt("pointermove", 400));
    });
    await user.dblClick(handle);

    expect(screen.getByTestId("logs-panel-content")).toHaveStyle({ height: "160px" });
  });
});

describe("BottomPanel logs context menu", () => {
  it("clears every log entry from the right-click menu", async () => {
    const user = userEvent.setup();
    useLogsStore.setState({
      entries: [{ timestamp: "12:00:00", level: "info", message: "Connecting finished" }],
      isExpanded: true,
    });
    renderPanel();
    expect(screen.getByText("Connecting finished")).toBeInTheDocument();

    fireEvent.contextMenu(screen.getByText("Connecting finished"));
    await user.click(screen.getByRole("menuitem", { name: "Clear logs" }));

    expect(screen.queryByText("Connecting finished")).not.toBeInTheDocument();
    expect(screen.getByText("No log entries yet.")).toBeInTheDocument();
  });

  it("opens the same menu from the panel's empty area, with Clear logs disabled when there is nothing to clear", async () => {
    useLogsStore.setState({ entries: [], isExpanded: true });
    renderPanel();

    fireEvent.contextMenu(screen.getByText("No log entries yet."));

    expect(screen.getByRole("menuitem", { name: "Clear logs" })).toHaveAttribute("aria-disabled", "true");
  });

  it("closes the menu without clearing when Escape is pressed", async () => {
    const user = userEvent.setup();
    useLogsStore.setState({
      entries: [{ timestamp: "12:00:00", level: "info", message: "Connecting finished" }],
      isExpanded: true,
    });
    renderPanel();

    fireEvent.contextMenu(screen.getByText("Connecting finished"));
    await user.keyboard("{Escape}");

    expect(screen.queryByRole("menuitem", { name: "Clear logs" })).not.toBeInTheDocument();
    expect(screen.getByText("Connecting finished")).toBeInTheDocument();
  });
});

describe("BottomPanel theme control", () => {
  it("puts a theme button in the status strip", () => {
    const { container } = renderPanel();

    const button = screen.getByRole("button", { name: /^Theme:/ });
    expect(button).toBeInTheDocument();
    expect(button.closest(".bottom-panel-status-strip")).toBe(container.querySelector(".bottom-panel-status-strip"));
  });

  // Changing theme is a look-at-it-and-decide action. From the strip the app
  // stays on screen while the picker is open, which opening Settings did not
  // allow — its tab covered the very thing being judged.
  it("changes the theme without leaving the current tab", async () => {
    const user = userEvent.setup();
    useThemeStore.setState({ appliedThemeId: DEFAULT_THEME_ID, lastByKind: {} });
    renderPanel();

    await user.click(screen.getByRole("button", { name: /^Theme:/ }));
    await user.click(screen.getByRole("radio", { name: "One Dark" }));

    expect(useThemeStore.getState().appliedThemeId).toBe("one-dark");
  });
});
