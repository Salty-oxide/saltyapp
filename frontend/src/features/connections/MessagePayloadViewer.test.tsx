import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { invoke } from "@tauri-apps/api/core";
import { setInvokeHandlers } from "../../lib/testInvoke";
import { useJsonViewerTabsStore } from "../tabs/useJsonViewerTabsStore";
import { useTabsStore } from "../tabs/useTabsStore";
import { useTabOrderStore } from "../tabs/useTabOrderStore";
import { useMessageViewerStore } from "../workspace/useMessageViewerStore";
import {
  DEFAULT_MESSAGE_VIEWER_PREFS,
  useMessageViewerPrefsStore,
} from "../workspace/useMessageViewerPrefsStore";
import { MessagePayloadViewer, TEXT_PREVIEW_CHARS } from "./MessagePayloadViewer";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));
const saveDialog = vi.hoisted(() => vi.fn());
vi.mock("@tauri-apps/plugin-dialog", () => ({ save: (...args: unknown[]) => saveDialog(...args) }));

function renderWithClient(ui: React.ReactElement) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(<QueryClientProvider client={client}>{ui}</QueryClientProvider>);
}

beforeEach(() => {
  useMessageViewerStore.setState({ message: null, connectionId: null, topic: null });
  useJsonViewerTabsStore.setState({ tabs: [] });
  useTabsStore.setState({ tabs: [], activeTabId: null, error: null });
  useTabOrderStore.setState({ anchors: {} });
  // `defaultValueMode` as well as `prefsByTab`: choosing a format now also
  // sets the app-wide default, so without this each case would start in
  // whatever format the previous one happened to select.
  localStorage.clear();
  saveDialog.mockReset();
  useMessageViewerPrefsStore.setState({
    prefsByTab: {},
    defaultValueMode: DEFAULT_MESSAGE_VIEWER_PREFS.valueMode,
    placement: "right",
  });
});

/**
 * Picks a format from the Value tab's dropdown — which replaced the row of
 * Text/JSON/Avro/XML buttons these tests used to click directly.
 */
async function selectFormat(user: ReturnType<typeof userEvent.setup>, label: string) {
  await user.click(screen.getByRole("button", { name: /^Value format:/ }));
  await user.click(screen.getByRole("option", { name: label }));
}

describe("MessagePayloadViewer", () => {
  it("shows a placeholder when no message is selected", () => {
    renderWithClient(<MessagePayloadViewer />);
    expect(screen.getByText(/select a message/i)).toBeInTheDocument();
  });

  it("keeps the header, panel tabs and mode buttons outside the scrolling payload region", async () => {
    const user = userEvent.setup();
    useMessageViewerStore.setState({
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("hello world"),
        payloadSizeBytes: null,
        headers: [{ key: "trace-id", valueBase64: btoa("abc") }],
      },
    });
    const { container } = renderWithClient(<MessagePayloadViewer />);

    // Only the decoded payload lives in the scroll region…
    expect(screen.getByText("hello world").closest(".message-payload-scroll")).not.toBeNull();
    // …the chrome the user needs while reading it does not.
    expect(container.querySelector(".message-payload-header")?.closest(".message-payload-scroll")).toBeNull();
    expect(screen.getByRole("tab", { name: "Value" }).closest(".message-payload-scroll")).toBeNull();
    expect(screen.getByRole("button", { name: /^Value format:/ }).closest(".message-payload-scroll")).toBeNull();

    // The Headers tab scrolls its rows the same way.
    await user.click(screen.getByRole("tab", { name: "Headers" }));
    expect(screen.getByText("trace-id").closest(".message-payload-scroll")).not.toBeNull();
  });

  it("shows the payload as text by default", () => {
    useMessageViewerStore.setState({
      message: { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: btoa("hello world"), payloadSizeBytes: null, headers: [] },
    });
    renderWithClient(<MessagePayloadViewer />);

    expect(screen.getByText("hello world")).toBeInTheDocument();
  });

  // The grid's rows deliberately carry only a bounded preview of each
  // payload — that truncation is what keeps a large fetch inside the
  // webview's memory. Opening a message therefore has to go and get the real
  // bytes, or the viewer would quietly show the first few KB of a multi-
  // megabyte message as though it were the whole thing.
  it("fetches the whole payload when the row only carried a truncated preview", async () => {
    setInvokeHandlers({
      connection_fetch_messages: () => ({
        messages: [
          {
            partition: 0,
            offset: 7,
            timestampMs: null,
            keyBase64: null,
            payloadBase64: btoa("the whole payload"),
            payloadSizeBytes: 17,
            headers: [],
          },
        ],
        totalMatching: 1,
      }),
    });
    useMessageViewerStore.setState({
      connectionId: "1",
      topic: "orders",
      message: {
        partition: 0,
        offset: 7,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("the whole"),
        payloadSizeBytes: 17,
        headers: [],
      },
    });

    renderWithClient(<MessagePayloadViewer />);

    expect(await screen.findByText("the whole payload")).toBeInTheDocument();
  });

  // Without this the preview is rendered as though it were the message: a
  // few KB of a multi-megabyte payload, indistinguishable from the whole
  // thing, until the real bytes quietly replace it.
  it("says the payload is still loading while only the preview is on screen", async () => {
    let resolveFetch: (result: unknown) => void = () => {};
    setInvokeHandlers({
      connection_fetch_messages: () => new Promise((resolve) => {
        resolveFetch = resolve;
      }),
    });
    useMessageViewerStore.setState({
      connectionId: "1",
      topic: "orders",
      message: {
        partition: 0,
        offset: 7,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("the whole"),
        payloadSizeBytes: 17,
        headers: [],
      },
    });

    renderWithClient(<MessagePayloadViewer />);

    expect(await screen.findByText(/Loading the full payload/)).toBeInTheDocument();

    resolveFetch({
      messages: [
        {
          partition: 0,
          offset: 7,
          timestampMs: null,
          keyBase64: null,
          payloadBase64: btoa("the whole payload"),
          payloadSizeBytes: 17,
          headers: [],
        },
      ],
      totalMatching: 1,
    });

    expect(await screen.findByText("the whole payload")).toBeInTheDocument();
    expect(screen.queryByText(/Loading the full payload/)).not.toBeInTheDocument();
  });

  // A truncated preview is not the message: parsing it reports "not valid
  // JSON" (and Avro decoding fails outright) for as long as the real bytes
  // take to arrive, which reads as the payload being broken rather than as
  // still loading. The structured views wait instead.
  it("shows a spinner rather than an invalid-JSON alert while the full payload is still loading", async () => {
    setInvokeHandlers({ connection_fetch_messages: () => new Promise(() => {}) });
    useMessageViewerStore.setState({
      connectionId: "1",
      topic: "orders",
      message: {
        partition: 0,
        offset: 7,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa('{"id":1,"nam'),
        payloadSizeBytes: 64,
        headers: [],
      },
    });
    const user = userEvent.setup();
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "JSON");

    expect(await screen.findByRole("status", { name: /loading the full payload/i })).toBeInTheDocument();
    expect(screen.queryByText(/not valid json/i)).not.toBeInTheDocument();
  });

  it("renders the JSON tree once the full payload arrives", async () => {
    let resolveFetch: (result: unknown) => void = () => {};
    setInvokeHandlers({
      connection_fetch_messages: () => new Promise((resolve) => {
        resolveFetch = resolve;
      }),
    });
    useMessageViewerStore.setState({
      connectionId: "1",
      topic: "orders",
      message: {
        partition: 0,
        offset: 7,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa('{"id":1,"nam'),
        payloadSizeBytes: 24,
        headers: [],
      },
    });
    const user = userEvent.setup();
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "JSON");
    resolveFetch({
      messages: [
        {
          partition: 0,
          offset: 7,
          timestampMs: null,
          keyBase64: null,
          payloadBase64: btoa('{"id":1,"name":"orders"}'),
          payloadSizeBytes: 24,
          headers: [],
        },
      ],
      totalMatching: 1,
    });

    expect(await screen.findByText("id:")).toBeInTheDocument();
    expect(screen.getByText('"orders"')).toBeInTheDocument();
  });

  // Decoding a cut Avro payload doesn't just look wrong, it costs a backend
  // round trip to be told so. The decode waits for the real bytes.
  it("shows a spinner and does not attempt an Avro decode while the full payload is still loading", async () => {
    const decodeAvro = vi.fn(() => ({ id: 1 }));
    setInvokeHandlers({
      connection_fetch_messages: () => new Promise(() => {}),
      connection_decode_avro: decodeAvro,
    });
    useMessageViewerStore.setState({
      connectionId: "1",
      topic: "orders",
      message: {
        partition: 0,
        offset: 7,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("avro-pre"),
        payloadSizeBytes: 64,
        headers: [],
      },
    });
    const user = userEvent.setup();
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "Avro");

    expect(await screen.findByRole("status", { name: /loading the full payload/i })).toBeInTheDocument();
    expect(decodeAvro).not.toHaveBeenCalled();
  });

  // The spinner means "still coming". A fetch that failed is not still
  // coming, and the error banner already says so, so the view goes back to
  // reporting what it actually has rather than spinning forever.
  it("stops spinning and reports the payload it has when the full-payload fetch fails", async () => {
    setInvokeHandlers({
      connection_fetch_messages: () => {
        throw new Error("broker unreachable");
      },
    });
    useMessageViewerStore.setState({
      connectionId: "1",
      topic: "orders",
      message: {
        partition: 0,
        offset: 7,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa('{"id":1,"nam'),
        payloadSizeBytes: 64,
        headers: [],
      },
    });
    const user = userEvent.setup();
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "JSON");

    expect(await screen.findByText(/not valid json/i)).toBeInTheDocument();
    expect(screen.queryByRole("status", { name: /loading the full payload/i })).not.toBeInTheDocument();
  });

  it("does not re-fetch a payload the row already carried in full", async () => {
    const fetchMessages = vi.fn(() => ({ messages: [], totalMatching: 0 }));
    setInvokeHandlers({ connection_fetch_messages: fetchMessages });
    useMessageViewerStore.setState({
      connectionId: "1",
      topic: "orders",
      message: {
        partition: 0,
        offset: 7,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("all of it"),
        payloadSizeBytes: 9,
        headers: [],
      },
    });

    renderWithClient(<MessagePayloadViewer />);

    expect(await screen.findByText("all of it")).toBeInTheDocument();
    expect(fetchMessages).not.toHaveBeenCalled();
  });

  it("pretty-prints the payload as JSON when the JSON toggle is clicked", async () => {
    useMessageViewerStore.setState({
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa('{"id":1,"name":"orders"}'),
        payloadSizeBytes: null,
        headers: [],
      },
    });
    const user = userEvent.setup();
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "JSON");

    expect(screen.getByText("id:")).toBeInTheDocument();
    expect(screen.getByText('"orders"')).toBeInTheDocument();
  });

  it("opens the JSON value as its own app tab and switches to it when 'Open in new tab' is clicked", async () => {
    useMessageViewerStore.setState({
      message: {
        partition: 2,
        offset: 7,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa('{"id":1}'),
        payloadSizeBytes: null,
        headers: [],
      },
    });
    const user = userEvent.setup();
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "JSON");
    await user.click(screen.getByRole("button", { name: "Open in new tab" }));

    const tabs = useJsonViewerTabsStore.getState().tabs;
    expect(tabs).toHaveLength(1);
    expect(tabs[0]).toMatchObject({ title: "Partition 2 · Offset 7", value: { id: 1 } });
    expect(useTabsStore.getState().activeTabId).toBe(tabs[0].id);
  });

  it("shows an error message when JSON is requested but the payload isn't valid JSON", async () => {
    useMessageViewerStore.setState({
      message: { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: btoa("not json"), payloadSizeBytes: null, headers: [] },
    });
    const user = userEvent.setup();
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "JSON");

    expect(screen.getByText(/not valid json/i)).toBeInTheDocument();
  });

  it("shows an error message when JSON is requested but the payload is XML", async () => {
    useMessageViewerStore.setState({
      message: { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: btoa("<a>1</a>"), payloadSizeBytes: null, headers: [] },
    });
    const user = userEvent.setup();
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "JSON");

    expect(screen.getByText(/not valid json/i)).toBeInTheDocument();
  });

  it("renders the payload as an XML tree when the XML toggle is clicked", async () => {
    useMessageViewerStore.setState({
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("<order><id>1</id></order>"),
        payloadSizeBytes: null,
        headers: [],
      },
    });
    const user = userEvent.setup();
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "XML");

    expect(screen.getByText("<order>")).toBeInTheDocument();
    expect(screen.getByText("1")).toBeInTheDocument();
  });

  it("opens the XML value as its own app tab and switches to it when 'Open in new tab' is clicked", async () => {
    useMessageViewerStore.setState({
      message: {
        partition: 2,
        offset: 7,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("<order/>"),
        payloadSizeBytes: null,
        headers: [],
      },
    });
    const user = userEvent.setup();
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "XML");
    await user.click(screen.getByRole("button", { name: "Open in new tab" }));

    const tabs = useJsonViewerTabsStore.getState().tabs;
    expect(tabs).toHaveLength(1);
    expect(tabs[0]).toMatchObject({ title: "Partition 2 · Offset 7", kind: "xml" });
    expect(useTabsStore.getState().activeTabId).toBe(tabs[0].id);
  });

  it("shows an error message when XML is requested but the payload isn't valid XML", async () => {
    useMessageViewerStore.setState({
      message: { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: btoa("not xml"), payloadSizeBytes: null, headers: [] },
    });
    const user = userEvent.setup();
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "XML");

    expect(screen.getByText(/not valid xml/i)).toBeInTheDocument();
  });

  it("shows an error message when XML is requested but the payload is JSON", async () => {
    useMessageViewerStore.setState({
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa('{"id":1}'),
        payloadSizeBytes: null,
        headers: [],
      },
    });
    const user = userEvent.setup();
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "XML");

    expect(screen.getByText(/not valid xml/i)).toBeInTheDocument();
  });

  it("decodes and renders the payload as a JSON tree when Avro is clicked", async () => {
    setInvokeHandlers({ connection_decode_avro: () => ({ id: 1, name: "orders" }) });
    useMessageViewerStore.setState({
      connectionId: "conn-1",
      topic: "orders",
      message: { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: btoa("avro-bytes"), payloadSizeBytes: null, headers: [] },
    });
    const user = userEvent.setup();
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "Avro");

    expect(await screen.findByText("id:")).toBeInTheDocument();
    expect(screen.getByText('"orders"')).toBeInTheDocument();
    expect(invoke).toHaveBeenCalledWith("connection_decode_avro", {
      id: "conn-1",
      topic: "orders",
      payloadBase64: btoa("avro-bytes"),
    });
  });

  it("shows an alert with the backend's message when Avro decoding fails", async () => {
    setInvokeHandlers({
      connection_decode_avro: () => {
        throw new Error("no manual schema is set for this topic and this connection has no Schema Registry configured");
      },
    });
    useMessageViewerStore.setState({
      connectionId: "conn-1",
      topic: "orders",
      message: { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: btoa("avro-bytes"), payloadSizeBytes: null, headers: [] },
    });
    const user = userEvent.setup();
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "Avro");

    expect(await screen.findByRole("alert")).toHaveTextContent(/no schema registry configured/i);
  });

  it("shows a placeholder, and no format picker, when payloadBase64 is null", () => {
    useMessageViewerStore.setState({
      message: { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] },
    });
    renderWithClient(<MessagePayloadViewer />);

    expect(screen.getByText("No payload.")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /^Value format:/ })).not.toBeInTheDocument();
  });

  it("shows the message's partition and offset even when it has no payload", () => {
    useMessageViewerStore.setState({
      message: { partition: 3, offset: 17, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] },
    });
    renderWithClient(<MessagePayloadViewer />);

    expect(screen.getByText(/partition 3/i)).toBeInTheDocument();
    expect(screen.getByText(/offset 17/i)).toBeInTheDocument();
  });

  it("shows the message's partition and offset", () => {
    useMessageViewerStore.setState({
      message: { partition: 3, offset: 17, timestampMs: null, keyBase64: null, payloadBase64: btoa("x"), payloadSizeBytes: null, headers: [] },
    });
    renderWithClient(<MessagePayloadViewer />);

    expect(screen.getByText(/partition 3/i)).toBeInTheDocument();
    expect(screen.getByText(/offset 17/i)).toBeInTheDocument();
  });

  it("clears the viewed message when the close button is clicked", async () => {
    useMessageViewerStore.setState({
      message: { partition: 3, offset: 17, timestampMs: null, keyBase64: null, payloadBase64: btoa("x"), payloadSizeBytes: null, headers: [] },
    });
    const user = userEvent.setup();
    renderWithClient(<MessagePayloadViewer />);

    await user.click(screen.getByLabelText("Close message payload viewer"));

    expect(useMessageViewerStore.getState().message).toBeNull();
  });

  it("opens on the Value tab by default", () => {
    useMessageViewerStore.setState({
      message: { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] },
    });
    renderWithClient(<MessagePayloadViewer />);

    expect(screen.getByRole("tab", { name: "Value" })).toHaveAttribute("aria-selected", "true");
  });

  it("shows a table of headers on the Headers tab", async () => {
    useMessageViewerStore.setState({
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: null,
        payloadSizeBytes: null,
        headers: [
          { key: "content-type", valueBase64: btoa("application/json") },
          { key: "empty-header", valueBase64: null },
        ],
      },
    });
    const user = userEvent.setup();
    renderWithClient(<MessagePayloadViewer />);

    await user.click(screen.getByRole("tab", { name: "Headers" }));

    expect(screen.getByText("content-type")).toBeInTheDocument();
    expect(screen.getByText("application/json")).toBeInTheDocument();
    expect(screen.getByText("empty-header")).toBeInTheDocument();
  });

  describe("copying a header value", () => {
    function viewHeaders() {
      useMessageViewerStore.setState({
        message: {
          partition: 0,
          offset: 1,
          timestampMs: null,
          keyBase64: null,
          payloadBase64: null,
          payloadSizeBytes: null,
          headers: [
            { key: "content-type", valueBase64: btoa("application/json") },
            { key: "empty-header", valueBase64: null },
          ],
        },
      });
    }

    it("offers a copy button on each header value that copies it to the clipboard", async () => {
      const writeText = vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue(undefined);
      viewHeaders();
      const user = userEvent.setup();
      renderWithClient(<MessagePayloadViewer />);
      await user.click(screen.getByRole("tab", { name: "Headers" }));

      await user.click(screen.getByRole("button", { name: "Copy value of content-type" }));

      expect(writeText).toHaveBeenCalledWith("application/json");
      expect(await screen.findByRole("button", { name: "Copied value of content-type" })).toBeInTheDocument();
    });

    it("has no copy button for a header with no value", async () => {
      viewHeaders();
      const user = userEvent.setup();
      renderWithClient(<MessagePayloadViewer />);
      await user.click(screen.getByRole("tab", { name: "Headers" }));

      expect(screen.queryByRole("button", { name: "Copy value of empty-header" })).not.toBeInTheDocument();
    });
  });

  it("shows a placeholder on the Headers tab when the message has no headers", async () => {
    useMessageViewerStore.setState({
      message: { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] },
    });
    const user = userEvent.setup();
    renderWithClient(<MessagePayloadViewer />);

    await user.click(screen.getByRole("tab", { name: "Headers" }));

    expect(screen.getByText(/no headers/i)).toBeInTheDocument();
  });

  it("shows headers even when the payload wasn't loaded for this fetch", async () => {
    useMessageViewerStore.setState({
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: null,
        payloadSizeBytes: null,
        headers: [{ key: "trace-id", valueBase64: btoa("abc") }],
      },
    });
    const user = userEvent.setup();
    renderWithClient(<MessagePayloadViewer />);

    await user.click(screen.getByRole("tab", { name: "Headers" }));

    expect(screen.getByText("trace-id")).toBeInTheDocument();
  });

  function viewMessage(payloadBase64: string, offset = 1) {
    useMessageViewerStore.setState({
      message: { partition: 0, offset, timestampMs: null, keyBase64: null, payloadBase64, payloadSizeBytes: null, headers: [] },
    });
  }

  /**
   * The viewer is keyed by tab, not by message (App.tsx), so it stays mounted
   * while the user clicks through the grid and its state carries over. Both
   * tests below cover that carry-over, which is where a per-message guard
   * quietly stops guarding.
   */
  it("re-truncates a large payload after the previous message was expanded in full", async () => {
    const short = "a".repeat(300_000);
    const long = "b".repeat(400_000);
    const user = userEvent.setup();
    viewMessage(btoa(short));
    renderWithClient(<MessagePayloadViewer />);

    await user.click(screen.getByRole("button", { name: /show the whole payload/i }));
    expect(screen.queryByText(/show the whole payload/i)).not.toBeInTheDocument();

    // Watching the DOM itself, because the bug is about a render that is
    // immediately corrected: resetting the flag in an effect leaves the first
    // render committing the whole payload, and the browser paints that before
    // the effect lands. Testing Library flushes effects before any assertion,
    // so the final DOM looks identical either way — only the mutations in
    // between tell the two apart, and that intermediate commit is the freeze.
    const committed: number[] = [];
    const observer = new MutationObserver((records) => {
      for (const record of records) {
        if (record.type === "characterData") committed.push((record.target.textContent ?? "").length);
        record.addedNodes.forEach((node) => committed.push((node.textContent ?? "").length));
      }
    });
    observer.observe(document.body, { subtree: true, childList: true, characterData: true });

    viewMessage(btoa(long), 2);
    expect(await screen.findByRole("button", { name: /show the whole payload/i })).toBeInTheDocument();
    observer.disconnect();

    expect(Math.max(0, ...committed)).toBeLessThanOrEqual(TEXT_PREVIEW_CHARS);
    expect(screen.queryByText(long)).not.toBeInTheDocument();
  });

  /**
   * The payload panel is not remounted between messages — it is handed new
   * props — so anything the reader did to the previous message's tree must
   * not follow them onto the next one, where the same path holds something
   * else entirely.
   */
  it("opens a newly selected message fully, even where the reader had collapsed the previous one", async () => {
    const first = JSON.stringify({ events: ["only-one"] });
    const second = JSON.stringify({
      events: Array.from({ length: 300 }, (_, i) => ({ id: `event-${i}`, seq: i, note: "n" })),
    });
    const user = userEvent.setup();
    viewMessage(btoa(first));
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "JSON");
    await user.click(screen.getByRole("button", { name: "Collapse events" }));
    expect(screen.queryByText('"only-one"')).not.toBeInTheDocument();

    viewMessage(btoa(second), 2);

    expect(await screen.findByText('"event-0"')).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Collapse events" })).toBeInTheDocument();
  });

  // --- Expansion -----------------------------------------------------------
  //
  // Every node opens. The tree is windowed, so a heavy document costs rows in
  // an array rather than elements in the DOM, and there is nothing for an
  // auto-collapse rule — or the Expand all button that used to undo it — to
  // protect against.

  it("opens a heavy payload fully, with no Expand all button anywhere", async () => {
    const heavy = JSON.stringify({
      orderId: "a-1",
      events: Array.from({ length: 300 }, (_, i) => ({ id: `event-${i}`, seq: i, note: "n" })),
    });
    const user = userEvent.setup();
    viewMessage(btoa(heavy));
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "JSON");

    expect(screen.getByText('"event-0"')).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Expand all" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Expand events" })).not.toBeInTheDocument();
  });

  it("still lets the reader close a section by hand", async () => {
    const heavy = JSON.stringify({
      events: Array.from({ length: 300 }, (_, i) => ({ id: `event-${i}`, seq: i, note: "n" })),
    });
    const user = userEvent.setup();
    viewMessage(btoa(heavy));
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "JSON");
    await user.click(screen.getByRole("button", { name: "Collapse events" }));

    expect(screen.queryByText('"event-0"')).not.toBeInTheDocument();
    expect(screen.getByText(/300 items/)).toBeInTheDocument();
  });

  // --- The chosen view survives a top-level tab switch ---------------------
  //
  // App.tsx renders this component `key={activeTabId}`, so leaving a tab and
  // coming back destroys and rebuilds it. Held in `useState`, the mode you
  // had chosen died with it and you landed back on raw Text.

  it("comes back in the JSON view after the top-level tab was switched away from and back", async () => {
    const user = userEvent.setup();
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    useMessageViewerStore.setState({
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa(JSON.stringify({ id: "order-42" })),
        payloadSizeBytes: null,
        headers: [],
      },
    });
    const { unmount } = renderWithClient(<MessagePayloadViewer />);
    await selectFormat(user, "JSON");
    expect(screen.getByText('"order-42"')).toBeInTheDocument();

    // Leaving the tab tears the viewer down; coming back builds a new one.
    unmount();
    renderWithClient(<MessagePayloadViewer />);

    expect(screen.getByText('"order-42"')).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Value format: JSON" })).toBeInTheDocument();
  });

  /**
   * A Data tab row carries a bounded preview, so opening a large message
   * costs one single-message fetch. The viewer is rendered `key={activeTabId}`
   * and so is destroyed on every top-level tab switch — and the query that
   * held the result was destroyed with it, sending the app back to the broker
   * for the same bytes every time the reader came back to the tab.
   */
  it("does not re-fetch the open message's payload after a top-level tab switch", async () => {
    const whole = JSON.stringify({ id: "order-42", note: "x".repeat(400) });
    let fetches = 0;
    setInvokeHandlers({
      connection_fetch_messages: () => {
        fetches += 1;
        return {
          messages: [
            {
              partition: 0,
              offset: 1,
              timestampMs: null,
              keyBase64: null,
              payloadBase64: btoa(whole),
              payloadSizeBytes: whole.length,
              headers: [],
            },
          ],
          totalMatching: 1,
          bytesFetched: whole.length,
        };
      },
    });
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    useMessageViewerStore.setState({ activeTabId: "tab-1", byTab: {} });
    // A row that carried only a preview: far shorter than the reported size.
    useMessageViewerStore
      .getState()
      .viewMessage(
        { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: btoa("{"), payloadSizeBytes: whole.length, headers: [] },
        "1",
        "orders",
      );

    const first = renderWithClient(<MessagePayloadViewer />);
    await waitFor(() => expect(fetches).toBe(1));
    await screen.findByText(/order-42/);
    first.unmount();

    // Back to the tab: a new viewer, and the payload already in hand.
    renderWithClient(<MessagePayloadViewer />);

    expect(await screen.findByText(/order-42/)).toBeInTheDocument();
    expect(fetches).toBe(1);
  });

  it("forgets the remembered payload when the tab's memory is cleared", async () => {
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    useMessageViewerStore.setState({ activeTabId: "tab-1", byTab: {} });
    useMessageViewerStore
      .getState()
      .viewMessage(
        { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: btoa("x"), payloadSizeBytes: 9999, headers: [] },
        "1",
        "orders",
      );
    useMessageViewerStore.getState().setFullPayload(btoa("the whole thing"));
    expect(useMessageViewerStore.getState().fullPayloadBase64).toBe(btoa("the whole thing"));

    useMessageViewerStore.getState().clearTabMemory("tab-1");

    expect(useMessageViewerStore.getState().fullPayloadBase64).toBeUndefined();
  });

  it("comes back on the Headers panel tab after the top-level tab was switched away from and back", async () => {
    const user = userEvent.setup();
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    useMessageViewerStore.setState({
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("hello"),
        payloadSizeBytes: null,
        headers: [{ key: "trace-id", valueBase64: btoa("abc") }],
      },
    });
    const { unmount } = renderWithClient(<MessagePayloadViewer />);
    await user.click(screen.getByRole("tab", { name: "Headers" }));
    expect(screen.getByText("trace-id")).toBeInTheDocument();

    unmount();
    renderWithClient(<MessagePayloadViewer />);

    expect(screen.getByText("trace-id")).toBeInTheDocument();
  });

  // Each top-level tab's right pane holds its own message
  // (`useMessageViewerStore.byTab`), and its own explicit format choice — but
  // a tab that has never chosen one follows the last format picked anywhere,
  // which is what "remember the format I read payloads in" means across the
  // several tabs a session accumulates.
  it("opens a fresh tab in the last format chosen", async () => {
    const user = userEvent.setup();
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    useMessageViewerStore.setState({
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa(JSON.stringify({ id: "order-42" })),
        payloadSizeBytes: null,
        headers: [],
      },
    });
    const { unmount } = renderWithClient(<MessagePayloadViewer />);
    await selectFormat(user, "JSON");
    unmount();

    useTabsStore.setState({ tabs: [], activeTabId: "tab-2", error: null });
    renderWithClient(<MessagePayloadViewer />);

    expect(screen.getByRole("button", { name: "Value format: JSON" })).toBeInTheDocument();
  });

  it("keeps a tab's own format when another tab changes the default", async () => {
    const user = userEvent.setup();
    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    useMessageViewerStore.setState({
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa(JSON.stringify({ id: "order-42" })),
        payloadSizeBytes: null,
        headers: [],
      },
    });
    const first = renderWithClient(<MessagePayloadViewer />);
    await selectFormat(user, "XML");
    first.unmount();

    useTabsStore.setState({ tabs: [], activeTabId: "tab-2", error: null });
    const second = renderWithClient(<MessagePayloadViewer />);
    await selectFormat(user, "Hex");
    second.unmount();

    useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
    renderWithClient(<MessagePayloadViewer />);

    expect(screen.getByRole("button", { name: "Value format: XML" })).toBeInTheDocument();
  });
});

// --- The format dropdown's literal renderings ------------------------------

describe("MessagePayloadViewer literal formats", () => {
  function viewText(payload: string) {
    useMessageViewerStore.setState({
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa(payload),
        payloadSizeBytes: null,
        headers: [],
      },
    });
  }

  it("renders the payload as a hex dump", async () => {
    const user = userEvent.setup();
    viewText("Hello, hex dump!");
    const { container } = renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "Hex");

    expect(container.querySelector(".code-body")?.textContent).toBe(
      "00000000  48 65 6c 6c 6f 2c 20 68  65 78 20 64 75 6d 70 21  |Hello, hex dump!|",
    );
  });

  it("renders the payload as wrapped base64", async () => {
    const user = userEvent.setup();
    viewText("a".repeat(120));
    const { container } = renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "Base64");

    const shown = container.querySelector(".code-body")?.textContent ?? "";
    expect(shown.replace(/\n/g, "")).toBe(btoa("a".repeat(120)));
    expect(shown.split("\n")[0]).toHaveLength(76);
  });

  it("numbers the lines of the raw payload", async () => {
    viewText("alpha\nbeta\ngamma");
    const { container } = renderWithClient(<MessagePayloadViewer />);

    expect(container.querySelector(".code-gutter")?.textContent).toBe("1\n2\n3");
  });

  it("numbers the lines of the JSON tree", async () => {
    const user = userEvent.setup();
    viewText(JSON.stringify({ id: "order-42" }));
    const { container } = renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "JSON");

    expect(container.querySelector(".json-tree-body--numbered")).not.toBeNull();
  });

  // The tree views bring their own copy/open toolbar; inside this pane the
  // panel's own toolbar provides those, and two stacked rows of the same
  // buttons is what this replaced.
  it("shows one toolbar, not the tree view's as well", async () => {
    const user = userEvent.setup();
    viewText(JSON.stringify({ id: 1 }));
    const { container } = renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "JSON");

    expect(container.querySelectorAll(".json-tree-toolbar")).toHaveLength(0);
    expect(screen.getAllByRole("button", { name: "Copy" })).toHaveLength(1);
  });

  // A hex dump is ~5 characters per byte, so the bound that keeps the raw
  // view from freezing the webview has to be a tighter one here.
  it("truncates a large hex dump and offers the rest behind a click", async () => {
    const user = userEvent.setup();
    viewText("a".repeat(200_000));
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "Hex");

    expect(screen.getByText(/showing the first/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /show the whole payload/i })).toBeInTheDocument();
  });

  // "Show me all 300 KB as text" must not also mean "and now dump it as hex",
  // which is five times the characters.
  it("re-truncates when the format changes after expanding", async () => {
    const user = userEvent.setup();
    viewText("a".repeat(300_000));
    renderWithClient(<MessagePayloadViewer />);

    await user.click(screen.getByRole("button", { name: /show the whole payload/i }));
    expect(screen.queryByRole("button", { name: /show the whole payload/i })).not.toBeInTheDocument();

    await selectFormat(user, "Hex");

    expect(screen.getByRole("button", { name: /show the whole payload/i })).toBeInTheDocument();
  });
});

// --- The toolbar: open / copy / save ---------------------------------------

describe("MessagePayloadViewer toolbar", () => {
  function viewJson(value: unknown) {
    useMessageViewerStore.setState({
      connectionId: "1",
      topic: "orders",
      message: {
        partition: 2,
        offset: 7,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa(JSON.stringify(value)),
        payloadSizeBytes: null,
        headers: [],
      },
    });
  }

  it("copies the payload in the chosen format", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    vi.spyOn(navigator.clipboard, "writeText").mockImplementation(writeText);
    const user = userEvent.setup();
    viewJson({ id: "order-42" });
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "JSON");
    await user.click(screen.getByRole("button", { name: "Copy" }));

    expect(writeText).toHaveBeenCalledWith(JSON.stringify({ id: "order-42" }, null, 2));
    expect(await screen.findByRole("button", { name: "Copied!" })).toBeInTheDocument();
  });

  // Copy takes the *whole* payload even where the view on screen is capped —
  // a clipboard has no rendering cost to protect.
  it("copies the whole payload even when the view is truncated", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    vi.spyOn(navigator.clipboard, "writeText").mockImplementation(writeText);
    const user = userEvent.setup();
    const long = "a".repeat(300_000);
    useMessageViewerStore.setState({
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa(long),
        payloadSizeBytes: null,
        headers: [],
      },
    });
    renderWithClient(<MessagePayloadViewer />);

    await user.click(screen.getByRole("button", { name: "Copy" }));

    expect(writeText).toHaveBeenCalledWith(long);
  });

  it("saves the value in the chosen format, through the native save dialog", async () => {
    saveDialog.mockResolvedValue("/home/u/order.json");
    const invoked: { command: string; args: unknown }[] = [];
    setInvokeHandlers({
      payload_save: (args) => {
        invoked.push({ command: "payload_save", args });
        return undefined;
      },
    });
    const user = userEvent.setup();
    viewJson({ id: "order-42" });
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "JSON");
    await user.click(screen.getByRole("button", { name: "Save as JSON" }));

    expect(saveDialog).toHaveBeenCalledWith(
      expect.objectContaining({ defaultPath: "partition-2-offset-7.json" }),
    );
    expect(await screen.findByText(/saved to \/home\/u\/order\.json/i)).toBeInTheDocument();
    expect(invoked).toHaveLength(1);
    const { path, contentsBase64 } = invoked[0].args as { path: string; contentsBase64: string };
    expect(path).toBe("/home/u/order.json");
    expect(atob(contentsBase64)).toBe(JSON.stringify({ id: "order-42" }, null, 2));
  });

  it("writes nothing when the save dialog is cancelled", async () => {
    saveDialog.mockResolvedValue(null);
    const writes: unknown[] = [];
    setInvokeHandlers({
      payload_save: (args) => {
        writes.push(args);
        return undefined;
      },
    });
    const user = userEvent.setup();
    viewJson({ id: "order-42" });
    renderWithClient(<MessagePayloadViewer />);

    await user.click(screen.getByRole("button", { name: /^Save as/ }));

    expect(saveDialog).toHaveBeenCalled();
    expect(writes).toEqual([]);
  });

  it("reports a failed save rather than failing silently", async () => {
    saveDialog.mockResolvedValue("/root/denied.txt");
    setInvokeHandlers({
      payload_save: () => {
        throw new Error("permission denied");
      },
    });
    const user = userEvent.setup();
    viewJson({ id: "order-42" });
    renderWithClient(<MessagePayloadViewer />);

    await user.click(screen.getByRole("button", { name: /^Save as/ }));

    expect(await screen.findByRole("alert")).toHaveTextContent(/save failed: permission denied/i);
  });

  it("refuses to save a payload the chosen format can't render", async () => {
    saveDialog.mockResolvedValue("/home/u/order.json");
    setInvokeHandlers({});
    const user = userEvent.setup();
    useMessageViewerStore.setState({
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("not json at all"),
        payloadSizeBytes: null,
        headers: [],
      },
    });
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "JSON");
    await user.click(screen.getByRole("button", { name: "Save as JSON" }));

    expect(await screen.findByText(/nothing to save/i)).toBeInTheDocument();
    expect(saveDialog).not.toHaveBeenCalled();
  });

  // Download used to sit at the end of this toolbar, writing the payload's
  // original bytes. It was removed from the app entirely — Save, which
  // writes what the selected format renders, is the only way out.
  it("offers no Download button", () => {
    viewJson({ id: "order-42" });
    renderWithClient(<MessagePayloadViewer />);

    expect(screen.queryByRole("button", { name: /download/i })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: /^Save as/ })).toBeInTheDocument();
  });

  it("opens the raw text as its own app tab", async () => {
    const user = userEvent.setup();
    useMessageViewerStore.setState({
      message: {
        partition: 2,
        offset: 7,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("plain text payload"),
        payloadSizeBytes: null,
        headers: [],
      },
    });
    renderWithClient(<MessagePayloadViewer />);

    await user.click(screen.getByRole("button", { name: "Open in new tab" }));

    const [tab] = useJsonViewerTabsStore.getState().tabs;
    expect(tab.kind).toBe("text");
    expect(tab.value).toBe("plain text payload");
    expect(tab.title).toBe("Partition 2 · Offset 7 · Raw");
  });

  /**
   * The size chip is the one thing the opened tab would otherwise lose, and
   * it has to be the *message's* size — the broker's figure, the same number
   * this pane shows — not the length of whatever text the tab renders.
   */
  it("hands the opened tab the message's size", async () => {
    const user = userEvent.setup();
    useMessageViewerStore.setState({
      message: {
        partition: 2,
        offset: 7,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("plain text payload"),
        payloadSizeBytes: 4096,
        headers: [],
      },
    });
    renderWithClient(<MessagePayloadViewer />);

    await user.click(screen.getByRole("button", { name: "Open in new tab" }));

    expect(useJsonViewerTabsStore.getState().tabs[0].payloadSizeBytes).toBe(4096);
  });
});

// --- The dock switch ------------------------------------------------------

describe("MessagePayloadViewer dock switch", () => {
  beforeEach(() => {
    useMessageViewerStore.setState({
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("hello"),
        payloadSizeBytes: null,
        headers: [],
      },
    });
  });

  it("offers to move the panel to the bottom while it is on the right", () => {
    renderWithClient(<MessagePayloadViewer />);

    expect(screen.getByRole("button", { name: "Move the payload panel to the bottom" })).toBeInTheDocument();
  });

  it("switches the placement, and offers the way back", async () => {
    const user = userEvent.setup();
    renderWithClient(<MessagePayloadViewer />);

    await user.click(screen.getByRole("button", { name: "Move the payload panel to the bottom" }));

    expect(useMessageViewerPrefsStore.getState().placement).toBe("bottom");
    expect(screen.getByRole("button", { name: "Move the payload panel to the right" })).toBeInTheDocument();
  });

  // The switch is chrome, not payload: it has to stay reachable however far
  // down a long message the user has scrolled.
  it("keeps the switch out of the scrolling region", () => {
    renderWithClient(<MessagePayloadViewer />);

    expect(
      screen.getByRole("button", { name: /^Move the payload panel/ }).closest(".message-payload-scroll"),
    ).toBeNull();
  });
});

describe("MessagePayloadViewer font handling", () => {
  function viewText(payload: string) {
    useMessageViewerStore.setState({
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa(payload),
        payloadSizeBytes: null,
        headers: [],
      },
    });
  }

  // The payload is the thing people asked to read in their chosen font, so
  // the raw view must not pin a font of its own.
  it("leaves the raw payload on the app's chosen font", () => {
    viewText("plain text");
    const { container } = renderWithClient(<MessagePayloadViewer />);

    expect(container.querySelector(".code-view")).not.toHaveClass("code-view--monospace");
  });

  // The hex dump is the exception: its offset, byte and ASCII columns only
  // line up in a monospace font.
  it("pins the hex dump to monospace", async () => {
    const user = userEvent.setup();
    viewText("plain text");
    const { container } = renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "Hex");

    expect(container.querySelector(".code-view")).toHaveClass("code-view--monospace");
  });

  // Base64 was pinned here too, on the grounds that `wrapBase64` lays it out
  // in 76-character MIME lines. It doesn't need to be: those lines are 76
  // characters in any font and nothing lines up *across* them, so the only
  // thing a proportional face costs is a ragged right edge — against the
  // Font style setting visibly doing nothing on a second payload format.
  it("lets base64 follow the chosen font, unlike the hex dump", async () => {
    const user = userEvent.setup();
    viewText("plain text");
    const { container } = renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "Base64");

    expect(container.querySelector(".code-view")).not.toHaveClass("code-view--monospace");
  });
});

describe("MessagePayloadViewer payload size", () => {
  it("shows the size beside the toolbar actions, scaled to a readable unit", () => {
    useMessageViewerStore.setState({
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("hello"),
        payloadSizeBytes: 4 * 1024 * 1024,
        headers: [],
      },
    });
    renderWithClient(<MessagePayloadViewer />);

    expect(screen.getByText("4.00 MB")).toBeInTheDocument();
  });

  /**
   * The row's `payloadBase64` is a bounded preview — the size label has to
   * report the *message*, or it would say "4 KB" for the 40 MB record the
   * user is deciding whether to open, which is the one thing it exists to
   * prevent.
   */
  it("reports the message's size, not the size of the preview slice it was handed", () => {
    setInvokeHandlers({ connection_fetch_messages: () => new Promise(() => {}) });
    useMessageViewerStore.setState({
      connectionId: "1",
      topic: "orders",
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("x".repeat(4096)),
        payloadSizeBytes: 40 * 1024 * 1024,
        headers: [],
      },
    });
    renderWithClient(<MessagePayloadViewer />);

    expect(screen.getByText("40.00 MB")).toBeInTheDocument();
    expect(screen.queryByText("4.0 KB")).not.toBeInTheDocument();
  });

  it("falls back to the payload itself when the backend reported no size", () => {
    useMessageViewerStore.setState({
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("x".repeat(2048)),
        payloadSizeBytes: null,
        headers: [],
      },
    });
    renderWithClient(<MessagePayloadViewer />);

    expect(screen.getByText("2.0 KB")).toBeInTheDocument();
  });
});

describe("MessagePayloadViewer protobuf", () => {
  function viewMessage() {
    useMessageViewerStore.setState({
      connectionId: "1",
      topic: "orders",
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("anything"),
        payloadSizeBytes: null,
        headers: [],
      },
    });
  }

  it("decodes the payload and shows the fields", async () => {
    setInvokeHandlers({
      connection_decode_protobuf: () => ({
        value: { order_id: "ORD-42", quantity: 3 },
        source: "manual",
        messageType: "shop.Order",
      }),
    });
    const user = userEvent.setup();
    viewMessage();
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "Protobuf");

    expect(await screen.findByText('"ORD-42"')).toBeInTheDocument();
    expect(screen.getByText("order_id:")).toBeInTheDocument();
  });

  it("says which schema decoded it", async () => {
    setInvokeHandlers({
      connection_decode_protobuf: () => ({
        value: { a: 1 },
        source: "registry",
        messageType: "shop.Order",
      }),
    });
    const user = userEvent.setup();
    viewMessage();
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "Protobuf");

    expect(await screen.findByText(/shop\.Order/)).toHaveTextContent(/Schema Registry/);
  });

  // Protobuf is the one format that decodes with no schema at all. A tree of
  // "1", "2", "3" otherwise reads as a schema that decoded badly rather than
  // as no schema at all.
  it("says so when it decoded without a schema", async () => {
    setInvokeHandlers({
      connection_decode_protobuf: () => ({ value: { "1": "ORD-42" }, source: "none", messageType: null }),
    });
    const user = userEvent.setup();
    viewMessage();
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "Protobuf");

    expect(await screen.findByText(/no \.proto schema for this topic/i)).toBeInTheDocument();
    expect(screen.getByText(/showing field numbers/i)).toBeInTheDocument();
  });

  it("reports a decode failure rather than showing nothing", async () => {
    setInvokeHandlers({
      connection_decode_protobuf: () => {
        throw new Error("the payload does not match this .proto schema");
      },
    });
    const user = userEvent.setup();
    viewMessage();
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "Protobuf");

    expect(await screen.findByRole("alert")).toHaveTextContent(/does not match this \.proto schema/);
  });

  it("copies the decoded message, not the raw bytes", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    vi.spyOn(navigator.clipboard, "writeText").mockImplementation(writeText);
    setInvokeHandlers({
      connection_decode_protobuf: () => ({ value: { order_id: "ORD-42" }, source: "manual", messageType: "Order" }),
    });
    const user = userEvent.setup();
    viewMessage();
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "Protobuf");
    await screen.findByText('"ORD-42"');
    await user.click(screen.getByRole("button", { name: "Copy" }));

    expect(writeText).toHaveBeenCalledWith(JSON.stringify({ order_id: "ORD-42" }, null, 2));
  });

  it("opens the decoded message as its own tab", async () => {
    setInvokeHandlers({
      connection_decode_protobuf: () => ({ value: { order_id: "ORD-42" }, source: "manual", messageType: "Order" }),
    });
    const user = userEvent.setup();
    viewMessage();
    renderWithClient(<MessagePayloadViewer />);

    await selectFormat(user, "Protobuf");
    await screen.findByText('"ORD-42"');
    await user.click(screen.getByRole("button", { name: "Open in new tab" }));

    expect(useJsonViewerTabsStore.getState().tabs[0].value).toEqual({ order_id: "ORD-42" });
  });

  // Same contract the Avro mode has: clicking through grid rows must re-decode
  // rather than leave the first message's fields on screen.
  it("re-decodes when a different message is selected", async () => {
    const decoded: string[] = [];
    setInvokeHandlers({
      connection_decode_protobuf: (args) => {
        decoded.push((args as { payloadBase64: string }).payloadBase64);
        return { value: { a: 1 }, source: "manual", messageType: "Order" };
      },
    });
    const user = userEvent.setup();
    viewMessage();
    renderWithClient(<MessagePayloadViewer />);
    await selectFormat(user, "Protobuf");
    await screen.findByText("a:");

    useMessageViewerStore.setState({
      connectionId: "1",
      topic: "orders",
      message: {
        partition: 0,
        offset: 2,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("a different message"),
        payloadSizeBytes: null,
        headers: [],
      },
    });

    await waitFor(() => expect(decoded).toHaveLength(2));
    expect(decoded[1]).toBe(btoa("a different message"));
  });
});

// --- Writing out a message that isn't all here yet ------------------------

describe("MessagePayloadViewer with only a preview loaded", () => {
  /**
   * A row whose payload the grid truncated, whose full-payload fetch fails.
   * `payloadBase64` then holds the bounded preview slice permanently.
   */
  function viewTruncatedMessage() {
    setInvokeHandlers({
      connection_fetch_messages: () => {
        throw new Error("broker unreachable");
      },
      payload_save: () => undefined,
    });
    useMessageViewerStore.setState({
      connectionId: "1",
      topic: "orders",
      message: {
        partition: 0,
        offset: 42,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("the first few by"),
        payloadSizeBytes: 5_000_000,
        headers: [],
      },
    });
  }

  it("refuses to save a payload that is only a preview", async () => {
    const user = userEvent.setup();
    viewTruncatedMessage();
    renderWithClient(<MessagePayloadViewer />);
    await screen.findByText(/showing a preview only/i);

    await user.click(screen.getByRole("button", { name: /^Save as/ }));

    expect(await screen.findByRole("alert")).toHaveTextContent(/not saved/i);
    expect(saveDialog).not.toHaveBeenCalled();
  });

  it("says why, so the banner and the refusal agree", async () => {
    const user = userEvent.setup();
    viewTruncatedMessage();
    renderWithClient(<MessagePayloadViewer />);
    await screen.findByText(/showing a preview only/i);

    await user.click(screen.getByRole("button", { name: /^Save as/ }));

    expect(await screen.findByRole("alert")).toHaveTextContent(/could not be loaded/i);
  });
});

// --- The toolbar must describe the message on screen ----------------------

describe("MessagePayloadViewer toolbar freshness", () => {
  /**
   * `useMutation` keeps the previous result until a new call resolves, and
   * the decode effects deliberately don't fire while a full payload is still
   * being fetched — so a successful decode can outlive the message it
   * describes. The views hide behind the spinner; the toolbar does not.
   */
  it("won't save one message's decode under the next message's name", async () => {
    const user = userEvent.setup();
    let failFetch = false;
    setInvokeHandlers({
      connection_decode_protobuf: () => ({
        value: { order_id: "FROM-THE-FIRST-MESSAGE" },
        source: "manual",
        messageType: "shop.Order",
      }),
      connection_fetch_messages: () => {
        if (failFetch) throw new Error("broker unreachable");
        return { messages: [], totalMatching: 0 };
      },
      payload_save: () => undefined,
    });
    useMessageViewerStore.setState({
      connectionId: "1",
      topic: "orders",
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("first"),
        payloadSizeBytes: null,
        headers: [],
      },
    });
    renderWithClient(<MessagePayloadViewer />);
    await selectFormat(user, "Protobuf");
    expect(await screen.findByText(/FROM-THE-FIRST-MESSAGE/)).toBeInTheDocument();

    // A second, larger message whose full payload never arrives — the decode
    // effect is gated on that, so the mutation keeps the first result.
    failFetch = true;
    useMessageViewerStore.setState({
      message: {
        partition: 0,
        offset: 2,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("second, trunc"),
        payloadSizeBytes: 5_000_000,
        headers: [],
      },
    });
    await screen.findByText(/showing a preview only/i);

    await user.click(screen.getByRole("button", { name: /^Save as/ }));

    expect(await screen.findByRole("alert")).toHaveTextContent(/not saved/i);
    expect(saveDialog).not.toHaveBeenCalled();
  });
});

// --- Opening a literal view in its own tab --------------------------------

describe("MessagePayloadViewer open-in-new-tab capping", () => {
  it("hands the tab what the pane is showing, not an uncapped rebuild", async () => {
    const user = userEvent.setup();
    useMessageViewerStore.setState({
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("a".repeat(400_000)),
        payloadSizeBytes: null,
        headers: [],
      },
    });
    renderWithClient(<MessagePayloadViewer />);
    // The pane caps the raw view at TEXT_PREVIEW_CHARS.
    expect(screen.getByRole("button", { name: /show the whole payload/i })).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Open in new tab" }));

    const [tab] = useJsonViewerTabsStore.getState().tabs;
    expect(String(tab.value)).toHaveLength(TEXT_PREVIEW_CHARS);
  });

  /** Expanding is the user's explicit choice, and the tab honours it. */
  it("hands over the whole payload once the user has expanded it", async () => {
    const user = userEvent.setup();
    useMessageViewerStore.setState({
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("a".repeat(400_000)),
        payloadSizeBytes: null,
        headers: [],
      },
    });
    renderWithClient(<MessagePayloadViewer />);
    await user.click(screen.getByRole("button", { name: /show the whole payload/i }));

    await user.click(screen.getByRole("button", { name: "Open in new tab" }));

    const [tab] = useJsonViewerTabsStore.getState().tabs;
    expect(String(tab.value)).toHaveLength(400_000);
  });
});

// --- Status messages ------------------------------------------------------

describe("MessagePayloadViewer status messages", () => {
  /**
   * Each action used to schedule its own unconditional clear, so a Copy's
   * 1.5s timer would wipe a Save error that appeared a second later — after
   * half a second, not the six the error branch asks for.
   */
  it("an earlier success's timer doesn't clear a later error", async () => {
    const user = userEvent.setup();
    vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue(undefined);
    saveDialog.mockResolvedValue("/home/u/out.json");
    setInvokeHandlers({
      payload_save: () => {
        throw new Error("permission denied");
      },
    });
    useMessageViewerStore.setState({
      message: {
        partition: 0,
        offset: 1,
        timestampMs: null,
        keyBase64: null,
        payloadBase64: btoa("hello"),
        payloadSizeBytes: null,
        headers: [],
      },
    });
    renderWithClient(<MessagePayloadViewer />);

    // Real timers: the interaction under test *is* two timers racing, and
    // faking them here fights userEvent's own scheduling. The copy clear is
    // armed for 1.5s, so a save that fails 1s later must still be on screen
    // after the moment that timer would have fired.
    await user.click(screen.getByRole("button", { name: "Copy" }));
    await new Promise((resolve) => setTimeout(resolve, 1000));
    await user.click(screen.getByRole("button", { name: /^Save as/ }));
    expect(await screen.findByRole("alert")).toHaveTextContent(/permission denied/i);

    await new Promise((resolve) => setTimeout(resolve, 800));

    expect(screen.getByRole("alert")).toHaveTextContent(/permission denied/i);
  });
});
