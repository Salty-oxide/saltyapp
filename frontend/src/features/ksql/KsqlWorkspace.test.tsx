import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { setInvokeHandlers } from "../../lib/testInvoke";
import { KsqlWorkspace } from "./KsqlWorkspace";
import { useKsqlStore } from "./useKsqlStore";
import { useTabsStore } from "../tabs/useTabsStore";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));

/**
 * Captures the event listeners so a test can deliver a `ksql-header` or
 * `ksql-rows` payload the way the backend would — the rows do not come back
 * from the command, because a push query does not return until it is stopped.
 */
const listeners = new Map<string, (event: { payload: unknown }) => void>();
vi.mock("@tauri-apps/api/event", () => ({
  listen: (name: string, handler: (event: { payload: unknown }) => void) => {
    listeners.set(name, handler);
    return Promise.resolve(() => listeners.delete(name));
  },
}));

function emit(name: string, payload: unknown) {
  listeners.get(name)?.({ payload });
}

function renderWithClient(ui: React.ReactElement) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}>{ui}</QueryClientProvider>);
}

beforeEach(() => {
  vi.clearAllMocks();
  listeners.clear();
  useKsqlStore.setState({ byKey: {} });
  useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
});

describe("KsqlWorkspace", () => {
  it("prefills the editor with the query it was given", () => {
    setInvokeHandlers({});
    renderWithClient(
      <KsqlWorkspace connectionId="1" scope="orders" initialSql="SELECT * FROM S EMIT CHANGES;" />,
    );

    expect(screen.getByLabelText("ksqlDB query")).toHaveValue("SELECT * FROM S EMIT CHANGES;");
  });

  it("cannot Run an empty editor", () => {
    setInvokeHandlers({});
    renderWithClient(<KsqlWorkspace connectionId="1" scope="cluster" />);

    expect(screen.getByLabelText("Run")).toBeDisabled();
  });

  it("enables Run once something is typed", async () => {
    setInvokeHandlers({});
    const user = userEvent.setup();
    renderWithClient(<KsqlWorkspace connectionId="1" scope="cluster" />);

    await user.type(screen.getByLabelText("ksqlDB query"), "SELECT 1;");

    expect(screen.getByLabelText("Run")).toBeEnabled();
  });

  it("Stop is only offered while a query is running", async () => {
    let resolveQuery: (value: unknown) => void = () => {};
    setInvokeHandlers({
      ksql_query: () => new Promise((resolve) => (resolveQuery = resolve)),
    });
    const user = userEvent.setup();
    renderWithClient(<KsqlWorkspace connectionId="1" scope="cluster" initialSql="SELECT 1;" />);
    expect(screen.getByLabelText("Stop")).toBeDisabled();

    await user.click(screen.getByLabelText("Run"));
    await waitFor(() => expect(screen.getByLabelText("Stop")).toBeEnabled());

    resolveQuery({ cancelled: false, rowCount: 0 });
    await waitFor(() => expect(screen.getByLabelText("Stop")).toBeDisabled());
  });

  // Rows arrive as events, not as the command's return value — a live tail
  // never returns until it is stopped.
  it("builds the grid's columns from the header event and fills it from row events", async () => {
    setInvokeHandlers({ ksql_query: () => ({ cancelled: false, rowCount: 2 }) });
    const user = userEvent.setup();
    renderWithClient(<KsqlWorkspace connectionId="1" scope="cluster" initialSql="SELECT 1;" />);

    await user.click(screen.getByLabelText("Run"));
    const requestId = useKsqlStore.getState().byKey["tab-1:1:cluster"]?.requestId;

    emit("ksql-header", { requestId, columns: [{ name: "ID", kind: "STRING" }] });
    emit("ksql-rows", { requestId, rows: [["a"], ["b"]] });

    await waitFor(() => {
      const state = useKsqlStore.getState().byKey["tab-1:1:cluster"];
      expect(state.columns).toHaveLength(1);
      expect(state.rows).toHaveLength(2);
    });
  });

  it("reports how many rows arrived", async () => {
    setInvokeHandlers({ ksql_query: () => new Promise(() => {}) });
    const user = userEvent.setup();
    renderWithClient(<KsqlWorkspace connectionId="1" scope="cluster" initialSql="SELECT 1;" />);

    await user.click(screen.getByLabelText("Run"));
    const requestId = useKsqlStore.getState().byKey["tab-1:1:cluster"]?.requestId;
    emit("ksql-rows", { requestId, rows: [["a"], ["b"], ["c"]] });

    expect(await screen.findByText(/3 rows · live/)).toBeInTheDocument();
  });

  it("says a query was stopped rather than finished when Stop ended it", async () => {
    setInvokeHandlers({
      ksql_query: () => ({ cancelled: true, rowCount: 1 }),
      ksql_cancel: () => null,
    });
    const user = userEvent.setup();
    renderWithClient(<KsqlWorkspace connectionId="1" scope="cluster" initialSql="SELECT 1;" />);

    await user.click(screen.getByLabelText("Run"));

    expect(await screen.findByText(/stopped/)).toBeInTheDocument();
  });

  it("asks the backend to cancel when Stop is pressed", async () => {
    const cancelled: string[] = [];
    setInvokeHandlers({
      ksql_query: () => new Promise(() => {}),
      ksql_cancel: (args: { requestId: string }) => {
        cancelled.push(args.requestId);
        return null;
      },
    });
    const user = userEvent.setup();
    renderWithClient(<KsqlWorkspace connectionId="1" scope="cluster" initialSql="SELECT 1;" />);
    await user.click(screen.getByLabelText("Run"));
    const requestId = useKsqlStore.getState().byKey["tab-1:1:cluster"]?.requestId;

    await user.click(screen.getByLabelText("Stop"));

    expect(cancelled).toEqual([requestId]);
  });

  // A refusal from the statement classifier arrives as a rejected command, and
  // has to be readable rather than swallowed.
  it("shows the reason a statement was refused", async () => {
    setInvokeHandlers({
      ksql_query: () => {
        throw new Error("DROP is not available from Salty.");
      },
    });
    const user = userEvent.setup();
    renderWithClient(<KsqlWorkspace connectionId="1" scope="cluster" initialSql="DROP STREAM S;" />);

    await user.click(screen.getByLabelText("Run"));

    expect(await screen.findByRole("alert")).toHaveTextContent("DROP is not available");
  });

  // The guard that stops a replaced query's rows landing under the new one's
  // columns. Events are not ordered against the command that started them.
  it("discards rows belonging to a superseded run", async () => {
    setInvokeHandlers({ ksql_query: () => new Promise(() => {}) });
    const user = userEvent.setup();
    renderWithClient(<KsqlWorkspace connectionId="1" scope="cluster" initialSql="SELECT 1;" />);

    await user.click(screen.getByLabelText("Run"));
    const first = useKsqlStore.getState().byKey["tab-1:1:cluster"]?.requestId;
    // A second Run supersedes it.
    useKsqlStore.getState().start("tab-1:1:cluster", "req-second");

    emit("ksql-rows", { requestId: first, rows: [["stale"]] });

    expect(useKsqlStore.getState().byKey["tab-1:1:cluster"].rows).toEqual([]);
  });

  it("keeps each scope's editor and results apart", async () => {
    setInvokeHandlers({});
    const user = userEvent.setup();
    const { rerender } = renderWithClient(
      <KsqlWorkspace connectionId="1" scope="cluster" />,
    );
    await user.type(screen.getByLabelText("ksqlDB query"), "cluster query");

    rerender(
      <QueryClientProvider client={new QueryClient()}>
        <KsqlWorkspace connectionId="1" scope="orders" />
      </QueryClientProvider>,
    );

    expect(screen.getByLabelText("ksqlDB query")).toHaveValue("");
    expect(useKsqlStore.getState().byKey["tab-1:1:cluster"].sql).toBe("cluster query");
  });

  // Prefill must not fight the user for the editor.
  it("does not overwrite text the user has already typed", async () => {
    setInvokeHandlers({});
    const user = userEvent.setup();
    renderWithClient(<KsqlWorkspace connectionId="1" scope="orders" initialSql="SELECT 1;" />);

    await user.clear(screen.getByLabelText("ksqlDB query"));
    await user.type(screen.getByLabelText("ksqlDB query"), "SELECT 2;");

    expect(screen.getByLabelText("ksqlDB query")).toHaveValue("SELECT 2;");
  });
});
