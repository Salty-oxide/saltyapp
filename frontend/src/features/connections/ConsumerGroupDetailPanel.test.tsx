import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { setInvokeHandlers } from "../../lib/testInvoke";
import { ConsumerGroupDetailPanel } from "./ConsumerGroupDetailPanel";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));

function renderWithClient(ui: React.ReactElement) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(<QueryClientProvider client={client}>{ui}</QueryClientProvider>);
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("ConsumerGroupDetailPanel", () => {
  it("shows the group id and does not fetch on mount", () => {
    const fetchLag = vi.fn();
    setInvokeHandlers({ connection_fetch_consumer_group_lag: fetchLag });
    renderWithClient(<ConsumerGroupDetailPanel connectionId="1" groupId="billing-service" />);

    expect(screen.getByText("billing-service")).toBeInTheDocument();
    expect(fetchLag).not.toHaveBeenCalled();
  });

  it("fetches and renders lag rows when Refresh is clicked", async () => {
    setInvokeHandlers({
      connection_fetch_consumer_group_lag: () => ({
        state: "Stable",
        partitions: [
          {
            topic: "orders",
            partition: 1,
            currentOffset: 9800,
            logEndOffset: 15200,
            lag: 5400,
            clientId: "c1",
            clientHost: "10.0.0.5",
          },
        ],
      }),
    });
    const user = userEvent.setup();
    renderWithClient(<ConsumerGroupDetailPanel connectionId="1" groupId="billing-service" />);

    await user.click(screen.getByRole("button", { name: "Refresh" }));

    const rows = await screen.findAllByRole("row");
    const cells = within(rows[1])
      .getAllByRole("cell")
      .map((c) => c.textContent);
    expect(cells).toEqual(["orders", "1", "9,800", "15,200", "5,400", "c1@10.0.0.5"]);
    expect(screen.getByText(/Stable/)).toBeInTheDocument();
  });

  it("shows the summed total lag", async () => {
    setInvokeHandlers({
      connection_fetch_consumer_group_lag: () => ({
        state: "Stable",
        partitions: [
          { topic: "orders", partition: 0, currentOffset: 100, logEndOffset: 100, lag: 0, clientId: null, clientHost: null },
          {
            topic: "orders",
            partition: 1,
            currentOffset: 100,
            logEndOffset: 5500,
            lag: 5400,
            clientId: null,
            clientHost: null,
          },
        ],
      }),
    });
    const user = userEvent.setup();
    renderWithClient(<ConsumerGroupDetailPanel connectionId="1" groupId="billing-service" />);

    await user.click(screen.getByRole("button", { name: "Refresh" }));

    expect(await screen.findByText("Total lag: 5,400 messages")).toBeInTheDocument();
  });

  it("shows an empty-assignment message when the group has no partitions", async () => {
    setInvokeHandlers({
      connection_fetch_consumer_group_lag: () => ({ state: "Empty", partitions: [] }),
    });
    const user = userEvent.setup();
    renderWithClient(<ConsumerGroupDetailPanel connectionId="1" groupId="billing-service" />);

    await user.click(screen.getByRole("button", { name: "Refresh" }));

    expect(await screen.findByText(/no active partition assignment/i)).toBeInTheDocument();
  });

  it("shows an error banner when the fetch fails", async () => {
    setInvokeHandlers({
      connection_fetch_consumer_group_lag: () => {
        throw new Error("Couldn't determine partition assignment for this group");
      },
    });
    const user = userEvent.setup();
    renderWithClient(<ConsumerGroupDetailPanel connectionId="1" groupId="billing-service" />);

    await user.click(screen.getByRole("button", { name: "Refresh" }));

    expect(await screen.findByRole("alert")).toHaveTextContent(
      "Couldn't determine partition assignment for this group",
    );
  });

  it("shows a dash for unknown current offset and lag", async () => {
    setInvokeHandlers({
      connection_fetch_consumer_group_lag: () => ({
        state: "Stable",
        partitions: [
          {
            topic: "orders",
            partition: 2,
            currentOffset: null,
            logEndOffset: 100,
            lag: null,
            clientId: null,
            clientHost: null,
          },
        ],
      }),
    });
    const user = userEvent.setup();
    renderWithClient(<ConsumerGroupDetailPanel connectionId="1" groupId="billing-service" />);

    await user.click(screen.getByRole("button", { name: "Refresh" }));

    const rows = await screen.findAllByRole("row");
    const cells = within(rows[1])
      .getAllByRole("cell")
      .map((c) => c.textContent);
    expect(cells).toEqual(["orders", "2", "—", "100", "—", "—"]);
  });

  it("filters rows by topic name via the search box", async () => {
    setInvokeHandlers({
      connection_fetch_consumer_group_lag: () => ({
        state: "Stable",
        partitions: [
          { topic: "orders", partition: 0, currentOffset: 0, logEndOffset: 0, lag: 0, clientId: null, clientHost: null },
          {
            topic: "payments",
            partition: 0,
            currentOffset: 0,
            logEndOffset: 0,
            lag: 0,
            clientId: null,
            clientHost: null,
          },
        ],
      }),
    });
    const user = userEvent.setup();
    renderWithClient(<ConsumerGroupDetailPanel connectionId="1" groupId="billing-service" />);
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    await screen.findByText("payments");

    await user.type(screen.getByLabelText("Search topics"), "pay");

    expect(screen.queryByText("orders")).not.toBeInTheDocument();
    expect(screen.getByText("payments")).toBeInTheDocument();
  });

  it("clears the previous group's lag table when switching groups (App remounts via key)", async () => {
    setInvokeHandlers({
      connection_fetch_consumer_group_lag: ({ groupId }: { groupId: string }) =>
        groupId === "billing-service"
          ? {
              state: "Stable",
              partitions: [
                { topic: "orders", partition: 0, currentOffset: 100, logEndOffset: 100, lag: 0, clientId: null, clientHost: null },
              ],
            }
          : { state: "Stable", partitions: [] },
    });
    const user = userEvent.setup();
    const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });

    // Mirrors how App.tsx renders this panel: keyed by group so switching
    // selection remounts it instead of reusing the previous group's state.
    const { rerender } = render(
      <QueryClientProvider client={client}>
        <ConsumerGroupDetailPanel key="billing-service" connectionId="1" groupId="billing-service" />
      </QueryClientProvider>,
    );
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    expect(await screen.findByText("orders")).toBeInTheDocument();

    rerender(
      <QueryClientProvider client={client}>
        <ConsumerGroupDetailPanel key="payments-service" connectionId="1" groupId="payments-service" />
      </QueryClientProvider>,
    );

    expect(screen.getByText("payments-service")).toBeInTheDocument();
    expect(screen.queryByText("orders")).not.toBeInTheDocument();
  });

  it("applies a warning class at 1,000+ lag and a critical class at 10,000+ lag", async () => {
    setInvokeHandlers({
      connection_fetch_consumer_group_lag: () => ({
        state: "Stable",
        partitions: [
          { topic: "a", partition: 0, currentOffset: 0, logEndOffset: 1000, lag: 1000, clientId: null, clientHost: null },
          { topic: "b", partition: 0, currentOffset: 0, logEndOffset: 10000, lag: 10000, clientId: null, clientHost: null },
          { topic: "c", partition: 0, currentOffset: 0, logEndOffset: 5, lag: 5, clientId: null, clientHost: null },
        ],
      }),
    });
    const user = userEvent.setup();
    renderWithClient(<ConsumerGroupDetailPanel connectionId="1" groupId="billing-service" />);

    await user.click(screen.getByRole("button", { name: "Refresh" }));

    const rows = await screen.findAllByRole("row");
    expect(rows[1]).toHaveClass("lag-row--warning");
    expect(rows[2]).toHaveClass("lag-row--critical");
    expect(rows[3]).not.toHaveClass("lag-row--warning");
    expect(rows[3]).not.toHaveClass("lag-row--critical");
  });
});

describe("ConsumerGroupDetailPanel metrics", () => {
  it("offers Lag, Metrics and Access tabs, and records a sample per Refresh", async () => {
    const { useLagHistoryStore } = await import("./useLagHistoryStore");
    useLagHistoryStore.setState({ byGroup: {} });
    setInvokeHandlers({
      connection_fetch_consumer_group_lag: () => ({
        state: "Stable",
        partitions: [
          { topic: "orders", partition: 0, currentOffset: 1, logEndOffset: 8, lag: 7, clientId: null, clientHost: null },
        ],
      }),
    });
    const user = userEvent.setup();
    renderWithClient(<ConsumerGroupDetailPanel connectionId="1" groupId="g" />);

    expect(screen.getAllByRole("tab").map((t) => t.textContent)).toEqual(["Lag", "Metrics", "Access"]);

    await user.click(screen.getByRole("button", { name: "Refresh" }));
    await screen.findAllByRole("row");
    expect(useLagHistoryStore.getState().byGroup["1::g"]).toHaveLength(1);

    await user.click(screen.getByRole("tab", { name: "Metrics" }));
    expect(await screen.findByRole("img", { name: "Lag by partition" })).toBeInTheDocument();
  });
});
