import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { setInvokeHandlers } from "../../lib/testInvoke";
import { ClusterResourceTree } from "./ClusterResourceTree";
import { useWorkspaceSelectionStore } from "../workspace/useWorkspaceSelectionStore";
import { useTreeUiStore } from "./useTreeUiStore";
import { usePartitionPanelTabStore } from "./usePartitionPanelTabStore";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));

function renderWithClient(ui: React.ReactElement) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}>{ui}</QueryClientProvider>);
}

beforeEach(() => {
  vi.clearAllMocks();
  useWorkspaceSelectionStore.setState({ selection: null });
  useTreeUiStore.setState({ expanded: {}, searchText: {}, hideEmptyConsumerGroups: {} });
  usePartitionPanelTabStore.setState({ activeByTab: {} });
});

describe("ClusterResourceTree", () => {
  it("warns under Topics when the topic listing fails", async () => {
    setInvokeHandlers({
      connection_list_topics: () => {
        throw new Error("Local: Timed out");
      },
    });
    renderWithClient(<ClusterResourceTree connectionId="1" />);

    expect(await screen.findByTestId("category-Topics-warning")).toBeInTheDocument();
  });


  it("warns under Consumers when the group listing fails, and leaves the other categories alone", async () => {
    // Listing consumer groups needs ACLs of its own, so it can be refused on
    // a cluster whose topics and brokers read perfectly well. That must stay
    // a note on this one category, not a failure of the whole cluster.
    setInvokeHandlers({
      connection_list_topics: () => [{ name: "orders", partitionCount: 1 }],
      connection_list_consumer_groups: () => {
        throw new Error("authentication error: failed to fetch consumer group list");
      },
    });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);

    // The group listing is lazy, so the refusal only surfaces once the
    // category is opened and the call is actually made.
    await user.click(screen.getByTestId("category-Consumers"));

    expect(await screen.findByTestId("category-Consumers-warning")).toBeInTheDocument();
    expect(screen.queryByTestId("category-Topics-warning")).not.toBeInTheDocument();

    await user.click(screen.getByTestId("category-Topics"));
    expect(await screen.findByText("orders")).toBeInTheDocument();
  });


  it("renders Brokers, Topics, and Consumers category headers", () => {
    setInvokeHandlers({});
    renderWithClient(<ClusterResourceTree connectionId="1" />);

    expect(screen.getByTestId("category-Brokers")).toBeInTheDocument();
    expect(screen.getByTestId("category-Topics")).toBeInTheDocument();
    expect(screen.getByTestId("category-Consumers")).toBeInTheDocument();
  });

  it("eagerly fetches brokers and topics on mount, without needing any category expanded first", async () => {
    const listBrokers = vi.fn(() => []);
    const listTopics = vi.fn(() => []);
    setInvokeHandlers({
      connection_list_brokers: listBrokers,
      connection_list_topics: listTopics,
    });
    renderWithClient(<ClusterResourceTree connectionId="1" />);

    await waitFor(() => expect(listBrokers).toHaveBeenCalledWith({ id: "1", refresh: false, readTimeoutMs: 10_000 }));
    await waitFor(() => expect(listTopics).toHaveBeenCalledWith({ id: "1", refresh: false, readTimeoutMs: 10_000 }));
  });

  // Consumer groups are the odd one out: the listing is the most expensive of
  // the three on a busy cluster, needs ACLs the other two don't (Describe on
  // Group), and is the category a user may never open at all. Paying for it
  // on every connect — and failing visibly when the principal isn't allowed
  // it — is work nobody asked for.
  it("does not list consumer groups until the Consumers category is opened", async () => {
    const listConsumerGroups = vi.fn(() => []);
    setInvokeHandlers({
      connection_list_topics: () => [],
      connection_list_consumer_groups: listConsumerGroups,
    });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);

    await waitFor(() => expect(screen.getByTestId("category-Consumers")).toBeInTheDocument());
    expect(listConsumerGroups).not.toHaveBeenCalled();

    await user.click(screen.getByTestId("category-Consumers"));

    await waitFor(() => expect(listConsumerGroups).toHaveBeenCalledWith({ id: "1", refresh: false, readTimeoutMs: 10_000 }));
  });

  // The listings are held indefinitely once fetched, so re-opening a
  // category is the user's way of saying "show me what's there now". Without
  // this there is no way to pick up a topic created since the app started,
  // short of reconnecting.
  it("re-lists topics each time the Topics category is re-opened", async () => {
    const listTopics = vi.fn(() => [{ name: "orders", partitionCount: 1 }]);
    setInvokeHandlers({ connection_list_topics: listTopics });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);
    await waitFor(() => expect(listTopics).toHaveBeenCalledTimes(1));

    await user.click(screen.getByTestId("category-Topics"));
    await waitFor(() => expect(listTopics).toHaveBeenCalledTimes(2));

    await user.click(screen.getByTestId("category-Topics"));
    await user.click(screen.getByTestId("category-Topics"));

    await waitFor(() => expect(listTopics).toHaveBeenCalledTimes(3));
  });

  it("re-lists consumer groups when Consumers is re-opened, having fetched none before the first open", async () => {
    const listConsumerGroups = vi.fn(() => [{ groupId: "g1", state: "Stable" }]);
    setInvokeHandlers({
      connection_list_topics: () => [],
      connection_list_consumer_groups: listConsumerGroups,
    });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);

    await user.click(screen.getByTestId("category-Consumers"));
    await waitFor(() => expect(listConsumerGroups).toHaveBeenCalledTimes(1));

    await user.click(screen.getByTestId("category-Consumers"));
    await user.click(screen.getByTestId("category-Consumers"));

    await waitFor(() => expect(listConsumerGroups).toHaveBeenCalledTimes(2));
  });

  it("lists consumer groups exactly once when Consumers is first opened", async () => {
    const listConsumerGroups = vi.fn(() => [{ groupId: "g1", state: "Stable" }]);
    setInvokeHandlers({
      connection_list_topics: () => [],
      connection_list_consumer_groups: listConsumerGroups,
    });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);

    await user.click(screen.getByTestId("category-Consumers"));

    expect(await screen.findByText("g1")).toBeInTheDocument();
    expect(listConsumerGroups).toHaveBeenCalledTimes(1);
  });

  it("fetches and shows brokers once Brokers is expanded", async () => {
    const listBrokers = vi.fn(() => [{ id: 1, host: "broker1", port: 9092 }]);
    setInvokeHandlers({ connection_list_brokers: listBrokers });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);

    await user.click(screen.getByTestId("category-Brokers"));

    await waitFor(() => expect(listBrokers).toHaveBeenCalledWith({ id: "1", refresh: false, readTimeoutMs: 10_000 }));
    expect(await screen.findByText("1 — broker1:9092")).toBeInTheDocument();
  });

  it("selects a broker into the workspace store when clicked", async () => {
    setInvokeHandlers({ connection_list_brokers: () => [{ id: 1, host: "broker1", port: 9092 }] });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);
    await user.click(screen.getByTestId("category-Brokers"));
    await screen.findByText("1 — broker1:9092");

    await user.click(screen.getByText("1 — broker1:9092"));

    expect(useWorkspaceSelectionStore.getState().selection).toEqual({
      type: "broker",
      connectionId: "1",
      brokerId: 1,
    });
  });

  it("fetches and shows topics once Topics is expanded", async () => {
    const listTopics = vi.fn(() => [{ name: "orders", partitionCount: 3 }]);
    setInvokeHandlers({ connection_list_topics: listTopics });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);

    await user.click(screen.getByTestId("category-Topics"));

    await waitFor(() => expect(listTopics).toHaveBeenCalledWith({ id: "1", refresh: false, readTimeoutMs: 10_000 }));
    expect(await screen.findByText("orders")).toBeInTheDocument();
  });

  // The listing is cached by the backend for 24 h; opening the category is how
  // the user asks for a fresh one, so that read must skip the cache.
  it("re-reads Topics from the broker (refresh) when the category is expanded", async () => {
    const listTopics = vi.fn(() => [{ name: "orders", partitionCount: 3 }]);
    setInvokeHandlers({ connection_list_topics: listTopics });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);
    await waitFor(() => expect(listTopics).toHaveBeenCalledWith({ id: "1", refresh: false, readTimeoutMs: 10_000 }));

    await user.click(screen.getByTestId("category-Topics"));

    await waitFor(() => expect(listTopics).toHaveBeenLastCalledWith({ id: "1", refresh: true, readTimeoutMs: 10_000 }));
  });

  it("keeps the topic search box in its own sticky band, outside the list it filters", async () => {
    setInvokeHandlers({ connection_list_topics: () => [{ name: "orders", partitionCount: 3 }] });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);

    await user.click(screen.getByTestId("category-Topics"));

    const search = await screen.findByLabelText("Search Topics");
    expect(search.parentElement).toHaveClass("resource-category-search-row");
    expect(search.closest(".resource-item-list")).toBeNull();
  });

  it("selects a topic into the workspace store when clicked", async () => {
    setInvokeHandlers({ connection_list_topics: () => [{ name: "orders", partitionCount: 3 }] });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);
    await user.click(screen.getByTestId("category-Topics"));
    await screen.findByText("orders");

    await user.click(screen.getByText("orders"));

    expect(useWorkspaceSelectionStore.getState().selection).toEqual({
      type: "topic",
      connectionId: "1",
      topicName: "orders",
    });
  });

  it("does not fetch a topic's partitions until it is expanded", async () => {
    const listPartitions = vi.fn(() => [{ id: 0, leader: 1, replicas: [1], isr: [1], lowOffset: 0, highOffset: 0 }]);
    setInvokeHandlers({
      connection_list_topics: () => [{ name: "orders", partitionCount: 3 }],
      connection_list_partitions: listPartitions,
    });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);
    await user.click(screen.getByTestId("category-Topics"));
    await screen.findByText("orders");

    expect(listPartitions).not.toHaveBeenCalled();
  });

  it("expands a topic to show its partitions when its caret is clicked", async () => {
    setInvokeHandlers({
      connection_list_topics: () => [{ name: "orders", partitionCount: 3 }],
      connection_list_partitions: () => [
        { id: 0, leader: 1, replicas: [1], isr: [1], lowOffset: 0, highOffset: 10 },
        { id: 1, leader: 2, replicas: [2], isr: [2], lowOffset: 0, highOffset: 5 },
      ],
    });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);
    await user.click(screen.getByTestId("category-Topics"));
    await screen.findByText("orders");

    await user.click(screen.getByLabelText("Expand orders"));

    expect(await screen.findByText("Partition 0")).toBeInTheDocument();
    expect(screen.getByText("Partition 1")).toBeInTheDocument();
  });

  it("selects a partition into the workspace store when clicked", async () => {
    setInvokeHandlers({
      connection_list_topics: () => [{ name: "orders", partitionCount: 3 }],
      connection_list_partitions: () => [
        { id: 0, leader: 1, replicas: [1], isr: [1], lowOffset: 0, highOffset: 10 },
      ],
    });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);
    await user.click(screen.getByTestId("category-Topics"));
    await screen.findByText("orders");
    await user.click(screen.getByLabelText("Expand orders"));
    await screen.findByText("Partition 0");

    await user.click(screen.getByText("Partition 0"));

    expect(useWorkspaceSelectionStore.getState().selection).toEqual({
      type: "partition",
      connectionId: "1",
      topicName: "orders",
      partitionId: 0,
    });
  });

  it("offers Publish messages… on a partition's context menu", async () => {
    setInvokeHandlers({
      connection_list_topics: () => [{ name: "orders", partitionCount: 3 }],
      connection_list_partitions: () => [
        { id: 0, leader: 1, replicas: [1], isr: [1], lowOffset: 0, highOffset: 10 },
        { id: 1, leader: 1, replicas: [1], isr: [1], lowOffset: 0, highOffset: 10 },
      ],
    });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);
    await user.click(screen.getByTestId("category-Topics"));
    await screen.findByText("orders");
    await user.click(screen.getByLabelText("Expand orders"));
    await screen.findByText("Partition 1");

    fireEvent.contextMenu(screen.getByText("Partition 1"));

    expect(await screen.findByRole("menuitem", { name: "Publish messages…" })).toBeInTheDocument();
  });

  it("selects the right-clicked partition and opens its Publish tab", async () => {
    // Both halves matter: without the sub-tab, the menu item would select a
    // partition and leave the panel on Data — doing half of what it says.
    setInvokeHandlers({
      connection_list_topics: () => [{ name: "orders", partitionCount: 3 }],
      connection_list_partitions: () => [
        { id: 0, leader: 1, replicas: [1], isr: [1], lowOffset: 0, highOffset: 10 },
        { id: 1, leader: 1, replicas: [1], isr: [1], lowOffset: 0, highOffset: 10 },
      ],
    });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);
    await user.click(screen.getByTestId("category-Topics"));
    await screen.findByText("orders");
    await user.click(screen.getByLabelText("Expand orders"));
    await screen.findByText("Partition 1");

    fireEvent.contextMenu(screen.getByText("Partition 1"));
    await user.click(await screen.findByRole("menuitem", { name: "Publish messages…" }));

    expect(useWorkspaceSelectionStore.getState().selection).toEqual({
      type: "partition",
      connectionId: "1",
      topicName: "orders",
      partitionId: 1,
    });
    expect(usePartitionPanelTabStore.getState().get(null)).toBe("publish");
  });

  it("closes the partition context menu after the item is chosen", async () => {
    setInvokeHandlers({
      connection_list_topics: () => [{ name: "orders", partitionCount: 1 }],
      connection_list_partitions: () => [
        { id: 0, leader: 1, replicas: [1], isr: [1], lowOffset: 0, highOffset: 10 },
      ],
    });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);
    await user.click(screen.getByTestId("category-Topics"));
    await screen.findByText("orders");
    await user.click(screen.getByLabelText("Expand orders"));
    await screen.findByText("Partition 0");

    fireEvent.contextMenu(screen.getByText("Partition 0"));
    await user.click(await screen.findByRole("menuitem", { name: "Publish messages…" }));

    await waitFor(() =>
      expect(screen.queryByRole("menuitem", { name: "Publish messages…" })).not.toBeInTheDocument(),
    );
  });

  it("marks the selected partition row visually", async () => {
    setInvokeHandlers({
      connection_list_topics: () => [{ name: "orders", partitionCount: 3 }],
      connection_list_partitions: () => [
        { id: 0, leader: 1, replicas: [1], isr: [1], lowOffset: 0, highOffset: 10 },
      ],
    });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);
    await user.click(screen.getByTestId("category-Topics"));
    await screen.findByText("orders");
    await user.click(screen.getByLabelText("Expand orders"));
    await screen.findByText("Partition 0");

    await user.click(screen.getByText("Partition 0"));

    expect(screen.getByTestId("resource-item-partition-orders-0").className).toContain(
      "topic-partition-item--selected",
    );
  });

  it("does not select the topic as a side effect of expanding it", async () => {
    setInvokeHandlers({
      connection_list_topics: () => [{ name: "orders", partitionCount: 3 }],
      connection_list_partitions: () => [],
    });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);
    await user.click(screen.getByTestId("category-Topics"));
    await screen.findByText("orders");

    await user.click(screen.getByLabelText("Expand orders"));

    expect(useWorkspaceSelectionStore.getState().selection).toBeNull();
  });

  it("fetches and shows consumer groups once Consumers is expanded", async () => {
    const listGroups = vi.fn(() => [{ groupId: "billing", state: "Stable" }]);
    setInvokeHandlers({ connection_list_consumer_groups: listGroups });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);

    await user.click(screen.getByTestId("category-Consumers"));

    await waitFor(() => expect(listGroups).toHaveBeenCalledWith({ id: "1", refresh: false, readTimeoutMs: 10_000 }));
    expect(await screen.findByText("billing")).toBeInTheDocument();
  });

  it("selects a consumer group into the workspace store when clicked", async () => {
    setInvokeHandlers({ connection_list_consumer_groups: () => [{ groupId: "billing", state: "Stable" }] });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);
    await user.click(screen.getByTestId("category-Consumers"));
    await screen.findByText("billing");

    await user.click(screen.getByText("billing"));

    expect(useWorkspaceSelectionStore.getState().selection).toEqual({
      type: "consumerGroup",
      connectionId: "1",
      groupId: "billing",
    });
  });

  it("hides empty consumer groups after selecting that context menu item, and shows them again after toggling back", async () => {
    setInvokeHandlers({
      connection_list_consumer_groups: () => [
        { groupId: "billing", state: "Stable" },
        { groupId: "stale-job", state: "Empty" },
      ],
    });
    const user = userEvent.setup();
    renderWithClient(<ClusterResourceTree connectionId="1" />);
    await user.click(screen.getByTestId("category-Consumers"));
    await screen.findByText("billing");
    expect(screen.getByText("stale-job")).toBeInTheDocument();

    fireEvent.contextMenu(screen.getByTestId("category-Consumers"));
    await user.click(screen.getByText("Hide empty consumer groups"));

    expect(screen.getByText("billing")).toBeInTheDocument();
    expect(screen.queryByText("stale-job")).not.toBeInTheDocument();

    fireEvent.contextMenu(screen.getByTestId("category-Consumers"));
    await user.click(screen.getByText("Show all consumer groups"));

    expect(screen.getByText("stale-job")).toBeInTheDocument();
  });
});
