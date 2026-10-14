import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { setInvokeHandlers } from "../../lib/testInvoke";
import { TopicRatesTab } from "./TopicRatesTab";
import { activeSamplerCount, stopSamplingForConnection } from "./topicRateSampler";
import { MINUTE_MS } from "./topicRates";
import { topicRateKey, useTopicRateStore } from "./useTopicRateStore";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));
vi.mock("recharts", async (importOriginal) => {
  const actual = await importOriginal<typeof import("recharts")>();
  return {
    ...actual,
    // jsdom has no layout, so the real container measures 0×0 and draws nothing.
    ResponsiveContainer: ({ children }: { children: React.ReactElement }) => (
      <div data-testid="chart">{children}</div>
    ),
  };
});

function renderTab() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <TopicRatesTab connectionId="1" topicName="orders" />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  stopSamplingForConnection("1");
  useTopicRateStore.setState({ byTopic: {} });
  // The tab starts sampling; keep that quiet unless a test says otherwise.
  setInvokeHandlers({ connection_list_partitions: () => [], connection_list_consumer_groups: () => [] });
});

afterEach(() => stopSamplingForConnection("1"));

function seedHistory() {
  const now = Date.now();
  const minute = Math.floor(now / MINUTE_MS) - 5;
  useTopicRateStore.setState({
    byTopic: {
      [topicRateKey("1", "orders")]: {
        last: null,
        minutes: {
          [minute]: { produced: 120, consumed: 100 },
          [minute + 1]: { produced: 80, consumed: 90 },
        },
      },
    },
  });
}

describe("TopicRatesTab", () => {
  it("shows a produce chart and a consume chart", async () => {
    renderTab();
    expect(await screen.findByRole("heading", { name: "Produce rate" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Consume rate" })).toBeInTheDocument();
    expect(screen.getByRole("img", { name: "Messages produced over time" })).toBeInTheDocument();
    expect(screen.getByRole("img", { name: "Messages consumed over time" })).toBeInTheDocument();
  });

  it("offers 1h, 1d, 7d and 30d on each card, starting on 1h", async () => {
    renderTab();
    await screen.findByRole("heading", { name: "Produce rate" });
    for (const title of ["Produce rate", "Consume rate"]) {
      const card = screen.getByRole("heading", { name: title }).closest("section") as HTMLElement;
      const options = within(card).getAllByRole("button").map((button) => button.textContent);
      expect(options).toEqual(["1h", "1d", "7d", "30d"]);
      expect(within(card).getByRole("button", { name: "1h" })).toHaveAttribute("aria-pressed", "true");
    }
  });

  it("switches both charts to the chosen range", async () => {
    const user = userEvent.setup();
    renderTab();
    const produceCard = (await screen.findByRole("heading", { name: "Produce rate" })).closest("section") as HTMLElement;

    await user.click(within(produceCard).getByRole("button", { name: "7d" }));

    for (const title of ["Produce rate", "Consume rate"]) {
      const card = screen.getByRole("heading", { name: title }).closest("section") as HTMLElement;
      expect(within(card).getByRole("button", { name: "7d" })).toHaveAttribute("aria-pressed", "true");
      expect(within(card).getByRole("button", { name: "1h" })).toHaveAttribute("aria-pressed", "false");
    }
  });

  it("explains that rates need a second sample while there is no history yet", async () => {
    renderTab();
    await screen.findByRole("heading", { name: "Produce rate" });
    expect(screen.getAllByText(/collecting samples/i).length).toBeGreaterThan(0);
  });

  it("totals the messages in the chosen range once there is history", async () => {
    seedHistory();
    renderTab();

    expect(await screen.findByText("Produced")).toBeInTheDocument();
    expect(screen.getByLabelText("Total produced in range")).toHaveTextContent("200");
    expect(screen.getByLabelText("Total consumed in range")).toHaveTextContent("190");
  });

  it("begins sampling the topic when it is opened", async () => {
    const partitions = vi.fn(() => [{ id: 0, leader: 1, replicas: [1], isr: [1], lowOffset: 0, highOffset: 5 }]);
    setInvokeHandlers({ connection_list_partitions: partitions, connection_list_consumer_groups: () => [] });
    renderTab();

    await vi.waitFor(() => expect(partitions).toHaveBeenCalled());
  });

  it("stops sampling the topic when the tab closes", async () => {
    const partitions = vi.fn(() => [{ id: 0, leader: 1, replicas: [1], isr: [1], lowOffset: 0, highOffset: 5 }]);
    setInvokeHandlers({ connection_list_partitions: partitions, connection_list_consumer_groups: () => [] });
    const { unmount } = renderTab();
    await vi.waitFor(() => expect(activeSamplerCount()).toBe(1));

    unmount();

    expect(activeSamplerCount()).toBe(0);
  });

  it("takes a sample at once when Refresh is pressed", async () => {
    const partitions = vi.fn(() => [{ id: 0, leader: 1, replicas: [1], isr: [1], lowOffset: 0, highOffset: 5 }]);
    setInvokeHandlers({ connection_list_partitions: partitions, connection_list_consumer_groups: () => [] });
    renderTab();
    await vi.waitFor(() => expect(partitions).toHaveBeenCalledTimes(1));

    await userEvent.click(screen.getByRole("button", { name: "Refresh" }));

    await vi.waitFor(() => expect(partitions).toHaveBeenCalledTimes(2));
  });
});
