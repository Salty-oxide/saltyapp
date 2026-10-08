import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { setInvokeHandlers } from "../../lib/testInvoke";
import { TopicMetricsTab } from "./metricsTabs";

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

function renderWithClient(ui: React.ReactElement) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}>{ui}</QueryClientProvider>);
}

beforeEach(() => vi.clearAllMocks());

const part = (id: number, high: number) => ({ id, leader: 1, replicas: [1], isr: [1], lowOffset: 0, highOffset: high });

describe("TopicMetricsTab on wide topics", () => {
  it("lists the busiest partitions, with how far each is above the mean, once there are enough to need it", async () => {
    const wide = Array.from({ length: 400 }, (_, i) => part(i, i === 7 ? 4000 : 1000));
    setInvokeHandlers({ connection_list_partitions: () => wide });
    renderWithClient(<TopicMetricsTab connectionId="1" topicName="orders" />);

    expect(await screen.findByText("Busiest partitions")).toBeInTheDocument();
    const items = screen.getAllByRole("listitem");
    expect(items).toHaveLength(8);
    expect(items[0]).toHaveTextContent("Partition 7");
    expect(items[0]).toHaveTextContent("4,000 messages");
  });

  it("shortens a huge total so the card does not truncate it, keeping the exact figure on hover", async () => {
    const wide = Array.from({ length: 20 }, (_, i) => part(i, 2_000_000));
    setInvokeHandlers({ connection_list_partitions: () => wide });
    renderWithClient(<TopicMetricsTab connectionId="1" topicName="orders" />);

    const card = (await screen.findByText("Total")).parentElement!;
    expect(card).toHaveAttribute("title", "40,000,000");
    expect(card).toHaveTextContent("40M");
  });

  it("leaves out the busiest list on a narrow topic, where the chart already says it all", async () => {
    setInvokeHandlers({ connection_list_partitions: () => [part(0, 90), part(1, 10)] });
    renderWithClient(<TopicMetricsTab connectionId="1" topicName="orders" />);

    await screen.findByLabelText("From");
    expect(screen.queryByText("Busiest partitions")).not.toBeInTheDocument();
  });
});

describe("TopicMetricsTab", () => {
  it("summarises partition skew and flags a hot partition", async () => {
    setInvokeHandlers({ connection_list_partitions: () => [part(0, 90), part(1, 10)] });
    renderWithClient(<TopicMetricsTab connectionId="1" topicName="orders" />);

    expect(await screen.findByText("100")).toBeInTheDocument();
    expect(screen.getByText("90")).toBeInTheDocument();
    expect(screen.getByText("10")).toBeInTheDocument();
    expect(screen.getByText("1.80×")).toBeInTheDocument();
    expect(screen.getByText("High skew")).toBeInTheDocument();
    expect(screen.getByRole("img", { name: "Messages per partition" })).toBeInTheDocument();
  });

  it("calls an even topic balanced", async () => {
    setInvokeHandlers({ connection_list_partitions: () => [part(0, 10), part(1, 10)] });
    renderWithClient(<TopicMetricsTab connectionId="1" topicName="orders" />);

    expect(await screen.findByText("Balanced")).toBeInTheDocument();
  });

  it("says so when the topic has no partitions", async () => {
    setInvokeHandlers({ connection_list_partitions: () => [] });
    renderWithClient(<TopicMetricsTab connectionId="1" topicName="orders" />);

    expect(await screen.findByText(/No partitions found/)).toBeInTheDocument();
  });

  describe("From / To window", () => {
    const FROM = "2026-01-01T00:00";
    const TO = "2026-01-02T00:00";

    it("counts the window on the broker and re-draws the skew from it", async () => {
      const counts = vi.fn(() => [
        { partition: 0, messages: 5 },
        { partition: 1, messages: 15 },
      ]);
      setInvokeHandlers({
        connection_list_partitions: () => [part(0, 90), part(1, 10)],
        connection_count_partition_messages: counts,
      });
      renderWithClient(<TopicMetricsTab connectionId="1" topicName="orders" />);
      await screen.findByText("100");

      fireEvent.change(screen.getByLabelText("From"), { target: { value: FROM } });
      fireEvent.change(screen.getByLabelText("To"), { target: { value: TO } });

      expect(await screen.findByText("20")).toBeInTheDocument();
      expect(screen.getByText("1.50×")).toBeInTheDocument();
      const calls = counts.mock.calls as unknown as [{ fromTimestampMs: number; toTimestampMs: number }][];
      const last = calls[calls.length - 1];
      expect(last[0].fromTimestampMs).toBe(new Date(FROM).getTime());
      expect(last[0].toTimestampMs).toBe(new Date(TO).getTime());
    });

    it("accepts a From alone, leaving the end of the window open", async () => {
      const counts = vi.fn(() => [{ partition: 0, messages: 3 }]);
      setInvokeHandlers({
        connection_list_partitions: () => [part(0, 90)],
        connection_count_partition_messages: counts,
      });
      renderWithClient(<TopicMetricsTab connectionId="1" topicName="orders" />);
      await screen.findByLabelText("From");

      fireEvent.change(screen.getByLabelText("From"), { target: { value: FROM } });

      await waitFor(() => expect(counts).toHaveBeenCalled());
      const [args] = counts.mock.calls[0] as unknown as [{ toTimestampMs: number | null }];
      expect(args.toTimestampMs).toBeNull();
    });

    it("does not ask the broker when To is not after From, and says why", async () => {
      const counts = vi.fn(() => []);
      setInvokeHandlers({
        connection_list_partitions: () => [part(0, 90)],
        connection_count_partition_messages: counts,
      });
      renderWithClient(<TopicMetricsTab connectionId="1" topicName="orders" />);
      await screen.findByLabelText("From");

      fireEvent.change(screen.getByLabelText("From"), { target: { value: TO } });
      fireEvent.change(screen.getByLabelText("To"), { target: { value: FROM } });

      expect(await screen.findByRole("alert")).toHaveTextContent('"To" date must be after "From" date');
      // From alone was a valid window on the way there; the inverted pair never is.
      const calls = counts.mock.calls as unknown as [{ fromTimestampMs: number | null; toTimestampMs: number | null }][];
      expect(calls.some(([a]) => a.fromTimestampMs !== null && a.toTimestampMs !== null)).toBe(false);
    });

    it("shows the broker's error when counting fails", async () => {
      setInvokeHandlers({
        connection_list_partitions: () => [part(0, 90)],
        connection_count_partition_messages: () => {
          throw new Error("broker went away");
        },
      });
      renderWithClient(<TopicMetricsTab connectionId="1" topicName="orders" />);
      await screen.findByLabelText("From");

      fireEvent.change(screen.getByLabelText("From"), { target: { value: FROM } });

      expect(await screen.findByRole("alert")).toHaveTextContent("broker went away");
    });

    it("says so when nothing falls in the window", async () => {
      setInvokeHandlers({
        connection_list_partitions: () => [part(0, 90)],
        connection_count_partition_messages: () => [{ partition: 0, messages: 0 }],
      });
      renderWithClient(<TopicMetricsTab connectionId="1" topicName="orders" />);
      await screen.findByLabelText("From");

      fireEvent.change(screen.getByLabelText("From"), { target: { value: FROM } });

      expect(await screen.findByText(/No messages in this window/)).toBeInTheDocument();
    });

    it("Clear returns to the whole log", async () => {
      setInvokeHandlers({
        connection_list_partitions: () => [part(0, 90), part(1, 10)],
        connection_count_partition_messages: () => [
          { partition: 0, messages: 1 },
          { partition: 1, messages: 1 },
        ],
      });
      const user = userEvent.setup();
      renderWithClient(<TopicMetricsTab connectionId="1" topicName="orders" />);
      await screen.findByText("100");
      expect(screen.queryByRole("button", { name: "Clear window" })).not.toBeInTheDocument();

      fireEvent.change(screen.getByLabelText("From"), { target: { value: FROM } });
      await screen.findByText("2");
      await user.click(screen.getByRole("button", { name: "Clear window" }));

      expect(await screen.findByText("100")).toBeInTheDocument();
      expect(screen.getByLabelText("From")).toHaveValue("");
    });
  });
});
