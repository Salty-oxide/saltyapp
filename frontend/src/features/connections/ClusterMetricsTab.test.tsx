import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { setInvokeHandlers } from "../../lib/testInvoke";
import { ClusterMetricsTab } from "./ClusterMetricsTab";
import { CLUSTER_HEALTH_MAX_AGE_MS, forgetClusterHealth } from "./clusterHealthCache";

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

const part = (id: number, leader: number, replicas: number[], isr: number[], high = 100) => ({
  id,
  leader,
  replicas,
  isr,
  lowOffset: 0,
  highOffset: high,
});

/** Three topics: healthy, under-replicated, and one with its ISR below min ISR and a hot partition. */
function broker() {
  const layouts: Record<string, ReturnType<typeof part>[]> = {
    healthy: [part(0, 1, [1, 2, 3], [1, 2, 3]), part(1, 2, [2, 3, 1], [2, 3, 1])],
    lagging: [part(0, 1, [1, 2, 3], [1, 2])],
    broken: [part(0, 1, [1, 2, 3], [1], 900), part(1, 1, [1, 2, 3], [1], 100)],
  };
  setInvokeHandlers({
    connection_list_brokers: () => [1, 2, 3].map((id) => ({ id, host: `b${id}`, port: 9092 })),
    connection_list_topics: () => Object.entries(layouts).map(([name, p]) => ({ name, partitionCount: p.length })),
    connection_list_partitions: ({ topic }: { topic: string }) => layouts[topic],
    connection_describe_topic_config: () => [{ name: "min.insync.replicas", value: "2" }],
  });
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.useRealTimers();
  // Results are reused for a minute; every case starts from a cluster nobody has analysed.
  forgetClusterHealth("1");
});

describe("ClusterMetricsTab", () => {
  it("shows total topics, partitions and a health score once analysed", async () => {
    broker();
    render(<ClusterMetricsTab connectionId="1" />);

    expect(await screen.findByLabelText("Total topics")).toHaveTextContent("3");
    expect(screen.getByLabelText("Total partitions")).toHaveTextContent("5");
    const score = screen.getByLabelText("Cluster health score");
    expect(score).toHaveTextContent(/\d+/);
    expect(Number(score.textContent!.match(/\d+/)![0])).toBeLessThan(100);
  });

  it("shows how topics are distributed across health states as a progress bar", async () => {
    broker();
    render(<ClusterMetricsTab connectionId="1" />);

    const bar = await screen.findByRole("img", { name: "Topic health distribution" });
    expect(bar).toBeInTheDocument();
    expect(screen.getByText(/1 healthy/i)).toBeInTheDocument();
    expect(screen.getByText(/1 under-replicated/i)).toBeInTheDocument();
    expect(screen.getByText(/1 below min ISR/i)).toBeInTheDocument();
  });

  it("has a Risk analysis row with the three cards", async () => {
    broker();
    render(<ClusterMetricsTab connectionId="1" />);

    expect(await screen.findByRole("heading", { name: "Risk analysis" })).toBeInTheDocument();
    for (const title of ["Data loss risk", "Cluster efficiency", "Load imbalance risk"]) {
      expect(screen.getByRole("heading", { name: title })).toBeInTheDocument();
    }
    expect(screen.getByRole("img", { name: "Under-replicated topics by replication factor and min ISR" })).toBeInTheDocument();
    expect(screen.getByRole("img", { name: "Topics by leader distribution" })).toBeInTheDocument();
    expect(screen.getByRole("img", { name: "Topics by message skew ratio" })).toBeInTheDocument();
  });

  it("lists every topic in the health overview, riskiest first, with RF, min ISR and skew", async () => {
    broker();
    render(<ClusterMetricsTab connectionId="1" />);

    const table = await screen.findByRole("table", { name: "Topic health overview" });
    expect(within(table).getAllByRole("columnheader").map((h) => h.textContent)).toEqual([
      "Topic",
      "Replication factor",
      "Min in-sync replicas",
      "Partition skew",
    ]);
    const rows = within(table).getAllByRole("row").slice(1);
    expect(rows.map((row) => within(row).getAllByRole("cell")[0].textContent)).toEqual(["broken", "lagging", "healthy"]);
    const broken = within(rows[0]).getAllByRole("cell");
    expect(broken[1]).toHaveTextContent("3");
    expect(broken[2]).toHaveTextContent("2");
    expect(broken[3]).toHaveTextContent("1.80×");
  });

  it("does not have an average message size column", async () => {
    broker();
    render(<ClusterMetricsTab connectionId="1" />);
    await screen.findByRole("table", { name: "Topic health overview" });
    expect(screen.queryByText(/avg.*size/i)).not.toBeInTheDocument();
  });

  it("shows progress while topics are being read", async () => {
    setInvokeHandlers({
      connection_list_brokers: () => [{ id: 1, host: "b", port: 9092 }],
      connection_list_topics: () => [{ name: "t", partitionCount: 1 }],
      connection_list_partitions: () => new Promise(() => {}),
      connection_describe_topic_config: () => [],
    });
    render(<ClusterMetricsTab connectionId="1" />);

    expect(await screen.findByRole("progressbar", { name: "Analysing topics" })).toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });

  it("says how many topics could not be read", async () => {
    setInvokeHandlers({
      connection_list_brokers: () => [{ id: 1, host: "b", port: 9092 }],
      connection_list_topics: () => [
        { name: "ok", partitionCount: 1 },
        { name: "denied", partitionCount: 1 },
      ],
      connection_list_partitions: ({ topic }: { topic: string }) => {
        if (topic === "denied") throw new Error("authorization error");
        return [part(0, 1, [1], [1])];
      },
      connection_describe_topic_config: () => [],
    });
    render(<ClusterMetricsTab connectionId="1" />);

    expect(await screen.findByText(/1 topic could not be read/i)).toBeInTheDocument();
  });

  it("reports a failure to list topics, and Refresh tries again", async () => {
    let attempts = 0;
    setInvokeHandlers({
      connection_list_brokers: () => [{ id: 1, host: "b", port: 9092 }],
      connection_list_topics: () => {
        attempts += 1;
        if (attempts === 1) throw new Error("connection refused");
        return [];
      },
    });
    const user = userEvent.setup();
    render(<ClusterMetricsTab connectionId="1" />);

    expect(await screen.findByRole("alert")).toHaveTextContent(/connection refused/i);

    await user.click(screen.getByRole("button", { name: "Refresh" }));
    expect(await screen.findByLabelText("Total topics")).toHaveTextContent("0");
  });

  describe("reuse of the last analysis", () => {
    function countingBroker() {
      const topics = vi.fn(() => [{ name: "orders", partitionCount: 1 }]);
      setInvokeHandlers({
        connection_list_brokers: () => [{ id: 1, host: "b", port: 9092 }],
        connection_list_topics: topics,
        connection_list_partitions: () => [part(0, 1, [1], [1])],
        connection_describe_topic_config: () => [],
      });
      return topics;
    }

    it("does not read the cluster again when the tab is reopened within the window", async () => {
      const topics = countingBroker();
      const first = render(<ClusterMetricsTab connectionId="1" />);
      await screen.findByLabelText("Total topics");
      first.unmount();

      render(<ClusterMetricsTab connectionId="1" />);

      expect(await screen.findByLabelText("Total topics")).toHaveTextContent("1");
      expect(topics).toHaveBeenCalledTimes(1);
    });

    it("says when the figures were analysed", async () => {
      countingBroker();
      render(<ClusterMetricsTab connectionId="1" />);
      expect(await screen.findByText(/Analysed at /)).toBeInTheDocument();
    });

    it("reads again on Refresh, however recent the last analysis", async () => {
      const topics = countingBroker();
      const user = userEvent.setup();
      render(<ClusterMetricsTab connectionId="1" />);
      await screen.findByLabelText("Total topics");

      await user.click(screen.getByRole("button", { name: "Refresh" }));

      await vi.waitFor(() => expect(topics).toHaveBeenCalledTimes(2));
    });

    it("reads again once the last analysis is older than the window", async () => {
      const topics = countingBroker();
      const now = Date.now();
      const spy = vi.spyOn(Date, "now").mockReturnValue(now);
      const first = render(<ClusterMetricsTab connectionId="1" />);
      await screen.findByLabelText("Total topics");
      first.unmount();

      spy.mockReturnValue(now + CLUSTER_HEALTH_MAX_AGE_MS + 1);
      render(<ClusterMetricsTab connectionId="1" />);

      await vi.waitFor(() => expect(topics).toHaveBeenCalledTimes(2));
      spy.mockRestore();
    });

    it("does not reuse one cluster's analysis for another", async () => {
      const topics = countingBroker();
      const first = render(<ClusterMetricsTab connectionId="1" />);
      await screen.findByLabelText("Total topics");
      first.unmount();

      render(<ClusterMetricsTab connectionId="2" />);

      await vi.waitFor(() => expect(topics).toHaveBeenCalledTimes(2));
      forgetClusterHealth("2");
    });
  });

  describe("a long topic list", () => {
    function manyTopics(count: number) {
      const names = Array.from({ length: count }, (_, i) => `topic-${String(i).padStart(4, "0")}`);
      setInvokeHandlers({
        connection_list_brokers: () => [{ id: 1, host: "b", port: 9092 }],
        connection_list_topics: () => names.map((name) => ({ name, partitionCount: 1 })),
        connection_list_partitions: () => [part(0, 1, [1], [1])],
        connection_describe_topic_config: () => [],
      });
      return names;
    }

    it("renders every row when the list is short", async () => {
      manyTopics(40);
      render(<ClusterMetricsTab connectionId="1" />);

      const table = await screen.findByRole("table", { name: "Topic health overview" });
      expect(within(table).getAllByRole("row")).toHaveLength(41);
    });

    it("renders only the rows near the viewport once there are many, leaving the rest to scroll", async () => {
      manyTopics(400);
      render(<ClusterMetricsTab connectionId="1" />);

      const table = await screen.findByRole("table", { name: "Topic health overview" });
      const rendered = within(table).getAllByRole("row").length;
      expect(rendered).toBeLessThan(60);
      expect(within(table).getByText("topic-0000")).toBeInTheDocument();
      expect(within(table).queryByText("topic-0399")).not.toBeInTheDocument();
    });

    it("still counts every topic in the figures above the table", async () => {
      manyTopics(400);
      render(<ClusterMetricsTab connectionId="1" />);

      expect(await screen.findByLabelText("Total topics")).toHaveTextContent("400");
    });
  });

  it("says so when the cluster has no topics", async () => {
    setInvokeHandlers({
      connection_list_brokers: () => [{ id: 1, host: "b", port: 9092 }],
      connection_list_topics: () => [],
    });
    render(<ClusterMetricsTab connectionId="1" />);
    expect(await screen.findByText(/no topics/i)).toBeInTheDocument();
  });
});
