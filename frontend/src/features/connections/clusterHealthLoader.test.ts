import { describe, expect, it, vi } from "vitest";
import { setInvokeHandlers } from "../../lib/testInvoke";
import { loadClusterHealth, LOAD_CONCURRENCY } from "./clusterHealthLoader";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));

const part = (id: number, leader = 1) => ({ id, leader, replicas: [1, 2], isr: [1, 2], lowOffset: 0, highOffset: 10 });
const brokers = [1, 2].map((id) => ({ id, host: `b${id}`, port: 9092 }));

describe("loadClusterHealth", () => {
  it("analyses every topic and reports progress as each one lands", async () => {
    setInvokeHandlers({
      connection_list_brokers: () => brokers,
      connection_list_topics: () => [
        { name: "a", partitionCount: 1 },
        { name: "b", partitionCount: 1 },
      ],
      connection_list_partitions: () => [part(0)],
      connection_describe_topic_config: () => [{ name: "min.insync.replicas", value: "2" }],
    });
    const progress: number[] = [];

    const result = await loadClusterHealth("1", { onProgress: (loaded) => progress.push(loaded) });

    expect(result.topics.map((t) => t.name).sort()).toEqual(["a", "b"]);
    expect(result.topics[0]).toMatchObject({ replicationFactor: 2, minIsr: 2 });
    expect(result.total).toBe(2);
    expect(result.failed).toBe(0);
    expect(progress[progress.length - 1]).toBe(2);
  });

  it("counts a topic it could not read rather than failing the whole load", async () => {
    setInvokeHandlers({
      connection_list_brokers: () => brokers,
      connection_list_topics: () => [
        { name: "ok", partitionCount: 1 },
        { name: "denied", partitionCount: 1 },
      ],
      connection_list_partitions: ({ topic }: { topic: string }) => {
        if (topic === "denied") throw new Error("authorization error");
        return [part(0)];
      },
      connection_describe_topic_config: () => [],
    });

    const result = await loadClusterHealth("1", {});

    expect(result.topics.map((t) => t.name)).toEqual(["ok"]);
    expect(result.failed).toBe(1);
  });

  it("falls back to default min ISR when only the config read is refused", async () => {
    setInvokeHandlers({
      connection_list_brokers: () => brokers,
      connection_list_topics: () => [{ name: "t", partitionCount: 1 }],
      connection_list_partitions: () => [part(0)],
      connection_describe_topic_config: () => {
        throw new Error("authorization error");
      },
    });

    const result = await loadClusterHealth("1", {});

    expect(result.topics[0].minIsr).toBe(1);
    expect(result.failed).toBe(0);
  });

  it("fails when the topic list itself cannot be read", async () => {
    setInvokeHandlers({
      connection_list_brokers: () => brokers,
      connection_list_topics: () => {
        throw new Error("connection refused");
      },
    });
    await expect(loadClusterHealth("1", {})).rejects.toThrow(/connection refused/);
  });

  it("never has more than the allowed number of topics in flight", async () => {
    let inFlight = 0;
    let peak = 0;
    setInvokeHandlers({
      connection_list_brokers: () => brokers,
      connection_list_topics: () => Array.from({ length: 30 }, (_, i) => ({ name: `t${i}`, partitionCount: 1 })),
      connection_list_partitions: async () => {
        inFlight += 1;
        peak = Math.max(peak, inFlight);
        await new Promise((r) => setTimeout(r, 2));
        inFlight -= 1;
        return [part(0)];
      },
      connection_describe_topic_config: () => [],
    });

    await loadClusterHealth("1", {});

    expect(peak).toBeLessThanOrEqual(LOAD_CONCURRENCY);
    expect(peak).toBeGreaterThan(1);
  });

  it("stops asking for topics once cancelled", async () => {
    const partitions = vi.fn(() => [part(0)]);
    const controller = new AbortController();
    setInvokeHandlers({
      connection_list_brokers: () => brokers,
      connection_list_topics: () => Array.from({ length: 50 }, (_, i) => ({ name: `t${i}`, partitionCount: 1 })),
      connection_list_partitions: () => {
        controller.abort();
        return partitions();
      },
      connection_describe_topic_config: () => [],
    });

    await expect(loadClusterHealth("1", { signal: controller.signal })).rejects.toThrow(/cancel/i);
    expect(partitions.mock.calls.length).toBeLessThan(50);
  });
});
