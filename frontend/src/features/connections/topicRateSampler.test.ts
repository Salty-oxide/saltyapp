import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { focusManager } from "@tanstack/react-query";
import { setInvokeHandlers } from "../../lib/testInvoke";
import {
  activeSamplerCount,
  DISCOVERY_INTERVAL_MS,
  ensureSampling,
  MAX_ACTIVE_SAMPLERS,
  SAMPLE_INTERVAL_MS,
  stopSamplingForConnection,
  touchSampling,
  UNVIEWED_STOP_AFTER_MS,
  takeSample,
} from "./topicRateSampler";
import { topicRateKey, useTopicRateStore } from "./useTopicRateStore";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));

const part = (id: number, high: number) => ({ id, leader: 1, replicas: [1], isr: [1], lowOffset: 0, highOffset: high });
const lagRow = (topic: string, partition: number, currentOffset: number | null) => ({
  topic,
  partition,
  currentOffset,
  logEndOffset: 0,
  lag: 0,
  clientId: null,
  clientHost: null,
});

beforeEach(() => {
  useTopicRateStore.setState({ byTopic: {} });
});

describe("takeSample", () => {
  it("sums the topic's high watermarks and each active group's committed offsets on that topic", async () => {
    const lagCalls: string[] = [];
    setInvokeHandlers({
      connection_list_partitions: () => [part(0, 100), part(1, 250)],
      connection_list_consumer_groups: () => [
        { groupId: "billing", state: "Stable" },
        { groupId: "idle", state: "Empty" },
        { groupId: "other-topic", state: "Stable" },
      ],
      connection_fetch_consumer_group_lag: ({ groupId }: { groupId: string }) => {
        lagCalls.push(groupId);
        if (groupId === "billing") {
          return { state: "Stable", partitions: [lagRow("orders", 0, 90), lagRow("orders", 1, 200), lagRow("x", 0, 5)] };
        }
        return { state: "Stable", partitions: [lagRow("elsewhere", 0, 7)] };
      },
    });

    const sample = await takeSample("1", "orders", { knownGroups: null }, 1_000);

    expect(sample.produced).toBe(350);
    expect(sample.consumedByGroup).toEqual({ billing: 290 });
    // Empty groups have no consumer running, so they are never asked.
    expect(lagCalls).not.toContain("idle");
  });

  it("only asks the groups it already found on later samples", async () => {
    const lagCalls: string[] = [];
    setInvokeHandlers({
      connection_list_partitions: () => [part(0, 10)],
      connection_list_consumer_groups: () => {
        throw new Error("listing should not be repeated");
      },
      connection_fetch_consumer_group_lag: ({ groupId }: { groupId: string }) => {
        lagCalls.push(groupId);
        return { state: "Stable", partitions: [lagRow("orders", 0, 4)] };
      },
    });

    const sample = await takeSample("1", "orders", { knownGroups: ["billing"], discoveredAt: 1_000 }, 1_000 + 1);

    expect(lagCalls).toEqual(["billing"]);
    expect(sample.consumedByGroup).toEqual({ billing: 4 });
  });

  it("looks for groups again once the discovery interval has passed", async () => {
    const listing = vi.fn(() => [{ groupId: "new-group", state: "Stable" }]);
    setInvokeHandlers({
      connection_list_partitions: () => [part(0, 10)],
      connection_list_consumer_groups: listing,
      connection_fetch_consumer_group_lag: () => ({ state: "Stable", partitions: [lagRow("orders", 0, 1)] }),
    });

    const sample = await takeSample(
      "1",
      "orders",
      { knownGroups: ["billing"], discoveredAt: 0 },
      DISCOVERY_INTERVAL_MS + 1,
    );

    expect(listing).toHaveBeenCalled();
    expect(Object.keys(sample.consumedByGroup ?? {})).toEqual(["new-group"]);
  });

  it("reports consumption as unknown, but still the produce side, when the group listing is refused", async () => {
    setInvokeHandlers({
      connection_list_partitions: () => [part(0, 42)],
      connection_list_consumer_groups: () => {
        throw new Error("authorization error: not allowed to list groups");
      },
    });

    const sample = await takeSample("1", "orders", { knownGroups: null }, 1_000);

    expect(sample.produced).toBe(42);
    expect(sample.consumedByGroup).toBeNull();
  });

  it("does not ask for the group listing again on the very next sample after it was refused", async () => {
    const listing = vi.fn(() => {
      throw new Error("authorization error: not allowed to list groups");
    });
    setInvokeHandlers({ connection_list_partitions: () => [part(0, 1)], connection_list_consumer_groups: listing });
    const discovery = { knownGroups: null } as { knownGroups: string[] | null; discoveredAt?: number; listingFailedAt?: number };

    await takeSample("1", "orders", discovery, 1_000);
    await takeSample("1", "orders", discovery, 31_000);

    expect(listing).toHaveBeenCalledTimes(1);
  });

  it("fails when the topic's partitions cannot be read", async () => {
    setInvokeHandlers({
      connection_list_partitions: () => {
        throw new Error("connection refused");
      },
    });
    await expect(takeSample("1", "orders", { knownGroups: null }, 1_000)).rejects.toThrow(/connection refused/);
  });
});

describe("ensureSampling", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(1_700_000_000_000);
    focusManager.setFocused(true);
  });
  afterEach(() => {
    stopSamplingForConnection("1");
    focusManager.setFocused(undefined);
    vi.useRealTimers();
  });

  function brokerAt(high: () => number) {
    setInvokeHandlers({
      connection_list_partitions: () => [part(0, high())],
      connection_list_consumer_groups: () => [],
    });
  }

  it("takes a sample at once and then one per interval, and a topic is only sampled once however often it is asked", async () => {
    let high = 100;
    const partitions = vi.fn(() => [part(0, high)]);
    setInvokeHandlers({ connection_list_partitions: partitions, connection_list_consumer_groups: () => [] });

    ensureSampling("1", "orders");
    ensureSampling("1", "orders");
    await vi.advanceTimersByTimeAsync(0);
    expect(partitions).toHaveBeenCalledTimes(1);

    high = 160;
    await vi.advanceTimersByTimeAsync(SAMPLE_INTERVAL_MS);
    expect(partitions).toHaveBeenCalledTimes(2);

    const history = useTopicRateStore.getState().byTopic[topicRateKey("1", "orders")];
    expect(Object.values(history.minutes).reduce((s, m) => s + m.produced, 0)).toBe(60);
  });

  it("skips ticks while the app is not in front", async () => {
    const partitions = vi.fn(() => [part(0, 1)]);
    setInvokeHandlers({ connection_list_partitions: partitions, connection_list_consumer_groups: () => [] });
    focusManager.setFocused(false);

    ensureSampling("1", "orders");
    await vi.advanceTimersByTimeAsync(SAMPLE_INTERVAL_MS * 3);

    expect(partitions).not.toHaveBeenCalled();
  });

  it("stops for a connection that is cleared", async () => {
    const partitions = vi.fn(() => [part(0, 1)]);
    setInvokeHandlers({ connection_list_partitions: partitions, connection_list_consumer_groups: () => [] });
    ensureSampling("1", "orders");
    await vi.advanceTimersByTimeAsync(0);

    stopSamplingForConnection("1");
    await vi.advanceTimersByTimeAsync(SAMPLE_INTERVAL_MS * 3);

    expect(partitions).toHaveBeenCalledTimes(1);
  });

  it("keeps no more than the allowed number of topics sampled, dropping the one viewed longest ago", async () => {
    const partitions = vi.fn(() => [part(0, 1)]);
    setInvokeHandlers({ connection_list_partitions: partitions, connection_list_consumer_groups: () => [] });

    for (let i = 0; i < MAX_ACTIVE_SAMPLERS; i++) {
      ensureSampling("1", `topic-${i}`);
      await vi.advanceTimersByTimeAsync(1_000);
    }
    expect(activeSamplerCount()).toBe(MAX_ACTIVE_SAMPLERS);

    // topic-0 is looked at again, so topic-1 is now the one viewed longest ago.
    touchSampling("1", "topic-0");
    ensureSampling("1", "topic-new");

    expect(activeSamplerCount()).toBe(MAX_ACTIVE_SAMPLERS);
    partitions.mockClear();
    await vi.advanceTimersByTimeAsync(SAMPLE_INTERVAL_MS);
    const sampled = partitions.mock.calls.map((call) => (call as unknown as [{ topic: string }])[0].topic);
    expect(sampled).toContain("topic-0");
    expect(sampled).toContain("topic-new");
    expect(sampled).not.toContain("topic-1");
  });

  it("stops sampling a topic nobody has looked at for a while, and keeps one that is being viewed", async () => {
    const partitions = vi.fn(() => [part(0, 1)]);
    setInvokeHandlers({ connection_list_partitions: partitions, connection_list_consumer_groups: () => [] });
    ensureSampling("1", "watched");
    ensureSampling("1", "forgotten");
    await vi.advanceTimersByTimeAsync(0);

    // The open tab keeps saying it is being looked at; the other never does.
    const steps = Math.ceil(UNVIEWED_STOP_AFTER_MS / SAMPLE_INTERVAL_MS) + 2;
    for (let i = 0; i < steps; i++) {
      touchSampling("1", "watched");
      await vi.advanceTimersByTimeAsync(SAMPLE_INTERVAL_MS);
    }

    expect(activeSamplerCount()).toBe(1);
    partitions.mockClear();
    touchSampling("1", "watched");
    await vi.advanceTimersByTimeAsync(SAMPLE_INTERVAL_MS);
    const sampled = partitions.mock.calls.map((call) => (call as unknown as [{ topic: string }])[0].topic);
    expect(sampled).toEqual(["watched"]);
  });

  it("gives up after repeated failures instead of hammering a cluster that is gone", async () => {
    const partitions = vi.fn(() => {
      throw new Error("connection refused");
    });
    setInvokeHandlers({ connection_list_partitions: partitions, connection_list_consumer_groups: () => [] });

    ensureSampling("1", "orders");
    await vi.advanceTimersByTimeAsync(SAMPLE_INTERVAL_MS * 10);

    expect(partitions).toHaveBeenCalledTimes(3);
    // Asking again (the tab being opened) starts it afresh.
    ensureSampling("1", "orders");
    await vi.advanceTimersByTimeAsync(0);
    expect(partitions).toHaveBeenCalledTimes(4);
  });
});
