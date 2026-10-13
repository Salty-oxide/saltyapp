import { describe, expect, it } from "vitest";
import {
  analyzeTopic,
  bucketCounts,
  EFFICIENCY_BUCKETS,
  healthScore,
  IMBALANCE_BUCKETS,
  replicationRiskGroups,
  summarize,
  TopicAnalysis,
  topicRisk,
} from "./clusterHealth";

const part = (id: number, leader: number, replicas: number[], isr: number[], low = 0, high = 100) => ({
  id,
  leader,
  replicas,
  isr,
  lowOffset: low,
  highOffset: high,
});
const config = (minIsr?: string) => (minIsr === undefined ? [] : [{ name: "min.insync.replicas", value: minIsr }]);

describe("analyzeTopic", () => {
  it("reads replication factor, min ISR and whether every replica is in sync", () => {
    const t = analyzeTopic(
      "orders",
      [part(0, 1, [1, 2, 3], [1, 2, 3]), part(1, 2, [2, 3, 1], [2, 3, 1])],
      config("2"),
      3,
    );
    expect(t).toMatchObject({ name: "orders", partitionCount: 2, replicationFactor: 3, minIsr: 2, underReplicated: false, belowMinIsr: false });
  });

  it("defaults min ISR to 1 when the broker reports none", () => {
    expect(analyzeTopic("t", [part(0, 1, [1, 2], [1, 2])], [], 3).minIsr).toBe(1);
  });

  it("flags a topic with a partition whose ISR is smaller than its replica set", () => {
    const t = analyzeTopic("t", [part(0, 1, [1, 2, 3], [1, 2])], config("2"), 3);
    expect(t.underReplicated).toBe(true);
    expect(t.belowMinIsr).toBe(false);
  });

  it("flags a topic whose ISR has dropped below min ISR", () => {
    const t = analyzeTopic("t", [part(0, 1, [1, 2, 3], [1])], config("2"), 3);
    expect(t.belowMinIsr).toBe(true);
  });

  it("uses the smallest replica count when partitions differ", () => {
    const t = analyzeTopic("t", [part(0, 1, [1, 2, 3], [1, 2, 3]), part(1, 1, [1, 2], [1, 2])], config(), 3);
    expect(t.replicationFactor).toBe(2);
  });

  it("measures leader spread against the ideal for the broker count", () => {
    // 4 partitions on 2 brokers, all led by broker 1: ideal is 2 per broker, so 4/2 = 2x.
    const t = analyzeTopic("t", [0, 1, 2, 3].map((i) => part(i, 1, [1, 2], [1, 2])), config(), 2);
    expect(t.leaderRatio).toBe(2);
  });

  it("calls a topic with fewer partitions than brokers balanced", () => {
    const t = analyzeTopic("t", [part(0, 1, [1], [1]), part(1, 2, [2], [2])], config(), 6);
    expect(t.leaderRatio).toBe(1);
  });

  it("derives message skew from the partitions' offsets", () => {
    const t = analyzeTopic("t", [part(0, 1, [1], [1], 0, 300), part(1, 1, [1], [1], 0, 100)], config(), 1);
    expect(t.skewRatio).toBeCloseTo(1.5);
  });
});

describe("topicRisk", () => {
  const base: TopicAnalysis = {
    name: "t", partitionCount: 1, replicationFactor: 3, minIsr: 2, underReplicated: false, belowMinIsr: false, leaderRatio: 1, skewRatio: 1,
  };
  it("is critical below min ISR", () => expect(topicRisk({ ...base, underReplicated: true, belowMinIsr: true })).toBe("critical"));
  it("is high when under-replicated but still at or above min ISR", () => expect(topicRisk({ ...base, underReplicated: true })).toBe("high"));
  it("is elevated when there is no redundancy to spare (RF 1, or min ISR equal to RF)", () => {
    expect(topicRisk({ ...base, replicationFactor: 1, minIsr: 1 })).toBe("elevated");
    expect(topicRisk({ ...base, replicationFactor: 3, minIsr: 3 })).toBe("elevated");
  });
  it("is low otherwise", () => expect(topicRisk(base)).toBe("low"));
});

describe("replicationRiskGroups", () => {
  it("groups topics by replication factor and min ISR, counting the under-replicated ones", () => {
    const mk = (rf: number, min: number, under: boolean): TopicAnalysis => ({
      name: `${rf}-${min}-${under}`, partitionCount: 1, replicationFactor: rf, minIsr: min, underReplicated: under, belowMinIsr: false, leaderRatio: 1, skewRatio: 1,
    });
    const groups = replicationRiskGroups([mk(3, 2, false), mk(3, 2, true), mk(3, 2, true), mk(1, 1, false)]);
    expect(groups).toEqual([
      { label: "RF 1 / min ISR 1", replicationFactor: 1, minIsr: 1, topics: 1, underReplicated: 0, risk: "elevated" },
      { label: "RF 3 / min ISR 2", replicationFactor: 3, minIsr: 2, topics: 3, underReplicated: 2, risk: "high" },
    ]);
  });
});

describe("bucketCounts", () => {
  it("counts topics into the first bucket whose upper bound they fit under", () => {
    const counts = bucketCounts([1, 1.2, 1.6, 2.5, 9], IMBALANCE_BUCKETS);
    expect(counts.map((c) => c.topics)).toEqual([2, 0, 1, 1, 1]);
    expect(counts.map((c) => c.label)).toEqual(IMBALANCE_BUCKETS.map((b) => b.label));
  });
  it("has its own, coarser edges for leader distribution", () => {
    expect(bucketCounts([1, 1.4, 3], EFFICIENCY_BUCKETS).map((c) => c.topics)).toEqual([1, 1, 0, 1]);
  });
});

describe("healthScore", () => {
  it("is 100 for a cluster with nothing wrong", () => {
    expect(healthScore({ topics: 10, critical: 0, underReplicated: 0, uneven: 0, skewed: 0 })).toBe(100);
  });
  it("is 100 for a cluster with no topics rather than dividing by zero", () => {
    expect(healthScore({ topics: 0, critical: 0, underReplicated: 0, uneven: 0, skewed: 0 })).toBe(100);
  });
  it("falls as more topics are in trouble, weighting data-loss risk most", () => {
    const some = healthScore({ topics: 10, critical: 0, underReplicated: 0, uneven: 5, skewed: 0 });
    const loss = healthScore({ topics: 10, critical: 5, underReplicated: 0, uneven: 0, skewed: 0 });
    expect(some).toBeLessThan(100);
    expect(loss).toBeLessThan(some);
  });
  it("never goes below 0", () => {
    expect(healthScore({ topics: 1, critical: 1, underReplicated: 1, uneven: 1, skewed: 1 })).toBeGreaterThanOrEqual(0);
  });
});

describe("summarize", () => {
  it("totals topics and partitions and derives the distribution and score", () => {
    const mk = (name: string, partitions: number, over: Partial<TopicAnalysis> = {}): TopicAnalysis => ({
      name, partitionCount: partitions, replicationFactor: 3, minIsr: 2, underReplicated: false, belowMinIsr: false, leaderRatio: 1, skewRatio: 1, ...over,
    });
    const s = summarize([
      mk("a", 6),
      mk("b", 12, { underReplicated: true }),
      mk("c", 1, { underReplicated: true, belowMinIsr: true }),
    ]);
    expect(s.totalTopics).toBe(3);
    expect(s.totalPartitions).toBe(19);
    expect(s.distribution).toEqual({ low: 1, elevated: 0, high: 1, critical: 1 });
    expect(s.score).toBeLessThan(100);
  });
});
