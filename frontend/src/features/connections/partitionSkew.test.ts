import { describe, expect, it } from "vitest";
import { PartitionSummary } from "../../lib/tauri";
import { computePartitionSkew, skewFromCounts } from "./partitionSkew";

const p = (id: number, low: number, high: number): PartitionSummary => ({
  id,
  leader: 1,
  replicas: [1],
  isr: [1],
  lowOffset: low,
  highOffset: high,
});

describe("computePartitionSkew", () => {
  it("counts messages per partition as high minus low, sorted by partition id", () => {
    const s = computePartitionSkew([p(1, 10, 40), p(0, 0, 20)]);
    expect(s.partitions).toEqual([
      { partition: 0, messages: 20 },
      { partition: 1, messages: 30 },
    ]);
    expect(s.total).toBe(50);
    expect(s.mean).toBe(25);
    expect(s.min).toBe(20);
    expect(s.max).toBe(30);
  });

  it("reports skew as the busiest partition over the mean", () => {
    const s = computePartitionSkew([p(0, 0, 90), p(1, 0, 10)]);
    expect(s.skewRatio).toBeCloseTo(1.8);
    expect(s.level).toBe("high");
  });

  it("is balanced when partitions are even", () => {
    const s = computePartitionSkew([p(0, 0, 10), p(1, 0, 10)]);
    expect(s.skewRatio).toBe(1);
    expect(s.level).toBe("balanced");
  });

  it("grades moderate skew between the two thresholds", () => {
    expect(computePartitionSkew([p(0, 0, 13), p(1, 0, 10)]).level).toBe("balanced");
    expect(computePartitionSkew([p(0, 0, 15), p(1, 0, 8)]).level).toBe("moderate");
  });

  it("treats an empty topic as balanced, not NaN", () => {
    const s = computePartitionSkew([p(0, 5, 5), p(1, 0, 0)]);
    expect(s.total).toBe(0);
    expect(s.skewRatio).toBe(1);
    expect(s.level).toBe("balanced");
  });

  it("never reports a negative count for an inverted watermark", () => {
    expect(computePartitionSkew([p(0, 9, 3)]).partitions[0].messages).toBe(0);
  });

  it("handles no partitions", () => {
    const s = computePartitionSkew([]);
    expect(s.partitions).toEqual([]);
    expect(s.level).toBe("balanced");
  });
});

describe("skewFromCounts", () => {
  it("measures counts the broker already computed, sorted, with negatives floored", () => {
    const s = skewFromCounts([
      { partition: 1, messages: 30 },
      { partition: 0, messages: -5 },
    ]);
    expect(s.partitions).toEqual([
      { partition: 0, messages: 0 },
      { partition: 1, messages: 30 },
    ]);
    expect(s.total).toBe(30);
  });
});
