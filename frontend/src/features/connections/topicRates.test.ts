import { describe, expect, it } from "vitest";
import {
  counterDelta,
  EMPTY_RATE_HISTORY,
  MINUTE_MS,
  pruneOlderThan,
  RATE_RANGES,
  recordSample,
  seriesFor,
} from "./topicRates";

const T0 = 1_700_000_000_000 - (1_700_000_000_000 % MINUTE_MS);

describe("counterDelta", () => {
  it("is how far the counter moved", () => {
    expect(counterDelta(100, 160)).toBe(60);
  });
  it("is 0 when the counter went backwards — a reset, a recreated topic or retention — rather than a negative rate", () => {
    expect(counterDelta(500, 20)).toBe(0);
  });
});

describe("recordSample", () => {
  it("takes no rate from the first sample, only a baseline", () => {
    const h = recordSample(EMPTY_RATE_HISTORY, { at: T0, produced: 1000, consumedByGroup: { g: 900 } });
    expect(h.last).toEqual({ at: T0, produced: 1000, consumedByGroup: { g: 900 } });
    expect(h.minutes).toEqual({});
  });

  it("adds the change since the previous sample to the minute of the later one", () => {
    let h = recordSample(EMPTY_RATE_HISTORY, { at: T0, produced: 1000, consumedByGroup: { g: 900 } });
    h = recordSample(h, { at: T0 + 30_000, produced: 1060, consumedByGroup: { g: 930 } });
    h = recordSample(h, { at: T0 + 50_000, produced: 1100, consumedByGroup: { g: 960 } });

    expect(h.minutes[T0 / MINUTE_MS]).toEqual({ produced: 100, consumed: 60 });
  });

  it("sums consumption across groups, counting only groups present in both samples", () => {
    let h = recordSample(EMPTY_RATE_HISTORY, { at: T0, produced: 0, consumedByGroup: { a: 100 } });
    // `b` appears with a large committed offset: that is a group joining, not 5,000 messages consumed.
    h = recordSample(h, { at: T0 + 30_000, produced: 10, consumedByGroup: { a: 110, b: 5_000 } });
    expect(h.minutes[T0 / MINUTE_MS].consumed).toBe(10);

    h = recordSample(h, { at: T0 + 60_000, produced: 20, consumedByGroup: { a: 120, b: 5_020 } });
    expect(h.minutes[T0 / MINUTE_MS + 1].consumed).toBe(30);
  });

  it("records consumption as unknown when the sample could not read it", () => {
    let h = recordSample(EMPTY_RATE_HISTORY, { at: T0, produced: 0, consumedByGroup: null });
    h = recordSample(h, { at: T0 + 30_000, produced: 25, consumedByGroup: null });
    expect(h.minutes[T0 / MINUTE_MS]).toEqual({ produced: 25, consumed: null });
  });

  it("ignores a sample that is not newer than the last", () => {
    let h = recordSample(EMPTY_RATE_HISTORY, { at: T0, produced: 10, consumedByGroup: null });
    const again = recordSample(h, { at: T0, produced: 99, consumedByGroup: null });
    expect(again).toBe(h);
  });
});

describe("pruneOlderThan", () => {
  it("drops minutes older than the cutoff", () => {
    const h = {
      last: null,
      minutes: { [T0 / MINUTE_MS]: { produced: 1, consumed: 1 }, [T0 / MINUTE_MS + 100]: { produced: 2, consumed: 2 } },
    };
    const pruned = pruneOlderThan(h, T0 + 50 * MINUTE_MS);
    expect(Object.keys(pruned.minutes)).toEqual([String(T0 / MINUTE_MS + 100)]);
  });
});

describe("seriesFor", () => {
  const range = (id: string) => RATE_RANGES.find((r) => r.id === id)!;

  it("offers 1h, 1d, 7d and 30d", () => {
    expect(RATE_RANGES.map((r) => r.id)).toEqual(["1h", "1d", "7d", "30d"]);
  });

  it("spans the window in whole buckets ending at now", () => {
    const now = T0 + 90 * MINUTE_MS;
    const points = seriesFor(EMPTY_RATE_HISTORY, range("1h"), now);
    expect(points.length).toBeGreaterThanOrEqual(60);
    expect(points[points.length - 1].at).toBeLessThanOrEqual(now);
    expect(points[0].at).toBeGreaterThanOrEqual(now - 60 * MINUTE_MS - range("1h").bucketMs);
    expect(points.every((p) => p.produced === null && p.consumed === null)).toBe(true);
  });

  it("sums the minutes that fall in a bucket and leaves unsampled buckets empty", () => {
    const now = T0 + 10 * MINUTE_MS;
    const h = {
      last: null,
      minutes: {
        [(T0 + 5 * MINUTE_MS) / MINUTE_MS]: { produced: 40, consumed: 10 },
        [(T0 + 5 * MINUTE_MS) / MINUTE_MS + 1]: { produced: 60, consumed: null },
      },
    };
    const points = seriesFor(h, range("1h"), now);
    const withData = points.filter((p) => p.produced !== null);
    expect(withData.reduce((s, p) => s + (p.produced ?? 0), 0)).toBe(100);
    expect(withData.reduce((s, p) => s + (p.consumed ?? 0), 0)).toBe(10);
  });

  it("buckets a longer range more coarsely", () => {
    expect(range("30d").bucketMs).toBeGreaterThan(range("7d").bucketMs);
    expect(range("7d").bucketMs).toBeGreaterThan(range("1d").bucketMs);
    expect(range("1d").bucketMs).toBeGreaterThan(range("1h").bucketMs);
  });

  it("excludes minutes older than the window", () => {
    const now = T0 + 3 * 60 * MINUTE_MS;
    const h = { last: null, minutes: { [T0 / MINUTE_MS]: { produced: 999, consumed: 999 } } };
    const points = seriesFor(h, range("1h"), now);
    expect(points.every((p) => p.produced === null)).toBe(true);
  });
});
