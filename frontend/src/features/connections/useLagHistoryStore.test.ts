import { beforeEach, describe, expect, it } from "vitest";
import { ConsumerGroupLag } from "../../lib/tauri";
import { lagSampleFrom, MAX_LAG_SAMPLES, useLagHistoryStore } from "./useLagHistoryStore";

const lag = (lags: (number | null)[]): ConsumerGroupLag => ({
  state: "Stable",
  partitions: lags.map((l, i) => ({
    topic: "t",
    partition: i,
    currentOffset: l === null ? null : 0,
    logEndOffset: 10,
    lag: l,
    clientId: null,
    clientHost: null,
  })),
});

describe("lagSampleFrom", () => {
  it("sums known lags, treating never-committed partitions as zero", () => {
    expect(lagSampleFrom(lag([5, null, 7]), 1000)).toEqual({ at: 1000, totalLag: 12 });
  });
});

describe("useLagHistoryStore", () => {
  beforeEach(() => useLagHistoryStore.setState({ byGroup: {} }));

  it("appends samples per group", () => {
    const { record } = useLagHistoryStore.getState();
    record("c1", "g", lag([1]), 1);
    record("c1", "g", lag([3]), 2);
    record("c1", "other", lag([9]), 3);
    expect(useLagHistoryStore.getState().byGroup["c1::g"]).toEqual([
      { at: 1, totalLag: 1 },
      { at: 2, totalLag: 3 },
    ]);
    expect(useLagHistoryStore.getState().byGroup["c1::other"]).toHaveLength(1);
  });

  it("keeps only the newest samples", () => {
    const { record } = useLagHistoryStore.getState();
    for (let i = 0; i < MAX_LAG_SAMPLES + 5; i++) record("c1", "g", lag([i]), i);
    const samples = useLagHistoryStore.getState().byGroup["c1::g"];
    expect(samples).toHaveLength(MAX_LAG_SAMPLES);
    expect(samples[0].at).toBe(5);
  });

  it("forgets one connection's history only", () => {
    const { record, clearForConnection } = useLagHistoryStore.getState();
    record("c1", "g", lag([1]), 1);
    record("c2", "g", lag([1]), 1);
    clearForConnection("c1");
    expect(Object.keys(useLagHistoryStore.getState().byGroup)).toEqual(["c2::g"]);
  });
});
