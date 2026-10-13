import { beforeEach, describe, expect, it } from "vitest";
import {
  dataTabCacheKey,
  tabDataKey,
  tabDataPrefix,
  totalRetainedPayloadBytes,
  UNASSIGNED_TAB_KEY,
  useTabDataStore,
} from "./useTabDataStore";

const sample = [{ partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] }];

beforeEach(() => {
  useTabDataStore.setState({ messagesByTab: {}, totalMatchingByTab: {}, fetchDurationMsByTab: {}, payloadBytesByTab: {}, lastUsedByTab: {}, evictedTabs: {} });
});

describe("tabDataKey", () => {
  it("passes through a real tab id", () => {
    expect(tabDataKey("tab-1")).toBe("tab-1");
  });

  it("falls back to the unassigned key when there is no active tab", () => {
    expect(tabDataKey(null)).toBe(UNASSIGNED_TAB_KEY);
  });
});

describe("dataTabCacheKey", () => {
  it("differs for different topics in the same tab", () => {
    expect(dataTabCacheKey("tab-1", "conn-1", "orders")).not.toBe(dataTabCacheKey("tab-1", "conn-1", "payments"));
  });

  it("differs for a topic's Data tab vs. one of its partitions'", () => {
    expect(dataTabCacheKey("tab-1", "conn-1", "orders")).not.toBe(dataTabCacheKey("tab-1", "conn-1", "orders", 0));
  });

  it("differs for different partitions of the same topic", () => {
    expect(dataTabCacheKey("tab-1", "conn-1", "orders", 0)).not.toBe(dataTabCacheKey("tab-1", "conn-1", "orders", 1));
  });

  it("is stable for the same inputs", () => {
    expect(dataTabCacheKey("tab-1", "conn-1", "orders", 0)).toBe(dataTabCacheKey("tab-1", "conn-1", "orders", 0));
  });
});

describe("useTabDataStore", () => {
  it("starts with no cached messages for any tab", () => {
    expect(useTabDataStore.getState().messagesByTab).toEqual({});
  });

  it("caches messages under the given tab id", () => {
    useTabDataStore.getState().setTabMessages("tab-1", sample);
    expect(useTabDataStore.getState().messagesByTab["tab-1"]).toEqual(sample);
  });

  it("keeps each tab's cached messages independent", () => {
    const other = [{ partition: 1, offset: 9, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] }];
    useTabDataStore.getState().setTabMessages("tab-1", sample);
    useTabDataStore.getState().setTabMessages("tab-2", other);

    expect(useTabDataStore.getState().messagesByTab["tab-1"]).toEqual(sample);
    expect(useTabDataStore.getState().messagesByTab["tab-2"]).toEqual(other);
  });

  it("clears only the given tab's cached messages", () => {
    useTabDataStore.getState().setTabMessages("tab-1", sample);
    useTabDataStore.getState().setTabMessages("tab-2", sample);

    useTabDataStore.getState().clearTabMessages("tab-1");

    expect(useTabDataStore.getState().messagesByTab["tab-1"]).toBeUndefined();
    expect(useTabDataStore.getState().messagesByTab["tab-2"]).toEqual(sample);
  });

  it("appends a streamed message onto a tab with no cached rows yet", () => {
    useTabDataStore.getState().appendTabMessage("tab-1", sample[0]);
    expect(useTabDataStore.getState().messagesByTab["tab-1"]).toEqual(sample);
  });

  it("appends a streamed message after a tab's existing cached rows", () => {
    const second = { partition: 1, offset: 9, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] };
    useTabDataStore.getState().setTabMessages("tab-1", sample);
    useTabDataStore.getState().appendTabMessage("tab-1", second);

    expect(useTabDataStore.getState().messagesByTab["tab-1"]).toEqual([sample[0], second]);
  });

  it("appends a whole batch of streamed messages in one update", () => {
    const second = { partition: 1, offset: 9, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] };
    const third = { partition: 2, offset: 3, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] };
    useTabDataStore.getState().setTabMessages("tab-1", sample);

    useTabDataStore.getState().appendTabMessages("tab-1", [second, third]);

    expect(useTabDataStore.getState().messagesByTab["tab-1"]).toEqual([sample[0], second, third]);
  });

  /** A flush with nothing buffered must not re-render every subscriber of the store. */
  it("leaves state untouched when an empty batch is appended", () => {
    useTabDataStore.getState().setTabMessages("tab-1", sample);
    const before = useTabDataStore.getState().messagesByTab;

    useTabDataStore.getState().appendTabMessages("tab-1", []);

    expect(useTabDataStore.getState().messagesByTab).toBe(before);
  });

  it("keeps appended messages scoped to their own tab", () => {
    const other = { partition: 1, offset: 9, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] };
    useTabDataStore.getState().appendTabMessage("tab-1", sample[0]);
    useTabDataStore.getState().appendTabMessage("tab-2", other);

    expect(useTabDataStore.getState().messagesByTab["tab-1"]).toEqual(sample);
    expect(useTabDataStore.getState().messagesByTab["tab-2"]).toEqual([other]);
  });

  it("clears every cached entry for a tab (every topic/partition it holds), leaving other tabs untouched", () => {
    useTabDataStore.getState().setTabMessages(dataTabCacheKey("tab-1", "1", "orders"), sample);
    useTabDataStore.getState().setTabMessages(dataTabCacheKey("tab-1", "1", "payments"), sample);
    useTabDataStore.getState().setTabMessages(dataTabCacheKey("tab-1", "1", "orders", 0), sample);
    useTabDataStore.getState().setTabMessages(dataTabCacheKey("tab-2", "1", "orders"), sample);

    useTabDataStore.getState().clearAllMessagesForTab("tab-1");

    expect(useTabDataStore.getState().messagesByTab).toEqual({
      [dataTabCacheKey("tab-2", "1", "orders")]: sample,
    });
  });

  it("records a tab's total-matching count separately from its cached rows", () => {
    useTabDataStore.getState().setTabTotalMatching("tab-1", 150);
    expect(useTabDataStore.getState().totalMatchingByTab["tab-1"]).toBe(150);
  });

  it("keeps each tab's total-matching count independent", () => {
    useTabDataStore.getState().setTabTotalMatching("tab-1", 150);
    useTabDataStore.getState().setTabTotalMatching("tab-2", 3);

    expect(useTabDataStore.getState().totalMatchingByTab["tab-1"]).toBe(150);
    expect(useTabDataStore.getState().totalMatchingByTab["tab-2"]).toBe(3);
  });

  it("clears a tab's total-matching count along with its cached messages", () => {
    useTabDataStore.getState().setTabMessages("tab-1", sample);
    useTabDataStore.getState().setTabTotalMatching("tab-1", 150);

    useTabDataStore.getState().clearTabMessages("tab-1");

    expect(useTabDataStore.getState().totalMatchingByTab["tab-1"]).toBeUndefined();
  });

  /**
   * The timing is cached with the rows for the same reason the total is:
   * the middle pane remounts per tab, so a number held in the component
   * would vanish the moment the user looked away from rows that are still
   * sitting there.
   */
  it("records how long a view's last fetch took, per view", () => {
    useTabDataStore.getState().setTabFetchDurationMs("tab-1", 2_000);
    useTabDataStore.getState().setTabFetchDurationMs("tab-2", 15);

    expect(useTabDataStore.getState().fetchDurationMsByTab["tab-1"]).toBe(2_000);
    expect(useTabDataStore.getState().fetchDurationMsByTab["tab-2"]).toBe(15);
  });

  it("clears a view's fetch timing along with its cached messages", () => {
    useTabDataStore.getState().setTabMessages("tab-1", sample);
    useTabDataStore.getState().setTabFetchDurationMs("tab-1", 2_000);

    useTabDataStore.getState().clearTabMessages("tab-1");

    expect(useTabDataStore.getState().fetchDurationMsByTab["tab-1"]).toBeUndefined();
  });

  it("drops a disconnected cluster's fetch timings with its rows, in every tab", () => {
    useTabDataStore.getState().setTabFetchDurationMs(dataTabCacheKey("tab-1", "gone", "orders"), 2_000);
    useTabDataStore.getState().setTabFetchDurationMs(dataTabCacheKey("tab-2", "gone", "payments"), 40);
    useTabDataStore.getState().setTabFetchDurationMs(dataTabCacheKey("tab-1", "stays", "orders"), 15);

    useTabDataStore.getState().clearForConnection("gone");

    expect(useTabDataStore.getState().fetchDurationMsByTab).toEqual({
      [dataTabCacheKey("tab-1", "stays", "orders")]: 15,
    });
  });

  it("clears every fetch timing under a tab's prefix, leaving other tabs untouched", () => {
    useTabDataStore.getState().setTabFetchDurationMs(dataTabCacheKey("tab-1", "1", "orders"), 2_000);
    useTabDataStore.getState().setTabFetchDurationMs(dataTabCacheKey("tab-2", "1", "orders"), 15);

    useTabDataStore.getState().clearAllMessagesForTab("tab-1");

    expect(useTabDataStore.getState().fetchDurationMsByTab).toEqual({
      [dataTabCacheKey("tab-2", "1", "orders")]: 15,
    });
  });

  it("clears every total-matching entry under a tab's prefix, leaving other tabs untouched", () => {
    useTabDataStore.getState().setTabTotalMatching(dataTabCacheKey("tab-1", "1", "orders"), 150);
    useTabDataStore.getState().setTabTotalMatching(dataTabCacheKey("tab-2", "1", "orders"), 3);

    useTabDataStore.getState().clearAllMessagesForTab("tab-1");

    expect(useTabDataStore.getState().totalMatchingByTab).toEqual({
      [dataTabCacheKey("tab-2", "1", "orders")]: 3,
    });
  });
});

describe("tabDataPrefix", () => {
  it("matches every dataTabCacheKey minted for the same tab", () => {
    const prefix = tabDataPrefix("tab-1");
    expect(dataTabCacheKey("tab-1", "1", "orders").startsWith(prefix)).toBe(true);
    expect(dataTabCacheKey("tab-1", "1", "orders", 0).startsWith(prefix)).toBe(true);
  });

  it("does not match a different tab's keys", () => {
    const prefix = tabDataPrefix("tab-1");
    expect(dataTabCacheKey("tab-2", "1", "orders").startsWith(prefix)).toBe(false);
  });
  // --- Max total fetch size accounting -------------------------------------

  it("starts a tab that has never fetched with no payload bytes charged", () => {
    expect(useTabDataStore.getState().payloadBytesByTab["tab-1:1:orders:all"]).toBeUndefined();
  });

  it("replaces the total on a completed Fetch and adds on each streamed batch", () => {
    const { setTabPayloadBytes, addTabPayloadBytes } = useTabDataStore.getState();

    setTabPayloadBytes("tab-1:1:orders:all", 5_000);
    addTabPayloadBytes("tab-1:1:orders:all", 1_000);
    addTabPayloadBytes("tab-1:1:orders:all", 1_000);

    expect(useTabDataStore.getState().payloadBytesByTab["tab-1:1:orders:all"]).toBe(7_000);

    // A new Fetch supersedes rather than accumulates — its rows replace the
    // ones the earlier bytes were charged for.
    setTabPayloadBytes("tab-1:1:orders:all", 200);
    expect(useTabDataStore.getState().payloadBytesByTab["tab-1:1:orders:all"]).toBe(200);
  });

  it("adds onto a tab with no total yet, treating it as zero", () => {
    useTabDataStore.getState().addTabPayloadBytes("tab-1:1:orders:all", 900);

    expect(useTabDataStore.getState().payloadBytesByTab["tab-1:1:orders:all"]).toBe(900);
  });

  // The total describes the rows being dropped, so clearing them must clear
  // it — otherwise a re-Fetch would start against a budget already spent by
  // the fetch it replaced.
  it("drops a tab's payload-byte total along with its rows", () => {
    const { setTabPayloadBytes, setTabMessages, clearTabMessages } = useTabDataStore.getState();
    setTabMessages("tab-1:1:orders:all", []);
    setTabPayloadBytes("tab-1:1:orders:all", 9_000);

    clearTabMessages("tab-1:1:orders:all");

    expect(useTabDataStore.getState().payloadBytesByTab["tab-1:1:orders:all"]).toBeUndefined();
  });

  it("drops every topic's payload-byte total when a whole tab's memory is cleared", () => {
    const { setTabPayloadBytes, clearAllMessagesForTab } = useTabDataStore.getState();
    setTabPayloadBytes("tab-1:1:orders:all", 9_000);
    setTabPayloadBytes("tab-1:1:shipments:all", 4_000);
    setTabPayloadBytes("tab-2:1:orders:all", 7_000);

    clearAllMessagesForTab("tab-1");

    expect(useTabDataStore.getState().payloadBytesByTab["tab-1:1:orders:all"]).toBeUndefined();
    expect(useTabDataStore.getState().payloadBytesByTab["tab-1:1:shipments:all"]).toBeUndefined();
    // Another tab's budget is its own.
    expect(useTabDataStore.getState().payloadBytesByTab["tab-2:1:orders:all"]).toBe(7_000);
  });

  // --- The app-wide retention ceiling --------------------------------------
  //
  // Every tab shares one webview process, so the number that decides whether
  // the app survives is the total across all of them — and it is restored by
  // evicting the coldest views, since refusing new work frees nothing already
  // held elsewhere.

  function seed(bytesByKey: Record<string, number>, lastUsed: Record<string, number>) {
    useTabDataStore.setState({
      payloadBytesByTab: bytesByKey,
      messagesByTab: Object.fromEntries(Object.keys(bytesByKey).map((k) => [k, []])),
      totalMatchingByTab: Object.fromEntries(Object.keys(bytesByKey).map((k) => [k, 1])),
      lastUsedByTab: lastUsed,
      evictedTabs: {},
    });
  }

  it("totals retained payload bytes across every tab", () => {
    seed({ a: 100, b: 250, c: 50 }, {});
    expect(totalRetainedPayloadBytes(useTabDataStore.getState().payloadBytesByTab)).toBe(400);
  });

  it("totals zero when nothing is cached", () => {
    expect(totalRetainedPayloadBytes({})).toBe(0);
  });

  it("does nothing while the total already fits", () => {
    seed({ a: 100, b: 100 }, { a: 1, b: 2 });

    expect(useTabDataStore.getState().evictToFit(1_000, "a")).toEqual([]);
    expect(useTabDataStore.getState().payloadBytesByTab).toEqual({ a: 100, b: 100 });
  });

  it("evicts least-recently-used first, and stops as soon as it fits", () => {
    seed({ oldest: 100, middle: 100, newest: 100 }, { oldest: 1, middle: 2, newest: 3 });

    const evicted = useTabDataStore.getState().evictToFit(250, "newest");

    expect(evicted).toEqual(["oldest"]);
    expect(Object.keys(useTabDataStore.getState().payloadBytesByTab).sort()).toEqual(["middle", "newest"]);
  });

  it("keeps evicting until the total fits", () => {
    seed({ a: 100, b: 100, c: 100 }, { a: 1, b: 2, c: 3 });

    const evicted = useTabDataStore.getState().evictToFit(100, "c");

    expect(evicted).toEqual(["a", "b"]);
    expect(useTabDataStore.getState().payloadBytesByTab).toEqual({ c: 100 });
  });

  // A fetch must never discard the results it just went and got.
  it("never evicts the protected view, even when it alone is over the limit", () => {
    seed({ huge: 10_000 }, { huge: 1 });

    const evicted = useTabDataStore.getState().evictToFit(1_000, "huge");

    expect(evicted).toEqual([]);
    expect(useTabDataStore.getState().payloadBytesByTab).toEqual({ huge: 10_000 });
  });

  // Views holding nothing free nothing; without skipping them the loop would
  // "evict" them forever without the total ever moving.
  it("terminates when the only evictable views hold nothing", () => {
    seed({ empty: 0, protectedView: 10_000 }, { empty: 1, protectedView: 2 });

    const evicted = useTabDataStore.getState().evictToFit(1_000, "protectedView");

    expect(evicted).toEqual([]);
  });

  it("drops an evicted view's rows and totals, and marks it so the tab can explain itself", () => {
    seed({ cold: 500, hot: 100 }, { cold: 1, hot: 2 });

    useTabDataStore.getState().evictToFit(100, "hot");

    expect(useTabDataStore.getState().messagesByTab.cold).toBeUndefined();
    expect(useTabDataStore.getState().totalMatchingByTab.cold).toBeUndefined();
    // The timing describes rows that are no longer there.
    expect(useTabDataStore.getState().fetchDurationMsByTab.cold).toBeUndefined();
    expect(useTabDataStore.getState().evictedTabs.cold).toBe(true);
    expect(useTabDataStore.getState().evictedTabs.hot).toBeUndefined();
  });

  it("marks a view used when it is touched, so it stops being the eviction candidate", () => {
    seed({ a: 100, b: 100 }, { a: 1, b: 2 });

    useTabDataStore.getState().touchTab("a");
    const evicted = useTabDataStore.getState().evictToFit(100, "protected-none");

    // "a" was the coldest until it was touched; now "b" is.
    expect(evicted).toEqual(["b"]);
  });

  it("clears the eviction marker when the view is fetched into again", () => {
    seed({ cold: 500, hot: 100 }, { cold: 1, hot: 2 });
    useTabDataStore.getState().evictToFit(100, "hot");
    expect(useTabDataStore.getState().evictedTabs.cold).toBe(true);

    useTabDataStore.getState().clearTabMessages("cold");

    expect(useTabDataStore.getState().evictedTabs.cold).toBeUndefined();
  });
});
