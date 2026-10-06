import { beforeEach, describe, expect, it } from "vitest";
import { EMPTY_DATA_TAB_GRID_STATE, useDataTabGridStateStore } from "./useDataTabGridStateStore";

beforeEach(() => {
  useDataTabGridStateStore.setState({ stateByTab: {} });
});

describe("useDataTabGridStateStore", () => {
  it("starts a key it has never seen from the empty arrangement", () => {
    expect(useDataTabGridStateStore.getState().stateByTab["tab-1:1:orders:all"]).toBeUndefined();
    expect(EMPTY_DATA_TAB_GRID_STATE).toEqual({ sortModel: [], filterModel: {} });
  });

  // The two parts of the arrangement are set by two separate places in
  // DataTab (a sort event and a filter event), so a patch has to leave the
  // part it doesn't mention alone — otherwise sorting a column silently
  // wiped the column filter you'd set.
  it("merges a patch into a key's existing arrangement instead of replacing it", () => {
    const { patchState } = useDataTabGridStateStore.getState();

    patchState("tab-1:1:orders:all", { sortModel: [{ colId: "offset", sort: "desc" }] });
    patchState("tab-1:1:orders:all", { filterModel: { partition: { filterType: "number" } } });

    expect(useDataTabGridStateStore.getState().stateByTab["tab-1:1:orders:all"]).toEqual({
      sortModel: [{ colId: "offset", sort: "desc" }],
      filterModel: { partition: { filterType: "number" } },
    });
  });

  it("keeps each tab/topic's arrangement separate", () => {
    const { patchState } = useDataTabGridStateStore.getState();

    patchState("tab-1:1:orders:all", { sortModel: [{ colId: "offset", sort: "desc" }] });
    patchState("tab-2:1:orders:all", { sortModel: [{ colId: "offset", sort: "asc" }] });

    expect(useDataTabGridStateStore.getState().stateByTab["tab-1:1:orders:all"]?.sortModel).toEqual([
      { colId: "offset", sort: "desc" },
    ]);
    expect(useDataTabGridStateStore.getState().stateByTab["tab-2:1:orders:all"]?.sortModel).toEqual([
      { colId: "offset", sort: "asc" },
    ]);
  });

  it("hands back the same empty-arrangement object every time, so a selector falling back to it doesn't look like a state change", () => {
    expect(EMPTY_DATA_TAB_GRID_STATE).toBe(EMPTY_DATA_TAB_GRID_STATE);
  });
});
