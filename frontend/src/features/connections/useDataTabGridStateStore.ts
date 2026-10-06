import { create } from "zustand";
import type { FilterModel, SortModelItem } from "ag-grid-community";
import { dataTabKeyBelongsTo } from "../workspace/useTabDataStore";

/**
 * The parts of a Data tab's grid that are the user's own arrangement of the
 * rows rather than the rows themselves: how they're sorted and which column
 * filters are set.
 */
export interface DataTabGridState {
  /** AG Grid's sort model — column id + direction, in sort priority order. */
  sortModel: SortModelItem[];
  /** AG Grid's column filter model, keyed by column id. */
  filterModel: FilterModel;
}

/**
 * A stable "nothing arranged yet" fallback, for the same reason
 * `EMPTY_TAB_MESSAGES` exists: a `?? { ... }` literal in a selector hands
 * back a new object every call, which zustand's reference-equality check
 * reads as a state change on every render.
 */
export const EMPTY_DATA_TAB_GRID_STATE: DataTabGridState = {
  sortModel: [],
  filterModel: {},
};

/**
 * Per-Data-tab sort/filter, keyed by `dataTabCacheKey` — the same key
 * as the cached rows (`useTabDataStore`) and the fetch form
 * (`useDataTabFiltersStore`), so a tab's arrangement travels with the rows
 * it arranges.
 *
 * This has to live outside `DataTab` because the middle pane is keyed by the
 * active top-level tab (`App.tsx`), so every top-level tab switch unmounts
 * the grid outright. Held in the component (or in AG Grid's own state), a
 * sort by Timestamp and a column filter were both thrown away on the way out
 * and the grid came back in its original order on the way in.
 *
 * Keying by topic/partition as well as by tab is also what keeps the old
 * reset-on-topic-switch behaviour: a different topic is a different key, so
 * it starts unsorted and unfiltered rather than inheriting an arrangement
 * from the topic before it.
 */
interface DataTabGridStateStore {
  stateByTab: Record<string, DataTabGridState>;
  /** Merges a patch into one tab's state, leaving the parts it doesn't mention alone. */
  patchState: (key: string, patch: Partial<DataTabGridState>) => void;
  /** Forgets every sort/filter belonging to one connection, in every tab — see `useTabDataStore`'s `clearForConnection`. */
  clearForConnection: (connectionId: string) => void;
}

export const useDataTabGridStateStore = create<DataTabGridStateStore>((set) => ({
  stateByTab: {},
  patchState: (key, patch) =>
    set((state) => ({
      stateByTab: {
        ...state.stateByTab,
        [key]: { ...(state.stateByTab[key] ?? EMPTY_DATA_TAB_GRID_STATE), ...patch },
      },
    })),
  clearForConnection: (connectionId) =>
    set((state) => ({
      stateByTab: Object.fromEntries(
        Object.entries(state.stateByTab).filter(([key]) => !dataTabKeyBelongsTo(key, connectionId)),
      ),
    })),
}));
