import { create } from "zustand";
import { FilterFormState } from "./dataFilters";
import { HeaderCriterion, HeaderFilterRow } from "./headerFilters";
import { dataTabKeyBelongsTo } from "../workspace/useTabDataStore";

/**
 * The Data tab's filter form (Max messages per partition, Partition,
 * Offset, From/To, ...), keyed the same way as `useTabDataStore`'s cached
 * messages (`dataTabCacheKey`) — tab + connection + topic + partition. Without
 * this, the form lived in `DataTab`'s own `useState`, reset on every topic
 * switch (including switching back to a topic you'd already set filters on),
 * so returning to a topic showed its cached messages but not the filters
 * that produced them.
 */
/** The header filter's editable rows, and the criteria last applied to the grid (they differ until the user presses Filter). */
export interface HeaderFilterState {
  rows: HeaderFilterRow[];
  applied: HeaderCriterion[];
}

interface DataTabFiltersState {
  formByTab: Record<string, FilterFormState>;
  setForm: (key: string, form: FilterFormState) => void;
  /** Kept apart from `formByTab`: these narrow the rows already loaded and are never sent to the broker. */
  headerFilterByTab: Record<string, HeaderFilterState>;
  setHeaderFilter: (key: string, state: HeaderFilterState) => void;
  /** Forgets every filter form belonging to one connection, in every tab — see `useTabDataStore`'s `clearForConnection`. */
  clearForConnection: (connectionId: string) => void;
}

export const useDataTabFiltersStore = create<DataTabFiltersState>((set) => ({
  formByTab: {},
  setForm: (key, form) => set((state) => ({ formByTab: { ...state.formByTab, [key]: form } })),
  headerFilterByTab: {},
  setHeaderFilter: (key, filter) =>
    set((state) => ({ headerFilterByTab: { ...state.headerFilterByTab, [key]: filter } })),
  clearForConnection: (connectionId) =>
    set((state) => ({
      formByTab: Object.fromEntries(
        Object.entries(state.formByTab).filter(([key]) => !dataTabKeyBelongsTo(key, connectionId)),
      ),
      headerFilterByTab: Object.fromEntries(
        Object.entries(state.headerFilterByTab).filter(([key]) => !dataTabKeyBelongsTo(key, connectionId)),
      ),
    })),
}));
