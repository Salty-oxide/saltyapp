import { beforeEach, describe, expect, it } from "vitest";
import { emptyFilterForm } from "./dataFilters";
import { emptyHeaderRow } from "./headerFilters";
import { useDataTabFiltersStore } from "./useDataTabFiltersStore";

beforeEach(() => {
  useDataTabFiltersStore.setState({ formByTab: {}, headerFilterByTab: {} });
});

describe("useDataTabFiltersStore", () => {
  it("starts with no stored form for any key", () => {
    expect(useDataTabFiltersStore.getState().formByTab["tab-1:1:orders:all"]).toBeUndefined();
  });

  it("stores a form under the given key", () => {
    const form = { ...emptyFilterForm(), maxMessagesPerPartition: "10" };
    useDataTabFiltersStore.getState().setForm("tab-1:1:orders:all", form);

    expect(useDataTabFiltersStore.getState().formByTab["tab-1:1:orders:all"]).toEqual(form);
  });

  it("keeps different keys' forms independent", () => {
    const ordersForm = { ...emptyFilterForm(), maxMessagesPerPartition: "10" };
    const paymentsForm = { ...emptyFilterForm(), offset: "500" };
    useDataTabFiltersStore.getState().setForm("tab-1:1:orders:all", ordersForm);
    useDataTabFiltersStore.getState().setForm("tab-1:1:payments:all", paymentsForm);

    expect(useDataTabFiltersStore.getState().formByTab["tab-1:1:orders:all"]).toEqual(ordersForm);
    expect(useDataTabFiltersStore.getState().formByTab["tab-1:1:payments:all"]).toEqual(paymentsForm);
  });

  it("overwrites a key's previous form", () => {
    useDataTabFiltersStore.getState().setForm("tab-1:1:orders:all", { ...emptyFilterForm(), offset: "1" });
    useDataTabFiltersStore.getState().setForm("tab-1:1:orders:all", { ...emptyFilterForm(), offset: "2" });

    expect(useDataTabFiltersStore.getState().formByTab["tab-1:1:orders:all"].offset).toBe("2");
  });
});

describe("useDataTabFiltersStore header filters", () => {
  it("stores the rows and the applied criteria per key, independently of the fetch form", () => {
    const rows = [{ ...emptyHeaderRow(), key: "source", value: " billing " }];
    const applied = [{ key: "source", value: "billing" }];
    useDataTabFiltersStore.getState().setHeaderFilter("tab-1:1:orders:all", { rows, applied });

    expect(useDataTabFiltersStore.getState().headerFilterByTab["tab-1:1:orders:all"]).toEqual({ rows, applied });
    expect(useDataTabFiltersStore.getState().headerFilterByTab["tab-1:1:payments:all"]).toBeUndefined();
    expect(useDataTabFiltersStore.getState().formByTab["tab-1:1:orders:all"]).toBeUndefined();
  });

  it("clearForConnection forgets that connection's header filters and keeps the others", () => {
    const state = { rows: [emptyHeaderRow()], applied: [] };
    useDataTabFiltersStore.getState().setHeaderFilter("tab-1:1:orders:all", state);
    useDataTabFiltersStore.getState().setHeaderFilter("tab-1:2:orders:all", state);

    useDataTabFiltersStore.getState().clearForConnection("1");

    expect(Object.keys(useDataTabFiltersStore.getState().headerFilterByTab)).toEqual(["tab-1:2:orders:all"]);
  });
});
