import { beforeEach, describe, expect, it, vi } from "vitest";

const STORAGE_KEY = "kafkaoxide.sidebar-open";

/** The stored value is read when the module loads, so each case gets a fresh copy. */
async function loadStore() {
  vi.resetModules();
  return (await import("./useSidebarStore")).useSidebarStore;
}

beforeEach(() => {
  localStorage.clear();
});

describe("useSidebarStore", () => {
  it("starts open when nothing has been stored", async () => {
    const store = await loadStore();
    expect(store.getState().isOpen).toBe(true);
  });

  it("closes and reopens on toggle", async () => {
    const store = await loadStore();

    store.getState().toggleOpen();
    expect(store.getState().isOpen).toBe(false);

    store.getState().toggleOpen();
    expect(store.getState().isOpen).toBe(true);
  });

  it("restores a closed sidebar on the next launch", async () => {
    (await loadStore()).getState().toggleOpen();
    expect(localStorage.getItem(STORAGE_KEY)).toBe("false");

    expect((await loadStore()).getState().isOpen).toBe(false);
  });

  it("restores a reopened sidebar on the next launch", async () => {
    const store = await loadStore();
    store.getState().toggleOpen();
    store.getState().toggleOpen();
    expect(localStorage.getItem(STORAGE_KEY)).toBe("true");

    expect((await loadStore()).getState().isOpen).toBe(true);
  });

  it("treats an unrecognised stored value as open", async () => {
    localStorage.setItem(STORAGE_KEY, "garbage");
    expect((await loadStore()).getState().isOpen).toBe(true);
  });
});
