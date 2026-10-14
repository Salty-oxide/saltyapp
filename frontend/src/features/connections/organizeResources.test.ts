import { describe, expect, it } from "vitest";
import { flattenRows, organize } from "./organizeResources";
import type { FolderAssignment, ResourceFolder } from "../../lib/tauri";

const folder = (id: string, name: string, kind: "topic" | "consumer" = "topic"): ResourceFolder => ({ id, kind, name });
const filed = (resourceName: string, folderId: string, kind: "topic" | "consumer" = "topic"): FolderAssignment => ({
  kind,
  resourceName,
  folderId,
});

const names = ["orders", "payments", "audit", "billing-events", "users"];
const byName = (name: string) => name;
const contains = (query: string) => (name: string) => name.toLowerCase().includes(query.toLowerCase());

function run(overrides: Partial<Parameters<typeof organize<string>>[0]> = {}) {
  return organize<string>({
    items: names,
    getName: byName,
    folders: [],
    assignments: [],
    pinned: new Set(),
    query: "",
    matches: contains(""),
    ...overrides,
  });
}

describe("organize", () => {
  it("leaves a list with no pins or folders exactly as it was", () => {
    const result = run();
    expect(result.pinned).toEqual([]);
    expect(result.folders).toEqual([]);
    expect(result.unfiled).toEqual(names);
  });

  it("puts pinned items first, in list order, and removes them from the rest", () => {
    const result = run({ pinned: new Set(["users", "orders"]) });
    expect(result.pinned).toEqual(["orders", "users"]);
    expect(result.unfiled).toEqual(["payments", "audit", "billing-events"]);
  });

  it("ignores a pin whose topic no longer exists", () => {
    const result = run({ pinned: new Set(["deleted-topic"]) });
    expect(result.pinned).toEqual([]);
    expect(result.unfiled).toEqual(names);
  });

  it("groups filed items under their folder, folders alphabetical ignoring case", () => {
    const result = run({
      folders: [folder("f2", "zeta"), folder("f1", "Alpha")],
      assignments: [filed("orders", "f2"), filed("audit", "f1"), filed("users", "f1")],
    });
    expect(result.folders.map((g) => g.folder.name)).toEqual(["Alpha", "zeta"]);
    expect(result.folders[0].items).toEqual(["audit", "users"]);
    expect(result.folders[1].items).toEqual(["orders"]);
    expect(result.unfiled).toEqual(["payments", "billing-events"]);
  });

  it("shows an empty folder when not searching", () => {
    const result = run({ folders: [folder("f1", "Empty")] });
    expect(result.folders).toHaveLength(1);
    expect(result.folders[0].items).toEqual([]);
  });

  it("shows a pinned item only in Pinned, keeping its folder for when it is unpinned", () => {
    const folders = [folder("f1", "Billing")];
    const assignments = [filed("orders", "f1")];

    const pinned = run({ folders, assignments, pinned: new Set(["orders"]) });
    expect(pinned.pinned).toEqual(["orders"]);
    expect(pinned.folders[0].items).toEqual([]);

    const unpinned = run({ folders, assignments });
    expect(unpinned.folders[0].items).toEqual(["orders"]);
  });

  it("treats an item filed under a folder that no longer exists as unfiled", () => {
    const result = run({ assignments: [filed("orders", "gone")] });
    expect(result.unfiled).toContain("orders");
  });

  it("never files an item under a folder of another kind", () => {
    const result = run({
      folders: [folder("c1", "Group folder", "consumer")],
      assignments: [filed("orders", "c1", "consumer")],
      kind: "topic",
    });
    expect(result.folders).toEqual([]);
    expect(result.unfiled).toContain("orders");
  });

  describe("while searching", () => {
    const folders = [folder("f1", "Billing"), folder("f2", "Platform")];
    const assignments = [filed("orders", "f1"), filed("billing-events", "f1"), filed("users", "f2")];

    it("matches items inside every folder, and hides folders with no match", () => {
      const result = run({ folders, assignments, query: "orders", matches: contains("orders") });
      expect(result.searching).toBe(true);
      expect(result.folders.map((g) => g.folder.name)).toEqual(["Billing"]);
      expect(result.folders[0].items).toEqual(["orders"]);
      expect(result.unfiled).toEqual([]);
    });

    it("keeps only matching pinned items", () => {
      const result = run({
        pinned: new Set(["orders", "users"]),
        query: "users",
        matches: contains("users"),
      });
      expect(result.pinned).toEqual(["users"]);
    });

    it("shows every item of a folder whose name matches", () => {
      const result = run({ folders, assignments, query: "platform", matches: contains("platform") });
      expect(result.folders.map((g) => g.folder.name)).toEqual(["Platform"]);
      expect(result.folders[0].items).toEqual(["users"]);
    });

    it("does not report searching for a blank query", () => {
      expect(run({ query: "   " }).searching).toBe(false);
    });
  });
});

describe("flattenRows", () => {
  const folders = [folder("f1", "Billing"), folder("f2", "Platform")];
  const assignments = [filed("orders", "f1"), filed("users", "f2")];

  it("emits Pinned, folder headers with their items when open, then the rest", () => {
    const organized = run({ folders, assignments, pinned: new Set(["audit"]) });
    const rows = flattenRows(organized, new Set(["f1"]));

    expect(rows.map((r) => (r.type === "item" ? `item:${r.section}:${r.item}` : r.type === "folder" ? `folder:${r.folder.name}` : r.type))).toEqual([
      "pinned-label",
      "item:pinned:audit",
      "folder:Billing",
      "item:folder:orders",
      "folder:Platform",
      "item:unfiled:payments",
      "item:unfiled:billing-events",
    ]);
  });

  it("omits the Pinned label when nothing is pinned", () => {
    expect(flattenRows(run(), new Set()).some((r) => r.type === "pinned-label")).toBe(false);
  });

  it("opens every folder that has a match while searching, whatever was collapsed", () => {
    const organized = run({ folders, assignments, query: "orders", matches: contains("orders") });
    const rows = flattenRows(organized, new Set());
    expect(rows.some((r) => r.type === "item" && r.item === "orders")).toBe(true);
  });

  it("keeps a folder shut that the reader closed during the search", () => {
    const organized = run({ folders, assignments, query: "orders", matches: contains("orders") });
    const rows = flattenRows(organized, new Set(), new Set(["f1"]));
    expect(rows.find((r) => r.type === "folder" && r.folder.id === "f1")).toMatchObject({ open: false });
    expect(rows.some((r) => r.type === "item" && r.item === "orders")).toBe(false);
  });

  it("reports each folder's item count and open state", () => {
    const organized = run({ folders, assignments });
    const header = flattenRows(organized, new Set(["f1"])).find((r) => r.type === "folder" && r.folder.id === "f1");
    expect(header).toMatchObject({ count: 1, open: true });
  });
});
