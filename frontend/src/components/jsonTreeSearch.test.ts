import { describe, expect, it } from "vitest";
import { flattenJsonTree, NO_OVERRIDES } from "./jsonTreeLines";
import { lineSearchText } from "./jsonTreeSearch";
import { findMatches } from "./findMatches";

function linesOf(value: unknown) {
  return flattenJsonTree(value, NO_OVERRIDES).lines;
}

describe("lineSearchText", () => {
  it("includes a primitive row's key and its value, which is what the row shows", () => {
    const lines = linesOf({ status: "shipped" });
    const row = lines.find((line) => line.label === "status");

    expect(lineSearchText(row!)).toContain("status");
    expect(lineSearchText(row!)).toContain("shipped");
  });

  it("searches a container row by its key", () => {
    const lines = linesOf({ shipment: { id: 1 } });
    const row = lines.find((line) => line.label === "shipment" && line.kind === "open");

    expect(lineSearchText(row!)).toContain("shipment");
  });

  it("finds numbers and booleans, which render as text but are not strings", () => {
    const lines = linesOf({ count: 42, ok: false });

    expect(lineSearchText(lines.find((l) => l.label === "count")!)).toContain("42");
    expect(lineSearchText(lines.find((l) => l.label === "ok")!)).toContain("false");
  });

  it("finds an explicit null, which is a value a reader looks for", () => {
    const lines = linesOf({ cancelledAt: null });

    expect(lineSearchText(lines.find((l) => l.label === "cancelledAt")!)).toContain("null");
  });
});

describe("searching JSON tree lines", () => {
  /**
   * The regression this whole module exists for. The tree is virtualized, so
   * only ~32 of these rows are ever in the DOM — a DOM-based find reports
   * "not found" for text that is plainly present. Matching runs over the
   * flattened line model instead, which holds every line.
   */
  it("finds a line far past the end of the rendered window", () => {
    const value: Record<string, string> = {};
    for (let i = 0; i < 500; i++) value[`key_${i}`] = `value_${i}`;
    value.needle = "FINDME";
    const lines = linesOf(value);

    const matches = findMatches(lines.map(lineSearchText), "FINDME");

    expect(matches).toHaveLength(1);
    expect(matches[0]).toBeGreaterThan(400);
  });

  it("matches case-insensitively, as a find bar is expected to", () => {
    const lines = linesOf({ Status: "SHIPPED" });

    expect(findMatches(lines.map(lineSearchText), "shipped")).toHaveLength(1);
    expect(findMatches(lines.map(lineSearchText), "STATUS")).toHaveLength(1);
  });

  it("returns every matching line in document order", () => {
    const lines = linesOf({ a: "hit", b: "miss", c: "hit" });

    const matches = findMatches(lines.map(lineSearchText), "hit");

    expect(matches).toHaveLength(2);
    expect(matches[0]).toBeLessThan(matches[1]);
  });

});
