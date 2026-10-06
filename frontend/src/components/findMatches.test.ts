import { describe, expect, it } from "vitest";
import { findMatches, stepMatch } from "./findMatches";

describe("findMatches", () => {
  it("returns every matching unit in document order", () => {
    expect(findMatches(["hit", "miss", "hit"], "hit")).toEqual([0, 2]);
  });

  it("matches case-insensitively, as a find bar is expected to", () => {
    expect(findMatches(["SHIPPED"], "shipped")).toEqual([0]);
    expect(findMatches(["shipped"], "SHIPPED")).toEqual([0]);
  });

  it("returns nothing for a query that matches nothing", () => {
    expect(findMatches(["a", "b"], "absent")).toEqual([]);
  });

  // An empty bar must not report that every line matches, which would light
  // up the whole document the moment it opens.
  it("returns nothing for an empty or whitespace-only query", () => {
    expect(findMatches(["a", "b"], "")).toEqual([]);
    expect(findMatches(["a", "b"], "   ")).toEqual([]);
  });

  it("finds a unit far past anything a view would have rendered", () => {
    const units = Array.from({ length: 5000 }, (_, i) => `line ${i}`);
    units[4999] = "needle";

    expect(findMatches(units, "needle")).toEqual([4999]);
  });
});

describe("stepMatch", () => {
  it("advances and steps back", () => {
    expect(stepMatch(3, 0, 1)).toBe(1);
    expect(stepMatch(3, 2, -1)).toBe(1);
  });

  // Wrapping is what makes Enter-Enter-Enter usable: cycling should never hit
  // a dead end mid-document.
  it("wraps at both ends", () => {
    expect(stepMatch(3, 2, 1)).toBe(0);
    expect(stepMatch(3, 0, -1)).toBe(2);
  });

  it("stays put when there is nothing to cycle", () => {
    expect(stepMatch(0, 0, 1)).toBe(0);
  });
});
