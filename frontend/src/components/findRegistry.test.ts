import { describe, expect, it } from "vitest";
import { chooseFindTarget, FindTarget } from "./findRegistry";

function target(id: number, element: HTMLElement | null): FindTarget {
  return { id, element: () => element, open: () => {}, close: () => {} };
}

describe("chooseFindTarget", () => {
  it("picks nothing when no view is registered", () => {
    expect(chooseFindTarget([], null)).toBeNull();
  });

  it("picks the only registered view", () => {
    const only = target(1, document.createElement("div"));

    expect(chooseFindTarget([only], null)).toBe(only);
  });

  // The case the app actually hits: a JSON viewer tab in the middle pane and
  // a payload viewer in the right pane are mounted together, so "whichever
  // one the reader was last in" is the only rule that isn't a coin toss.
  it("prefers the view containing the reader's last interaction", () => {
    const middle = document.createElement("div");
    const right = document.createElement("div");
    const inRight = document.createElement("span");
    right.appendChild(inRight);

    const chosen = chooseFindTarget([target(1, middle), target(2, right)], inRight);

    expect(chosen?.id).toBe(2);
  });

  it("matches a view when the interaction is the container itself", () => {
    const middle = document.createElement("div");

    expect(chooseFindTarget([target(1, middle), target(2, document.createElement("div"))], middle)?.id).toBe(1);
  });

  // Before the reader has touched either pane there is nothing to go on, so
  // the most recently mounted one wins — it is the one that just appeared.
  it("falls back to the most recently registered view", () => {
    const chosen = chooseFindTarget(
      [target(1, document.createElement("div")), target(2, document.createElement("div"))],
      null,
    );

    expect(chosen?.id).toBe(2);
  });

  it("falls back when the last interaction was outside every view", () => {
    const outside = document.createElement("div");

    const chosen = chooseFindTarget(
      [target(1, document.createElement("div")), target(2, document.createElement("div"))],
      outside,
    );

    expect(chosen?.id).toBe(2);
  });

  it("skips a view whose element has gone", () => {
    const live = document.createElement("div");

    expect(chooseFindTarget([target(1, live), target(2, null)], null)?.id).toBe(1);
  });
});
