import { describe, expect, it } from "vitest";
import { applyTopicClick, EMPTY_TOPIC_SELECTION } from "./topicMultiSelect";

const order = ["a", "b", "c", "d", "e"];
const plain = { toggle: false, range: false };
const toggle = { toggle: true, range: false };
const range = { toggle: false, range: true };

describe("applyTopicClick", () => {
  it("a plain click selects only that topic and anchors on it", () => {
    const next = applyTopicClick({ names: ["a", "b"], anchor: "a" }, "c", plain, order, "a");
    expect(next).toEqual({ names: ["c"], anchor: "c" });
  });

  it("ctrl+click promotes the open topic into the set and adds the clicked one", () => {
    const next = applyTopicClick(EMPTY_TOPIC_SELECTION, "c", toggle, order, "a");
    expect(next.names).toEqual(["a", "c"]);
    expect(next.anchor).toBe("c");
  });

  it("ctrl+click with nothing open starts a set of one", () => {
    const next = applyTopicClick(EMPTY_TOPIC_SELECTION, "b", toggle, order, null);
    expect(next.names).toEqual(["b"]);
  });

  it("ctrl+click on a member removes it", () => {
    const next = applyTopicClick({ names: ["a", "b", "c"], anchor: "b" }, "b", toggle, order, null);
    expect(next.names).toEqual(["a", "c"]);
  });

  it("ctrl+click keeps list order, not click order", () => {
    const first = applyTopicClick(EMPTY_TOPIC_SELECTION, "d", toggle, order, null);
    const second = applyTopicClick(first, "b", toggle, order, null);
    expect(second.names).toEqual(["b", "d"]);
  });

  it("shift+click selects the visible range from the anchor, either direction", () => {
    const down = applyTopicClick({ names: ["b"], anchor: "b" }, "d", range, order, null);
    expect(down.names).toEqual(["b", "c", "d"]);
    expect(down.anchor).toBe("b");
    const up = applyTopicClick({ names: ["d"], anchor: "d" }, "b", range, order, null);
    expect(up.names).toEqual(["b", "c", "d"]);
  });

  it("shift+click with no anchor ranges from the open topic", () => {
    const next = applyTopicClick(EMPTY_TOPIC_SELECTION, "c", range, order, "a");
    expect(next.names).toEqual(["a", "b", "c"]);
  });

  it("shift+click with neither anchor nor open topic selects just that topic", () => {
    const next = applyTopicClick(EMPTY_TOPIC_SELECTION, "c", range, order, null);
    expect(next.names).toEqual(["c"]);
  });

  it("shift+click whose anchor is no longer visible falls back to the clicked topic", () => {
    const next = applyTopicClick({ names: ["z"], anchor: "z" }, "c", range, order, null);
    expect(next.names).toEqual(["c"]);
  });

  it("ctrl+shift+click adds the range to the existing set", () => {
    const next = applyTopicClick(
      { names: ["a", "c"], anchor: "c" },
      "e",
      { toggle: true, range: true },
      order,
      null,
    );
    expect(next.names).toEqual(["a", "c", "d", "e"]);
  });
});
