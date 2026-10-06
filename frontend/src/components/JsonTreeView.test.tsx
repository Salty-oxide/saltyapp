import { afterEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { JsonTreeView } from "./JsonTreeView";

describe("JsonTreeView", () => {
  it("renders primitive values with their keys", () => {
    render(<JsonTreeView value={{ name: "orders", count: 3, active: true, note: null }} onOpenInNewTab={vi.fn()} />);

    expect(screen.getByText('"orders"')).toBeInTheDocument();
    expect(screen.getByText("3")).toBeInTheDocument();
    expect(screen.getByText("true")).toBeInTheDocument();
    expect(screen.getByText("null")).toBeInTheDocument();
  });

  it("renders nested objects and arrays expanded by default", () => {
    render(<JsonTreeView value={{ user: { id: 1, tags: ["a", "b"] } }} onOpenInNewTab={vi.fn()} />);

    expect(screen.getByText('"a"')).toBeInTheDocument();
    expect(screen.getByText('"b"')).toBeInTheDocument();
  });

  it("collapses a node when its arrow is clicked, hiding its children", async () => {
    const user = userEvent.setup();
    render(<JsonTreeView value={{ user: { id: 1 } }} onOpenInNewTab={vi.fn()} />);
    expect(screen.getByText("1")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Collapse user" }));

    expect(screen.queryByText("1")).not.toBeInTheDocument();
    expect(screen.getByText(/1 keys/)).toBeInTheDocument();
  });

  it("re-expands a collapsed node when its arrow is clicked again", async () => {
    const user = userEvent.setup();
    render(<JsonTreeView value={{ user: { id: 1 } }} onOpenInNewTab={vi.fn()} />);

    await user.click(screen.getByRole("button", { name: "Collapse user" }));
    await user.click(screen.getByRole("button", { name: "Expand user" }));

    expect(screen.getByText("1")).toBeInTheDocument();
  });

  it("shows array indices as labels", () => {
    render(<JsonTreeView value={["first", "second"]} onOpenInNewTab={vi.fn()} />);

    expect(screen.getByText("0:")).toBeInTheDocument();
    expect(screen.getByText("1:")).toBeInTheDocument();
  });

  it("copies the pretty-printed JSON to the clipboard when Copy is clicked", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    vi.spyOn(navigator.clipboard, "writeText").mockImplementation(writeText);
    const user = userEvent.setup();
    render(<JsonTreeView value={{ a: 1 }} onOpenInNewTab={vi.fn()} />);

    await user.click(screen.getByRole("button", { name: "Copy" }));

    expect(writeText).toHaveBeenCalledWith(JSON.stringify({ a: 1 }, null, 2));
  });

  it("shows 'Copied!' briefly after copying", async () => {
    vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue(undefined);
    const user = userEvent.setup();
    render(<JsonTreeView value={{ a: 1 }} onOpenInNewTab={vi.fn()} />);

    await user.click(screen.getByRole("button", { name: "Copy" }));

    expect(await screen.findByRole("button", { name: "Copied!" })).toBeInTheDocument();
  });

  it("calls onOpenInNewTab when the 'Open in new tab' button is clicked", async () => {
    const onOpenInNewTab = vi.fn();
    const user = userEvent.setup();
    render(<JsonTreeView value={{ a: 1 }} onOpenInNewTab={onOpenInNewTab} />);

    await user.click(screen.getByRole("button", { name: "Open in new tab" }));

    expect(onOpenInNewTab).toHaveBeenCalled();
  });

  it("hides the 'Open in new tab' button when onOpenInNewTab isn't provided", () => {
    render(<JsonTreeView value={{ a: 1 }} />);

    expect(screen.queryByRole("button", { name: "Open in new tab" })).not.toBeInTheDocument();
  });

  /**
   * There is no size rule any more. The tree is windowed, so a heavy node
   * costs rows in an array rather than elements in the DOM, and a payload
   * arrives readable instead of behind a click.
   */
  it("opens a heavy array on sight", () => {
    const items = Array.from({ length: 300 }, (_, i) => ({ id: `item-${i}`, quantity: i, sku: `sku-${i}` }));

    render(<JsonTreeView value={{ events: items }} />);

    expect(screen.getByText('"item-0"')).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Expand events" })).not.toBeInTheDocument();
    expect(screen.queryByText(/300 items/)).not.toBeInTheDocument();
  });


  it("opens the root, and every node under it, on a large document", () => {
    const items = Array.from({ length: 300 }, (_, i) => ({ id: `item-${i}`, sku: `sku-${i}` }));

    render(<JsonTreeView value={{ orderId: "a-1", events: items }} />);

    expect(screen.getByText('"a-1"')).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Collapse events" })).toBeInTheDocument();
  });

  it("still expands an ordinary-sized container on sight", () => {
    render(<JsonTreeView value={{ order: { id: "a-1" } }} />);

    expect(screen.getByText('"a-1"')).toBeInTheDocument();
  });
});

describe("JsonTreeView line numbers and toolbar", () => {
  it("numbers the lines only when asked to", () => {
    const { container, rerender } = render(<JsonTreeView value={{ a: 1 }} />);
    expect(container.querySelector(".json-tree-body--numbered")).toBeNull();

    rerender(<JsonTreeView value={{ a: 1 }} lineNumbers />);
    expect(container.querySelector(".json-tree-body--numbered")).not.toBeNull();
  });

  // The indent has to sit on the line's content rather than on the line box,
  // or the number column staggers right along with each node's depth.
  it("keeps the indent off the line box so the number column stays straight", () => {
    const { container } = render(<JsonTreeView value={{ outer: { inner: 1 } }} lineNumbers />);

    for (const line of container.querySelectorAll<HTMLElement>(".json-tree-line")) {
      expect(line.style.paddingLeft).toBe("");
      expect(line.querySelector(".json-tree-line-content")).not.toBeNull();
    }
  });

  it("hides its own toolbar when the surrounding panel provides one", () => {
    const { container } = render(<JsonTreeView value={{ a: 1 }} onOpenInNewTab={() => {}} showToolbar={false} />);

    expect(container.querySelector(".json-tree-toolbar")).toBeNull();
    expect(screen.queryByRole("button", { name: "Copy" })).not.toBeInTheDocument();
  });
});

/**
 * The tree is virtualized: only the rows near the viewport are real DOM
 * elements, however much of the document is expanded.
 *
 * Without this, Expand all on a large payload mounts every line at once —
 * measured in a real browser before the change, a 1 MB payload blocked the
 * main thread for 6.6s and a 4 MB payload crashed the renderer outright.
 */
/**
 * The tree is virtualized: only the rows near the viewport are real DOM
 * elements, however much of the document is expanded. Without this, opening a
 * large payload fully expanded would mount every line at once — measured in a
 * real browser, a 1 MB payload blocked the main thread for 6.6s and a 4 MB one
 * crashed the renderer.
 */
describe("JsonTreeView virtualization", () => {
  it("renders only a window of rows for a fully expanded document", () => {
    // 2,000 three-field objects: 10,002 rendered lines, all of them open.
    const items = Array.from({ length: 2000 }, (_, i) => ({ id: `item-${i}`, sku: `sku-${i}`, note: "x" }));
    const { container } = render(<JsonTreeView value={{ events: items }} showToolbar={false} />);

    const rendered = container.querySelectorAll(".json-tree-line").length;
    expect(rendered).toBeGreaterThan(0);
    expect(rendered).toBeLessThan(200);
    expect(screen.queryByText('"item-1999"')).not.toBeInTheDocument();
    // Expanded, not collapsed: the first rows are the document's real content.
    expect(screen.getByText('"item-0"')).toBeInTheDocument();
  });
});

/**
 * Only a windowful of rows is in the DOM, so the browser can size the scroll
 * box to the widest row *on screen* and nothing more — the content would get
 * wider and narrower as the user scrolled. The tree is monospace, so the
 * document's width comes from one measured character instead.
 *
 * jsdom reports every box as zero, so the measurement has to be faked to
 * exercise it at all; what is asserted is the consequence, not the number.
 */
describe("JsonTreeView content width", () => {
  const CHAR_PX = 8;

  // The measurement spy is global; left in place it would size the rows in
  // the next test too.
  afterEach(() => {
    vi.restoreAllMocks();
  });

  function withMeasurableText() {
    return vi.spyOn(Element.prototype, "getBoundingClientRect").mockImplementation(function (this: Element) {
      const width = this.classList.contains("json-tree-measure-sample")
        ? (this.textContent ?? "").length * CHAR_PX
        : 0;
      return { width, height: 0, top: 0, left: 0, right: width, bottom: 0, x: 0, y: 0, toJSON: () => ({}) };
    });
  }

  it("gives every row the width of the document's longest line", () => {
    withMeasurableText();
    const { container } = render(<JsonTreeView value={{ short: "a", longest: "x".repeat(200) }} />);

    const rows = container.querySelectorAll<HTMLElement>(".json-tree-body .json-tree-line");
    const widths = [...rows].map((row) => row.style.minWidth);

    expect(widths.length).toBeGreaterThan(1);
    // Every row the same, and wide enough for the 200-character value rather
    // than for whichever row happens to be on screen.
    expect(new Set(widths).size).toBe(1);
    expect(parseInt(widths[0], 10)).toBeGreaterThan(200 * CHAR_PX);
  });

  it("leaves the rows unconstrained when nothing can be measured", () => {
    const { container } = render(<JsonTreeView value={{ a: 1 }} />);

    for (const row of container.querySelectorAll<HTMLElement>(".json-tree-body .json-tree-line")) {
      expect(row.style.minWidth).toBe("");
    }
  });
});

describe("find in document (Ctrl+F)", () => {
  /**
   * The reported bug. The tree is virtualized — only ~32 rows of a 500-line
   * document are ever in the DOM — so a find that reads the DOM reports "not
   * found" for text that is plainly in the payload. These tests drive the
   * find bar, not the DOM, and assert on the *count*, which is derived from
   * the line model.
   */
  function bigDocument() {
    const value: Record<string, string> = {};
    for (let i = 0; i < 500; i++) value[`key_${i}`] = `value_${i}`;
    value.needle = "FINDME_UNIQUE";
    return value;
  }

  it("opens on Ctrl+F, which previously did nothing at all", async () => {
    render(<JsonTreeView value={{ a: 1 }} />);
    expect(screen.queryByLabelText("Find in document")).not.toBeInTheDocument();

    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    expect(await screen.findByLabelText("Find in document")).toBeInTheDocument();
  });

  it("opens on Cmd+F for macOS", async () => {
    render(<JsonTreeView value={{ a: 1 }} />);

    fireEvent.keyDown(document, { key: "f", metaKey: true });

    expect(await screen.findByLabelText("Find in document")).toBeInTheDocument();
  });

  // The regression itself: a match far outside the rendered window.
  it("finds a string that virtualization keeps out of the DOM", async () => {
    const user = userEvent.setup();
    render(<JsonTreeView value={bigDocument()} />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    await user.type(await screen.findByLabelText("Find in document"), "FINDME_UNIQUE");

    expect(await screen.findByText("1 of 1")).toBeInTheDocument();
  });

  it("counts every match across the whole document, not just the rendered rows", async () => {
    const user = userEvent.setup();
    render(<JsonTreeView value={bigDocument()} />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    await user.type(await screen.findByLabelText("Find in document"), "value_");

    // 500 leaf values, all outside the ~32-row window except the first few.
    expect(await screen.findByText("1 of 500")).toBeInTheDocument();
  });

  it("says so plainly when nothing matches", async () => {
    const user = userEvent.setup();
    render(<JsonTreeView value={{ a: 1 }} />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    await user.type(await screen.findByLabelText("Find in document"), "absent");

    expect(await screen.findByText("No results")).toBeInTheDocument();
  });

  it("steps to the next match on Enter and wraps at the end", async () => {
    const user = userEvent.setup();
    render(<JsonTreeView value={{ a: "hit", b: "hit", c: "hit" }} />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });
    const input = await screen.findByLabelText("Find in document");
    await user.type(input, "hit");
    expect(await screen.findByText("1 of 3")).toBeInTheDocument();

    await user.type(input, "{Enter}");
    expect(await screen.findByText("2 of 3")).toBeInTheDocument();

    await user.type(input, "{Enter}{Enter}");
    expect(await screen.findByText("1 of 3")).toBeInTheDocument();
  });

  it("steps with the next and previous buttons, for readers who don't reach for Enter", async () => {
    const user = userEvent.setup();
    render(<JsonTreeView value={{ a: "hit", b: "hit", c: "hit" }} />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });
    await user.type(await screen.findByLabelText("Find in document"), "hit");
    await screen.findByText("1 of 3");

    await user.click(screen.getByLabelText("Next match"));
    expect(await screen.findByText("2 of 3")).toBeInTheDocument();

    await user.click(screen.getByLabelText("Previous match"));
    expect(await screen.findByText("1 of 3")).toBeInTheDocument();
  });

  it("disables the step buttons when nothing matches", async () => {
    const user = userEvent.setup();
    render(<JsonTreeView value={{ a: "hit" }} />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    await user.type(await screen.findByLabelText("Find in document"), "absent");
    await screen.findByText("No results");

    expect(screen.getByLabelText("Next match")).toBeDisabled();
    expect(screen.getByLabelText("Previous match")).toBeDisabled();
  });

  it("closes with the ✕ button as well as Escape", async () => {
    const user = userEvent.setup();
    render(<JsonTreeView value={{ a: "hit" }} />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });
    await screen.findByLabelText("Find in document");

    await user.click(screen.getByLabelText("Close find"));

    expect(screen.queryByLabelText("Find in document")).not.toBeInTheDocument();
  });

  it("steps backwards on Shift+Enter", async () => {
    const user = userEvent.setup();
    render(<JsonTreeView value={{ a: "hit", b: "hit", c: "hit" }} />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });
    const input = await screen.findByLabelText("Find in document");
    await user.type(input, "hit");

    await user.type(input, "{Shift>}{Enter}{/Shift}");

    expect(await screen.findByText("3 of 3")).toBeInTheDocument();
  });

  it("closes on Escape and forgets the query", async () => {
    const user = userEvent.setup();
    render(<JsonTreeView value={{ a: "hit" }} />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });
    const input = await screen.findByLabelText("Find in document");
    await user.type(input, "hit");

    await user.type(input, "{Escape}");

    expect(screen.queryByLabelText("Find in document")).not.toBeInTheDocument();
  });

  it("marks the row it is standing on differently from the other matches", async () => {
    const user = userEvent.setup();
    const { container } = render(<JsonTreeView value={{ a: "hit", b: "hit" }} />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    await user.type(await screen.findByLabelText("Find in document"), "hit");
    await screen.findByText("1 of 2");

    expect(container.querySelectorAll(".json-tree-line--match-current")).toHaveLength(1);
    expect(container.querySelectorAll(".json-tree-line--match")).toHaveLength(1);
  });

  // An empty bar must not light up every row in the document.
  it("highlights nothing while the query is still empty", async () => {
    const { container } = render(<JsonTreeView value={{ a: "hit", b: "hit" }} />);

    fireEvent.keyDown(document, { key: "f", ctrlKey: true });
    await screen.findByLabelText("Find in document");

    expect(container.querySelectorAll(".json-tree-line--match")).toHaveLength(0);
    expect(container.querySelectorAll(".json-tree-line--match-current")).toHaveLength(0);
  });
});
