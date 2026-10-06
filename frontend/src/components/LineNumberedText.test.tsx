import { describe, expect, it } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { LineNumberedText } from "./LineNumberedText";

function gutter(container: HTMLElement): string {
  return container.querySelector(".code-gutter")?.textContent ?? "";
}

describe("LineNumberedText", () => {
  it("numbers every line of the text", () => {
    const { container } = render(<LineNumberedText text={"alpha\nbeta\ngamma"} />);

    expect(gutter(container)).toBe("1\n2\n3");
  });

  it("numbers a single line as line 1", () => {
    const { container } = render(<LineNumberedText text="alpha" />);

    expect(gutter(container)).toBe("1");
  });

  // A trailing newline ends the last line rather than starting an empty one —
  // numbering it would put a number against no text.
  it("doesn't number a phantom line after a trailing newline", () => {
    const { container } = render(<LineNumberedText text={"alpha\nbeta\n"} />);

    expect(gutter(container)).toBe("1\n2");
  });

  it("numbers empty text as one line rather than none", () => {
    const { container } = render(<LineNumberedText text="" />);

    expect(gutter(container)).toBe("1");
  });

  // The gutter is its own element beside the content, never interleaved with
  // it, so selecting and copying the payload yields the payload.
  it("keeps the numbers out of the copyable text", () => {
    const { container } = render(<LineNumberedText text={"alpha\nbeta"} />);

    expect(container.querySelector(".code-body")?.textContent).toBe("alpha\nbeta");
  });

  it("hides the gutter from assistive technology", () => {
    const { container } = render(<LineNumberedText text="alpha" />);

    expect(container.querySelector(".code-gutter")).toHaveAttribute("aria-hidden", "true");
  });

  it("labels the region when given a label", () => {
    render(<LineNumberedText text="alpha" ariaLabel="Payload as Hex" />);

    expect(screen.getByRole("group", { name: "Payload as Hex" })).toBeInTheDocument();
  });
});

describe("LineNumberedText monospace override", () => {
  it("follows the app's font setting by default", () => {
    const { container } = render(<LineNumberedText text="alpha" />);

    expect(container.querySelector(".code-view")).not.toHaveClass("code-view--monospace");
  });

  // The hex dump's columns are the view; a proportional font turns an aligned
  // grid of byte pairs into ragged text.
  it("pins monospace when asked", () => {
    const { container } = render(<LineNumberedText text="alpha" forceMonospace />);

    expect(container.querySelector(".code-view")).toHaveClass("code-view--monospace");
  });
});

describe("find in text (Ctrl+F)", () => {
  const text = ["alpha line", "beta line", "gamma line", "beta again"].join("\n");

  it("opens on Ctrl+F, which previously did nothing here", async () => {
    render(<LineNumberedText text={text} />);
    expect(screen.queryByLabelText("Find in document")).not.toBeInTheDocument();

    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    expect(await screen.findByLabelText("Find in document")).toBeInTheDocument();
  });

  it("counts every matching line", async () => {
    const user = userEvent.setup();
    render(<LineNumberedText text={text} />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    await user.type(await screen.findByLabelText("Find in document"), "beta");

    expect(await screen.findByText("1 of 2")).toBeInTheDocument();
  });

  it("cycles with Enter and wraps", async () => {
    const user = userEvent.setup();
    render(<LineNumberedText text={text} />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });
    const input = await screen.findByLabelText("Find in document");
    await user.type(input, "beta");

    await user.type(input, "{Enter}");
    expect(await screen.findByText("2 of 2")).toBeInTheDocument();

    await user.type(input, "{Enter}");
    expect(await screen.findByText("1 of 2")).toBeInTheDocument();
  });

  it("says so plainly when nothing matches", async () => {
    const user = userEvent.setup();
    render(<LineNumberedText text={text} />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    await user.type(await screen.findByLabelText("Find in document"), "absent");

    expect(await screen.findByText("No results")).toBeInTheDocument();
  });

  // The body has to stay one text node — per-line elements on a 256 KB
  // preview are the freeze the upstream caps exist to prevent — so the match
  // is marked with a single strip laid over the right row.
  it("marks the current match without splitting the body into elements", async () => {
    const user = userEvent.setup();
    const { container } = render(<LineNumberedText text={text} />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    await user.type(await screen.findByLabelText("Find in document"), "gamma");
    await screen.findByText("1 of 1");

    expect(screen.getByTestId("find-highlight")).toBeInTheDocument();
    expect(container.querySelectorAll("pre.code-body")).toHaveLength(1);
  });

  it("shows no highlight until something matches", async () => {
    render(<LineNumberedText text={text} />);

    fireEvent.keyDown(document, { key: "f", ctrlKey: true });
    await screen.findByLabelText("Find in document");

    expect(screen.queryByTestId("find-highlight")).not.toBeInTheDocument();
  });

  /**
   * Raw and Hex render a capped preview. Reporting a bare "No results" there
   * would be untrue of the payload — the same wrong answer the DOM was
   * giving before any of this existed — so the scope is stated.
   */
  it("says the search only covered the shown preview when the view is capped", async () => {
    const user = userEvent.setup();
    render(<LineNumberedText text={text} findScopeNote="in the shown preview" />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    await user.type(await screen.findByLabelText("Find in document"), "absent");

    expect(await screen.findByText(/in the shown preview/)).toBeInTheDocument();
  });

  it("does not mention a preview scope when the whole value is shown", async () => {
    const user = userEvent.setup();
    render(<LineNumberedText text={text} />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    await user.type(await screen.findByLabelText("Find in document"), "absent");
    await screen.findByText("No results");

    expect(screen.queryByText(/shown preview/)).not.toBeInTheDocument();
  });
});
