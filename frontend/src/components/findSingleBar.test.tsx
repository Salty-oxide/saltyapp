import { afterEach, describe, expect, it } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { JsonTreeView } from "./JsonTreeView";
import { LineNumberedText } from "./LineNumberedText";
import { XmlTreeView } from "./XmlTreeView";
import { resetFindRegistry } from "./findRegistry";

afterEach(() => resetFindRegistry());

/**
 * `App.tsx` gates the right pane on a selected message, not on which tab is
 * active, so a JSON viewer tab in the middle pane and the payload viewer in
 * the right one are mounted together routinely. Each view used to bind its
 * own `document` keydown listener, so one Ctrl+F opened a find bar in every
 * one of them.
 */
describe("Ctrl+F with more than one searchable view on screen", () => {
  const xml = { tag: "order", attributes: [] as [string, string][], text: "x", children: [] };

  it("opens exactly one bar across two JSON trees", async () => {
    render(
      <>
        <div data-testid="middle">
          <JsonTreeView value={{ a: 1 }} />
        </div>
        <div data-testid="right">
          <JsonTreeView value={{ b: 2 }} />
        </div>
      </>,
    );

    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    expect(await screen.findAllByLabelText("Find in document")).toHaveLength(1);
  });

  it("opens exactly one bar across views of different kinds", async () => {
    render(
      <>
        <JsonTreeView value={{ a: 1 }} />
        <XmlTreeView value={xml} />
        <LineNumberedText text={"one\ntwo"} />
      </>,
    );

    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    expect(await screen.findAllByLabelText("Find in document")).toHaveLength(1);
  });

  // The rule that makes the choice predictable rather than a coin toss.
  it("opens in the view the reader last clicked into", async () => {
    const user = userEvent.setup();
    render(
      <>
        <div data-testid="middle">
          <JsonTreeView value={{ middle_only_key: 1 }} />
        </div>
        <div data-testid="right">
          <JsonTreeView value={{ right_only_key: 2 }} />
        </div>
      </>,
    );

    await user.click(screen.getByText(/middle_only_key/));
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    const bars = await screen.findAllByLabelText("Find in document");
    expect(bars).toHaveLength(1);
    expect(screen.getByTestId("middle")).toContainElement(bars[0]);
  });

  it("moves to the other view once the reader clicks into it", async () => {
    const user = userEvent.setup();
    render(
      <>
        <div data-testid="middle">
          <JsonTreeView value={{ middle_only_key: 1 }} />
        </div>
        <div data-testid="right">
          <JsonTreeView value={{ right_only_key: 2 }} />
        </div>
      </>,
    );

    await user.click(screen.getByText(/right_only_key/));
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    const bars = await screen.findAllByLabelText("Find in document");
    expect(bars).toHaveLength(1);
    expect(screen.getByTestId("right")).toContainElement(bars[0]);
  });

  // Switching panes must not leave the previous bar behind, or the reader
  // ends up with two again by a different route.
  it("closes the previous view's bar when the shortcut moves to another view", async () => {
    const user = userEvent.setup();
    render(
      <>
        <div data-testid="middle">
          <JsonTreeView value={{ middle_only_key: 1 }} />
        </div>
        <div data-testid="right">
          <JsonTreeView value={{ right_only_key: 2 }} />
        </div>
      </>,
    );

    await user.click(screen.getByText(/middle_only_key/));
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });
    await screen.findAllByLabelText("Find in document");

    await user.click(screen.getByText(/right_only_key/));
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    const bars = await screen.findAllByLabelText("Find in document");
    expect(bars).toHaveLength(1);
    expect(screen.getByTestId("right")).toContainElement(bars[0]);
  });

  it("still opens a bar when only one view is on screen", async () => {
    render(<JsonTreeView value={{ a: 1 }} />);

    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    expect(await screen.findAllByLabelText("Find in document")).toHaveLength(1);
  });
});
