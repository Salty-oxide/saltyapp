import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { XmlTreeView } from "./XmlTreeView";
import { tryParseXml, XmlElementNode } from "../features/connections/payloadDecoding";

function parse(xml: string): XmlElementNode {
  const node = tryParseXml(xml);
  if (!node) throw new Error("test fixture XML failed to parse");
  return node;
}

describe("XmlTreeView", () => {
  it("renders a leaf element's text content", () => {
    render(<XmlTreeView value={parse("<name>orders</name>")} onOpenInNewTab={vi.fn()} />);

    expect(screen.getByText("orders")).toBeInTheDocument();
  });

  it("renders attributes inline with the opening tag", () => {
    render(<XmlTreeView value={parse('<user id="1" active="true"/>')} onOpenInNewTab={vi.fn()} />);

    expect(screen.getByText('<user id="1" active="true">')).toBeInTheDocument();
  });

  it("renders nested child elements expanded by default", () => {
    render(<XmlTreeView value={parse("<root><a>1</a><b>2</b></root>")} onOpenInNewTab={vi.fn()} />);

    expect(screen.getByText("1")).toBeInTheDocument();
    expect(screen.getByText("2")).toBeInTheDocument();
  });

  it("collapses a node when its arrow is clicked, hiding its children", async () => {
    const user = userEvent.setup();
    render(<XmlTreeView value={parse("<root><child>1</child></root>")} onOpenInNewTab={vi.fn()} />);
    expect(screen.getByText("1")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Collapse root" }));

    expect(screen.queryByText("1")).not.toBeInTheDocument();
    expect(screen.getByText(/1 child/)).toBeInTheDocument();
  });

  it("re-expands a collapsed node when its arrow is clicked again", async () => {
    const user = userEvent.setup();
    render(<XmlTreeView value={parse("<root><child>1</child></root>")} onOpenInNewTab={vi.fn()} />);

    await user.click(screen.getByRole("button", { name: "Collapse root" }));
    await user.click(screen.getByRole("button", { name: "Expand root" }));

    expect(screen.getByText("1")).toBeInTheDocument();
  });

  it("copies the pretty-printed XML to the clipboard when Copy is clicked", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    vi.spyOn(navigator.clipboard, "writeText").mockImplementation(writeText);
    const user = userEvent.setup();
    render(<XmlTreeView value={parse("<a>1</a>")} onOpenInNewTab={vi.fn()} />);

    await user.click(screen.getByRole("button", { name: "Copy" }));

    expect(writeText).toHaveBeenCalledWith("<a>1</a>");
  });

  it("shows 'Copied!' briefly after copying", async () => {
    vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue(undefined);
    const user = userEvent.setup();
    render(<XmlTreeView value={parse("<a>1</a>")} onOpenInNewTab={vi.fn()} />);

    await user.click(screen.getByRole("button", { name: "Copy" }));

    expect(await screen.findByRole("button", { name: "Copied!" })).toBeInTheDocument();
  });

  it("calls onOpenInNewTab when the 'Open in new tab' button is clicked", async () => {
    const onOpenInNewTab = vi.fn();
    const user = userEvent.setup();
    render(<XmlTreeView value={parse("<a>1</a>")} onOpenInNewTab={onOpenInNewTab} />);

    await user.click(screen.getByRole("button", { name: "Open in new tab" }));

    expect(onOpenInNewTab).toHaveBeenCalled();
  });

  it("hides the 'Open in new tab' button when onOpenInNewTab isn't provided", () => {
    render(<XmlTreeView value={parse("<a>1</a>")} />);

    expect(screen.queryByRole("button", { name: "Open in new tab" })).not.toBeInTheDocument();
  });
});

describe("XmlTreeView line numbers and toolbar", () => {
  const node = { tag: "order", attributes: [] as [string, string][], children: [], text: "42" };

  it("numbers the lines only when asked to", () => {
    const { container, rerender } = render(<XmlTreeView value={node} />);
    expect(container.querySelector(".json-tree-body--numbered")).toBeNull();

    rerender(<XmlTreeView value={node} lineNumbers />);
    expect(container.querySelector(".json-tree-body--numbered")).not.toBeNull();
  });

  it("hides its own toolbar when the surrounding panel provides one", () => {
    const { container } = render(<XmlTreeView value={node} onOpenInNewTab={() => {}} showToolbar={false} />);

    expect(container.querySelector(".json-tree-toolbar")).toBeNull();
  });
});

describe("find in an XML document (Ctrl+F)", () => {
  const tree = {
    tag: "orders",
    attributes: [] as [string, string][],
    text: null,
    children: [
      { tag: "order", attributes: [["status", "shipped"]] as [string, string][], text: null, children: [
        { tag: "id", attributes: [] as [string, string][], text: "ORDER-42", children: [] },
      ] },
      { tag: "order", attributes: [["status", "pending"]] as [string, string][], text: null, children: [
        { tag: "id", attributes: [] as [string, string][], text: "ORDER-43", children: [] },
      ] },
    ],
  };

  it("opens on Ctrl+F, which previously did nothing here", async () => {
    render(<XmlTreeView value={tree} />);
    expect(screen.queryByLabelText("Find in document")).not.toBeInTheDocument();

    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    expect(await screen.findByLabelText("Find in document")).toBeInTheDocument();
  });

  it("finds an element by its text content", async () => {
    const user = userEvent.setup();
    render(<XmlTreeView value={tree} />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    await user.type(await screen.findByLabelText("Find in document"), "ORDER-43");

    expect(await screen.findByText("1 of 1")).toBeInTheDocument();
  });

  // Attributes are where XML payloads keep their ids and codes.
  it("finds an element by an attribute value", async () => {
    const user = userEvent.setup();
    render(<XmlTreeView value={tree} />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    await user.type(await screen.findByLabelText("Find in document"), "pending");

    expect(await screen.findByText("1 of 1")).toBeInTheDocument();
  });

  it("counts every matching row and cycles with Enter", async () => {
    const user = userEvent.setup();
    render(<XmlTreeView value={tree} />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });
    const input = await screen.findByLabelText("Find in document");

    await user.type(input, "ORDER-");
    expect(await screen.findByText("1 of 2")).toBeInTheDocument();

    await user.type(input, "{Enter}");
    expect(await screen.findByText("2 of 2")).toBeInTheDocument();
  });

  it("says so plainly when nothing matches", async () => {
    const user = userEvent.setup();
    render(<XmlTreeView value={tree} />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    await user.type(await screen.findByLabelText("Find in document"), "absent");

    expect(await screen.findByText("No results")).toBeInTheDocument();
  });

  it("marks the row it is standing on differently from the other matches", async () => {
    const user = userEvent.setup();
    const { container } = render(<XmlTreeView value={tree} />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });

    await user.type(await screen.findByLabelText("Find in document"), "ORDER-");
    await screen.findByText("1 of 2");

    expect(container.querySelectorAll(".json-tree-line--match-current")).toHaveLength(1);
    expect(container.querySelectorAll(".json-tree-line--match")).toHaveLength(1);
  });

  // The behaviour that made the flattened model necessary: a collapsed
  // subtree has no rows on screen, so the count has to follow the document
  // as the reader actually has it open.
  it("stops counting rows inside a subtree the reader has collapsed", async () => {
    const user = userEvent.setup();
    render(<XmlTreeView value={tree} />);
    fireEvent.keyDown(document, { key: "f", ctrlKey: true });
    await user.type(await screen.findByLabelText("Find in document"), "ORDER-");
    expect(await screen.findByText("1 of 2")).toBeInTheDocument();

    await user.click(screen.getAllByLabelText(/^Collapse order$/)[0]);

    expect(await screen.findByText("1 of 1")).toBeInTheDocument();
  });
});
