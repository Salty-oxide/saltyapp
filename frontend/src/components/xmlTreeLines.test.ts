import { describe, expect, it } from "vitest";
import { XmlElementNode } from "../features/connections/payloadDecoding";
import { flattenXmlTree, NO_XML_OVERRIDES, xmlLineSearchText } from "./xmlTreeLines";

function node(tag: string, children: XmlElementNode[] = [], text: string | null = null, attributes: [string, string][] = []): XmlElementNode {
  return { tag, attributes, children, text };
}

describe("flattenXmlTree", () => {
  it("renders a childless element as a single row", () => {
    const lines = flattenXmlTree(node("id", [], "42"), NO_XML_OVERRIDES);

    expect(lines).toHaveLength(1);
    expect(lines[0].kind).toBe("leaf");
    expect(lines[0].text).toBe("42");
  });

  it("wraps children between an open and a close row, as the view draws them", () => {
    const lines = flattenXmlTree(node("order", [node("id", [], "42")]), NO_XML_OVERRIDES);

    expect(lines.map((l) => l.kind)).toEqual(["open", "leaf", "close"]);
  });

  it("nests depth so indentation matches the structure", () => {
    const lines = flattenXmlTree(node("a", [node("b", [node("c", [], "x")])]), NO_XML_OVERRIDES);

    expect(lines.map((l) => l.depth)).toEqual([0, 1, 2, 1, 0]);
  });

  // The reason the model exists rather than reading the DOM: a collapsed
  // subtree contributes no rows, so row N of this list is row N on screen.
  it("omits a collapsed element's children and its close row", () => {
    const tree = node("order", [node("id", [], "42")]);
    const collapsed = flattenXmlTree(tree, new Map([["0", false]]));

    expect(collapsed.map((l) => l.kind)).toEqual(["open"]);
    expect(collapsed[0].expanded).toBe(false);
    expect(collapsed[0].childCount).toBe(1);
  });

  it("opens every node by default, so a payload arrives readable", () => {
    const lines = flattenXmlTree(node("order", [node("id", [], "42")]), NO_XML_OVERRIDES);

    expect(lines[0].expanded).toBe(true);
  });

  it("gives each row a path identifying it in the tree", () => {
    const lines = flattenXmlTree(node("a", [node("b", [], "x"), node("c", [], "y")]), NO_XML_OVERRIDES);

    expect(lines.map((l) => l.path)).toEqual(["0", "0.0", "0.1", "0"]);
  });
});

describe("xmlLineSearchText", () => {
  it("searches an element by its tag", () => {
    const [line] = flattenXmlTree(node("shipment", [], "x"), NO_XML_OVERRIDES);

    expect(xmlLineSearchText(line)).toContain("shipment");
  });

  it("searches an element's text content", () => {
    const [line] = flattenXmlTree(node("id", [], "ORDER-42"), NO_XML_OVERRIDES);

    expect(xmlLineSearchText(line)).toContain("ORDER-42");
  });

  // XML payloads routinely carry ids and codes as attributes, so a find that
  // skipped them would miss the thing most worth searching for.
  it("searches attribute names and values", () => {
    const [line] = flattenXmlTree(node("order", [], null, [["status", "shipped"]]), NO_XML_OVERRIDES);

    expect(xmlLineSearchText(line)).toContain("status");
    expect(xmlLineSearchText(line)).toContain("shipped");
  });
});
