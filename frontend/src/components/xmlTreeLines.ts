import { XmlElementNode } from "../features/connections/payloadDecoding";

/**
 * Flattening an XML document to the rows it renders as.
 *
 * `XmlTreeView` renders recursively and is not virtualized, so it might look
 * like the DOM already holds everything a find could want. It does not: a
 * collapsed subtree is genuinely absent, and a reader searching a document
 * expects to be told a match exists inside a node they happen to have shut.
 *
 * Flattening against the same expansion state the view renders from keeps the
 * two in step — row *n* of this list is row *n* on screen — which is what
 * lets a match index be turned back into something to scroll to.
 */

export interface XmlLine {
  /** Stable identity: the child-index path from the root, e.g. "0.2.1". */
  path: string;
  depth: number;
  /** An element's opening row, its closing row, or a childless element on one line. */
  kind: "open" | "close" | "leaf";
  tag: string;
  /** Rendered attributes, for display and for searching. */
  attributes: [string, string][];
  /** Leaf rows only — the element's text content. */
  text: string | null;
  /** `open` rows only. */
  childCount?: number;
  expanded?: boolean;
}

/** Which nodes the reader has shut. Everything absent is open — the tree opens fully expanded. */
export type XmlTreeOverrides = ReadonlyMap<string, boolean>;

export const NO_XML_OVERRIDES: XmlTreeOverrides = new Map();

export function isXmlNodeExpanded(overrides: XmlTreeOverrides, path: string): boolean {
  return overrides.get(path) ?? true;
}

/** `key="value" key2="value2"`, or "" — the suffix the open tag renders with. */
export function attributesSuffix(attributes: [string, string][]): string {
  if (attributes.length === 0) return "";
  return " " + attributes.map(([key, value]) => `${key}="${value}"`).join(" ");
}

/**
 * The text one row is searched against.
 *
 * Tag name, attributes and text content — everything the row displays that a
 * reader might look for. Attribute values matter as much as element text
 * here: XML payloads routinely carry ids and codes as attributes, and a find
 * that skipped them would miss the thing most worth searching for.
 */
export function xmlLineSearchText(line: XmlLine): string {
  const attributes = line.attributes.map(([key, value]) => `${key} ${value}`).join(" ");
  return `${line.tag} ${attributes} ${line.text ?? ""}`;
}

/**
 * The document as a flat list of rows, in render order.
 *
 * A childless element is one row; an element with children is an open row,
 * its children, then a close row — and contributes no children or close row
 * at all when it is collapsed, exactly as the view renders it.
 */
export function flattenXmlTree(root: XmlElementNode, overrides: XmlTreeOverrides): XmlLine[] {
  const lines: XmlLine[] = [];

  function walk(node: XmlElementNode, depth: number, path: string) {
    if (node.children.length === 0) {
      lines.push({
        path,
        depth,
        kind: "leaf",
        tag: node.tag,
        attributes: node.attributes,
        text: node.text,
      });
      return;
    }

    const expanded = isXmlNodeExpanded(overrides, path);
    lines.push({
      path,
      depth,
      kind: "open",
      tag: node.tag,
      attributes: node.attributes,
      text: null,
      childCount: node.children.length,
      expanded,
    });
    if (!expanded) return;

    node.children.forEach((child, index) => walk(child, depth + 1, `${path}.${index}`));
    lines.push({
      path,
      depth,
      kind: "close",
      tag: node.tag,
      attributes: [],
      text: null,
    });
  }

  walk(root, 0, "0");
  return lines;
}
