import { useCallback, useMemo, useRef, useState } from "react";
import { formatXmlNode, XmlElementNode } from "../features/connections/payloadDecoding";
import { FindBar } from "./FindBar";
import {
  attributesSuffix,
  flattenXmlTree,
  NO_XML_OVERRIDES,
  XmlLine,
  xmlLineSearchText,
  XmlTreeOverrides,
} from "./xmlTreeLines";
import { useFind } from "./useFind";

export interface XmlTreeViewProps {
  value: XmlElementNode;
  /** Opens `value` as its own tab in the app (there's no browser to open a real new tab in). Omit to hide the button — e.g. a view that's already a dedicated XML tab has nothing new to open. */
  onOpenInNewTab?: () => void;
  /** Numbers every rendered line down the left edge. On wherever a message payload is rendered — the payload panel and the viewer tab both — so a line in a long document can be pointed at; off by default for a caller showing something too small to need the column. */
  lineNumbers?: boolean;
  /** Set false where the surrounding panel already provides copy/open/save controls for this value, so the two toolbars don't stack. */
  showToolbar?: boolean;
}

function CopyIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" aria-hidden="true">
      <rect x="5.5" y="5.5" width="9" height="9" rx="1.5" stroke="currentColor" />
      <path d="M3.5 10.5h-1a1 1 0 0 1-1-1v-7a1 1 0 0 1 1-1h7a1 1 0 0 1 1 1v1" stroke="currentColor" />
    </svg>
  );
}

function CheckIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" aria-hidden="true">
      <path d="M3 8.5l3 3 7-7" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

function ExternalLinkIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" aria-hidden="true">
      <path d="M6.5 3.5h-3a1 1 0 0 0-1 1v8a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1v-3" stroke="currentColor" />
      <path d="M9.5 2.5h4v4M13.3 2.7L7.5 8.5" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

interface XmlRowProps {
  line: XmlLine;
  index: number;
  isMatch: boolean;
  isCurrent: boolean;
  onToggle: (path: string) => void;
}

/**
 * One rendered row.
 *
 * Expansion state no longer lives here. It was `useState` per node, which
 * made it impossible to know what the document currently looks like from
 * outside — and a find bar has to know, because a match inside a collapsed
 * subtree is not on screen. It is lifted to the view and keyed by path; see
 * `flattenXmlTree`.
 */
function XmlRow({ line, index, isMatch, isCurrent, onToggle }: XmlRowProps) {
  const indent = { paddingLeft: `${line.depth * 14}px` };
  const openTag = `<${line.tag}${attributesSuffix(line.attributes)}>`;
  // Two levels of highlight: "matches" and "you are here" are different
  // questions, and without the second, Enter looks like it did nothing on a
  // screen where every row matches.
  const matchClass = isCurrent ? " json-tree-line--match-current" : isMatch ? " json-tree-line--match" : "";

  return (
    // No line-number element: this tree is not virtualized, so every row is
    // in the DOM and the `.json-tree-line` CSS counter numbers them correctly
    // — which is exactly why that counter is still scoped to
    // `:not(.json-tree-body--virtual)`. Rendering a number here too would
    // print it twice.
    <div className={`json-tree-line${matchClass}`} data-testid={`xml-line-${index}`}>
      <span className="json-tree-line-content" style={indent}>
        {line.kind === "open" ? (
          <button
            type="button"
            className={`tree-caret-button${line.expanded ? " tree-caret-button--expanded" : ""}`}
            aria-label={line.expanded ? `Collapse ${line.tag}` : `Expand ${line.tag}`}
            onClick={() => onToggle(line.path)}
          >
            <span className="tree-caret" aria-hidden="true" />
          </button>
        ) : (
          <span className="json-tree-indent" aria-hidden="true" />
        )}
        {line.kind === "close" ? (
          <span className="json-tree-key">{`</${line.tag}>`}</span>
        ) : (
          <>
            <span className="json-tree-key">{openTag}</span>
            {line.kind === "leaf" && line.text !== null && (
              <span className="json-tree-value json-tree-value--string">{line.text}</span>
            )}
            {line.kind === "leaf" && <span className="json-tree-key">{`</${line.tag}>`}</span>}
            {line.kind === "open" && !line.expanded && (
              <span className="json-tree-summary">
                {line.childCount} {line.childCount === 1 ? "child" : "children"}
              </span>
            )}
          </>
        )}
      </span>
    </div>
  );
}

/**
 * A collapsible XML element tree, mirroring JsonTreeView's look and
 * interaction (expand/collapse arrows, copy, open-in-new-tab) for XML
 * payloads.
 */
export function XmlTreeView({ value, onOpenInNewTab, lineNumbers = false, showToolbar = true }: XmlTreeViewProps) {
  const [copied, setCopied] = useState(false);
  const [overrides, setOverrides] = useState<XmlTreeOverrides>(NO_XML_OVERRIDES);
  const bodyRef = useRef<HTMLDivElement | null>(null);

  const lines = useMemo(() => flattenXmlTree(value, overrides), [value, overrides]);
  const findUnits = useMemo(() => lines.map(xmlLineSearchText), [lines]);

  const onToggle = useCallback((path: string) => {
    setOverrides((current) => {
      const next = new Map(current);
      next.set(path, !(current.get(path) ?? true));
      return next;
    });
  }, []);

  // Not virtualized, so every visible row is a real element and scrolling to
  // one is just asking it to come into view. Matches inside a *collapsed*
  // subtree are still counted — they are in the model — but there is no row
  // to scroll to until the reader opens it.
  const reveal = useCallback((index: number) => {
    const row = bodyRef.current?.querySelector(`[data-testid="xml-line-${index}"]`);
    // Optional-called: jsdom does not implement `scrollIntoView`, and a
    // missing scroll must never break the search that asked for it.
    row?.scrollIntoView?.({ block: "center" });
  }, []);
  const find = useFind(findUnits, reveal);
  const matched = useMemo(() => new Set(find.matches), [find.matches]);

  async function handleCopy() {
    await navigator.clipboard.writeText(formatXmlNode(value));
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  }

  return (
    <div className="json-tree" ref={find.containerRef}>
      {showToolbar && (
      <div className="json-tree-toolbar">
        {onOpenInNewTab && (
          <button
            type="button"
            className="json-tree-icon-button"
            title="Open in new tab"
            aria-label="Open in new tab"
            onClick={onOpenInNewTab}
          >
            <ExternalLinkIcon />
          </button>
        )}
        <button
          type="button"
          className="json-tree-icon-button"
          title={copied ? "Copied!" : "Copy"}
          aria-label={copied ? "Copied!" : "Copy"}
          onClick={handleCopy}
        >
          {copied ? <CheckIcon /> : <CopyIcon />}
        </button>
      </div>
      )}
      <FindBar find={find} />
      <div
        ref={bodyRef}
        className={`json-tree-body${lineNumbers ? " json-tree-body--numbered" : ""}`}
        role="tree"
      >
        {lines.map((line, index) => (
          <XmlRow
            key={`${line.path}:${line.kind}`}
            line={line}
            index={index}
            isMatch={matched.has(index)}
            isCurrent={index === find.activeUnit}
            onToggle={onToggle}
          />
        ))}
      </div>
    </div>
  );
}
