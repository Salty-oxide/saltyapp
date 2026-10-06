import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { List, ListImperativeAPI, RowComponentProps } from "react-window";
import { INDENT_PX, JsonLine, JsonTreeOverrides, LEAD_PX, NO_OVERRIDES, flattenJsonTree } from "./jsonTreeLines";
import { FindBar } from "./FindBar";
import { lineSearchText } from "./jsonTreeSearch";
import { useFind } from "./useFind";

export interface JsonTreeViewProps {
  value: unknown;
  /** Opens `value` as its own tab in the app (there's no browser to open a real new tab in). Omit to hide the button — e.g. a view that's already a dedicated JSON tab has nothing new to open. */
  onOpenInNewTab?: () => void;
  /** Numbers every rendered line down the left edge. On wherever a message payload is rendered — the payload panel and the viewer tab both — so a line in a long document can be pointed at; off by default for a caller showing something too small to need the column. */
  lineNumbers?: boolean;
  /** Set false where the surrounding panel already provides copy/open/save controls for this value, so the two toolbars don't stack. */
  showToolbar?: boolean;
}

/**
 * Row height before the real one has been measured, and the one used where
 * there is nothing to measure against (jsdom reports every box as zero).
 * Close to `.json-tree-line`'s height at the default font size.
 */
export const DEFAULT_ROW_HEIGHT_PX = 22;

/**
 * The list's height until its container has been measured.
 *
 * In the app a `ResizeObserver` replaces this on the first frame. In jsdom
 * there is no layout and the stub observer never fires, so this *is* the
 * height for the whole test — which is why it is a screenful rather than a
 * token value: a component test should see a plausible window of rows.
 */
export const DEFAULT_LIST_HEIGHT_PX = 600;

/** The line-number gutter's margin, padding and rule — everything about it that isn't characters. */
const GUTTER_CHROME_PX = 21;

/** Measured to get the width of one character; long enough that rounding in the font doesn't matter. */
const PROBE_SAMPLE = "0".repeat(40);

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

function formatPrimitive(value: unknown): string {
  if (value === null) return "null";
  if (typeof value === "string") return `"${value}"`;
  return String(value);
}

function primitiveTypeClass(value: unknown): string {
  if (value === null) return "json-tree-value--null";
  return `json-tree-value--${typeof value}`;
}

interface JsonTreeRowProps {
  lines: JsonLine[];
  lineNumbers: boolean;
  onToggle: (path: string, expanded: boolean) => void;
  /** Width of the document's widest line, so the horizontal scroll extent doesn't change as rows come and go. */
  contentWidth: number | null;
  /** Line indices matching the find query. A Set because every rendered row asks. */
  matchedLines: ReadonlySet<number>;
  /** The match the reader is currently standing on, or -1 when there is none. */
  currentMatchLine: number;
}

/** The contents of one row, without the positioning the list wraps it in. */
function LineContent({ line, onToggle }: { line: JsonLine; onToggle: JsonTreeRowProps["onToggle"] }) {
  const indent = { paddingLeft: `${line.depth * INDENT_PX}px` };

  if (line.kind === "close") {
    return (
      <span className="json-tree-line-content" style={indent}>
        <span className="json-tree-indent" aria-hidden="true" />
        <span className="json-tree-bracket">{line.isArray ? "]" : "}"}</span>
      </span>
    );
  }

  if (line.kind === "primitive") {
    return (
      <span className="json-tree-line-content" style={indent}>
        <span className="json-tree-indent" aria-hidden="true" />
        {line.label !== null && <span className="json-tree-key">{line.label}: </span>}
        <span className={`json-tree-value ${primitiveTypeClass(line.value)}`}>{formatPrimitive(line.value)}</span>
      </span>
    );
  }

  const expanded = line.expanded === true;
  return (
    <span className="json-tree-line-content" style={indent}>
      <button
        type="button"
        className={`tree-caret-button${expanded ? " tree-caret-button--expanded" : ""}`}
        aria-label={expanded ? `Collapse ${line.label ?? "value"}` : `Expand ${line.label ?? "value"}`}
        onClick={() => onToggle(line.path, !expanded)}
      >
        <span className="tree-caret" aria-hidden="true" />
      </button>
      {line.label !== null && <span className="json-tree-key">{line.label}: </span>}
      <span className="json-tree-bracket">{line.isArray ? "[" : "{"}</span>
      {!expanded && (
        <>
          <span className="json-tree-summary">
            {line.entryCount} {line.isArray ? "items" : "keys"}
          </span>
          <span className="json-tree-bracket">{line.isArray ? "]" : "}"}</span>
        </>
      )}
    </span>
  );
}

function JsonTreeRow({
  index,
  style,
  lines,
  lineNumbers,
  onToggle,
  contentWidth,
  matchedLines,
  currentMatchLine,
}: RowComponentProps<JsonTreeRowProps>) {
  const line = lines[index];
  // Two levels of highlight, because "this row matches" and "this is the one
  // you are standing on" are different questions — without the second,
  // pressing Enter appears to do nothing on a screen where every row matches.
  const isMatch = matchedLines.has(index);
  const isCurrent = index === currentMatchLine;
  const matchClass = isCurrent ? " json-tree-line--match-current" : isMatch ? " json-tree-line--match" : "";
  return (
    <div
      className={`json-tree-line${matchClass}`}
      style={contentWidth === null ? style : { ...style, minWidth: contentWidth }}
    >
      {/* The number used to be a CSS counter on `.json-tree-line::before`,
          which counted the elements in the DOM. Only a windowful of those
          exists now, so every screen would have restarted at 1 — the row's
          index in the flattened document is the only thing that still knows
          which line this is. */}
      {lineNumbers && (
        <span className="json-tree-line-number" aria-hidden="true">
          {index + 1}
        </span>
      )}
      <LineContent line={line} onToggle={onToggle} />
    </div>
  );
}

interface Metrics {
  rowHeight: number;
  /** Width of one character in the tree's font, or null before anything has been measured. */
  charWidth: number | null;
}

/**
 * The pixel geometry the list can't derive on its own.
 *
 * A virtualized list positions rows by a number, so it needs the row height up
 * front — and that height moves with the app's Font size setting, so it has to
 * be measured rather than declared.
 *
 * Width is the same problem one axis over, with a worse cause: the tree
 * scrolls horizontally, but only a windowful of rows is ever in the DOM, so
 * the browser can size to the widest row *currently on screen* and nothing
 * more — the content width would lurch about as the user scrolled. The tree
 * is set in a monospace face (see `.json-tree`), so one measured character
 * plus the character counts the flattener already has is enough to know the
 * document's width without rendering any row twice. Not rendering rows twice
 * matters beyond tidiness: a hidden copy of a row is a second match for every
 * query that looks for its text.
 *
 * Both come off the same hidden probe, re-measured whenever it resizes —
 * which is what a font change does to it.
 */
function useJsonTreeMetrics(probe: HTMLElement | null): Metrics {
  const [metrics, setMetrics] = useState<Metrics>({ rowHeight: DEFAULT_ROW_HEIGHT_PX, charWidth: null });

  useLayoutEffect(() => {
    if (!probe) return;

    function measure() {
      const element = probe as HTMLElement;
      const sample = element.querySelector<HTMLElement>(".json-tree-measure-sample");
      const height = element.offsetHeight;
      const sampleWidth = sample?.getBoundingClientRect().width ?? 0;
      setMetrics((current) => {
        const next: Metrics = {
          rowHeight: height > 0 ? height : DEFAULT_ROW_HEIGHT_PX,
          charWidth: sampleWidth > 0 ? sampleWidth / PROBE_SAMPLE.length : null,
        };
        return current.rowHeight === next.rowHeight && current.charWidth === next.charWidth ? current : next;
      });
    }

    measure();
    if (typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver(measure);
    observer.observe(probe);
    return () => observer.disconnect();
  }, [probe]);

  return metrics;
}

/**
 * A collapsible, syntax-highlighted JSON tree — every object/array node gets
 * an expand/collapse arrow. The toolbar has two icon buttons (label shown
 * on hover): copy the whole pretty-printed value to the clipboard, or open
 * it in its own tab in the app.
 *
 * The document is flattened to a list of lines and windowed, so the DOM holds
 * a screenful of rows however much of it is expanded — which is why every node
 * can start open. Rendering a line per element instead, a 4 MB payload mounted
 * ~1.5 million of them and killed the webview.
 */
export function JsonTreeView({ value, onOpenInNewTab, lineNumbers = false, showToolbar = true }: JsonTreeViewProps) {
  const [copied, setCopied] = useState(false);
  const [probe, setProbe] = useState<HTMLElement | null>(null);
  const { rowHeight, charWidth } = useJsonTreeMetrics(probe);

  // Which containers the reader has closed. Everything starts open.
  const [overrides, setOverrides] = useState<JsonTreeOverrides>(NO_OVERRIDES);
  // A different document is a different set of paths, so the old clicks mean
  // nothing — and a `payments` node the reader closed on one message must not
  // arrive closed on the next one, where it holds something else entirely.
  const [previousValue, setPreviousValue] = useState(value);
  if (previousValue !== value) {
    setPreviousValue(value);
    setOverrides(NO_OVERRIDES);
  }

  const { lines, widestLines } = useMemo(() => flattenJsonTree(value, overrides), [value, overrides]);

  /** Digits in the last line's number — what the gutter has to be wide enough for. */
  const gutterChars = lineNumbers ? String(lines.length).length + 1 : 0;

  // How wide the document is, from the widest few rows and one measured
  // character. Every row is given it as a `min-width`, so the horizontal
  // scroll extent is the document's rather than the current screenful's.
  const contentWidth = useMemo(() => {
    if (charWidth === null) return null;
    let widest = 0;
    for (const line of widestLines) {
      widest = Math.max(widest, line.depth * INDENT_PX + LEAD_PX + line.weight * charWidth);
    }
    // The gutter and its rule, the row's right padding, and a little slack for
    // the gaps `.json-tree-line-content` puts between its spans.
    return Math.ceil(widest + gutterChars * charWidth + GUTTER_CHROME_PX + 24);
  }, [widestLines, charWidth, gutterChars]);

  const onToggle = useCallback((path: string, expanded: boolean) => {
    setOverrides((current) => {
      const next = new Map(current);
      next.set(path, expanded);
      return next;
    });
  }, []);

  // --- Find -------------------------------------------------------------
  //
  // Searching the *model*, not the DOM. The list is virtualized, so only a
  // screenful of rows exists at any moment (measured: 32 rows of a 503-line
  // document). A DOM-based find — and the webview has no native one anyway —
  // would report "not found" for text that is plainly in the payload.
  const listRef = useRef<ListImperativeAPI | null>(null);
  const findUnits = useMemo(() => lines.map(lineSearchText), [lines]);
  const reveal = useCallback((row: number) => {
    // `align: "center"` rather than "auto": a match scrolled to the very
    // bottom edge is technically visible and practically missed.
    listRef.current?.scrollToRow({ index: row, align: "center", behavior: "auto" });
  }, []);
  const find = useFind(findUnits, reveal);
  const matchedLines = useMemo(() => new Set(find.matches), [find.matches]);
  const currentMatchLine = find.activeUnit;

  const copyTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(
    () => () => {
      if (copyTimer.current) clearTimeout(copyTimer.current);
    },
    [],
  );

  async function handleCopy() {
    await navigator.clipboard.writeText(JSON.stringify(value, null, 2));
    setCopied(true);
    copyTimer.current = setTimeout(() => setCopied(false), 1500);
  }

  const rowProps = useMemo<JsonTreeRowProps>(
    () => ({ lines, lineNumbers, onToggle, contentWidth, matchedLines, currentMatchLine }),
    [lines, lineNumbers, onToggle, contentWidth, matchedLines, currentMatchLine],
  );
  // Must not be inline: `List` calls it during render and cannot memoize it.
  const rowKey = useCallback((index: number, data: JsonTreeRowProps) => data.lines[index].key, []);

  return (
    <div className="json-tree json-tree--virtual" ref={find.containerRef}>
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
      <List<JsonTreeRowProps>
        className={`json-tree-body json-tree-body--virtual${lineNumbers ? " json-tree-body--numbered" : ""}`}
        role="tree"
        // The gutter is sized from the largest number it will ever hold, so
        // the content column doesn't shift left and right as the user scrolls
        // from line 9 to line 10,000.
        style={{ ["--json-tree-gutter" as string]: `${gutterChars}ch` }}
        defaultHeight={DEFAULT_LIST_HEIGHT_PX}
        rowCount={lines.length}
        rowHeight={rowHeight}
        rowKey={rowKey}
        rowProps={rowProps}
        rowComponent={JsonTreeRow}
        listRef={listRef}
      />
      {/* One sample string in the tree's own font, laid out and never painted.
          Everything the list needs to know about pixels comes from this: the
          height of a row, and the width of a character. */}
      <div className="json-tree-line json-tree-measure" aria-hidden="true" ref={setProbe}>
        <span className="json-tree-line-content">
          <span className="json-tree-indent" />
          <span className="json-tree-measure-sample">{PROBE_SAMPLE}</span>
        </span>
      </div>
    </div>
  );
}
