import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { FindBar } from "./FindBar";
import { useFind } from "./useFind";

export interface LineNumberedTextProps {
  text: string;
  /** aria-label for the code region, e.g. "Payload as hex". */
  ariaLabel?: string;
  /**
   * Forces a monospace stack instead of following the app's font setting.
   *
   * For the hex dump only, where the columns *are* the view: a proportional
   * font turns an aligned grid of byte pairs into ragged text and the offset
   * and ASCII columns stop lining up at all. Everywhere else — Raw and
   * Base64 included — the chosen font is what the user asked to read
   * payloads in.
   */
  forceMonospace?: boolean;
  /**
   * Said beside the find count when this view is showing only part of its
   * content — the Raw and Hex previews cap what they render.
   *
   * Without it, "No results" is a lie for text that is in the payload but
   * past the cap: the same class of wrong answer the find bar exists to stop
   * the DOM from giving.
   */
  findScopeNote?: string;
}

/** Fallback row height for working out where a match sits, when nothing can be measured (jsdom reports every box as zero). */
const FALLBACK_LINE_HEIGHT_PX = 18;

/**
 * Monospaced text with a line-number gutter, the way an editor shows a file.
 *
 * The gutter is its own `<pre>` beside the content rather than a number
 * prepended to each line, so selecting and copying the payload yields the
 * payload — not the payload with a line number welded to the front of every
 * line, which is what any markup that interleaves them produces.
 *
 * That alignment is only true while one logical line occupies exactly one
 * visual row, so the content deliberately does not wrap (`white-space: pre`)
 * and scrolls horizontally instead. Wrapping would slide the payload down
 * against a gutter that cannot know it happened, and every number below a
 * wrapped line would point at the wrong text.
 *
 * **The body stays one text node.** Find highlights the current match with a
 * single absolutely-positioned strip rather than by splitting the text into
 * per-line elements: a 256 KB preview is tens of thousands of lines, and
 * giving each one an element is precisely the freeze the caps upstream exist
 * to prevent. One overlay costs nothing and the `<pre>` is untouched.
 */
export function LineNumberedText({
  text,
  ariaLabel,
  forceMonospace = false,
  findScopeNote,
}: LineNumberedTextProps) {
  const bodyRef = useRef<HTMLPreElement | null>(null);
  const highlightRef = useRef<HTMLDivElement | null>(null);
  const [lineHeight, setLineHeight] = useState(FALLBACK_LINE_HEIGHT_PX);

  /**
   * The lines, which are both what the gutter numbers and what find searches.
   *
   * A trailing newline ends the last line rather than starting an empty one;
   * numbering it would show a number against no text, and find would offer a
   * blank line as a match.
   */
  const lines = useMemo(() => {
    const split = text.split("\n");
    if (split.length > 1 && split[split.length - 1] === "") split.pop();
    return split;
  }, [text]);

  // Counting was O(text) on every re-render of the viewer (panel tab
  // switches, a hover, a parent's state change); it now falls out of the
  // split above, which is memoised on the same key.
  const gutter = useMemo(() => lines.map((_, index) => String(index + 1)).join("\n"), [lines]);

  // Measured from the gutter rather than from `getComputedStyle`: the gutter
  // holds exactly one row per line, so its height divided by its line count
  // is the row height as actually laid out, font settings and all.
  const gutterRef = useRef<HTMLPreElement | null>(null);
  useEffect(() => {
    const height = gutterRef.current?.getBoundingClientRect().height ?? 0;
    if (height > 0 && lines.length > 0) setLineHeight(height / lines.length);
  }, [lines.length, forceMonospace]);

  // `reveal` cannot scroll directly: the strip moves on the render that
  // follows this call, so there is nothing at the new offset yet. The effect
  // below scrolls once it is in place.
  const reveal = useCallback(() => {}, []);
  const find = useFind(lines, reveal);

  useEffect(() => {
    // Optional-called: jsdom does not implement `scrollIntoView`.
    if (find.activeUnit >= 0) highlightRef.current?.scrollIntoView?.({ block: "center" });
  }, [find.activeUnit]);

  return (
    // A wrapper rather than a fragment: the registry needs one element to
    // decide whether a Ctrl+F belongs to this view. `display: contents` keeps
    // it out of the layout the gutter alignment depends on.
    <div className="code-find-scope" ref={find.containerRef}>
      <FindBar find={find} scopeNote={findScopeNote} />
      <div className={`code-view${forceMonospace ? " code-view--monospace" : ""}`} role="group" aria-label={ariaLabel}>
        <pre className="code-gutter" aria-hidden="true" ref={gutterRef}>
          {gutter}
        </pre>
        <div className="code-body-wrap">
          {find.activeUnit >= 0 && (
            <div
              ref={highlightRef}
              className="code-find-highlight"
              data-testid="find-highlight"
              aria-hidden="true"
              style={{ top: find.activeUnit * lineHeight, height: lineHeight }}
            />
          )}
          <pre className="code-body message-payload-body" ref={bodyRef}>
            {text}
          </pre>
        </div>
      </div>
    </div>
  );
}
