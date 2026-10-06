import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { findMatches, stepMatch } from "./findMatches";
import { registerFindTarget } from "./findRegistry";

export interface FindController {
  open: boolean;
  query: string;
  setQuery: (query: string) => void;
  /** Indices into the unit array, in document order. */
  matches: number[];
  /** Position within `matches`, or -1 when nothing matches. */
  activeIndex: number;
  /** The unit the reader is standing on, or -1 when nothing matches. */
  activeUnit: number;
  openFind: () => void;
  closeFind: () => void;
  step: (direction: 1 | -1) => void;
  inputRef: React.RefObject<HTMLInputElement | null>;
  /**
   * Must be attached to the view's root element.
   *
   * It is how the registry decides which view a Ctrl+F belongs to when more
   * than one is on screen — see `findRegistry`.
   */
  containerRef: React.RefObject<HTMLDivElement | null>;
}

/**
 * The find bar's behaviour, without any of its markup.
 *
 * Every searchable view shares this so the interaction cannot drift between
 * them — Ctrl+F opening, typing jumping to the first hit, Enter cycling with
 * wrap, Escape closing. A reader who learns it in the JSON tree already knows
 * it in the XML tree and the hex dump.
 *
 * `units` is the searchable text of each thing the view can scroll to, and
 * `reveal` is how the view scrolls to one. Keeping both on the caller is what
 * lets a virtualized list, a DOM tree and a single `<pre>` share this.
 */
export function useFind(units: readonly string[], reveal: (unit: number) => void): FindController {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [activeIndex, setActiveIndex] = useState(0);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);

  const matches = useMemo(() => findMatches(units, query), [units, query]);

  // Clamped rather than reset: re-deriving matches after the document changes
  // shape — a collapse, a "show all" — should keep the reader near where they
  // were rather than throwing them back to match 1.
  const safeIndex = matches.length === 0 ? -1 : Math.min(activeIndex, matches.length - 1);
  const activeUnit = safeIndex === -1 ? -1 : matches[safeIndex];

  const step = useCallback(
    (direction: 1 | -1) => {
      if (matches.length === 0) return;
      const next = stepMatch(matches.length, safeIndex === -1 ? 0 : safeIndex, direction);
      setActiveIndex(next);
      reveal(matches[next]);
    },
    [matches, safeIndex, reveal],
  );

  const openFind = useCallback(() => {
    setOpen(true);
    // Deferred: the input does not exist until this render commits. Selecting
    // rather than focusing so a second Ctrl+F replaces the previous query,
    // which is what every other find bar does.
    queueMicrotask(() => inputRef.current?.select());
  }, []);

  const closeFind = useCallback(() => {
    setOpen(false);
    setQuery("");
    setActiveIndex(0);
  }, []);

  // Ctrl+F / Cmd+F is handled centrally rather than here. More than one
  // searchable view is mounted at a time — a JSON viewer tab in the middle
  // pane beside the payload viewer in the right one — and a listener per view
  // meant one keypress opened a bar in every one of them. The registry owns a
  // single listener and picks the view the reader was last inside.
  useEffect(
    () =>
      registerFindTarget({
        element: () => containerRef.current,
        open: openFind,
        close: closeFind,
      }),
    [openFind, closeFind],
  );

  // Jump to the first hit as the reader types, rather than making them press
  // Enter once before anything happens.
  useEffect(() => {
    if (matches.length > 0) {
      setActiveIndex(0);
      reveal(matches[0]);
    }
  }, [matches, reveal]);

  return {
    open,
    query,
    setQuery,
    matches,
    activeIndex: safeIndex,
    activeUnit,
    openFind,
    closeFind,
    step,
    inputRef,
    containerRef,
  };
}
