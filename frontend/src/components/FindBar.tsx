import { FindController } from "./useFind";

export interface FindBarProps {
  find: FindController;
  /**
   * Said after the match count when the view is only showing part of its
   * content — the Raw and Hex previews cap what they render.
   *
   * Without it "No results" is a lie for text that is in the payload but past
   * the cap, which is the same class of wrong answer the find bar was built
   * to stop the DOM from giving.
   */
  scopeNote?: string;
}

/**
 * The find bar, shared by every searchable view.
 *
 * One component so the interaction cannot drift: the count reads the same,
 * Enter and Shift+Enter do the same, Escape closes the same. The *searching*
 * differs per view — a virtualized list, a collapsible tree and a single
 * `<pre>` reveal a match in three different ways — but none of that is
 * visible here.
 */
export function FindBar({ find, scopeNote }: FindBarProps) {
  if (!find.open) return null;

  const hasQuery = find.query.trim().length > 0;
  const count = !hasQuery
    ? ""
    : find.matches.length === 0
      ? "No results"
      : `${find.activeIndex + 1} of ${find.matches.length}`;

  return (
    <div className="json-tree-find" role="search">
      <input
        ref={find.inputRef}
        className="json-tree-find-input"
        type="text"
        aria-label="Find in document"
        placeholder="Find…"
        value={find.query}
        onChange={(e) => find.setQuery(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter") {
            e.preventDefault();
            find.step(e.shiftKey ? -1 : 1);
          } else if (e.key === "Escape") {
            e.preventDefault();
            find.closeFind();
          }
        }}
      />
      {/* Derived from the model, so it counts matches the DOM has never
          rendered — which is the whole reason this bar exists. */}
      <span className="json-tree-find-count" role="status">
        {count}
        {hasQuery && scopeNote ? ` · ${scopeNote}` : ""}
      </span>
      <button
        type="button"
        className="json-tree-icon-button"
        aria-label="Previous match"
        title="Previous match (Shift+Enter)"
        disabled={find.matches.length === 0}
        onClick={() => find.step(-1)}
      >
        ↑
      </button>
      <button
        type="button"
        className="json-tree-icon-button"
        aria-label="Next match"
        title="Next match (Enter)"
        disabled={find.matches.length === 0}
        onClick={() => find.step(1)}
      >
        ↓
      </button>
      <button
        type="button"
        className="json-tree-icon-button"
        aria-label="Close find"
        title="Close (Esc)"
        onClick={find.closeFind}
      >
        ✕
      </button>
    </div>
  );
}
