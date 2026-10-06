import { JsonLine } from "./jsonTreeLines";

/**
 * The searchable text of one JSON tree row.
 *
 * **Why this exists at all.** `JsonTreeView` renders through `react-window`,
 * so the DOM holds only a screenful of rows however large the document is —
 * measured at 32 rows of a 503-line document, about 600 characters of text.
 * Any search that reads the DOM (a native find bar, `window.find`, a
 * `textContent` scan) therefore reports "not found" for text that is plainly
 * in the payload, which is worse than having no search: it answers the
 * question wrongly rather than declining to answer it.
 *
 * So matching runs over the flattened line model, which holds every line
 * whether or not it is on screen. The same principle the tree's siblings
 * already follow — `ResourceCategory` filters its `items` before handing
 * them to `react-window`, never the rendered rows.
 */

/**
 * The text one row is searched against: its key and, for a leaf, its value.
 *
 * Deliberately the row's *content* rather than its rendered punctuation —
 * nobody searches for a brace or a comma, and including them would make a
 * query like `{` match half the document.
 *
 * Values are stringified rather than matched only when they are strings,
 * because a reader looking for `42`, `false` or `null` is looking at what the
 * row displays and has no reason to care about its JSON type.
 */
export function lineSearchText(line: JsonLine): string {
  const label = line.label ?? "";
  if (line.kind !== "primitive") return label;
  // `String(null)` is "null", which is exactly what the row renders.
  const value = line.value === undefined ? "" : String(line.value);
  return `${label} ${value}`;
}
