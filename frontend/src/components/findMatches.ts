/**
 * Finding text across a list of searchable units.
 *
 * Shared by every view that can be searched, because each of them holds more
 * content than it has on screen — for different reasons, all of which break a
 * DOM-based find:
 *
 * - `JsonTreeView` is virtualized: ~32 of 503 rows exist at any moment.
 * - `XmlTreeView` renders every node, but a collapsed subtree is genuinely
 *   absent from the DOM.
 * - `LineNumberedText` is one `<pre>` text node deliberately, so there are no
 *   per-line elements to walk.
 *
 * And the webview has no native find bar at all (that is browser chrome, not
 * a web-engine feature), so there is nothing to fall back on.
 *
 * A "unit" is whatever the view can scroll to: a tree line, an XML row, a
 * line of text. The caller turns its content into strings; this module knows
 * nothing about what they mean.
 */

/**
 * The indices of every unit matching `query`, in document order.
 *
 * Indices rather than the units themselves: the caller needs them to reveal
 * the match — `scrollToRow` for a virtualized list, `scrollIntoView` for an
 * element, an offset for a block of text — and an index is what all three
 * take.
 *
 * A blank query matches nothing rather than everything. The bar opens empty,
 * and lighting up every line in the document at that moment would be both
 * useless and alarming.
 */
export function findMatches(units: readonly string[], query: string): number[] {
  const needle = query.trim().toLowerCase();
  if (needle.length === 0) return [];

  const matches: number[] = [];
  for (let index = 0; index < units.length; index++) {
    if (units[index].toLowerCase().includes(needle)) matches.push(index);
  }
  return matches;
}

/**
 * The next match position, wrapping at both ends.
 *
 * Wrapping is what makes pressing Enter repeatedly usable: cycling should
 * never stop at a dead end partway through, leaving the reader unsure whether
 * that was the last match or a broken button.
 */
export function stepMatch(matchCount: number, current: number, direction: 1 | -1): number {
  if (matchCount <= 0) return 0;
  return (current + direction + matchCount) % matchCount;
}
