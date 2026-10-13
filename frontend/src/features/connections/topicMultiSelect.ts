/**
 * Pure rules for selecting several topics in the tree with Ctrl/Cmd+click and
 * Shift+click. No React, no store, so every modifier combination is
 * unit-testable. The caller decides what a result of 0, 1 or 2+ names means
 * for the single-topic selection.
 */

export interface TopicSelection {
  /** Selected topic names, in the order they appear in the list. */
  names: string[];
  /** The row a Shift+click ranges from — the last one clicked without Shift. */
  anchor: string | null;
}

export const EMPTY_TOPIC_SELECTION: TopicSelection = { names: [], anchor: null };

/**
 * @param order every topic name currently visible, top to bottom
 * @param openTopic the topic open in the middle pane right now, if any — it
 *   counts as selected, so the first Ctrl/Shift+click extends it rather than
 *   replacing it
 */
export function applyTopicClick(
  current: TopicSelection,
  name: string,
  modifiers: { toggle: boolean; range: boolean },
  order: readonly string[],
  openTopic: string | null,
): TopicSelection {
  const inOrder = (names: Iterable<string>) => {
    const wanted = new Set(names);
    return order.filter((n) => wanted.has(n));
  };
  const base = current.names.length > 0 ? current.names : openTopic ? [openTopic] : [];

  if (modifiers.range) {
    const from = current.anchor ?? openTopic;
    const a = from === null ? -1 : order.indexOf(from);
    const b = order.indexOf(name);
    if (a === -1 || b === -1) return { names: [name], anchor: name };
    const span = order.slice(Math.min(a, b), Math.max(a, b) + 1);
    const names = inOrder(modifiers.toggle ? [...base, ...span] : span);
    return { names, anchor: from };
  }

  if (modifiers.toggle) {
    const set = new Set(base);
    if (set.has(name)) set.delete(name);
    else set.add(name);
    return { names: inOrder(set), anchor: name };
  }

  return { names: [name], anchor: name };
}
