import { MouseEvent as ReactMouseEvent, useEffect, useMemo } from "react";
import { useWorkspaceSelectionStore } from "../workspace/useWorkspaceSelectionStore";
import { applyTopicClick, EMPTY_TOPIC_SELECTION } from "./topicMultiSelect";
import { useTopicMultiSelectStore } from "./useTopicMultiSelectStore";

export interface UseMultiSelectOptions<T> {
  /** The list's tree key — each tab, connection and list keeps its own set. */
  storeKey: string;
  items: T[] | undefined;
  getName: (item: T) => string;
  /** The item open in the middle pane right now; it counts as selected when a set is started. */
  isOpen: (item: T) => boolean;
  /** Opens one item the normal way. */
  onOpen: (item: T) => void;
  /** Empties the workspace selection, which is what takes the middle and right panes away. */
  onClearSelection: () => void;
}

/**
 * Ctrl/Cmd+click and Shift+click multi-selection for a tree list. Two or
 * more items empty the workspace selection; one opens normally; a plain click
 * leaves the set. Shared by Topics and Consumers.
 */
export function useMultiSelect<T>({ storeKey, items, getName, isOpen, onOpen, onClearSelection }: UseMultiSelectOptions<T>) {
  const setStored = useTopicMultiSelectStore((s) => s.set);
  const clearStored = useTopicMultiSelectStore((s) => s.clear);
  const stored = useTopicMultiSelectStore((s) => s.byKey[storeKey]);
  const selection = useWorkspaceSelectionStore((s) => s.selection);

  // A multi-selection exists only while the workspace has no single selection
  // (that is what empties the middle and right panes). The set is kept apart
  // from that selection, so when anything else is selected — a broker, a
  // partition, a group, a topic in another list — it has to notice, or its
  // rows stay highlighted beside a pane showing something else.
  const hasStored = stored !== undefined;
  useEffect(() => {
    if (selection !== null && hasStored) clearStored(storeKey);
  }, [selection, hasStored, clearStored, storeKey]);

  // Names that are still listed — an item that disappeared since it was selected drops out.
  const selectedNames = useMemo(() => {
    const listed = new Set((items ?? []).map(getName));
    return (stored?.names ?? []).filter((name) => listed.has(name));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [stored, items]);
  const multiSelected = selectedNames.length >= 2;

  useEffect(() => {
    if (!multiSelected) return;
    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") clearStored(storeKey);
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [multiSelected, clearStored, storeKey]);

  /** `order` is every visible item name, top to bottom — what Shift+click ranges over. */
  function handleClick(item: T, e: ReactMouseEvent, order: string[]) {
    const toggle = e.ctrlKey || e.metaKey;
    const range = e.shiftKey;
    if (!toggle && !range) {
      if (stored) clearStored(storeKey);
      onOpen(item);
      return;
    }
    const openName = (items ?? []).find(isOpen);
    const current = multiSelected ? { ...(stored ?? EMPTY_TOPIC_SELECTION), names: selectedNames } : EMPTY_TOPIC_SELECTION;
    const next = applyTopicClick(current, getName(item), { toggle, range }, order, openName ? getName(openName) : null);
    if (next.names.length >= 2) {
      // Selection first, so the set is never stored beside a live selection.
      onClearSelection();
      setStored(storeKey, next);
      return;
    }
    clearStored(storeKey);
    const only = (items ?? []).find((candidate) => getName(candidate) === next.names[0]);
    if (only) onOpen(only);
    else onClearSelection();
  }

  return {
    selectedNames,
    multiSelected,
    /** The set a row belongs to, for drag and menu; `undefined` when nothing is multi-selected. */
    group: multiSelected ? selectedNames : undefined,
    isMultiSelected: (name: string) => multiSelected && selectedNames.includes(name),
    handleClick,
  };
}
