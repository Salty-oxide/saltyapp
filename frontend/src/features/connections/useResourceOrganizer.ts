import { useEffect, useMemo, useRef, useState } from "react";
import type { PointerEvent as ReactPointerEvent } from "react";
import type { ContextMenuItem } from "../../components/ContextMenu";
import type { FolderKind, ResourceFolder } from "../../lib/tauri";
import { flattenRows, organize, TreeRow } from "./organizeResources";
import { useOrganization, useOrganizationActions } from "./useOrganization";
import { useTreeUiStore } from "./useTreeUiStore";

/** Pixels the pointer must travel before a press becomes a drag — below that it is a click. */
const DRAG_THRESHOLD_PX = 5;

/** What the inline name box (the one input shared by create and rename) is doing. */
export type FolderEdit =
  | { mode: "create"; moveNames?: string[] }
  | { mode: "rename"; folder: ResourceFolder };

export interface UseResourceOrganizerOptions {
  connectionId: string;
  kind: FolderKind;
  /** The list's own tree key (see `treeKey`); a folder's open state is stored under `<key>:folder:<id>`. */
  categoryKey: string;
}

/**
 * Everything a tree list needs to show pins and folders: the arranged rows,
 * the menus, the inline name box and the drag-to-folder gesture. Shared by
 * the Topics list and the generic `ResourceCategory` (Consumers), so both
 * behave identically.
 */
export function useResourceOrganizer({ connectionId, kind, categoryKey }: UseResourceOrganizerOptions) {
  const { organization } = useOrganization(connectionId);
  const actions = useOrganizationActions(connectionId);
  const expanded = useTreeUiStore((s) => s.expanded);
  const toggleExpanded = useTreeUiStore((s) => s.toggleExpanded);

  const folderKey = (id: string) => `${categoryKey}:folder:${id}`;
  const folders = useMemo(
    () => organization.folders.filter((folder) => folder.kind === kind),
    [organization.folders, kind],
  );
  const openFolderIds = useMemo(
    () => new Set(folders.filter((folder) => expanded[`${categoryKey}:folder:${folder.id}`]).map((f) => f.id)),
    [folders, expanded, categoryKey],
  );
  // Folders closed by hand while a search is active. A ref plus a tick: it is
  // cleared as soon as a render sees no search, so it never outlives one.
  const searchClosedRef = useRef(new Set<string>());
  const searchingRef = useRef(false);
  const [, setSearchTick] = useState(0);
  const pinned = useMemo(
    () => (kind === "topic" ? new Set(organization.pinnedTopics) : new Set<string>()),
    [organization.pinnedTopics, kind],
  );

  /** Arranges `items` for display. `query` is the raw search text. */
  function rowsFor<T>(
    items: T[],
    getName: (item: T) => string,
    query: string,
    matches: (item: T) => boolean,
  ): TreeRow<T>[] {
    const organized = organize({
      items,
      getName,
      kind,
      folders: organization.folders,
      assignments: organization.assignments,
      pinned,
      query,
      matches,
    });
    searchingRef.current = organized.searching;
    if (!organized.searching) searchClosedRef.current.clear();
    return flattenRows(organized, openFolderIds, searchClosedRef.current);
  }

  /** Moves each name in turn (one write at a time, so a failure stops where it happened). `null` is the root. */
  async function moveAll(names: string[], folderId: string | null) {
    for (const name of names) await actions.moveResource(kind, name, folderId);
  }

  // ---- inline name box --------------------------------------------------
  const [edit, setEdit] = useState<FolderEdit | null>(null);
  const [editError, setEditError] = useState<string | null>(null);

  function startCreate(moveNames?: string[]) {
    setEditError(null);
    setEdit({ mode: "create", moveNames });
  }
  function startRename(folder: ResourceFolder) {
    setEditError(null);
    setEdit({ mode: "rename", folder });
  }
  function cancelEdit() {
    setEdit(null);
    setEditError(null);
  }
  /** Commits the name box. A rejected name (a duplicate, too long) keeps it open with the reason. */
  async function commitEdit(name: string) {
    if (!edit) return;
    if (name.trim() === "") {
      cancelEdit();
      return;
    }
    try {
      if (edit.mode === "create") {
        const folder = await actions.createFolder(kind, name);
        if (edit.moveNames !== undefined) await moveAll(edit.moveNames, folder.id);
        if (!expanded[folderKey(folder.id)]) toggleExpanded(folderKey(folder.id));
      } else if (name.trim() !== edit.folder.name) {
        await actions.renameFolder(edit.folder.id, name);
      }
      cancelEdit();
    } catch (error) {
      setEditError(error instanceof Error ? error.message : String(error));
    }
  }

  // ---- menus --------------------------------------------------------------
  /**
   * The right-click menu for one item, or for a whole selection when given
   * several names — every entry then applies to all of them.
   */
  function itemMenuItems(target: string | string[]): ContextMenuItem[] {
    const names = Array.isArray(target) ? target : [target];
    const folderOf = (name: string) =>
      organization.assignments.find((a) => a.kind === kind && a.resourceName === name)?.folderId;
    const existing = (id: string | undefined) => (id !== undefined && folders.some((f) => f.id === id) ? id : undefined);
    const items: ContextMenuItem[] = [];
    if (kind === "topic") {
      const allPinned = names.every((name) => pinned.has(name));
      items.push({
        label: allPinned ? "Unpin from top" : "Pin to top",
        onSelect: () =>
          void (async () => {
            for (const name of names) {
              if (pinned.has(name) === allPinned) await actions.setPinned(name, !allPinned);
            }
          })(),
      });
    }
    for (const folder of folders) {
      // Nothing to offer for a folder every one of them is already in.
      if (names.every((name) => folderOf(name) === folder.id)) continue;
      items.push({
        label: `Move to folder: ${folder.name}`,
        onSelect: () => void moveAll(names, folder.id),
      });
    }
    items.push({ label: "Move to new folder…", onSelect: () => startCreate(names) });
    if (names.some((name) => existing(folderOf(name)) !== undefined)) {
      items.push({ label: "Remove from folder", onSelect: () => void moveAll(names, null) });
    }
    return items;
  }

  const headerMenuItems: ContextMenuItem[] = [{ label: "New folder…", onSelect: () => startCreate() }];

  function folderMenuItems(folder: ResourceFolder): ContextMenuItem[] {
    return [
      { label: "Rename folder", onSelect: () => startRename(folder) },
      { label: "Delete folder", destructive: true, onSelect: () => void actions.deleteFolder(folder.id) },
    ];
  }

  // ---- drag to folder -----------------------------------------------------
  const [dragging, setDragging] = useState<string | null>(null);
  /** Every name the current drag carries: the pressed row, or its whole selection. */
  const [draggingNames, setDraggingNames] = useState<string[]>([]);
  /** The folder id under the pointer (`""` for the list's background), or `null` when over nothing droppable. */
  const [dropTarget, setDropTarget] = useState<string | null>(null);
  const pressRef = useRef<{ name: string; names: string[]; x: number; y: number } | null>(null);
  const didDragRef = useRef(false);
  const actionsRef = useRef(actions);
  actionsRef.current = actions;

  function dropTargetOf(target: EventTarget | null): string | null {
    if (!(target instanceof Element)) return null;
    const folderEl = target.closest("[data-drop-folder]");
    if (folderEl) return folderEl.getAttribute("data-drop-folder");
    return target.closest("[data-drop-root]") ? "" : null;
  }

  useEffect(() => {
    function onMove(e: PointerEvent) {
      const press = pressRef.current;
      if (!press) return;
      if (dragging === null) {
        if (Math.hypot(e.clientX - press.x, e.clientY - press.y) < DRAG_THRESHOLD_PX) return;
        didDragRef.current = true;
        setDragging(press.name);
        setDraggingNames(press.names);
      }
      setDropTarget(dropTargetOf(e.target));
    }
    function onUp(e: PointerEvent) {
      const press = pressRef.current;
      pressRef.current = null;
      if (!press) return;
      setDragging(null);
      setDraggingNames([]);
      setDropTarget(null);
      if (!didDragRef.current) return;
      const target = dropTargetOf(e.target);
      if (target === null) return;
      const folderId = target === "" ? null : target;
      void (async () => {
        for (const name of press.names) await actionsRef.current.moveResource(kind, name, folderId);
      })();
    }
    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp);
    return () => {
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
    };
  }, [dragging, kind]);

  /**
   * Spread on an item row. Only the primary button starts a press. `group` is
   * the row's whole selection when it is part of one — dragging that row then
   * carries all of them.
   */
  function itemDragProps(name: string, group?: string[]) {
    return {
      onPointerDown: (e: ReactPointerEvent) => {
        if (e.button !== 0) return;
        didDragRef.current = false;
        pressRef.current = { name, names: group && group.includes(name) ? group : [name], x: e.clientX, y: e.clientY };
      },
    };
  }

  /** True once, right after a drag ends, so the click that follows a drop is not taken as a selection. */
  function consumeDragClick(): boolean {
    if (!didDragRef.current) return false;
    didDragRef.current = false;
    return true;
  }

  return {
    folders,
    pinned,
    isFolderOpen: (id: string) => openFolderIds.has(id),
    toggleFolder: (id: string, open?: boolean) => {
      if (searchingRef.current) {
        // During a search a folder's openness is derived, so a click only
        // changes this search's view; the stored open state is left alone.
        const closed = searchClosedRef.current;
        if (open === false) closed.delete(id);
        else closed.add(id);
        setSearchTick((n) => n + 1);
        return;
      }
      toggleExpanded(folderKey(id));
    },
    rowsFor,
    edit,
    editError,
    startCreate,
    cancelEdit,
    commitEdit,
    itemMenuItems,
    headerMenuItems,
    folderMenuItems,
    dragging,
    draggingNames,
    dropTarget,
    itemDragProps,
    consumeDragClick,
  };
}

export type ResourceOrganizer = ReturnType<typeof useResourceOrganizer>;
