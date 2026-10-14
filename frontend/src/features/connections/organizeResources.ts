import type { FolderAssignment, FolderKind, ResourceFolder } from "../../lib/tauri";

/**
 * Pure arrangement of a tree list into Pinned / folders / everything else.
 * No React, no I/O, so every ordering and search rule is unit-testable.
 */

export interface OrganizeInput<T> {
  /** The list as it should read when nothing is organised (already past any call-site filter). */
  items: T[];
  getName: (item: T) => string;
  /** Defaults to "topic". Folders and assignments of another kind are ignored. */
  kind?: FolderKind;
  folders: ResourceFolder[];
  assignments: FolderAssignment[];
  /** Names pinned for this list. Only topics are pinnable; consumers pass an empty set. */
  pinned: ReadonlySet<string>;
  /** The raw search text; blank means "not searching". */
  query: string;
  /** Whether an item matches the search. */
  matches: (item: T) => boolean;
}

export interface FolderGroup<T> {
  folder: ResourceFolder;
  items: T[];
}

export interface Organized<T> {
  pinned: T[];
  folders: FolderGroup<T>[];
  unfiled: T[];
  searching: boolean;
}

export function organize<T>(input: OrganizeInput<T>): Organized<T> {
  const kind = input.kind ?? "topic";
  const searching = input.query.trim() !== "";
  const needle = input.query.trim().toLowerCase();

  const folders = input.folders
    .filter((folder) => folder.kind === kind)
    .sort((a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: "base" }));
  const folderIds = new Set(folders.map((folder) => folder.id));
  const folderOf = new Map<string, string>();
  for (const assignment of input.assignments) {
    // An assignment to a folder that is gone (or of another kind) is as good as none.
    if (assignment.kind === kind && folderIds.has(assignment.folderId)) {
      folderOf.set(assignment.resourceName, assignment.folderId);
    }
  }

  const visible = (item: T) => !searching || input.matches(item);
  const pinned: T[] = [];
  const unfiled: T[] = [];
  const members = new Map<string, T[]>(folders.map((folder) => [folder.id, []]));
  // Every item of a folder, matched or not — needed when the folder's own name matches.
  const allMembers = new Map<string, T[]>(folders.map((folder) => [folder.id, []]));

  for (const item of input.items) {
    const name = input.getName(item);
    // A pinned item lives only in Pinned; its folder assignment is kept
    // (in the database) and applies again once it is unpinned.
    if (input.pinned.has(name)) {
      if (visible(item)) pinned.push(item);
      continue;
    }
    const folderId = folderOf.get(name);
    if (folderId === undefined) {
      if (visible(item)) unfiled.push(item);
      continue;
    }
    allMembers.get(folderId)?.push(item);
    if (visible(item)) members.get(folderId)?.push(item);
  }

  const groups: FolderGroup<T>[] = [];
  for (const folder of folders) {
    const nameMatches = searching && folder.name.toLowerCase().includes(needle);
    const items = nameMatches ? (allMembers.get(folder.id) ?? []) : (members.get(folder.id) ?? []);
    if (searching && !nameMatches && items.length === 0) continue;
    groups.push({ folder, items });
  }

  return { pinned, folders: groups, unfiled, searching };
}

export type TreeRow<T> =
  | { type: "pinned-label" }
  | { type: "folder"; folder: ResourceFolder; count: number; open: boolean }
  | { type: "item"; item: T; section: "pinned" | "folder" | "unfiled"; folderId?: string };

/**
 * Flattens `organize`'s result into one row list, which is what both the
 * plain list and `react-window` render. A folder is open when the user opened
 * it, or — while searching — whenever it has anything to show, so hits are
 * never hidden behind a collapsed folder — unless the reader closed that folder
 * themselves during the search. That search-time opening is not
 * stored: clearing the search restores the folders to how they were left.
 */
export function flattenRows<T>(
  organized: Organized<T>,
  openFolderIds: ReadonlySet<string>,
  /** Folders the reader closed during this search; they stay shut until the search ends. */
  closedWhileSearching: ReadonlySet<string> = new Set(),
): TreeRow<T>[] {
  const rows: TreeRow<T>[] = [];
  if (organized.pinned.length > 0) {
    rows.push({ type: "pinned-label" });
    for (const item of organized.pinned) rows.push({ type: "item", item, section: "pinned" });
  }
  for (const group of organized.folders) {
    const open = organized.searching
      ? !closedWhileSearching.has(group.folder.id) && (group.items.length > 0 || openFolderIds.has(group.folder.id))
      : openFolderIds.has(group.folder.id);
    rows.push({ type: "folder", folder: group.folder, count: group.items.length, open });
    if (open) {
      for (const item of group.items) {
        rows.push({ type: "item", item, section: "folder", folderId: group.folder.id });
      }
    }
  }
  for (const item of organized.unfiled) rows.push({ type: "item", item, section: "unfiled" });
  return rows;
}
