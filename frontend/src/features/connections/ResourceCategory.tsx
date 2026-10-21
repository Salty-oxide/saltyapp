import { CSSProperties, useRef, useState } from "react";
import { List } from "react-window";
import { CategoryLoadWarning, CategoryWarningMarker } from "./CategoryLoadWarning";
import { ContextMenu, ContextMenuItem } from "../../components/ContextMenu";
import { ROW_HEIGHT_PX, useTreeListRows } from "./useTreeListHeight";
import { useTreeUiStore } from "./useTreeUiStore";
import { FolderHeaderRow, NewFolderRow, PinnedLabelRow } from "./OrganizedRows";
import type { TreeRow } from "./organizeResources";
import type { ResourceOrganizer } from "./useResourceOrganizer";
import { useMultiSelect } from "./useMultiSelect";

/** Below this many (post-filter) items, the plain unvirtualized `<ul>` is cheap enough to just render outright — avoids giving small lists an arbitrary fixed-height scroll box for no benefit. */
const VIRTUALIZE_THRESHOLD = 50;

export interface ResourceCategoryProps<T> {
  label: string;
  items: T[] | undefined;
  isLoading: boolean;
  getKey: (item: T) => string;
  getLabel: (item: T) => string;
  matchesSearch: (item: T, query: string) => boolean;
  isSelected: (item: T) => boolean;
  onSelect: (item: T) => void;
  /** Called each time this category is opened (never on collapse) — refreshes its listing, which is otherwise fetched once and held. */
  onExpand: () => void;
  /** Tab-scoped key (see `treeKey`) this category's expand/search state is stored under, so it starts fresh for a new tab and stays exactly as left for one you've already visited. */
  treeKey: string;
  /** Applied before matchesSearch/rendering — lets a call site narrow the list (e.g. hiding empty consumer groups) without this generic component knowing anything about what the filter means. */
  additionalFilter?: (item: T) => boolean;
  /** When given, right-clicking the category header opens a menu with these items instead of the browser's default context menu. */
  contextMenuItems?: ContextMenuItem[];
  /**
   * The failure from this category's own listing query, if it failed.
   *
   * Each category is fetched independently, so one of them being refused
   * says nothing about the other two — a principal is routinely allowed to
   * read topics and brokers but not to list consumer groups, which needs
   * `Describe` on the `Group` resource. Reporting that here, on the one
   * category it applies to, keeps the rest of the cluster usable instead of
   * presenting the whole connection as broken.
   */
  error?: Error | null;
  /**
   * Rendered inside the expanded category, above the list.
   *
   * For a category whose emptiness needs explaining rather than reporting as
   * a failure — Access Control on a cluster with no authorizer is neither an
   * error nor ordinary emptiness, and rendering it through `error` would
   * present an unsecured cluster as a broken one. Deliberately an opaque
   * node so this component stays ignorant of what the notice means.
   */
  notice?: React.ReactNode;
  /**
   * Turns on folders (and drag-to-folder) for this list. Optional so the
   * categories that have no use for them — Brokers, Access Control — stay
   * exactly as they were. The hook lives with the caller because it needs the
   * connection and kind, which this generic component deliberately doesn't know.
   */
  organizer?: ResourceOrganizer;
  /**
   * With an organizer, Ctrl/Cmd+click and Shift+click select several items;
   * this empties the workspace selection (and so the middle and right panes)
   * while two or more are selected.
   */
  onClearSelection?: () => void;
}

/**
 * One expandable, searchable, lazily-loaded sub-list in the tree (Brokers,
 * Topics, or Consumers under a connected cluster). Generic over the item
 * type so the same component backs all three.
 */
export function ResourceCategory<T>({
  label,
  items,
  isLoading,
  getKey,
  getLabel,
  matchesSearch,
  isSelected,
  onSelect,
  onExpand,
  treeKey: key,
  additionalFilter,
  contextMenuItems,
  error,
  notice,
  organizer,
  onClearSelection,
}: ResourceCategoryProps<T>) {
  const expanded = useTreeUiStore((s) => s.expanded[key] ?? false);
  const toggleExpanded = useTreeUiStore((s) => s.toggleExpanded);
  const query = useTreeUiStore((s) => s.searchText[key] ?? "");
  const setSearchText = useTreeUiStore((s) => s.setSearchText);
  const [menuPosition, setMenuPosition] = useState<{ x: number; y: number } | null>(null);
  const [itemMenu, setItemMenu] = useState<{ name: string; x: number; y: number } | null>(null);
  const bodyRef = useRef<HTMLDivElement>(null);
  const headerItems: ContextMenuItem[] = [...(contextMenuItems ?? []), ...(organizer?.headerMenuItems ?? [])];
  const visibleRows = useTreeListRows(bodyRef, expanded);

  function toggle() {
    // Every time the category is opened, not just the first time. The
    // listing behind it is fetched once and then held indefinitely (see
    // `CLUSTER_LISTING_OPTIONS`), so opening the category is the user's way
    // of saying "show me what's there now" — without this it would be the
    // only way to refresh, and it would work exactly once per session.
    // Collapsing is not a request for anything, so it doesn't fetch.
    if (!expanded) {
      onExpand();
    }
    toggleExpanded(key);
  }

  const filtered = (items ?? [])
    .filter((item) => (additionalFilter ? additionalFilter(item) : true))
    .filter((item) => matchesSearch(item, query));

  const rows: TreeRow<T>[] = organizer
    ? organizer.rowsFor(filtered, getKey, query, (item) => matchesSearch(item, query))
    : filtered.map((item) => ({ type: "item", item, section: "unfiled" }));

  const multi = useMultiSelect<T>({
    storeKey: key,
    items,
    getName: getKey,
    isOpen: isSelected,
    onOpen: onSelect,
    onClearSelection: onClearSelection ?? (() => {}),
  });
  const itemOrder = rows.flatMap((row) => (row.type === "item" ? [getKey(row.item)] : []));

  function renderRow(row: TreeRow<T>, style?: CSSProperties) {
    if (row.type === "pinned-label") return <PinnedLabelRow key="pinned-label" style={style} />;
    if (row.type === "folder" && organizer) {
      return (
        <FolderHeaderRow
          key={`folder-${row.folder.id}`}
          folder={row.folder}
          count={row.count}
          open={row.open}
          organizer={organizer}
          style={style}
        />
      );
    }
    if (row.type !== "item") return null;
    const item = row.item;
    const dragging = organizer?.draggingNames.includes(getKey(item)) ?? false;
    const selected = isSelected(item) || (organizer !== undefined && multi.isMultiSelected(getKey(item)));
    return (
      <li
        key={`${row.section}-${getKey(item)}`}
        style={style}
        data-testid={`resource-item-${getKey(item)}`}
        data-drop-folder={row.folderId}
        className={`resource-item${selected ? " resource-item--selected" : ""}${row.folderId ? " resource-row--in-folder" : ""}${dragging ? " resource-item--dragging" : ""}`}
        onClick={(e) => {
          if (organizer?.consumeDragClick()) return;
          // Without an organizer (Brokers, Access Control) there is no
          // multi-selection: every click is a plain one.
          if (organizer) multi.handleClick(item, e, itemOrder);
          else onSelect(item);
        }}
        onContextMenu={
          organizer
            ? (e) => {
                e.preventDefault();
                e.stopPropagation();
                setItemMenu({ name: getKey(item), x: e.clientX, y: e.clientY });
              }
            : undefined
        }
        {...(organizer ? organizer.itemDragProps(getKey(item), multi.group) : {})}
      >
        {getLabel(item)}
      </li>
    );
  }

  return (
    <li className="resource-category">
      <div
        className={`resource-category-header${expanded ? " resource-category-header--expanded" : ""}`}
        data-testid={`category-${label}`}
        onClick={toggle}
        onContextMenu={
          headerItems.length > 0
            ? (e) => {
                e.preventDefault();
                setMenuPosition({ x: e.clientX, y: e.clientY });
              }
            : undefined
        }
      >
        <span className="tree-caret" aria-hidden="true" />
        {label}
        {error && <CategoryWarningMarker label={label} error={error} />}
      </div>
      {menuPosition && headerItems.length > 0 && (
        <ContextMenu x={menuPosition.x} y={menuPosition.y} items={headerItems} onClose={() => setMenuPosition(null)} />
      )}
      {itemMenu && organizer && (
        <ContextMenu
          x={itemMenu.x}
          y={itemMenu.y}
          items={organizer.itemMenuItems(multi.group?.includes(itemMenu.name) ? multi.group : itemMenu.name)}
          onClose={() => setItemMenu(null)}
        />
      )}
      {expanded && (
        <div
          className={`resource-category-body${organizer?.dragging != null && organizer.dropTarget === "" ? " resource-category-body--drop" : ""}`}
          ref={bodyRef}
          data-drop-root={organizer ? "" : undefined}
          data-multiselect-list={organizer ? "" : undefined}
          tabIndex={organizer ? -1 : undefined}
          onKeyDown={organizer ? (e) => multi.handleKeyDown(e, itemOrder) : undefined}
        >
          {/* Sticky, like the category header and cluster row above it: with
              a few hundred topics the search box is the one control the user
              needs *while* scrolling the list it filters. */}
          <div className="resource-category-search-row">
            <input
              className="resource-category-search"
              aria-label={`Search ${label}`}
              placeholder={`Search ${label.toLowerCase()}…`}
              value={query}
              onChange={(e) => setSearchText(key, e.target.value)}
            />
          </div>
          {error && <CategoryLoadWarning label={label} error={error} />}
          {notice}
          {isLoading && <p>Loading…</p>}
          {organizer && (
            <ul className="resource-item-list">
              <NewFolderRow organizer={organizer} />
            </ul>
          )}
          {rows.length > VIRTUALIZE_THRESHOLD ? (
            // react-window v2 replaced `FixedSizeList` with a single `List`:
            // rows come from a `rowComponent` rather than a child render
            // function, the height moves from a `height` prop into `style`,
            // and `tagName` sets the scroll container's element — which is
            // where the `<ul>` now lives, since v2 renders rows as its direct
            // children instead of wrapping them in an inner element.
            <List
              className="resource-item-list"
              tagName="ul"
              style={{ height: Math.min(rows.length, visibleRows) * ROW_HEIGHT_PX }}
              rowCount={rows.length}
              rowHeight={ROW_HEIGHT_PX}
              rowProps={{}}
              rowComponent={({ index, style }) => renderRow(rows[index], { ...style, marginBottom: 0 })}
            />
          ) : (
            <ul className="resource-item-list">{rows.map((row) => renderRow(row))}</ul>
          )}
        </div>
      )}
    </li>
  );
}
