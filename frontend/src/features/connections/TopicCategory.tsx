import { MouseEvent as ReactMouseEvent, useState } from "react";
import { ContextMenu } from "../../components/ContextMenu";
import { TopicSummary } from "../../lib/tauri";
import { CategoryLoadWarning, CategoryWarningMarker } from "./CategoryLoadWarning";
import { useTabsStore } from "../tabs/useTabsStore";
import { useWorkspaceSelectionStore } from "../workspace/useWorkspaceSelectionStore";
import { usePartitions } from "./useClusterResources";
import { usePartitionPanelTabStore } from "./usePartitionPanelTabStore";
import { treeKey, useTreeUiStore } from "./useTreeUiStore";
import { FolderHeaderRow, NewFolderRow, PinnedLabelRow, PinnedMark } from "./OrganizedRows";
import { ResourceOrganizer, useResourceOrganizer } from "./useResourceOrganizer";

export interface TopicCategoryProps {
  connectionId: string;
  topics: TopicSummary[] | undefined;
  isLoading: boolean;
  /** The topic listing's failure, if it failed — reported on this category rather than as a failure of the whole connection. See `CategoryLoadWarning`. */
  error?: Error | null;
  /** Called exactly once, the first time this category is expanded — triggers the lazy fetch. */
  /** Called each time the category is opened (never on collapse) — refreshes the topic list, which is otherwise fetched once and held. */
  onExpand: () => void;
  isSelected: (topic: TopicSummary) => boolean;
  onSelect: (topic: TopicSummary) => void;
}

/**
 * The tree's "Topics" sub-list. Unlike Brokers/Consumers (plain
 * ResourceCategory), each topic row can itself expand to lazily fetch and
 * show its partitions, so this gets its own component rather than forcing
 * a per-item-expand concept onto the generic ResourceCategory.
 */
export function TopicCategory({ connectionId, topics, isLoading, error, onExpand, isSelected, onSelect }: TopicCategoryProps) {
  const activeTabId = useTabsStore((s) => s.activeTabId);
  const key = treeKey(activeTabId, connectionId, "Topics");
  const expanded = useTreeUiStore((s) => s.expanded[key] ?? false);
  const toggleExpanded = useTreeUiStore((s) => s.toggleExpanded);
  const query = useTreeUiStore((s) => s.searchText[key] ?? "");
  const setSearchText = useTreeUiStore((s) => s.setSearchText);
  const organizer = useResourceOrganizer({ connectionId, kind: "topic", categoryKey: key });
  const [headerMenu, setHeaderMenu] = useState<{ x: number; y: number } | null>(null);

  function toggle() {
    // Refetches on every open, not just the first — see the matching
    // comment in ResourceCategory.
    if (!expanded) {
      onExpand();
    }
    toggleExpanded(key);
  }

  const rows = organizer.rowsFor(
    topics ?? [],
    (topic) => topic.name,
    query,
    (topic) => topic.name.toLowerCase().includes(query.trim().toLowerCase()),
  );

  return (
    <li className="resource-category">
      <div
        className={`resource-category-header${expanded ? " resource-category-header--expanded" : ""}`}
        data-testid="category-Topics"
        onClick={toggle}
        onContextMenu={(e) => {
          e.preventDefault();
          setHeaderMenu({ x: e.clientX, y: e.clientY });
        }}
      >
        <span className="tree-caret" aria-hidden="true" />
        Topics
        {error && <CategoryWarningMarker label="Topics" error={error} />}
      </div>
      {headerMenu && (
        <ContextMenu x={headerMenu.x} y={headerMenu.y} items={organizer.headerMenuItems} onClose={() => setHeaderMenu(null)} />
      )}
      {expanded && (
        <div
          className={`resource-category-body${organizer.dragging !== null && organizer.dropTarget === "" ? " resource-category-body--drop" : ""}`}
          data-drop-root=""
        >
          {/* Sticky band, like the category header and cluster row above it —
              see `.resource-category-search-row`. This is the category the
              pinning matters most for: a cluster with hundreds of topics is
              exactly when you need the box that filters them while you are
              scrolling the list. */}
          <div className="resource-category-search-row">
            <input
              className="resource-category-search"
              aria-label="Search Topics"
              placeholder="Search topics…"
              value={query}
              onChange={(e) => setSearchText(key, e.target.value)}
            />
          </div>
          {error && <CategoryLoadWarning label="Topics" error={error} />}
          {isLoading && <p>Loading…</p>}
          <ul className="resource-item-list">
            <NewFolderRow organizer={organizer} />
            {rows.map((row) => {
              if (row.type === "pinned-label") return <PinnedLabelRow key="pinned-label" />;
              if (row.type === "folder") {
                return (
                  <FolderHeaderRow
                    key={`folder-${row.folder.id}`}
                    folder={row.folder}
                    count={row.count}
                    open={row.open}
                    organizer={organizer}
                  />
                );
              }
              return (
                <TopicRow
                  key={`${row.section}-${row.item.name}`}
                  connectionId={connectionId}
                  topic={row.item}
                  isSelected={isSelected(row.item)}
                  onSelect={() => onSelect(row.item)}
                  organizer={organizer}
                  pinned={row.section === "pinned"}
                  folderId={row.folderId}
                />
              );
            })}
          </ul>
        </div>
      )}
    </li>
  );
}

interface TopicRowProps {
  connectionId: string;
  topic: TopicSummary;
  isSelected: boolean;
  onSelect: () => void;
  organizer: ResourceOrganizer;
  /** Shown in the Pinned section — marks the row with a pin. */
  pinned: boolean;
  /** Set when the row sits inside an open folder: indents it and makes it a drop target for that folder. */
  folderId?: string;
}

function TopicRow({ connectionId, topic, isSelected, onSelect, organizer, pinned, folderId }: TopicRowProps) {
  const activeTabId = useTabsStore((s) => s.activeTabId);
  const key = treeKey(activeTabId, connectionId, "topic", topic.name);
  const expanded = useTreeUiStore((s) => s.expanded[key] ?? false);
  const toggleExpanded = useTreeUiStore((s) => s.toggleExpanded);
  const partitions = usePartitions(connectionId, topic.name, expanded);
  const selection = useWorkspaceSelectionStore((s) => s.selection);
  const selectPartition = useWorkspaceSelectionStore((s) => s.selectPartition);
  const setPartitionTab = usePartitionPanelTabStore((s) => s.set);
  /** Which partition's context menu is open, and where — `null` when none is. */
  const [menu, setMenu] = useState<{ partitionId: number; x: number; y: number } | null>(null);
  /** Where the topic's own (pin / move to folder) menu is open, when it is. */
  const [topicMenu, setTopicMenu] = useState<{ x: number; y: number } | null>(null);

  function toggleExpand(e: ReactMouseEvent) {
    e.stopPropagation();
    toggleExpanded(key);
  }

  function isPartitionSelected(partitionId: number) {
    return (
      selection?.type === "partition" &&
      selection.connectionId === connectionId &&
      selection.topicName === topic.name &&
      selection.partitionId === partitionId
    );
  }

  return (
    <li
      data-testid={`resource-item-${topic.name}`}
      className={folderId ? "resource-row--in-folder" : undefined}
      data-drop-folder={folderId}
    >
      <div
        className={`resource-item${isSelected ? " resource-item--selected" : ""}${organizer.dragging === topic.name ? " resource-item--dragging" : ""}`}
        onClick={() => {
          // The click that ends a drag is not a selection.
          if (organizer.consumeDragClick()) return;
          onSelect();
        }}
        onContextMenu={(e) => {
          e.preventDefault();
          setTopicMenu({ x: e.clientX, y: e.clientY });
        }}
        {...organizer.itemDragProps(topic.name)}
      >
        <button
          type="button"
          className={`tree-caret-button${expanded ? " tree-caret-button--expanded" : ""}`}
          aria-label={`${expanded ? "Collapse" : "Expand"} ${topic.name}`}
          onClick={toggleExpand}
        >
          <span className="tree-caret" aria-hidden="true" />
        </button>
        {topic.name}
        {pinned && <PinnedMark />}
      </div>
      {topicMenu && (
        <ContextMenu
          x={topicMenu.x}
          y={topicMenu.y}
          items={organizer.itemMenuItems(topic.name)}
          onClose={() => setTopicMenu(null)}
        />
      )}
      {expanded && (
        <ul className="topic-partition-list" data-testid={`partitions-${topic.name}`}>
          {partitions.isLoading && <li className="topic-partition-empty">Loading partitions…</li>}
          {!partitions.isLoading && (partitions.data?.length ?? 0) === 0 && (
            <li className="topic-partition-empty">No partitions found.</li>
          )}
          {partitions.data?.map((partition) => (
            <li
              key={partition.id}
              data-testid={`resource-item-partition-${topic.name}-${partition.id}`}
              className={`topic-partition-item${isPartitionSelected(partition.id) ? " topic-partition-item--selected" : ""}`}
              onClick={() => selectPartition(connectionId, topic.name, partition.id)}
              onContextMenu={(e) => {
                e.preventDefault();
                setMenu({ partitionId: partition.id, x: e.clientX, y: e.clientY });
              }}
            >
              Partition {partition.id}
            </li>
          ))}
        </ul>
      )}
      {menu && (
        <ContextMenu
          x={menu.x}
          y={menu.y}
          items={[
            {
              label: "Publish messages…",
              onSelect: () => {
                // Selects the partition *and* opens its Publish tab. Right-clicking
                // a partition you were not already looking at otherwise opened the
                // panel on Data, leaving the menu item half-done.
                selectPartition(connectionId, topic.name, menu.partitionId);
                setPartitionTab(activeTabId, "publish");
              },
            },
          ]}
          onClose={() => setMenu(null)}
        />
      )}
    </li>
  );
}
