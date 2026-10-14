import { CSSProperties, useState } from "react";
import { ContextMenu, ContextMenuItem } from "../../components/ContextMenu";
import { FolderIcon, PinIcon } from "../../components/AppIcons";
import type { ResourceFolder } from "../../lib/tauri";
import type { ResourceOrganizer } from "./useResourceOrganizer";

/** The "Pinned" caption above pinned topics. */
export function PinnedLabelRow({ style }: { style?: CSSProperties }) {
  return (
    <li className="resource-section-label" style={style} data-testid="pinned-label">
      <PinIcon />
      Pinned
    </li>
  );
}

/** The small pin shown after a pinned topic's name. */
export function PinnedMark() {
  return (
    <span className="resource-pin-mark" title="Pinned" aria-label="Pinned">
      <PinIcon />
    </span>
  );
}

interface FolderHeaderRowProps {
  folder: ResourceFolder;
  count: number;
  open: boolean;
  organizer: ResourceOrganizer;
  style?: CSSProperties;
}

/** A folder's header row: caret, name and count, a right-click menu, and a drop target for dragged items. */
export function FolderHeaderRow({ folder, count, open, organizer, style }: FolderHeaderRowProps) {
  const [menu, setMenu] = useState<{ x: number; y: number } | null>(null);
  const renaming = organizer.edit?.mode === "rename" && organizer.edit.folder.id === folder.id;
  const isDropTarget = organizer.dragging !== null && organizer.dropTarget === folder.id;

  if (renaming) {
    return (
      <li className="resource-folder-edit" style={style}>
        <FolderNameInput organizer={organizer} initial={folder.name} label={`Rename folder ${folder.name}`} />
      </li>
    );
  }

  return (
    <li style={style} data-testid={`folder-${folder.name}`}>
      <div
        className={`resource-folder${open ? " resource-folder--open" : ""}${isDropTarget ? " resource-folder--drop" : ""}`}
        data-drop-folder={folder.id}
        role="button"
        tabIndex={0}
        aria-expanded={open}
        aria-label={`Folder ${folder.name}`}
        onClick={() => organizer.toggleFolder(folder.id, open)}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            organizer.toggleFolder(folder.id, open);
          }
        }}
        onContextMenu={(e) => {
          e.preventDefault();
          e.stopPropagation();
          setMenu({ x: e.clientX, y: e.clientY });
        }}
      >
        <span className="tree-caret-button" aria-hidden="true">
          <span className="tree-caret" style={open ? { transform: "rotate(90deg)" } : undefined} />
        </span>
        <FolderIcon />
        <span className="resource-folder-name">{folder.name}</span>
        <span className="resource-folder-count">{count}</span>
      </div>
      {menu && (
        <ContextMenu
          x={menu.x}
          y={menu.y}
          items={organizer.folderMenuItems(folder) as ContextMenuItem[]}
          onClose={() => setMenu(null)}
        />
      )}
    </li>
  );
}

interface FolderNameInputProps {
  organizer: ResourceOrganizer;
  initial?: string;
  label: string;
}

/** The one inline name box, for both "New folder…" and rename. Enter commits, Escape or an empty name cancels. */
export function FolderNameInput({ organizer, initial = "", label }: FolderNameInputProps) {
  const [value, setValue] = useState(initial);
  return (
    <div className="resource-folder-input">
      <input
        autoFocus
        aria-label={label}
        placeholder="Folder name"
        value={value}
        maxLength={80}
        onChange={(e) => setValue(e.target.value)}
        onBlur={() => {
          // Leaving the box commits what is there; an empty name just closes it.
          if (organizer.editError === null) void organizer.commitEdit(value);
        }}
        onKeyDown={(e) => {
          e.stopPropagation();
          if (e.key === "Enter") void organizer.commitEdit(value);
          if (e.key === "Escape") organizer.cancelEdit();
        }}
      />
      {organizer.editError && (
        <p className="resource-folder-error" role="alert">
          {organizer.editError}
        </p>
      )}
    </div>
  );
}

/** The new-folder box, rendered at the top of a list while a create is in progress. */
export function NewFolderRow({ organizer }: { organizer: ResourceOrganizer }) {
  if (organizer.edit?.mode !== "create") return null;
  return (
    <li className="resource-folder-edit">
      <FolderNameInput organizer={organizer} label="New folder name" />
    </li>
  );
}
