import { create } from "zustand";
import { ValueMode } from "../workspace/useMessageViewerPrefsStore";
import { useTabOrderStore } from "./useTabOrderStore";
import { useTabsStore } from "./useTabsStore";

/**
 * `"text"` covers the payload viewer's literal formats (Raw/Hex/Base64),
 * whose value is a plain string rather than a parsed document — there is no
 * tree to show, so the tab renders it as line-numbered monospaced text.
 */
export type JsonViewerTabKind = "json" | "xml" | "text";

export interface JsonViewerTab {
  id: string;
  /** Descriptive header text shown in the panel, e.g. "Partition 2 · Offset 7" — never changes. */
  title: string;
  /** Short, user-renamable label shown in the tab strip — defaults to "Json"/"Xml". */
  name: string;
  kind: JsonViewerTabKind;
  /**
   * The payload format the tab was opened *as*, when it came from the
   * message payload viewer — purely so the tab strip can show that format's
   * own glyph.
   *
   * `kind` cannot stand in for it and is not meant to: three formats
   * (Raw/Hex/Base64) share `"text"` because they render identically, and
   * three more (JSON/Avro/Protobuf) share `"json"` for the same reason. That
   * is the right grouping for *rendering* and the wrong one for an icon,
   * where the whole point is telling a hex dump from a raw string at a
   * glance. Optional because a tab can be opened without one, and the strip
   * falls back to the `kind`.
   */
  format?: ValueMode;
  /**
   * The size of the message this tab's value came from, in bytes.
   *
   * Shown in the tab's toolbar exactly as the payload viewer shows it, so a
   * value opened into its own tab doesn't lose the one fact the pane it came
   * from was telling you about it. It is the *message's* size as the broker
   * reported it — not the length of the text rendered here, which for a
   * pretty-printed tree or a hex dump is a different number entirely.
   * Absent for a tab opened from anything that isn't a message.
   */
  payloadSizeBytes?: number;
  /** `partition-0-offset-42` — the filename stem Save defaults to. */
  fileStem?: string;
  value: unknown;
}

/**
 * What a tab opened from the message payload viewer knows about where its
 * value came from. Grouped into one argument rather than three more
 * positional parameters on `openTab`, which would otherwise take five.
 */
export interface JsonViewerTabOrigin {
  format?: ValueMode;
  payloadSizeBytes?: number;
  fileStem?: string;
}

interface JsonViewerTabsState {
  tabs: JsonViewerTab[];
  /** Creates a new ephemeral viewer tab and returns its id — doesn't activate it, callers do that via useTabsStore.selectTab. */
  openTab: (title: string, value: unknown, kind?: JsonViewerTabKind, origin?: JsonViewerTabOrigin) => string;
  closeTab: (id: string) => void;
  /** Renames the tab strip's short label — the panel header (title) is unaffected. */
  renameTab: (id: string, name: string) => void;
}

function generateId(): string {
  return `json-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

function defaultName(kind: JsonViewerTabKind): string {
  if (kind === "xml") return "Xml";
  if (kind === "text") return "Text";
  return "Json";
}

/**
 * "Open in new tab" on a JSON/XML value opens it as a tab in the app's own
 * tab bar (there's no browser to open a real new tab in — this is a desktop
 * webview). These tabs are local-only, ephemeral scratch views — unlike
 * useTabsStore's tabs, they're never persisted to the backend, so opening one
 * doesn't need a delete/rename/reorder round trip
 * for something meant to be glanced at and closed.
 */
export const useJsonViewerTabsStore = create<JsonViewerTabsState>((set) => ({
  tabs: [],
  openTab: (title, value, kind = "json", origin) => {
    const id = generateId();
    useTabOrderStore.getState().registerAfter(id, useTabsStore.getState().activeTabId);
    set((state) => ({ tabs: [...state.tabs, { id, title, value, kind, ...origin, name: defaultName(kind) }] }));
    return id;
  },
  closeTab: (id) => {
    useTabOrderStore.getState().remove(id);
    set((state) => ({ tabs: state.tabs.filter((tab) => tab.id !== id) }));
  },
  renameTab: (id, name) => {
    set((state) => ({ tabs: state.tabs.map((tab) => (tab.id === id ? { ...tab, name } : tab)) }));
  },
}));
