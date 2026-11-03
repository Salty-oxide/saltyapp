import { create } from "zustand";
import { SETTINGS_TAB_ID } from "../settings/useSettingsPanelStore";
import { useJsonViewerTabsStore } from "../tabs/useJsonViewerTabsStore";
import { useTabsStore } from "../tabs/useTabsStore";

interface SidebarState {
  isOpen: boolean;
  toggleOpen: () => void;
}

const STORAGE_KEY = "kafkaoxide.sidebar-open";

/** Open unless the user has explicitly closed it, so a first launch always shows the tree. */
function loadStoredOpen(): boolean {
  if (typeof localStorage === "undefined") return true;
  return localStorage.getItem(STORAGE_KEY) !== "false";
}

export const useSidebarStore = create<SidebarState>((set, get) => ({
  isOpen: loadStoredOpen(),
  toggleOpen: () => {
    const next = !get().isOpen;
    if (typeof localStorage !== "undefined") {
      localStorage.setItem(STORAGE_KEY, String(next));
    }
    set({ isOpen: next });
  },
}));

/**
 * Whether the sidebar is really on screen, as opposed to what the user last
 * asked for: Settings and JSON viewer tabs have no sidebar at all, whatever
 * the toggle says. The shell and the toggle read this one answer, so the icon
 * can't stay highlighted over a pane that isn't there. `available` is false on
 * those tabs, where the toggle would have nothing to act on.
 */
export function useSidebarVisibility(): { available: boolean; visible: boolean } {
  const isOpen = useSidebarStore((s) => s.isOpen);
  const activeTabId = useTabsStore((s) => s.activeTabId);
  const isJsonTabActive = useJsonViewerTabsStore((s) => s.tabs.some((tab) => tab.id === activeTabId));
  const available = activeTabId !== SETTINGS_TAB_ID && !isJsonTabActive;
  return { available, visible: available && isOpen };
}
