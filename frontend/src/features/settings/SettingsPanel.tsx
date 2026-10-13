import { useEffect, useState } from "react";
import { ThemePicker } from "../theme/ThemePicker";
import { FONT_FAMILIES, FONT_SIZE_OPTIONS_PX } from "./fonts";
import { Dropdown } from "../../components/Dropdown";
import { activeFontFamilyId, usePreferencesStore } from "./usePreferencesStore";
import {
  MAX_PAYLOAD_CACHE_BYTES,
  MIN_PAYLOAD_CACHE_BYTES,
  useGeneralSettingsStore,
} from "./useGeneralSettingsStore";

const MIB = 1024 * 1024;

const FONT_FAMILY_OPTIONS = FONT_FAMILIES.map((f) => ({ id: f.id, label: f.label }));
const FONT_SIZE_OPTIONS = FONT_SIZE_OPTIONS_PX.map((px) => ({ id: String(px), label: `${px}px` }));

type SettingsTabId = "general" | "appearance";

const SETTINGS_TABS: { id: SettingsTabId; label: string }[] = [
  { id: "general", label: "General" },
  { id: "appearance", label: "Appearance" },
];

export function SettingsPanel() {
  const [activeTab, setActiveTab] = useState<SettingsTabId>("general");

  return (
    <div className="settings-panel">
      <h2>Settings</h2>

      <div className="settings-panel__tabs" role="tablist" aria-label="Settings sections">
        {SETTINGS_TABS.map((tab) => (
          <button
            key={tab.id}
            type="button"
            role="tab"
            aria-selected={activeTab === tab.id}
            className="settings-panel__tab"
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {activeTab === "general" ? <GeneralSettingsTab /> : <AppearanceSettingsTab />}
    </div>
  );
}

/**
 * The payload cache's size, in MB. Edited as text and applied on blur or
 * Enter rather than on every keystroke: applying a size empties the cache, and
 * the minimum would otherwise rewrite "512" to "64" the moment "5" was typed.
 */
function PayloadCacheSizeField() {
  const payloadCacheBytes = useGeneralSettingsStore((s) => s.payloadCacheBytes);
  const setPayloadCacheBytes = useGeneralSettingsStore((s) => s.setPayloadCacheBytes);
  const stored = String(Math.round(payloadCacheBytes / MIB));
  const [draft, setDraft] = useState(stored);

  // Follows the setting when it changes from outside this field — including
  // the clamp this field's own commit just applied.
  useEffect(() => setDraft(stored), [stored]);

  function commit() {
    const megabytes = Number(draft);
    if (draft.trim() === "" || !Number.isFinite(megabytes)) {
      setDraft(stored);
      return;
    }
    setPayloadCacheBytes(megabytes * MIB);
    // Show what was actually applied (the clamped value), even when that is
    // the value already stored and so would not change `stored` on its own.
    setDraft(String(Math.round(useGeneralSettingsStore.getState().payloadCacheBytes / MIB)));
  }

  return (
    <>
      <label className="dropdown-field">
        <span>Payload Cache Size (MB)</span>
        <input
          type="number"
          min={0}
          step={64}
          aria-label="Payload cache size (MB)"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onBlur={commit}
          onKeyDown={(e) => {
            if (e.key === "Enter") commit();
          }}
        />
      </label>
      <p className="settings-panel__hint">
        Memory the app may use to keep message payloads larger than 4 KB, compressed, so opening a message is instant
        instead of a trip to the broker. {payloadCacheBytes === 0 ? "The cache is off. " : ""}
        0 turns it off; otherwise {MIN_PAYLOAD_CACHE_BYTES / MIB}–{MAX_PAYLOAD_CACHE_BYTES / MIB} MB. This is separate
        from — and in addition to — the memory the tabs hold. Changing it empties the cache. Entries not opened for 15
        minutes are dropped regardless.
      </p>
    </>
  );
}

function GeneralSettingsTab() {
  const zookeeperTimeoutMs = useGeneralSettingsStore((s) => s.zookeeperTimeoutMs);
  const setZookeeperTimeoutMs = useGeneralSettingsStore((s) => s.setZookeeperTimeoutMs);
  const brokerReadTimeoutMs = useGeneralSettingsStore((s) => s.brokerReadTimeoutMs);
  const setBrokerReadTimeoutMs = useGeneralSettingsStore((s) => s.setBrokerReadTimeoutMs);
  const maxMessageSizeBytes = useGeneralSettingsStore((s) => s.maxMessageSizeBytes);
  const maxTotalFetchBytes = useGeneralSettingsStore((s) => s.maxTotalFetchBytes);
  const setMaxTotalFetchBytes = useGeneralSettingsStore((s) => s.setMaxTotalFetchBytes);
  const setMaxMessageSizeBytes = useGeneralSettingsStore((s) => s.setMaxMessageSizeBytes);

  return (
    <div className="settings-panel__tabpanel" role="tabpanel" aria-label="General">
      <section className="settings-panel__section">
        <h3>Zookeeper</h3>
        <label className="dropdown-field">
          <span>Timeout (ms)</span>
          <input
            type="number"
            min={1}
            step={1}
            aria-label="Zookeeper timeout (ms)"
            value={zookeeperTimeoutMs}
            onChange={(e) => setZookeeperTimeoutMs(Number(e.target.value))}
          />
        </label>
      </section>

      <section className="settings-panel__section">
        <h3>Brokers</h3>
        <label className="dropdown-field">
          <span>Read Timeout (ms)</span>
          <input
            type="number"
            min={1}
            step={1}
            aria-label="Broker read timeout (ms)"
            value={brokerReadTimeoutMs}
            onChange={(e) => setBrokerReadTimeoutMs(Number(e.target.value))}
          />
        </label>
      </section>

      <section className="settings-panel__section">
        <h3>Messages</h3>
        <label className="dropdown-field">
          <span>Max Message Size (bytes)</span>
          <input
            type="number"
            min={1}
            step={1}
            aria-label="Max message size (bytes)"
            value={maxMessageSizeBytes}
            onChange={(e) => setMaxMessageSizeBytes(Number(e.target.value))}
          />
        </label>
        <label className="dropdown-field">
          <span>Max Total Fetch Size (MB)</span>
          <input
            type="number"
            min={1}
            step={1}
            aria-label="Max total fetch size (MB)"
            value={Math.round(maxTotalFetchBytes / (1024 * 1024))}
            onChange={(e) => setMaxTotalFetchBytes(Number(e.target.value) * 1024 * 1024)}
          />
        </label>
        <p className="settings-panel__hint">
          How much message payload one Fetch delivers to the UI before stopping — the previews shown, not the bytes read
          from the network. Every other limit on the Data tab counts messages, which says
          nothing about size — on a topic of multi-megabyte records, "100 messages" can be several gigabytes.
        </p>
        <PayloadCacheSizeField />
      </section>
    </div>
  );
}

function AppearanceSettingsTab() {
  const appliedFontFamilyId = usePreferencesStore((s) => s.appliedFontFamilyId);
  const previewFontFamilyId = usePreferencesStore((s) => s.previewFontFamilyId);
  const setPreviewFontFamily = usePreferencesStore((s) => s.setPreviewFontFamily);
  const setAppliedFontFamily = usePreferencesStore((s) => s.setAppliedFontFamily);
  const fontSizePx = usePreferencesStore((s) => s.fontSizePx);
  const setFontSizePx = usePreferencesStore((s) => s.setFontSizePx);

  const displayedFontFamilyId = activeFontFamilyId({ appliedFontFamilyId, previewFontFamilyId });

  return (
    <div className="settings-panel__tabpanel" role="tabpanel" aria-label="Appearance">
      <section className="settings-panel__section">
        <h3>Theme</h3>
        <ThemePicker />
      </section>

      <section className="settings-panel__section">
        <h3>Font</h3>
        <Dropdown
          label="Font style"
          ariaLabel="Font style"
          options={FONT_FAMILY_OPTIONS}
          displayedId={displayedFontFamilyId}
          appliedId={appliedFontFamilyId}
          onCommit={setAppliedFontFamily}
          onPreview={setPreviewFontFamily}
        />
        <Dropdown
          label="Font size"
          ariaLabel="Font size"
          options={FONT_SIZE_OPTIONS}
          displayedId={String(fontSizePx)}
          appliedId={String(fontSizePx)}
          onCommit={(id) => setFontSizePx(Number(id))}
        />
      </section>
    </div>
  );
}
