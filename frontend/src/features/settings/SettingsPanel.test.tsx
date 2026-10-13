import { beforeEach, describe, expect, it } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { SettingsPanel } from "./SettingsPanel";
import { usePreferencesStore } from "./usePreferencesStore";
import { useThemeStore } from "../theme/useThemeStore";
import { DEFAULT_FONT_FAMILY_ID, DEFAULT_FONT_SIZE_PX } from "./fonts";
import { DEFAULT_THEME_ID } from "../theme/themes";
import {
  DEFAULT_BROKER_READ_TIMEOUT_MS,
  DEFAULT_MAX_MESSAGE_SIZE_BYTES,
  DEFAULT_PAYLOAD_CACHE_BYTES,
  DEFAULT_ZOOKEEPER_TIMEOUT_MS,
  useGeneralSettingsStore,
} from "./useGeneralSettingsStore";

beforeEach(() => {
  localStorage.clear();
  usePreferencesStore.setState({
    appliedFontFamilyId: DEFAULT_FONT_FAMILY_ID,
    previewFontFamilyId: null,
    fontSizePx: DEFAULT_FONT_SIZE_PX,
  });
  useThemeStore.setState({ appliedThemeId: DEFAULT_THEME_ID, lastByKind: {} });
  useGeneralSettingsStore.setState({
    zookeeperTimeoutMs: DEFAULT_ZOOKEEPER_TIMEOUT_MS,
    brokerReadTimeoutMs: DEFAULT_BROKER_READ_TIMEOUT_MS,
    maxMessageSizeBytes: DEFAULT_MAX_MESSAGE_SIZE_BYTES,
    payloadCacheBytes: DEFAULT_PAYLOAD_CACHE_BYTES,
  });
  document.documentElement.style.removeProperty("--font-family-base");
});

async function openAppearanceTab() {
  const user = userEvent.setup();
  render(<SettingsPanel />);
  await user.click(screen.getByRole("tab", { name: "Appearance" }));
  return user;
}

describe("SettingsPanel tabs", () => {
  it("shows the General tab by default", () => {
    render(<SettingsPanel />);
    expect(screen.getByRole("tab", { name: "General", selected: true })).toBeInTheDocument();
    expect(screen.getByRole("tabpanel", { name: "General" })).toBeInTheDocument();
  });

  it("switches to Appearance and shows the theme/font controls there", async () => {
    const user = userEvent.setup();
    render(<SettingsPanel />);

    await user.click(screen.getByRole("tab", { name: "Appearance" }));

    expect(screen.getByRole("tab", { name: "Appearance", selected: true })).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: "Zed Dark" })).toBeInTheDocument();
  });

  it("does not show General's timeout fields while Appearance is active", async () => {
    await openAppearanceTab();
    expect(screen.queryByLabelText("Zookeeper timeout (ms)")).not.toBeInTheDocument();
  });
});

describe("SettingsPanel General tab", () => {
  it("shows Zookeeper, Brokers, and Messages sections with their defaults", () => {
    render(<SettingsPanel />);

    expect(screen.getByRole("heading", { name: "Zookeeper" })).toBeInTheDocument();
    expect(screen.getByLabelText("Zookeeper timeout (ms)")).toHaveValue(DEFAULT_ZOOKEEPER_TIMEOUT_MS);

    expect(screen.getByRole("heading", { name: "Brokers" })).toBeInTheDocument();
    expect(screen.getByLabelText("Broker read timeout (ms)")).toHaveValue(DEFAULT_BROKER_READ_TIMEOUT_MS);

    expect(screen.getByRole("heading", { name: "Messages" })).toBeInTheDocument();
    expect(screen.getByLabelText("Max message size (bytes)")).toHaveValue(DEFAULT_MAX_MESSAGE_SIZE_BYTES);
  });

  it("updates and persists the Zookeeper timeout", () => {
    render(<SettingsPanel />);

    const input = screen.getByLabelText("Zookeeper timeout (ms)");
    fireEvent.change(input, { target: { value: "5000" } });

    expect(useGeneralSettingsStore.getState().zookeeperTimeoutMs).toBe(5000);
    const stored = JSON.parse(localStorage.getItem("kafkaoxide.generalSettings") ?? "{}");
    expect(stored.zookeeperTimeoutMs).toBe(5000);
  });

  it("updates and persists the broker read timeout", () => {
    render(<SettingsPanel />);

    const input = screen.getByLabelText("Broker read timeout (ms)");
    fireEvent.change(input, { target: { value: "20000" } });

    expect(useGeneralSettingsStore.getState().brokerReadTimeoutMs).toBe(20000);
  });

  it("updates and persists the max message size", () => {
    render(<SettingsPanel />);

    const input = screen.getByLabelText("Max message size (bytes)");
    fireEvent.change(input, { target: { value: "2097152" } });

    expect(useGeneralSettingsStore.getState().maxMessageSizeBytes).toBe(2097152);
  });
});

describe("SettingsPanel payload cache size", () => {
  const MB = 1024 * 1024;
  const field = () => screen.getByLabelText("Payload cache size (MB)");

  it("shows the default in megabytes", () => {
    render(<SettingsPanel />);
    expect(field()).toHaveValue(1024);
  });

  it("applies and persists the size when the field is left", () => {
    render(<SettingsPanel />);

    fireEvent.change(field(), { target: { value: "512" } });
    // Not applied while typing: applying empties the backend cache.
    expect(useGeneralSettingsStore.getState().payloadCacheBytes).toBe(DEFAULT_PAYLOAD_CACHE_BYTES);
    fireEvent.blur(field());

    expect(useGeneralSettingsStore.getState().payloadCacheBytes).toBe(512 * MB);
    expect(JSON.parse(localStorage.getItem("kafkaoxide.generalSettings") ?? "{}").payloadCacheBytes).toBe(512 * MB);
  });

  it("applies the size on Enter", () => {
    render(<SettingsPanel />);

    fireEvent.change(field(), { target: { value: "256" } });
    fireEvent.keyDown(field(), { key: "Enter" });

    expect(useGeneralSettingsStore.getState().payloadCacheBytes).toBe(256 * MB);
  });

  it("raises a value below the minimum to it, and shows that", () => {
    render(<SettingsPanel />);

    fireEvent.change(field(), { target: { value: "10" } });
    fireEvent.blur(field());

    expect(useGeneralSettingsStore.getState().payloadCacheBytes).toBe(64 * MB);
    expect(field()).toHaveValue(64);
  });

  it("holds a value above the maximum to it", () => {
    render(<SettingsPanel />);

    fireEvent.change(field(), { target: { value: "99999" } });
    fireEvent.blur(field());

    expect(useGeneralSettingsStore.getState().payloadCacheBytes).toBe(4096 * MB);
    expect(field()).toHaveValue(4096);
  });

  it("accepts 0 as off and says so", () => {
    render(<SettingsPanel />);

    fireEvent.change(field(), { target: { value: "0" } });
    fireEvent.blur(field());

    expect(useGeneralSettingsStore.getState().payloadCacheBytes).toBe(0);
    expect(screen.getByText(/The cache is off\./)).toBeInTheDocument();
  });

  it("puts the stored value back when the box is emptied", () => {
    render(<SettingsPanel />);

    fireEvent.change(field(), { target: { value: "" } });
    fireEvent.blur(field());

    expect(useGeneralSettingsStore.getState().payloadCacheBytes).toBe(DEFAULT_PAYLOAD_CACHE_BYTES);
    expect(field()).toHaveValue(1024);
  });
});

describe("SettingsPanel Appearance tab", () => {
  it("renders the theme picker and the font dropdowns", async () => {
    await openAppearanceTab();
    expect(screen.getByRole("radiogroup", { name: "Light or dark" })).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: "Zed Dark" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /System UI/ })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: `${DEFAULT_FONT_SIZE_PX}px ▾` })).toBeInTheDocument();
  });

  it("marks the applied font family with a checkmark when the dropdown is open", async () => {
    const user = await openAppearanceTab();

    await user.click(screen.getByRole("button", { name: /System UI/ }));

    expect(screen.getByRole("option", { name: "✓ System UI" })).toBeInTheDocument();
  });

  it("live-previews a font family on hover without committing it", async () => {
    const user = await openAppearanceTab();

    await user.click(screen.getByRole("button", { name: /System UI/ }));
    await user.hover(screen.getByRole("option", { name: "Inter" }));

    expect(document.documentElement.style.getPropertyValue("--font-family-base")).toBe("");
    // The dropdown itself doesn't own CSS application (PreferencesProvider does,
    // tested separately) — what SettingsPanel owns is calling setPreviewFontFamily,
    // asserted directly against the store:
    expect(usePreferencesStore.getState().previewFontFamilyId).toBe("inter");
    expect(usePreferencesStore.getState().appliedFontFamilyId).toBe(DEFAULT_FONT_FAMILY_ID);
  });

  it("commits a font family on click", async () => {
    const user = await openAppearanceTab();

    await user.click(screen.getByRole("button", { name: /System UI/ }));
    await user.click(screen.getByRole("option", { name: "Inter" }));

    expect(usePreferencesStore.getState().appliedFontFamilyId).toBe("inter");
  });

  it("commits a font size change on click, same as the other dropdowns", async () => {
    const user = await openAppearanceTab();

    await user.click(screen.getByRole("button", { name: `${DEFAULT_FONT_SIZE_PX}px ▾` }));
    await user.click(screen.getByRole("option", { name: `${DEFAULT_FONT_SIZE_PX + 1}px` }));

    expect(usePreferencesStore.getState().fontSizePx).toBe(DEFAULT_FONT_SIZE_PX + 1);
  });

  it("marks the currently applied theme as checked", async () => {
    await openAppearanceTab();

    expect(screen.getByRole("radio", { name: "Zed Dark" })).toBeChecked();
    expect(screen.getByRole("radio", { name: "Ayu Dark" })).not.toBeChecked();
  });

  it("applies the selected theme immediately and persists it", async () => {
    const user = await openAppearanceTab();

    // Zed Light is on the other side of the Light/Dark switch.
    await user.click(screen.getByRole("radio", { name: "Light" }));
    await user.click(screen.getByRole("radio", { name: "Zed Light" }));

    expect(useThemeStore.getState().appliedThemeId).toBe("zed-light");
    expect(localStorage.getItem("kafkaoxide.theme")).toBe("zed-light");
  });

  it("closes the font menu when clicking outside of it", async () => {
    const user = await openAppearanceTab();

    await user.click(screen.getByRole("button", { name: /System UI/ }));
    expect(screen.getByRole("listbox", { name: "Font style" })).toBeInTheDocument();

    await user.click(screen.getByText("Settings"));

    expect(screen.queryByRole("listbox", { name: "Font style" })).not.toBeInTheDocument();
  });

  it("closes the font menu on Escape", async () => {
    const user = await openAppearanceTab();

    await user.click(screen.getByRole("button", { name: /System UI/ }));
    expect(screen.getByRole("listbox", { name: "Font style" })).toBeInTheDocument();

    await user.keyboard("{Escape}");

    expect(screen.queryByRole("listbox", { name: "Font style" })).not.toBeInTheDocument();
  });

  it("selects a font family via keyboard navigation (ArrowDown + Enter)", async () => {
    const user = await openAppearanceTab();

    await user.click(screen.getByRole("button", { name: /System UI/ }));
    await user.keyboard("{ArrowDown}{Enter}");

    expect(usePreferencesStore.getState().appliedFontFamilyId).toBe("inter");
  });

  // `role="radio"` promises arrow-key navigation within the group, and moving
  // focus selects — the same contract a native radio group has.
  it("moves through the themes with the arrow keys", async () => {
    const user = await openAppearanceTab();

    screen.getByRole("radio", { name: "Zed Dark" }).focus();
    await user.keyboard("{ArrowDown}");

    // The dark themes in list order: Zed Dark, Ayu Dark, One Dark, ...
    expect(useThemeStore.getState().appliedThemeId).toBe("ayu-dark");
    expect(screen.getByRole("radio", { name: "Ayu Dark" })).toHaveFocus();
  });
});
