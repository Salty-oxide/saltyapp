import { beforeEach, describe, expect, it, vi } from "vitest";
import { act, renderHook } from "@testing-library/react";
import { invoke } from "@tauri-apps/api/core";
import { usePayloadCacheLimitSync } from "./usePayloadCacheLimitSync";
import { DEFAULT_PAYLOAD_CACHE_BYTES, useGeneralSettingsStore } from "./useGeneralSettingsStore";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));

beforeEach(() => {
  vi.mocked(invoke).mockReset();
  vi.mocked(invoke).mockResolvedValue(false);
  localStorage.clear();
  useGeneralSettingsStore.setState({ payloadCacheBytes: DEFAULT_PAYLOAD_CACHE_BYTES });
});

describe("usePayloadCacheLimitSync", () => {
  it("states the current setting to the backend when the app starts", () => {
    renderHook(() => usePayloadCacheLimitSync());

    expect(invoke).toHaveBeenCalledWith("set_payload_cache_limit", { maxBytes: DEFAULT_PAYLOAD_CACHE_BYTES });
  });

  it("states it again when the setting changes", () => {
    renderHook(() => usePayloadCacheLimitSync());

    act(() => useGeneralSettingsStore.getState().setPayloadCacheBytes(256 * 1024 * 1024));

    expect(invoke).toHaveBeenLastCalledWith("set_payload_cache_limit", { maxBytes: 256 * 1024 * 1024 });
  });

  it("sends 0 when the cache is turned off", () => {
    renderHook(() => usePayloadCacheLimitSync());

    act(() => useGeneralSettingsStore.getState().setPayloadCacheBytes(0));

    expect(invoke).toHaveBeenLastCalledWith("set_payload_cache_limit", { maxBytes: 0 });
  });

  it("does not throw when the backend refuses", async () => {
    vi.mocked(invoke).mockRejectedValue(new Error("backend unavailable"));
    expect(() => renderHook(() => usePayloadCacheLimitSync())).not.toThrow();
    await Promise.resolve();
  });
});
