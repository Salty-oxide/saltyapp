import { useEffect } from "react";
import { api } from "../../lib/tauri";
import { useGeneralSettingsStore } from "./useGeneralSettingsStore";

/**
 * Keeps the backend payload cache's size in step with the setting.
 *
 * The backend starts with its own default and knows nothing of the stored
 * setting, so this states it once at startup and again on every change.
 * Stating an unchanged value is a no-op there (`set_payload_cache_limit`), so
 * a default install does not empty anything by doing so. A failure is
 * ignored: the cache then simply keeps the size it had.
 */
export function usePayloadCacheLimitSync(): void {
  const payloadCacheBytes = useGeneralSettingsStore((s) => s.payloadCacheBytes);

  useEffect(() => {
    void Promise.resolve(api.setPayloadCacheLimit(payloadCacheBytes)).catch(() => {});
  }, [payloadCacheBytes]);
}
