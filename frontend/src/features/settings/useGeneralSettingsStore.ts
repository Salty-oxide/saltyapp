import { create } from "zustand";

export const DEFAULT_ZOOKEEPER_TIMEOUT_MS = 10_000;
export const DEFAULT_BROKER_READ_TIMEOUT_MS = 10_000;
export const DEFAULT_MAX_MESSAGE_SIZE_BYTES = 1_048_576;
/**
 * Ceiling on how many payload bytes one Fetch reads from the broker before
 * stopping and saying so. Mirrors the backend's own
 * `DEFAULT_MAX_TOTAL_PAYLOAD_BYTES`.
 *
 * Every other limit on the Data tab counts *messages*, which says nothing
 * about what a fetch costs: on a topic of multi-megabyte records the
 * prefilled "100 per partition" across a 12-partition topic is a multi-
 * gigabyte read, and nothing in the form hints at that. Half a gigabyte is
 * far more than a browse needs and keeps the pathological case bounded.
 */
export const DEFAULT_MAX_TOTAL_FETCH_BYTES = 536_870_912;

/**
 * Compressed bytes the backend's payload cache may hold (`salty_cache`).
 * Mirrors `DEFAULT_MAX_BYTES` there. `0` turns the cache off, and the app then
 * behaves as it did before it existed: opening a row with a truncated preview
 * is a broker round trip.
 */
export const DEFAULT_PAYLOAD_CACHE_BYTES = 1_073_741_824;
/** Below this a cache holds a handful of payloads and is not worth the bookkeeping; a non-zero setting is raised to it. */
export const MIN_PAYLOAD_CACHE_BYTES = 64 * 1024 * 1024;
/** Above this it competes with the rest of the app for a desktop's memory. */
export const MAX_PAYLOAD_CACHE_BYTES = 4 * 1024 * 1024 * 1024;

/** `0` stays `0` (off); anything else is held between the minimum and maximum. */
export function clampPayloadCacheBytes(bytes: number): number {
  const whole = Math.trunc(bytes);
  if (!Number.isFinite(whole) || whole <= 0) return 0;
  return Math.min(MAX_PAYLOAD_CACHE_BYTES, Math.max(MIN_PAYLOAD_CACHE_BYTES, whole));
}

interface GeneralSettingsState {
  zookeeperTimeoutMs: number;
  brokerReadTimeoutMs: number;
  maxMessageSizeBytes: number;
  maxTotalFetchBytes: number;
  payloadCacheBytes: number;
  setZookeeperTimeoutMs: (ms: number) => void;
  setBrokerReadTimeoutMs: (ms: number) => void;
  setMaxMessageSizeBytes: (bytes: number) => void;
  setMaxTotalFetchBytes: (bytes: number) => void;
  setPayloadCacheBytes: (bytes: number) => void;
}

// The `kafkaoxide.` prefix is the app's previous name, and it stays.
// Renaming these keys cannot help anyone: a browser key that survives the
// upgrade carries the user's settings forward, and one that doesn't is gone
// either way. Changing them would only guarantee the first case is lost too.
// See the localStorage note in .claude/CLAUDE.md.
const STORAGE_KEY = "kafkaoxide.generalSettings";

interface StoredGeneralSettings {
  zookeeperTimeoutMs: number;
  brokerReadTimeoutMs: number;
  maxMessageSizeBytes: number;
  maxTotalFetchBytes: number;
  payloadCacheBytes: number;
}

const DEFAULTS: StoredGeneralSettings = {
  zookeeperTimeoutMs: DEFAULT_ZOOKEEPER_TIMEOUT_MS,
  brokerReadTimeoutMs: DEFAULT_BROKER_READ_TIMEOUT_MS,
  maxMessageSizeBytes: DEFAULT_MAX_MESSAGE_SIZE_BYTES,
  maxTotalFetchBytes: DEFAULT_MAX_TOTAL_FETCH_BYTES,
  payloadCacheBytes: DEFAULT_PAYLOAD_CACHE_BYTES,
};

export function loadStoredGeneralSettings(): StoredGeneralSettings {
  if (typeof localStorage === "undefined") return DEFAULTS;
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return DEFAULTS;
    const parsed = JSON.parse(raw) as Partial<StoredGeneralSettings>;
    return {
      zookeeperTimeoutMs: parsed.zookeeperTimeoutMs ?? DEFAULT_ZOOKEEPER_TIMEOUT_MS,
      brokerReadTimeoutMs: parsed.brokerReadTimeoutMs ?? DEFAULT_BROKER_READ_TIMEOUT_MS,
      maxMessageSizeBytes: parsed.maxMessageSizeBytes ?? DEFAULT_MAX_MESSAGE_SIZE_BYTES,
      maxTotalFetchBytes: parsed.maxTotalFetchBytes ?? DEFAULT_MAX_TOTAL_FETCH_BYTES,
      // Passed through the clamp: a stored value is only a number someone
      // could have edited, and `0` is a legitimate stored setting.
      payloadCacheBytes:
        parsed.payloadCacheBytes === undefined
          ? DEFAULT_PAYLOAD_CACHE_BYTES
          : clampPayloadCacheBytes(parsed.payloadCacheBytes),
    };
  } catch {
    return DEFAULTS;
  }
}

function persist(patch: Partial<StoredGeneralSettings>) {
  if (typeof localStorage === "undefined") return;
  const current = loadStoredGeneralSettings();
  localStorage.setItem(STORAGE_KEY, JSON.stringify({ ...current, ...patch }));
}

/** Positive-integer guard — a timeout or max size of 0/negative would either hang every request or reject it outright. */
function clampPositiveInt(value: number): number {
  const truncated = Math.trunc(value);
  return truncated > 0 ? truncated : 1;
}

const initial = loadStoredGeneralSettings();

export const useGeneralSettingsStore = create<GeneralSettingsState>((set) => ({
  zookeeperTimeoutMs: initial.zookeeperTimeoutMs,
  brokerReadTimeoutMs: initial.brokerReadTimeoutMs,
  maxMessageSizeBytes: initial.maxMessageSizeBytes,
  maxTotalFetchBytes: initial.maxTotalFetchBytes,
  payloadCacheBytes: initial.payloadCacheBytes,
  setZookeeperTimeoutMs: (ms) => {
    const clamped = clampPositiveInt(ms);
    persist({ zookeeperTimeoutMs: clamped });
    set({ zookeeperTimeoutMs: clamped });
  },
  setBrokerReadTimeoutMs: (ms) => {
    const clamped = clampPositiveInt(ms);
    persist({ brokerReadTimeoutMs: clamped });
    set({ brokerReadTimeoutMs: clamped });
  },
  setMaxMessageSizeBytes: (bytes) => {
    const clamped = clampPositiveInt(bytes);
    persist({ maxMessageSizeBytes: clamped });
    set({ maxMessageSizeBytes: clamped });
  },
  setMaxTotalFetchBytes: (bytes) => {
    const clamped = clampPositiveInt(bytes);
    persist({ maxTotalFetchBytes: clamped });
    set({ maxTotalFetchBytes: clamped });
  },
  setPayloadCacheBytes: (bytes) => {
    const clamped = clampPayloadCacheBytes(bytes);
    persist({ payloadCacheBytes: clamped });
    set({ payloadCacheBytes: clamped });
  },
}));
