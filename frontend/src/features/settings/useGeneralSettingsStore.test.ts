import { beforeEach, describe, expect, it } from "vitest";
import {
  clampPayloadCacheBytes,
  DEFAULT_BROKER_READ_TIMEOUT_MS,
  DEFAULT_MAX_MESSAGE_SIZE_BYTES,
  DEFAULT_MAX_TOTAL_FETCH_BYTES,
  DEFAULT_PAYLOAD_CACHE_BYTES,
  DEFAULT_ZOOKEEPER_TIMEOUT_MS,
  loadStoredGeneralSettings,
  MAX_PAYLOAD_CACHE_BYTES,
  MIN_PAYLOAD_CACHE_BYTES,
  useGeneralSettingsStore,
} from "./useGeneralSettingsStore";

const STORAGE_KEY = "kafkaoxide.generalSettings";

beforeEach(() => {
  localStorage.clear();
  useGeneralSettingsStore.setState({
    zookeeperTimeoutMs: DEFAULT_ZOOKEEPER_TIMEOUT_MS,
    brokerReadTimeoutMs: DEFAULT_BROKER_READ_TIMEOUT_MS,
    maxMessageSizeBytes: DEFAULT_MAX_MESSAGE_SIZE_BYTES,
  });
});

describe("payload cache size setting", () => {
  const MB = 1024 * 1024;

  beforeEach(() => {
    localStorage.clear();
    useGeneralSettingsStore.setState({ payloadCacheBytes: DEFAULT_PAYLOAD_CACHE_BYTES });
  });

  it("defaults to 1 GiB, matching the backend", () => {
    expect(DEFAULT_PAYLOAD_CACHE_BYTES).toBe(1024 * MB);
    expect(loadStoredGeneralSettings().payloadCacheBytes).toBe(1024 * MB);
  });

  it("commits a size and persists it", () => {
    useGeneralSettingsStore.getState().setPayloadCacheBytes(256 * MB);

    expect(useGeneralSettingsStore.getState().payloadCacheBytes).toBe(256 * MB);
    expect(loadStoredGeneralSettings().payloadCacheBytes).toBe(256 * MB);
  });

  it("keeps 0 as off rather than raising it to the minimum", () => {
    useGeneralSettingsStore.getState().setPayloadCacheBytes(0);
    expect(useGeneralSettingsStore.getState().payloadCacheBytes).toBe(0);
    expect(loadStoredGeneralSettings().payloadCacheBytes).toBe(0);
  });

  it("raises a small size to the minimum and holds a huge one to the maximum", () => {
    expect(clampPayloadCacheBytes(1)).toBe(MIN_PAYLOAD_CACHE_BYTES);
    expect(clampPayloadCacheBytes(10 * 1024 * MB)).toBe(MAX_PAYLOAD_CACHE_BYTES);
  });

  it("treats a negative or non-numeric size as off", () => {
    expect(clampPayloadCacheBytes(-5)).toBe(0);
    expect(clampPayloadCacheBytes(Number.NaN)).toBe(0);
    expect(clampPayloadCacheBytes(Number.POSITIVE_INFINITY)).toBe(0);
  });

  it("clamps a hand-edited stored value on load, and defaults when it is absent", () => {
    localStorage.setItem("kafkaoxide.generalSettings", JSON.stringify({ payloadCacheBytes: 5 }));
    expect(loadStoredGeneralSettings().payloadCacheBytes).toBe(MIN_PAYLOAD_CACHE_BYTES);

    localStorage.setItem("kafkaoxide.generalSettings", JSON.stringify({ zookeeperTimeoutMs: 1 }));
    expect(loadStoredGeneralSettings().payloadCacheBytes).toBe(DEFAULT_PAYLOAD_CACHE_BYTES);
  });
});

describe("useGeneralSettingsStore", () => {
  it("defaults to 10000ms/10000ms/1048576 bytes when nothing is stored", () => {
    expect(useGeneralSettingsStore.getState().zookeeperTimeoutMs).toBe(10_000);
    expect(useGeneralSettingsStore.getState().brokerReadTimeoutMs).toBe(10_000);
    expect(useGeneralSettingsStore.getState().maxMessageSizeBytes).toBe(1_048_576);
  });

  it("commits a Zookeeper timeout and persists it to localStorage", () => {
    useGeneralSettingsStore.getState().setZookeeperTimeoutMs(5000);

    expect(useGeneralSettingsStore.getState().zookeeperTimeoutMs).toBe(5000);
    const stored = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "{}");
    expect(stored.zookeeperTimeoutMs).toBe(5000);
  });

  it("commits a broker read timeout and persists it to localStorage", () => {
    useGeneralSettingsStore.getState().setBrokerReadTimeoutMs(20_000);

    expect(useGeneralSettingsStore.getState().brokerReadTimeoutMs).toBe(20_000);
    const stored = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "{}");
    expect(stored.brokerReadTimeoutMs).toBe(20_000);
  });

  it("commits a max message size and persists it to localStorage", () => {
    useGeneralSettingsStore.getState().setMaxMessageSizeBytes(2_097_152);

    expect(useGeneralSettingsStore.getState().maxMessageSizeBytes).toBe(2_097_152);
    const stored = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "{}");
    expect(stored.maxMessageSizeBytes).toBe(2_097_152);
  });

  it("clamps a zero or negative value up to 1 rather than accepting an unusable timeout", () => {
    useGeneralSettingsStore.getState().setZookeeperTimeoutMs(0);
    expect(useGeneralSettingsStore.getState().zookeeperTimeoutMs).toBe(1);

    useGeneralSettingsStore.getState().setBrokerReadTimeoutMs(-500);
    expect(useGeneralSettingsStore.getState().brokerReadTimeoutMs).toBe(1);
  });

  it("truncates a fractional value to a whole number", () => {
    useGeneralSettingsStore.getState().setMaxMessageSizeBytes(1024.7);
    expect(useGeneralSettingsStore.getState().maxMessageSizeBytes).toBe(1024);
  });

  it("reads persisted settings via the store's storage-read helper, falling back to defaults for missing fields", () => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify({ zookeeperTimeoutMs: 7000 }));

    expect(loadStoredGeneralSettings()).toEqual({
      zookeeperTimeoutMs: 7000,
      brokerReadTimeoutMs: DEFAULT_BROKER_READ_TIMEOUT_MS,
      maxMessageSizeBytes: DEFAULT_MAX_MESSAGE_SIZE_BYTES,
      maxTotalFetchBytes: DEFAULT_MAX_TOTAL_FETCH_BYTES,
      payloadCacheBytes: DEFAULT_PAYLOAD_CACHE_BYTES,
    });
  });

  it("clamps the max total fetch size to a positive integer and persists it", () => {
    useGeneralSettingsStore.getState().setMaxTotalFetchBytes(0);
    expect(useGeneralSettingsStore.getState().maxTotalFetchBytes).toBe(1);

    useGeneralSettingsStore.getState().setMaxTotalFetchBytes(268_435_456);
    expect(useGeneralSettingsStore.getState().maxTotalFetchBytes).toBe(268_435_456);
    expect(loadStoredGeneralSettings().maxTotalFetchBytes).toBe(268_435_456);
  });
});
