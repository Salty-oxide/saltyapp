import { focusManager } from "@tanstack/react-query";
import { api } from "../../lib/tauri";
import { RateSample } from "./topicRates";
import { topicRateKey, useTopicRateStore } from "./useTopicRateStore";

/** How often a watched topic is sampled. */
export const SAMPLE_INTERVAL_MS = 30_000;

/** How often the groups consuming a topic are looked for again; between those, only the ones already found are asked. */
export const DISCOVERY_INTERVAL_MS = 10 * 60_000;

/** Consecutive failed samples after which a topic's sampler stops, so a cluster that is gone is not polled forever. */
const MAX_CONSECUTIVE_FAILURES = 3;

/**
 * Topics sampled at once. Each costs a partitions call and a lag call per
 * consuming group every [`SAMPLE_INTERVAL_MS`]; visiting fifty topics must not
 * mean fifty of them, so opening another past this drops the one looked at
 * longest ago (its history stays, it just stops growing).
 */
export const MAX_ACTIVE_SAMPLERS = 5;

/** A sampler for a topic nobody has had open for this long stops. */
export const UNVIEWED_STOP_AFTER_MS = 30 * 60_000;

/** Groups asked at once while discovering — a cluster can have hundreds, and each answer is its own broker round trip. */
const DISCOVERY_CONCURRENCY = 4;

export interface GroupDiscovery {
  /** Groups found consuming the topic; `null` before the first discovery. */
  knownGroups: string[] | null;
  discoveredAt?: number;
  /** When listing groups was last refused. Asked again only after the discovery interval, not on every sample. */
  listingFailedAt?: number;
}

async function inBatches<T>(items: T[], size: number, run: (item: T) => Promise<void>): Promise<void> {
  for (let i = 0; i < items.length; i += size) {
    await Promise.all(items.slice(i, i + size).map(run));
  }
}

/**
 * Reads the counters one sample is made of: the topic's high watermarks, and
 * the committed offsets of each group consuming it.
 *
 * Groups in the `Empty` state have no consumer running, so their committed
 * offsets cannot move and they are never asked. A group whose lag cannot be
 * read is left out of the sample rather than failing it. If groups cannot be
 * listed at all, consumption is reported as unknown but production still is —
 * a principal is routinely allowed to read a topic and not to list groups.
 *
 * `discovery` is updated in place with what was learned.
 */
export async function takeSample(
  connectionId: string,
  topic: string,
  discovery: GroupDiscovery,
  now: number = Date.now(),
): Promise<RateSample> {
  const partitions = await api.listPartitions(connectionId, topic);
  const produced = partitions.reduce((sum, partition) => sum + partition.highOffset, 0);

  if (discovery.listingFailedAt !== undefined && now - discovery.listingFailedAt < DISCOVERY_INTERVAL_MS) {
    return { at: now, produced, consumedByGroup: null };
  }

  const needsDiscovery =
    discovery.knownGroups === null || now - (discovery.discoveredAt ?? 0) >= DISCOVERY_INTERVAL_MS;

  let candidates: string[];
  if (needsDiscovery) {
    try {
      const groups = await api.listConsumerGroups(connectionId);
      candidates = groups.filter((group) => group.state !== "Empty").map((group) => group.groupId);
      discovery.listingFailedAt = undefined;
    } catch {
      discovery.listingFailedAt = now;
      return { at: now, produced, consumedByGroup: null };
    }
  } else {
    candidates = discovery.knownGroups ?? [];
  }

  const consumedByGroup: Record<string, number> = {};
  await inBatches(candidates, DISCOVERY_CONCURRENCY, async (groupId) => {
    try {
      const lag = await api.fetchConsumerGroupLag(connectionId, groupId);
      let total = 0;
      let seen = false;
      for (const row of lag.partitions) {
        if (row.topic === topic && row.currentOffset !== null) {
          total += row.currentOffset;
          seen = true;
        }
      }
      if (seen) consumedByGroup[groupId] = total;
    } catch {
      // This group is left out of the sample; it does not fail the others.
    }
  });

  if (needsDiscovery) {
    discovery.knownGroups = Object.keys(consumedByGroup);
    discovery.discoveredAt = now;
  }
  return { at: now, produced, consumedByGroup };
}

interface Sampler {
  connectionId: string;
  timer: ReturnType<typeof setInterval>;
  /** When the topic's Rates tab was last open (or asked for), for eviction and for stopping unwatched samplers. */
  lastViewedAt: number;
  /** Takes a sample now, whatever the interval says. */
  sampleNow: () => Promise<void>;
}

const samplers = new Map<string, Sampler>();

/**
 * Starts sampling a topic, once. It runs only while the tab that asked for it
 * is open (`stopSampling`), and ends earlier if the cluster disconnects
 * (`stopSamplingForConnection`) or sampling keeps failing. History recorded so
 * far stays, so reopening the tab shows it with a gap.
 *
 * Ticks are skipped while the app is not in front. That is the same rule the
 * rest of the app's polling follows, and it matters here: these requests count
 * as use of the pooled clients, which would otherwise never go idle.
 */
export function ensureSampling(connectionId: string, topic: string): void {
  const key = topicRateKey(connectionId, topic);
  const existing = samplers.get(key);
  if (existing) {
    existing.lastViewedAt = Date.now();
    return;
  }
  // Room for one more: drop the sampler whose topic was looked at longest ago.
  while (samplers.size >= MAX_ACTIVE_SAMPLERS) {
    let oldest: string | null = null;
    for (const [candidate, sampler] of samplers) {
      if (oldest === null || sampler.lastViewedAt < (samplers.get(oldest)?.lastViewedAt ?? Infinity)) {
        oldest = candidate;
      }
    }
    if (oldest === null) break;
    stop(oldest);
  }

  const discovery: GroupDiscovery = { knownGroups: null };
  let failures = 0;
  let inFlight = false;

  /** `force` is the Refresh button: the person is looking, so focus is not checked. */
  async function tick(force = false) {
    const sampler = samplers.get(key);
    // Nobody has had this topic's Rates tab open for a long while: its
    // history stays, but it stops costing the cluster a call every 30 s.
    if (sampler && Date.now() - sampler.lastViewedAt > UNVIEWED_STOP_AFTER_MS) {
      stop(key);
      return;
    }
    if (inFlight || (!force && !focusManager.isFocused())) return;
    inFlight = true;
    try {
      const sample = await takeSample(connectionId, topic, discovery);
      failures = 0;
      useTopicRateStore.getState().record(connectionId, topic, sample);
    } catch {
      failures += 1;
      if (failures >= MAX_CONSECUTIVE_FAILURES) stop(key);
    } finally {
      inFlight = false;
    }
  }

  samplers.set(key, {
    connectionId,
    timer: setInterval(() => void tick(), SAMPLE_INTERVAL_MS),
    lastViewedAt: Date.now(),
    sampleNow: () => tick(true),
  });
  void tick();
}

/**
 * Takes a sample of a topic now (the Refresh button) and resolves once it is
 * recorded. Starts sampling the topic if it was not already. A sample already
 * in flight is not duplicated.
 */
export async function sampleNow(connectionId: string, topic: string): Promise<void> {
  ensureSampling(connectionId, topic);
  const sampler = samplers.get(topicRateKey(connectionId, topic));
  if (sampler) await sampler.sampleNow();
}

/**
 * Says the topic's Rates tab is still open, so its sampler is not stopped for
 * being unwatched. Does nothing for a topic that is not being sampled.
 */
export function touchSampling(connectionId: string, topic: string): void {
  const sampler = samplers.get(topicRateKey(connectionId, topic));
  if (sampler) sampler.lastViewedAt = Date.now();
}

/**
 * Stops sampling a topic — called when its Rates tab closes, so an unwatched
 * topic costs the cluster nothing. Its recorded history stays in the store.
 */
export function stopSampling(connectionId: string, topic: string): void {
  stop(topicRateKey(connectionId, topic));
}

/** How many topics are being sampled. */
export function activeSamplerCount(): number {
  return samplers.size;
}

function stop(key: string) {
  const sampler = samplers.get(key);
  if (!sampler) return;
  clearInterval(sampler.timer);
  samplers.delete(key);
}

/** Stops every sampler for a cluster — called when its session ends. */
export function stopSamplingForConnection(connectionId: string): void {
  for (const [key, sampler] of samplers) {
    if (sampler.connectionId === connectionId) stop(key);
  }
}
