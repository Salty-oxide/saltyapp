/**
 * Pure rate maths for the topic Rates tab. Kafka only reports counters (a
 * topic's high watermarks, a group's committed offsets), never a rate or a
 * history, so every figure here is the difference between two samples the app
 * took itself.
 */

export const MINUTE_MS = 60_000;

/** Samples kept this long; the longest range offered is 30 days. */
export const RETENTION_MS = 30 * 24 * 60 * MINUTE_MS;

export type RateRangeId = "1h" | "1d" | "7d" | "30d";

export interface RateRange {
  id: RateRangeId;
  label: string;
  windowMs: number;
  /** One chart point per bucket — coarser for longer ranges, so a chart stays a few hundred points. */
  bucketMs: number;
}

export const RATE_RANGES: RateRange[] = [
  { id: "1h", label: "1h", windowMs: 60 * MINUTE_MS, bucketMs: MINUTE_MS },
  { id: "1d", label: "1d", windowMs: 24 * 60 * MINUTE_MS, bucketMs: 15 * MINUTE_MS },
  { id: "7d", label: "7d", windowMs: 7 * 24 * 60 * MINUTE_MS, bucketMs: 60 * MINUTE_MS },
  { id: "30d", label: "30d", windowMs: 30 * 24 * 60 * MINUTE_MS, bucketMs: 6 * 60 * MINUTE_MS },
];

/** One reading of the counters. */
export interface RateSample {
  at: number;
  /** Sum of the topic's partitions' high watermarks — messages ever produced, as far as this topic's log goes. */
  produced: number;
  /** Per consumer group, the sum of its committed offsets on this topic; `null` when they could not be read. */
  consumedByGroup: Record<string, number> | null;
}

/** Messages produced and consumed within one minute. `consumed` is `null` when nothing could be read for it. */
export interface RateMinute {
  produced: number;
  consumed: number | null;
}

export interface TopicRateHistory {
  last: RateSample | null;
  /** Keyed by minutes since the epoch. */
  minutes: Record<number, RateMinute>;
}

export const EMPTY_RATE_HISTORY: TopicRateHistory = { last: null, minutes: {} };

/** How far a counter moved; a counter that went backwards (reset, recreated topic, retention) contributes nothing rather than a negative rate. */
export function counterDelta(previous: number, next: number): number {
  return next >= previous ? next - previous : 0;
}

/**
 * Folds one sample into the history. The first sample is only a baseline. A
 * consumer group counts only once it appears in two consecutive samples — one
 * that joins carries a large committed offset that is not consumption.
 */
export function recordSample(history: TopicRateHistory, sample: RateSample): TopicRateHistory {
  const previous = history.last;
  if (previous === null) return { last: sample, minutes: history.minutes };
  if (sample.at <= previous.at) return history;

  const produced = counterDelta(previous.produced, sample.produced);
  let consumed: number | null = null;
  if (previous.consumedByGroup !== null && sample.consumedByGroup !== null) {
    consumed = 0;
    for (const [group, offset] of Object.entries(sample.consumedByGroup)) {
      const before = previous.consumedByGroup[group];
      if (before !== undefined) consumed += counterDelta(before, offset);
    }
  }

  const minute = Math.floor(sample.at / MINUTE_MS);
  const existing = history.minutes[minute];
  const merged: RateMinute = {
    produced: (existing?.produced ?? 0) + produced,
    consumed: consumed === null ? (existing?.consumed ?? null) : (existing?.consumed ?? 0) + consumed,
  };
  return { last: sample, minutes: { ...history.minutes, [minute]: merged } };
}

/** Drops minutes before `cutoffMs`. Returns the same object when there is nothing to drop. */
export function pruneOlderThan(history: TopicRateHistory, cutoffMs: number): TopicRateHistory {
  const cutoffMinute = Math.floor(cutoffMs / MINUTE_MS);
  const entries = Object.entries(history.minutes);
  const kept = entries.filter(([minute]) => Number(minute) >= cutoffMinute);
  if (kept.length === entries.length) return history;
  return { last: history.last, minutes: Object.fromEntries(kept) };
}

export interface RatePoint {
  /** Start of the bucket, in epoch ms. */
  at: number;
  /** Messages produced in the bucket; `null` when nothing was sampled in it, so the chart shows a gap rather than a false zero. */
  produced: number | null;
  consumed: number | null;
}

/** The chart series for a range: one point per bucket from the window's start up to `now`. */
export function seriesFor(history: TopicRateHistory, range: RateRange, now: number): RatePoint[] {
  const start = Math.floor((now - range.windowMs) / range.bucketMs) * range.bucketMs;
  const count = Math.floor((now - start) / range.bucketMs) + 1;
  const points: RatePoint[] = Array.from({ length: count }, (_, i) => ({
    at: start + i * range.bucketMs,
    produced: null,
    consumed: null,
  }));

  for (const [minuteKey, minute] of Object.entries(history.minutes)) {
    const at = Number(minuteKey) * MINUTE_MS;
    if (at < start || at > now) continue;
    const point = points[Math.floor((at - start) / range.bucketMs)];
    if (!point) continue;
    point.produced = (point.produced ?? 0) + minute.produced;
    if (minute.consumed !== null) point.consumed = (point.consumed ?? 0) + minute.consumed;
  }
  return points;
}
