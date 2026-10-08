import { PartitionSummary } from "../../lib/tauri";

export type SkewLevel = "balanced" | "moderate" | "high";

export interface PartitionSkew {
  partitions: { partition: number; messages: number }[];
  total: number;
  mean: number;
  min: number;
  max: number;
  /** Busiest partition over the mean; 1 is perfectly even. */
  skewRatio: number;
  level: SkewLevel;
}

/** Max/mean at or above this is worth a look; at or above `HIGH`, a hot partition. */
const MODERATE_SKEW = 1.25;
export const HIGH_SKEW = 1.5;

/** Messages per partition (high minus low watermark) and how uneven they are. */
export function computePartitionSkew(partitions: PartitionSummary[]): PartitionSkew {
  return skewFromCounts(partitions.map((p) => ({ partition: p.id, messages: p.highOffset - p.lowOffset })));
}

/** The same measure over counts the broker already worked out — a From/To window's per-partition totals. */
export function skewFromCounts(input: { partition: number; messages: number }[]): PartitionSkew {
  const rows = input
    .map((c) => ({ partition: c.partition, messages: Math.max(0, c.messages) }))
    .sort((a, b) => a.partition - b.partition);
  const total = rows.reduce((sum, r) => sum + r.messages, 0);
  const mean = rows.length === 0 ? 0 : total / rows.length;
  const counts = rows.map((r) => r.messages);
  const max = counts.length === 0 ? 0 : Math.max(...counts);
  const min = counts.length === 0 ? 0 : Math.min(...counts);
  const skewRatio = mean === 0 ? 1 : max / mean;
  const level: SkewLevel = skewRatio >= HIGH_SKEW ? "high" : skewRatio >= MODERATE_SKEW ? "moderate" : "balanced";
  return { partitions: rows, total, mean, min, max, skewRatio, level };
}
