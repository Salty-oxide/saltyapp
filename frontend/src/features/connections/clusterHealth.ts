import { ConfigEntry, PartitionSummary } from "../../lib/tauri";
import { computePartitionSkew } from "./partitionSkew";

/**
 * Pure analysis behind the cluster Metrics tab: what a topic's partitions and
 * config say about its replication, how its leaders are spread over brokers,
 * and how its messages are spread over partitions. No React, no I/O.
 */

export interface TopicAnalysis {
  name: string;
  partitionCount: number;
  /** The smallest replica count of any partition. */
  replicationFactor: number;
  /** `min.insync.replicas`, defaulting to 1 as the broker does. */
  minIsr: number;
  /** Some partition has fewer in-sync replicas than replicas. */
  underReplicated: boolean;
  /** Some partition has fewer in-sync replicas than `minIsr` — producers using acks=all are being refused. */
  belowMinIsr: boolean;
  /** The most leaders on one broker over the even share (1 = even). */
  leaderRatio: number;
  /** Busiest partition over the mean message count (1 = even). */
  skewRatio: number;
}

export type TopicRisk = "low" | "elevated" | "high" | "critical";

export function analyzeTopic(
  name: string,
  partitions: PartitionSummary[],
  config: ConfigEntry[],
  brokerCount: number,
): TopicAnalysis {
  const configured = Number(config.find((entry) => entry.name === "min.insync.replicas")?.value);
  const minIsr = Number.isFinite(configured) && configured > 0 ? configured : 1;
  const replicationFactor = partitions.length === 0 ? 0 : Math.min(...partitions.map((p) => p.replicas.length));

  const leaders = new Map<number, number>();
  for (const partition of partitions) leaders.set(partition.leader, (leaders.get(partition.leader) ?? 0) + 1);
  const busiest = leaders.size === 0 ? 0 : Math.max(...leaders.values());
  // The even share is rounded up: fewer partitions than brokers cannot be
  // spread any thinner than one each.
  const ideal = Math.max(1, Math.ceil(partitions.length / Math.max(1, brokerCount)));

  return {
    name,
    partitionCount: partitions.length,
    replicationFactor,
    minIsr,
    underReplicated: partitions.some((p) => p.isr.length < p.replicas.length),
    belowMinIsr: partitions.some((p) => p.isr.length < minIsr),
    leaderRatio: partitions.length === 0 ? 1 : busiest / ideal,
    skewRatio: computePartitionSkew(partitions).skewRatio,
  };
}

/**
 * Data-loss risk of one topic.
 *
 * - **critical** — in-sync replicas below `min.insync.replicas`.
 * - **high** — under-replicated, but still meeting the minimum.
 * - **elevated** — nothing wrong now, but no redundancy to spare: a single
 *   copy, or a minimum equal to the replication factor so one broker down
 *   stops writes.
 * - **low** — otherwise.
 */
export function topicRisk(topic: TopicAnalysis): TopicRisk {
  if (topic.belowMinIsr) return "critical";
  if (topic.underReplicated) return "high";
  if (topic.replicationFactor <= 1 || topic.minIsr >= topic.replicationFactor) return "elevated";
  return "low";
}

const RISK_ORDER: Record<TopicRisk, number> = { low: 0, elevated: 1, high: 2, critical: 3 };

export function riskRank(risk: TopicRisk): number {
  return RISK_ORDER[risk];
}

export interface ReplicationGroup {
  /** "RF 3 / min ISR 2". */
  label: string;
  replicationFactor: number;
  minIsr: number;
  topics: number;
  underReplicated: number;
  risk: TopicRisk;
}

/** Topics grouped by (replication factor, min ISR), the data-loss card's x axis. */
export function replicationRiskGroups(topics: TopicAnalysis[]): ReplicationGroup[] {
  const groups = new Map<string, ReplicationGroup>();
  for (const topic of topics) {
    const key = `${topic.replicationFactor}/${topic.minIsr}`;
    let group = groups.get(key);
    if (!group) {
      group = {
        label: `RF ${topic.replicationFactor} / min ISR ${topic.minIsr}`,
        replicationFactor: topic.replicationFactor,
        minIsr: topic.minIsr,
        topics: 0,
        underReplicated: 0,
        risk: "low",
      };
      groups.set(key, group);
    }
    group.topics += 1;
    if (topic.underReplicated) group.underReplicated += 1;
    const risk = topicRisk(topic);
    if (riskRank(risk) > riskRank(group.risk)) group.risk = risk;
  }
  return [...groups.values()].sort(
    (a, b) => a.replicationFactor - b.replicationFactor || a.minIsr - b.minIsr,
  );
}

export interface RatioBucket {
  label: string;
  /** Exclusive upper bound; `Infinity` for the last. */
  below: number;
  tone: "good" | "warn" | "bad";
}

/** Message skew ratio — busiest partition over the mean. */
export const IMBALANCE_BUCKETS: RatioBucket[] = [
  { label: "< 1.25×", below: 1.25, tone: "good" },
  { label: "1.25–1.5×", below: 1.5, tone: "good" },
  { label: "1.5–2×", below: 2, tone: "warn" },
  { label: "2–3×", below: 3, tone: "bad" },
  { label: "≥ 3×", below: Infinity, tone: "bad" },
];

/** Leader spread over brokers — most leaders on one broker over the even share. */
export const EFFICIENCY_BUCKETS: RatioBucket[] = [
  { label: "Even (1×)", below: 1.0001, tone: "good" },
  { label: "1–1.5×", below: 1.5, tone: "warn" },
  { label: "1.5–2×", below: 2, tone: "bad" },
  { label: "≥ 2×", below: Infinity, tone: "bad" },
];

export function bucketCounts(ratios: number[], buckets: RatioBucket[]): { label: string; topics: number; tone: RatioBucket["tone"] }[] {
  const counts = buckets.map((bucket) => ({ label: bucket.label, topics: 0, tone: bucket.tone }));
  for (const ratio of ratios) {
    const index = buckets.findIndex((bucket) => ratio < bucket.below);
    counts[index === -1 ? counts.length - 1 : index].topics += 1;
  }
  return counts;
}

export interface HealthInputs {
  topics: number;
  critical: number;
  underReplicated: number;
  uneven: number;
  skewed: number;
}

/**
 * 0–100, starting from 100 and losing points in proportion to the share of
 * topics in trouble: data-loss risk weighs most (up to 50 for topics below
 * min ISR, 25 for under-replicated ones), then leader spread (15) and message
 * skew (10). A cluster with no topics has nothing wrong with it.
 */
export function healthScore(input: HealthInputs): number {
  if (input.topics <= 0) return 100;
  const share = (n: number) => Math.min(1, n / input.topics);
  const penalty =
    50 * share(input.critical) + 25 * share(input.underReplicated) + 15 * share(input.uneven) + 10 * share(input.skewed);
  return Math.max(0, Math.round(100 - penalty));
}

export const UNEVEN_LEADERS_AT = 1.5;
export const SKEWED_MESSAGES_AT = 1.5;

export interface ClusterSummary {
  totalTopics: number;
  totalPartitions: number;
  /** Topics by data-loss risk, for the distribution bar. */
  distribution: Record<TopicRisk, number>;
  score: number;
}

export function summarize(topics: TopicAnalysis[]): ClusterSummary {
  const distribution: Record<TopicRisk, number> = { low: 0, elevated: 0, high: 0, critical: 0 };
  for (const topic of topics) distribution[topicRisk(topic)] += 1;
  return {
    totalTopics: topics.length,
    totalPartitions: topics.reduce((sum, t) => sum + t.partitionCount, 0),
    distribution,
    score: healthScore({
      topics: topics.length,
      critical: distribution.critical,
      // Under-replicated but not already counted as critical.
      underReplicated: distribution.high,
      uneven: topics.filter((t) => t.leaderRatio >= UNEVEN_LEADERS_AT).length,
      skewed: topics.filter((t) => t.skewRatio >= SKEWED_MESSAGES_AT).length,
    }),
  };
}
