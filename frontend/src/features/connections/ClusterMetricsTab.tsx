import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { List, type RowComponentProps } from "react-window";
import { Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import {
  bucketCounts,
  ClusterSummary,
  EFFICIENCY_BUCKETS,
  IMBALANCE_BUCKETS,
  replicationRiskGroups,
  riskRank,
  summarize,
  TopicAnalysis,
  topicRisk,
  TopicRisk,
} from "./clusterHealth";
import { cachedClusterHealth, rememberClusterHealth } from "./clusterHealthCache";
import { ClusterHealthLoad, loadClusterHealth } from "./clusterHealthLoader";
import { AXIS_PROPS, ChartCard, GRID_PROPS, StatCard, TOOLTIP_PROPS } from "./metricsChartStyle";

export interface ClusterMetricsTabProps {
  connectionId: string;
}

const TONE_COLOR = {
  good: "var(--color-status-green)",
  warn: "var(--color-status-amber)",
  bad: "var(--color-status-red)",
} as const;

const RISK_COLOR: Record<TopicRisk, string> = {
  low: TONE_COLOR.good,
  elevated: TONE_COLOR.warn,
  high: "var(--color-status-amber)",
  critical: TONE_COLOR.bad,
};

function scoreTone(score: number): { tone: "good" | "warn" | "bad"; label: string } {
  if (score >= 90) return { tone: "good", label: "Healthy" };
  if (score >= 70) return { tone: "warn", label: "Fair" };
  return { tone: "bad", label: "At risk" };
}

type LoadState =
  | { phase: "loading"; loaded: number; total: number | null }
  | { phase: "error"; message: string }
  | { phase: "ready"; load: ClusterHealthLoad; at: number };

/** A small count chart: `data` rows of `{ label, value, color }`. */
function CountBars({
  ariaLabel,
  seriesName,
  data,
  yLabel,
}: {
  ariaLabel: string;
  seriesName: string;
  data: { label: string; value: number; color: string; note?: string }[];
  yLabel: string;
}) {
  return (
    <div className="metrics-chart" role="img" aria-label={ariaLabel}>
      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={data} margin={{ top: 12, right: 12, bottom: 4, left: 0 }}>
          <CartesianGrid {...GRID_PROPS} />
          <XAxis dataKey="label" {...AXIS_PROPS} interval={0} />
          <YAxis
            {...AXIS_PROPS}
            axisLine={false}
            allowDecimals={false}
            label={{ value: yLabel, angle: -90, position: "insideLeft", fill: "var(--color-fg-muted)", fontSize: 11 }}
          />
          <Tooltip {...TOOLTIP_PROPS} />
          <Bar dataKey="value" name={seriesName} radius={[4, 4, 0, 0]} isAnimationActive={false}>
            {data.map((row) => (
              <Cell key={row.label} fill={row.color} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

function DistributionBar({ summary }: { summary: ClusterSummary }) {
  const { distribution, totalTopics } = summary;
  const segments: { key: TopicRisk; label: string; count: number }[] = [
    { key: "low", label: "healthy", count: distribution.low },
    { key: "elevated", label: "no redundancy to spare", count: distribution.elevated },
    { key: "high", label: "under-replicated", count: distribution.high },
    { key: "critical", label: "below min ISR", count: distribution.critical },
  ];
  return (
    <div className="metrics-distribution">
      <div className="metrics-distribution__bar" role="img" aria-label="Topic health distribution">
        {segments
          .filter((segment) => segment.count > 0)
          .map((segment) => (
            <span
              key={segment.key}
              style={{ width: `${(segment.count / Math.max(1, totalTopics)) * 100}%`, background: RISK_COLOR[segment.key] }}
            />
          ))}
      </div>
      <ul className="metrics-distribution__legend">
        {segments.map((segment) => (
          <li key={segment.key}>
            <span className="metrics-distribution__swatch" style={{ background: RISK_COLOR[segment.key] }} aria-hidden="true" />
            {segment.count.toLocaleString()} {segment.label}
          </li>
        ))}
      </ul>
    </div>
  );
}

/** Past this many topics the overview windows its rows, as the sidebar lists do — a cluster can have thousands. */
const OVERVIEW_VIRTUALIZE_AFTER = 50;
/** Must match `.metrics-overview__row`'s height: react-window positions rows by this, it does not measure. */
const OVERVIEW_ROW_PX = 36;
const OVERVIEW_VISIBLE_ROWS = 12;

function OverviewRow({ topic, style }: { topic: TopicAnalysis; style?: React.CSSProperties }) {
  return (
    <div role="row" className="metrics-overview__row" style={style}>
      <div role="cell">{topic.name}</div>
      <div role="cell">{topic.replicationFactor}</div>
      <div role="cell">{topic.minIsr}</div>
      <div role="cell">{`${topic.skewRatio.toFixed(2)}×`}</div>
    </div>
  );
}

function VirtualOverviewRow({ index, style, rows }: RowComponentProps<{ rows: TopicAnalysis[] }>) {
  return <OverviewRow topic={rows[index]} style={style} />;
}

/**
 * Every topic, riskiest first. A div grid with table roles rather than a
 * `<table>`: a table cannot be windowed, and rendering a row per topic is
 * thousands of DOM nodes on a large cluster. Short lists are rendered whole.
 */
function HealthOverviewTable({ topics }: { topics: TopicAnalysis[] }) {
  const rows = useMemo(
    () =>
      [...topics].sort(
        (a, b) => riskRank(topicRisk(b)) - riskRank(topicRisk(a)) || b.skewRatio - a.skewRatio || a.name.localeCompare(b.name),
      ),
    [topics],
  );
  return (
    <div className="metrics-overview" role="table" aria-label="Topic health overview">
      <div role="row" className="metrics-overview__row metrics-overview__row--head">
        <div role="columnheader">Topic</div>
        <div role="columnheader">Replication factor</div>
        <div role="columnheader">Min in-sync replicas</div>
        <div role="columnheader">Partition skew</div>
      </div>
      {rows.length > OVERVIEW_VIRTUALIZE_AFTER ? (
        <List
          className="metrics-overview__body"
          role="rowgroup"
          style={{ height: Math.min(rows.length, OVERVIEW_VISIBLE_ROWS) * OVERVIEW_ROW_PX }}
          rowCount={rows.length}
          rowHeight={OVERVIEW_ROW_PX}
          rowProps={{ rows }}
          rowComponent={VirtualOverviewRow}
        />
      ) : (
        <div className="metrics-overview__body" role="rowgroup">
          {rows.map((topic) => (
            <OverviewRow key={topic.name} topic={topic} />
          ))}
        </div>
      )}
    </div>
  );
}

/**
 * The cluster's Metrics tab: size, a health score, where the risk is, and a
 * per-topic overview. Everything is derived from each topic's partitions and
 * config, which is a few broker round trips per topic — so it is read when the
 * tab is opened (and on Refresh), never in the background, with progress while
 * it runs.
 */
export function ClusterMetricsTab({ connectionId }: ClusterMetricsTabProps) {
  const [state, setState] = useState<LoadState>(() => {
    const recent = cachedClusterHealth(connectionId);
    return recent
      ? { phase: "ready", load: recent.load, at: recent.at }
      : { phase: "loading", loaded: 0, total: null };
  });
  const controllerRef = useRef<AbortController | null>(null);

  /** `force` reads the cluster even when a recent analysis is on hand — the Refresh button. */
  const load = useCallback((force: boolean) => {
    if (!force) {
      const recent = cachedClusterHealth(connectionId);
      if (recent) {
        setState({ phase: "ready", load: recent.load, at: recent.at });
        return;
      }
    }
    controllerRef.current?.abort();
    const controller = new AbortController();
    controllerRef.current = controller;
    setState({ phase: "loading", loaded: 0, total: null });
    loadClusterHealth(connectionId, {
      signal: controller.signal,
      onProgress: (loaded, total) => {
        if (!controller.signal.aborted) setState({ phase: "loading", loaded, total });
      },
    })
      .then((result) => {
        if (controller.signal.aborted) return;
        const at = Date.now();
        rememberClusterHealth(connectionId, result, at);
        setState({ phase: "ready", load: result, at });
      })
      .catch((error: unknown) => {
        if (controller.signal.aborted) return;
        setState({ phase: "error", message: error instanceof Error ? error.message : "Failed to analyse the cluster" });
      });
  }, [connectionId]);

  useEffect(() => {
    load(false);
    return () => controllerRef.current?.abort();
  }, [load]);

  const loading = state.phase === "loading";
  const topics = state.phase === "ready" ? state.load.topics : [];
  const summary = useMemo(() => summarize(topics), [topics]);

  const replication = useMemo(() => replicationRiskGroups(topics), [topics]);
  const efficiency = useMemo(() => bucketCounts(topics.map((t) => t.leaderRatio), EFFICIENCY_BUCKETS), [topics]);
  const imbalance = useMemo(() => bucketCounts(topics.map((t) => t.skewRatio), IMBALANCE_BUCKETS), [topics]);

  const header = (
    <div className="metrics-tab__toolbar">
      <button type="button" onClick={() => load(true)} disabled={loading}>
        Refresh
      </button>
    </div>
  );

  if (state.phase === "error") {
    return (
      <div className="metrics-tab">
        {header}
        <p role="alert" className="connection-modal-error">
          {state.message}
        </p>
      </div>
    );
  }

  if (state.phase === "loading") {
    const { loaded, total } = state;
    return (
      <div className="metrics-tab">
        {header}
        <div className="metrics-progress">
          <progress aria-label="Analysing topics" value={total === null ? undefined : loaded} max={total ?? undefined} />
          <span>{total === null ? "Reading topics…" : `Analysing topics — ${loaded} of ${total}`}</span>
        </div>
      </div>
    );
  }

  const { load: result, at } = state;
  const score = scoreTone(summary.score);

  return (
    <div className="metrics-tab">
      {header}
      <p className="metrics-hint">Analysed at {new Date(at).toLocaleTimeString()}.</p>

      <div className="metrics-stats">
        <div role="group" aria-label="Total topics">
          <StatCard label="Topics" value={summary.totalTopics.toLocaleString()} />
        </div>
        <div role="group" aria-label="Total partitions">
          <StatCard label="Partitions" value={summary.totalPartitions.toLocaleString()} />
        </div>
        <div role="group" aria-label="Cluster health score">
          <StatCard
            label="Health score"
            value={String(summary.score)}
            note={score.label}
            tone={score.tone}
            title="Starts at 100; loses points for topics below min ISR (up to 50), under-replicated (up to 25), uneven leader spread (up to 15) and message skew (up to 10)"
          />
        </div>
      </div>

      {result.failed > 0 && (
        <p className="metrics-hint">
          {result.failed} {result.failed === 1 ? "topic" : "topics"} could not be read and{" "}
          {result.failed === 1 ? "is" : "are"} left out of these figures.
        </p>
      )}

      {summary.totalTopics === 0 ? (
        <p className="metrics-hint">This cluster has no topics.</p>
      ) : (
        <>
          <ChartCard title="Topic distribution" hint={`${summary.totalTopics.toLocaleString()} topics by data-loss risk`}>
            <DistributionBar summary={summary} />
          </ChartCard>

          <h3 className="metrics-section-title">Risk analysis</h3>
          <div className="metrics-risk-grid">
            <ChartCard title="Data loss risk" hint="under-replicated topics, by replication factor vs min in-sync replicas">
              <CountBars
                ariaLabel="Under-replicated topics by replication factor and min ISR"
                seriesName="Under-replicated topics"
                yLabel="Topics"
                data={replication.map((group) => ({
                  label: group.label,
                  value: group.underReplicated,
                  color: RISK_COLOR[group.risk],
                }))}
              />
            </ChartCard>
            <ChartCard title="Cluster efficiency" hint="topics by how unevenly their leaders sit across brokers">
              <CountBars
                ariaLabel="Topics by leader distribution"
                seriesName="Topics"
                yLabel="Topics"
                data={efficiency.map((b) => ({ label: b.label, value: b.topics, color: TONE_COLOR[b.tone] }))}
              />
            </ChartCard>
            <ChartCard title="Load imbalance risk" hint="topics by message skew ratio (busiest partition over the mean)">
              <CountBars
                ariaLabel="Topics by message skew ratio"
                seriesName="Topics"
                yLabel="Topics"
                data={imbalance.map((b) => ({ label: b.label, value: b.topics, color: TONE_COLOR[b.tone] }))}
              />
            </ChartCard>
          </div>

          <h3 className="metrics-section-title">Topic health overview</h3>
          <HealthOverviewTable topics={topics} />
        </>
      )}
    </div>
  );
}
