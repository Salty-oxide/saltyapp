import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { ConsumerGroupLag } from "../../lib/tauri";
import {
  AXIS_PROPS,
  ChartCard,
  compact,
  GRID_PROPS,
  StatCard,
  statNumber,
  TOOLTIP_PROPS,
  TOP_LIST_MIN,
  TOP_LIST_SIZE,
  TopList,
} from "./metricsChartStyle";
import { PartitionBarRows } from "./PartitionBarRows";
import { LagSample } from "./useLagHistoryStore";

export interface LagMetricsTabProps {
  /** The latest Refresh result; `undefined` until one has been fetched. */
  data: ConsumerGroupLag | undefined;
  /** Every total-lag sample taken for this group since the app started. */
  samples: LagSample[];
}

/** Same bands as the Lag table's row colours, so a bar and its row agree. */
const WARNING_LAG = 1_000;
const CRITICAL_LAG = 10_000;

function lagColor(lag: number): string {
  if (lag >= CRITICAL_LAG) return "var(--color-status-red)";
  if (lag >= WARNING_LAG) return "var(--color-status-amber)";
  return "var(--color-accent)";
}

const clock = (at: number) => new Date(at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
const clockWithSeconds = (at: number) => new Date(at).toLocaleTimeString();

/**
 * Lag by partition (the latest snapshot) and lag over time (one point per
 * Refresh). Kafka only reports the present, so the time series is as dense as
 * the user's refreshing — hence the hint when there is a single point.
 */
export function LagMetricsTab({ data, samples }: LagMetricsTabProps) {
  if (!data) return <p>Press Refresh on the Lag tab to load metrics.</p>;
  if (data.partitions.length === 0) return <p>This group has no active partition assignment.</p>;

  const byPartition = data.partitions.map((p) => ({ name: `${p.topic}-${p.partition}`, lag: p.lag ?? 0 }));
  const total = byPartition.reduce((sum, p) => sum + p.lag, 0);
  const worst = byPartition.reduce((a, b) => (b.lag > a.lag ? b : a));
  const laggiest = [...byPartition]
    .filter((p) => p.lag > 0)
    .sort((a, b) => b.lag - a.lag)
    .slice(0, TOP_LIST_SIZE)
    .map((p) => ({ name: p.name, value: p.lag }));
  const trend = samples.length >= 2 ? samples[samples.length - 1].totalLag - samples[samples.length - 2].totalLag : null;

  return (
    <div className="metrics-tab">
      <div className="metrics-stats">
        <StatCard label="Total lag" value={statNumber(total)} title={total.toLocaleString()} />
        <StatCard label="Partitions" value={byPartition.length.toLocaleString()} />
        <StatCard label="Worst partition" value={worst.lag > 0 ? worst.name : "—"} note={worst.lag > 0 ? `${worst.lag.toLocaleString()} behind` : undefined} />
        <StatCard
          label="Since last refresh"
          value={trend === null ? "—" : `${trend > 0 ? "+" : ""}${trend.toLocaleString()}`}
          tone={trend === null || trend === 0 ? undefined : trend > 0 ? "bad" : "good"}
        />
      </div>

      <ChartCard title="Lag by partition" hint="latest Refresh">
        <PartitionBarRows
          ariaLabel="Lag by partition"
          seriesName="Lag"
          bars={byPartition.map((p) => ({ key: p.name, label: p.name, title: p.name, value: p.lag }))}
          colorOf={(bar) => lagColor(bar.value)}
        />
      </ChartCard>

      {byPartition.length >= TOP_LIST_MIN && laggiest.length > 0 && <TopList title="Most lagging partitions" rows={laggiest} unit="behind" />}

      <ChartCard title="Total lag over time" hint={samples.length < 2 ? "Refresh again to see the trend — each Refresh adds a point." : `${samples.length} samples`}>
        <div className="metrics-chart" role="img" aria-label="Total lag over time">
          <ResponsiveContainer width="100%" height={240}>
            <AreaChart data={samples} margin={{ top: 16, right: 24, bottom: 4, left: 0 }}>
              <defs>
                <linearGradient id="lag-fill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="var(--color-accent)" stopOpacity={0.35} />
                  <stop offset="100%" stopColor="var(--color-accent)" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid {...GRID_PROPS} />
              <XAxis
                dataKey="at"
                type="number"
                domain={["dataMin", "dataMax"]}
                {...AXIS_PROPS}
                tickFormatter={clock}
              />
              <YAxis {...AXIS_PROPS} axisLine={false} allowDecimals={false} tickFormatter={(v) => compact.format(v)} />
              <Tooltip {...TOOLTIP_PROPS} labelFormatter={(at) => clockWithSeconds(Number(at))} />
              <Area
                type="monotone"
                dataKey="totalLag"
                name="Total lag"
                stroke="var(--color-accent)"
                strokeWidth={2}
                fill="url(#lag-fill)"
                dot={{ r: 3, fill: "var(--color-accent)", strokeWidth: 0 }}
                activeDot={{ r: 5 }}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </ChartCard>
    </div>
  );
}
