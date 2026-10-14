import { useEffect, useMemo, useState } from "react";
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import {
  AXIS_PROPS,
  ChartCard,
  compact,
  GRID_PROPS,
  StatCard,
  statNumber,
  TOOLTIP_PROPS,
} from "./metricsChartStyle";
import { ensureSampling, sampleNow, SAMPLE_INTERVAL_MS, stopSampling, touchSampling } from "./topicRateSampler";
import { EMPTY_RATE_HISTORY, RATE_RANGES, RateRange, RateRangeId, seriesFor } from "./topicRates";
import { topicRateKey, useTopicRateStore } from "./useTopicRateStore";

export interface TopicRatesTabProps {
  connectionId: string;
  topicName: string;
}

const PRODUCE_COLOR = "var(--color-accent)";
const CONSUME_COLOR = "var(--color-status-green, var(--color-accent))";

/** Short ranges read as a clock, long ones as a date — the x axis would otherwise repeat "14:00" a week over. */
function tickFormatter(range: RateRange) {
  return (at: number) =>
    range.windowMs <= 24 * 60 * 60_000
      ? new Date(at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
      : new Date(at).toLocaleDateString([], { month: "short", day: "numeric" });
}

function RangeOptions({ value, onChange }: { value: RateRangeId; onChange: (id: RateRangeId) => void }) {
  return (
    <div className="metrics-range" role="group" aria-label="Time range">
      {RATE_RANGES.map((range) => (
        <button
          key={range.id}
          type="button"
          className="metrics-range__option"
          aria-pressed={value === range.id}
          onClick={() => onChange(range.id)}
        >
          {range.label}
        </button>
      ))}
    </div>
  );
}

interface RateChartProps {
  title: string;
  ariaLabel: string;
  seriesName: string;
  dataKey: "produced" | "consumed";
  color: string;
  range: RateRange;
  onRangeChange: (id: RateRangeId) => void;
  points: ReturnType<typeof seriesFor>;
  hasData: boolean;
  /** Said under the title while there is nothing to draw, if it is not the same for both charts. */
  emptyHint: string;
}

function RateChart({
  title,
  ariaLabel,
  seriesName,
  dataKey,
  color,
  range,
  onRangeChange,
  points,
  hasData,
  emptyHint,
}: RateChartProps) {
  return (
    <ChartCard
      title={title}
      hint={hasData ? `messages per ${range.bucketMs >= 3_600_000 ? `${range.bucketMs / 3_600_000} h` : `${range.bucketMs / 60_000} min`}` : emptyHint}
      actions={<RangeOptions value={range.id} onChange={onRangeChange} />}
    >
      <div className="metrics-chart" role="img" aria-label={ariaLabel}>
        <ResponsiveContainer width="100%" height={240}>
          <LineChart data={points} margin={{ top: 16, right: 24, bottom: 4, left: 0 }}>
            <CartesianGrid {...GRID_PROPS} />
            <XAxis
              dataKey="at"
              type="number"
              domain={["dataMin", "dataMax"]}
              {...AXIS_PROPS}
              tickFormatter={tickFormatter(range)}
            />
            <YAxis
              {...AXIS_PROPS}
              axisLine={false}
              allowDecimals={false}
              tickFormatter={(v) => compact.format(v)}
              label={{ value: "Messages", angle: -90, position: "insideLeft", fill: "var(--color-fg-muted)", fontSize: 11 }}
            />
            <Tooltip {...TOOLTIP_PROPS} labelFormatter={(at) => new Date(Number(at)).toLocaleString()} />
            {/* `linear`, not `monotone`: the series is a count per interval, and the straight segments show each interval's value rather than a curve smoothed between them. */}
            <Line
              type="linear"
              dataKey={dataKey}
              name={seriesName}
              stroke={color}
              strokeWidth={2}
              dot={{ r: 3, fill: color, strokeWidth: 0 }}
              activeDot={{ r: 5 }}
              connectNulls={false}
              isAnimationActive={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </ChartCard>
  );
}

/**
 * Messages produced to and consumed from a topic over time, 1 hour to 30 days.
 *
 * Kafka reports counters, not rates or history, so both series are built from
 * samples this app takes itself (see `topicRateSampler`): production is the
 * change in the topic's high watermarks, consumption the change in its active
 * consumer groups' committed offsets, all groups combined. The history is in
 * memory and only grows while the app is in front, so a long range holds only
 * what was observed — gaps are left as gaps, not drawn as zero.
 */
export function TopicRatesTab({ connectionId, topicName }: TopicRatesTabProps) {
  const [rangeId, setRangeId] = useState<RateRangeId>("1h");
  const range = RATE_RANGES.find((r) => r.id === rangeId) ?? RATE_RANGES[0];
  const history = useTopicRateStore((s) => s.byTopic[topicRateKey(connectionId, topicName)]) ?? EMPTY_RATE_HISTORY;

  useEffect(() => {
    ensureSampling(connectionId, topicName);
    // Sample only while the tab is open: nobody is looking otherwise.
    return () => stopSampling(connectionId, topicName);
  }, [connectionId, topicName]);

  // Re-evaluated as time passes, so the window slides even when no sample lands.
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = setInterval(() => {
      setNow(Date.now());
      // Still open: tells the sampler it is being watched.
      touchSampling(connectionId, topicName);
    }, SAMPLE_INTERVAL_MS);
    return () => clearInterval(timer);
  }, [connectionId, topicName]);

  const points = useMemo(() => seriesFor(history, range, Math.max(now, history.last?.at ?? 0)), [history, range, now]);
  const producedTotal = points.reduce((sum, p) => sum + (p.produced ?? 0), 0);
  const consumedTotal = points.reduce((sum, p) => sum + (p.consumed ?? 0), 0);
  const hasProduced = points.some((p) => p.produced !== null);
  const hasConsumed = points.some((p) => p.consumed !== null);

  const [refreshing, setRefreshing] = useState(false);
  const refresh = () => {
    setRefreshing(true);
    void sampleNow(connectionId, topicName).finally(() => {
      setRefreshing(false);
      setNow(Date.now());
    });
  };

  const collecting = `Collecting samples — a rate appears after the second one (every ${SAMPLE_INTERVAL_MS / 1000} s, while the app is in front).`;

  return (
    <div className="metrics-tab">
      <div className="metrics-tab__toolbar">
        <button type="button" onClick={refresh} disabled={refreshing}>
          Refresh
        </button>
      </div>
      <div className="metrics-stats">
        <div role="group" aria-label="Total produced in range">
          <StatCard label="Produced" value={statNumber(producedTotal)} title={producedTotal.toLocaleString()} />
        </div>
        <div role="group" aria-label="Total consumed in range">
          <StatCard label="Consumed" value={statNumber(consumedTotal)} title={consumedTotal.toLocaleString()} />
        </div>
      </div>

      <RateChart
        title="Produce rate"
        ariaLabel="Messages produced over time"
        seriesName="Produced"
        dataKey="produced"
        color={PRODUCE_COLOR}
        range={range}
        onRangeChange={setRangeId}
        points={points}
        hasData={hasProduced}
        emptyHint={collecting}
      />
      <RateChart
        title="Consume rate"
        ariaLabel="Messages consumed over time"
        seriesName="Consumed"
        dataKey="consumed"
        color={CONSUME_COLOR}
        range={range}
        onRangeChange={setRangeId}
        points={points}
        hasData={hasConsumed}
        emptyHint={hasProduced ? "No consumer group reading this topic has been seen — or listing groups is not permitted." : collecting}
      />
    </div>
  );
}
