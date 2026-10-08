import { useEffect, useRef, useState } from "react";
import { Bar, BarChart, CartesianGrid, Cell, LabelList, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { AXIS_PROPS, compact, GRID_PROPS, statNumber, TOOLTIP_PROPS } from "./metricsChartStyle";

export interface PartitionBar {
  /** Unique within the chart. */
  key: string;
  /** Under the bar, truncated if long. */
  label: string;
  /** Heading of the tooltip. */
  title: string;
  value: number;
}

export interface PartitionBarRowsProps {
  bars: PartitionBar[];
  /** Tooltip name of the value, e.g. "Messages". */
  seriesName: string;
  colorOf: (bar: PartitionBar) => string;
  /** Dashed line across every row. */
  mean?: number;
  ariaLabel: string;
}

/** Every bar is the same width and gets this much room, so a count printed on top of it always fits. */
export const BAR_SLOT_PX = 64;
export const BAR_WIDTH_PX = 36;
const ROW_HEIGHT_PX = 220;
/** Room the Y axis and chart margins take from a row. */
const ROW_CHROME_PX = 80;
const MIN_PER_ROW = 4;
/** Used until the container has been measured, and wherever there is no layout at all. */
const DEFAULT_PER_ROW = 12;

function truncate(label: string, max = 11): string {
  return label.length > max ? `…${label.slice(-(max - 1))}` : label;
}

/** Rounds up to 1, 2, 2.5, 5 or 10 times a power of ten, so the axis ends on a figure a person would pick. */
export function niceCeil(value: number): number {
  if (value <= 1) return 1;
  const magnitude = 10 ** Math.floor(Math.log10(value));
  const step = [1, 2, 2.5, 5, 10].find((m) => m * magnitude >= value) ?? 10;
  return step * magnitude;
}

/** Bars that fit side by side in `width` pixels. */
export function barsPerRow(width: number): number {
  if (width <= 0) return DEFAULT_PER_ROW;
  return Math.max(MIN_PER_ROW, Math.floor((width - ROW_CHROME_PX) / BAR_SLOT_PX));
}

function useElementWidth() {
  const ref = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    setWidth(el.clientWidth);
    if (typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver(() => setWidth(el.clientWidth));
    observer.observe(el);
    return () => observer.disconnect();
  }, []);
  return [ref, width] as const;
}

/**
 * One bar per partition at a fixed width with its count printed above it. When
 * they don't fit across the panel they wrap onto further rows rather than
 * shrinking — a topic with 400 partitions is a tall chart to scroll, not a
 * thin one to squint at. Every row shares one Y scale, so a bar's height means
 * the same thing wherever it sits, and a short last row is padded with empty
 * slots so its bars stay the same width as the rows above.
 */
export function PartitionBarRows({ bars, seriesName, colorOf, mean, ariaLabel }: PartitionBarRowsProps) {
  const [ref, width] = useElementWidth();
  const perRow = barsPerRow(width);
  const peak = Math.max(1, ...bars.map((b) => b.value));
  // Headroom above the tallest bar for its count.
  const top = niceCeil(peak * 1.12);
  const domain: [number, number] = [0, top];
  const ticks = [0, top / 4, top / 2, (top * 3) / 4, top];

  const rows: { key: string; label: string; title: string; value: number | null; color: string }[][] = [];
  for (let start = 0; start < bars.length; start += perRow) {
    const row = bars.slice(start, start + perRow).map((b) => ({
      key: b.key,
      label: b.label,
      title: b.title,
      value: b.value as number | null,
      color: colorOf(b),
    }));
    // Only a wrapped chart needs its last row padded: a single row spreads to the panel on its own.
    while (bars.length > perRow && row.length < perRow) {
      row.push({ key: `pad-${start}-${row.length}`, label: "", title: "", value: null, color: "transparent" });
    }
    rows.push(row);
  }

  return (
    <div ref={ref} className="metrics-chart partition-rows" role="img" aria-label={ariaLabel}>
      {rows.map((row, index) => (
        <ResponsiveContainer key={index} width="100%" height={ROW_HEIGHT_PX}>
          <BarChart data={row} margin={{ top: 22, right: 16, bottom: 4, left: 0 }}>
            <CartesianGrid {...GRID_PROPS} />
            <XAxis dataKey="key" {...AXIS_PROPS} interval={0} tickFormatter={(_, i) => truncate(row[i]?.label ?? "")} />
            <YAxis {...AXIS_PROPS} axisLine={false} allowDecimals={false} domain={domain} ticks={ticks} width={48} tickFormatter={(v) => compact.format(v)} />
            <Tooltip
              {...TOOLTIP_PROPS}
              labelFormatter={(key) => row.find((r) => r.key === key)?.title ?? ""}
              formatter={(value) => [Number(value).toLocaleString(), seriesName]}
            />
            {mean !== undefined && <ReferenceLine y={mean} stroke="var(--color-fg-muted)" strokeDasharray="5 4" />}
            <Bar dataKey="value" name={seriesName} barSize={BAR_WIDTH_PX} radius={[4, 4, 0, 0]} isAnimationActive={false}>
              {row.map((r) => (
                <Cell key={r.key} fill={r.color} />
              ))}
              <LabelList
                dataKey="value"
                position="top"
                fill="var(--color-fg)"
                stroke="var(--color-bg-elevated)"
                strokeWidth={3}
                paintOrder="stroke"
                fontSize={11}
                formatter={(v: unknown) => (typeof v === "number" ? statNumber(v) : "")}
              />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      ))}
    </div>
  );
}
