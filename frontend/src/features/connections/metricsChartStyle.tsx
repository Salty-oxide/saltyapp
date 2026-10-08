import { ReactNode } from "react";

/** Shared look for the Metrics tabs' charts, all drawn from the app's theme variables so every theme gets them for free. */
export const AXIS_PROPS = {
  stroke: "var(--color-border)",
  tick: { fill: "var(--color-fg-muted)", fontSize: 11 },
  tickLine: false,
} as const;

export const GRID_PROPS = { stroke: "var(--color-border)", strokeDasharray: "3 4", vertical: false } as const;

export const TOOLTIP_PROPS = {
  cursor: { fill: "color-mix(in srgb, var(--color-fg) 6%, transparent)" },
  contentStyle: {
    background: "var(--color-bg-elevated)",
    border: "1px solid var(--color-border)",
    borderRadius: 8,
    fontSize: 12,
    boxShadow: "0 4px 14px rgba(0, 0, 0, 0.25)",
  },
  labelStyle: { color: "var(--color-fg-muted)", marginBottom: 2 },
  itemStyle: { color: "var(--color-fg)" },
} as const;

export const compact = new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 });

export function ChartCard({ title, hint, children }: { title: string; hint?: string; children: ReactNode }) {
  return (
    <section className="metrics-card">
      <header className="metrics-card__header">
        <h3 className="metrics-card__title">{title}</h3>
        {hint && <span className="metrics-card__hint">{hint}</span>}
      </header>
      {children}
    </section>
  );
}

export function StatCard({ label, value, note, tone, title }: { label: string; value: string; note?: string; tone?: string; title?: string }) {
  return (
    <div className={`metrics-stat${tone ? ` metrics-stat--${tone}` : ""}`} title={title}>
      <span className="metrics-stat__label">{label}</span>
      <span className="metrics-stat__value">{value}</span>
      {note && <span className="metrics-stat__note">{note}</span>}
    </div>
  );
}

/** Below this many rows a "busiest" list only repeats the chart. */
export const TOP_LIST_MIN = 12;
export const TOP_LIST_SIZE = 8;

/** Whole numbers up to a million, compact beyond — a stat card is too narrow for "20,082,300". */
export function statNumber(value: number): string {
  return Math.abs(value) >= 1_000_000 ? compact.format(value) : value.toLocaleString();
}

export function TopList({ title, rows, unit }: { title: string; rows: { name: string; value: number; note?: string }[]; unit: string }) {
  const peak = Math.max(1, ...rows.map((r) => r.value));
  return (
    <ChartCard title={title} hint={`top ${rows.length}`}>
      <ol className="metrics-toplist">
        {rows.map((row) => (
          <li key={row.name} className="metrics-toplist__row">
            <span className="metrics-toplist__name">{row.name}</span>
            <span className="metrics-toplist__bar" aria-hidden="true">
              <span style={{ width: `${(row.value / peak) * 100}%` }} />
            </span>
            <span className="metrics-toplist__value">
              {row.value.toLocaleString()} {unit}
              {row.note && <span className="metrics-toplist__note"> · {row.note}</span>}
            </span>
          </li>
        ))}
      </ol>
    </ChartCard>
  );
}
