import { ComponentProps, lazy, Suspense } from "react";

/**
 * The two views that draw Recharts charts, loaded on demand for the same
 * reason `gridTabs` loads AG Grid: Recharts (with d3) is a large chunk and is
 * reachable only from a tab the user has to click into. Import the tabs from
 * here, never from their own modules, or Recharts lands in the initial chunk.
 */
const TopicMetricsTabLazy = lazy(() => import("./TopicMetricsTab").then((m) => ({ default: m.TopicMetricsTab })));
const LagMetricsTabLazy = lazy(() => import("./LagMetricsTab").then((m) => ({ default: m.LagMetricsTab })));

export function TopicMetricsTab(props: ComponentProps<typeof TopicMetricsTabLazy>) {
  return (
    <Suspense fallback={<p>Loading…</p>}>
      <TopicMetricsTabLazy {...props} />
    </Suspense>
  );
}

export function LagMetricsTab(props: ComponentProps<typeof LagMetricsTabLazy>) {
  return (
    <Suspense fallback={<p>Loading…</p>}>
      <LagMetricsTabLazy {...props} />
    </Suspense>
  );
}
