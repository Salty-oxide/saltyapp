import { ComponentProps, lazy, Suspense } from "react";

/**
 * The three views that use AG Grid, loaded on demand.
 *
 * AG Grid is **two thirds of the frontend bundle** — `ag-grid-community` plus
 * `ag-stack` and `ag-grid-react` came to ~2.8 MB of the 4.0 MB of source the
 * bundle represented, against ~0.5 MB for this app's own code. It is reachable
 * from exactly three places, all of them tabs the user has to click into: a
 * topic's (or partition's) Data tab, a partition's Replicas tab, and a ksqlDB
 * Query tab. Everything else — the cluster tree, the payload viewer, settings,
 * publish — never touches it, yet every one of them waited for it to parse
 * before the window could paint.
 *
 * Tauri serves the bundle off local disk, so the cost this removes is not
 * download time but **parse and compile time on every launch**: measured at
 * 111 ms from HTML to DOMContentLoaded before this split.
 *
 * Kept in one module rather than a `lazy()` at each call site so all three
 * share a single chunk, and so the reasoning lives in one place. The exported
 * names match the underlying components, so the panels' only change is the
 * import path.
 */

const DataTabLazy = lazy(() => import("./DataTab").then((m) => ({ default: m.DataTab })));
const PartitionReplicasTabLazy = lazy(() =>
  import("./PartitionReplicasTab").then((m) => ({ default: m.PartitionReplicasTab })),
);
const KsqlResultsGridLazy = lazy(() =>
  import("../ksql/KsqlResultsGrid").then((m) => ({ default: m.KsqlResultsGrid })),
);

/**
 * Shown while the grid chunk loads. Deliberately the same "Loading…" the data
 * tabs already show while their own queries are in flight, so a cold first
 * open reads as one continuous load rather than two different ones.
 */
function GridChunkLoading() {
  return <p>Loading…</p>;
}

export function DataTab(props: ComponentProps<typeof DataTabLazy>) {
  return (
    <Suspense fallback={<GridChunkLoading />}>
      <DataTabLazy {...props} />
    </Suspense>
  );
}

export function PartitionReplicasTab(props: ComponentProps<typeof PartitionReplicasTabLazy>) {
  return (
    <Suspense fallback={<GridChunkLoading />}>
      <PartitionReplicasTabLazy {...props} />
    </Suspense>
  );
}

/** The ksqlDB result grid. Imported from here, never from its own module — see above. */
export function KsqlResultsGrid(props: ComponentProps<typeof KsqlResultsGridLazy>) {
  return (
    <Suspense fallback={<GridChunkLoading />}>
      <KsqlResultsGridLazy {...props} />
    </Suspense>
  );
}
