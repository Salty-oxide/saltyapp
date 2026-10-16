export interface RefreshBarProps {
  /** When the broker was last asked, epoch ms — `undefined` before the first load. */
  fetchedAtMs: number | undefined;
  refreshing: boolean;
  onRefresh: () => void;
}

/**
 * "Loaded at …" plus a Refresh button, for views backed by the backend's
 * metadata cache (24 h, `salty_kafka::metadata_cache`). The date is shown once
 * the data is not from today, so a day-old figure never reads as current.
 */
export function RefreshBar({ fetchedAtMs, refreshing, onRefresh }: RefreshBarProps) {
  const at = fetchedAtMs === undefined ? null : new Date(fetchedAtMs);
  const sameDay = at !== null && at.toDateString() === new Date().toDateString();
  return (
    <div className="metrics-tab__toolbar">
      {at && <span className="metrics-hint">Loaded at {sameDay ? at.toLocaleTimeString() : at.toLocaleString()}</span>}
      <button type="button" onClick={onRefresh} disabled={refreshing}>
        Refresh
      </button>
    </div>
  );
}
