import { useState } from "react";
import { parseDate, validateDateStrings } from "./dataFilters";
import { ChartCard, StatCard, statNumber, TOP_LIST_MIN, TOP_LIST_SIZE, TopList } from "./metricsChartStyle";
import { PartitionBarRows } from "./PartitionBarRows";
import { computePartitionSkew, HIGH_SKEW, skewFromCounts } from "./partitionSkew";
import { usePartitionMessageCounts, usePartitions } from "./useClusterResources";

export interface TopicMetricsTabProps {
  connectionId: string;
  topicName: string;
}

const SKEW_LABEL = { balanced: "Balanced", moderate: "Moderate skew", high: "High skew" } as const;
const SKEW_TONE = { balanced: "good", moderate: "warn", high: "bad" } as const;

/**
 * Partition skew: how many messages each partition holds. Kafka spreads keyed
 * messages by hash, so one hot key — or a null-key topic with a sticky
 * producer — shows up here as one tall bar against the mean line. Counts are
 * high minus low watermark, so compacted topics overstate what is live.
 * Bars at or past the "high skew" ratio are drawn in the alert colour.
 *
 * An optional From/To window narrows the count to the messages produced in it.
 * That is counted on the broker by offset distance (see
 * `count_partition_messages`), so it is cheap, but it assumes timestamps rise
 * with offset and is approximate for producer-assigned timestamps that don't.
 */
export function TopicMetricsTab({ connectionId, topicName }: TopicMetricsTabProps) {
  const [fromDate, setFromDate] = useState("");
  const [toDate, setToDate] = useState("");
  const { data: partitions, isLoading } = usePartitions(connectionId, topicName);

  const fromMs = parseDate(fromDate);
  const toMs = parseDate(toDate);
  const windowed = fromMs !== null || toMs !== null;
  const rangeError = validateDateStrings(fromDate, toDate);
  const counts = usePartitionMessageCounts(connectionId, topicName, fromMs, toMs, windowed && rangeError === null);

  if (isLoading) return <p>Loading metrics…</p>;
  if (!partitions || partitions.length === 0) return <p>No partitions found for this topic.</p>;

  const skew = windowed && counts.data ? skewFromCounts(counts.data) : computePartitionSkew(partitions);
  const windowError = rangeError ?? (counts.isError ? (counts.error instanceof Error ? counts.error.message : "Failed to count messages") : null);
  const showWindowResult = windowed && rangeError === null && !counts.isError;
  const windowPending = showWindowResult && counts.isFetching && !counts.data;
  const hideCharts = windowError !== null || windowPending;
  const busiest = [...skew.partitions]
    .sort((a, b) => b.messages - a.messages || a.partition - b.partition)
    .slice(0, TOP_LIST_SIZE)
    .map((p) => ({ name: `Partition ${p.partition}`, value: p.messages, note: skew.mean > 0 ? `${(p.messages / skew.mean).toFixed(2)}× mean` : undefined }));
  const emptyWindow = showWindowResult && counts.data !== undefined && skew.total === 0;

  return (
    <div className="metrics-tab">
      <div className="metrics-window" role="group" aria-label="Time window">
        <label>
          From
          <input type="datetime-local" value={fromDate} onChange={(e) => setFromDate(e.target.value)} />
        </label>
        <label>
          To
          <input type="datetime-local" value={toDate} onChange={(e) => setToDate(e.target.value)} />
        </label>
        {windowed && (
          <button
            type="button"
            className="metrics-window__clear"
            onClick={() => {
              setFromDate("");
              setToDate("");
            }}
          >
            Clear window
          </button>
        )}
      </div>
      {windowError && (
        <p role="alert" className="connection-modal-error">
          {windowError}
        </p>
      )}
      {windowPending && <p>Counting messages in the window…</p>}
      {emptyWindow && <p className="metrics-hint">No messages in this window.</p>}
      {!hideCharts && (
        <>
      <div className="metrics-stats">
        <StatCard label="Total" value={statNumber(skew.total)} title={skew.total.toLocaleString()} />
        <StatCard label="Mean" value={statNumber(Math.round(skew.mean))} title={Math.round(skew.mean).toLocaleString()} />
        <StatCard label="Min" value={statNumber(skew.min)} title={skew.min.toLocaleString()} />
        <StatCard label="Max" value={statNumber(skew.max)} title={skew.max.toLocaleString()} />
        <StatCard label="Skew" value={`${skew.skewRatio.toFixed(2)}×`} note={SKEW_LABEL[skew.level]} tone={SKEW_TONE[skew.level]} />
      </div>
      <ChartCard
        title="Messages per partition"
        hint={windowed ? "messages in the window, by offset; dashed line is the mean" : "high − low watermark; dashed line is the mean"}
      >
        <PartitionBarRows
          ariaLabel="Messages per partition"
          seriesName="Messages"
          mean={skew.mean}
          bars={skew.partitions.map((p) => ({ key: String(p.partition), label: `P${p.partition}`, title: `Partition ${p.partition}`, value: p.messages }))}
          colorOf={(bar) => (skew.mean > 0 && bar.value / skew.mean >= HIGH_SKEW ? "var(--color-status-red)" : "var(--color-accent)")}
        />
      </ChartCard>
      {skew.partitions.length >= TOP_LIST_MIN && <TopList title="Busiest partitions" rows={busiest} unit="messages" />}
        </>
      )}
    </div>
  );
}
