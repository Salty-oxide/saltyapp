import { useState } from "react";
import { PartitionLag } from "../../lib/tauri";
import { AclAccessTab } from "./AclAccessTab";
import { useFetchConsumerGroupLag } from "./useClusterResources";

type GroupTabId = "lag" | "access";

/**
 * Lag first and by default — it is what the panel has always shown and the
 * reason people open a group. Access is second for the same reason Config and
 * Access sit last on the topic panel: it is the broker's own read-only view.
 *
 * Group ACLs are worth surfacing here specifically because a principal with
 * topic `Read` but no group `Read` fails in a way that looks like a broker
 * problem rather than a permission one.
 */
const GROUP_TABS: { id: GroupTabId; label: string }[] = [
  { id: "lag", label: "Lag" },
  { id: "access", label: "Access" },
];

export interface ConsumerGroupDetailPanelProps {
  connectionId: string;
  groupId: string;
}

const WARNING_THRESHOLD = 1_000;
const CRITICAL_THRESHOLD = 10_000;

function lagRowClass(lag: number | null): string {
  if (lag === null) return "";
  if (lag >= CRITICAL_THRESHOLD) return "lag-row--critical";
  if (lag >= WARNING_THRESHOLD) return "lag-row--warning";
  return "";
}

function formatNumber(value: number | null): string {
  return value === null ? "—" : value.toLocaleString();
}

function formatOwner(row: PartitionLag): string {
  return row.clientId && row.clientHost ? `${row.clientId}@${row.clientHost}` : "—";
}

export function ConsumerGroupDetailPanel({ connectionId, groupId }: ConsumerGroupDetailPanelProps) {
  const [activeTab, setActiveTab] = useState<GroupTabId>("lag");
  const [searchText, setSearchText] = useState("");
  const fetchLag = useFetchConsumerGroupLag();
  const data = fetchLag.data;

  const totalLag = data?.partitions.reduce((sum, p) => sum + (p.lag ?? 0), 0) ?? null;
  const filteredPartitions = data?.partitions.filter((p) =>
    p.topic.toLowerCase().includes(searchText.toLowerCase()),
  );

  return (
    <div className="cluster-detail-panel">
      <header className="cluster-detail-header">
        <h2>
          {groupId}
          {data && <> ({data.state})</>}
        </h2>
      </header>

      <div className="connection-modal-tabs" role="tablist">
        {GROUP_TABS.map((tab) => (
          <button
            key={tab.id}
            type="button"
            role="tab"
            aria-selected={activeTab === tab.id}
            className={`connection-modal-tab${activeTab === tab.id ? " connection-modal-tab--active" : ""}`}
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {activeTab === "access" && (
        <AclAccessTab connectionId={connectionId} resourceType="group" resourceName={groupId} />
      )}

      {activeTab === "lag" && (
        <>
      <div className="lag-panel-summary">
        <span>{totalLag !== null ? `Total lag: ${totalLag.toLocaleString()} messages` : ""}</span>
        <button
          type="button"
          onClick={() => fetchLag.mutate({ connectionId, groupId })}
          disabled={fetchLag.isPending}
        >
          Refresh
        </button>
      </div>

      {fetchLag.isError && (
        <p role="alert" className="connection-modal-error">
          {fetchLag.error instanceof Error ? fetchLag.error.message : "Failed to fetch lag"}
        </p>
      )}

      {data && data.partitions.length === 0 && <p>This group has no active partition assignment.</p>}

      {data && data.partitions.length > 0 && (
        <>
          <label className="panel-search">
            Search topics
            <input value={searchText} onChange={(e) => setSearchText(e.target.value)} placeholder="Search…" />
          </label>

          <table className="topic-detail-table">
            <thead>
              <tr>
                <th>Topic</th>
                <th>Partition</th>
                <th>Current</th>
                <th>Log End</th>
                <th>Lag</th>
                <th>Consumer</th>
              </tr>
            </thead>
            <tbody>
              {filteredPartitions?.map((row) => (
                <tr key={`${row.topic}-${row.partition}`} className={lagRowClass(row.lag)}>
                  <td>{row.topic}</td>
                  <td>{row.partition}</td>
                  <td>{formatNumber(row.currentOffset)}</td>
                  <td>{formatNumber(row.logEndOffset)}</td>
                  <td>{formatNumber(row.lag)}</td>
                  <td>{formatOwner(row)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
        </>
      )}
    </div>
  );
}
