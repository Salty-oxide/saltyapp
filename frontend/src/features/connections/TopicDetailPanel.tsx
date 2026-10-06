import { useState } from "react";
import { AclAccessTab } from "./AclAccessTab";
import { TopicKsqlTab } from "../ksql/TopicKsqlTab";
import { ConfigTab } from "./ConfigTab";
import { PartitionsTab } from "./PartitionsTab";
import { TopicMetadataTab } from "./TopicMetadataTab";
import { TopicSchemaTab } from "./TopicSchemaTab";
// Via `gridTabs`, never directly: that wrapper is what keeps AG Grid out of
// the initial bundle.
import { DataTab } from "./gridTabs";

export interface TopicDetailPanelProps {
  connectionId: string;
  topicName: string;
}

type TopicTabId = "data" | "metadata" | "partitions" | "schema" | "config" | "access" | "query";

/**
 * Data leads because it is what opening a topic is for — reading its
 * messages. It is also the default tab, so the first tab and the landing tab
 * are the same one and the tab strip doesn't open with its selection in the
 * middle.
 *
 * "Meta Data" was "Properties", which collided with the Properties tab on
 * every other panel while holding something different: the topic's name and a
 * message count, not settings.
 *
 * Config is last because it is the only tab that is purely read-back — the
 * broker's own view of the topic's settings, which nothing here can change.
 * Access sits after it for the same reason: it is the broker's own view of
 * who may touch this topic, and equally unchangeable from here. The two
 * read-only tabs are kept together at the end.
 */
const TOPIC_TABS: { id: TopicTabId; label: string }[] = [
  { id: "data", label: "Data" },
  { id: "metadata", label: "Meta Data" },
  { id: "partitions", label: "Partitions" },
  { id: "schema", label: "Schema" },
  { id: "config", label: "Config" },
  { id: "access", label: "Access" },
  // Last, and read-only like the two before it — ksqlDB is a separate server
  // and this tab only reads from it.
  { id: "query", label: "Query" },
];

export function TopicDetailPanel({ connectionId, topicName }: TopicDetailPanelProps) {
  const [activeTab, setActiveTab] = useState<TopicTabId>("data");

  return (
    <div className="cluster-detail-panel">
      <header className="cluster-detail-header">
        <h2>{topicName}</h2>
      </header>

      <div className="connection-modal-tabs" role="tablist">
        {TOPIC_TABS.map((tab) => (
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

      <div className="connection-modal-body">
        {activeTab === "data" && <DataTab connectionId={connectionId} topicName={topicName} />}
        {activeTab === "metadata" && <TopicMetadataTab connectionId={connectionId} topicName={topicName} />}
        {activeTab === "partitions" && <PartitionsTab connectionId={connectionId} topicName={topicName} />}
        {activeTab === "schema" && <TopicSchemaTab connectionId={connectionId} topicName={topicName} />}
        {activeTab === "config" && <ConfigTab connectionId={connectionId} topicName={topicName} />}
        {activeTab === "access" && (
          <AclAccessTab connectionId={connectionId} resourceType="topic" resourceName={topicName} />
        )}
        {activeTab === "query" && <TopicKsqlTab connectionId={connectionId} topicName={topicName} />}
      </div>
    </div>
  );
}
