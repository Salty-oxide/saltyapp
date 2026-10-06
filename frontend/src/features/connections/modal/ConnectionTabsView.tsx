import { ConnectionDraft } from "./draft";
import { PropertiesTab } from "./PropertiesTab";
import { KsqlTab } from "./KsqlTab";
import { SchemaTab } from "./SchemaTab";
import { SecurityAuthenticationTab } from "./SecurityAuthenticationTab";

export type ConnectionTabId = "properties" | "security" | "schema" | "ksqldb" | "query";

export interface ConnectionTab {
  id: ConnectionTabId;
  label: string;
}

export const CONNECTION_TABS: ConnectionTab[] = [
  { id: "properties", label: "Properties" },
  // One tab, not two: the security protocol decides whether a SASL mechanism
  // is needed, so the two belong in front of the user together.
  { id: "security", label: "Security & Authentication" },
  // Named for what it holds. It was "Advanced", which said nothing — every
  // field on it is a Schema Registry setting.
  { id: "schema", label: "Schema" },
  // A different server from the registry above, so a tab of its own rather
  // than a section on Schema.
  { id: "ksqldb", label: "ksqlDB" },
];

export interface ConnectionTabsViewProps {
  /**
   * Which tabs to show, defaulting to the connection's own.
   *
   * A prop because the cluster panel appends a Query tab that the New
   * Connection modal must not have: a half-written connection has nothing to
   * query. The panel renders that tab's body itself — see `children`.
   */
  tabs?: ConnectionTab[];
  /** Rendered instead of a built-in panel, for a tab this component does not own. */
  children?: React.ReactNode;
  activeTab: ConnectionTabId;
  onTabChange: (tab: ConnectionTabId) => void;
  draft: ConnectionDraft;
  onChange: (patch: Partial<ConnectionDraft>) => void;
  disabled?: boolean;
}

/** Shared tab bar + tab-panel switching, used by both the New Connection modal and the cluster detail panel. */
export function ConnectionTabsView({
  tabs = CONNECTION_TABS,
  children,
  activeTab,
  onTabChange,
  draft,
  onChange,
  disabled,
}: ConnectionTabsViewProps) {
  return (
    <>
      <div className="connection-modal-tabs" role="tablist">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            type="button"
            role="tab"
            aria-selected={activeTab === tab.id}
            className={`connection-modal-tab${activeTab === tab.id ? " connection-modal-tab--active" : ""}`}
            onClick={() => onTabChange(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </div>
      <div className="connection-modal-body">
        {activeTab === "properties" && <PropertiesTab draft={draft} onChange={onChange} disabled={disabled} />}
        {activeTab === "security" && (
          <SecurityAuthenticationTab draft={draft} onChange={onChange} disabled={disabled} />
        )}
        {activeTab === "schema" && <SchemaTab draft={draft} onChange={onChange} disabled={disabled} />}
        {activeTab === "ksqldb" && <KsqlTab draft={draft} onChange={onChange} disabled={disabled} />}
        {/* A tab this component does not own — the cluster panel's Query. */}
        {children}
      </div>
    </>
  );
}
