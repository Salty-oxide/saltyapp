import { AclAvailabilityNotice } from "./AclAvailabilityNotice";
import { AclBindingsTable } from "./AclBindingsTable";
import { groupByResourceType, resourceTypeLabel, selfPrincipal, WILDCARD_PRINCIPAL } from "./acl";
import { useConnectionsQuery } from "./useConnections";
import { useAcls, useTopics } from "./useClusterResources";

export interface PrincipalDetailPanelProps {
  connectionId: string;
  principal: string;
}

/**
 * Everything one principal is granted, across the whole cluster.
 *
 * The bindings are already in hand — the tree's own listing fetched them, and
 * this filters that cached result rather than asking the broker again for a
 * subset of what it just sent.
 */
export function PrincipalDetailPanel({ connectionId, principal }: PrincipalDetailPanelProps) {
  const acls = useAcls(connectionId, true);
  const topics = useTopics(connectionId, true);
  const { data: connections } = useConnectionsQuery();
  const connection = connections?.find((c) => c.id === connectionId);
  const isSelf = selfPrincipal(connection) === principal;

  if (acls.isLoading) return <p>Loading ACLs…</p>;
  if (acls.error) {
    return (
      <p role="alert" className="connection-modal-error">
        ACLs could not be loaded. {acls.error.message}
      </p>
    );
  }
  if (!acls.data) return null;

  const mine = acls.data.bindings.filter((binding) => binding.principal === principal);
  const groups = groupByResourceType(mine);

  return (
    <div className="detail-panel acl-principal-panel">
      <header className="detail-panel-header">
        <h3>
          {principal}
          {isSelf && <span className="acl-self-tag"> this connection</span>}
        </h3>
        <span className="acl-binding-count">
          {mine.length} binding{mine.length === 1 ? "" : "s"}
        </span>
      </header>

      {principal === WILDCARD_PRINCIPAL && (
        // Easy to read as "a principal called *" rather than "every
        // principal", and the difference matters when deciding what to revoke.
        <p className="acl-notice acl-notice--warning" role="status">
          <code>{WILDCARD_PRINCIPAL}</code> is Kafka's any-principal wildcard. These bindings apply to{" "}
          <strong>every</strong> principal on this cluster, in addition to whatever each holds by name.
        </p>
      )}

      <AclAvailabilityNotice availability={acls.data.availability} isEmpty={acls.data.bindings.length === 0} />

      {groups.map(([resourceType, bindings]) => (
        <section key={resourceType} className="acl-resource-group">
          <h4 className="acl-section-heading">{resourceTypeLabel(resourceType).toUpperCase()}</h4>
          <AclBindingsTable bindings={bindings} topics={topics.data} />
        </section>
      ))}

      {mine.length === 0 && acls.data.bindings.length > 0 && (
        <p className="acl-notice" role="status">
          This principal holds no bindings.
        </p>
      )}
    </div>
  );
}
