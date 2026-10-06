import { useMemo } from "react";
import { BrokerSummary, ConsumerGroupSummary } from "../../lib/tauri";
import { AclAvailabilityNotice } from "./AclAvailabilityNotice";
import { bindingCountsByPrincipal, distinctPrincipals, orderPrincipals, selfPrincipal } from "./acl";
import { useConnectionsQuery } from "./useConnections";
import { useTabsStore } from "../tabs/useTabsStore";
import { useWorkspaceSelectionStore } from "../workspace/useWorkspaceSelectionStore";
import { ResourceCategory } from "./ResourceCategory";
import { TopicCategory } from "./TopicCategory";
import { useAcls, useBrokers, useConsumerGroups, useTopics } from "./useClusterResources";
import { treeKey, useTreeUiStore } from "./useTreeUiStore";

/** A group in this state currently has no members, and so no active partition assignment — see the "hide empty consumer groups" context menu item below. */
const EMPTY_GROUP_STATE = "Empty";

export interface ClusterResourceTreeProps {
  connectionId: string;
}

/**
 * The three searchable sub-lists shown once a cluster is connected. Data is
 * fetched eagerly as soon as this mounts (i.e. as soon as the cluster is
 * connected — the parent ConnectionTree mounts this before the tree row is
 * ever expanded, only hiding it visually via CSS), so expanding a category
 * is instant instead of showing a spinner.
 */
export function ClusterResourceTree({ connectionId }: ClusterResourceTreeProps) {
  const activeTabId = useTabsStore((s) => s.activeTabId);
  const consumersTreeKey = treeKey(activeTabId, connectionId, "Consumers");
  // Consumers is the one category fetched lazily. Listing groups is the most
  // expensive of the three on a busy cluster and needs ACLs the other two
  // don't (Describe on the Group resource), so a principal that reads topics
  // and brokers perfectly well can still be refused it — paying that cost,
  // and surfacing that failure, on every connect for a category the user may
  // never open is work nobody asked for.
  const consumersExpanded = useTreeUiStore((s) => s.expanded[consumersTreeKey] ?? false);
  // Access Control is lazy for exactly the same reason as Consumers: listing
  // ACLs needs `Describe` on the `Cluster` resource, which a principal with
  // full read access to every topic is routinely not granted.
  const aclsTreeKey = treeKey(activeTabId, connectionId, "Access Control");
  const aclsExpanded = useTreeUiStore((s) => s.expanded[aclsTreeKey] ?? false);
  const brokers = useBrokers(connectionId, true);
  const topics = useTopics(connectionId, true);
  const groups = useConsumerGroups(connectionId, consumersExpanded);
  const acls = useAcls(connectionId, aclsExpanded);
  // Each listing is fetched once and then held indefinitely (see
  // `CLUSTER_LISTING_OPTIONS`), so opening a category is how the user asks
  // for a fresh one. Wrapped to drop `refetch`'s return value, which
  // `onExpand` does not want.
  const refetchBrokers = () => void brokers.refetch();
  const refetchTopics = () => void topics.refetch();
  // Consumers only: the first open is what enables the query, so it fetches
  // on its own — refetching here too would fire two listings at once for the
  // one click. Later opens have nothing else to trigger them.
  const refetchGroups = () => {
    if (consumersExpanded || groups.data !== undefined) void groups.refetch();
  };
  // Same first-open rule as Consumers — see `refetchGroups`.
  const refetchAcls = () => {
    if (aclsExpanded || acls.data !== undefined) void acls.refetch();
  };
  const hideEmptyGroups = useTreeUiStore((s) => s.hideEmptyConsumerGroups[consumersTreeKey] ?? false);
  const toggleHideEmptyConsumerGroups = useTreeUiStore((s) => s.toggleHideEmptyConsumerGroups);

  const selection = useWorkspaceSelectionStore((s) => s.selection);
  const selectBroker = useWorkspaceSelectionStore((s) => s.selectBroker);
  const selectTopic = useWorkspaceSelectionStore((s) => s.selectTopic);
  const selectConsumerGroup = useWorkspaceSelectionStore((s) => s.selectConsumerGroup);
  const selectPrincipal = useWorkspaceSelectionStore((s) => s.selectPrincipal);

  const { data: connections } = useConnectionsQuery();
  const connection = connections?.find((c) => c.id === connectionId);
  // `null` when the principal cannot be known (mTLS, Kerberos) — the "You"
  // row is then simply absent rather than guessed at. See `selfPrincipal`.
  const self = selfPrincipal(connection);
  const bindings = acls.data?.bindings;
  const principals = useMemo(
    () => orderPrincipals(distinctPrincipals(bindings ?? []), self),
    [bindings, self],
  );
  const bindingCounts = useMemo(() => bindingCountsByPrincipal(bindings ?? []), [bindings]);

  return (
    <ul className="resource-tree" data-testid={`resource-tree-${connectionId}`}>
      <ResourceCategory<BrokerSummary>
        label="Brokers"
        items={brokers.data}
        isLoading={brokers.isLoading}
        error={brokers.error}
        onExpand={refetchBrokers}
        treeKey={treeKey(activeTabId, connectionId, "Brokers")}
        getKey={(broker) => String(broker.id)}
        getLabel={(broker) => `${broker.id} — ${broker.host}:${broker.port}`}
        matchesSearch={(broker, query) =>
          `${broker.id} ${broker.host}`.toLowerCase().includes(query.toLowerCase())
        }
        isSelected={(broker) =>
          selection?.type === "broker" &&
          selection.connectionId === connectionId &&
          selection.brokerId === broker.id
        }
        onSelect={(broker) => selectBroker(connectionId, broker.id)}
      />
      <TopicCategory
        connectionId={connectionId}
        topics={topics.data}
        isLoading={topics.isLoading}
        error={topics.error}
        onExpand={refetchTopics}
        isSelected={(topic) =>
          selection?.type === "topic" &&
          selection.connectionId === connectionId &&
          selection.topicName === topic.name
        }
        onSelect={(topic) => selectTopic(connectionId, topic.name)}
      />
      <ResourceCategory<ConsumerGroupSummary>
        label="Consumers"
        items={groups.data}
        isLoading={groups.isLoading}
        onExpand={refetchGroups}
        treeKey={consumersTreeKey}
        getKey={(group) => group.groupId}
        getLabel={(group) => group.groupId}
        error={groups.error}
        matchesSearch={(group, query) => group.groupId.toLowerCase().includes(query.toLowerCase())}
        isSelected={(group) =>
          selection?.type === "consumerGroup" &&
          selection.connectionId === connectionId &&
          selection.groupId === group.groupId
        }
        onSelect={(group) => selectConsumerGroup(connectionId, group.groupId)}
        additionalFilter={hideEmptyGroups ? (group) => group.state !== EMPTY_GROUP_STATE : undefined}
        contextMenuItems={[
          {
            label: hideEmptyGroups ? "Show all consumer groups" : "Hide empty consumer groups",
            onSelect: () => toggleHideEmptyConsumerGroups(consumersTreeKey),
          },
        ]}
      />
      <ResourceCategory<string>
        label="Access Control"
        items={acls.data ? principals : undefined}
        isLoading={acls.isLoading}
        error={acls.error}
        onExpand={refetchAcls}
        treeKey={aclsTreeKey}
        getKey={(principal) => principal}
        getLabel={(principal) => {
          const count = bindingCounts.get(principal) ?? 0;
          const name = principal === self ? `You (${principal})` : principal;
          return `${name} — ${count}`;
        }}
        matchesSearch={(principal, query) => principal.toLowerCase().includes(query.toLowerCase())}
        isSelected={(principal) =>
          selection?.type === "principal" &&
          selection.connectionId === connectionId &&
          selection.principal === principal
        }
        onSelect={(principal) => selectPrincipal(connectionId, principal)}
        // An empty ACL list means opposite things on an unsecured cluster and
        // on one that denies by default, so the category always says which —
        // through `notice` rather than `error`, since neither case is a
        // failure of the call.
        notice={
          acls.data ? (
            <AclAvailabilityNotice
              availability={acls.data.availability}
              isEmpty={acls.data.bindings.length === 0}
            />
          ) : undefined
        }
      />
    </ul>
  );
}
