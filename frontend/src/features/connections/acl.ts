import { AclBinding, AclOperation, AclPatternType, Connection, TopicSummary } from "../../lib/tauri";

/**
 * Kafka's any-principal wildcard.
 *
 * Kept in step with `salty_core::WILDCARD_PRINCIPAL` — this half only labels
 * a row in the UI; the backend is what actually folds wildcard bindings into
 * each principal's verdict.
 */
export const WILDCARD_PRINCIPAL = "User:*";

/**
 * The principal this connection authenticates as, or `null` when it cannot be
 * known.
 *
 * Deliberately conservative. A SASL username is the principal Kafka will see
 * (`User:<username>`), and a PLAINTEXT connection with no SASL is
 * `User:ANONYMOUS`. An mTLS or GSSAPI principal comes from a certificate DN
 * or a Kerberos ticket that rdkafka does not hand back, so those return
 * `null` and the tree simply omits the "You" row.
 *
 * Returning a guess would be worse than returning nothing: the row exists to
 * tell someone what *they* can do, and a wrong one misinforms them about
 * their own access.
 */
export function selfPrincipal(connection: Connection | undefined): string | null {
  if (!connection) return null;
  const username = connection.saslUsername?.trim();
  if (username) return `User:${username}`;
  if (connection.securityProtocol === "PLAINTEXT") return "User:ANONYMOUS";
  return null;
}

/**
 * Principals in the order the tree lists them: the connection's own first,
 * then the rest as the backend sorted them.
 *
 * The backend already returns a stable, sorted, de-duplicated list (see
 * `salty_core::principals`); this only lifts "you" to the top, because the
 * question people open this category with is usually about themselves.
 */
export function orderPrincipals(principals: string[], self: string | null): string[] {
  if (!self || !principals.includes(self)) return principals;
  return [self, ...principals.filter((principal) => principal !== self)];
}

/** The distinct principals named by a set of bindings, sorted. Mirrors the backend for the tree's own listing. */
export function distinctPrincipals(bindings: AclBinding[]): string[] {
  return [...new Set(bindings.map((binding) => binding.principal))].sort();
}

/** How many bindings each principal holds — the count badge beside a tree row. */
export function bindingCountsByPrincipal(bindings: AclBinding[]): Map<string, number> {
  const counts = new Map<string, number>();
  for (const binding of bindings) {
    counts.set(binding.principal, (counts.get(binding.principal) ?? 0) + 1);
  }
  return counts;
}

/**
 * The topics a `prefixed` binding currently governs.
 *
 * Resolved against the topic list the tree has already cached, so it costs no
 * broker call. "Currently" is the operative word and the reason this is worth
 * showing at all: a prefixed grant covers topics that do not exist yet, so
 * this is a snapshot of a set that grows on its own the next time somebody
 * creates a matching topic.
 *
 * Only meaningful for `prefixed`. A `literal` name matches itself (or, when
 * it is `*`, everything), which the table already shows plainly.
 */
export function topicsMatchingPrefix(prefix: string, topics: TopicSummary[] | undefined): string[] {
  if (!topics) return [];
  return topics
    .filter((topic) => topic.name.startsWith(prefix))
    .map((topic) => topic.name)
    .sort();
}

/** Whether a binding's pattern can cover resources beyond the one it names. */
export function isExpandablePattern(patternType: AclPatternType): boolean {
  return patternType === "prefixed";
}

/** Display order for the resource-type groups in the principal panel. */
const RESOURCE_TYPE_ORDER = ["topic", "group", "broker", "transactionalId", "any", "unknown"];

/**
 * A principal's bindings, grouped by what kind of resource they govern.
 *
 * Grouped because the same principal routinely holds topic and consumer-group
 * grants that have nothing to do with each other, and reading them
 * interleaved is how people miss that a group grant is absent.
 */
export function groupByResourceType(bindings: AclBinding[]): [string, AclBinding[]][] {
  const groups = new Map<string, AclBinding[]>();
  for (const binding of bindings) {
    const existing = groups.get(binding.resourceType);
    if (existing) existing.push(binding);
    else groups.set(binding.resourceType, [binding]);
  }
  return [...groups.entries()].sort(
    ([a], [b]) => RESOURCE_TYPE_ORDER.indexOf(a) - RESOURCE_TYPE_ORDER.indexOf(b),
  );
}

/** The label Kafka's own tooling uses. `broker` is librdkafka's name for what `kafka-acls.sh` calls the cluster. */
export function resourceTypeLabel(resourceType: string): string {
  if (resourceType === "broker") return "Cluster";
  return resourceType.charAt(0).toUpperCase() + resourceType.slice(1);
}

/** Pattern types render in Kafka's own upper-case spelling, matching what `kafka-acls.sh --list` prints. */
export function patternTypeLabel(patternType: AclPatternType): string {
  return patternType.toUpperCase();
}

/** Operations render capitalised, as Kafka names them. */
export function operationLabel(operation: AclOperation): string {
  return operation.charAt(0).toUpperCase() + operation.slice(1);
}
