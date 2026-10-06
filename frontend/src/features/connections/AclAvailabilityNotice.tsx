import { AclAvailability } from "../../lib/tauri";

export interface AclAvailabilityNoticeProps {
  availability: AclAvailability;
  /** True when the listing came back with nothing in it. */
  isEmpty: boolean;
}

/**
 * Explains an ACL listing that shows nothing.
 *
 * An empty ACL list is the most misleading thing this feature can render: on
 * an unsecured cluster it means *everyone may do anything*, and on a cluster
 * with `allow.everyone.if.no.acl.found=false` it means *nobody may do
 * anything*. Those are opposites, and they look identical. So the reason is
 * always stated rather than left to be inferred from a blank list.
 *
 * The third case is an honest admission. librdkafka discards the broker's
 * error code for DescribeAcls (see `salty_core::AclAvailability`), so when an
 * authorizer is running and nothing came back, the app genuinely cannot tell
 * "no ACLs are defined" from "you are not allowed to read them" — and says
 * so instead of picking one.
 */
export function AclAvailabilityNotice({ availability, isEmpty }: AclAvailabilityNoticeProps) {
  if (!isEmpty && availability === "available") return null;

  if (availability === "noAuthorizer") {
    return (
      <p className="acl-notice" role="status" data-testid="acl-notice-no-authorizer">
        This cluster has no authorizer configured, so ACLs are never consulted and every principal may do
        anything. An empty list here means unrestricted, not restricted.
      </p>
    );
  }

  if (availability === "indeterminate") {
    return (
      <p className="acl-notice acl-notice--warning" role="status" data-testid="acl-notice-indeterminate">
        The broker returned no ACLs, and the app cannot tell why. Either none are defined, or this connection's
        principal lacks <code>Describe</code> on the <code>Cluster</code> resource — the Kafka client library
        reports both identically, so this is not a question Salty can answer for you.
      </p>
    );
  }

  return (
    <p className="acl-notice" role="status" data-testid="acl-notice-empty">
      No ACLs are defined. On a cluster with <code>allow.everyone.if.no.acl.found=false</code> this denies
      everything.
    </p>
  );
}
