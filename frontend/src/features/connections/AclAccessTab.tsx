import { AclOperationVerdict, AclResourceType } from "../../lib/tauri";
import { AclAvailabilityNotice } from "./AclAvailabilityNotice";
import { AclBindingsTable } from "./AclBindingsTable";
import { operationLabel } from "./acl";
import { useResourceAcls, useTopics } from "./useClusterResources";

export interface AclAccessTabProps {
  connectionId: string;
  resourceType: AclResourceType;
  resourceName: string;
}

/** The tick, cross, or dash in one matrix cell, with the provenance that explains it. */
function VerdictCell({ verdict }: { verdict: AclOperationVerdict }) {
  const { allowed, reason, viaWildcardPrincipal } = verdict;

  // An implied grant is shown as a grant, but never as the *same* thing as a
  // direct one: the reader has to know that revoking the named operation is
  // what removes it, because there is no binding to delete for this column.
  const implied = reason.kind === "impliedAllow";
  const title = ((): string => {
    switch (reason.kind) {
      case "explicitDeny":
        return viaWildcardPrincipal
          ? "Denied by a binding on User:* — it applies to every principal"
          : "Denied by an explicit Deny binding, which overrides any Allow";
      case "directAllow":
        return viaWildcardPrincipal
          ? "Allowed by a binding on User:* — it applies to every principal"
          : "Allowed by a binding naming this operation";
      case "impliedAllow":
        return `Implied by ${operationLabel(reason.via)} — no binding names this operation directly`;
      case "defaultDeny":
        return "No binding grants this, so Kafka denies it";
    }
  })();

  return (
    <td
      className={allowed ? "acl-verdict--allow" : "acl-verdict--deny"}
      title={title}
      data-testid={`verdict-${verdict.operation}`}
    >
      {allowed ? "✓" : "✗"}
      {implied && <span className="acl-verdict-implied"> implied</span>}
      {viaWildcardPrincipal && <span className="acl-verdict-wildcard"> via *</span>}
    </td>
  );
}

/**
 * Who can do what to one topic or consumer group.
 *
 * Two halves, in this order on purpose. The **derived matrix** answers the
 * question people arrive with, and is labelled as derived. The **bindings**
 * below it are the facts the broker actually reported, and are never hidden
 * behind a toggle — a subtly wrong implication rule has to stay visible, and
 * the pattern-type column is what explains why each binding matched this
 * resource at all.
 *
 * Neither half is computed here: the verdicts come from
 * `salty_core::resource_access`, so Kafka's precedence and implication rules
 * have exactly one implementation.
 */
export function AclAccessTab({ connectionId, resourceType, resourceName }: AclAccessTabProps) {
  const access = useResourceAcls(connectionId, resourceType, resourceName, true);
  // Already cached by the tree — used only to resolve what a PREFIXED pattern
  // currently covers, so no broker call is made for it here.
  const topics = useTopics(connectionId, true);

  if (access.isLoading) return <p>Loading ACLs…</p>;
  if (access.error) {
    return (
      <p role="alert" className="connection-modal-error">
        ACLs could not be loaded. {access.error.message}
      </p>
    );
  }
  if (!access.data) return null;

  const { listing, access: rows } = access.data;
  const operations = rows[0]?.verdicts.map((verdict) => verdict.operation) ?? [];

  return (
    <div className="acl-access-tab">
      <AclAvailabilityNotice availability={listing.availability} isEmpty={listing.bindings.length === 0} />

      {listing.bindingErrors.length > 0 && (
        <p role="status" className="acl-notice acl-notice--warning">
          {listing.bindingErrors.length} binding(s) could not be read: {listing.bindingErrors.join("; ")}
        </p>
      )}

      {rows.length > 0 && (
        <>
          <h4 className="acl-section-heading">
            Effective access <span className="acl-derived-tag">derived from the bindings below</span>
          </h4>
          <div className="acl-table-scroll">
            <table className="acl-table acl-matrix" data-testid="acl-matrix">
              <thead>
                <tr>
                  <th>Principal</th>
                  {operations.map((operation) => (
                    <th key={operation}>{operationLabel(operation)}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => (
                  <tr key={row.principal} data-testid={`acl-matrix-row-${row.principal}`}>
                    <td className="acl-cell-name">{row.principal}</td>
                    {row.verdicts.map((verdict) => (
                      <VerdictCell key={verdict.operation} verdict={verdict} />
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <h4 className="acl-section-heading">
            Matching bindings <span className="acl-derived-tag">what the broker reported</span>
          </h4>
          <AclBindingsTable bindings={listing.bindings} topics={topics.data} showPrincipal />
        </>
      )}
    </div>
  );
}
