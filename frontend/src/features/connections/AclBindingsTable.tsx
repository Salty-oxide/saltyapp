import { useState } from "react";
import { AclBinding, TopicSummary } from "../../lib/tauri";
import {
  isExpandablePattern,
  operationLabel,
  patternTypeLabel,
  topicsMatchingPrefix,
} from "./acl";

export interface AclBindingsTableProps {
  bindings: AclBinding[];
  /**
   * The cluster's topics, for resolving what a `PREFIXED` pattern currently
   * covers. Optional — without it the pattern is still shown, just not
   * expanded.
   */
  topics?: TopicSummary[];
  /**
   * Adds a Principal column, for views where the rows belong to different
   * principals — the Access tabs. The principal panel omits it, since every
   * row there belongs to the principal the panel is about.
   *
   * It *adds* rather than replaces: the pattern has to stay visible either
   * way. On a topic's Access tab a `PREFIXED` badge with no prefix beside it
   * says a rule of that shape granted the access without saying which rule,
   * and the prefix is the thing you would have to go and change.
   */
  showPrincipal?: boolean;
}

/**
 * The bindings themselves, as the broker reported them.
 *
 * Shared by the principal panel and both Access tabs so that pattern type —
 * the attribute that decides whether a grant reaches resources that do not
 * exist yet — is presented one way everywhere.
 */
export function AclBindingsTable({ bindings, topics, showPrincipal = false }: AclBindingsTableProps) {
  const [expanded, setExpanded] = useState<string | null>(null);

  if (bindings.length === 0) return null;

  const columnCount = showPrincipal ? 6 : 5;

  return (
    <div className="acl-table-scroll">
      <table className="acl-table">
        <thead>
          <tr>
            {showPrincipal && <th>Principal</th>}
            <th>Pattern</th>
            <th>Type</th>
            <th>Operation</th>
            <th>Permission</th>
            <th>Host</th>
          </tr>
        </thead>
        <tbody>
          {bindings.map((binding, index) => {
            const rowKey = `${binding.principal}:${binding.resourceType}:${binding.resourceName}:${binding.patternType}:${binding.operation}:${binding.permission}:${index}`;
            const expandable = isExpandablePattern(binding.patternType);
            const matches = expandable ? topicsMatchingPrefix(binding.resourceName, topics) : [];
            const isOpen = expanded === rowKey;

            return [
              <tr
                key={rowKey}
                // Deny is rare and overrides every allow, so it must not read
                // as just another row.
                className={binding.permission === "deny" ? "acl-row--deny" : undefined}
                data-testid={`acl-row-${binding.principal}-${binding.operation}`}
              >
                {showPrincipal && <td className="acl-cell-name">{binding.principal}</td>}
                <td className="acl-cell-name">
                  {binding.resourceName}
                  {/* A prefix is a prefix of something, and rendering it bare
                      reads as a literal name one character short. */}
                  {binding.patternType === "prefixed" && <span className="acl-prefix-ellipsis">…</span>}
                </td>
                <td>
                  <span className={`acl-pattern acl-pattern--${binding.patternType}`}>
                    {patternTypeLabel(binding.patternType)}
                  </span>
                  {expandable && topics && (
                    <button
                      type="button"
                      className="acl-prefix-toggle"
                      aria-expanded={isOpen}
                      aria-label={`${isOpen ? "Hide" : "Show"} topics matching ${binding.resourceName}`}
                      onClick={() => setExpanded(isOpen ? null : rowKey)}
                    >
                      {isOpen ? "▾" : "▸"}
                    </button>
                  )}
                </td>
                <td>{operationLabel(binding.operation)}</td>
                <td className={binding.permission === "deny" ? "acl-permission--deny" : "acl-permission--allow"}>
                  {binding.permission === "deny" ? "Deny" : "Allow"}
                </td>
                <td>{binding.host}</td>
              </tr>,
              expandable && topics && isOpen ? (
                <tr key={`${rowKey}-matches`} className="acl-row-matches">
                  <td colSpan={columnCount}>
                    {/* "Currently" is the point: a prefixed grant covers
                        topics that do not exist yet, so this set grows on its
                        own the next time somebody creates a matching topic. */}
                    <span className="acl-matches-label">
                      PREFIXED <code>{binding.resourceName}</code> currently matches {matches.length} of{" "}
                      {topics.length} topics
                      {matches.length > 0 ? ":" : "."}
                    </span>{" "}
                    {matches.length > 0 && <span className="acl-matches-list">{matches.join(", ")}</span>}
                  </td>
                </tr>
              ) : null,
            ];
          })}
        </tbody>
      </table>
    </div>
  );
}
