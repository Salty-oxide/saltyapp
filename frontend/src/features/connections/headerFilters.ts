import { TopicMessage } from "../../lib/tauri";
import { base64ToDisplayText } from "./payloadDecoding";

/** One key/value row in the Data tab's header filter. `id` is only a React key — rows are added and removed, so an index would not be stable. */
export interface HeaderFilterRow {
  id: string;
  key: string;
  value: string;
}

/** A row that is complete enough to filter on, value already trimmed. */
export interface HeaderCriterion {
  key: string;
  value: string;
}

export function emptyHeaderRow(): HeaderFilterRow {
  return { id: crypto.randomUUID(), key: "", value: "" };
}

/**
 * The rows that actually constrain the grid: a key and a non-blank value.
 * The value is trimmed of leading and trailing whitespace (a pasted value
 * routinely carries a stray space that would otherwise match nothing);
 * spaces inside it are kept. A half-filled row is ignored rather than
 * treated as "match everything" or "match nothing".
 */
export function activeHeaderCriteria(rows: HeaderFilterRow[]): HeaderCriterion[] {
  const criteria: HeaderCriterion[] = [];
  for (const row of rows) {
    const value = row.value.trim();
    if (row.key !== "" && value !== "") criteria.push({ key: row.key, value });
  }
  return criteria;
}

/** Every distinct header key across the given messages, sorted — the options the key dropdown offers for a topic. */
export function collectHeaderKeys(messages: TopicMessage[]): string[] {
  const keys = new Set<string>();
  for (const message of messages) {
    // `?? []`: this runs on every row of every render, and a row without the
    // field (the type says it is always there) must not take the whole grid down.
    for (const header of message.headers ?? []) keys.add(header.key);
  }
  return [...keys].sort();
}

/**
 * Client-side filter over the rows already fetched — Kafka has no
 * server-side header filtering. Every criterion must match (AND); one
 * matches when any header with that key decodes to exactly the value
 * (case-sensitive, whole value). Returns the same array when there is
 * nothing to filter on, so callers can rely on referential equality.
 */
export function filterByHeaders(messages: TopicMessage[], criteria: HeaderCriterion[]): TopicMessage[] {
  if (criteria.length === 0) return messages;
  return messages.filter((message) =>
    criteria.every(({ key, value }) =>
      message.headers.some((header) => header.key === key && base64ToDisplayText(header.valueBase64) === value),
    ),
  );
}
