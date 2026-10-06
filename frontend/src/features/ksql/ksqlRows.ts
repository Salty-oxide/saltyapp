import type { ColDef } from "ag-grid-community";
import { KsqlColumn, KsqlRow } from "../../lib/tauri";

/**
 * How many result rows the grid keeps.
 *
 * A push query is unbounded by definition — `EMIT CHANGES` on a busy topic
 * streams until it is stopped — so without a ceiling this is a memory leak
 * with a Run button. The newest rows are the ones worth keeping, so the
 * buffer drops from the front.
 *
 * A fixed constant rather than a setting: nothing yet suggests the number
 * needs tuning per user, and a Settings field nobody changes is a field that
 * still has to be explained.
 */
export const MAX_RESULT_ROWS = 10_000;

/** One grid row: the values, plus the arrival index that identifies it. */
export interface KsqlGridRow {
  /**
   * Position in the stream.
   *
   * A ksql row has no natural key — unlike a fetched message, which has a
   * partition and an offset — so arrival order is the only stable identity
   * available, and it is what `getRowId` uses.
   */
  index: number;
  values: KsqlRow;
}

/**
 * ksqlDB types that should behave as numbers in the grid.
 *
 * Worth the mapping: a numeric column gets a numeric filter and right
 * alignment, where a string column gets neither. Anything unrecognised stays
 * a string, which renders correctly even when the comparison is lexical.
 */
const NUMERIC_TYPES = new Set(["INT", "INTEGER", "BIGINT", "DOUBLE", "DECIMAL"]);

function isNumeric(kind: string): boolean {
  // ksqlDB spells parameterised types like `DECIMAL(4,2)`, so the base name is
  // what identifies them.
  const base = kind.split("(")[0].trim().toUpperCase();
  return NUMERIC_TYPES.has(base);
}

/**
 * Builds the grid's columns from the header frame ksqlDB sent.
 *
 * Dynamic, unlike the Data tab's fixed `COLUMN_DEFS`: a query's shape is not
 * known until it runs. The column's position in the header is what ties it to
 * a position in each row, so `valueGetter` indexes rather than naming a field.
 */
export function columnDefsFor(columns: KsqlColumn[]): ColDef<KsqlGridRow>[] {
  return columns.map((column, index) => ({
    headerName: column.name,
    colId: `${index}:${column.name}`,
    // Tooltipped with the ksqlDB type, which is otherwise invisible once the
    // header is drawn.
    headerTooltip: `${column.name} — ${column.kind}`,
    valueGetter: (params) => {
      const value = params.data?.values[index];
      // `null` is a value in a result, and rendering it as an empty cell would
      // make it indistinguishable from an empty string.
      if (value === null) return "null";
      if (value === undefined) return "";
      return typeof value === "object" ? JSON.stringify(value) : value;
    },
    filter: isNumeric(column.kind) ? "agNumberColumnFilter" : "agTextColumnFilter",
    type: isNumeric(column.kind) ? "numericColumn" : undefined,
    sortable: true,
    resizable: true,
    minWidth: 120,
    flex: 1,
  }));
}

/**
 * Appends rows, keeping at most [`MAX_RESULT_ROWS`] of the newest.
 *
 * Returns a new array rather than mutating, so the grid sees a changed
 * reference and re-renders. `nextIndex` is the arrival counter, which keeps
 * increasing across appends — a row's identity must not change because older
 * rows were dropped ahead of it.
 */
export function appendRows(
  existing: KsqlGridRow[],
  incoming: KsqlRow[],
  nextIndex: number,
): { rows: KsqlGridRow[]; nextIndex: number } {
  if (incoming.length === 0) return { rows: existing, nextIndex };

  const added = incoming.map((values, offset) => ({ index: nextIndex + offset, values }));
  const combined = existing.concat(added);
  const rows = combined.length > MAX_RESULT_ROWS ? combined.slice(combined.length - MAX_RESULT_ROWS) : combined;

  return { rows, nextIndex: nextIndex + incoming.length };
}

/**
 * What the toolbar says about a run.
 *
 * `received` is every row the query produced, which is not `rows.length` once
 * the cap has started dropping — saying "10,000 rows" when 143,902 arrived
 * would be a quiet lie about what the query did.
 */
export function resultSummary(
  shown: number,
  received: number,
  status: "idle" | "running" | "stopped" | "done" | "error",
): string {
  if (status === "idle") return "";
  const capped = received > shown;
  const rows = capped
    ? `showing last ${shown.toLocaleString()} of ${received.toLocaleString()} received`
    : `${shown.toLocaleString()} ${shown === 1 ? "row" : "rows"}`;

  switch (status) {
    case "running":
      return `${rows} · live`;
    case "stopped":
      return `${rows} · stopped`;
    case "done":
      return `${rows} · finished`;
    case "error":
      return rows;
  }
}

/**
 * The query a topic's tab opens with.
 *
 * A live tail rather than a bounded read, because ksqlDB cannot express "the
 * most recent N rows" — a push query runs *forward* from an offset, so a
 * LIMIT from the earliest offset returns the oldest rows, not the newest.
 * Tailing is the thing ksqlDB is actually good at, and it is what makes the
 * Run/Stop pair mean something.
 */
export function defaultTopicQuery(stream: string): string {
  return `SELECT * FROM ${stream} EMIT CHANGES;`;
}

/** The statement that registers a stream over a topic whose schema the registry knows. */
export function createStreamStatement(topic: string, stream: string, valueFormat: string): string {
  return `CREATE STREAM ${stream} WITH (KAFKA_TOPIC='${topic}', VALUE_FORMAT='${valueFormat}');`;
}

/**
 * A stream name derived from a topic name.
 *
 * ksqlDB identifiers are upper-cased and cannot contain a hyphen, which topic
 * names routinely do.
 */
export function suggestedStreamName(topic: string): string {
  const cleaned = topic.replace(/[^A-Za-z0-9_]/g, "_").toUpperCase();
  // An identifier cannot start with a digit.
  return /^[0-9]/.test(cleaned) ? `S_${cleaned}` : cleaned;
}
