import { Dropdown } from "../../components/Dropdown";
import { activeHeaderCriteria, emptyHeaderRow, HeaderFilterRow } from "./headerFilters";

export interface HeaderFilterPanelProps {
  rows: HeaderFilterRow[];
  /** Header keys seen in the topic's loaded messages. */
  availableKeys: string[];
  /** How many criteria the grid is currently filtered by. */
  appliedCount: number;
  onChange: (rows: HeaderFilterRow[]) => void;
  onApply: () => void;
  onClear: () => void;
}

const NO_KEY = "";

/**
 * Key/value rows that narrow the grid to messages carrying matching headers.
 * Presentational: the rows live in the caller, which also owns what Filter
 * does with them. The key is picked from the headers actually present in the
 * loaded messages; the value is free text, trimmed when applied.
 */
export function HeaderFilterPanel({ rows, availableKeys, appliedCount, onChange, onApply, onClear }: HeaderFilterPanelProps) {
  const canApply = activeHeaderCriteria(rows).length > 0;
  const canClear = appliedCount > 0 || rows.length > 1 || rows.some((r) => r.key !== "" || r.value !== "");

  function updateRow(id: string, patch: Partial<HeaderFilterRow>) {
    onChange(rows.map((row) => (row.id === id ? { ...row, ...patch } : row)));
  }

  return (
    <div className="header-filter" role="group" aria-label="Header filter">
      {rows.map((row, index) => {
        const n = index + 1;
        // A key that is no longer among the loaded headers (after a re-fetch)
        // stays selectable as its own option rather than silently showing
        // "Select key" for a row that is still filtering on it.
        const keys = row.key !== "" && !availableKeys.includes(row.key) ? [...availableKeys, row.key] : availableKeys;
        return (
          <div className="header-filter-row" key={row.id}>
            <Dropdown
              label="Header key"
              ariaLabel={`Header key ${n}`}
              options={[{ id: NO_KEY, label: "Select key" }, ...keys.map((key) => ({ id: key, label: key }))]}
              displayedId={row.key}
              appliedId={row.key}
              onCommit={(key) => updateRow(row.id, { key })}
            />
            <label>
              Header value
              <input
                aria-label={`Header value ${n}`}
                value={row.value}
                onChange={(e) => updateRow(row.id, { value: e.target.value })}
                onKeyDown={(e) => {
                  if (e.key === "Enter" && canApply) onApply();
                }}
                placeholder="Value"
              />
            </label>
            <button
              type="button"
              className="header-filter-remove"
              aria-label={`Remove header ${n}`}
              onClick={() => onChange(rows.filter((r) => r.id !== row.id))}
              disabled={rows.length === 1}
            >
              ✕
            </button>
          </div>
        );
      })}
      <div className="header-filter-actions">
        <button type="button" onClick={() => onChange([...rows, emptyHeaderRow()])}>
          Add header
        </button>
        <button type="button" onClick={onApply} disabled={!canApply}>
          Filter
        </button>
        <button type="button" onClick={onClear} disabled={!canClear}>
          Clear
        </button>
      </div>
    </div>
  );
}
