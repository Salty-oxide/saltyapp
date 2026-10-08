import { Dropdown } from "../../components/Dropdown";
import {
  activeHeaderCriteria,
  emptyHeaderRow,
  HeaderFilterRow,
} from "./headerFilters";

export interface HeaderFilterPanelProps {
  rows: HeaderFilterRow[];
  /** Header keys seen in the topic's loaded messages. */
  availableKeys: string[];
  /** How many criteria the grid is currently filtered by. */
  appliedCount: number;
  onChange: (rows: HeaderFilterRow[]) => void;
  /** Apply these rows as the grid's filter. Passed explicitly so a just-cleared row is applied without waiting for the caller's state to catch up. */
  onApply: (rows: HeaderFilterRow[]) => void;
}

const NO_KEY = "";

function Icon({ d }: { d: string }) {
  return (
    <svg
      width="16"
      height="16"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d={d} />
    </svg>
  );
}

/**
 * Key/value rows that narrow the grid to messages carrying matching headers.
 * Presentational: the rows live in the caller, which also owns what Filter
 * does with them. The key is picked from the headers actually present in the
 * loaded messages; the value is free text, trimmed when applied.
 */
export function HeaderFilterPanel({
  rows,
  availableKeys,
  appliedCount,
  onChange,
  onApply,
}: HeaderFilterPanelProps) {
  const canApply = activeHeaderCriteria(rows).length > 0;
  // Stays enabled while a filter is applied so emptying the rows and pressing
  // Filter can drop it (applying no criteria shows every message again).
  const canFilter = canApply || appliedCount > 0;

  // Each row needs its own key (a key is offered in one row only), so more
  // rows than distinct keys could never all be filled in.
  const canAddRow = rows.length < availableKeys.length;

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
        const takenElsewhere = new Set(
          rows.filter((r) => r.id !== row.id && r.key !== "").map((r) => r.key),
        );
        const offered = availableKeys.filter((key) => !takenElsewhere.has(key));
        const keys =
          row.key !== "" && !availableKeys.includes(row.key)
            ? [...offered, row.key]
            : offered;
        return (
          <div className="header-filter-row" key={row.id}>
            <Dropdown
              label="Header key"
              hideLabel
              ariaLabel={`Header key ${n}`}
              options={[
                { id: NO_KEY, label: "Select key" },
                ...keys.map((key) => ({ id: key, label: key })),
              ]}
              displayedId={row.key}
              appliedId={row.key}
              onCommit={(key) => updateRow(row.id, { key })}
            />
            <input
              aria-label={`Header value ${n}`}
              className="header-filter-value"
              value={row.value}
              onChange={(e) => updateRow(row.id, { value: e.target.value })}
              onKeyDown={(e) => {
                if (e.key === "Enter" && canFilter) onApply(rows);
              }}
              placeholder="Value"
            />
            <div className="header-filter-icons">
              <button
                type="button"
                className="header-filter-icon"
                aria-label={`Clear header ${n}`}
                title="Clear"
                onClick={() => {
                  const cleared = rows.map((r) =>
                    r.id === row.id ? { ...r, key: "", value: "" } : r,
                  );
                  onChange(cleared);
                  onApply(cleared);
                }}
              >
                <Icon d="M6 6l12 12M18 6L6 18" />
              </button>
              {/*
                With one row there is nothing to line up with, so Clear and Add
                sit side by side. From two rows on, every row keeps three
                columns — Clear, Delete, Add — and the first row's Delete and
                every non-last row's Add hold their column open as an empty
                slot so the icons stay in line down the panel.
              */}
              {index > 0 ? (
                <button
                  type="button"
                  className="header-filter-icon header-filter-icon--danger"
                  aria-label={`Delete header ${n}`}
                  title="Delete"
                  onClick={() => onChange(rows.filter((r) => r.id !== row.id))}
                >
                  <Icon d="M4 7h16M10 11v6M14 11v6M6 7l1 12a1 1 0 001 1h8a1 1 0 001-1l1-12M9 7V4h6v3" />
                </button>
              ) : (
                rows.length > 1 && <span className="header-filter-icon-slot" />
              )}
              {index === rows.length - 1 ? (
                <button
                  type="button"
                  className="header-filter-icon header-filter-icon--add"
                  aria-label="Add header"
                  title={
                    canAddRow
                      ? "Add header"
                      : "Every available header key already has a row"
                  }
                  disabled={!canAddRow}
                  onClick={() => onChange([...rows, emptyHeaderRow()])}
                >
                  <Icon d="M12 5v14M5 12h14" />
                </button>
              ) : (
                <span className="header-filter-icon-slot" />
              )}
            </div>
          </div>
        );
      })}
      <div className="header-filter-actions">
        <button
          type="button"
          className="header-filter-submit"
          onClick={() => onApply(rows)}
          disabled={!canFilter}
        >
          Filter
        </button>
      </div>
    </div>
  );
}
