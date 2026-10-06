import { useMemo } from "react";
import { AgGridReact } from "ag-grid-react";
import { AllCommunityModule, ColDef, ModuleRegistry } from "ag-grid-community";
import { APP_GRID_THEME } from "../connections/agGridTheme";
import { KsqlColumn } from "../../lib/tauri";
import { columnDefsFor, KsqlGridRow } from "./ksqlRows";

ModuleRegistry.registerModules([AllCommunityModule]);

export interface KsqlResultsGridProps {
  columns: KsqlColumn[];
  rows: KsqlGridRow[];
}

const DEFAULT_COL_DEF: ColDef<KsqlGridRow> = {
  sortable: true,
  resizable: true,
  filter: true,
};

/**
 * A ksqlDB result, in the same grid the Data tab uses.
 *
 * The one structural difference is that the columns are **dynamic**: a
 * query's shape is not known until it runs, so they are rebuilt from the
 * header frame rather than declared up front. Everything else — the theme,
 * the density, the module registration — is what every other grid in the app
 * already uses.
 *
 * This module is reached through `features/connections/gridTabs.tsx`, never
 * imported directly. AG Grid is two thirds of the frontend bundle and is
 * deliberately kept out of the initial chunk.
 */
export function KsqlResultsGrid({ columns, rows }: KsqlResultsGridProps) {
  // Rebuilt only when the shape changes. AG Grid resets column state whenever
  // `columnDefs` changes identity, so handing it a fresh array on every batch
  // of rows would discard the user's sort and column widths a few times a
  // second on a live tail.
  const columnDefs = useMemo(() => columnDefsFor(columns), [columns]);

  if (columns.length === 0) return null;

  return (
    <div className="ksql-results-grid" data-testid="ksql-results-grid">
      <AgGridReact<KsqlGridRow>
        theme={APP_GRID_THEME}
        rowData={rows}
        columnDefs={columnDefs}
        defaultColDef={DEFAULT_COL_DEF}
        // Arrival order is the only stable identity a ksql row has — unlike a
        // fetched message, which has a partition and an offset.
        getRowId={(params) => String(params.data.index)}
      />
    </div>
  );
}
