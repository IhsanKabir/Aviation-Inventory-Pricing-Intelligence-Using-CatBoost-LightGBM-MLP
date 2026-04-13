import { DataPanel } from "@/components/data-panel";
import type { SnapshotRow } from "@/lib/api";
import { formatDhakaDateTime, formatMoney } from "@/lib/format";

function snapshotWindowLabel(rows: SnapshotRow[]) {
  const departures = rows
    .map((row) => row.departure_utc)
    .filter((value): value is string => Boolean(value))
    .sort();
  if (!departures.length) {
    return "Departure times not available";
  }
  return `${formatDhakaDateTime(departures[0])} to ${formatDhakaDateTime(departures[departures.length - 1])}`;
}

export function RouteSnapshotSection({
  rows,
  cycleId
}: {
  rows: SnapshotRow[];
  cycleId?: string | null;
}) {
  const visibleRows = rows.slice(0, 20);
  const airlineCount = new Set(visibleRows.map((row) => row.airline).filter(Boolean)).size;
  const routeCount = new Set(visibleRows.map((row) => row.route_key).filter(Boolean)).size;

  return (
    <DataPanel
      title="Latest published route snapshot"
      copy="This is the last published route state, cached for lightweight viewing. Live fare history and comparisons still require approval."
    >
      {!visibleRows.length ? (
        <div className="empty-state">No snapshot rows matched the current route filters.</div>
      ) : (
        <div className="stack">
          <div className="table-list compact-list">
            <div className="table-row">
              <div>
                <strong>{cycleId || "Latest cycle"}</strong>
                <span>{snapshotWindowLabel(visibleRows)}</span>
              </div>
              <div className="pill good">{routeCount.toLocaleString()} routes</div>
              <span>{airlineCount.toLocaleString()} airlines</span>
            </div>
          </div>

          <div className="data-table-wrap">
            <table className="data-table compact-table">
              <thead>
                <tr>
                  <th>Route</th>
                  <th>Airline</th>
                  <th>Flight</th>
                  <th>Departure</th>
                  <th>Cabin</th>
                  <th>Total fare</th>
                  <th>Tax</th>
                  <th>Seats</th>
                </tr>
              </thead>
              <tbody>
                {visibleRows.map((row, index) => (
                  <tr key={`${row.route_key}-${row.airline}-${row.flight_number}-${row.departure_utc ?? index}`}>
                    <td>{row.route_key}</td>
                    <td>{row.airline}</td>
                    <td>{row.flight_number}</td>
                    <td>{formatDhakaDateTime(row.departure_utc)}</td>
                    <td>{row.cabin ?? "-"}</td>
                    <td>{formatMoney(row.total_price_bdt, "BDT")}</td>
                    <td>{formatMoney(row.tax_amount, row.currency ?? "BDT")}</td>
                    <td>{row.seat_available ?? "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </DataPanel>
  );
}
