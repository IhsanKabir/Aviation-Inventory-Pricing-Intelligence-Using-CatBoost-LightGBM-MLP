import { DataPanel } from "@/components/data-panel";
import { MetricCard } from "@/components/metric-card";
import { RouteMonitorMatrix } from "@/components/route-monitor-matrix";
import {
  getRouteMonitorMatrixPayload,
  type CycleSummary,
  type RouteMonitorMatrixRoute
} from "@/lib/api";
import { formatDhakaDateTime, formatMoney } from "@/lib/format";

function buildRoutePriorityBoard(routes: RouteMonitorMatrixRoute[]) {
  return routes
    .map((route) => {
      let cheapest:
        | {
            airline: string;
            flightNumber: string;
            amount: number;
          }
        | undefined;
      let strongestMove:
        | {
            delta: number;
            airline: string;
            flightNumber: string;
            departureDate: string;
          }
        | undefined;
      let minTax: number | null = null;
      let maxTax: number | null = null;
      let soldOutCount = 0;
      let lowSeatCount = 0;
      let highLoadCount = 0;
      let latestCaptureAt: string | null = null;

      const flightById = new Map(route.flight_groups.map((flight) => [flight.flight_group_id, flight]));

      for (const dateGroup of route.date_groups) {
        const latestCapture = dateGroup.captures[0];
        if (!latestCapture) {
          continue;
        }
        const captureTimestamp = latestCapture.captured_at_utc ?? null;
        if (captureTimestamp && (!latestCaptureAt || captureTimestamp > latestCaptureAt)) {
          latestCaptureAt = captureTimestamp;
        }

        for (const cell of latestCapture.cells) {
          const flight = flightById.get(cell.flight_group_id);
          if (!flight) {
            continue;
          }
          if (cell.min_total_price_bdt != null && (!cheapest || Number(cell.min_total_price_bdt) < cheapest.amount)) {
            cheapest = {
              airline: flight.airline,
              flightNumber: flight.flight_number,
              amount: Number(cell.min_total_price_bdt)
            };
          }
          if (cell.tax_amount != null) {
            const tax = Number(cell.tax_amount);
            minTax = minTax === null ? tax : Math.min(minTax, tax);
            maxTax = maxTax === null ? tax : Math.max(maxTax, tax);
          }
          if (cell.soldout) {
            soldOutCount += 1;
          }
          if (cell.seat_available != null && Number(cell.seat_available) <= 5) {
            lowSeatCount += 1;
          }
          if (cell.load_factor_pct != null && Number(cell.load_factor_pct) >= 85) {
            highLoadCount += 1;
          }
        }

        const previousCapture = dateGroup.captures[1];
        if (!previousCapture) {
          continue;
        }
        const previousByFlight = new Map(previousCapture.cells.map((cell) => [cell.flight_group_id, cell]));
        for (const cell of latestCapture.cells) {
          const previous = previousByFlight.get(cell.flight_group_id);
          if (!previous || cell.min_total_price_bdt == null || previous.min_total_price_bdt == null) {
            continue;
          }
          const delta = Number(cell.min_total_price_bdt) - Number(previous.min_total_price_bdt);
          if (!strongestMove || Math.abs(delta) > Math.abs(strongestMove.delta)) {
            const flight = flightById.get(cell.flight_group_id);
            if (!flight) {
              continue;
            }
            strongestMove = {
              delta,
              airline: flight.airline,
              flightNumber: flight.flight_number,
              departureDate: dateGroup.departure_date
            };
          }
        }
      }

      const taxSpread = minTax !== null && maxTax !== null ? maxTax - minTax : null;
      const inventoryLabel =
        soldOutCount > 0
          ? "Sold-out pressure"
          : highLoadCount > 0 || lowSeatCount > 0
            ? "Tight inventory"
            : minTax !== null || latestCaptureAt
              ? "Stable inventory"
              : "No inventory signal";
      const priorityScore =
        Math.abs(strongestMove?.delta ?? 0) +
        (taxSpread ?? 0) +
        soldOutCount * 2000 +
        highLoadCount * 600 +
        lowSeatCount * 300;
      const statusLabel =
        soldOutCount > 0 || Math.abs(strongestMove?.delta ?? 0) >= 1500
          ? "Watch closely"
          : highLoadCount > 0 || (taxSpread ?? 0) >= 500
            ? "Needs review"
            : "Stable";

      return {
        routeKey: route.route_key,
        cheapest,
        strongestMove,
        taxSpread,
        inventoryLabel,
        latestCaptureAt,
        priorityScore,
        statusLabel
      };
    })
    .sort((left, right) => right.priorityScore - left.priorityScore || left.routeKey.localeCompare(right.routeKey))
    .slice(0, 8);
}

export function RouteMonitorSectionFallback() {
  return (
    <>
      <div className="grid cards">
        <MetricCard label="Cycle" value="Loading..." footnote="Resolving route matrix scope" />
        <MetricCard label="Route blocks" value="..." footnote="Preparing comparable routes" />
        <MetricCard label="Flight groups" value="..." footnote="Collecting visible airline blocks" />
        <MetricCard label="Departure rows" value="..." footnote="Loading capture history" />
      </div>

      <DataPanel
        title="Route flight fare monitor"
        copy="Latest captures are shown first. Use the capture column to expand older observations for the same departure date."
      >
        <div className="empty-state">Loading route matrix for the selected scope...</div>
      </DataPanel>
    </>
  );
}

export async function RouteMonitorSection({
  requestId,
  cycleId,
  selectedAirlines,
  origin,
  destination,
  cabin,
  tripType,
  returnDate,
  returnDateStart,
  returnDateEnd,
  routeLimit,
  historyLimit,
  recentCycles
}: {
  requestId: string;
  cycleId?: string;
  selectedAirlines: string[];
  origin?: string;
  destination?: string;
  cabin?: string;
  tripType: string;
  returnDate?: string;
  returnDateStart?: string;
  returnDateEnd?: string;
  routeLimit: number;
  historyLimit: number;
  recentCycles: CycleSummary[];
}) {
  const matrix = await getRouteMonitorMatrixPayload({
    requestId,
    cycleId,
    airlines: selectedAirlines.length ? selectedAirlines : undefined,
    origins: origin ? [origin] : undefined,
    destinations: destination ? [destination] : undefined,
    cabins: cabin ? [cabin] : undefined,
    tripTypes: tripType ? [tripType] : undefined,
    returnDate,
    returnDateStart,
    returnDateEnd,
    routeLimit,
    historyLimit,
    compactHistory: false
  });

  const routeBlocks = matrix.data?.routes ?? [];
  const routePriorityBoard = buildRoutePriorityBoard(routeBlocks);
  const availableAirlineCount = new Set(
    routeBlocks.flatMap((route) => route.flight_groups.map((flight) => flight.airline))
  ).size;
  const flightGroupCount = routeBlocks.reduce((sum, route) => sum + route.flight_groups.length, 0);
  const datedRowCount = routeBlocks.reduce((sum, route) => sum + route.date_groups.length, 0);
  const activeCycle = recentCycles.find((item) => item.cycle_id === (matrix.data?.cycle_id ?? cycleId));

  return (
    <>
      <div className="grid cards">
        <MetricCard
          label="Cycle"
          value={activeCycle?.cycle_completed_at_utc ? formatDhakaDateTime(activeCycle.cycle_completed_at_utc) : "Not available"}
          footnote={matrix.ok ? "Latest warehouse-backed route matrix" : "No cycle loaded"}
        />
        <MetricCard label="Route blocks" value={routeBlocks.length.toLocaleString()} footnote={`Limit ${routeLimit.toLocaleString()}`} />
        <MetricCard
          label="Flight groups"
          value={flightGroupCount.toLocaleString()}
          footnote={`${availableAirlineCount.toLocaleString()} airlines in scope${tripType === "RT" ? " | round-trip" : ""}`}
        />
        <MetricCard
          label="Departure rows"
          value={datedRowCount.toLocaleString()}
          footnote={`History depth ${historyLimit.toLocaleString()}`}
        />
      </div>

      <DataPanel
        title="Route flight fare monitor"
        copy="Latest captures are shown first. Use the capture column to expand older observations for the same departure date."
      >
        {matrix.ok && routePriorityBoard.length ? (
          <div className="table-list compact-list">
            {routePriorityBoard.map((item) => (
              <div className="table-row" key={item.routeKey}>
                <div>
                  <strong>{item.routeKey}</strong>
                  <span>
                    {item.statusLabel}
                    {item.cheapest
                      ? ` | Cheapest ${item.cheapest.airline}${item.cheapest.flightNumber} @ ${formatMoney(item.cheapest.amount, "BDT")}`
                      : " | No fare leader"}
                  </span>
                </div>
                <div className={`pill ${item.statusLabel === "Stable" ? "good" : "warn"}`}>{item.inventoryLabel}</div>
                <span>
                  {item.strongestMove
                    ? `Move ${item.strongestMove.delta > 0 ? "+" : ""}${formatMoney(item.strongestMove.delta, "BDT")} | ${item.strongestMove.departureDate}`
                    : "No capture-to-capture move"}
                  {item.taxSpread != null ? ` | Tax spread ${formatMoney(item.taxSpread, "BDT")}` : ""}
                  {item.latestCaptureAt ? ` | Fresh ${formatDhakaDateTime(item.latestCaptureAt)}` : ""}
                </span>
              </div>
            ))}
          </div>
        ) : null}
        {!matrix.ok ? (
          <div className="empty-state error-state">API error: {matrix.error ?? "Unable to load route monitor matrix."}</div>
        ) : routeBlocks.length === 0 ? (
          <div className="empty-state">No route blocks matched the current scope.</div>
        ) : (
          <RouteMonitorMatrix
            initialAirlines={selectedAirlines}
            payload={matrix.data!}
            scopeQuery={{
              requestId,
              cycleId,
              airlines: selectedAirlines,
              origin,
              destination,
              cabin,
              tripType,
              returnDate,
              returnDateStart,
              returnDateEnd,
              historyLimit
            }}
          />
        )}
      </DataPanel>
    </>
  );
}
