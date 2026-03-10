import { DataPanel } from "@/components/data-panel";
import { MetricCard } from "@/components/metric-card";
import { RouteMonitorMatrix } from "@/components/route-monitor-matrix";
import {
  getRecentCycles,
  getRouteMonitorMatrixPayload,
  getRoutes,
  type RouteMonitorMatrixRoute
} from "@/lib/api";
import { buildReportingExportUrl } from "@/lib/export";
import { formatDhakaDateTime, formatMoney } from "@/lib/format";
import { buildHref, firstParam, manyParams, parseLimit, setParam, type RawSearchParams } from "@/lib/query";

type PageProps = {
  searchParams?: Promise<RawSearchParams>;
};

function uniqueByKey<T>(items: T[], keyFn: (item: T) => string) {
  const seen = new Set<string>();
  const unique: T[] = [];
  for (const item of items) {
    const key = keyFn(item);
    if (!key || seen.has(key)) {
      continue;
    }
    seen.add(key);
    unique.push(item);
  }
  return unique;
}

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

export default async function RoutesPage({ searchParams }: PageProps) {
  const params = (await searchParams) ?? {};
  const selectedAirlines = manyParams(params, "airline");
  const origin = firstParam(params, "origin");
  const destination = firstParam(params, "destination");
  const cabin = firstParam(params, "cabin");
  const tripType = firstParam(params, "trip_type") ?? "OW";
  const returnDate = firstParam(params, "return_date");
  const cycleId = firstParam(params, "cycle_id") ?? undefined;
  const routeLimit = parseLimit(firstParam(params, "route_limit"), 5);
  const historyLimit = parseLimit(firstParam(params, "history_limit"), 6);

  const [routes, recentCycles, matrix] = await Promise.all([
    getRoutes(),
    getRecentCycles(8),
    getRouteMonitorMatrixPayload({
      cycleId,
      origins: origin ? [origin] : undefined,
      destinations: destination ? [destination] : undefined,
      cabins: cabin ? [cabin] : undefined,
      tripTypes: tripType ? [tripType] : undefined,
      returnDate: returnDate ?? undefined,
      routeLimit,
      historyLimit
    })
  ]);

  const routeBlocks = matrix.data?.routes ?? [];
  const routePriorityBoard = buildRoutePriorityBoard(routeBlocks);
  const recentCycleOptions = uniqueByKey(recentCycles.data?.items ?? [], (item) => item.cycle_id ?? "");
  const routeOptions = uniqueByKey(routes.data?.items ?? [], (item) => item.route_key)
    .sort((left, right) => (right.offer_rows ?? 0) - (left.offer_rows ?? 0) || left.route_key.localeCompare(right.route_key))
    .slice(0, 16)
    .map((item) => ({ routeKey: item.route_key, origin: item.origin, destination: item.destination }));

  const availableAirlineCount = new Set(
    routeBlocks.flatMap((route) => route.flight_groups.map((flight) => flight.airline))
  ).size;
  const flightGroupCount = routeBlocks.reduce((sum, route) => sum + route.flight_groups.length, 0);
  const datedRowCount = routeBlocks.reduce((sum, route) => sum + route.date_groups.length, 0);
  const activeCycle = recentCycleOptions.find((item) => item.cycle_id === (matrix.data?.cycle_id ?? cycleId));
  const exportHref = buildReportingExportUrl(params, ["routes"]);

  return (
    <>
      <h1 className="page-title">Route Monitor</h1>
      <p className="page-copy">
        Report-style route matrix against the reporting API. Hosted reads now prefer the
        BigQuery warehouse path; airline, signal, and capture-history interaction stay in
        the page for workbook-like review without Excel.
      </p>

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
          footnote={`${availableAirlineCount.toLocaleString()} airlines in scope${tripType === "RT" ? " · round-trip" : ""}`}
        />
        <MetricCard
          label="Departure rows"
          value={datedRowCount.toLocaleString()}
          footnote={`History depth ${historyLimit.toLocaleString()}`}
        />
      </div>

      <div className="stack">
        <DataPanel
          title="Matrix scope"
          copy="Use route scope controls to load a tighter matrix from the API. Inside the matrix itself, airline and signal toggles behave like the workbook."
        >
          {recentCycleOptions.length ? (
            <div className="filter-group">
              <div className="filter-label">Comparable cycles</div>
              <div className="chip-row">
                {recentCycleOptions.map((item) => (
                  <a
                    className="chip"
                    data-active={cycleId === item.cycle_id}
                    href={buildHref(setParam(params, "cycle_id", item.cycle_id ?? undefined))}
                    key={item.cycle_id ?? "latest-cycle"}
                  >
                    {item.cycle_completed_at_utc ? formatDhakaDateTime(item.cycle_completed_at_utc) : "Latest"}
                  </a>
                ))}
              </div>
            </div>
          ) : null}
          <form className="filter-form" action="/routes">
            {cycleId ? <input name="cycle_id" type="hidden" value={cycleId} /> : null}
            <div className="field-grid route-scope-grid">
              <label className="field">
                <span>Origin</span>
                <input defaultValue={origin ?? ""} name="origin" placeholder="DAC" type="text" />
              </label>
              <label className="field">
                <span>Destination</span>
                <input defaultValue={destination ?? ""} name="destination" placeholder="CXB" type="text" />
              </label>
              <label className="field">
                <span>Cabin</span>
                <input defaultValue={cabin ?? ""} name="cabin" placeholder="Economy" type="text" />
              </label>
              <label className="field">
                <span>Trip type</span>
                <select defaultValue={tripType} name="trip_type">
                  <option value="OW">One-way</option>
                  <option value="RT">Round-trip</option>
                </select>
              </label>
              <label className="field">
                <span>Return date</span>
                <input defaultValue={returnDate ?? ""} name="return_date" type="date" />
              </label>
              <label className="field">
                <span>Route blocks</span>
                <input defaultValue={String(routeLimit)} inputMode="numeric" name="route_limit" pattern="[0-9]*" type="text" />
              </label>
              <label className="field">
                <span>History depth</span>
                <input defaultValue={String(historyLimit)} inputMode="numeric" name="history_limit" pattern="[0-9]*" type="text" />
              </label>
            </div>
            <div className="button-row">
              <button className="button-link" type="submit">
                Reload matrix
              </button>
              <a className="button-link ghost" href={exportHref}>
                Download Excel
              </a>
              <a className="button-link ghost" href="/routes">
                Reset scope
              </a>
            </div>
            {routeOptions.length ? (
              <div className="route-hint-row">
                {routeOptions.map((item) => (
                  <a
                    className="route-hint-chip"
                    href={`/routes?origin=${encodeURIComponent(item.origin)}&destination=${encodeURIComponent(item.destination)}&route_limit=${routeLimit}&history_limit=${historyLimit}${cabin ? `&cabin=${encodeURIComponent(cabin)}` : ""}${tripType ? `&trip_type=${encodeURIComponent(tripType)}` : ""}${returnDate ? `&return_date=${encodeURIComponent(returnDate)}` : ""}${cycleId ? `&cycle_id=${encodeURIComponent(cycleId)}` : ""}`}
                    key={item.routeKey}
                  >
                    {item.routeKey}
                  </a>
                ))}
              </div>
            ) : null}
          </form>
        </DataPanel>

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
                        ? ` · Cheapest ${item.cheapest.airline}${item.cheapest.flightNumber} @ ${formatMoney(item.cheapest.amount, "BDT")}`
                        : " · No fare leader"}
                    </span>
                  </div>
                  <div className={`pill ${item.statusLabel === "Stable" ? "good" : "warn"}`}>{item.inventoryLabel}</div>
                  <span>
                    {item.strongestMove
                      ? `Move ${item.strongestMove.delta > 0 ? "+" : ""}${formatMoney(item.strongestMove.delta, "BDT")} · ${item.strongestMove.departureDate}`
                      : "No capture-to-capture move"}
                    {item.taxSpread != null ? ` · Tax spread ${formatMoney(item.taxSpread, "BDT")}` : ""}
                    {item.latestCaptureAt ? ` · Fresh ${formatDhakaDateTime(item.latestCaptureAt)}` : ""}
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
            <RouteMonitorMatrix initialAirlines={selectedAirlines} payload={matrix.data!} />
          )}
        </DataPanel>
      </div>
    </>
  );
}
