import { LiveFilterControls } from "@/components/live-filter-controls";
import { DataPanel } from "@/components/data-panel";
import { MetricCard } from "@/components/metric-card";
import { getAirlines, getRecentCycles, getRoutes, getTaxPayload } from "@/lib/api";
import { buildReportingExportUrl } from "@/lib/export";
import { formatDhakaDateTime, formatMoney, formatNumber, formatRouteGeo, formatRouteType } from "@/lib/format";
import { firstParam, manyParams, parseLimit, type RawSearchParams } from "@/lib/query";

type PageProps = {
  searchParams?: Promise<RawSearchParams>;
};

function selectedRouteKey(origin?: string, destination?: string) {
  if (!origin || !destination) {
    return undefined;
  }
  return `${origin}-${destination}`;
}

function toNumber(value: unknown) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function formatDelta(value?: number | null) {
  if (value === null || value === undefined) {
    return "-";
  }
  const prefix = value > 0 ? "+" : "";
  return `${prefix}${formatNumber(value)}`;
}

function renderTrendStrip(timeline?: Array<Record<string, unknown>>) {
  if (!timeline?.length) {
    return "-";
  }
  return timeline
    .slice(-4)
    .map((item) => formatMoney(toNumber(item.avg_tax_amount), "BDT").replace("BDT ", ""))
    .join(" -> ");
}

export default async function TaxesPage({ searchParams }: PageProps) {
  const params = (await searchParams) ?? {};

  const load = firstParam(params, "load");
  if (!load) {
    return (
      <>
        <h1 className="page-title">Tax Monitor</h1>
        <p className="page-copy">Departure tax and fuel surcharge breakdown by route and airline.</p>
        <div className="market-gate">
          <p className="market-gate-title">Live market data — load on demand</p>
          <p className="market-gate-copy">This view queries live data on each load. Click below only when you need current data.</p>
          <a className="button-link" href="?load=1">Load Data</a>
        </div>
      </>
    );
  }

  const selectedAirlines = manyParams(params, "airline");
  const selectedRouteTypes = manyParams(params, "route_type");
  const origin = firstParam(params, "origin");
  const destination = firstParam(params, "destination");
  const cycleId = firstParam(params, "cycle_id") ?? undefined;
  const limit = parseLimit(firstParam(params, "limit"), 120);
  const trendLimit = parseLimit(firstParam(params, "trend_limit"), 8);
  const routeKey = selectedRouteKey(origin, destination);

  const [airlines, routes, recentCycles, taxes] = await Promise.all([
    getAirlines(),
    getRoutes(),
    getRecentCycles(8),
    getTaxPayload({
      cycleId,
      airlines: selectedAirlines,
      origins: origin ? [origin] : undefined,
      destinations: destination ? [destination] : undefined,
      routeTypes: selectedRouteTypes,
      limit,
      trendLimit,
    }),
  ]);

  const rows = taxes.data?.rows ?? [];
  const routeSummaries = taxes.data?.route_summaries ?? [];
  const airlineSummaries = taxes.data?.airline_summaries ?? [];
  const airlineOptions = [...(airlines.data?.items ?? [])]
    .sort((left, right) => (right.offer_rows ?? 0) - (left.offer_rows ?? 0) || left.airline.localeCompare(right.airline))
    .slice(0, 20)
    .map((item) => item.airline);
  const routeOptions = [...(routes.data?.items ?? [])]
    .sort((left, right) => (right.offer_rows ?? 0) - (left.offer_rows ?? 0) || left.route_key.localeCompare(right.route_key))
    .slice(0, 16)
    .map((item) => ({ routeKey: item.route_key, origin: item.origin, destination: item.destination }));
  const cycleOptions = (recentCycles.data?.items ?? [])
    .filter((item) => item.cycle_id)
    .map((item) => ({
      label: item.cycle_completed_at_utc ? formatDhakaDateTime(item.cycle_completed_at_utc) : "Latest",
      value: item.cycle_id as string,
    }));
  const activeCycle = (recentCycles.data?.items ?? []).find((item) => item.cycle_id === (taxes.data?.cycle_id ?? cycleId));

  const routeCount = routeSummaries.length;
  const airlineCount = new Set(rows.map((row) => row.airline)).size;
  const maxTax = routeSummaries.reduce((current, row) => Math.max(current, toNumber(row.max_tax_amount) ?? 0), 0);
  const minTax =
    routeSummaries.length > 0
      ? routeSummaries.reduce((current, row) => Math.min(current, toNumber(row.min_tax_amount) ?? current), toNumber(routeSummaries[0].min_tax_amount) ?? 0)
      : 0;
  const widestSpread = routeSummaries.reduce((current, row) => Math.max(current, toNumber(row.spread_amount) ?? 0), 0);
  const largestShift = airlineSummaries.reduce((current, row) => Math.max(current, Math.abs(toNumber(row.avg_tax_change_amount) ?? 0)), 0);
  const topSpreadRoutes = routeSummaries.slice(0, 6);
  const topAirlineMoves = airlineSummaries.slice(0, 8);
  const exportHref = buildReportingExportUrl(params, ["taxes"]);

  return (
    <>
      <h1 className="page-title">Tax Monitor</h1>
      <p className="page-copy">
        Tax comparison and movement review by route and airline. The page surfaces spread and recent direction before
        dropping into row-level verification.
      </p>

      <div className="grid cards">
        <MetricCard
          label="Cycle"
          value={activeCycle?.cycle_completed_at_utc ? formatDhakaDateTime(activeCycle.cycle_completed_at_utc) : "Not available"}
          footnote={taxes.ok ? "Latest warehouse-backed tax slice" : "No cycle loaded"}
        />
        <MetricCard label="Tax rows" value={rows.length.toLocaleString()} footnote={`Detail limit ${limit.toLocaleString()}`} />
        <MetricCard
          label="Tax range"
          value={`${formatMoney(minTax, "BDT")} to ${formatMoney(maxTax, "BDT")}`}
          footnote={`${routeCount.toLocaleString()} routes | ${airlineCount.toLocaleString()} airlines`}
        />
        <MetricCard
          label="Monitor signal"
          value={formatMoney(widestSpread, "BDT")}
          footnote={`Largest spread | biggest shift ${formatMoney(largestShift, "BDT")}`}
        />
      </div>

      <div className="stack">
        <DataPanel
          title="Tax filters"
          copy="Use route, airline, cycle, and route-type filters to narrow the monitor. The summary layer and detail table share the same scope."
        >
          <LiveFilterControls
            airlineOptions={airlineOptions}
            clearKeys={["airline", "origin", "destination", "route_type", "cycle_id", "limit", "trend_limit"]}
            extraGroups={[
              ...(cycleOptions.length
                ? [
                    {
                      key: "cycle_id",
                      label: "Comparable cycles",
                      selected: cycleId ? [cycleId] : [],
                      options: cycleOptions,
                      multi: false,
                    },
                  ]
                : []),
              {
                key: "route_type",
                label: "Route type",
                selected: selectedRouteTypes,
                options: [
                  { label: "Domestic", value: "DOM" },
                  { label: "International", value: "INT" },
                  { label: "Unknown", value: "UNK" },
                ],
              },
            ]}
            initialValues={{
              origin: origin ?? "",
              destination: destination ?? "",
              limit: String(limit),
              trend_limit: String(trendLimit),
            }}
            manualFields={[
              { name: "origin", label: "Origin", placeholder: "DAC" },
              { name: "destination", label: "Destination", placeholder: "DOH" },
              { name: "limit", label: "Row limit", inputMode: "numeric", pattern: "[0-9]*" },
              { name: "trend_limit", label: "Trend cycles", inputMode: "numeric", pattern: "[0-9]*" },
            ]}
            routeOptions={routeOptions}
            selectedAirlines={selectedAirlines}
            selectedRouteKey={routeKey}
          />

          <div className="button-row">
            <a className="button-link ghost" href={exportHref}>
              Download Excel
            </a>
          </div>
        </DataPanel>

        <div className="section-grid">
          <DataPanel
            title="Route spread"
            copy={routeKey ? `Largest tax spreads within ${routeKey}.` : "Largest route-level tax spreads in the current filtered scope."}
          >
            {!taxes.ok ? (
              <div className="empty-state error-state">API error: {taxes.error ?? "Unable to load taxes."}</div>
            ) : topSpreadRoutes.length === 0 ? (
              <div className="empty-state">No route tax summaries matched the current filter set.</div>
            ) : (
              <div className="table-list compact-list">
                {topSpreadRoutes.map((row) => (
                  <div className="table-row" key={`tax-route-${row.route_key}`}>
                    <div>
                      <strong>{row.route_key}</strong>
                      <span className="route-inline-meta">
                        <span className="route-type-pill" data-type={formatRouteType(row.route_type)}>
                          {formatRouteType(row.route_type)}
                        </span>
                        <span>{formatRouteGeo(row.origin_country_code, row.destination_country_code)}</span>
                      </span>
                    </div>
                    <div className="pill warn">{formatMoney(toNumber(row.spread_amount), "BDT")}</div>
                    <span>{formatDelta(toNumber(row.avg_tax_change_amount))}</span>
                  </div>
                ))}
              </div>
            )}
          </DataPanel>

          <DataPanel
            title="Airline tax movement"
            copy="Largest recent airline-level tax shifts with compact trend strips from recent cycles."
          >
            {!taxes.ok ? (
              <div className="empty-state error-state">API error: {taxes.error ?? "Unable to load taxes."}</div>
            ) : topAirlineMoves.length === 0 ? (
              <div className="empty-state">No airline tax movement matched the current filter set.</div>
            ) : (
              <div className="table-list compact-list">
                {topAirlineMoves.map((row) => (
                  <div className="table-row" key={`tax-airline-${row.route_key}-${row.airline}`}>
                    <div>
                      <strong>{`${row.route_key} ${row.airline}`}</strong>
                      <span>{renderTrendStrip(row.timeline)}</span>
                    </div>
                    <div className={`pill ${toNumber(row.avg_tax_change_amount) && Number(row.avg_tax_change_amount) > 0 ? "warn" : "good"}`}>
                      {formatDelta(toNumber(row.avg_tax_change_amount))}
                    </div>
                    <span>{formatMoney(toNumber(row.avg_tax_amount), "BDT")}</span>
                  </div>
                ))}
              </div>
            )}
          </DataPanel>
        </div>

        <DataPanel
          title="Tax rows"
          copy={routeKey ? `Showing tax rows for ${routeKey}.` : "Showing tax rows across the selected operational scope."}
        >
          {!taxes.ok ? (
            <div className="empty-state error-state">API error: {taxes.error ?? "Unable to load taxes."}</div>
          ) : rows.length === 0 ? (
            <div className="empty-state">No tax rows matched the current filter set.</div>
          ) : (
            <div className="data-table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Route</th>
                    <th>Airline</th>
                    <th>Flight</th>
                    <th>Departure</th>
                    <th>Cabin</th>
                    <th>Fare basis</th>
                    <th>Tax amount</th>
                    <th>Currency</th>
                    <th>Captured</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row, index) => (
                    <tr
                      key={`${row.route_key}-${row.airline}-${row.flight_number}-${row.departure_utc}-${row.fare_basis ?? ""}-${row.captured_at_utc ?? ""}-${index}`}
                    >
                      <td>
                        <div className="table-cell-stack">
                          <strong>{row.route_key}</strong>
                          <span className="route-inline-meta">
                            <span className="route-type-pill" data-type={formatRouteType(row.route_type)}>
                              {formatRouteType(row.route_type)}
                            </span>
                            <span>{formatRouteGeo(row.origin_country_code, row.destination_country_code)}</span>
                          </span>
                        </div>
                      </td>
                      <td>{row.airline}</td>
                      <td>{row.flight_number}</td>
                      <td>{formatDhakaDateTime(row.departure_utc)}</td>
                      <td>{row.cabin ?? "-"}</td>
                      <td>{row.fare_basis ?? "-"}</td>
                      <td>{formatMoney(row.tax_amount, row.currency ?? "BDT")}</td>
                      <td>{row.currency ?? "-"}</td>
                      <td>{formatDhakaDateTime(row.captured_at_utc)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </DataPanel>
      </div>
    </>
  );
}
