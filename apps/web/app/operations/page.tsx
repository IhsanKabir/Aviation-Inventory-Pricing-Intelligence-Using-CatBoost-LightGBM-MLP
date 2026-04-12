import { LiveFilterControls } from "@/components/live-filter-controls";
import { DataPanel } from "@/components/data-panel";
import { MetricCard } from "@/components/metric-card";
import {
  getAirlineOperationsPayload,
  getAirlines,
  getRecentCycles,
  getRoutes,
  type OperationsRoute,
} from "@/lib/api";
import { buildReportingExportUrl } from "@/lib/export";
import {
  formatDhakaDateTime,
  formatRouteGeo,
  formatRouteType,
  formatRouteTypeDetail,
} from "@/lib/format";
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

function summarizeNetworkWindow(routes: OperationsRoute[]) {
  const times = routes.flatMap((route) => [route.first_departure_time, route.last_departure_time]).filter(Boolean) as string[];
  if (!times.length) {
    return "-";
  }
  const sorted = times.slice().sort();
  return `${sorted[0]} to ${sorted[sorted.length - 1]}`;
}

function uniqueSorted(values: string[]) {
  return Array.from(new Set(values.filter(Boolean))).sort();
}

function summarizeAirportList(values: string[], maxVisible = 3) {
  const unique = uniqueSorted(values);
  if (!unique.length) {
    return "Direct only";
  }
  if (unique.length <= maxVisible) {
    return unique.join(", ");
  }
  return `${unique.slice(0, maxVisible).join(", ")} +${unique.length - maxVisible}`;
}

function normalizeOperationsRoutes(routes: OperationsRoute[]): OperationsRoute[] {
  return routes.map((route) => ({
    ...route,
    departure_times: route.departure_times ?? [],
    service_patterns: route.service_patterns ?? [],
    via_airports: route.via_airports ?? [],
    departure_days: route.departure_days ?? [],
    weekday_profile: route.weekday_profile ?? [],
    timeline: route.timeline ?? [],
    airlines: (route.airlines ?? []).map((airline) => ({
      ...airline,
      departure_times: airline.departure_times ?? [],
      flight_numbers: airline.flight_numbers ?? [],
      service_patterns: airline.service_patterns ?? [],
      via_airports: airline.via_airports ?? [],
      weekday_profile: airline.weekday_profile ?? [],
      timeline: airline.timeline ?? [],
    })),
  }));
}

export default async function OperationsPage({ searchParams }: PageProps) {
  const params = (await searchParams) ?? {};
  const selectedAirlines = manyParams(params, "airline");
  const selectedRouteTypes = manyParams(params, "route_type");
  const origin = firstParam(params, "origin");
  const destination = firstParam(params, "destination");
  const selectedViaAirports = manyParams(params, "via_airport");
  const viaAirport = selectedViaAirports[0];
  const cycleId = firstParam(params, "cycle_id") ?? undefined;
  const startDate = firstParam(params, "start_date") ?? undefined;
  const endDate = firstParam(params, "end_date") ?? undefined;
  const routeLimit = parseLimit(firstParam(params, "route_limit"), 3);
  const trendLimit = parseLimit(firstParam(params, "trend_limit"), 8);
  const routeKey = selectedRouteKey(origin, destination);

  const [airlines, routes, recentCycles, operations] = await Promise.all([
    getAirlines(),
    getRoutes(),
    getRecentCycles(8),
    getAirlineOperationsPayload({
      cycleId,
      airlines: selectedAirlines,
      origins: origin ? [origin] : undefined,
      destinations: destination ? [destination] : undefined,
      viaAirports: selectedViaAirports.length ? selectedViaAirports : undefined,
      routeTypes: selectedRouteTypes,
      startDate,
      endDate,
      routeLimit,
      trendLimit,
    }),
  ]);

  const routeBlocks = normalizeOperationsRoutes(operations.data?.routes ?? []);
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
  const exportHref = buildReportingExportUrl(params, ["operations"]);
  const viaAirportOptions = Array.from(
    new Set(
      routeBlocks.flatMap((route) => [
        ...route.via_airports,
        ...route.airlines.flatMap((airline) => airline.via_airports),
      ])
    )
  )
    .sort()
    .slice(0, 16)
    .map((code) => ({ label: code, value: code }));

  const routeCount = routeBlocks.length;
  const activeCycle = (recentCycles.data?.items ?? []).find((item) => item.cycle_id === (operations.data?.cycle_id ?? cycleId));
  const airlineCount = new Set(routeBlocks.flatMap((route) => route.airlines.map((item) => item.airline))).size;
  const flightInstanceCount = routeBlocks.reduce((sum, route) => sum + route.flight_instance_count, 0);
  const activeDateCount = routeBlocks.reduce((sum, route) => sum + route.active_date_count, 0);
  const transitAirportCount = viaAirportOptions.length;

  return (
    <>
      <h1 className="page-title">Airline Operations</h1>
      <p className="page-copy">
        Route-level operating pattern review across airlines. This page focuses on who is flying, how often they
        are flying, when they depart, and whether the operation footprint is expanding or narrowing across recent cycles,
        including the actual transit airports used in connecting service.
      </p>

      <div className="grid cards">
        <MetricCard
          label="Cycle"
          value={activeCycle?.cycle_completed_at_utc ? formatDhakaDateTime(activeCycle.cycle_completed_at_utc) : "Not available"}
          footnote={operations.ok ? "Latest warehouse-backed operations slice" : "No cycle loaded"}
        />
        <MetricCard label="Routes" value={routeCount.toLocaleString()} footnote={`Route block limit ${routeLimit.toLocaleString()}`} />
        <MetricCard label="Airlines" value={airlineCount.toLocaleString()} footnote={`${flightInstanceCount.toLocaleString()} visible departures`} />
        <MetricCard label="Transit airports" value={transitAirportCount.toLocaleString()} footnote={`${summarizeNetworkWindow(routeBlocks)} network window | ${activeDateCount.toLocaleString()} departure dates`} />
      </div>

      <div className="stack">
        <DataPanel
          title="Operations filters"
          copy="Use the shared route and airline controls here, then narrow by cycle, route type, departure-date window, or one or more transit airports."
        >
          <LiveFilterControls
            airlineOptions={airlineOptions}
            clearKeys={["airline", "origin", "destination", "via_airport", "route_type", "cycle_id", "start_date", "end_date", "route_limit", "trend_limit"]}
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
              ...(viaAirportOptions.length
                ? [
                    {
                      key: "via_airport",
                      label: "Via airport",
                      selected: selectedViaAirports,
                      options: viaAirportOptions,
                      multi: true,
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
              via_airport: viaAirport ?? "",
              start_date: startDate ?? "",
              end_date: endDate ?? "",
              route_limit: String(routeLimit),
              trend_limit: String(trendLimit),
            }}
            manualFields={[
              { name: "origin", label: "Origin", placeholder: "DAC" },
              { name: "destination", label: "Destination", placeholder: "DXB" },
              { name: "via_airport", label: "Via airport", placeholder: "AUH" },
              { name: "start_date", label: "Start date", type: "date" },
              { name: "end_date", label: "End date", type: "date" },
              { name: "route_limit", label: "Route blocks", inputMode: "numeric", pattern: "[0-9]*" },
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

        {!operations.ok ? (
          <DataPanel
            title="Operations blocks"
            copy="The API request for operations data did not complete."
          >
            <div className="empty-state error-state">API error: {operations.error ?? "Unable to load airline operations."}</div>
          </DataPanel>
        ) : routeBlocks.length === 0 ? (
          <DataPanel
            title="Operations blocks"
            copy="No operational patterns matched the current scope."
          >
            <div className="empty-state">No routes matched the current operations filter set.</div>
          </DataPanel>
        ) : (
          routeBlocks.map((route) => (
            <DataPanel
              key={route.route_key}
              title={`${route.route_key} operations`}
              copy={formatRouteTypeDetail(route.route_type, route.origin_country_code, route.destination_country_code)}
            >
              <div className="operations-summary-grid">
                <div className="card operations-stat">
                  <span>Route type</span>
                  <strong>{formatRouteType(route.route_type)}</strong>
                  <small>{formatRouteGeo(route.origin_country_code, route.destination_country_code)}</small>
                </div>
                <div className="card operations-stat">
                  <span>Active airlines</span>
                  <strong>{route.airline_count.toLocaleString()}</strong>
                  <small>{route.flight_instance_count.toLocaleString()} visible departures</small>
                </div>
                <div className="card operations-stat">
                  <span>Departure dates</span>
                  <strong>{route.active_date_count.toLocaleString()}</strong>
                  <small>{route.departure_times.length.toLocaleString()} departure-time bands</small>
                </div>
                <div className="card operations-stat">
                  <span>Operating window</span>
                  <strong>{route.first_departure_time ?? "-"}</strong>
                  <small>{route.last_departure_time ?? "-"}</small>
                </div>
                <div className="card operations-stat">
                  <span>Transit footprint</span>
                  <strong>{route.service_patterns.join(", ") || "-"}</strong>
                  <small>{route.via_airports.length ? `Via ${summarizeAirportList(route.via_airports, 4)}` : "Direct vs connecting pattern in current scope"}</small>
                  <div className="operations-airport-list">
                    {route.via_airports.length ? (
                      uniqueSorted(route.via_airports).map((code) => (
                        <span className="operations-airport-chip" key={`${route.route_key}-${code}`}>
                          {code}
                        </span>
                      ))
                    ) : (
                      <span className="operations-direct-chip">Direct only</span>
                    )}
                  </div>
                </div>
              </div>

              <div className="section-grid operations-grid">
                <div className="table-list">
                  <div className="filter-label">Airline schedule map</div>
                  {route.airlines.map((airline) => (
                    <div className="table-row" key={`${route.route_key}-${airline.airline}`}>
                      <div>
                        <strong>{airline.airline}</strong>
                        <span className="operations-airline-meta">
                          {airline.departure_times.join(", ") || "-"} | Flights: {airline.flight_numbers.join(", ") || "-"} | Transit: {airline.service_patterns.join(", ") || "-"}
                        </span>
                        <div className="operations-airport-list">
                          {airline.via_airports.length ? (
                            uniqueSorted(airline.via_airports).map((code) => (
                              <span className="operations-airport-chip" key={`${route.route_key}-${airline.airline}-${code}`}>
                                {code}
                              </span>
                            ))
                          ) : (
                            <span className="operations-direct-chip">Direct only</span>
                          )}
                        </div>
                      </div>
                      <div className="pill good">{airline.flight_instance_count.toLocaleString()} deps</div>
                      <span>{airline.active_date_count.toLocaleString()} days</span>
                    </div>
                  ))}
                </div>

                <div className="operations-weekday-grid">
                  {(() => {
                    const maxCount = Math.max(1, ...route.weekday_profile.map((d) => d.flight_instance_count));
                    return route.weekday_profile.map((day) => (
                      <div className="card operations-weekday-card" key={`${route.route_key}-${day.day_label}`}>
                        <span>{day.day_label.slice(0, 3)}</span>
                        <strong>{day.flight_instance_count.toLocaleString()}</strong>
                        <small>{day.airline_count?.toLocaleString() ?? "0"} airlines</small>
                        <div className="weekday-bar-track">
                          <div
                            className="weekday-bar-fill"
                            style={{ width: `${Math.round((day.flight_instance_count / maxCount) * 100)}%` }}
                          />
                        </div>
                      </div>
                    ));
                  })()}
                </div>
              </div>

              <div className="stack">
                <div className="data-table-wrap">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Departure date</th>
                        <th>Day</th>
                        <th>Airlines</th>
                        <th>Departures</th>
                        <th>First departure</th>
                        <th>Last departure</th>
                      </tr>
                    </thead>
                    <tbody>
                      {route.departure_days.map((day) => (
                        <tr key={`${route.route_key}-${day.departure_date}`}>
                          <td>{day.departure_date}</td>
                          <td>{day.day_label}</td>
                          <td>{day.airline_count.toLocaleString()}</td>
                          <td>{day.flight_instance_count.toLocaleString()}</td>
                          <td>{day.first_departure_time ?? "-"}</td>
                          <td>{day.last_departure_time ?? "-"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div className="data-table-wrap">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Snapshot time</th>
                        <th>Airlines</th>
                        <th>Departures</th>
                        <th>Active dates</th>
                        <th>Window</th>
                      </tr>
                    </thead>
                    <tbody>
                      {route.timeline.map((point) => (
                        <tr key={`${route.route_key}-${point.cycle_id}`}>
                          <td>{formatDhakaDateTime(point.cycle_completed_at_utc)}</td>
                          <td>{point.airline_count?.toLocaleString() ?? "-"}</td>
                          <td>{point.flight_instance_count.toLocaleString()}</td>
                          <td>{point.active_date_count.toLocaleString()}</td>
                          <td>{`${point.first_departure_time ?? "-"} to ${point.last_departure_time ?? "-"}`}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </DataPanel>
          ))
        )}
      </div>
    </>
  );
}
