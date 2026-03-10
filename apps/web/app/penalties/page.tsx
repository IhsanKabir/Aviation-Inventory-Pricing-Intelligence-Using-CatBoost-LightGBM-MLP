import { LiveFilterControls } from "@/components/live-filter-controls";
import { DataPanel } from "@/components/data-panel";
import { MetricCard } from "@/components/metric-card";
import { getAirlines, getPenaltyPayload, getRecentCycles, getRoutes } from "@/lib/api";
import { buildReportingExportUrl } from "@/lib/export";
import { formatBooleanFlag, formatDhakaDateTime, formatMoney, formatRouteGeo, formatRouteType, normalizeLongText, summarizePenaltyText } from "@/lib/format";
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

export default async function PenaltiesPage({ searchParams }: PageProps) {
  const params = (await searchParams) ?? {};
  const selectedAirlines = manyParams(params, "airline");
  const origin = firstParam(params, "origin");
  const destination = firstParam(params, "destination");
  const cycleId = firstParam(params, "cycle_id") ?? undefined;
  const limit = parseLimit(firstParam(params, "limit"), 120);
  const routeKey = selectedRouteKey(origin, destination);

  const [airlines, routes, recentCycles, penalties] = await Promise.all([
    getAirlines(),
    getRoutes(),
    getRecentCycles(8),
    getPenaltyPayload({
      cycleId,
      airlines: selectedAirlines,
      origins: origin ? [origin] : undefined,
      destinations: destination ? [destination] : undefined,
      limit,
    }),
  ]);

  const rows = penalties.data?.rows ?? [];
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
  const activeCycle = (recentCycles.data?.items ?? []).find((item) => item.cycle_id === (penalties.data?.cycle_id ?? cycleId));

  const airlineCount = new Set(rows.map((row) => row.airline)).size;
  const routeCount = new Set(rows.map((row) => row.route_key)).size;
  const refundableCount = rows.filter((row) => row.fare_refundable).length;
  const exportHref = buildReportingExportUrl(params, ["penalties"]);

  return (
    <>
      <h1 className="page-title">Penalty Reference</h1>
      <p className="page-copy">
        Current-cycle penalty view for route and airline comparison. This exposes
        structured change and refund fees without requiring workbook sheet scans.
      </p>

      <div className="grid cards">
        <MetricCard
          label="Cycle"
          value={activeCycle?.cycle_completed_at_utc ? formatDhakaDateTime(activeCycle.cycle_completed_at_utc) : "Not available"}
          footnote={penalties.ok ? "Latest warehouse-backed penalty slice" : "No cycle loaded"}
        />
        <MetricCard label="Penalty rows" value={rows.length.toLocaleString()} footnote={`Limit ${limit.toLocaleString()}`} />
        <MetricCard label="Airlines" value={airlineCount.toLocaleString()} footnote={selectedAirlines.length ? `${selectedAirlines.length} selected` : "All carriers"} />
        <MetricCard label="Refundable fares" value={refundableCount.toLocaleString()} footnote={`${routeCount.toLocaleString()} routes in view`} />
      </div>

      <div className="stack">
        <DataPanel
          title="Penalty filters"
          copy="Use the same operational route and airline filters here. Click chips for immediate updates, or pin an exact route below."
        >
          <LiveFilterControls
            airlineOptions={airlineOptions}
            clearKeys={["airline", "origin", "destination", "cycle_id", "limit"]}
            extraGroups={
              cycleOptions.length
                ? [
                    {
                      key: "cycle_id",
                      label: "Comparable cycles",
                      selected: cycleId ? [cycleId] : [],
                      options: cycleOptions,
                      multi: false,
                    },
                  ]
                : []
            }
            initialValues={{
              origin: origin ?? "",
              destination: destination ?? "",
              limit: String(limit),
            }}
            manualFields={[
              { name: "origin", label: "Origin", placeholder: "DAC" },
              { name: "destination", label: "Destination", placeholder: "RUH" },
              { name: "limit", label: "Row limit", inputMode: "numeric", pattern: "[0-9]*" },
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

        <DataPanel
          title="Penalty rows"
          copy={routeKey ? `Showing penalty rows for ${routeKey}.` : "Showing penalty rows across the selected operational scope."}
        >
          {!penalties.ok ? (
            <div className="empty-state error-state">API error: {penalties.error ?? "Unable to load penalties."}</div>
          ) : rows.length === 0 ? (
            <div className="empty-state">No penalty rows matched the current filter set.</div>
          ) : (
            <div className="data-table-wrap">
              <table className="data-table compact-table">
                <thead>
                  <tr>
                    <th>Route</th>
                    <th>Airline</th>
                    <th>Flight</th>
                    <th>Departure</th>
                    <th>Change fees</th>
                    <th>Cancel fees</th>
                    <th>Flags</th>
                    <th>Rule text</th>
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
                      <td>
                        <div className="table-cell-stack">
                          <span>{`24h+: ${formatMoney(row.fare_change_fee_before_24h, row.penalty_currency ?? "BDT")}`}</span>
                          <span>{`<24h: ${formatMoney(row.fare_change_fee_within_24h, row.penalty_currency ?? "BDT")}`}</span>
                          <span>{`No-show: ${formatMoney(row.fare_change_fee_no_show, row.penalty_currency ?? "BDT")}`}</span>
                        </div>
                      </td>
                      <td>
                        <div className="table-cell-stack">
                          <span>{`24h+: ${formatMoney(row.fare_cancel_fee_before_24h, row.penalty_currency ?? "BDT")}`}</span>
                          <span>{`<24h: ${formatMoney(row.fare_cancel_fee_within_24h, row.penalty_currency ?? "BDT")}`}</span>
                          <span>{`No-show: ${formatMoney(row.fare_cancel_fee_no_show, row.penalty_currency ?? "BDT")}`}</span>
                        </div>
                      </td>
                      <td>
                        <div className="table-cell-stack">
                          <span>{`Changeable: ${formatBooleanFlag(row.fare_changeable)}`}</span>
                          <span>{`Refundable: ${formatBooleanFlag(row.fare_refundable)}`}</span>
                        </div>
                      </td>
                      <td className="long-text">
                        {row.penalty_rule_text ? (
                          <details className="expand-text">
                            <summary>{summarizePenaltyText(row.penalty_rule_text)}</summary>
                            <pre>{normalizeLongText(row.penalty_rule_text)}</pre>
                          </details>
                        ) : (
                          "-"
                        )}
                      </td>
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
