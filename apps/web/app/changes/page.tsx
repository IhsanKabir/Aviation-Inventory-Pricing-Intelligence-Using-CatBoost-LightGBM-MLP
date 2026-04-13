import { DataPanel } from "@/components/data-panel";
import { LiveFilterControls } from "@/components/live-filter-controls";
import { MetricCard } from "@/components/metric-card";
import { ReportAccessRequestPanel } from "@/components/report-access-request-panel";
import {
  getAirlines,
  getChangeDashboardPayload,
  getChangeEventsPayload,
  getReportAccessRequest,
  getRoutes,
} from "@/lib/api";
import { buildReportingExportUrl } from "@/lib/export";
import { formatDhakaDateTime, formatNumber, formatPublicValue, formatRouteGeo, formatRouteType } from "@/lib/format";
import { buildHref, firstParam, manyParams, parseLimit, removeParams, setParam, type RawSearchParams } from "@/lib/query";
import { getCurrentUserSession } from "@/lib/user-auth";

type PageProps = {
  searchParams?: Promise<RawSearchParams>;
};

const WINDOW_OFFSETS: Record<string, number> = {
  today: 0,
  last_3d: 2,
  last_7d: 6,
  last_14d: 13,
};

const HIDDEN_FIELD_NAMES = new Set([
  "scraped_at",
  "source_endpoint",
  "raw_offer",
  "ota_name",
  "penalty_source",
  "fare_search_signature",
  "fare_search_reference",
  "fare_ref_num",
]);

function selectedRouteKey(origin?: string, destination?: string) {
  if (!origin || !destination) return undefined;
  return `${origin}-${destination}`;
}

function formatIsoDate(value: Date) {
  return value.toISOString().slice(0, 10);
}

function offsetIsoDate(value: string, deltaDays: number) {
  const next = new Date(`${value}T00:00:00.000Z`);
  next.setUTCDate(next.getUTCDate() + deltaDays);
  return formatIsoDate(next);
}

function resolveDateWindow(startDate?: string, endDate?: string, windowKey?: string) {
  if (startDate || endDate) return { startDate, endDate };
  const offset = windowKey ? WINDOW_OFFSETS[windowKey] : undefined;
  if (offset === undefined) return { startDate, endDate };
  const today = formatIsoDate(new Date());
  return { startDate: offsetIsoDate(today, -offset), endDate: today };
}

function toDisplayFieldName(value?: string | null) {
  if (!value) return "-";
  return value
    .split("_")
    .filter(Boolean)
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(" ");
}

function formatShortDay(value?: string | null) {
  if (!value) return "-";
  const dateValue = new Date(`${value}T00:00:00.000Z`);
  return new Intl.DateTimeFormat("en-GB", { day: "2-digit", month: "short" }).format(dateValue);
}

function pct(part: number, total: number) {
  if (!total) return 0;
  return Math.round((part / total) * 100);
}

export default async function ChangesPage({ searchParams }: PageProps) {
  const params = (await searchParams) ?? {};
  const selectedAirlines = manyParams(params, "airline");
  const selectedDomains = manyParams(params, "domain");
  const selectedChangeTypes = manyParams(params, "change_type");
  const selectedDirections = manyParams(params, "direction");
  const origin = firstParam(params, "origin");
  const destination = firstParam(params, "destination");
  const explicitStartDate = firstParam(params, "start_date");
  const explicitEndDate = firstParam(params, "end_date");
  const selectedWindow = firstParam(params, "window");
  const limit = parseLimit(firstParam(params, "limit"), 150);
  const requestId = firstParam(params, "request_id") ?? undefined;
  const routeKey = selectedRouteKey(origin, destination);
  const { startDate, endDate } = resolveDateWindow(explicitStartDate, explicitEndDate, selectedWindow);

  const [airlines, routes, accessRequest] = await Promise.all([
    getAirlines(),
    getRoutes(),
    requestId ? getReportAccessRequest(requestId) : Promise.resolve({ ok: true, data: null as null, error: undefined }),
  ]);
  const { user } = await getCurrentUserSession();
  const accessGranted = accessRequest.ok && accessRequest.data?.page_key === "changes" && accessRequest.data?.status === "approved";

  const [dashboard, changes] =
    accessGranted && requestId
      ? await Promise.all([
          getChangeDashboardPayload({
            requestId,
            airlines: selectedAirlines,
            origins: origin ? [origin] : undefined,
            destinations: destination ? [destination] : undefined,
            domains: selectedDomains,
            changeTypes: selectedChangeTypes,
            directions: selectedDirections,
            startDate,
            endDate,
            topN: 8,
          }),
          getChangeEventsPayload({
            requestId,
            airlines: selectedAirlines,
            origins: origin ? [origin] : undefined,
            destinations: destination ? [destination] : undefined,
            domains: selectedDomains,
            changeTypes: selectedChangeTypes,
            directions: selectedDirections,
            startDate,
            endDate,
            limit,
          }),
        ])
      : [
          { ok: true, data: null as null, error: undefined },
          { ok: true, data: null as null, error: undefined },
        ];

  const rows = (changes.data?.items ?? []).filter((row) => !HIDDEN_FIELD_NAMES.has((row.field_name ?? "").trim()));
  const summary = dashboard.data?.summary;
  const totalEvents = summary?.event_count ?? rows.length;
  const upCount = summary?.up_count ?? rows.filter((row) => row.direction === "up").length;
  const downCount = summary?.down_count ?? rows.filter((row) => row.direction === "down").length;
  const latestEventAt = summary?.latest_event_at_utc ?? rows[0]?.detected_at_utc ?? null;
  const dailySeries = (dashboard.data?.daily_series ?? []).slice().reverse();
  const maxDailyEvents = dailySeries.reduce((current, item) => Math.max(current, item.event_count ?? 0), 1);
  const exportHref = buildReportingExportUrl(params, ["changes"]);

  const airlineOptions = [...(airlines.data?.items ?? [])]
    .sort((left, right) => (right.offer_rows ?? 0) - (left.offer_rows ?? 0) || left.airline.localeCompare(right.airline))
    .slice(0, 20)
    .map((item) => item.airline);
  const routeOptions = [...(routes.data?.items ?? [])]
    .sort((left, right) => (right.offer_rows ?? 0) - (left.offer_rows ?? 0) || left.route_key.localeCompare(right.route_key))
    .slice(0, 16)
    .map((item) => ({ routeKey: item.route_key, origin: item.origin, destination: item.destination }));

  return (
    <>
      <div className="page-header-row">
        <h1 className="page-title" style={{ margin: 0 }}>Changes</h1>
        <div className="window-chip-row">
          {[
            { label: "Today", value: "today" },
            { label: "Last 3d", value: "last_3d" },
            { label: "Last 7d", value: "last_7d" },
            { label: "Last 14d", value: "last_14d" },
          ].map((item) => (
            <a
              key={item.value}
              className="window-chip"
              data-active={!explicitStartDate && !explicitEndDate && selectedWindow === item.value}
              href={buildHref(setParam(removeParams(params, ["start_date", "end_date"]), "window", item.value))}
            >
              {item.label}
            </a>
          ))}
          <a
            className="window-chip"
            data-active={!explicitStartDate && !explicitEndDate && !selectedWindow}
            href={buildHref(removeParams(params, ["window", "start_date", "end_date"]))}
          >
            All history
          </a>
        </div>
      </div>

      <p className="page-copy">
        Market movement across fare, inventory, schedule, penalty, and tax changes.
        {latestEventAt ? ` Latest movement: ${formatDhakaDateTime(latestEventAt)}.` : ""}
      </p>

      <div className="grid cards five-up">
        <MetricCard label="Events" value={totalEvents.toLocaleString()} footnote={routeKey || "All routes in scope"} />
        <MetricCard label="Routes" value={(summary?.route_count ?? 0).toLocaleString()} footnote={selectedAirlines.length ? `${selectedAirlines.length} carriers selected` : "All carriers"} />
        <MetricCard label="Airlines" value={(summary?.airline_count ?? 0).toLocaleString()} footnote={`${(summary?.added_count ?? 0).toLocaleString()} added | ${(summary?.removed_count ?? 0).toLocaleString()} removed`} />
        <MetricCard label="Up events" value={upCount.toLocaleString()} footnote={`${pct(upCount, totalEvents)}% of total movement`} />
        <MetricCard label="Down events" value={downCount.toLocaleString()} footnote={`${pct(downCount, totalEvents)}% of total movement`} />
      </div>

      <div className="stack">
        <DataPanel
          title="Event filters"
          copy="Narrow by airline, route, domain, change type, or date window."
        >
          <LiveFilterControls
            airlineOptions={airlineOptions}
            clearKeys={["airline", "origin", "destination", "domain", "change_type", "direction", "window", "start_date", "end_date", "limit"]}
            extraGroups={[
              {
                key: "domain",
                label: "Domains",
                selected: selectedDomains,
                options: [
                  { label: "Price", value: "price" },
                  { label: "Availability", value: "availability" },
                  { label: "Capacity", value: "capacity" },
                  { label: "Schedule", value: "schedule" },
                  { label: "Seat", value: "seat" },
                  { label: "Field", value: "field" },
                ],
              },
              {
                key: "change_type",
                label: "Change types",
                selected: selectedChangeTypes,
                options: [
                  { label: "Increase", value: "increase" },
                  { label: "Decrease", value: "decrease" },
                  { label: "Added", value: "added" },
                  { label: "Removed", value: "removed" },
                ],
              },
              {
                key: "direction",
                label: "Directions",
                selected: selectedDirections,
                options: [
                  { label: "Up", value: "up" },
                  { label: "Down", value: "down" },
                  { label: "None", value: "none" },
                ],
              },
            ]}
            initialValues={{
              origin: origin ?? "",
              destination: destination ?? "",
              start_date: startDate ?? "",
              end_date: endDate ?? "",
              limit: String(limit),
            }}
            manualFields={[
              { name: "origin", label: "Origin", placeholder: "DAC" },
              { name: "destination", label: "Destination", placeholder: "RUH" },
              { name: "start_date", label: "Start date", type: "date" },
              { name: "end_date", label: "End date", type: "date" },
              { name: "limit", label: "Row limit", inputMode: "numeric", pattern: "[0-9]*" },
            ]}
            routeOptions={routeOptions}
            selectedAirlines={selectedAirlines}
            selectedRouteKey={routeKey}
          />

          {accessGranted && requestId ? (
            <div className="button-row">
              <a className="button-link ghost" href={exportHref}>Download Excel</a>
            </div>
          ) : null}
        </DataPanel>

        <DataPanel
          title="Data access request"
          copy="Approve the selected changes scope before loading the live change-event analytics."
        >
          <ReportAccessRequestPanel
            currentUser={user}
            description="Submit this market-change scope for approval. After approval, the analytics, event rows, and workbook export unlock for the same filters."
            headline="Changes access requires approval."
            pageKey="changes"
            request={accessRequest.ok ? accessRequest.data : null}
            requestWindow={{ startDate, endDate }}
            resourceLabel="changes"
            scope={{
              airline: selectedAirlines,
              origin: origin ?? undefined,
              destination: destination ?? undefined,
              domain: selectedDomains,
              change_type: selectedChangeTypes,
              direction: selectedDirections,
              start_date: startDate,
              end_date: endDate,
              limit,
            }}
            scopeSummary={[
              `Route: ${origin || "any"} -> ${destination || "any"}`,
              selectedAirlines.length ? `Airlines: ${selectedAirlines.join(", ")}` : "Airlines: all carriers",
              selectedDomains.length ? `Domains: ${selectedDomains.join(", ")}` : "Domains: all",
              selectedChangeTypes.length ? `Change types: ${selectedChangeTypes.join(", ")}` : "Change types: all",
              selectedDirections.length ? `Directions: ${selectedDirections.join(", ")}` : "Directions: all",
              startDate || endDate ? `Window: ${startDate ?? "any"} to ${endDate ?? "any"}` : "Window: all history",
              `Detail limit: ${limit.toLocaleString()} events`,
            ]}
            submitLabel="Submit changes request"
          />
        </DataPanel>

        {!accessGranted || !requestId ? (
          <DataPanel
            title="Market change analytics"
            copy="This live change monitor unlocks after the current request is approved."
          >
            <div className="empty-state">
              {requestId
                ? "This change scope is not approved yet. Refresh after manual review, or adjust the filters and submit a new request."
                : "Submit a changes access request above to unlock the live change analytics for this scope."}
            </div>
          </DataPanel>
        ) : (
          <>
            <DataPanel
              title="Daily activity"
              copy="Event volume per day broken down by direction."
            >
              {!dashboard.ok ? (
                <div className="empty-state error-state">API error: {dashboard.error ?? "Unable to load change analytics."}</div>
              ) : dailySeries.length === 0 ? (
                <div className="empty-state">No daily change history in the selected scope.</div>
              ) : (
                <div className="daily-activity-list">
                  {dailySeries.map((item) => {
                    const upW = pct(item.up_count ?? 0, maxDailyEvents);
                    const downW = pct(item.down_count ?? 0, maxDailyEvents);
                    const otherW = pct(Math.max(0, item.event_count - (item.up_count ?? 0) - (item.down_count ?? 0)), maxDailyEvents);
                    return (
                      <div className="daily-activity-row" key={item.report_day}>
                        <div className="daily-activity-date">{formatShortDay(item.report_day)}</div>
                        <div className="daily-activity-bar-track">
                          <div className="daily-bar-up" style={{ width: `${upW}%` }} />
                          <div className="daily-bar-down" style={{ width: `${downW}%` }} />
                          <div className="daily-bar-other" style={{ width: `${otherW}%` }} />
                        </div>
                        <div className="daily-activity-count">{item.event_count.toLocaleString()} events</div>
                      </div>
                    );
                  })}
                </div>
              )}
            </DataPanel>

            <DataPanel
              title="Event rows"
              copy={`${rows.length.toLocaleString()} detected changes in the current scope.`}
            >
              {!changes.ok ? (
                <div className="empty-state error-state">API error: {changes.error ?? "Unable to load change events."}</div>
              ) : rows.length === 0 ? (
                <div className="empty-state">No change events matched the current filter set.</div>
              ) : (
                <div className="data-table-wrap">
                  <table className="data-table compact-table">
                    <thead>
                      <tr>
                        <th>Detected</th>
                        <th>Route</th>
                        <th>Airline</th>
                        <th>Flight</th>
                        <th>Domain</th>
                        <th>Field</th>
                        <th>Type</th>
                        <th>Direction</th>
                        <th>Old value</th>
                        <th>New value</th>
                        <th>Magnitude</th>
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((row, index) => (
                        <tr key={`${row.id}-${row.detected_at_utc ?? ""}-${index}`}>
                          <td>{formatDhakaDateTime(row.detected_at_utc)}</td>
                          <td>
                            <div className="table-cell-stack">
                              <strong>{row.route_key ?? "-"}</strong>
                              <span className="route-inline-meta">
                                <span className="route-type-pill" data-type={formatRouteType(row.route_type)}>
                                  {formatRouteType(row.route_type)}
                                </span>
                                <span>{formatRouteGeo(row.origin_country_code, row.destination_country_code)}</span>
                              </span>
                            </div>
                          </td>
                          <td>{row.airline}</td>
                          <td>
                            <div className="table-cell-stack">
                              <strong>{row.flight_number ?? "-"}</strong>
                              <span>{row.departure_time ?? "-"}</span>
                            </div>
                          </td>
                          <td>{row.domain ?? "-"}</td>
                          <td>{toDisplayFieldName(row.field_name)}</td>
                          <td>{row.change_type ?? "-"}</td>
                          <td>{row.direction ?? "-"}</td>
                          <td className="long-text">{formatPublicValue(row.old_value)}</td>
                          <td className="long-text">{formatPublicValue(row.new_value)}</td>
                          <td>
                            <div className="table-cell-stack">
                              <span>{formatNumber(row.magnitude)}</span>
                              <span>{row.percent_change != null ? `${row.percent_change.toFixed(2)}%` : "-"}</span>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </DataPanel>
          </>
        )}
      </div>
    </>
  );
}
