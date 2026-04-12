import { LiveFilterControls } from "@/components/live-filter-controls";
import { DataPanel } from "@/components/data-panel";
import { MetricCard } from "@/components/metric-card";
import {
  getAirlines,
  getChangeDashboardPayload,
  getChangeEventsPayload,
  getRoutes
} from "@/lib/api";
import { buildReportingExportUrl } from "@/lib/export";
import { formatDhakaDateTime, formatNumber, formatPublicValue, formatRouteGeo, formatRouteType } from "@/lib/format";
import { buildHref, firstParam, manyParams, parseLimit, removeParams, setParam, type RawSearchParams } from "@/lib/query";

type PageProps = {
  searchParams?: Promise<RawSearchParams>;
};

const WINDOW_OFFSETS: Record<string, number> = {
  today: 0,
  last_3d: 2,
  last_7d: 6,
  last_14d: 13
};

const PRICE_FIELD_NAMES = new Set(["total_price_bdt", "base_fare_amount"]);

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

const HIDDEN_FIELD_NAMES = new Set([
  "scraped_at", "source_endpoint", "raw_offer", "ota_name", "penalty_source",
  "fare_search_signature", "fare_search_reference", "fare_ref_num"
]);

function toDisplayFieldName(value?: string | null) {
  if (!value) return "-";
  const explicitLabels: Record<string, string> = {
    tax_amount: "Tax amount",
    total_price_bdt: "Total price",
    base_fare_amount: "Base fare",
    ota_gross_fare: "Channel gross fare",
    ota_discount_amount: "Channel discount amount",
    ota_discount_pct: "Channel discount percent",
    seat_available: "Seat available",
    seat_capacity: "Seat capacity",
    load_factor_pct: "Load factor",
    booking_class: "Booking class",
    penalty_rule_text: "Penalty text",
    operating_airline: "Operating airline"
  };
  if (explicitLabels[value]) return explicitLabels[value];
  return value.split("_").filter(Boolean).map((t) => t.charAt(0).toUpperCase() + t.slice(1)).join(" ");
}

function formatDayLabel(value?: string | null) {
  if (!value) return "-";
  const dateValue = new Date(`${value}T00:00:00.000Z`);
  return new Intl.DateTimeFormat("en-GB", { weekday: "short", day: "2-digit", month: "short" }).format(dateValue);
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
  const topN = 8;
  const routeKey = selectedRouteKey(origin, destination);
  const { startDate, endDate } = resolveDateWindow(explicitStartDate, explicitEndDate, selectedWindow);

  const [airlines, routes, dashboard, changes] = await Promise.all([
    getAirlines(),
    getRoutes(),
    getChangeDashboardPayload({
      airlines: selectedAirlines,
      origins: origin ? [origin] : undefined,
      destinations: destination ? [destination] : undefined,
      domains: selectedDomains,
      changeTypes: selectedChangeTypes,
      directions: selectedDirections,
      startDate,
      endDate,
      topN
    }),
    getChangeEventsPayload({
      airlines: selectedAirlines,
      origins: origin ? [origin] : undefined,
      destinations: destination ? [destination] : undefined,
      domains: selectedDomains,
      changeTypes: selectedChangeTypes,
      directions: selectedDirections,
      startDate,
      endDate,
      limit
    })
  ]);

  const rawRows = changes.data?.items ?? [];
  const rows = rawRows.filter((row) => !HIDDEN_FIELD_NAMES.has((row.field_name ?? "").trim()));

  const airlineOptions = [...(airlines.data?.items ?? [])]
    .sort((l, r) => (r.offer_rows ?? 0) - (l.offer_rows ?? 0) || l.airline.localeCompare(r.airline))
    .slice(0, 20).map((i) => i.airline);
  const routeOptions = [...(routes.data?.items ?? [])]
    .sort((l, r) => (r.offer_rows ?? 0) - (l.offer_rows ?? 0) || l.route_key.localeCompare(r.route_key))
    .slice(0, 16).map((i) => ({ routeKey: i.route_key, origin: i.origin, destination: i.destination }));

  const summary = dashboard.data?.summary;
  const totalEvents = summary?.event_count ?? rows.length;
  const upCount = summary?.up_count ?? rows.filter((r) => r.direction === "up").length;
  const downCount = summary?.down_count ?? rows.filter((r) => r.direction === "down").length;
  const addedCount = summary?.added_count ?? rows.filter((r) => r.change_type === "added").length;
  const removedCount = summary?.removed_count ?? rows.filter((r) => r.change_type === "removed").length;
  const routeCount = summary?.route_count ?? new Set(rows.map((r) => r.route_key).filter(Boolean)).size;
  const airlineCount = summary?.airline_count ?? new Set(rows.map((r) => r.airline)).size;
  const latestEventAt = summary?.latest_event_at_utc ?? rows[0]?.detected_at_utc ?? null;
  const topRoutes = dashboard.data?.top_routes ?? [];
  const topAirlines = dashboard.data?.top_airlines ?? [];
  const topDomains = dashboard.data?.domain_mix ?? [];
  const dailySeries = (dashboard.data?.daily_series ?? []).slice().reverse(); // oldest→newest for visual
  const maxDailyEvents = dailySeries.reduce((m, i) => Math.max(m, i.event_count ?? 0), 1);
  const exportHref = buildReportingExportUrl(params, ["changes"]);

  // Direction split bar proportions
  const neutralCount = Math.max(0, totalEvents - upCount - downCount);
  const upPct = pct(upCount, totalEvents);
  const downPct = pct(downCount, totalEvents);

  // Biggest price moves (total_price_bdt or base_fare_amount, direction up or down, sort by abs magnitude)
  const priceMoves = (dashboard.data?.largest_moves ?? rows)
    .filter((r) => PRICE_FIELD_NAMES.has(r.field_name ?? "") && (r.direction === "up" || r.direction === "down") && r.magnitude != null)
    .sort((a, b) => Math.abs(b.magnitude ?? 0) - Math.abs(a.magnitude ?? 0))
    .slice(0, 8);

  // Route-level up/down split from rows
  const routeUpDown: Record<string, { up: number; down: number; total: number }> = {};
  for (const row of rows) {
    const key = row.route_key ?? "";
    if (!key) continue;
    if (!routeUpDown[key]) routeUpDown[key] = { up: 0, down: 0, total: 0 };
    routeUpDown[key].total++;
    if (row.direction === "up") routeUpDown[key].up++;
    if (row.direction === "down") routeUpDown[key].down++;
  }

  // Airline-level up/down split
  const airlineUpDown: Record<string, { up: number; down: number; total: number }> = {};
  for (const row of rows) {
    const key = row.airline ?? "";
    if (!key) continue;
    if (!airlineUpDown[key]) airlineUpDown[key] = { up: 0, down: 0, total: 0 };
    airlineUpDown[key].total++;
    if (row.direction === "up") airlineUpDown[key].up++;
    if (row.direction === "down") airlineUpDown[key].down++;
  }

  // Date-group event table
  const rowsByDate: Record<string, typeof rows> = {};
  for (const row of rows) {
    const day = row.departure_day ?? row.detected_at_utc?.slice(0, 10) ?? "unknown";
    if (!rowsByDate[day]) rowsByDate[day] = [];
    rowsByDate[day].push(row);
  }
  const sortedDates = Object.keys(rowsByDate).sort((a, b) => b.localeCompare(a));

  const maxDomainCount = Math.max(1, ...topDomains.map((d) => d.event_count));

  return (
    <>
      {/* ── Page header with window chips inline ── */}
      <div className="page-header-row">
        <h1 className="page-title" style={{ margin: 0 }}>Changes</h1>
        <div className="window-chip-row">
          {[
            { label: "Today", value: "today" },
            { label: "Last 3d", value: "last_3d" },
            { label: "Last 7d", value: "last_7d" },
            { label: "Last 14d", value: "last_14d" }
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

      {/* ── 5 metric cards ── */}
      <div className="grid cards five-up">
        <MetricCard label="Events" value={totalEvents.toLocaleString()} footnote={routeKey ? routeKey : "All routes in scope"} />
        <MetricCard label="Routes" value={routeCount.toLocaleString()} footnote={selectedAirlines.length ? `${selectedAirlines.length} carrier selected` : "All carriers"} />
        <MetricCard label="Airlines" value={airlineCount.toLocaleString()} footnote={`${addedCount.toLocaleString()} added · ${removedCount.toLocaleString()} removed`} />
        <MetricCard label="↑ Up events" value={upCount.toLocaleString()} footnote={`${upPct}% of total movement`} />
        <MetricCard label="↓ Down events" value={downCount.toLocaleString()} footnote={`${pct(downCount, totalEvents)}% of total movement`} />
      </div>

      {/* ── Direction split bar ── */}
      {totalEvents > 0 && (
        <div className="direction-split-bar-wrap">
          <div className="direction-split-bar">
            <div className="direction-split-bar-up" style={{ width: `${upPct}%` }} />
            <div className="direction-split-bar-down" style={{ width: `${downPct}%` }} />
          </div>
          <div className="direction-split-labels">
            <span>{upCount.toLocaleString()} up ({upPct}%)</span>
            <span>{neutralCount.toLocaleString()} neutral</span>
            <span>{downCount.toLocaleString()} down ({pct(downCount, totalEvents)}%)</span>
          </div>
        </div>
      )}

      {/* ── Daily activity (full width) ── */}
      <div style={{ marginTop: 22 }}>
        <DataPanel
          title="Daily activity"
          copy="Event volume per day broken down by direction. Green = fare increases, red = decreases. Width is relative to the busiest day in the window."
        >
          {dailySeries.length === 0 ? (
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
                    <div className="daily-activity-split">
                      <span className="up-text">{(item.up_count ?? 0).toLocaleString()}↑</span>
                      {" · "}
                      <span className="down-text">{(item.down_count ?? 0).toLocaleString()}↓</span>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </DataPanel>
      </div>

      {/* ── Analytics 3-column grid: routes · airlines · domains ── */}
      {(topRoutes.length > 0 || topAirlines.length > 0 || topDomains.length > 0) && (
        <div className="analytics-three-grid">
          {/* Top routes */}
          <section className="card analytics-panel">
            <h2 style={{ margin: "0 0 10px", fontSize: "1.1rem" }}>Top routes</h2>
            <div style={{ color: "var(--muted)", fontSize: "0.86rem", marginBottom: 14, lineHeight: 1.5 }}>
              Most active routes · bar shows up/down share
            </div>
            <div style={{ display: "grid", gap: 8 }}>
              {topRoutes.map((item) => {
                const split = routeUpDown[item.route_key ?? ""] ?? { up: 0, down: 0, total: item.event_count };
                const upW = pct(split.up, split.total || 1);
                const downW = pct(split.down, split.total || 1);
                return (
                  <div className="analytics-row" key={`route-${item.route_key}`}>
                    <div className="analytics-row-header">
                      <span className="analytics-row-label">{item.route_key}</span>
                      <span className="analytics-row-count">{item.event_count.toLocaleString()} events</span>
                    </div>
                    <div className="analytics-split-bar">
                      <div className="analytics-split-up" style={{ width: `${upW}%` }} />
                      <div className="analytics-split-down" style={{ width: `${downW}%` }} />
                    </div>
                  </div>
                );
              })}
            </div>
          </section>

          {/* Top airlines */}
          <section className="card analytics-panel">
            <h2 style={{ margin: "0 0 10px", fontSize: "1.1rem" }}>Top airlines</h2>
            <div style={{ color: "var(--muted)", fontSize: "0.86rem", marginBottom: 14, lineHeight: 1.5 }}>
              Most active carriers · bar shows up/down share
            </div>
            <div style={{ display: "grid", gap: 8 }}>
              {topAirlines.map((item) => {
                const split = airlineUpDown[item.airline ?? ""] ?? { up: 0, down: 0, total: item.event_count };
                const upW = pct(split.up, split.total || 1);
                const downW = pct(split.down, split.total || 1);
                return (
                  <div className="analytics-row" key={`airline-${item.airline}`}>
                    <div className="analytics-row-header">
                      <span className="analytics-row-label">{item.airline}</span>
                      <span className="analytics-row-count">{item.event_count.toLocaleString()} events</span>
                    </div>
                    <div className="analytics-split-bar">
                      <div className="analytics-split-up" style={{ width: `${upW}%` }} />
                      <div className="analytics-split-down" style={{ width: `${downW}%` }} />
                    </div>
                  </div>
                );
              })}
            </div>
          </section>

          {/* Domain mix */}
          <section className="card analytics-panel">
            <h2 style={{ margin: "0 0 10px", fontSize: "1.1rem" }}>Domain mix</h2>
            <div style={{ color: "var(--muted)", fontSize: "0.86rem", marginBottom: 14, lineHeight: 1.5 }}>
              Share of events by change category
            </div>
            <div style={{ display: "grid", gap: 8 }}>
              {topDomains.map((item) => (
                <div className="domain-bar-row" key={`domain-${item.domain}`}>
                  <div className="domain-bar-header">
                    <span className="domain-bar-name">{item.domain}</span>
                    <span className="domain-bar-count">{item.event_count.toLocaleString()} · {pct(item.event_count, totalEvents)}%</span>
                  </div>
                  <div className="domain-fill-track">
                    <div className="domain-fill-bar" style={{ width: `${pct(item.event_count, maxDomainCount)}%` }} />
                  </div>
                </div>
              ))}
            </div>
          </section>
        </div>
      )}

      {/* ── Biggest price moves card grid ── */}
      {priceMoves.length > 0 && (
        <div style={{ marginTop: 22 }}>
          <DataPanel
            title="Biggest price moves"
            copy="Largest total-price and base-fare shifts in the current scope, sorted by magnitude. These are the fare changes worth acting on."
          >
            <div className="moves-card-grid">
              {priceMoves.map((row, idx) => (
                <div className="moves-card" key={`move-${row.id ?? idx}`}>
                  <div className="moves-card-badges">
                    <span className="moves-badge">{row.route_key ?? "-"}</span>
                    <span className="moves-badge airline">{row.airline ?? "-"}</span>
                  </div>
                  <div className="moves-card-field">{toDisplayFieldName(row.field_name)}</div>
                  <div className="moves-card-values">
                    <span className="moves-value-old">{formatPublicValue(row.old_value)}</span>
                    <span className="moves-arrow">→</span>
                    <span>{formatPublicValue(row.new_value)}</span>
                  </div>
                  <div className={`moves-card-magnitude ${row.direction === "up" ? "up" : "down"}`}>
                    {row.direction === "up" ? "+" : ""}{formatNumber(row.magnitude)}
                  </div>
                  <div className="moves-card-meta">
                    {row.cabin ? `${row.cabin} · ` : ""}{formatDhakaDateTime(row.detected_at_utc)}
                  </div>
                </div>
              ))}
            </div>
          </DataPanel>
        </div>
      )}

      {/* ── Filters + event table ── */}
      <div className="stack">
        <DataPanel
          title="Event filters"
          copy="Narrow by airline, route, domain, change type, or date window. Changes apply to both this table and the analytics above."
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
                  { label: "Field", value: "field" }
                ]
              },
              {
                key: "change_type",
                label: "Change types",
                selected: selectedChangeTypes,
                options: [
                  { label: "Increase", value: "increase" },
                  { label: "Decrease", value: "decrease" },
                  { label: "Added", value: "added" },
                  { label: "Removed", value: "removed" }
                ]
              },
              {
                key: "direction",
                label: "Directions",
                selected: selectedDirections,
                options: [
                  { label: "Up", value: "up" },
                  { label: "Down", value: "down" },
                  { label: "None", value: "none" }
                ]
              }
            ]}
            initialValues={{
              origin: origin ?? "",
              destination: destination ?? "",
              start_date: startDate ?? "",
              end_date: endDate ?? "",
              limit: String(limit)
            }}
            manualFields={[
              { name: "origin", label: "Origin", placeholder: "DAC" },
              { name: "destination", label: "Destination", placeholder: "RUH" },
              { name: "start_date", label: "Start date", type: "date" },
              { name: "end_date", label: "End date", type: "date" },
              { name: "limit", label: "Row limit", inputMode: "numeric", pattern: "[0-9]*" }
            ]}
            routeOptions={routeOptions}
            selectedAirlines={selectedAirlines}
            selectedRouteKey={routeKey}
          />
          <div className="button-row">
            <a className="button-link ghost" href={exportHref}>Download Excel</a>
          </div>
        </DataPanel>

        <DataPanel
          title="Event rows"
          copy={`${rows.length.toLocaleString()} events grouped by departure date. Expand a day to see field-level changes.`}
        >
          {!changes.ok ? (
            <div className="empty-state error-state">API error: {changes.error ?? "Unable to load change events."}</div>
          ) : rows.length === 0 ? (
            <div className="empty-state">No change events matched the current filter set.</div>
          ) : (
            <div>
              {sortedDates.map((day, dayIdx) => {
                const dayRows = rowsByDate[day];
                const dayUp = dayRows.filter((r) => r.direction === "up").length;
                const dayDown = dayRows.filter((r) => r.direction === "down").length;
                return (
                  <details className="change-date-group" key={day} open={dayIdx === 0}>
                    <summary className="change-date-group-summary">
                      <div className="change-date-group-title">
                        <span className="change-date-toggle">▶</span>
                        <span>{formatDayLabel(day)}</span>
                        <span style={{ color: "var(--muted)", fontWeight: 400, fontSize: "0.84rem" }}>
                          · {dayRows.length.toLocaleString()} events
                        </span>
                      </div>
                      <span className="change-date-stat up-stat">{dayUp}↑</span>
                      <span className="change-date-stat down-stat">{dayDown}↓</span>
                      <span className="change-date-stat" style={{ color: "var(--muted)" }}>
                        {dayRows.length - dayUp - dayDown} other
                      </span>
                    </summary>
                    <div className="change-date-group-body">
                      <div className="data-table-wrap" style={{ borderRadius: 0, border: "none" }}>
                        <table className="data-table compact-table change-table">
                          <thead>
                            <tr>
                              <th className="sticky-change-col">Detected</th>
                              <th className="sticky-change-col second">Route</th>
                              <th className="sticky-change-col third">Airline</th>
                              <th className="sticky-change-col fourth">Flight</th>
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
                            {dayRows.map((row, index) => (
                              <tr key={`${row.id}-${row.detected_at_utc ?? ""}-${index}`}>
                                <td className="sticky-change-col change-table-meta">{formatDhakaDateTime(row.detected_at_utc)}</td>
                                <td className="sticky-change-col second change-table-meta">
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
                                <td className="sticky-change-col third change-table-meta">{row.airline}</td>
                                <td className="sticky-change-col fourth change-table-meta">
                                  <div className="table-cell-stack">
                                    <strong>{row.flight_number ?? "-"}</strong>
                                    <span>{row.departure_time ?? "-"}</span>
                                  </div>
                                </td>
                                <td><span className="change-pill">{row.domain ?? "-"}</span></td>
                                <td>{toDisplayFieldName(row.field_name)}</td>
                                <td><span className="change-pill">{row.change_type ?? "-"}</span></td>
                                <td>
                                  <span className={`change-pill direction-${row.direction ?? "neutral"}`}>
                                    {row.direction ?? "-"}
                                  </span>
                                </td>
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
                    </div>
                  </details>
                );
              })}
            </div>
          )}
        </DataPanel>
      </div>
    </>
  );
}
