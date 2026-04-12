/**
 * app/gds/page.tsx - GDS Fare Intelligence Dashboard
 * All-time fare history, change detection markers, and analytics
 */

import { DataPanel } from "@/components/data-panel";
import { MetricCard } from "@/components/metric-card";
import {
  getGdsChangeSummary,
  getGdsChanges,
  getGdsFares,
  getGdsLatestRun,
  getGdsRuns,
  type GdsChangeEvent,
  type GdsChangeSummaryPoint,
  type GdsFareRow,
  type GdsFareRun,
} from "@/lib/gds";

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

function pct(part: number, total: number) {
  if (!total) return 0;
  return Math.round((part / total) * 100);
}

function formatShortDay(value?: string | null) {
  if (!value) return "-";
  const d = new Date(`${value}T00:00:00.000Z`);
  return new Intl.DateTimeFormat("en-GB", { day: "2-digit", month: "short" }).format(d);
}

function formatTs(value?: string | null) {
  if (!value) return "-";
  return new Date(value).toLocaleString("en-GB", {
    day: "2-digit", month: "short", year: "numeric",
    hour: "2-digit", minute: "2-digit",
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Data fetch
// ─────────────────────────────────────────────────────────────────────────────

async function getPageData() {
  const [latestRunR, runsR, faresR, changesR, summaryR] = await Promise.allSettled([
    getGdsLatestRun(),
    getGdsRuns(100),
    getGdsFares({ limit: 500 }),
    getGdsChanges({ days: 365, limit: 2000 }),
    getGdsChangeSummary(365),
  ]);

  return {
    latestRun: latestRunR.status === "fulfilled" ? latestRunR.value : null,
    runs:      runsR.status      === "fulfilled" ? runsR.value      : [] as GdsFareRun[],
    fares:     faresR.status     === "fulfilled" ? faresR.value     : [] as GdsFareRow[],
    changes:   changesR.status   === "fulfilled" ? changesR.value   : [] as GdsChangeEvent[],
    summary:   summaryR.status   === "fulfilled" ? summaryR.value   : [] as GdsChangeSummaryPoint[],
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Page
// ─────────────────────────────────────────────────────────────────────────────

export default async function GdsDashboardPage() {
  const { latestRun, runs, fares, changes, summary } = await getPageData();

  // Metric card values
  const soldOutCount = fares.filter((f) => f.is_sold_out).length;
  const soldOutPct   = pct(soldOutCount, fares.length || 1);

  // All-time run trend
  const sortedRuns = [...runs].sort(
    (a, b) => new Date(a.captured_at_utc).getTime() - new Date(b.captured_at_utc).getTime()
  );
  const maxFares = Math.max(...sortedRuns.map((r) => r.total_fares ?? 0), 1);
  const daysWithChanges = new Set(summary.filter((s) => s.change_count > 0).map((s) => s.report_day));

  // Daily activity grouped by report_day
  const byDay: Record<string, { up: number; down: number; other: number; total: number }> = {};
  for (const row of summary) {
    if (!byDay[row.report_day]) byDay[row.report_day] = { up: 0, down: 0, other: 0, total: 0 };
    const isUp   = row.change_type === "new" || row.change_type === "available";
    const isDown = row.change_type === "removed" || row.change_type === "sold_out";
    if (isUp)        byDay[row.report_day].up    += row.change_count;
    else if (isDown) byDay[row.report_day].down  += row.change_count;
    else             byDay[row.report_day].other += row.change_count;
    byDay[row.report_day].total += row.change_count;
  }
  const dailySeries = Object.entries(byDay)
    .sort(([a], [b]) => b.localeCompare(a))
    .map(([day, v]) => ({ day, ...v }));
  const maxDailyEvents = Math.max(...dailySeries.map((d) => d.total), 1);

  // All-time direction totals
  const totalUp    = summary.filter((s) => s.change_type === "new" || s.change_type === "available").reduce((n, s) => n + s.change_count, 0);
  const totalDown  = summary.filter((s) => s.change_type === "removed" || s.change_type === "sold_out").reduce((n, s) => n + s.change_count, 0);
  const totalOther = summary.filter((s) => s.change_type === "price_change").reduce((n, s) => n + s.change_count, 0);
  const totalAll   = totalUp + totalDown + totalOther;

  // Top routes
  const routeCounts: Record<string, { up: number; down: number; total: number }> = {};
  for (const ev of changes) {
    const k = ev.route_key ?? "";
    if (!routeCounts[k]) routeCounts[k] = { up: 0, down: 0, total: 0 };
    if (ev.change_type === "new" || ev.change_type === "available") routeCounts[k].up++;
    if (ev.change_type === "removed" || ev.change_type === "sold_out") routeCounts[k].down++;
    routeCounts[k].total++;
  }
  const topRoutes = Object.entries(routeCounts)
    .sort(([, a], [, b]) => b.total - a.total)
    .slice(0, 8);

  // Biggest fare moves
  const priceMoves = changes
    .filter((c) => c.change_type === "price_change" && c.old_ow_fare != null && c.new_ow_fare != null)
    .map((c) => ({
      ...c,
      delta:    (c.new_ow_fare ?? 0) - (c.old_ow_fare ?? 0),
      absDelta: Math.abs((c.new_ow_fare ?? 0) - (c.old_ow_fare ?? 0)),
    }))
    .sort((a, b) => b.absDelta - a.absDelta)
    .slice(0, 6);

  // Change events grouped by day (most recent 200)
  const eventsByDay: Record<string, GdsChangeEvent[]> = {};
  for (const ev of changes.slice(0, 200)) {
    const day = ev.report_day ?? "unknown";
    if (!eventsByDay[day]) eventsByDay[day] = [];
    eventsByDay[day].push(ev);
  }

  const changeTypeColor: Record<string, string> = {
    price_change: "#b88700",
    new:          "var(--good)",
    removed:      "var(--alert)",
    sold_out:     "var(--alert)",
    available:    "var(--good)",
  };

  return (
    <div className="shell stack" style={{ paddingTop: 28, paddingBottom: 48 }}>

      {/* Header */}
      <div>
        <h1 style={{ margin: 0, fontSize: "1.6rem", fontWeight: 700 }}>GDS Fare Intelligence</h1>
        <div style={{ color: "var(--muted)", fontSize: "0.9rem", marginTop: 4 }}>
          Live fare and tax data from Travelport Smartpoint
          {latestRun && (
            <span> · Last run: <strong>{formatTs(latestRun.captured_at_utc)}</strong></span>
          )}
        </div>
      </div>

      {/* Metric cards */}
      {latestRun && (
        <div className="grid cards">
          <MetricCard
            label="Routes"
            value={latestRun.total_routes.toLocaleString()}
            footnote="In latest extraction run"
          />
          <MetricCard
            label="Airlines"
            value={latestRun.total_airlines.toLocaleString()}
            footnote={latestRun.cycle_id}
          />
          <MetricCard
            label="Sold Out"
            value={`${soldOutPct}%`}
            footnote={`${soldOutCount.toLocaleString()} of ${fares.length.toLocaleString()} fares`}
          />
          <MetricCard
            label="All-time Changes"
            value={totalAll.toLocaleString()}
            footnote={`${totalUp.toLocaleString()} up · ${totalDown.toLocaleString()} down`}
          />
        </div>
      )}

      {/* All-time run history bar chart */}
      {sortedRuns.length > 0 && (
        <DataPanel
          title="Extraction run history"
          copy="Each bar = one extraction run. Height = fares captured. Red dot = change events detected in that run's period."
        >
          <div style={{ display: "flex", alignItems: "flex-end", gap: 3, height: 90, overflowX: "auto", paddingBottom: 4 }}>
            {sortedRuns.map((run) => {
              const h = Math.max(4, pct(run.total_fares ?? 0, maxFares) * 0.8);
              const runDay = run.captured_at_utc?.slice(0, 10);
              const hasChanges = runDay ? daysWithChanges.has(runDay) : false;
              return (
                <div
                  key={run.cycle_id}
                  style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 2, flexShrink: 0 }}
                  title={`${run.cycle_id} · ${(run.total_fares ?? 0).toLocaleString()} fares · ${formatTs(run.captured_at_utc)}`}
                >
                  {hasChanges ? (
                    <div style={{ width: 5, height: 5, borderRadius: "50%", background: "var(--alert)", marginBottom: 1 }} />
                  ) : (
                    <div style={{ width: 5, height: 5, marginBottom: 1 }} />
                  )}
                  <div
                    style={{
                      width: 7,
                      height: `${h}px`,
                      background: "var(--hero, #2563eb)",
                      borderRadius: 2,
                      opacity: 0.75,
                    }}
                  />
                </div>
              );
            })}
          </div>
          <div style={{ fontSize: "0.78rem", color: "var(--muted)", marginTop: 6 }}>
            {sortedRuns.length} runs · {sortedRuns[0]?.captured_at_utc?.slice(0, 10)} → {sortedRuns[sortedRuns.length - 1]?.captured_at_utc?.slice(0, 10)}
          </div>
        </DataPanel>
      )}

      {/* Direction split bar */}
      {totalAll > 0 && (
        <div className="direction-split-bar-wrap">
          <div className="direction-split-bar">
            <div className="direction-split-bar-up"   style={{ width: `${pct(totalUp,   totalAll)}%` }} />
            <div className="direction-split-bar-down" style={{ width: `${pct(totalDown, totalAll)}%` }} />
          </div>
          <div className="direction-split-labels">
            <span>{totalUp.toLocaleString()} new/available ({pct(totalUp, totalAll)}%)</span>
            <span>{totalOther.toLocaleString()} price changes</span>
            <span>{totalDown.toLocaleString()} removed/sold-out ({pct(totalDown, totalAll)}%)</span>
          </div>
        </div>
      )}

      {/* All-time change activity */}
      {dailySeries.length > 0 && (
        <DataPanel
          title="All-time change activity"
          copy="Change events per day. Green = new/available fares, red = removed/sold-out, grey = price changes."
        >
          <div className="daily-activity-list">
            {dailySeries.map((item) => (
              <div className="daily-activity-row" key={item.day}>
                <div className="daily-activity-date">{formatShortDay(item.day)}</div>
                <div className="daily-activity-bar-track">
                  <div className="daily-bar-up"    style={{ width: `${pct(item.up,    maxDailyEvents)}%` }} />
                  <div className="daily-bar-down"  style={{ width: `${pct(item.down,  maxDailyEvents)}%` }} />
                  <div className="daily-bar-other" style={{ width: `${pct(item.other, maxDailyEvents)}%` }} />
                </div>
                <div className="daily-activity-count">{item.total.toLocaleString()} events</div>
                <div className="daily-activity-split">
                  <span className="up-text">{item.up.toLocaleString()}↑</span>
                  {" · "}
                  <span className="down-text">{item.down.toLocaleString()}↓</span>
                </div>
              </div>
            ))}
          </div>
        </DataPanel>
      )}

      {/* Analytics 2-col: top routes + biggest moves */}
      {(topRoutes.length > 0 || priceMoves.length > 0) && (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>

          {topRoutes.length > 0 && (
            <section className="card analytics-panel">
              <h2 style={{ margin: "0 0 10px", fontSize: "1.1rem" }}>Top routes by change volume</h2>
              <div style={{ color: "var(--muted)", fontSize: "0.86rem", marginBottom: 14 }}>
                All-time · green = new/available · red = removed/sold-out
              </div>
              <div style={{ display: "grid", gap: 8 }}>
                {topRoutes.map(([routeKey, split]) => (
                  <div className="analytics-row" key={routeKey}>
                    <div className="analytics-row-header">
                      <span className="analytics-row-label">{routeKey}</span>
                      <span className="analytics-row-count">{split.total.toLocaleString()} events</span>
                    </div>
                    <div className="analytics-split-bar">
                      <div className="analytics-split-up"   style={{ width: `${pct(split.up,   split.total || 1)}%` }} />
                      <div className="analytics-split-down" style={{ width: `${pct(split.down, split.total || 1)}%` }} />
                    </div>
                  </div>
                ))}
              </div>
            </section>
          )}

          {priceMoves.length > 0 && (
            <section className="card analytics-panel">
              <h2 style={{ margin: "0 0 10px", fontSize: "1.1rem" }}>Biggest fare moves</h2>
              <div style={{ color: "var(--muted)", fontSize: "0.86rem", marginBottom: 14 }}>
                Largest one-way fare changes detected all time
              </div>
              <div className="moves-card-grid" style={{ gridTemplateColumns: "1fr 1fr" }}>
                {priceMoves.map((ev, i) => (
                  <div className="moves-card" key={i}>
                    <div className="moves-card-badges">
                      <span className="moves-badge">{ev.route_key ?? "-"}</span>
                      <span className="moves-badge airline">{ev.airline ?? "-"}</span>
                    </div>
                    <div className="moves-card-field">{ev.rbd} · OW fare</div>
                    <div className="moves-card-values">
                      <span className="moves-value-old">{ev.old_ow_fare?.toFixed(0)}</span>
                      <span className="moves-arrow">→</span>
                      <span>{ev.new_ow_fare?.toFixed(0)}</span>
                    </div>
                    <div className={`moves-card-magnitude ${ev.delta >= 0 ? "up" : "down"}`}>
                      {ev.delta >= 0 ? "+" : ""}{ev.delta.toFixed(0)}
                    </div>
                    <div className="moves-card-meta">{ev.report_day}</div>
                  </div>
                ))}
              </div>
            </section>
          )}
        </div>
      )}

      {/* Current fares table */}
      {fares.length > 0 && (
        <DataPanel
          title={`Current fares (${fares.length.toLocaleString()} rows)`}
          copy="Latest extraction snapshot. Sold-out and unsaleable fares included for completeness."
        >
          <div className="data-table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Airline</th>
                  <th>Route</th>
                  <th>RBD</th>
                  <th>Cabin</th>
                  <th>Type</th>
                  <th style={{ textAlign: "right" }}>Base Fare</th>
                  <th style={{ textAlign: "right" }}>Total Fare</th>
                  <th>Currency</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {fares.map((row, i) => (
                  <tr key={i}>
                    <td><span style={{ fontFamily: "monospace" }}>{row.airline}</span></td>
                    <td><span style={{ fontFamily: "monospace" }}>{row.route_key}</span></td>
                    <td><span style={{ fontFamily: "monospace" }}>{row.rbd}</span></td>
                    <td>{row.cabin ?? "—"}</td>
                    <td>{row.journey_type}</td>
                    <td style={{ textAlign: "right" }}>{row.base_fare != null ? row.base_fare.toFixed(2) : "—"}</td>
                    <td style={{ textAlign: "right" }}>
                      {row.total_fare != null && row.total_fare > 0 ? row.total_fare.toFixed(2) : "—"}
                    </td>
                    <td>{row.currency ?? "—"}</td>
                    <td>
                      {row.is_sold_out ? (
                        <span className="chip" style={{ background: "var(--alert)", color: "#fff", fontSize: "0.72rem" }}>Sold Out</span>
                      ) : row.is_unsaleable ? (
                        <span className="chip" style={{ fontSize: "0.72rem" }}>Unsaleable</span>
                      ) : (
                        <span className="chip" style={{ background: "var(--good)", color: "#fff", fontSize: "0.72rem" }}>Available</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </DataPanel>
      )}

      {/* Change events table grouped by day */}
      {Object.keys(eventsByDay).length > 0 && (
        <DataPanel
          title="Change events"
          copy="All-time fare changes detected between consecutive extraction runs. Showing most recent 200."
        >
          {Object.entries(eventsByDay)
            .sort(([a], [b]) => b.localeCompare(a))
            .map(([day, evs]) => (
              <div key={day} style={{ marginBottom: 20 }}>
                <div style={{ fontSize: "0.82rem", fontWeight: 600, color: "var(--muted)", marginBottom: 6 }}>
                  {formatShortDay(day)} — {evs.length} events
                </div>
                <div className="data-table-wrap" style={{ border: "none", borderRadius: 0, marginBottom: 0 }}>
                  <table>
                    <thead>
                      <tr>
                        <th>Airline</th>
                        <th>Route</th>
                        <th>RBD</th>
                        <th>Type</th>
                        <th style={{ textAlign: "right" }}>Old OW</th>
                        <th style={{ textAlign: "right" }}>New OW</th>
                        <th style={{ textAlign: "right" }}>Old RT</th>
                        <th style={{ textAlign: "right" }}>New RT</th>
                      </tr>
                    </thead>
                    <tbody>
                      {evs.map((ev, i) => (
                        <tr key={i}>
                          <td><span style={{ fontFamily: "monospace" }}>{ev.airline}</span></td>
                          <td><span style={{ fontFamily: "monospace" }}>{ev.route_key}</span></td>
                          <td><span style={{ fontFamily: "monospace" }}>{ev.rbd}</span></td>
                          <td>
                            <span
                              className="chip"
                              style={{
                                background: changeTypeColor[ev.change_type] ?? "#888",
                                color: "#fff",
                                fontSize: "0.72rem",
                              }}
                            >
                              {ev.change_type.replace("_", " ")}
                            </span>
                          </td>
                          <td style={{ textAlign: "right" }}>{ev.old_ow_fare != null ? ev.old_ow_fare.toFixed(2) : "—"}</td>
                          <td style={{ textAlign: "right" }}>{ev.new_ow_fare != null ? ev.new_ow_fare.toFixed(2) : "—"}</td>
                          <td style={{ textAlign: "right" }}>{ev.old_rt_fare != null ? ev.old_rt_fare.toFixed(2) : "—"}</td>
                          <td style={{ textAlign: "right" }}>{ev.new_rt_fare != null ? ev.new_rt_fare.toFixed(2) : "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            ))}
        </DataPanel>
      )}
    </div>
  );
}

export const metadata = {
  title: "GDS Fare Intelligence",
  description: "Travelport Smartpoint fare and tax data",
};
