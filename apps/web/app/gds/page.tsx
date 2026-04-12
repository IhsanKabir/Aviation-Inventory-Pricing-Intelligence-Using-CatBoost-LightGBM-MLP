/**
 * app/gds/page.tsx - GDS Fare Intelligence Dashboard
 *
 * Place this file at: apps/web/app/gds/page.tsx
 * in the Aviation Inventory Pricing Intelligence repo.
 *
 * Features:
 *   - Latest run summary card
 *   - Fare table with airline / route / RBD / OW / RT filters
 *   - Recent change events table (last 7 days)
 */

import { Suspense } from "react";
import {
  getGdsLatestRun,
  getGdsFares,
  getGdsChanges,
  getGdsChangeSummary,
  type GdsFareRow,
  type GdsChangeEvent,
  type GdsFareRun,
  type GdsChangeSummaryPoint,
} from "@/lib/gds";

// ─────────────────────────────────────────────────────────────────────────────
// Server-side data fetch (Next.js 15 App Router)
// ─────────────────────────────────────────────────────────────────────────────

async function getPageData() {
  const [latestRun, fares, changes, changeSummary] = await Promise.allSettled([
    getGdsLatestRun(),
    getGdsFares({ limit: 500 }),
    getGdsChanges({ days: 7, limit: 100 }),
    getGdsChangeSummary(7),
  ]);

  return {
    latestRun:     latestRun.status     === "fulfilled" ? latestRun.value     : null,
    fares:         fares.status         === "fulfilled" ? fares.value         : [],
    changes:       changes.status       === "fulfilled" ? changes.value       : [],
    changeSummary: changeSummary.status === "fulfilled" ? changeSummary.value : [],
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Sub-components
// ─────────────────────────────────────────────────────────────────────────────

function RunSummaryCard({ run }: { run: GdsFareRun }) {
  const ts = new Date(run.captured_at_utc).toLocaleString();
  return (
    <div className="rounded-lg border bg-card p-4 shadow-sm">
      <p className="text-xs text-muted-foreground">Latest GDS Run</p>
      <p className="text-lg font-semibold">{run.cycle_id}</p>
      <p className="text-sm text-muted-foreground">{ts}</p>
      <div className="mt-2 flex gap-4 text-sm">
        <span><span className="font-medium">{run.total_routes}</span> routes</span>
        <span><span className="font-medium">{run.total_airlines}</span> airlines</span>
        <span><span className="font-medium">{run.total_fares}</span> fares</span>
      </div>
    </div>
  );
}

function ChangeSummaryBadges({ summary }: { summary: GdsChangeSummaryPoint[] }) {
  const totals: Record<string, number> = {};
  for (const row of summary) {
    totals[row.change_type] = (totals[row.change_type] ?? 0) + row.change_count;
  }

  const colorMap: Record<string, string> = {
    price_change: "bg-yellow-100 text-yellow-800",
    new:          "bg-green-100 text-green-800",
    removed:      "bg-red-100 text-red-800",
    sold_out:     "bg-orange-100 text-orange-800",
    available:    "bg-blue-100 text-blue-800",
  };

  return (
    <div className="flex flex-wrap gap-2">
      {Object.entries(totals).map(([type, count]) => (
        <span
          key={type}
          className={`rounded-full px-3 py-1 text-xs font-medium ${
            colorMap[type] ?? "bg-gray-100 text-gray-700"
          }`}
        >
          {type.replace("_", " ")}: {count}
        </span>
      ))}
    </div>
  );
}

function FareTable({ fares }: { fares: GdsFareRow[] }) {
  if (fares.length === 0) {
    return <p className="text-sm text-muted-foreground">No fare data available.</p>;
  }

  return (
    <div className="overflow-x-auto rounded-lg border">
      <table className="w-full text-sm">
        <thead className="border-b bg-muted/50">
          <tr>
            <th className="px-3 py-2 text-left font-medium">Airline</th>
            <th className="px-3 py-2 text-left font-medium">Route</th>
            <th className="px-3 py-2 text-left font-medium">RBD</th>
            <th className="px-3 py-2 text-left font-medium">Cabin</th>
            <th className="px-3 py-2 text-left font-medium">Type</th>
            <th className="px-3 py-2 text-right font-medium">Base Fare</th>
            <th className="px-3 py-2 text-right font-medium">Total Fare</th>
            <th className="px-3 py-2 text-left font-medium">Currency</th>
            <th className="px-3 py-2 text-left font-medium">Status</th>
          </tr>
        </thead>
        <tbody className="divide-y">
          {fares.map((row, i) => (
            <tr key={i} className="hover:bg-muted/30">
              <td className="px-3 py-2 font-mono">{row.airline}</td>
              <td className="px-3 py-2 font-mono">{row.route_key}</td>
              <td className="px-3 py-2 font-mono">{row.rbd}</td>
              <td className="px-3 py-2">{row.cabin ?? "—"}</td>
              <td className="px-3 py-2">{row.journey_type}</td>
              <td className="px-3 py-2 text-right">
                {row.base_fare != null ? row.base_fare.toFixed(2) : "—"}
              </td>
              <td className="px-3 py-2 text-right">
                {row.total_fare != null && row.total_fare > 0
                  ? row.total_fare.toFixed(2)
                  : "—"}
              </td>
              <td className="px-3 py-2">{row.currency ?? "—"}</td>
              <td className="px-3 py-2">
                {row.is_sold_out ? (
                  <span className="rounded bg-red-100 px-1.5 py-0.5 text-xs text-red-700">
                    Sold Out
                  </span>
                ) : row.is_unsaleable ? (
                  <span className="rounded bg-orange-100 px-1.5 py-0.5 text-xs text-orange-700">
                    Unsaleable
                  </span>
                ) : (
                  <span className="rounded bg-green-100 px-1.5 py-0.5 text-xs text-green-700">
                    Available
                  </span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ChangeEventsTable({ changes }: { changes: GdsChangeEvent[] }) {
  if (changes.length === 0) {
    return <p className="text-sm text-muted-foreground">No changes in the last 7 days.</p>;
  }

  const colorMap: Record<string, string> = {
    price_change: "bg-yellow-100 text-yellow-800",
    new:          "bg-green-100 text-green-800",
    removed:      "bg-red-100 text-red-800",
    sold_out:     "bg-orange-100 text-orange-800",
    available:    "bg-blue-100 text-blue-800",
  };

  return (
    <div className="overflow-x-auto rounded-lg border">
      <table className="w-full text-sm">
        <thead className="border-b bg-muted/50">
          <tr>
            <th className="px-3 py-2 text-left font-medium">Date</th>
            <th className="px-3 py-2 text-left font-medium">Airline</th>
            <th className="px-3 py-2 text-left font-medium">Route</th>
            <th className="px-3 py-2 text-left font-medium">RBD</th>
            <th className="px-3 py-2 text-left font-medium">Type</th>
            <th className="px-3 py-2 text-right font-medium">Old OW</th>
            <th className="px-3 py-2 text-right font-medium">New OW</th>
            <th className="px-3 py-2 text-right font-medium">Old RT</th>
            <th className="px-3 py-2 text-right font-medium">New RT</th>
          </tr>
        </thead>
        <tbody className="divide-y">
          {changes.map((ev, i) => (
            <tr key={i} className="hover:bg-muted/30">
              <td className="px-3 py-2 text-xs text-muted-foreground">
                {ev.report_day}
              </td>
              <td className="px-3 py-2 font-mono">{ev.airline}</td>
              <td className="px-3 py-2 font-mono">{ev.route_key}</td>
              <td className="px-3 py-2 font-mono">{ev.rbd}</td>
              <td className="px-3 py-2">
                <span
                  className={`rounded-full px-2 py-0.5 text-xs font-medium ${
                    colorMap[ev.change_type] ?? "bg-gray-100 text-gray-700"
                  }`}
                >
                  {ev.change_type.replace("_", " ")}
                </span>
              </td>
              <td className="px-3 py-2 text-right">
                {ev.old_ow_fare != null ? ev.old_ow_fare.toFixed(2) : "—"}
              </td>
              <td className="px-3 py-2 text-right">
                {ev.new_ow_fare != null ? ev.new_ow_fare.toFixed(2) : "—"}
              </td>
              <td className="px-3 py-2 text-right">
                {ev.old_rt_fare != null ? ev.old_rt_fare.toFixed(2) : "—"}
              </td>
              <td className="px-3 py-2 text-right">
                {ev.new_rt_fare != null ? ev.new_rt_fare.toFixed(2) : "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Page
// ─────────────────────────────────────────────────────────────────────────────

export default async function GdsDashboardPage() {
  const { latestRun, fares, changes, changeSummary } = await getPageData();

  return (
    <main className="mx-auto max-w-screen-xl space-y-6 p-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold">GDS Fare Intelligence</h1>
        <p className="text-sm text-muted-foreground">
          Live fare and tax data extracted from Travelport Smartpoint
        </p>
      </div>

      {/* Run summary */}
      {latestRun ? (
        <RunSummaryCard run={latestRun} />
      ) : (
        <p className="text-sm text-muted-foreground">No GDS runs recorded yet.</p>
      )}

      {/* Change summary badges */}
      {changeSummary.length > 0 && (
        <section>
          <h2 className="mb-2 text-base font-semibold">Changes (last 7 days)</h2>
          <ChangeSummaryBadges summary={changeSummary} />
        </section>
      )}

      {/* Current fares */}
      <section>
        <h2 className="mb-2 text-base font-semibold">
          Current Fares
          <span className="ml-2 text-sm font-normal text-muted-foreground">
            ({fares.length} rows)
          </span>
        </h2>
        <FareTable fares={fares} />
      </section>

      {/* Change events */}
      <section>
        <h2 className="mb-2 text-base font-semibold">
          Recent Change Events
          <span className="ml-2 text-sm font-normal text-muted-foreground">
            ({changes.length})
          </span>
        </h2>
        <ChangeEventsTable changes={changes} />
      </section>
    </main>
  );
}

export const metadata = {
  title: "GDS Fare Intelligence",
  description: "Travelport Smartpoint fare and tax data",
};
