import Link from "next/link";

import { DataPanel } from "@/components/data-panel";
import { MetricCard } from "@/components/metric-card";
import { getDashboardPayload } from "@/lib/api";
import { formatRouteGeo, formatRouteType } from "@/lib/format";

export const revalidate = 3600;

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

function formatDate(value?: string | null) {
  if (!value) return "Not available";
  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "Asia/Dhaka",
  }).format(new Date(value));
}

export default async function HomePage() {
  const payload = await getDashboardPayload();
  const latestCycle = payload.latestCycle.data;
  const cycleHealth = payload.cycleHealth.data;
  const airlines = uniqueByKey(payload.airlines.data?.items ?? [], (item) => item.airline).sort(
    (left, right) =>
      (right.offer_rows ?? 0) - (left.offer_rows ?? 0) || left.airline.localeCompare(right.airline)
  );
  const routes = uniqueByKey(payload.routes.data?.items ?? [], (item) => item.route_key).sort(
    (left, right) =>
      (right.offer_rows ?? 0) - (left.offer_rows ?? 0) || left.route_key.localeCompare(right.route_key)
  );

  return (
    <>
      <section className="hero">
        <div className="eyebrow">Latest Snapshot</div>
        <h1>Review routes, fares, operations, and market shifts in one place.</h1>
        <p>
          Review the latest published market snapshot, then open live comparison views only
          when you need a fresh request-backed read.
        </p>
      </section>

      <div className="grid cards">
        <MetricCard
          label="Latest update"
          value={formatDate(latestCycle?.cycle_completed_at_utc)}
          footnote="Most recent published market snapshot"
        />
        <MetricCard
          label="Fare rows"
          value={latestCycle?.offer_rows?.toLocaleString() ?? "0"}
          footnote="Current monitored fare records"
        />
        <MetricCard
          label="Airlines"
          value={latestCycle?.airline_count?.toLocaleString() ?? "0"}
          footnote="Distinct carriers in the latest update"
        />
        <MetricCard
          label="Routes"
          value={latestCycle?.route_count?.toLocaleString() ?? "0"}
          footnote="Origin-destination pairs currently covered"
        />
      </div>

      <div className="section-grid">
        <DataPanel title="Coverage snapshot" copy="A quick view of market breadth and monitoring completeness.">
          <div className="table-list">
            <div className="table-row">
              <div>
                <strong>Coverage</strong>
                <span>Configured route-airline pairs currently visible in the latest update</span>
              </div>
              <div className={`pill ${cycleHealth?.stale ? "warn" : "good"}`}>
                {cycleHealth?.route_pair_coverage_pct?.toFixed(1) ?? "0.0"}%
              </div>
              <span>{(cycleHealth?.observed_route_pair_count ?? 0).toLocaleString()} active pairs</span>
            </div>
            <div className="table-row">
              <div>
                <strong>Expected scope</strong>
                <span>Total route-airline pairs tracked in the current monitoring scope</span>
              </div>
              <div className="pill good">
                {(cycleHealth?.configured_route_pair_count ?? 0).toLocaleString()}
              </div>
              <span>{(cycleHealth?.missing_route_pairs?.length ?? 0).toLocaleString()} currently missing</span>
            </div>
            <div className="table-row">
              <div>
                <strong>Freshness</strong>
                <span>Time of the most recent completed update</span>
              </div>
              <div className={`pill ${cycleHealth?.stale ? "warn" : "good"}`}>
                {cycleHealth?.stale ? "Needs refresh" : "Current"}
              </div>
              <span>{formatDate(latestCycle?.cycle_completed_at_utc)}</span>
            </div>
          </div>
        </DataPanel>

        <DataPanel
          title="Primary workspaces"
          copy="Start with the top-level workspace that matches the question you want to answer."
        >
          <div className="stack">
            {[
              {
                href: "/market",
                label: "Market Intelligence",
                desc: "Routes, operations, changes, taxes, penalties, and GDS monitoring from one grouped workspace.",
              },
              {
                href: "/forecasting",
                label: "Forecasting",
                desc: "Next-day price outlook, model leaderboard, route-level confidence, and backtest stability.",
              },
              {
                href: "/downloads",
                label: "Downloads",
                desc: "Operational workbook releases, Smartpoint tooling updates, and packaged desktop utilities.",
              },
            ].map(({ href, label, desc }, idx) => (
              <Link
                href={href}
                className="card roadmap-step"
                key={href}
                style={{ textDecoration: "none", color: "inherit" }}
              >
                <div className="roadmap-step-header">
                  <div className="step-number">{idx + 1}</div>
                  <strong>{label}</strong>
                  <div className="nav-card-arrow">{">"}</div>
                </div>
                <div className="roadmap-step-desc">{desc}</div>
              </Link>
            ))}
          </div>
        </DataPanel>
      </div>

      <div className="section-grid">
        <DataPanel title="Top airlines" copy="Carrier-level presence in the latest monitored update.">
          <div className="table-list">
            {airlines.slice(0, 8).map((item) => (
              <div className="table-row" key={item.airline}>
                <div>
                  <strong>{item.airline}</strong>
                  <span>Last seen {formatDate(item.last_seen_at_utc)}</span>
                </div>
                <div className="pill good">Active</div>
                <span>{(item.offer_rows ?? 0).toLocaleString()} rows</span>
              </div>
            ))}
          </div>
        </DataPanel>

        <DataPanel title="Top routes" copy="Current route coverage in the latest published snapshot.">
          <div className="table-list">
            {routes.slice(0, 8).map((item) => (
              <div className="table-row" key={item.route_key}>
                <div>
                  <strong>{item.route_key}</strong>
                  <span className="route-inline-meta">
                    <span className="route-type-pill" data-type={formatRouteType(item.route_type)}>
                      {formatRouteType(item.route_type)}
                    </span>
                    <span>{formatRouteGeo(item.origin_country_code, item.destination_country_code)}</span>
                  </span>
                </div>
                <div className="pill good">{(item.airlines_present ?? 0).toLocaleString()} airlines</div>
                <span>{(item.offer_rows ?? 0).toLocaleString()} rows</span>
              </div>
            ))}
          </div>
        </DataPanel>
      </div>
    </>
  );
}
