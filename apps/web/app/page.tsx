import { DataPanel } from "@/components/data-panel";
import { MetricCard } from "@/components/metric-card";
import { getApiBaseUrl, getDashboardPayload } from "@/lib/api";

function formatDate(value?: string | null) {
  if (!value) return "Not available";
  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "Asia/Dhaka"
  }).format(new Date(value));
}

export default async function HomePage() {
  const payload = await getDashboardPayload();
  const latestCycle = payload.latestCycle.data;
  const airlines = payload.airlines.data?.items ?? [];
  const routes = payload.routes.data?.items ?? [];
  const health = payload.health.data;
  const cycleHealth = payload.cycleHealth.data;

  return (
    <>
      <section className="hero">
        <div className="eyebrow">Operational Shell</div>
        <h1>PostgreSQL-backed monitoring, ready for the web.</h1>
        <p>
          This shell replaces the heavy workbook as the main interactive surface.
          It reads current cycle state, route coverage, and change summaries from
          the reporting API while BigQuery remains the historical analytics layer.
        </p>
      </section>

      <div className="grid cards">
        <MetricCard
          label="Latest cycle"
          value={latestCycle?.cycle_id?.slice(0, 8) ?? "None"}
          footnote={formatDate(latestCycle?.cycle_completed_at_utc)}
        />
        <MetricCard
          label="Offer rows"
          value={latestCycle?.offer_rows?.toLocaleString() ?? "0"}
          footnote="Current cycle snapshot size"
        />
        <MetricCard
          label="Airlines"
          value={latestCycle?.airline_count?.toLocaleString() ?? "0"}
          footnote="Distinct carriers in the latest cycle"
        />
        <MetricCard
          label="Routes"
          value={latestCycle?.route_count?.toLocaleString() ?? "0"}
          footnote="Origin-destination pairs currently present"
        />
      </div>

      <div className="section-grid">
        <DataPanel
          title="Platform health"
          copy="Root API and reporting endpoints should be the primary interaction layer going forward."
        >
          <div className="table-list">
            <div className="table-row">
              <div>
                <strong>API status</strong>
                <span>{getApiBaseUrl()}</span>
              </div>
              <div className={`pill ${health?.database_ok ? "good" : "warn"}`}>
                {health?.database_ok ? "Online" : "Check"}
              </div>
              <span>{formatDate(health?.latest_cycle_completed_at_utc)}</span>
            </div>
            <div className="table-row">
              <div>
                <strong>Coverage</strong>
                <span>{(cycleHealth?.configured_route_pair_count ?? 0).toLocaleString()} configured route-airline pairs</span>
              </div>
              <div className={`pill ${cycleHealth?.stale ? "warn" : "good"}`}>
                {cycleHealth?.route_pair_coverage_pct?.toFixed(1) ?? "0.0"}%
              </div>
              <span>{(cycleHealth?.observed_route_pair_count ?? 0).toLocaleString()} observed</span>
            </div>
          </div>
        </DataPanel>

        <DataPanel
          title="Immediate build order"
          copy="These are the next engineering moves for replacing workbook interactivity."
        >
          <div className="stack">
            {[
              "Route-first monitor with API-driven airline/signal toggles",
              "Detailed change-history drawer from column_change_events",
              "Penalty and tax tabs",
              "Forecasting and backtest visibility"
            ].map((step, idx) => (
              <div className="card roadmap-step" key={step}>
                <div className="roadmap-step-header">
                  <div className="step-number">{idx + 1}</div>
                  <strong>{step}</strong>
                </div>
              </div>
            ))}
          </div>
        </DataPanel>
      </div>

      <div className="section-grid">
        <DataPanel
          title="Top airlines"
          copy="Carrier-level presence from PostgreSQL metadata."
        >
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

        <DataPanel
          title="Top routes"
          copy="Route metadata for the route-first UI shell."
        >
          <div className="table-list">
            {routes.slice(0, 8).map((item) => (
              <div className="table-row" key={item.route_key}>
                <div>
                  <strong>{item.route_key}</strong>
                  <span>
                    {item.origin} {"->"} {item.destination}
                  </span>
                </div>
                <div className="pill good">
                  {(item.airlines_present ?? 0).toLocaleString()} airlines
                </div>
                <span>{(item.offer_rows ?? 0).toLocaleString()} rows</span>
              </div>
            ))}
          </div>
        </DataPanel>
      </div>
    </>
  );
}


