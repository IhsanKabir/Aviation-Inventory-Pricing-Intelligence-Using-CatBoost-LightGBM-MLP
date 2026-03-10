import { DataPanel } from "@/components/data-panel";
import { MetricCard } from "@/components/metric-card";
import { getApiBaseUrl, getDashboardPayload } from "@/lib/api";
import { formatRouteGeo, formatRouteType } from "@/lib/format";

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
    timeZone: "Asia/Dhaka"
  }).format(new Date(value));
}

export default async function HomePage() {
  const payload = await getDashboardPayload();
  const latestCycle = payload.latestCycle.data;
  const airlines = uniqueByKey(payload.airlines.data?.items ?? [], (item) => item.airline)
    .sort((left, right) => (right.offer_rows ?? 0) - (left.offer_rows ?? 0) || left.airline.localeCompare(right.airline));
  const routes = uniqueByKey(payload.routes.data?.items ?? [], (item) => item.route_key)
    .sort((left, right) => (right.offer_rows ?? 0) - (left.offer_rows ?? 0) || left.route_key.localeCompare(right.route_key));
  const health = payload.health.data;
  const cycleHealth = payload.cycleHealth.data;

  return (
    <>
      <section className="hero">
        <div className="eyebrow">Live Platform</div>
        <h1>Warehouse-backed airline intelligence, now operating on the web.</h1>
        <p>
          The web monitor is now the primary interactive surface for route
          monitoring, change review, penalties, taxes, and forecasting. Local
          collection and training still write first to PostgreSQL, then the
          curated analytics layer is synchronized into BigQuery for hosted use.
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
          copy="Hosted API, warehouse-backed views, and cycle coverage are the main operational checkpoints."
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
          title="Current delivery state"
          copy="Core web surfaces are already live. The remaining work is refinement, consistency, and model quality."
        >
          <div className="stack">
            {[
              "Route monitor matrix is live with airline/signal toggles and capture-history expansion",
              "Hosted reporting reads are BigQuery-first for routes, changes, penalties, taxes, and forecasting",
              "Looker Studio forecasting and backtest review is already connected to curated warehouse views",
              "Next major focus after UI fixes is ML/DL improvement and route-level model quality"
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
          copy="Carrier-level presence in the latest synchronized operational cycle."
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
          copy="Current route coverage for the web route-monitor surface."
        >
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


