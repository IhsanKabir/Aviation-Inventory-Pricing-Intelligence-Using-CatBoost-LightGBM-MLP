import { DataPanel } from "@/components/data-panel";
import { MetricCard } from "@/components/metric-card";
import { getApiBaseUrl, getCycleHealth } from "@/lib/api";
import { formatDhakaDateTime, formatNumber, formatPercent, shortCycle } from "@/lib/format";

export default async function HealthPage() {
  const result = await getCycleHealth();
  const data = result.data;
  const runStatus = data?.latest_run_status;

  return (
    <>
      <h1 className="page-title">Cycle Health</h1>
      <p className="page-copy">
        Freshness, coverage, and latest run integrity for the operational cycle.
        This is the primary validity screen for deciding whether the current outputs are usable.
      </p>

      <div className="grid cards">
        <MetricCard
          label="Cycle"
          value={shortCycle(data?.cycle_id ?? null)}
          footnote={data?.cycle_completed_at_utc ? formatDhakaDateTime(data.cycle_completed_at_utc) : "No cycle available"}
        />
        <MetricCard
          label="Age"
          value={data?.cycle_age_minutes !== null && data?.cycle_age_minutes !== undefined ? `${formatNumber(data.cycle_age_minutes)} min` : "-"}
          footnote={data?.stale ? "Marked stale" : "Fresh enough for monitoring"}
        />
        <MetricCard
          label="Coverage"
          value={formatPercent(data?.route_pair_coverage_pct ?? null)}
          footnote={`${formatNumber(data?.observed_route_pair_count ?? 0)} / ${formatNumber(data?.configured_route_pair_count ?? 0)} configured pairs`}
        />
        <MetricCard
          label="Query completion"
          value={
            runStatus?.overall_query_completed !== null && runStatus?.overall_query_completed !== undefined
              ? `${formatNumber(runStatus.overall_query_completed)} / ${formatNumber(runStatus.overall_query_total ?? 0)}`
              : "-"
          }
          footnote={runStatus?.state ? `${runStatus.state} / ${runStatus.phase ?? "-"}` : "No latest run status file"}
        />
      </div>

      <div className="section-grid">
        <DataPanel
          title="Run integrity"
          copy="Latest file-backed run state and API freshness. If this page is clean, the latest cycle is structurally usable."
        >
          <div className="table-list">
            <div className="table-row">
              <div>
                <strong>API endpoint</strong>
                <span>{getApiBaseUrl()}</span>
              </div>
              <div className={`pill ${result.ok && data?.database_ok ? "good" : "warn"}`}>
                {result.ok && data?.database_ok ? "Online" : "Check"}
              </div>
              <span>{data?.cycle_completed_at_utc ? formatDhakaDateTime(data.cycle_completed_at_utc) : "-"}</span>
            </div>
            <div className="table-row">
              <div>
                <strong>Latest run rows</strong>
                <span>Accumulated row count from latest run status</span>
              </div>
              <div className="pill good">{formatNumber(runStatus?.total_rows_accumulated ?? 0)}</div>
              <span>{runStatus?.completed_at_utc ? formatDhakaDateTime(runStatus.completed_at_utc) : "-"}</span>
            </div>
            <div className="table-row">
              <div>
                <strong>Latest cycle scope</strong>
                <span>Offer rows, airlines, and route count from PostgreSQL</span>
              </div>
              <div className="pill good">{formatNumber(data?.offer_rows ?? 0)} rows</div>
              <span>{formatNumber(data?.airline_count ?? 0)} airlines / {formatNumber(data?.route_count ?? 0)} routes</span>
            </div>
          </div>
        </DataPanel>

        <DataPanel
          title="Missing configured pairs"
          copy="Configured route-airline pairs absent from the latest cycle. This is the fastest gap list for operational follow-up."
        >
          {!data?.missing_route_pairs?.length ? (
            <div className="empty-state">No missing configured pairs detected in the latest cycle sample.</div>
          ) : (
            <div className="missing-grid">
              {data.missing_route_pairs.map((item) => (
                <div className="missing-chip" key={item}>{item}</div>
              ))}
            </div>
          )}
        </DataPanel>
      </div>
    </>
  );
}
