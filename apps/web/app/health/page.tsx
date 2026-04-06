import { DataPanel } from "@/components/data-panel";
import { MetricCard } from "@/components/metric-card";
import { requireAdminSession } from "@/lib/admin";
import { getApiBaseUrl, getCycleHealth } from "@/lib/api";
import { formatDhakaDateTime, formatNumber, formatPercent } from "@/lib/format";

export default async function HealthPage() {
  await requireAdminSession("/health");
  const result = await getCycleHealth();
  const data = result.data;
  const runStatus = data?.latest_run_status;
  const runMatchesLatestCycle = runStatus?.matches_latest_cycle !== false;
  const runStatusSource = runStatus?.status_source ?? "worker_heartbeat";
  const latestRunRows = runStatus?.total_rows_accumulated;
  const showRunRowsAsReported =
    runMatchesLatestCycle &&
    latestRunRows !== null &&
    latestRunRows !== undefined &&
    !((latestRunRows ?? 0) === 0 && (data?.offer_rows ?? 0) > 0);
  const runRowsLabel = showRunRowsAsReported ? formatNumber(latestRunRows ?? 0) : "Not reported";
  const runRowsTone = showRunRowsAsReported ? "good" : "warn";
  const runRowsContext = !runStatus
    ? "No latest run status file"
    : runStatusSource === "parallel_aggregate"
      ? "Whole-cycle aggregate artifact from the parallel airline runner"
    : !runMatchesLatestCycle
      ? "Status file belongs to an older cycle, not the latest cycle"
      : "Accumulated row count from latest aligned run status";
  const queryCompletionValue =
    runStatus?.overall_query_completed !== null && runStatus?.overall_query_completed !== undefined
      ? `${formatNumber(runStatus.overall_query_completed)} / ${formatNumber(runStatus.overall_query_total ?? 0)}`
      : runStatusSource === "parallel_aggregate" && runStatus?.aggregate_airline_count !== null && runStatus?.aggregate_airline_count !== undefined
        ? `${formatNumber(runStatus.aggregate_airline_count)} airlines`
        : "-";
  const queryCompletionFootnote = !runStatus
    ? "No latest run status file"
    : runStatusSource === "parallel_aggregate"
      ? `aggregate parallel / ${formatNumber(runStatus.aggregate_failed_count ?? 0)} failed`
      : `${runStatus.state ?? "-"} / ${runStatus.phase ?? "-"}`;

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
          value={data?.cycle_completed_at_utc ? formatDhakaDateTime(data.cycle_completed_at_utc) : "Not available"}
          footnote={data?.cycle_completed_at_utc ? "Latest completed operational cycle" : "No cycle available"}
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
          value={queryCompletionValue}
          footnote={queryCompletionFootnote}
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
                <span>{runRowsContext}</span>
              </div>
              <div className={`pill ${runRowsTone}`}>{runRowsLabel}</div>
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
