import { DataPanel } from "@/components/data-panel";
import { MetricCard } from "@/components/metric-card";
import { getForecastingPayload } from "@/lib/api";
import { formatDhakaDateTime, formatNumber, formatPercent, shortCycle } from "@/lib/format";

function pickMetric(bundle: { overall_eval: Array<Record<string, unknown>> } | null, key: string) {
  if (!bundle?.overall_eval?.length) {
    return null;
  }
  const best = [...bundle.overall_eval]
    .filter((row) => row[key] !== null && row[key] !== undefined)
    .sort((a, b) => Number(a[key]) - Number(b[key]))[0];
  return best ?? null;
}

export default async function ForecastingPage() {
  const payload = await getForecastingPayload();
  const latestPrediction = payload.data?.latest_prediction_bundle ?? null;
  const latestBacktest = payload.data?.latest_backtest_bundle ?? null;

  const bestPredictionMae = pickMetric(latestPrediction, "mae");
  const bestBacktestMae = pickMetric(latestBacktest, "mae");
  const backtestMeta = latestBacktest?.backtest_meta as Record<string, unknown> | null | undefined;
  const backtestSummary = (backtestMeta?.backtest ?? null) as Record<string, unknown> | null;

  return (
    <>
      <h1 className="page-title">Forecasting Console</h1>
      <p className="page-copy">
        ML/DL forecast and backtest results from the latest prediction artifacts.
        This page surfaces bundle freshness, model ranking, route-level evaluation,
        and next-day output without opening CSV files manually.
      </p>

      <div className="grid cards">
        <MetricCard
          label="Latest prediction target"
          value={latestPrediction?.target ?? "None"}
          footnote={latestPrediction?.modified_at_utc ? formatDhakaDateTime(latestPrediction.modified_at_utc) : "No prediction bundle found"}
        />
        <MetricCard
          label="Prediction models"
          value={formatNumber(latestPrediction?.overall_eval?.length ?? 0)}
          footnote={latestPrediction?.bundle_name ?? "No bundle"}
        />
        <MetricCard
          label="Best prediction MAE"
          value={bestPredictionMae?.mae !== null && bestPredictionMae?.mae !== undefined ? formatNumber(bestPredictionMae.mae as number) : "-"}
          footnote={bestPredictionMae?.model ? String(bestPredictionMae.model) : "No eval rows"}
        />
        <MetricCard
          label="Best backtest MAE"
          value={bestBacktestMae?.mae !== null && bestBacktestMae?.mae !== undefined ? formatNumber(bestBacktestMae.mae as number) : "-"}
          footnote={bestBacktestMae?.model ? String(bestBacktestMae.model) : "No backtest bundle"}
        />
      </div>

      <div className="section-grid">
        <DataPanel
          title="Prediction bundle"
          copy="Most recent operational prediction output available in the reports directory."
        >
          {!latestPrediction ? (
            <div className="empty-state">No prediction bundle found.</div>
          ) : (
            <div className="table-list">
              <div className="table-row">
                <div>
                  <strong>Target</strong>
                  <span>{latestPrediction.target}</span>
                </div>
                <div className="pill good">{latestPrediction.bundle_name}</div>
                <span>{formatDhakaDateTime(latestPrediction.modified_at_utc)}</span>
              </div>
              <div className="table-row">
                <div>
                  <strong>Next-day rows</strong>
                  <span>Forward-looking prediction outputs</span>
                </div>
                <div className="pill good">{formatNumber(latestPrediction.next_day.length)}</div>
                <span>{shortCycle(latestPrediction.stamp)}</span>
              </div>
            </div>
          )}
        </DataPanel>

        <DataPanel
          title="Backtest bundle"
          copy="Latest bundle that includes rolling backtest metadata and evaluation."
        >
          {!latestBacktest ? (
            <div className="empty-state">No backtest-enabled bundle found.</div>
          ) : (
            <div className="table-list">
              <div className="table-row">
                <div>
                  <strong>Status</strong>
                  <span>{String(backtestSummary?.status ?? "unknown")}</span>
                </div>
                <div className="pill good">{latestBacktest.bundle_name}</div>
                <span>{formatDhakaDateTime(latestBacktest.modified_at_utc)}</span>
              </div>
              <div className="table-row">
                <div>
                  <strong>Splits</strong>
                  <span>Rolling validation/test windows</span>
                </div>
                <div className="pill good">{formatNumber((backtestSummary?.split_count as number | null | undefined) ?? 0)}</div>
                <span>{String(backtestMeta?.target_column ?? latestBacktest.target)}</span>
              </div>
            </div>
          )}
        </DataPanel>
      </div>

      <div className="stack">
        <DataPanel
          title="Overall evaluation"
          copy="Best available model rows from the latest prediction bundle."
        >
          {!latestPrediction?.overall_eval?.length ? (
            <div className="empty-state">No overall evaluation rows found.</div>
          ) : (
            <div className="data-table-wrap">
              <table className="data-table compact-table">
                <thead>
                  <tr>
                    <th>Model</th>
                    <th>N</th>
                    <th>MAE</th>
                    <th>RMSE</th>
                    <th>MAPE</th>
                    <th>sMAPE</th>
                    <th>Directional accuracy</th>
                    <th>F1 macro</th>
                  </tr>
                </thead>
                <tbody>
                  {latestPrediction.overall_eval.map((row, index) => (
                    <tr key={`${String(row.model)}-${index}`}>
                      <td>{String(row.model ?? "-")}</td>
                      <td>{formatNumber(row.n as number ?? null)}</td>
                      <td>{formatNumber(row.mae as number ?? null)}</td>
                      <td>{formatNumber(row.rmse as number ?? null)}</td>
                      <td>{formatPercent(row.mape_pct as number ?? null)}</td>
                      <td>{formatPercent(row.smape_pct as number ?? null)}</td>
                      <td>{formatPercent(row.directional_accuracy_pct as number ?? null)}</td>
                      <td>{row.f1_macro !== null && row.f1_macro !== undefined ? Number(row.f1_macro).toFixed(3) : "-"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </DataPanel>

        <DataPanel
          title="Route-level evaluation"
          copy="Route performance snapshot from the latest prediction bundle."
        >
          {!latestPrediction?.route_eval?.length ? (
            <div className="empty-state">No route-level evaluation rows found.</div>
          ) : (
            <div className="data-table-wrap">
              <table className="data-table compact-table">
                <thead>
                  <tr>
                    <th>Airline</th>
                    <th>Route</th>
                    <th>Cabin</th>
                    <th>Model</th>
                    <th>N</th>
                    <th>MAE</th>
                    <th>Directional accuracy</th>
                  </tr>
                </thead>
                <tbody>
                  {latestPrediction.route_eval.map((row, index) => (
                    <tr key={`${String(row.airline)}-${String(row.origin)}-${String(row.destination)}-${String(row.model)}-${index}`}>
                      <td>{String(row.airline ?? "-")}</td>
                      <td>{`${String(row.origin ?? "-")}-${String(row.destination ?? "-")}`}</td>
                      <td>{String(row.cabin ?? "-")}</td>
                      <td>{String(row.model ?? "-")}</td>
                      <td>{formatNumber(row.n as number ?? null)}</td>
                      <td>{formatNumber(row.mae as number ?? null)}</td>
                      <td>{formatPercent(row.directional_accuracy_pct as number ?? null)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </DataPanel>

        <DataPanel
          title="Next-day output"
          copy="Forward-looking per-route predictions from the latest available prediction bundle."
        >
          {!latestPrediction?.next_day?.length ? (
            <div className="empty-state">No next-day rows found.</div>
          ) : (
            <div className="data-table-wrap">
              <table className="data-table compact-table">
                <thead>
                  <tr>
                    <th>Predicted day</th>
                    <th>Airline</th>
                    <th>Route</th>
                    <th>Cabin</th>
                    <th>Latest actual</th>
                    <th>Last value</th>
                    <th>Rolling mean 3</th>
                    <th>EWM 0.30</th>
                    <th>MLP q50</th>
                  </tr>
                </thead>
                <tbody>
                  {latestPrediction.next_day.map((row, index) => (
                    <tr key={`${String(row.airline)}-${String(row.origin)}-${String(row.destination)}-${String(row.predicted_for_day)}-${index}`}>
                      <td>{String(row.predicted_for_day ?? "-")}</td>
                      <td>{String(row.airline ?? "-")}</td>
                      <td>{`${String(row.origin ?? "-")}-${String(row.destination ?? "-")}`}</td>
                      <td>{String(row.cabin ?? "-")}</td>
                      <td>{formatNumber(row.latest_actual_value as number ?? null)}</td>
                      <td>{formatNumber(row.pred_last_value as number ?? null)}</td>
                      <td>{formatNumber(row.pred_rolling_mean_3 as number ?? null)}</td>
                      <td>{formatNumber(row["pred_ewm_alpha_0.30"] as number ?? null)}</td>
                      <td>{formatNumber(row.pred_dl_mlp_q50 as number ?? null)}</td>
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
