import { DataPanel } from "@/components/data-panel";
import { MetricCard } from "@/components/metric-card";
import { getForecastingPayload } from "@/lib/api";
import { formatDhakaDateTime, formatNumber, formatPercent } from "@/lib/format";

type MetricLike = Record<string, unknown>;

type NextDayRow = Record<string, unknown>;

function toNumber(value: unknown) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function formatModelName(value: unknown) {
  const raw = String(value ?? "-");
  const map: Record<string, string> = {
    pred_last_value: "Last value",
    pred_rolling_mean_3: "Rolling mean 3",
    pred_rolling_mean_7: "Rolling mean 7",
    pred_seasonal_naive_7: "Seasonal naive 7",
    "pred_ewm_alpha_0.30": "EWM 0.30",
    pred_ml_catboost_q10: "CatBoost q10",
    pred_ml_catboost_q50: "CatBoost q50",
    pred_ml_catboost_q90: "CatBoost q90",
    pred_ml_lightgbm_q10: "LightGBM q10",
    pred_ml_lightgbm_q50: "LightGBM q50",
    pred_ml_lightgbm_q90: "LightGBM q90",
    pred_dl_mlp_q10: "MLP q10",
    pred_dl_mlp_q50: "MLP q50",
    pred_dl_mlp_q90: "MLP q90"
  };
  return map[raw] ?? raw.replaceAll("_", " ");
}

function rankByMetric(rows: MetricLike[] | undefined, metric: string, direction: "asc" | "desc" = "asc") {
  return [...(rows ?? [])]
    .filter((row) => toNumber(row[metric]) !== null)
    .sort((left, right) => {
      const a = toNumber(left[metric]) ?? 0;
      const b = toNumber(right[metric]) ?? 0;
      return direction === "asc" ? a - b : b - a;
    });
}

function pickPreferredForecast(row: NextDayRow) {
  const options = [
    "pred_dl_mlp_q50",
    "pred_ml_catboost_q50",
    "pred_ml_lightgbm_q50",
    "pred_rolling_mean_3",
    "pred_last_value"
  ];

  for (const key of options) {
    const value = toNumber(row[key]);
    if (value !== null) {
      return { key, value };
    }
  }

  return { key: "none", value: null };
}

function buildNextDayWatchlist(rows: NextDayRow[] | undefined) {
  return [...(rows ?? [])]
    .map((row) => {
      const preferred = pickPreferredForecast(row);
      const latestActual = toNumber(row.latest_actual_value);
      const delta = preferred.value !== null && latestActual !== null ? preferred.value - latestActual : null;
      return {
        row,
        preferredKey: preferred.key,
        preferredValue: preferred.value,
        latestActual,
        delta,
        absDelta: delta === null ? -1 : Math.abs(delta)
      };
    })
    .filter((item) => item.preferredValue !== null)
    .sort((left, right) => right.absDelta - left.absDelta);
}

function assessForecastConfidence(mae: number | null, directionalAccuracy: number | null) {
  if (directionalAccuracy !== null && directionalAccuracy >= 65 && (mae === null || mae <= 800)) {
    return { label: "High confidence", tone: "good" as const };
  }
  if (directionalAccuracy !== null && directionalAccuracy >= 58 && (mae === null || mae <= 1500)) {
    return { label: "Moderate confidence", tone: "warn" as const };
  }
  return { label: "Low confidence", tone: "warn" as const };
}

function describeVolatility(mae: number | null, directionalAccuracy: number | null) {
  if ((mae !== null && mae >= 1500) || (directionalAccuracy !== null && directionalAccuracy < 55)) {
    return "High-volatility route";
  }
  if ((mae !== null && mae <= 600) && (directionalAccuracy !== null && directionalAccuracy >= 65)) {
    return "Stable market";
  }
  return "Mixed signal";
}

function recommendAction(delta: number | null, confidenceLabel: string) {
  const absDelta = Math.abs(delta ?? 0);
  if (absDelta >= 1500) {
    return "Watch closely";
  }
  if (absDelta >= 700) {
    return "Review pricing";
  }
  if (confidenceLabel === "Low confidence") {
    return "Monitor only";
  }
  return "Stable market";
}

export default async function ForecastingPage() {
  const payload = await getForecastingPayload();
  const latestPrediction = payload.data?.latest_prediction_bundle ?? null;
  const latestBacktest = payload.data?.latest_backtest_bundle ?? null;
  const source = payload.data?.source ?? "unknown";
  const sourceWarning = payload.data?.warning ?? null;

  const overallEval = rankByMetric(latestPrediction?.overall_eval, "mae", "asc");
  const routeEval = latestPrediction?.route_eval ?? [];
  const harderRoutes = rankByMetric(routeEval, "mae", "desc").filter((row) => (toNumber(row.n) ?? 0) >= 5).slice(0, 8);
  const cleanerRoutes = rankByMetric(routeEval, "mae", "asc").filter((row) => (toNumber(row.n) ?? 0) >= 5).slice(0, 8);
  const routeWinners = [...(latestPrediction?.route_winners ?? [])]
    .filter((row) => (toNumber(row.winner_n) ?? 0) >= 5)
    .sort((left, right) => (toNumber(left.winner_mae) ?? 999999) - (toNumber(right.winner_mae) ?? 999999))
    .slice(0, 10);
  const backtestRouteWinners = [...(latestBacktest?.backtest_route_winners ?? [])]
    .filter((row) => (toNumber(row.winner_n) ?? 0) >= 5)
    .sort((left, right) => (toNumber(left.winner_mae) ?? 999999) - (toNumber(right.winner_mae) ?? 999999))
    .slice(0, 10);
  const bestPredictionMae = overallEval[0] ?? null;
  const bestDirectional = rankByMetric(latestPrediction?.overall_eval, "directional_accuracy_pct", "desc")[0] ?? null;
  const nextDayWatchlist = buildNextDayWatchlist(latestPrediction?.next_day).slice(0, 12);
  const backtestLeaderboard = rankByMetric(latestBacktest?.backtest_eval, "mae", "asc").slice(0, 10);
  const backtestMeta = latestBacktest?.backtest_meta as Record<string, unknown> | null | undefined;
  const backtestSummary = (backtestMeta?.backtest ?? null) as Record<string, unknown> | null;
  const overallConfidence = assessForecastConfidence(
    toNumber(bestPredictionMae?.mae),
    toNumber(bestDirectional?.directional_accuracy_pct)
  );
  const operationalPosture =
    overallConfidence.label === "Low confidence"
      ? "Use this as an early warning screen, not an automated pricing signal."
      : overallConfidence.label === "Moderate confidence"
        ? "Use forecast direction as a decision aid, then confirm against route movement and tax context."
        : "Forecast direction is strong enough to support active pricing review on prioritized routes.";
  const routeWinnerMap = new Map(
    routeWinners.map((row) => [
      `${String(row.route_key ?? `${String(row.origin ?? "-")}-${String(row.destination ?? "-")}`)}|${String(row.airline ?? "-")}|${String(row.cabin ?? "-")}`,
      row
    ])
  );
  const actionWatchlist = nextDayWatchlist.map((item) => {
    const routeKey = `${String(item.row.origin ?? "-")}-${String(item.row.destination ?? "-")}`;
    const winner = routeWinnerMap.get(`${routeKey}|${String(item.row.airline ?? "-")}|${String(item.row.cabin ?? "-")}`);
    const confidence = assessForecastConfidence(
      toNumber(winner?.winner_mae),
      toNumber(winner?.winner_directional_accuracy_pct)
    );
    return {
      ...item,
      routeKey,
      confidence,
      recommendation: recommendAction(item.delta, confidence.label),
      volatility: describeVolatility(toNumber(winner?.winner_mae), toNumber(winner?.winner_directional_accuracy_pct))
    };
  });

  return (
    <>
      <h1 className="page-title">Forecasting Console</h1>
      <p className="page-copy">
        Warehouse-backed forecast review for operational price movement work. The page is organized for
        decision-making first: strongest models, hardest markets, next-day watchlist, then backtest stability.
      </p>
      {sourceWarning ? <div className="status-banner warn">{sourceWarning}</div> : null}

      <section className="forecast-banner card">
        <div>
          <div className="forecast-banner-label">Live source</div>
          <h2>{latestPrediction?.target ?? "No prediction bundle"}</h2>
          <p>
            {latestPrediction?.modified_at_utc
              ? `Latest prediction bundle updated ${formatDhakaDateTime(latestPrediction.modified_at_utc)} from ${source}.`
              : "No prediction bundle is currently available from the warehouse."}
          </p>
        </div>
        <div className="forecast-banner-meta">
          <div className="forecast-inline-stat">
            <span>Bundle</span>
            <strong>{latestPrediction?.bundle_name ?? "None"}</strong>
          </div>
          <div className="forecast-inline-stat">
            <span>Updated</span>
            <strong>{latestPrediction?.modified_at_utc ? formatDhakaDateTime(latestPrediction.modified_at_utc) : "Not available"}</strong>
          </div>
          <div className="forecast-inline-stat">
            <span>Source</span>
            <strong>{source}</strong>
          </div>
        </div>
      </section>

      <div className="grid cards">
        <MetricCard
          label="Prediction models"
          value={formatNumber(latestPrediction?.overall_eval?.length ?? 0)}
          footnote={latestPrediction?.bundle_name ?? "No bundle"}
        />
        <MetricCard
          label="Best prediction MAE"
          value={bestPredictionMae?.mae !== null && bestPredictionMae?.mae !== undefined ? formatNumber(bestPredictionMae.mae as number) : "-"}
          footnote={bestPredictionMae?.model ? formatModelName(bestPredictionMae.model) : "No eval rows"}
        />
        <MetricCard
          label="Best directional accuracy"
          value={bestDirectional?.directional_accuracy_pct !== null && bestDirectional?.directional_accuracy_pct !== undefined ? formatPercent(bestDirectional.directional_accuracy_pct as number) : "-"}
          footnote={bestDirectional?.model ? formatModelName(bestDirectional.model) : "No directional rows"}
        />
        <MetricCard
          label="Backtest splits"
          value={formatNumber((backtestSummary?.split_count as number | null | undefined) ?? 0)}
          footnote={latestBacktest?.bundle_name ?? "No backtest bundle"}
        />
      </div>

      <div className="section-grid forecast-grid">
        <DataPanel
          title="Decision frame"
          copy="Operational interpretation of the current bundle. This answers whether the forecast is ready for action or only for monitoring."
        >
          <div className="table-list">
            <div className="table-row">
              <div>
                <strong>Model confidence</strong>
                <span>{operationalPosture}</span>
              </div>
              <div className={`pill ${overallConfidence.tone}`}>{overallConfidence.label}</div>
              <span>
                Best directional {formatPercent(toNumber(bestDirectional?.directional_accuracy_pct))} · MAE {formatNumber(toNumber(bestPredictionMae?.mae))}
              </span>
            </div>
            <div className="table-row">
              <div>
                <strong>Action posture</strong>
                <span>Highest-signal rows from the current next-day watchlist.</span>
              </div>
              <div className={`pill ${actionWatchlist.length ? "warn" : "good"}`}>{Math.min(actionWatchlist.length, 6)} routes</div>
              <span>{actionWatchlist[0]?.recommendation ?? "No active route recommendation"}</span>
            </div>
          </div>
        </DataPanel>

        <DataPanel
          title="Prediction bundle"
          copy="Operational prediction bundle currently backing the web and BI surfaces."
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
                <span>{latestPrediction.modified_at_utc ? formatDhakaDateTime(latestPrediction.modified_at_utc) : "Not available"}</span>
              </div>
            </div>
          )}
        </DataPanel>

        <DataPanel
          title="Backtest bundle"
          copy="Latest backtest-enabled bundle for rolling validation review."
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
                  <strong>Selection metric</strong>
                  <span>Winning model chosen on validation windows</span>
                </div>
                <div className="pill good">{String(backtestMeta?.backtest_selection_metric ?? "mae")}</div>
                <span>{String(backtestMeta?.target_column ?? latestBacktest.target)}</span>
              </div>
            </div>
          )}
        </DataPanel>
      </div>

      <div className="stack">
        <DataPanel
          title="Action watchlist"
          copy="Decision-oriented shortlist combining next-day forecast gap, route-level confidence, and volatility framing."
        >
          {!actionWatchlist.length ? (
            <div className="empty-state">No decision watchlist rows qualified from the latest bundle.</div>
          ) : (
            <div className="table-list compact-list">
              {actionWatchlist.slice(0, 6).map((item, index) => (
                <div
                  className="table-row"
                  key={`${String(item.row.airline)}-${item.routeKey}-${String(item.row.predicted_for_day)}-action-${index}`}
                >
                  <div>
                    <strong>{item.routeKey}</strong>
                    <span>{`${String(item.row.airline ?? "-")} · ${String(item.row.cabin ?? "-")} · ${item.volatility}`}</span>
                  </div>
                  <div className={`pill ${item.confidence.tone}`}>{item.recommendation}</div>
                  <span>
                    {item.delta !== null ? `${item.delta > 0 ? "+" : ""}${formatNumber(item.delta)}` : "-"} · {item.confidence.label}
                  </span>
                </div>
              ))}
            </div>
          )}
        </DataPanel>

        <DataPanel
          title="Model leaderboard"
          copy="Lowest-error models from the latest prediction bundle. This is the page to check before trusting the next-day outputs."
        >
          {!overallEval.length ? (
            <div className="empty-state">No overall evaluation rows found.</div>
          ) : (
            <div className="data-table-wrap">
              <table className="data-table compact-table forecasting-table">
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Model</th>
                    <th>N</th>
                    <th>MAE</th>
                    <th>RMSE</th>
                    <th>MAPE</th>
                    <th>Directional accuracy</th>
                    <th>F1 macro</th>
                  </tr>
                </thead>
                <tbody>
                  {(() => {
                    const rows = overallEval.slice(0, 12);
                    const maxMae = Math.max(1, ...rows.map((r) => toNumber(r.mae) ?? 0));
                    return rows.map((row, index) => {
                      const mae = toNumber(row.mae);
                      const barPct = mae !== null ? Math.round((mae / maxMae) * 100) : 0;
                      const isWorse = index >= Math.ceil(rows.length / 2);
                      return (
                        <tr key={`${String(row.model)}-${index}`}>
                          <td>{index + 1}</td>
                          <td>{formatModelName(row.model)}</td>
                          <td>{formatNumber(toNumber(row.n))}</td>
                          <td>
                            <div className="mae-bar-cell">
                              <span className="mae-bar-value">{formatNumber(mae)}</span>
                              <div className="mae-bar-track">
                                <div className={`mae-bar-fill${isWorse ? " warn" : ""}`} style={{ width: `${barPct}%` }} />
                              </div>
                            </div>
                          </td>
                          <td>{formatNumber(toNumber(row.rmse))}</td>
                          <td>{formatPercent(toNumber(row.mape_pct))}</td>
                          <td>{formatPercent(toNumber(row.directional_accuracy_pct))}</td>
                          <td>{toNumber(row.f1_macro) !== null ? Number(row.f1_macro).toFixed(3) : "-"}</td>
                        </tr>
                      );
                    });
                  })()}
                </tbody>
              </table>
            </div>
          )}
        </DataPanel>

        <div className="section-grid forecast-grid-split">
          <DataPanel
            title="Hardest route segments"
            copy="Highest route-level MAE among groups with at least 5 scored rows. These are the markets where the model family is still weakest."
          >
            {!harderRoutes.length ? (
              <div className="empty-state">No difficult route rows qualified under the current bundle.</div>
            ) : (
              <div className="table-list compact-list">
                {harderRoutes.map((row, index) => (
                  <div className="table-row" key={`${String(row.airline)}-${String(row.origin)}-${String(row.destination)}-${String(row.model)}-${index}`}>
                    <div>
                      <strong>{`${String(row.origin ?? "-")}-${String(row.destination ?? "-")}`}</strong>
                      <span>{`${String(row.airline ?? "-")} · ${String(row.cabin ?? "-")} · ${formatModelName(row.model)}`}</span>
                    </div>
                    <div className="pill warn">MAE {formatNumber(toNumber(row.mae))}</div>
                    <span>{formatPercent(toNumber(row.directional_accuracy_pct))}</span>
                  </div>
                ))}
              </div>
            )}
          </DataPanel>

          <DataPanel
            title="Cleanest route segments"
            copy="Lowest route-level MAE among groups with at least 5 scored rows. Use this as the confidence side of the market map."
          >
            {!cleanerRoutes.length ? (
              <div className="empty-state">No strong route rows qualified under the current bundle.</div>
            ) : (
              <div className="table-list compact-list">
                {cleanerRoutes.map((row, index) => (
                  <div className="table-row" key={`${String(row.airline)}-${String(row.origin)}-${String(row.destination)}-${String(row.model)}-clean-${index}`}>
                    <div>
                      <strong>{`${String(row.origin ?? "-")}-${String(row.destination ?? "-")}`}</strong>
                      <span>{`${String(row.airline ?? "-")} · ${String(row.cabin ?? "-")} · ${formatModelName(row.model)}`}</span>
                    </div>
                    <div className="pill good">MAE {formatNumber(toNumber(row.mae))}</div>
                    <span>{formatPercent(toNumber(row.directional_accuracy_pct))}</span>
                  </div>
                ))}
              </div>
            )}
          </DataPanel>
        </div>

        <div className="section-grid forecast-grid-split">
          <DataPanel
            title="Current route winners"
            copy="Per-route winning model from the latest prediction bundle. This is the operational ML/DL selection layer."
          >
            {!routeWinners.length ? (
              <div className="empty-state">No route-winner rows available in the latest prediction bundle.</div>
            ) : (
              <div className="data-table-wrap">
                <table className="data-table compact-table forecasting-table">
                  <thead>
                    <tr>
                      <th>Route</th>
                      <th>Airline</th>
                      <th>Cabin</th>
                      <th>Winner model</th>
                      <th>N</th>
                      <th>MAE</th>
                      <th>Directional accuracy</th>
                    </tr>
                  </thead>
                  <tbody>
                    {routeWinners.map((row, index) => (
                      <tr key={`${String(row.airline)}-${String(row.route_key)}-${String(row.winner_model)}-${index}`}>
                        <td>{String(row.route_key ?? `${String(row.origin ?? "-")}-${String(row.destination ?? "-")}`)}</td>
                        <td>{String(row.airline ?? "-")}</td>
                        <td>{String(row.cabin ?? "-")}</td>
                        <td>{formatModelName(row.winner_model)}</td>
                        <td>{formatNumber(toNumber(row.winner_n))}</td>
                        <td>{formatNumber(toNumber(row.winner_mae))}</td>
                        <td>{formatPercent(toNumber(row.winner_directional_accuracy_pct))}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </DataPanel>

          <DataPanel
            title="Backtest route winners"
            copy="Route-level winners confirmed during backtest windows. Use this to judge whether the current winning model is stable, not accidental."
          >
            {!backtestRouteWinners.length ? (
              <div className="empty-state">No backtest route-winner rows available in the latest backtest bundle.</div>
            ) : (
              <div className="data-table-wrap">
                <table className="data-table compact-table forecasting-table">
                  <thead>
                    <tr>
                      <th>Split</th>
                      <th>Route</th>
                      <th>Dataset</th>
                      <th>Winner model</th>
                      <th>N</th>
                      <th>MAE</th>
                      <th>Selected on val</th>
                    </tr>
                  </thead>
                  <tbody>
                    {backtestRouteWinners.map((row, index) => (
                      <tr key={`${String(row.split_id)}-${String(row.route_key)}-${String(row.winner_model)}-${index}`}>
                        <td>{String(row.split_id ?? "-")}</td>
                        <td>{String(row.route_key ?? `${String(row.origin ?? "-")}-${String(row.destination ?? "-")}`)}</td>
                        <td>{String(row.dataset ?? "-")}</td>
                        <td>{formatModelName(row.winner_model)}</td>
                        <td>{formatNumber(toNumber(row.winner_n))}</td>
                        <td>{formatNumber(toNumber(row.winner_mae))}</td>
                        <td>{row.selected_on_val ? "Yes" : "No"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </DataPanel>
        </div>

        <DataPanel
          title="Next-day watchlist"
          copy="Rows sorted by the largest absolute gap between the current actual value and the strongest available next-day point forecast."
        >
          {!nextDayWatchlist.length ? (
            <div className="empty-state">No next-day rows found.</div>
          ) : (
            <div className="data-table-wrap">
              <table className="data-table compact-table forecasting-table">
                <thead>
                  <tr>
                    <th>Predicted day</th>
                    <th>Route</th>
                    <th>Airline</th>
                    <th>Cabin</th>
                    <th>Latest actual</th>
                    <th>Preferred forecast</th>
                    <th>Forecast model</th>
                    <th>Delta</th>
                    <th>Confidence</th>
                    <th>Recommendation</th>
                  </tr>
                </thead>
                <tbody>
                  {actionWatchlist.map((item, index) => (
                    <tr key={`${String(item.row.airline)}-${String(item.row.origin)}-${String(item.row.destination)}-${String(item.row.predicted_for_day)}-${index}`}>
                      <td>{String(item.row.predicted_for_day ?? "-")}</td>
                      <td>{`${String(item.row.origin ?? "-")}-${String(item.row.destination ?? "-")}`}</td>
                      <td>{String(item.row.airline ?? "-")}</td>
                      <td>{String(item.row.cabin ?? "-")}</td>
                      <td>{formatNumber(item.latestActual)}</td>
                      <td>{formatNumber(item.preferredValue)}</td>
                      <td>{formatModelName(item.preferredKey)}</td>
                      <td className={item.delta !== null && item.delta < 0 ? "forecast-delta-down" : "forecast-delta-up"}>
                        {item.delta !== null ? `${item.delta > 0 ? "+" : ""}${formatNumber(item.delta)}` : "-"}
                      </td>
                      <td>{item.confidence.label}</td>
                      <td>{item.recommendation}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </DataPanel>

        <DataPanel
          title="Backtest stability"
          copy="Best backtest rows from the latest bundle. Treat this as the model reliability screen before acting on forecast outputs."
        >
          {!backtestLeaderboard.length ? (
            <div className="empty-state">No backtest evaluation rows found.</div>
          ) : (
            <div className="data-table-wrap">
              <table className="data-table compact-table forecasting-table">
                <thead>
                  <tr>
                    <th>Split</th>
                    <th>Dataset</th>
                    <th>Model</th>
                    <th>Selected</th>
                    <th>N</th>
                    <th>MAE</th>
                    <th>RMSE</th>
                    <th>Directional accuracy</th>
                  </tr>
                </thead>
                <tbody>
                  {backtestLeaderboard.map((row, index) => (
                    <tr key={`${String(row.split_id)}-${String(row.dataset)}-${String(row.model)}-${index}`}>
                      <td>{String(row.split_id ?? "-")}</td>
                      <td>{String(row.dataset ?? "-")}</td>
                      <td>{formatModelName(row.model)}</td>
                      <td>{row.selected_on_val ? "Yes" : "No"}</td>
                      <td>{formatNumber(toNumber(row.n))}</td>
                      <td>{formatNumber(toNumber(row.mae))}</td>
                      <td>{formatNumber(toNumber(row.rmse))}</td>
                      <td>{formatPercent(toNumber(row.directional_accuracy_pct))}</td>
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

