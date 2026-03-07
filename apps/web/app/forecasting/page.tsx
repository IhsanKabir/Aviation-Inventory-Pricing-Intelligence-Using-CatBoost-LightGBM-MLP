import { DataPanel } from "@/components/data-panel";

export default function ForecastingPage() {
  return (
    <>
      <h1 className="page-title">Forecasting Console</h1>
      <p className="page-copy">
        This page is reserved for ML/DL forecast outputs, backtest scores, and
        route-level confidence bands once the API exposes model result views.
      </p>

      <div className="stack">
        <DataPanel
          title="Planned data surfaces"
          copy="The operational app will expose model outputs separately from historical BI."
        >
          <div className="table-list">
            <div className="table-row">
              <div>
                <strong>Baseline vs ML/DL</strong>
                <span>catboost, lightgbm, mlp against naive and EWMA baselines</span>
              </div>
              <div className="pill good">Planned</div>
              <span>API view needed</span>
            </div>
            <div className="table-row">
              <div>
                <strong>Forward validation</strong>
                <span>compare predicted movement with future observed cycles</span>
              </div>
              <div className="pill good">Planned</div>
              <span>thesis-facing</span>
            </div>
          </div>
        </DataPanel>
      </div>
    </>
  );
}
