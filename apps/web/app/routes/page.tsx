import { DataPanel } from "@/components/data-panel";

export default function RoutesPage() {
  return (
    <>
      <h1 className="page-title">Route Monitor Shell</h1>
      <p className="page-copy">
        This page will replace workbook-driven route filtering. It will query the
        reporting API for route, airline, and signal selections and show current
        rows plus expandable cycle history.
      </p>

      <div className="stack">
        <DataPanel
          title="Planned interactions"
          copy="These replace slow Excel row hiding and macro-driven UI."
        >
          <div className="table-list">
            <div className="table-row">
              <div>
                <strong>Route selector</strong>
                <span>DAC, CGP, RUH, MCT and other live routes</span>
              </div>
              <div className="pill good">Planned</div>
              <span>API-backed</span>
            </div>
            <div className="table-row">
              <div>
                <strong>Airline toggles</strong>
                <span>Single or multi-select with immediate refresh</span>
              </div>
              <div className="pill good">Planned</div>
              <span>Context + strict modes</span>
            </div>
            <div className="table-row">
              <div>
                <strong>Signal filters</strong>
                <span>Increase, decrease, new, sold out, unknown</span>
              </div>
              <div className="pill good">Planned</div>
              <span>SQL-driven</span>
            </div>
          </div>
        </DataPanel>
      </div>
    </>
  );
}
