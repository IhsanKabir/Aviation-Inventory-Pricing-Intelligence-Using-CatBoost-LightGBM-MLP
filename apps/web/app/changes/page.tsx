import { DataPanel } from "@/components/data-panel";

export default function ChangesPage() {
  return (
    <>
      <h1 className="page-title">Change Event Browser</h1>
      <p className="page-copy">
        This screen will use field-level events from
        <span className="mono"> airline_intel.column_change_events </span>
        to expose exact fare, inventory, penalty, and tax movements with proper
        filtering and cycle drilldown.
      </p>

      <div className="stack">
        <DataPanel
          title="Designed query model"
          copy="The UI will request filtered event pages instead of masking a large workbook."
        >
          <div className="table-list">
            <div className="table-row">
              <div>
                <strong>Dimensions</strong>
                <span>route, airline, domain, field, change type, direction</span>
              </div>
              <div className="pill good">Ready</div>
              <span>API scaffold exists</span>
            </div>
            <div className="table-row">
              <div>
                <strong>History drilldown</strong>
                <span>expand one route/date/flight without loading every row in Excel</span>
              </div>
              <div className="pill good">Next</div>
              <span>frontend work</span>
            </div>
          </div>
        </DataPanel>
      </div>
    </>
  );
}
