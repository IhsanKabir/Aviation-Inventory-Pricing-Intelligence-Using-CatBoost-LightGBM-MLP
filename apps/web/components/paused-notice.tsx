import Link from "next/link";

import { DataPanel } from "@/components/data-panel";

interface PausedNoticeProps {
  /** Page name shown in the heading, e.g. "Routes". */
  section: string;
}

/** Shown in place of a Market Intelligence / Forecasting page while its data is paused. */
export function PausedNotice({ section }: PausedNoticeProps) {
  return (
    <>
      <h1 className="page-title">{section}</h1>
      <DataPanel
        title="Updating soon"
        copy="This section is temporarily paused because of running costs. Live market data will be back soon."
      >
        <div className="table-list">
          <div className="table-row">
            <div>
              <strong>Still available</strong>
              <span>The OTA discount comparison and the desktop downloads are working as usual.</span>
            </div>
            <div className="pill warn">Paused</div>
            <span>
              <Link href="/discount-comparison">OTA Discounts</Link>
              {" · "}
              <Link href="/downloads">Downloads</Link>
            </span>
          </div>
        </div>
      </DataPanel>
    </>
  );
}
