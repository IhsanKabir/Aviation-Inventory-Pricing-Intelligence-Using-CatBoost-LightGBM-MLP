import Link from "next/link";

import { ReportAccessRequestPanel } from "@/components/report-access-request-panel";
import { getReportAccessRequest } from "@/lib/api";
import {
  getDiscountAccess,
  getDiscountHistory,
  getDiscountReport,
  type DiscountGridBlock,
  type DiscountReportPayload,
} from "@/lib/discounts";
import { firstParam, type RawSearchParams } from "@/lib/query";
import { getCurrentUserSession } from "@/lib/user-auth";

import "./discounts.css";

export const metadata = {
  title: "OTA Discount Comparison",
  description:
    "Channel x airline discount grid across OTA sources — best rate, runner-up, and day-over-day changes.",
};

type PageProps = {
  searchParams?: Promise<RawSearchParams>;
};

/** Mirror the xlsx cell display: plain rates get a % suffix, coupon text gets % after each number. */
function formatCellDisplay(raw: string | undefined): string {
  const trimmed = (raw ?? "").trim();
  if (!trimmed) {
    return "—";
  }
  if (/^-?\d+(\.\d+)?$/.test(trimmed)) {
    return `${trimmed}%`;
  }
  // append % to each standalone number, but NOT one already followed by % (fees
  // like "2% fee") nor a digit inside a word/code (e.g. the "2" in "FT-B2C").
  return trimmed.replace(/(?<![A-Za-z\d.])(\d+(?:\.\d+)?)(?![\d.%A-Za-z])/g, "$1%");
}

function GridBlock({
  title,
  block,
  channelStatus,
}: {
  title: string;
  block: DiscountGridBlock;
  channelStatus: Record<string, string>;
}) {
  const dataRows = block.rows.filter((row) => row.kind !== "sep");
  return (
    <section className="card dg-block" aria-label={title}>
      <header className="dg-block-title">
        <h2>{title}</h2>
        <span className="dg-legend">
          <span className="dg-legend-item dg-legend--highest">Best</span>
          <span className="dg-legend-item dg-legend--second">2nd</span>
          <span className="dg-legend-item dg-legend--changed">Change</span>
        </span>
      </header>
      <div className="data-table-wrap" role="region" tabIndex={0}>
        <table className="dg-table">
          <thead>
            <tr>
              <th scope="col">OTA</th>
              {block.columns.map((airline) => (
                <th scope="col" key={airline}>{airline}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {dataRows.map((row) => (
              <tr key={row.label}>
                <th scope="row">
                  {row.label}
                  {channelStatus[row.label] === "captured_but_empty" ? (
                    <span className="dg-status-warn" title="Capture parsed to zero rows — not a 0% discount">
                      ⚠
                    </span>
                  ) : null}
                </th>
                {block.columns.map((airline) => {
                  const flag = row.highlights?.[airline] ?? "none";
                  const flagClass = flag !== "none" ? ` dg-cell--${flag}` : "";
                  return (
                    <td key={airline} className={`dg-cell${flagClass}`}>
                      {formatCellDisplay(row.cells?.[airline])}
                    </td>
                  );
                })}
              </tr>
            ))}
            <tr className="dg-best-row">
              <th scope="row">Best (OTA)</th>
              {block.columns.map((airline) => (
                <td key={airline}>{block.best?.[airline]?.display ?? "—"}</td>
              ))}
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  );
}

export default async function DiscountComparisonPage({ searchParams }: PageProps) {
  const params = (await searchParams) ?? {};
  const selectedDate = firstParam(params, "date") ?? undefined;
  const requestId = firstParam(params, "request_id") ?? undefined;

  const session = await getCurrentUserSession();
  const { token, user, bridgeFailed } = session;
  const sessionInvalid = "sessionInvalid" in session && session.sessionInvalid;

  if (!user) {
    return (
      <div className="page dg-page">
        <section className="card dg-notice">
          <h1>OTA Discount Comparison</h1>
          {sessionInvalid ? (
            <>
              <p>Your session ended. Sign in again to view the report.</p>
              <Link className="button-link" href="/login?next=/discount-comparison">
                Sign in
              </Link>
            </>
          ) : bridgeFailed ? (
            <>
              <p className="dg-error">
                You&apos;re signed in with Google, but the server couldn&apos;t
                establish your session — this is why sign-in keeps looping. The
                site&apos;s <code>OAUTH_BRIDGE_SECRET</code> must match the
                API&apos;s; an admin needs to set them and redeploy.
              </p>
              <Link className="button-link" href="/login?next=/discount-comparison">
                Try again
              </Link>
            </>
          ) : (
            <>
              <p>Sign in to view the team discount grid.</p>
              <Link className="button-link" href="/login?next=/discount-comparison">
                Sign in
              </Link>
            </>
          )}
        </section>
      </div>
    );
  }

  const [reportResult, historyResult, accessResult, accessRequest] = await Promise.all([
    getDiscountReport(token, selectedDate),
    getDiscountHistory(token),
    getDiscountAccess(token),
    requestId
      ? getReportAccessRequest(requestId)
      : Promise.resolve({ ok: true, data: null, error: undefined }),
  ]);

  if (!reportResult.ok && reportResult.status === 403) {
    return (
      <div className="page dg-page">
        <section className="card dg-notice">
          <h1>OTA Discount Comparison</h1>
          <p>This report needs an approved access request.</p>
        </section>
        <ReportAccessRequestPanel
          pageKey="discount-comparison"
          scope={{}}
          scopeSummary={["Full OTA discount comparison grid (all channels, DOM + INTL)"]}
          request={accessRequest.data ?? null}
          currentUser={user}
          headline="Request discount report access"
          description="An admin approves discount-comparison access once per user; after that the desktop app and this page both unlock."
          submitLabel="Request access"
          resourceLabel="discount report"
        />
      </div>
    );
  }

  if (!reportResult.ok && reportResult.status === 401) {
    // Signed-in cookie but the backend session is expired/revoked (30-day TTL).
    return (
      <div className="page dg-page">
        <section className="card dg-notice">
          <h1>OTA Discount Comparison</h1>
          <p>Your session has expired. Sign in again to view the report.</p>
          <Link className="button-link" href="/login?next=/discount-comparison">Sign in</Link>
        </section>
      </div>
    );
  }

  if (!reportResult.ok && reportResult.status === 404) {
    return (
      <div className="page dg-page">
        <section className="card dg-notice">
          <h1>OTA Discount Comparison</h1>
          <p>
            {selectedDate
              ? `No report is stored for ${selectedDate}.`
              : "No discount report has been synced yet. Run the desktop app against today's HAR captures and press Sync."}
        </p>
        <p>
          <Link className="button-link" href="/discount-comparison/guide">HAR collection guide</Link>
          </p>
          {selectedDate ? (
            <Link className="button-link" href="/discount-comparison">View latest</Link>
          ) : null}
        </section>
      </div>
    );
  }

  if (!reportResult.ok || !reportResult.data) {
    return (
      <div className="page dg-page">
        <section className="card dg-notice">
          <h1>OTA Discount Comparison</h1>
          <p className="dg-error">The report service is unavailable: {reportResult.error}</p>
        </section>
      </div>
    );
  }

  const stored = reportResult.data;
  const report: DiscountReportPayload = stored.report;
  const history = historyResult.data?.items ?? [];
  const plan = accessResult.data?.plan;
  const xlsxHref = `/api/discount-comparison/xlsx${selectedDate ? `?date=${encodeURIComponent(selectedDate)}` : ""}`;

  return (
    <div className="page dg-page">
      {plan ? (
        <div className="dg-plan-strip">
          <span>Your plan</span>
          {plan.end_date ? <span className="dg-plan-chip">valid to {plan.end_date}</span> : <span className="dg-plan-chip">no expiry</span>}
          {plan.use_quota !== null ? (
            <span className="dg-plan-chip">
              {plan.uses_remaining} of {plan.use_quota} syncs left
            </span>
          ) : (
            <span className="dg-plan-chip">unlimited syncs</span>
          )}
        </div>
      ) : null}
      <section className="card dg-header">
        <div>
          <h1>OTA Discount Comparison</h1>
          <p className="dg-subtitle">
            {report.report_date} / {report.report_time}hrs
            {stored.submitted_by_email ? ` · synced by ${stored.submitted_by_email}` : ""}
            {stored.prev_report_date ? ` · changes vs ${stored.prev_report_date}` : " · first stored report (no change diff)"}
          </p>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <Link className="button-link" href="/discount-comparison/guide">HAR guide</Link>
          <a className="button-link" href={xlsxHref}>Download xlsx</a>
        </div>
      </section>

      {report.normalized === false ? (
        <section className="status-banner dg-banner-warn" role="status">
          Not normalized: no exact-base source was available for this run — BDFare/AKIJ are shown on
          their own (altered) base. Capture a FirstTrip B2B HAR to normalize.
        </section>
      ) : null}

      {history.length > 1 ? (
        <nav className="dg-history" aria-label="Report history">
          {history.map((item) => {
            const isActive = item.report_date === stored.report_date;
            return (
              <Link
                key={item.report_id}
                className={`chip dg-history-chip${isActive ? " dg-history-chip--active" : ""}`}
                href={`/discount-comparison?date=${item.report_date}`}
              >
                {item.report_date}
              </Link>
            );
          })}
        </nav>
      ) : null}

      {report.grids.INTL ? (
        <GridBlock
          title={`${report.report_date} (INTERNATIONAL) / ${report.report_time}hrs`}
          block={report.grids.INTL}
          channelStatus={report.channel_status ?? {}}
        />
      ) : null}
      {report.grids.DOM ? (
        <GridBlock
          title={`${report.report_date} (DOMESTIC) / ${report.report_time}hrs`}
          block={report.grids.DOM}
          channelStatus={report.channel_status ?? {}}
        />
      ) : null}
    </div>
  );
}
