import Link from "next/link";

import "../discounts.css";

export const metadata = {
  title: "HAR Collection Guide — OTA Discount Comparison",
  description:
    "How to capture the HAR files the OTA Discount Report app needs, per site.",
};

const CHANNELS = [
  {
    channel: "FirstTrip B2C",
    site: "(live) / b2c-api.firsttrip.com",
    login: "no",
    steps:
      "Nothing to capture — the app fetches it live using your routes + a FUTURE travel date. Failsafe: if the live fetch is blocked, search on firsttrip.com in the browser and export that HAR (filename containing “b2c-api.firsttrip” or “firsttrip_b2c”).",
    filename: "(none) / firsttrip_b2c",
  },
  {
    channel: "FirstTrip B2B (USBA row)",
    site: "booking.firsttrip.com",
    login: "yes (agent)",
    steps:
      "Search the route/date. IMPORTANT: type the carriers you need into the “Preferred Airline” box (e.g. BS, BG, 2A, VQ) BEFORE searching — results are paginated ~20 pages and only page 1 lands in the HAR, so un-preferred airlines are silently missing. Wait for the spinner to fully stop, then export.",
    filename: "booking.firsttrip",
  },
  {
    channel: "BDFare",
    site: "bdfare.com/searchpad",
    login: "yes (agent)",
    steps: "Search the route/date; optionally click one flight (improves base-fare accuracy). Export.",
    filename: "bdfare",
  },
  {
    channel: "Amy",
    site: "amyweb.amybd.com",
    login: "yes (agent)",
    steps: "Search the route/date. Export.",
    filename: "amyweb / amybd",
  },
  {
    channel: "AKIJ Air",
    site: "akijair.com",
    login: "yes (Google)",
    steps: "Search the route/date, wait for results. Export.",
    filename: "akij",
  },
  {
    channel: "ShareTrip B2C",
    site: "sharetrip.net",
    login: "recommended",
    steps:
      "The search page alone gives only the COMMON rate. For the detailed cell (bKash + card coupons): select a flight → booking page → wait for the DISCOUNT COUPON list → export IMMEDIATELY. Use short per-airline captures (sharetrip bs.har, sharetrip 2a.har, …) — long sessions lose response bodies.",
    filename: "sharetrip",
  },
  {
    channel: "GoZayaan",
    site: "gozayaan.com",
    login: "yes",
    steps:
      "Search results are NOT enough — the coupon list only loads on the booking/payment page. Select a flight, reach the booking screen, then export. Open one booking per airline you want a detailed cell for.",
    filename: "gozayaan",
  },
];

export default function HarGuidePage() {
  return (
    <div className="page dg-page">
      <section className="card">
        <h1>HAR Collection Guide</h1>
        <p style={{ color: "var(--muted)" }}>
          Capture one HAR per site, drop them all in one folder, point the OTA
          Discount Report desktop app at it, and press Run. HAR files never leave
          your machine — only the computed percentages sync to the dashboard.
        </p>
        <p>
          <Link className="button-link" href="/downloads?product=discount-report">
            Get the desktop app
          </Link>
        </p>
      </section>

      <section className="card">
        <h2>The golden rules</h2>
        <ol style={{ lineHeight: 1.9, paddingLeft: 20 }}>
          <li>Chrome DevTools (F12) → <strong>Network</strong> tab → tick <strong>Preserve log</strong> BEFORE searching.</li>
          <li>Log in to the site first (if it needs an account).</li>
          <li>Do the search; wait until results <strong>fully finish loading</strong>.</li>
          <li>Right-click the request list → <strong>“Save all as HAR with content”</strong> — “with content” is essential, otherwise the bodies are empty and the parser sees nothing.</li>
          <li>Name the file with the site name (the app auto-detects the channel from the filename).</li>
          <li><strong>Export per site, immediately.</strong> One long multi-site session makes DevTools silently evict older response bodies — a 234&nbsp;MB combined capture once kept only the last site&apos;s data. After each site: export → clear the log (🚫) → next site.</li>
        </ol>
      </section>

      <section className="card dg-block">
        <h2>Per-site steps</h2>
        <div className="data-table-wrap" role="region" tabIndex={0}>
          <table className="dg-table">
            <thead>
              <tr>
                <th>Channel</th><th>Site</th><th>Login</th><th>What to do</th><th>Filename must contain</th>
              </tr>
            </thead>
            <tbody>
              {CHANNELS.map((row) => (
                <tr key={row.channel}>
                  <th scope="row">{row.channel}</th>
                  <td>{row.site}</td>
                  <td>{row.login}</td>
                  <td style={{ whiteSpace: "normal", textAlign: "left", maxWidth: 460 }}>{row.steps}</td>
                  <td><code>{row.filename}</code></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="card">
        <h2>New / unsupported sites</h2>
        <p style={{ color: "var(--muted)", lineHeight: 1.8 }}>
          Only the channels above are parsed today. A HAR from any other site shows up
          in the desktop app&apos;s scan list as <em>“unrecognized (site not supported
          yet)”</em> and is safely ignored. Want a new OTA added? Capture one search
          (plus its booking page, if the site shows coupons there) with the rules
          above and share which site it is — each new channel needs a small parser
          for that site&apos;s fare responses.
        </p>
      </section>
    </div>
  );
}
