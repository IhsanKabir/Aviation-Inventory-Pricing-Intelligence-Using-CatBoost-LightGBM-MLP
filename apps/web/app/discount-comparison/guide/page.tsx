import Link from "next/link";

import "../discounts.css";

export const metadata = {
  title: "HAR Collection Guide — OTA Discount Comparison",
  description:
    "What a HAR file is, and exact click-by-click steps to capture one from each OTA site.",
};

type SiteGuide = {
  id: string;
  channel: string;
  site: string;
  login: string;
  filename: string;
  steps: string[];
  notes?: string[];
};

const SITES: SiteGuide[] = [
  {
    id: "firsttrip-b2c",
    channel: "FirstTrip B2C",
    site: "firsttrip.com (usually automatic — no capture needed)",
    login: "not needed",
    filename: "b2c-api.firsttrip  (only for the failsafe capture)",
    steps: [
      "Normally you capture NOTHING for this channel — the desktop app fetches it live. Just set your routes (e.g. DAC-CGP,DAC-DXB) and a FUTURE travel date in the app.",
      "FAILSAFE (only if the app's Run log says the live fetch failed): open firsttrip.com in Chrome, press F12 → Network tab → tick “Preserve log”.",
      "Search your route with the same travel date, wait for all results to load.",
      "Right-click the request list → “Save all as HAR with content” → name it b2c-api.firsttrip.har and put it in your HAR folder.",
    ],
  },
  {
    id: "firsttrip-b2b",
    channel: "FirstTrip B2B (the USBA row)",
    site: "booking.firsttrip.com",
    login: "yes — your agent account",
    filename: "booking.firsttrip",
    steps: [
      "Log in to booking.firsttrip.com with your agent account.",
      "Press F12 → Network tab → tick “Preserve log”.",
      "CRITICAL: in the search form, find the “Preferred Airline” box (placeholder “Ex: BS, BG, TK”) and TYPE THE AIRLINES YOU NEED, e.g. BS, 2A, BG, VQ for domestic. Results come in ~20 pages and only page 1 is captured — un-preferred airlines will be silently missing.",
      "Search the route and date. Wait for the airline list to completely finish loading (the spinner fully stops).",
      "Right-click the Network list → “Save all as HAR with content”.",
      "Save as booking.firsttrip.com.har into your HAR folder. For extra airlines, repeat with a different Preferred Airline list and save as booking.firsttrip.2.har — the app merges all of them.",
    ],
    notes: [
      "Do one domestic search (e.g. DAC-CGP) and one international (e.g. DAC-DXB) — both can be in the same capture session.",
    ],
  },
  {
    id: "bdfare",
    channel: "BDFare",
    site: "bdfare.com → Searchpad",
    login: "yes — agent account",
    filename: "bdfare",
    steps: [
      "Log in to bdfare.com and open the Searchpad.",
      "Press F12 → Network → tick “Preserve log”.",
      "Search the route/date. Wait for results.",
      "Optional but recommended: click ONE flight to open its details — this improves base-fare accuracy.",
      "Right-click → “Save all as HAR with content” → save as bdfare.com.har.",
    ],
  },
  {
    id: "amy",
    channel: "Amy",
    site: "amyweb.amybd.com",
    login: "yes — agent account",
    filename: "amyweb or amybd",
    steps: [
      "Log in to amyweb.amybd.com.",
      "F12 → Network → “Preserve log”.",
      "Search the route/date, wait for the fare list.",
      "Right-click → “Save all as HAR with content” → save as amyweb.amybd.com.har.",
    ],
  },
  {
    id: "akij",
    channel: "AKIJ Air",
    site: "akijair.com",
    login: "yes (Google sign-in works)",
    filename: "akij",
    steps: [
      "Sign in to akijair.com.",
      "F12 → Network → “Preserve log”.",
      "Search the route/date, wait for all results.",
      "Right-click → “Save all as HAR with content” → save as akijair.com.har.",
    ],
  },
  {
    id: "sharetrip",
    channel: "ShareTrip B2C",
    site: "sharetrip.net",
    login: "recommended",
    filename: "sharetrip (e.g. sharetrip bs.har, sharetrip 2a.har)",
    steps: [
      "Sign in to sharetrip.net.",
      "F12 → Network → “Preserve log”.",
      "Search your route/date. This alone captures only the COMMON rate.",
      "For the detailed cell (bKash + card coupons): click ONE airline's flight → continue to the BOOKING page → wait for the “DISCOUNT COUPON” list to appear.",
      "IMMEDIATELY right-click → “Save all as HAR with content” → save as sharetrip bs.har (name the airline).",
      "Click the clear button (🚫) in the Network tab, go back, pick the NEXT airline's flight, repeat → sharetrip 2a.har, sharetrip bg.har, …",
    ],
    notes: [
      "Why short per-airline captures? In a long session Chrome silently drops older response bodies — one big capture loses most airlines' coupons. Export right after each booking page.",
    ],
  },
  {
    id: "gozayaan",
    channel: "GoZayaan",
    site: "gozayaan.com",
    login: "yes",
    filename: "gozayaan",
    steps: [
      "Sign in to gozayaan.com.",
      "F12 → Network → “Preserve log”.",
      "Search the route/date. IMPORTANT: search results are NOT enough for this site.",
      "Click a flight and continue until you reach the BOOKING / payment page — the coupon list (bKash, EBL, AMEX…) only loads there.",
      "Right-click → “Save all as HAR with content” → save as gozayaan.com.har.",
      "The tool fills a detailed cell only for the airline whose booking you opened — open one booking per airline you care about (coupons are payment-based and mostly uniform, so 1-2 bookings per route type usually covers it).",
    ],
  },
];

export default function HarGuidePage() {
  return (
    <div className="page dg-page">
      <section className="card">
        <h1>HAR Collection Guide</h1>
        <p style={{ color: "var(--muted)" }}>
          Everything you need to feed the OTA Discount Report app — written for
          first-time users.
        </p>
        <p>
          <Link className="button-link" href="/downloads?product=discount-report">
            Get the desktop app
          </Link>
        </p>
      </section>

      <section className="card">
        <h2>What is a HAR file?</h2>
        <p style={{ lineHeight: 1.9 }}>
          When you search flights on a website, your browser quietly receives the raw
          fare data — prices, discounts, coupons — from the site&apos;s own servers.
          A <strong>HAR file</strong> (<em>HTTP Archive</em>) is simply a recording of
          that traffic, saved as one file from the browser&apos;s built-in DevTools.
          No extension or extra software is needed — Chrome/Edge can save one out of
          the box.
        </p>
        <p style={{ lineHeight: 1.9 }}>
          Why we use it: the discount numbers you see on screen are rounded and
          incomplete, but the HAR contains the <strong>exact</strong> fares, base
          prices, and coupon rules the site itself received. The desktop app reads
          those numbers <strong>locally on your machine</strong> — the HAR never
          leaves your computer; only the computed percentages sync to the dashboard.
        </p>
        <p style={{ lineHeight: 1.9, color: "var(--muted)" }}>
          ⚠️ A HAR records everything in the session, including your logged-in
          cookies for that site. Treat the files like passwords: keep them in your
          HAR folder, don&apos;t email them, and use the app&apos;s “Archive old
          HARs” button rather than sharing them.
        </p>
      </section>

      <section className="card">
        <h2>The capture routine (same for every site)</h2>
        <ol style={{ lineHeight: 2, paddingLeft: 20 }}>
          <li>Open the site in <strong>Chrome or Edge</strong> and log in if the site needs it.</li>
          <li>Press <strong>F12</strong> (or right-click → Inspect) → open the <strong>Network</strong> tab.</li>
          <li>Tick <strong>“Preserve log”</strong> (checkbox at the top of the Network tab).</li>
          <li>Do the flight search; wait until results <strong>completely finish loading</strong>.</li>
          <li>Right-click anywhere in the Network request list → <strong>“Save all as HAR with content”</strong>. The words <em>“with content”</em> matter — without them the file is empty shells.</li>
          <li>Save it into your HAR folder with the <strong>site name in the filename</strong> (the app auto-detects the channel from it).</li>
          <li>Click the <strong>clear button (🚫)</strong> before moving to the next site — one long multi-site recording silently loses data.</li>
        </ol>
      </section>

      <section className="card">
        <h2>Site-by-site steps</h2>
        {SITES.map((site) => (
          <details key={site.id} style={{ marginBottom: 10 }}>
            <summary style={{ cursor: "pointer", fontWeight: 600, padding: "6px 0" }}>
              {site.channel} — {site.site}
            </summary>
            <div style={{ padding: "8px 0 8px 16px" }}>
              <p style={{ color: "var(--muted)", marginBottom: 8 }}>
                Login: {site.login} · Filename must contain: <code>{site.filename}</code>
              </p>
              <ol style={{ lineHeight: 1.9, paddingLeft: 20 }}>
                {site.steps.map((step, index) => (
                  <li key={index}>{step}</li>
                ))}
              </ol>
              {site.notes?.map((note, index) => (
                <p key={index} style={{ color: "var(--muted)", marginTop: 6 }}>💡 {note}</p>
              ))}
            </div>
          </details>
        ))}
      </section>

      <section className="card">
        <h2>Then, in the desktop app</h2>
        <ol style={{ lineHeight: 2, paddingLeft: 20 }}>
          <li>Click <strong>Browse…</strong> and pick your HAR folder — the scan table shows every file and which channel it matched.</li>
          <li>Set routes + a <strong>future</strong> travel date for the live FirstTrip B2C fetch.</li>
          <li><strong>Run report</strong> → review the colored grid (open the <em>Run log</em> if any channel looks empty).</li>
          <li><strong>Sync to dashboard</strong> → the team sees it on the OTA Discounts page.</li>
          <li>Next day: click <strong>Archive old HARs</strong> and capture fresh files.</li>
        </ol>
      </section>

      <section className="card">
        <h2>New / unsupported sites</h2>
        <p style={{ color: "var(--muted)", lineHeight: 1.8 }}>
          Only the channels above are parsed today. A HAR from any other site shows in
          the app&apos;s scan list as <em>“unrecognized (site not supported yet)”</em>
          and is safely ignored. Want a new OTA added? Capture one search (plus its
          booking page if coupons appear there) using the routine above, and report
          which site it is — each new channel is a small parser we can add.
        </p>
      </section>
    </div>
  );
}
