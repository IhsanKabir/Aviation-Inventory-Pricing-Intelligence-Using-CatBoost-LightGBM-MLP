import { Fragment } from "react";
import Link from "next/link";

import "../discounts.css";

export const metadata = {
  title: "HAR Collection Guide — OTA Discount Comparison",
  description:
    "What a HAR file is, the visual capture workflow, and one general procedure that works on every site.",
};

function Flow({ steps, accentLast }: { steps: Array<{ title: string; hint?: string }>; accentLast?: boolean }) {
  return (
    <div className="dg-flow">
      {steps.map((step, index) => (
        <Fragment key={step.title}>
          <div
            className={`dg-flow-step${accentLast && index === steps.length - 1 ? " dg-flow-step--accent" : ""}`}
          >
            <span className="dg-flow-num">{index + 1}</span>
            <strong>{step.title}</strong>
            {step.hint ? <span>{step.hint}</span> : null}
          </div>
          {index < steps.length - 1 ? (
            <div className="dg-flow-arrow" aria-hidden>
              →
            </div>
          ) : null}
        </Fragment>
      ))}
    </div>
  );
}

export default function HarGuidePage() {
  return (
    <div className="page dg-page">
      <section className="card">
        <h1>HAR Collection Guide</h1>
        <p style={{ color: "var(--muted)" }}>
          One general procedure works on every site — including sites we haven&apos;t
          supported yet. Site categories below only add one extra step each.
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
          When you search flights, the website&apos;s servers send your browser the raw
          fare data — exact prices, base fares, coupons. A <strong>HAR file</strong>
          (HTTP Archive) is a <em>recording of that traffic</em>, saved as one file
          straight from Chrome/Edge — no extension needed. The desktop app reads the
          exact numbers out of it, <strong>locally on your machine</strong>; only the
          computed percentages ever leave your computer.
        </p>
        <p style={{ lineHeight: 1.9, color: "var(--muted)" }}>
          ⚠️ A HAR also records your logged-in cookies for that site. Treat the files
          like passwords: keep them in your HAR folder, never email or share them,
          and clear them daily with the app&apos;s “Archive old HARs” button.
        </p>
      </section>

      <section className="card">
        <h2>The daily loop</h2>
        <Flow
          accentLast
          steps={[
            { title: "Capture", hint: "one HAR per site" },
            { title: "HAR folder", hint: "drop all files in" },
            { title: "Run", hint: "desktop app, local" },
            { title: "Review", hint: "colored grid + Run log" },
            { title: "Sync", hint: "sanitized % only" },
            { title: "Team dashboard", hint: "OTA Discounts page" },
          ]}
        />
      </section>

      <section className="card">
        <h2>The GENERAL capture procedure — works on ANY site</h2>
        <Flow
          steps={[
            { title: "Open site + log in", hint: "Chrome or Edge" },
            { title: "F12 → Network", hint: "tick “Preserve log”" },
            { title: "Search flights", hint: "wait till fully loaded" },
            { title: "Save all as HAR", hint: "“…with content”!" },
            { title: "Name by site", hint: "e.g. bdfare.com.har" },
            { title: "Clear log 🚫", hint: "before the next site" },
          ]}
        />
        <p style={{ lineHeight: 1.8 }}>
          That&apos;s the whole skill. Two details decide success or failure:
        </p>
        <ul style={{ lineHeight: 1.9, paddingLeft: 20 }}>
          <li>
            <strong>“Save all as HAR with content”</strong> — the words <em>with
            content</em> matter; without them the file contains empty shells.
          </li>
          <li>
            <strong>Export per site, immediately</strong> — one long multi-site
            recording silently drops older data. Export → clear → next site.
          </li>
        </ul>
        <div className="dg-exception">
          <strong>Unknown / new sites:</strong> use exactly this procedure (search
          page + booking page if the site shows coupons there), name the file with
          the site&apos;s name, and drop it in the folder. The app will list it as
          <em> “unrecognized (site not supported yet)”</em> — the capture is still
          valuable: report the site to us and it becomes a supported channel with a
          small parser.
        </div>
      </section>

      <section className="card">
        <h2>Site categories — the one extra step each</h2>

        <div className="dg-cat">
          <h3>Category A — Search page is enough</h3>
          <div className="dg-cat-sites">BDFare · Amy (amyweb) · AKIJ Air · FirstTrip B2B</div>
          <p style={{ lineHeight: 1.8, margin: 0 }}>
            Run the general procedure as-is: search, wait, export. Done.
          </p>
          <div className="dg-exception">
            <strong>FirstTrip B2B exception:</strong> before searching, type the
            airlines you need into the <em>“Preferred Airline”</em> box (e.g.
            <code> BS, 2A, BG, VQ</code>). Results arrive in ~20 pages and only page 1
            is captured — un-preferred airlines go missing silently.
          </div>
          <div className="dg-exception">
            <strong>BDFare — base fares are cross-checked, never guessed:</strong>{" "}
            BDFare&apos;s search list carries no base fare, and the base/tax split
            differs hugely per airline (Air India&apos;s base is ~59% of the fare vs
            Biman&apos;s ~77%). The app takes the base from a solid source
            automatically: the <em>same flight</em> (airline + date + time + gross)
            found in your <strong>Amy or USBA/FirstTrip-B2B capture</strong> supplies
            its exact base — so capture those sites for the same route and the BDFare
            %s become exact on their own. For airlines those sites don&apos;t cover,
            click <strong>Flight Details → Fare Summary</strong> once on the airline&apos;s
            cheapest BDFare fare before exporting. Anything still estimated shows as{" "}
            <code>~x.x</code> in the grid and is named in the Run log.
          </div>
        </div>

        <div className="dg-cat dg-cat--b">
          <h3>Category B — Booking page needed for coupons</h3>
          <div className="dg-cat-sites">ShareTrip · GoZayaan</div>
          <p style={{ lineHeight: 1.8, margin: 0 }}>
            The search page only shows the basic rate. Extra step: <strong>click a
            flight and continue to the booking/payment page</strong>, wait for the
            coupon list (bKash / EBL / AMEX…), <em>then</em> export.
          </p>
          <div className="dg-exception">
            <strong>ShareTrip — one booking capture per airline (per market):</strong>{" "}
            the browser drops ShareTrip&apos;s big search responses from exported HARs,
            so the booking page is the only reliable source. For each airline you need,
            open its flight, wait for the DISCOUNT COUPON list, and export as e.g.{" "}
            <code>sharetrip bs.har</code>, <code>sharetrip bsint.har</code> (domestic and
            international are separate coupon sets — capture both if you need both).
            <br />
            <strong>The app judges every coupon, not the advertised %:</strong> card
            coupons carry hidden caps (e.g. &quot;18%&quot; Stellar Signature is capped
            at 6,000 BDT — only ~7.4% on a 91k itinerary, where the 1% GPStar stack
            actually wins). All coupon %s apply to the BASE fare. Cells show the honest
            effective rates with the payment gateway&apos;s convenience fee noted:{" "}
            <code>7.1, 8.1 (GPStar, 1.5% fee), 7.4 (Stellar Signature, capped, 2% fee)</code>
            — every ShareTrip rail charges 0.5–5% (bKash 2%, Nagad 1.5%, most cards 2%),
            and the app ranks options <em>net</em> of that fee even though the shown %
            stays gross for cross-site comparability. TripCoin earn (≈ base/1000 coins)
            is tracked as a note only — its cash value isn&apos;t exposed.
            <br />
            <strong>Why not one search capture for all airlines?</strong> The browser
            never keeps ShareTrip&apos;s search response bodies in an exported HAR
            (verified: 0 bytes in every capture), and the automatic discount differs per
            airline — so each airline needs its own booking view. The coupon list itself
            is identical market-wide, so it&apos;s only the per-airline rate you&apos;re
            collecting each time.
          </div>
        </div>

        <div className="dg-cat dg-cat--c">
          <h3>Category C — No capture needed (live)</h3>
          <div className="dg-cat-sites">FirstTrip B2C</div>
          <p style={{ lineHeight: 1.8, margin: 0 }}>
            Fetched live by the app — just set routes and a <strong>future</strong>{" "}
            travel date. If the Run log says the live fetch failed, capture
            firsttrip.com with the general procedure and name it{" "}
            <code>b2c-api.firsttrip.har</code> — the app uses it as a failsafe.
          </p>
        </div>
      </section>

      <section className="card">
        <h2>Then, in the desktop app</h2>
        <Flow
          steps={[
            { title: "Browse…", hint: "pick the HAR folder" },
            { title: "Check the scan", hint: "every file matched?" },
            { title: "Run report", hint: "check Run log if blank" },
            { title: "Export / Sync", hint: "xlsx or dashboard" },
            { title: "Archive old HARs", hint: "next day, start clean" },
          ]}
        />
      </section>
    </div>
  );
}
