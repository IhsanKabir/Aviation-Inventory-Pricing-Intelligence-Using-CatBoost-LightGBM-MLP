/**
 * app/downloads/page.tsx — Downloads hub for all internal tools
 *
 * Renders one tab per product. Each tab fetches its own GitHub releases
 * with 1-hour ISR revalidation. Active tab is driven by ?product= query.
 *
 * To add a new product: append a new entry to PRODUCTS below.
 */

interface Release {
  version: string;
  date: string;
  label?: "Latest" | "Beta";
  notes: string[];
  exe_url: string | null;
  guide_url: string | null;
}

interface GitHubRelease {
  tag_name: string;
  name: string;
  published_at: string;
  draft: boolean;
  prerelease: boolean;
  body: string;
  assets: { name: string; browser_download_url: string }[];
}

interface Product {
  /** URL slug used in ?product= */
  slug: string;
  /** Tab label */
  name: string;
  /** Sub-headline shown above the latest release card */
  tagline: string;
  /** GitHub repo (owner/name) to fetch releases from */
  repo: string;
  /** File matcher used to pick the download asset from a release */
  assetMatch: (assetName: string) => boolean;
  /** Optional system-requirements line shown at the bottom of the latest card */
  requirements?: string;
  /** Optional guide URL — if set, renders the User Guide button on the latest card */
  guideUrl?: string;
  /** Fallback releases shown when the GitHub API is unavailable */
  fallback: Release[];
  /** When set, the LATEST card's download points here instead of GitHub —
   *  serves firewalled users from a reachable mirror. */
  latestDownloadOverride?: string;
  /** Public version manifest on our own API (never GitHub-rate-limited) —
   *  used to synthesize the latest card when the GitHub list is unavailable. */
  manifestUrl?: string;
}

const TRAVELPORT_REPO = "IhsanKabir/Process_Optimization_Using_pywinauto";
const IATA_REPO = "IhsanKabir/iata-code-validator";
const MAILER_REPO = "IhsanKabir/bulk-mailer";
const DISCOUNT_REPO =
  "IhsanKabir/Aviation-Inventory-Pricing-Intelligence-Using-CatBoost-LightGBM-MLP";

// Reachable download mirror for corporate networks that block GitHub. The
// Cloud Run backend streams the latest IATA exe over a public route; once a
// user is on it, the app auto-updates through the same backend — so this is
// the one-time bootstrap for firewalled users (no manual hand-out).
const IATA_DOWNLOAD_URL =
  "https://aero-pulse-api-591603094460.asia-south1.run.app/api/v1/app/download";
const DISCOUNT_DOWNLOAD_URL = `${IATA_DOWNLOAD_URL}?app=discount-report`;

const PRODUCTS: Product[] = [
  {
    slug: "travelport",
    name: "TravelportAuto",
    tagline:
      "Travelport Smartpoint automation tool for fare, tax, penalty and baggage extraction",
    repo: TRAVELPORT_REPO,
    assetMatch: (n) => n.endsWith(".exe") || n.endsWith(".zip"),
    requirements:
      "Windows 10/11 · Travelport Smartpoint running and signed in · Screen resolution ≥ 1920×1080",
    guideUrl: `https://github.com/${TRAVELPORT_REPO}/blob/main/user_guide.md`,
    fallback: [
      {
        version: "v1.5.29",
        date: "2026-05-12",
        label: "Latest",
        notes: [
          "Time Calculator redesigned — two side-by-side panels to calculate two arrival times at once",
          "Layout stays contained when window is maximized",
        ],
        exe_url: `https://github.com/${TRAVELPORT_REPO}/releases/download/v1.5.29/TravelportAuto-v1.5.29.zip`,
        guide_url: `https://github.com/${TRAVELPORT_REPO}/blob/main/user_guide.md`,
      },
      {
        version: "v1.5.28",
        date: "2026-05-12",
        notes: [
          "Time Calculator — enter UTC offsets, departure time and duration to get local arrival time instantly",
          "Handles overnight flights with +1d / +2d indicator",
        ],
        exe_url: `https://github.com/${TRAVELPORT_REPO}/releases/download/v1.5.28/TravelportAuto-v1.5.28.zip`,
        guide_url: `https://github.com/${TRAVELPORT_REPO}/blob/main/user_guide.md`,
      },
      {
        version: "v1.5.27",
        date: "2026-05-11",
        notes: [
          "Button text cleaned up — no more garbled symbols on Start, Stop, and other buttons",
          "Auto-update dialog enlarged and resizable so Download button is always visible",
          "BOOK-click for baggage mode now uses calibration fan-out — reliable on all machines",
        ],
        exe_url: `https://github.com/${TRAVELPORT_REPO}/releases/download/v1.5.27/TravelportAuto-v1.5.27.zip`,
        guide_url: `https://github.com/${TRAVELPORT_REPO}/blob/main/user_guide.md`,
      },
      {
        version: "v1.5.26",
        date: "2026-05-11",
        notes: [
          "New Baggage Allowance mode — standalone baggage extraction with Excel report",
          "Include baggage checkbox: run fare + tax + baggage in one session",
          "Baggage file browse field: load saved baggage JSON into any fare report",
        ],
        exe_url: `https://github.com/${TRAVELPORT_REPO}/releases/download/v1.5.26/TravelportAuto-v1.5.26.zip`,
        guide_url: `https://github.com/${TRAVELPORT_REPO}/blob/main/user_guide.md`,
      },
    ],
  },
  {
    slug: "iata",
    name: "Travel Ops Console",
    tagline:
      "The combined desktop console (IATACodeValidator.exe): IATA Code Validator, BD Travel Agency Lookup, Traffic Movement, Zenith (customer / PNR / flight loads / history + Flight Load Inspection + per-passenger passport & contact details to Excel), built-in Bulk Mailer with Split & Send, free WhatsApp Blast (text + one image, from your own number), Health checks and an in-app visual Guide. One portable .exe.",
    repo: IATA_REPO,
    assetMatch: (n) => n === "IATACodeValidator.exe" || n.endsWith(".exe"),
    requirements:
      "Windows 10/11 · Internet connection · Excel inputs per tool · WhatsApp Blast: your own WhatsApp account (QR scan)",
    latestDownloadOverride: IATA_DOWNLOAD_URL,
    manifestUrl: `${IATA_DOWNLOAD_URL.replace("/download", "/latest")}`,
    fallback: [
      {
        version: "v1.29.7",
        date: "2026-07-15",
        label: "Latest",
        notes: [
          "NEW: Passenger details → Excel — PNR Bulk Lookup can now pull every passenger's passport no., document type & expiry, issuing country, nationality, DOB, title, gender, email and phones into one workbook (validated on a 985-PNR file: 1,379 passengers, ~99.8% passport-field fill)",
          "In-app visual Guide tab + Agency Visit Tracking in Instant Reports (v1.27–1.28)",
          "Health tab launches a real browser check — a browser that can't start shows red with a fix hint, not a false green (v1.28.1)",
          "WhatsApp Blast — message + one shared image to a contact list over WhatsApp, free, from your own number, any country (v1.25)",
          "Bulk Mailer Split & Send by email column; Zenith Flight Load Inspection + dossier-ID lookup (v1.20–1.23)",
        ],
        exe_url: `https://github.com/${IATA_REPO}/releases/download/v1.29.7/IATACodeValidator.exe`,
        guide_url: null,
      },
      {
        version: "v1.26.0",
        date: "2026-07-05",
        notes: [
          "Health tab — one click shows green/amber/red per feature (is each site reachable? is the browser present?)",
          "Speed presets (Safe / Balanced / Fast) with a daily cap and ban-risk warnings at every step",
          "UI polish: zebra-striped grids, consistent buttons, 3x faster startup, crisp text on scaled displays (v1.23–1.24)",
        ],
        exe_url: `https://github.com/${IATA_REPO}/releases/download/v1.26.0/IATACodeValidator.exe`,
        guide_url: null,
      },
    ],
  },
  {
    slug: "mailer",
    name: "Bulk Email Sending",
    tagline:
      "Standalone Bulk Mailer (separate .exe — also built into the Travel Ops Console). Personalised email per recipient from one Excel list, Split & Send by email column, AND a free WhatsApp Blast: send a message + one shared image to a phone list over WhatsApp, from your own number. Email via Outlook desktop, Microsoft 365, or any SMTP host.",
    repo: MAILER_REPO,
    assetMatch: (n) => n === "BulkMailer.exe" || n.endsWith(".exe"),
    requirements:
      "Windows 10/11 · Email: Outlook desktop / Microsoft 365 / any SMTP · WhatsApp Blast: Google Chrome or Edge installed + your own WhatsApp (QR scan)",
    fallback: [
      {
        version: "v1.3.0",
        date: "2026-07-05",
        label: "Latest",
        notes: [
          "NEW: Health / Diagnostics — one click checks each feature (browser present? mail hosts + WhatsApp reachable?) with green/amber/red + a fix hint",
          "WhatsApp Blast — message + one shared image to a phone list over WhatsApp, free, from your own number, any country (v1.2)",
          "Speed presets (Safe / Balanced / Fast) + daily cap; scan a QR once, then Preview → Send",
          "Ban-risk disclaimer shown at every step — WhatsApp automation is against WhatsApp's ToS; use at your own risk",
        ],
        exe_url: `https://github.com/${MAILER_REPO}/releases/download/v1.3.0/BulkMailer.exe`,
        guide_url: null,
      },
      {
        version: "v1.1.0",
        date: "2026-07-04",
        notes: [
          "Split & Send by email column — one main sheet, no mapping or separate files needed",
          "One click writes one Excel per address; CC/BCC applied to every message",
          "Blank/invalid addresses parked in _UNMATCHED_ROWS.xlsx — never sent",
        ],
        exe_url: `https://github.com/${MAILER_REPO}/releases/download/v1.1.0/BulkMailer.exe`,
        guide_url: null,
      },
      {
        version: "v1.0.0",
        date: "2026-06-09",
        notes: [
          "Standalone Bulk Mailer — one personalised email per recipient from an Excel list",
          "Per-recipient attachments, {name}/{column} templating, CC + BCC",
          "Three transports: Outlook desktop, Microsoft 365 (Graph sign-in), or any SMTP host",
          "Draft-first review, skip-already-sent resume log, delay throttle + Stop for large runs",
        ],
        exe_url: `https://github.com/${MAILER_REPO}/releases/download/v1.0.0/BulkMailer.exe`,
        guide_url: null,
      },
    ],
  },
  {
    slug: "discount-report",
    name: "OTA Discount Comparison",
    tagline:
      "Compare airline discounts across OTA channels (FirstTrip, ShareTrip, GoZayaan, BDFare, AKIJ, Amy). Point it at your HAR capture folder — it analyzes locally, shows the colored best-discount grid, exports Excel, and syncs the result to the team dashboard. HAR files never leave your machine.",
    repo: DISCOUNT_REPO,
    assetMatch: (n) => n === "OTADiscountReport.exe" || n.endsWith(".exe"),
    requirements:
      "Windows 10/11 · single .exe, no install · 8 GB RAM recommended (HAR parsing). Free to download — no approval needed. You only need to sign in and be admin-approved to RUN reports and sync inside the app (request access from the app once it's open).",
    guideUrl: "/discount-comparison/guide",
    latestDownloadOverride: DISCOUNT_DOWNLOAD_URL,
    manifestUrl: `${IATA_DOWNLOAD_URL.replace("/download", "/latest")}?app=discount-report`,
    fallback: [],
  },
];

/** "v1.2.3" and "desktop-v0.1.9" both display as their dotted version. */
function versionLabel(tag: string): string {
  const m = tag.match(/v?(\d+(?:\.\d+)*)\s*$/);
  return m ? `v${m[1]}` : tag;
}

/** Numeric tuple for version comparison ("v0.1.14" > "v0.1.9"). */
function versionNums(v: string): number[] {
  return (v.match(/\d+/g) || []).map(Number);
}

function isNewerVersion(a: string, b: string): boolean {
  const [x, y] = [versionNums(a), versionNums(b)];
  for (let i = 0; i < Math.max(x.length, y.length); i++) {
    const d = (x[i] ?? 0) - (y[i] ?? 0);
    if (d !== 0) return d > 0;
  }
  return false;
}

function parseGitHubReleases(items: GitHubRelease[], product: Product): Release[] {
  const releases: Release[] = [];

  // GitHub's /releases list is NOT reliably ordered — a field capture showed
  // desktop-v0.1.9 listed before v0.1.14. Sort by published date ourselves.
  const ordered = [...items].sort((a, b) =>
    (b.published_at || "").localeCompare(a.published_at || "")
  );

  for (const item of ordered) {
    if (item.draft) continue;

    const version = versionLabel(item.tag_name);
    const date = item.published_at.slice(0, 10);

    const exeAsset = item.assets.find((a) => product.assetMatch(a.name));
    const exe_url = exeAsset?.browser_download_url ?? null;

    const hasGuide =
      product.guideUrl != null &&
      (item.body?.toLowerCase().includes("user guide") ||
        item.assets.some((a) => a.name.includes("guide")));
    const guide_url = hasGuide ? product.guideUrl ?? null : null;

    const notes = parseNotes(item.body || "");

    releases.push({
      version,
      date,
      notes: notes.length > 0 ? notes : [item.name || version],
      exe_url,
      guide_url,
    });
  }

  if (releases.length > 0) {
    releases[0].label = "Latest";
  }

  return releases;
}

function parseNotes(body: string): string[] {
  return body
    .split("\n")
    .map((l) => l.replace(/^[-*•]\s*/, "").trim())
    .filter((l) => l.length > 0 && !l.startsWith("#") && !l.startsWith("http"));
}

/**
 * Latest-release card from our own API's public version manifest — the same
 * channel the desktop updater uses. Never GitHub-rate-limited (the backend
 * caches it), so the newest version stays visible even when the anonymous
 * GitHub list below is throttled.
 */
async function fetchLatestFromMirror(product: Product): Promise<Release[]> {
  if (!product.manifestUrl) return [];
  try {
    const res = await fetch(product.manifestUrl, { next: { revalidate: 60 } });
    if (!res.ok) return [];
    const manifest: {
      version?: string;
      notes?: string;
      published_at?: string;
      download_url?: string;
    } = await res.json();
    if (!manifest?.version) return [];
    const notes = parseNotes(manifest.notes || "");
    return [
      {
        version: `v${manifest.version}`,
        date:
          (manifest.published_at || "").slice(0, 10) ||
          new Date().toISOString().slice(0, 10),
        label: "Latest",
        notes: notes.length > 0 ? notes : [`Version ${manifest.version}`],
        exe_url: product.latestDownloadOverride ?? manifest.download_url ?? null,
        guide_url: product.guideUrl ?? null,
      },
    ];
  } catch {
    return [];
  }
}

async function fetchReleases(product: Product): Promise<Release[]> {
  // Primary: the GitHub release list (full version history).
  let parsed: Release[] = [];
  try {
    const res = await fetch(
      `https://api.github.com/repos/${product.repo}/releases?per_page=30`,
      {
        // Revalidate at most every 60 seconds. Releases are tagged a few
        // times a month, so 1-minute freshness is plenty and ensures a
        // new release appears on the page right after GitHub Actions
        // publishes it — without manual redeploys.
        next: { revalidate: 60 },
        headers: { Accept: "application/vnd.github+json" },
      }
    );
    if (res.ok) {
      const items: GitHubRelease[] = await res.json();
      parsed = parseGitHubReleases(
        items.filter((r) => !r.prerelease),
        product
      );
    }
  } catch {
    // fall through to the mirror manifest
  }
  // Our own API manifest (latest release only, never rate-limited) is the
  // freshness authority: when GitHub's list is unavailable it replaces it, and
  // when a stale cached list lags behind (rate-limited revalidation keeps
  // serving old data) the newer manifest version is prepended as Latest.
  const mirror = await fetchLatestFromMirror(product);
  if (parsed.length === 0) return mirror.length > 0 ? mirror : product.fallback;
  if (mirror.length > 0 && isNewerVersion(mirror[0].version, parsed[0].version)) {
    return [
      mirror[0],
      ...parsed.map((r) => ({ ...r, label: undefined })),
    ];
  }
  return parsed;
}

// Force this page to be rendered on every request. The fetch above still
// uses Next.js's data cache with revalidate=60s, so we don't lose
// performance — but we never serve a build-time-cached page after a
// fresh deploy.
export const dynamic = "force-dynamic";

function fmt(iso: string) {
  return new Date(iso).toLocaleDateString("en-GB", {
    day: "numeric",
    month: "long",
    year: "numeric",
  });
}

function ProductCard({
  product,
  releases,
}: {
  product: Product;
  releases: Release[];
}) {
  const latest =
    product.latestDownloadOverride && releases[0]
      ? { ...releases[0], exe_url: product.latestDownloadOverride }
      : releases[0];
  const older = releases.slice(1);

  // No releases yet (new product) or GitHub API rate-limited with an empty
  // fallback — render a friendly empty state instead of crashing on latest.label.
  if (!latest) {
    return (
      <>
        <div style={{ marginBottom: "32px" }}>
          <h1 className="page-title" style={{ marginBottom: "8px" }}>
            {product.name}
          </h1>
          <p className="page-copy">{product.tagline}</p>
        </div>
        <div className="card" style={{ padding: "28px 32px", marginBottom: "32px" }}>
          {product.latestDownloadOverride ? (
            <>
              <p className="page-copy" style={{ marginTop: 0 }}>
                Release details are unavailable right now (GitHub may be rate-limited),
                but the latest build is always downloadable from the mirror:
              </p>
              <a className="button-link" href={product.latestDownloadOverride}>
                Download latest
              </a>
            </>
          ) : (
            <p className="page-copy" style={{ margin: 0 }}>
              No releases are published yet. Check back soon.
            </p>
          )}
        </div>
      </>
    );
  }

  return (
    <>
      <div style={{ marginBottom: "32px" }}>
        <h1 className="page-title" style={{ marginBottom: "8px" }}>
          {product.name}
        </h1>
        <p className="page-copy">{product.tagline}</p>
      </div>

      {/* Latest release card */}
      <div
        className="card"
        style={{
          padding: "28px 32px",
          marginBottom: "32px",
          borderLeft: "4px solid var(--good)",
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "flex-start",
            justifyContent: "space-between",
            flexWrap: "wrap",
            gap: "16px",
            marginBottom: "20px",
          }}
        >
          <div>
            <div className="chip-row" style={{ marginBottom: "8px" }}>
              <span
                className="chip"
                style={{
                  background: "var(--good)",
                  color: "#fff",
                  fontWeight: 700,
                  fontSize: "0.72rem",
                  padding: "4px 12px",
                  minHeight: "unset",
                }}
              >
                {latest.label ?? "Latest"}
              </span>
              <span
                className="chip"
                style={{
                  fontSize: "0.78rem",
                  fontFamily:
                    "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
                  fontWeight: 600,
                }}
              >
                {latest.version}
              </span>
            </div>
            <p style={{ color: "var(--muted)", fontSize: "0.85rem", margin: 0 }}>
              Released {fmt(latest.date)}
            </p>
          </div>

          <div style={{ display: "flex", gap: "10px", flexWrap: "wrap" }}>
            {latest.exe_url ? (
              <a
                href={latest.exe_url}
                className="button-link"
                download
                style={{ display: "inline-flex", alignItems: "center", gap: "6px" }}
              >
                ↓ Download .exe
              </a>
            ) : (
              <span
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  padding: "10px 20px",
                  borderRadius: "12px",
                  border: "1px solid var(--line)",
                  color: "var(--muted)",
                  fontSize: "0.85rem",
                }}
              >
                Download coming soon
              </span>
            )}
            {latest.guide_url && (
              <a href={latest.guide_url} className="chip" style={{ fontSize: "0.85rem" }}>
                User Guide
              </a>
            )}
          </div>
        </div>

        <div>
          <p style={{ fontWeight: 600, fontSize: "0.85rem", marginBottom: "10px" }}>
            What&apos;s in this release:
          </p>
          <ul
            style={{
              margin: 0,
              paddingLeft: "20px",
              display: "flex",
              flexDirection: "column",
              gap: "4px",
            }}
          >
            {latest.notes.map((note, i) => (
              <li key={i} style={{ fontSize: "0.85rem" }}>
                {note}
              </li>
            ))}
          </ul>
        </div>

        {product.requirements && (
          <div
            style={{
              marginTop: "20px",
              padding: "12px 16px",
              borderRadius: "12px",
              background: "rgba(255,255,255,0.56)",
              fontSize: "0.78rem",
              color: "var(--muted)",
            }}
          >
            <strong style={{ color: "var(--ink)" }}>Requirements:</strong>{" "}
            {product.requirements}
          </div>
        )}
      </div>

      {/* Previous versions */}
      {older.length > 0 && (
        <div className="card" style={{ overflow: "hidden" }}>
          <div style={{ padding: "16px 20px", borderBottom: "1px solid var(--line)" }}>
            <h2 style={{ fontWeight: 600, fontSize: "1rem", margin: 0 }}>
              Previous Versions
            </h2>
          </div>
          <div className="data-table-wrap" role="region" aria-label="Previous versions" tabIndex={0}>
            <table>
              <thead>
                <tr>
                  <th>Version</th>
                  <th>Released</th>
                  <th>Changes</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {older.map((rel) => (
                  <tr key={rel.version}>
                    <td>
                      <span
                        style={{
                          fontFamily:
                            "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
                          fontWeight: 600,
                        }}
                      >
                        {rel.version}
                      </span>
                    </td>
                    <td style={{ color: "var(--muted)", fontSize: "0.82rem" }}>
                      {fmt(rel.date)}
                    </td>
                    <td style={{ fontSize: "0.82rem" }}>
                      <ul style={{ margin: 0, paddingLeft: "16px" }}>
                        {rel.notes.map((n, i) => (
                          <li key={i}>{n}</li>
                        ))}
                      </ul>
                    </td>
                    <td>
                      {rel.exe_url ? (
                        <a
                          href={rel.exe_url}
                          className="chip"
                          download
                          style={{
                            fontSize: "0.72rem",
                            padding: "4px 10px",
                            minHeight: "unset",
                          }}
                        >
                          ↓ .exe
                        </a>
                      ) : (
                        <span style={{ color: "var(--muted)", fontSize: "0.78rem" }}>—</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </>
  );
}

function ProductTabs({ active }: { active: string }) {
  return (
    <div
      role="tablist"
      style={{
        display: "flex",
        gap: "4px",
        borderBottom: "1px solid var(--line)",
        marginBottom: "32px",
      }}
    >
      {PRODUCTS.map((p) => {
        const isActive = p.slug === active;
        return (
          <a
            key={p.slug}
            href={`/downloads?product=${p.slug}`}
            role="tab"
            aria-selected={isActive}
            style={{
              padding: "12px 20px",
              fontSize: "0.9rem",
              fontWeight: isActive ? 600 : 500,
              color: isActive ? "var(--ink)" : "var(--muted)",
              borderBottom: isActive ? "2px solid var(--ink)" : "2px solid transparent",
              marginBottom: "-1px",
              textDecoration: "none",
              transition: "color 0.15s ease",
            }}
          >
            {p.name}
          </a>
        );
      })}
    </div>
  );
}

export default async function DownloadsPage({
  searchParams,
}: {
  searchParams: Promise<{ product?: string }>;
}) {
  const params = await searchParams;
  const requested = params.product ?? PRODUCTS[0].slug;
  const product = PRODUCTS.find((p) => p.slug === requested) ?? PRODUCTS[0];

  // Fetch all products' releases in parallel so tab clicks revalidate together.
  const allReleases = await Promise.all(PRODUCTS.map(fetchReleases));
  const activeIdx = PRODUCTS.findIndex((p) => p.slug === product.slug);
  const releases = allReleases[activeIdx];

  return (
    <section>
      <ProductTabs active={product.slug} />
      <ProductCard product={product} releases={releases} />
    </section>
  );
}

export const metadata = {
  title: "Downloads",
};
