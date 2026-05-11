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
}

const TRAVELPORT_REPO = "IhsanKabir/Process_Optimization_Using_pywinauto";
const IATA_REPO = "IhsanKabir/iata-code-validator";

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
        version: "v1.5.27",
        date: "2026-05-11",
        label: "Latest",
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
    name: "IATA Code Validator",
    tagline:
      "Two tools in one: validate IATA Numeric Codes against IATA's public CheckACode page, and look up Bangladesh travel agencies from regtravelagency.gov.bd. Both export to Excel.",
    repo: IATA_REPO,
    assetMatch: (n) => n === "IATACodeValidator.exe" || n.endsWith(".exe"),
    requirements:
      "Windows 10/11 · Internet connection · Excel file with IATA codes or agency names",
    fallback: [
      {
        version: "v1.1.0",
        date: "2026-05-06",
        label: "Latest",
        notes: [
          "NEW tab: BD Travel Agency Lookup (regtravelagency.gov.bd)",
          "Export the full ~6,113 active agency list to Excel in one click",
          "Lookup mode: match an Excel of names/license numbers against the cached list",
          "Match priority: EXACT → CONTAINS → FUZZY (rapidfuzz), each row tagged with the method that matched",
          "Filter to exclude EXPIRED-PENDING agencies",
          "Local SQLite cache so re-running the app doesn't re-download",
        ],
        exe_url: `https://github.com/${IATA_REPO}/releases/download/v1.1.0/IATACodeValidator.exe`,
        guide_url: null,
      },
      {
        version: "v1.0.0",
        date: "2026-05-05",
        notes: [
          "Initial release — IATA Code Validator tab",
          "Bulk validation with Excel input/output",
          "Tkinter GUI with sheet/column picker, row range, pause/resume",
          "Local SQLite cache makes re-runs skip already-validated codes",
          "Single portable Windows .exe (~380 MB) — no Python or admin required",
        ],
        exe_url: `https://github.com/${IATA_REPO}/releases/download/v1.0.0/IATACodeValidator.exe`,
        guide_url: null,
      },
    ],
  },
];

function parseGitHubReleases(items: GitHubRelease[], product: Product): Release[] {
  const releases: Release[] = [];

  for (const item of items) {
    if (item.draft) continue;

    const version = item.tag_name.startsWith("v") ? item.tag_name : `v${item.tag_name}`;
    const date = item.published_at.slice(0, 10);

    const exeAsset = item.assets.find((a) => product.assetMatch(a.name));
    const exe_url = exeAsset?.browser_download_url ?? null;

    const hasGuide =
      product.guideUrl != null &&
      (item.body?.toLowerCase().includes("user guide") ||
        item.assets.some((a) => a.name.includes("guide")));
    const guide_url = hasGuide ? product.guideUrl ?? null : null;

    const notes = (item.body || "")
      .split("\n")
      .map((l) => l.replace(/^[-*•]\s*/, "").trim())
      .filter((l) => l.length > 0 && !l.startsWith("#") && !l.startsWith("http"));

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

async function fetchReleases(product: Product): Promise<Release[]> {
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

    if (!res.ok) return product.fallback;

    const items: GitHubRelease[] = await res.json();
    const parsed = parseGitHubReleases(
      items.filter((r) => !r.prerelease),
      product
    );
    return parsed.length > 0 ? parsed : product.fallback;
  } catch {
    return product.fallback;
  }
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
  const latest = releases[0];
  const older = releases.slice(1);

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
          <div className="data-table-wrap">
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
    <main className="shell" style={{ paddingBlock: "32px" }}>
      <ProductTabs active={product.slug} />
      <ProductCard product={product} releases={releases} />
    </main>
  );
}

export const metadata = {
  title: "Downloads",
};
