/**
 * app/downloads/page.tsx - TravelportAuto release downloads
 *
 * Fetches live releases from GitHub API with 1-hour ISR revalidation.
 * Falls back to FALLBACK_RELEASES if the fetch fails at build/revalidate time.
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

const REPO = "IhsanKabir/Process_Optimization_Using_pywinauto";
const GUIDE_URL = `https://github.com/${REPO}/blob/main/user_guide.md`;

const FALLBACK_RELEASES: Release[] = [
  {
    version: "v1.4.0",
    date: "2026-04-13",
    label: "Latest",
    notes: [
      "FS two-window date schedule — rescues airlines whose first-month inventory has dried up",
      "FZS precision preserved to 6 decimal places in currency reports",
      "India K3 origin-sensitive tax correction for round-trip fares from non-Indian airports",
    ],
    exe_url: `https://github.com/${REPO}/releases/download/v1.4.0/TravelportAuto.exe`,
    guide_url: GUIDE_URL,
  },
  {
    version: "v1.3.8",
    date: "2026-04-13",
    notes: [
      "Performance: eliminated fixed sleeps, replaced with adaptive polling — 1-3 minutes faster per full run",
      "Startup connection 14s faster, FS freshness check and currency redirect no longer wait the full timeout when data arrives early",
    ],
    exe_url: `https://github.com/${REPO}/releases/download/v1.3.8/TravelportAuto.exe`,
    guide_url: GUIDE_URL,
  },
];

function parseGitHubReleases(items: GitHubRelease[]): Release[] {
  const releases: Release[] = [];

  for (const item of items) {
    if (item.draft) continue;

    const version = item.tag_name.startsWith("v") ? item.tag_name : `v${item.tag_name}`;
    const date = item.published_at.slice(0, 10);

    const exeAsset = item.assets.find(
      (a) => a.name.endsWith(".exe") || a.name.endsWith(".zip")
    );
    const exe_url = exeAsset?.browser_download_url ?? null;

    const hasGuide = item.body?.toLowerCase().includes("user guide") || item.assets.some((a) => a.name.includes("guide"));
    const guide_url = hasGuide ? GUIDE_URL : null;

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

async function fetchReleases(): Promise<Release[]> {
  try {
    const res = await fetch(
      `https://api.github.com/repos/${REPO}/releases?per_page=30`,
      {
        next: { revalidate: 3600 },
        headers: { Accept: "application/vnd.github+json" },
      }
    );

    if (!res.ok) return FALLBACK_RELEASES;

    const items: GitHubRelease[] = await res.json();
    const parsed = parseGitHubReleases(items.filter((r) => !r.prerelease));
    return parsed.length > 0 ? parsed : FALLBACK_RELEASES;
  } catch {
    return FALLBACK_RELEASES;
  }
}

function fmt(iso: string) {
  return new Date(iso).toLocaleDateString("en-GB", {
    day: "numeric",
    month: "long",
    year: "numeric",
  });
}

export default async function DownloadsPage() {
  const releases = await fetchReleases();
  const latest = releases[0];
  const older = releases.slice(1);

  return (
    <main className="shell" style={{ paddingBlock: "32px" }}>
      {/* Header */}
      <div style={{ marginBottom: "32px" }}>
        <h1 className="page-title">Downloads</h1>
        <p className="page-copy">
          TravelportAuto — Travelport Smartpoint automation tool for fare, tax and penalty extraction
        </p>
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
        <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", flexWrap: "wrap", gap: "16px", marginBottom: "20px" }}>
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
                style={{ fontSize: "0.78rem", fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace", fontWeight: 600 }}
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
          <ul style={{ margin: 0, paddingLeft: "20px", display: "flex", flexDirection: "column", gap: "4px" }}>
            {latest.notes.map((note, i) => (
              <li key={i} style={{ fontSize: "0.85rem" }}>{note}</li>
            ))}
          </ul>
        </div>

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
          <strong style={{ color: "var(--ink)" }}>Requirements:</strong> Windows 10/11 &nbsp;·&nbsp;
          Travelport Smartpoint running and signed in &nbsp;·&nbsp; Screen resolution ≥ 1920×1080
        </div>
      </div>

      {/* Previous versions */}
      {older.length > 0 && (
        <div className="card" style={{ overflow: "hidden" }}>
          <div style={{ padding: "16px 20px", borderBottom: "1px solid var(--line)" }}>
            <h2 style={{ fontWeight: 600, fontSize: "1rem", margin: 0 }}>Previous Versions</h2>
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
                      <span style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace", fontWeight: 600 }}>
                        {rel.version}
                      </span>
                    </td>
                    <td style={{ color: "var(--muted)", fontSize: "0.82rem" }}>{fmt(rel.date)}</td>
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
                          style={{ fontSize: "0.72rem", padding: "4px 10px", minHeight: "unset" }}
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
    </main>
  );
}

export const metadata = {
  title: "Downloads — TravelportAuto",
};
