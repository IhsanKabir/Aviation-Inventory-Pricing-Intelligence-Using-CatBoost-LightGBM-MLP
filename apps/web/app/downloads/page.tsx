/**
 * app/downloads/page.tsx - TravelportAuto release downloads
 *
 * Static page — no API call. Releases are hardcoded below.
 * Update RELEASES array when a new version ships.
 */

interface Release {
  version: string;
  date: string;
  label?: "Latest" | "Beta";
  notes: string[];
  exe_url: string | null;
  guide_url: string | null;
}

const RELEASES: Release[] = [
  {
    version: "v1.3.8",
    date: "2026-04-13",
    label: "Latest",
    notes: [
      "Performance: eliminated fixed sleeps, replaced with adaptive polling — 1-3 minutes faster per full run",
      "Startup connection 14s faster, FS freshness check and currency redirect no longer wait the full timeout when data arrives early",
    ],
    exe_url:
      "https://github.com/IhsanKabir/Process_Optimization_Using_pywinauto/releases/download/v1.3.8/TravelportAuto.exe",
    guide_url:
      "https://github.com/IhsanKabir/Process_Optimization_Using_pywinauto/blob/main/user_guide.md",
  },
  {
    version: "v1.3.7",
    date: "2026-04-13",
    notes: [
      'Fixed "Application Window 1 not found" without admin rights — Win32 title matching works on IT-managed work laptops',
    ],
    exe_url:
      "https://github.com/IhsanKabir/Process_Optimization_Using_pywinauto/releases/download/v1.3.7/TravelportAuto.exe",
    guide_url: null,
  },
  {
    version: "v1.3.6",
    date: "2026-04-13",
    notes: [
      "Fixed Smartpoint connection for elevated installs (superseded by v1.3.7)",
    ],
    exe_url: null,
    guide_url: null,
  },
  {
    version: "v1.3.5",
    date: "2026-04-13",
    notes: [
      "Auto-detect Smartpoint window title across all known versions and install types",
    ],
    exe_url:
      "https://github.com/IhsanKabir/Process_Optimization_Using_pywinauto/releases/download/v1.3.5/TravelportAuto.exe",
    guide_url: null,
  },
  {
    version: "v1.3.4",
    date: "2026-04-13",
    notes: [
      'Fixed "Failed to load Python DLL" crash on machines where the Windows username is longer than 8 characters',
    ],
    exe_url:
      "https://github.com/IhsanKabir/Process_Optimization_Using_pywinauto/releases/download/v1.3.4/TravelportAuto.exe",
    guide_url: null,
  },
  {
    version: "v1.3.3",
    date: "2026-04-13",
    notes: [
      'Fixed "Failed to load Python DLL" crash on launch — UPX compression disabled',
    ],
    exe_url:
      "https://github.com/IhsanKabir/Process_Optimization_Using_pywinauto/releases/download/v1.3.3/TravelportAuto.exe",
    guide_url: null,
  },
  {
    version: "v1.3.2",
    date: "2026-04-13",
    notes: [
      'Fixed "Failed to load Python DLL" crash after using the built-in auto-update',
    ],
    exe_url:
      "https://github.com/IhsanKabir/Process_Optimization_Using_pywinauto/releases/download/v1.3.2/TravelportAuto.exe",
    guide_url: null,
  },
  {
    version: "v1.3.1",
    date: "2026-04-13",
    notes: [
      "Fixed crash in Manual (paste) mode — step-by-step clipboard wizard replaces stdin prompt",
      'New "Compare against" field — leave blank for previous run or enter a date to compare a specific snapshot',
      "Creator credit shown in title bar",
    ],
    exe_url:
      "https://github.com/IhsanKabir/Process_Optimization_Using_pywinauto/releases/download/v1.3.1/TravelportAuto.exe",
    guide_url:
      "https://github.com/IhsanKabir/Process_Optimization_Using_pywinauto/blob/main/user_guide.md",
  },
  {
    version: "v1.3.0",
    date: "2026-04-12",
    notes: [
      "Full fare extraction across all routes (85 routes, DAC hub)",
      "BigQuery streaming push — fare + tax data synced to live dashboard",
      "Tax extraction from Travelport FTAX commands (17 airports)",
      "Penalty extraction from FQ/FQN commands",
      "Excel reports with change tracking (new / removed / price change / sold out)",
      "JSON archive snapshots for historical comparison",
      "PostgreSQL persistence with full run history",
      "Checkpoint resume — safely restart interrupted runs",
      "--speed flag: safe | normal | fast profiles",
      "New YQ-YR-Q Charges Excel sheet — GDS surcharge codes per route",
      "Comprehensive user guide included",
    ],
    exe_url:
      "https://github.com/IhsanKabir/Process_Optimization_Using_pywinauto/releases/download/v1.3.0/TravelportAuto.exe",
    guide_url:
      "https://github.com/IhsanKabir/Process_Optimization_Using_pywinauto/blob/main/user_guide.md",
  },
  {
    version: "v1.2.0",
    date: "2026-03-28",
    notes: [
      "PostgreSQL database integration — fare records persisted per run",
      "Unsaleable fare detection",
      "End-of-data detection improvements",
      "RBD sorting fix",
    ],
    exe_url: null,
    guide_url: null,
  },
  {
    version: "v1.1.0",
    date: "2026-03-16",
    notes: [
      "Tax parsing (FTAX) with effective date tracking",
      "Penalty parsing (FQ/FQN)",
      "FS/FD page navigation fix",
      "Black formatting enforced in CI",
    ],
    exe_url: null,
    guide_url: null,
  },
  {
    version: "v1.0.0",
    date: "2026-02-20",
    notes: [
      "Initial release",
      "Fare extraction via pywinauto + pyautogui",
      "Excel report generation",
      "Change detection vs JSON snapshots",
    ],
    exe_url: null,
    guide_url: null,
  },
];

const latest = RELEASES[0];
const older = RELEASES.slice(1);

function fmt(iso: string) {
  return new Date(iso).toLocaleDateString("en-GB", {
    day: "numeric",
    month: "long",
    year: "numeric",
  });
}

export default function DownloadsPage() {
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
                User Guide (PDF)
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
