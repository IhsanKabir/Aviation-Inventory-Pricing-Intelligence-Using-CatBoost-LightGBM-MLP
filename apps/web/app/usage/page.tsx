/**
 * app/usage/page.tsx — Admin-only usage dashboard.
 *
 * Reads from the Cloud Run API's GET /api/v1/usage/summary endpoint
 * (added in the backend-app-id-and-usage PR). Gated behind the same
 * admin session cookie as the /admin page.
 *
 * Purpose: see who is using which desktop app (TravelportAuto vs IATA
 * Code Validator vs anything we add later) and how much, broken down
 * by user. Foundation for future per-app price tiers.
 */

import { requireAdminSession } from "@/lib/admin";
import { getApiBaseUrl } from "@/lib/api";

interface UsageTotal {
  app_id: string;
  events: number;
  rows: number;
}

interface UsageByUser {
  app_id: string;
  user_email: string;
  events: number;
  rows: number;
  last_seen_utc: string;
}

interface UsageEvent {
  occurred_at_utc: string;
  app_id: string;
  user_email: string;
  action: string;
  target: string;
  count: number;
}

interface UsageSummary {
  window_days: number;
  as_of_utc: string;
  totals: UsageTotal[];
  by_user: UsageByUser[];
  recent: UsageEvent[];
}

const WINDOW_DAYS_DEFAULT = 30;

const APP_LABEL: Record<string, string> = {
  "iata-validator": "IATA Code Validator",
  "travelport-auto": "TravelportAuto",
};

const APP_BADGE_COLOR: Record<string, string> = {
  "iata-validator": "#0d9488", // teal
  "travelport-auto": "#1d4ed8", // blue
};

function formatLabel(appId: string): string {
  return APP_LABEL[appId] || appId || "(untagged)";
}

function badgeColor(appId: string): string {
  return APP_BADGE_COLOR[appId] || "#475569";
}

function formatRelativeTime(iso: string): string {
  if (!iso) return "—";
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return iso;
  const diffMs = Date.now() - then;
  if (diffMs < 60_000) return "just now";
  const m = Math.round(diffMs / 60_000);
  if (m < 60) return `${m}m ago`;
  const h = Math.round(m / 60);
  if (h < 24) return `${h}h ago`;
  const d = Math.round(h / 24);
  return `${d}d ago`;
}

function formatDateTime(iso: string): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString("en-GB", {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

async function fetchUsage(): Promise<UsageSummary | { error: string }> {
  const adminToken =
    process.env.REPORT_ACCESS_ADMIN_TOKEN?.trim() ||
    process.env.WEB_ADMIN_PASSWORD?.trim() ||
    "";
  if (!adminToken) {
    return {
      error:
        "REPORT_ACCESS_ADMIN_TOKEN is not configured on the web environment.",
    };
  }
  try {
    const res = await fetch(
      `${getApiBaseUrl()}/api/v1/usage/summary?days=${WINDOW_DAYS_DEFAULT}`,
      {
        headers: { "X-Admin-Token": adminToken },
        cache: "no-store",
      },
    );
    if (!res.ok) {
      const body = await res.text();
      return {
        error: `API returned ${res.status}: ${body.slice(0, 300)}`,
      };
    }
    return (await res.json()) as UsageSummary;
  } catch (err) {
    return { error: `Could not reach the API: ${(err as Error).message}` };
  }
}

export const dynamic = "force-dynamic";

export const metadata = {
  title: "Usage — Aero Pulse Intelligence",
};

export default async function UsagePage() {
  await requireAdminSession("/usage");
  const data = await fetchUsage();

  if ("error" in data) {
    return (
      <main style={pageStyle}>
        <header style={headerStyle}>
          <h1 style={titleStyle}>Usage</h1>
          <p style={taglineStyle}>
            Who is using which desktop app, in the last {WINDOW_DAYS_DEFAULT} days.
          </p>
        </header>
        <section style={cardStyle}>
          <h2 style={sectionTitleStyle}>Cannot load usage data</h2>
          <p style={bodyTextStyle}>{data.error}</p>
          <p style={hintStyle}>
            This page reads <code>GET /api/v1/usage/summary</code> from the API. Make sure
            the backend PR is deployed and that{" "}
            <code>REPORT_ACCESS_ADMIN_TOKEN</code> is set in the Vercel environment.
          </p>
        </section>
      </main>
    );
  }

  return (
    <main style={pageStyle}>
      <header style={headerStyle}>
        <h1 style={titleStyle}>Usage</h1>
        <p style={taglineStyle}>
          Last {data.window_days} days · refreshed {formatRelativeTime(data.as_of_utc)}
        </p>
      </header>

      <section style={cardStyle}>
        <h2 style={sectionTitleStyle}>Totals by app</h2>
        {data.totals.length === 0 ? (
          <p style={emptyStateStyle}>
            No usage events recorded yet. Once a desktop app posts a
            lookup, it will appear here.
          </p>
        ) : (
          <table style={tableStyle}>
            <thead>
              <tr>
                <th style={thStyle}>App</th>
                <th style={thNumberStyle}>Events</th>
                <th style={thNumberStyle}>Records processed</th>
              </tr>
            </thead>
            <tbody>
              {data.totals.map((t) => (
                <tr key={t.app_id}>
                  <td style={tdStyle}>
                    <AppBadge appId={t.app_id} />
                  </td>
                  <td style={tdNumberStyle}>{t.events.toLocaleString()}</td>
                  <td style={tdNumberStyle}>{t.rows.toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <section style={cardStyle}>
        <h2 style={sectionTitleStyle}>By user × app</h2>
        {data.by_user.length === 0 ? (
          <p style={emptyStateStyle}>No per-user breakdown available yet.</p>
        ) : (
          <table style={tableStyle}>
            <thead>
              <tr>
                <th style={thStyle}>User</th>
                <th style={thStyle}>App</th>
                <th style={thNumberStyle}>Events</th>
                <th style={thNumberStyle}>Records</th>
                <th style={thStyle}>Last seen</th>
              </tr>
            </thead>
            <tbody>
              {data.by_user.map((row, i) => (
                <tr key={`${row.app_id}-${row.user_email}-${i}`}>
                  <td style={tdStyle}>{row.user_email}</td>
                  <td style={tdStyle}>
                    <AppBadge appId={row.app_id} />
                  </td>
                  <td style={tdNumberStyle}>{row.events.toLocaleString()}</td>
                  <td style={tdNumberStyle}>{row.rows.toLocaleString()}</td>
                  <td style={tdStyle}>
                    {formatRelativeTime(row.last_seen_utc)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <section style={cardStyle}>
        <h2 style={sectionTitleStyle}>Recent events</h2>
        {data.recent.length === 0 ? (
          <p style={emptyStateStyle}>No recent events.</p>
        ) : (
          <table style={tableStyle}>
            <thead>
              <tr>
                <th style={thStyle}>When</th>
                <th style={thStyle}>App</th>
                <th style={thStyle}>User</th>
                <th style={thStyle}>Action</th>
                <th style={thStyle}>Target</th>
                <th style={thNumberStyle}>Count</th>
              </tr>
            </thead>
            <tbody>
              {data.recent.map((e, i) => (
                <tr key={`${e.occurred_at_utc}-${i}`}>
                  <td style={tdStyle}>{formatDateTime(e.occurred_at_utc)}</td>
                  <td style={tdStyle}>
                    <AppBadge appId={e.app_id} />
                  </td>
                  <td style={tdStyle}>{e.user_email}</td>
                  <td style={tdStyle}>{e.action}</td>
                  <td style={tdStyle}>{e.target || "—"}</td>
                  <td style={tdNumberStyle}>{e.count.toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </main>
  );
}

function AppBadge({ appId }: { appId: string }) {
  return (
    <span
      style={{
        background: badgeColor(appId),
        color: "white",
        padding: "2px 8px",
        borderRadius: 999,
        fontSize: "0.85em",
        fontWeight: 600,
        display: "inline-block",
      }}
    >
      {formatLabel(appId)}
    </span>
  );
}

// ---------- Inline styles (matches the look-and-feel of /downloads) ----------

const pageStyle: React.CSSProperties = {
  maxWidth: 1100,
  margin: "0 auto",
  padding: "32px 24px 64px",
  fontFamily:
    "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
  color: "#0f172a",
};

const headerStyle: React.CSSProperties = {
  marginBottom: 24,
};

const titleStyle: React.CSSProperties = {
  fontSize: "2.25rem",
  fontWeight: 700,
  margin: 0,
  letterSpacing: "-0.02em",
};

const taglineStyle: React.CSSProperties = {
  margin: "8px 0 0",
  color: "#475569",
};

const cardStyle: React.CSSProperties = {
  background: "white",
  border: "1px solid #e2e8f0",
  borderRadius: 12,
  padding: "20px 24px",
  marginBottom: 16,
  boxShadow: "0 1px 2px rgba(0,0,0,0.04)",
};

const sectionTitleStyle: React.CSSProperties = {
  fontSize: "1.1rem",
  fontWeight: 600,
  margin: "0 0 12px",
};

const bodyTextStyle: React.CSSProperties = {
  margin: "0 0 8px",
  color: "#334155",
};

const hintStyle: React.CSSProperties = {
  margin: 0,
  color: "#64748b",
  fontSize: "0.9rem",
};

const tableStyle: React.CSSProperties = {
  width: "100%",
  borderCollapse: "collapse",
  fontSize: "0.95rem",
};

const thStyle: React.CSSProperties = {
  textAlign: "left",
  padding: "8px 12px",
  borderBottom: "2px solid #e2e8f0",
  color: "#475569",
  fontWeight: 600,
};

const thNumberStyle: React.CSSProperties = { ...thStyle, textAlign: "right" };

const tdStyle: React.CSSProperties = {
  padding: "8px 12px",
  borderBottom: "1px solid #f1f5f9",
};

const tdNumberStyle: React.CSSProperties = {
  ...tdStyle,
  textAlign: "right",
  fontVariantNumeric: "tabular-nums",
};

const emptyStateStyle: React.CSSProperties = {
  margin: 0,
  color: "#64748b",
  fontStyle: "italic",
};
