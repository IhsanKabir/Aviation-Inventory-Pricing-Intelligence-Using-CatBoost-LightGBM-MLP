/**
 * app/gds/taxes/[airport]/page.tsx - Per-airport tax rate detail page
 *
 * Place at: apps/web/app/gds/taxes/[airport]/page.tsx
 * URL:  /gds/taxes/SIN  →  shows rates for Singapore Changi
 */

import Link from "next/link";
import { getGdsTaxRates, type GdsTaxRate } from "@/lib/gds";

interface Props {
  params: Promise<{ airport: string }>;
  searchParams: Promise<{ status?: string }>;
}

export default async function AirportTaxPage({ params, searchParams }: Props) {
  const { airport } = await params;
  const { status: statusParam } = await searchParams;
  const airportCode = airport.toUpperCase();
  const status = (statusParam ?? "current") as
    | "current"
    | "future"
    | "expired"
    | "all";

  let rates: GdsTaxRate[] = [];
  let error: string | null = null;
  try {
    rates = await getGdsTaxRates(airportCode, status);
  } catch (e: unknown) {
    error = (e as Error)?.message ?? "Failed to load tax data";
  }

  // Group by tax_code → category → subcategory
  const grouped: Record<
    string,
    { name: string; categories: Record<string, Record<string, GdsTaxRate[]>> }
  > = {};

  for (const rate of rates) {
    if (!grouped[rate.tax_code]) {
      grouped[rate.tax_code] = { name: rate.tax_name ?? "", categories: {} };
    }
    const cat = rate.category ?? "";
    const sub = rate.subcategory ?? "";
    if (!grouped[rate.tax_code].categories[cat]) {
      grouped[rate.tax_code].categories[cat] = {};
    }
    if (!grouped[rate.tax_code].categories[cat][sub]) {
      grouped[rate.tax_code].categories[cat][sub] = [];
    }
    grouped[rate.tax_code].categories[cat][sub].push(rate);
  }

  // Count by status for chips
  const currentCount = rates.filter((r) => r.status === "current").length;
  const futureCount = rates.filter((r) => r.status === "future").length;
  const expiredCount = rates.filter((r) => r.status === "expired").length;

  const tabs = ["current", "future", "expired", "all"] as const;

  const chipStyle = (active: boolean): React.CSSProperties => ({
    fontSize: "0.78rem",
    padding: "6px 14px",
    minHeight: "unset",
    background: active ? "#0f3758" : undefined,
    color: active ? "#fff" : undefined,
    fontWeight: active ? 600 : undefined,
  });

  const statusChipStyle = (s: string): React.CSSProperties => {
    if (s === "current") return { background: "#dcfce7", color: "#166534", fontSize: "0.72rem", padding: "2px 8px", borderRadius: "999px", display: "inline-block" };
    if (s === "future") return { background: "#dbeafe", color: "#1e40af", fontSize: "0.72rem", padding: "2px 8px", borderRadius: "999px", display: "inline-block" };
    return { background: "#f3f4f6", color: "#6b7280", fontSize: "0.72rem", padding: "2px 8px", borderRadius: "999px", display: "inline-block" };
  };

  return (
    <main className="shell" style={{ paddingBlock: "32px" }}>
      {/* Breadcrumb + count chips */}
      <div style={{ marginBottom: "24px" }}>
        <div className="chip-row" style={{ marginBottom: "12px" }}>
          <Link href="/gds" className="chip" style={{ fontSize: "0.75rem" }}>← GDS</Link>
          <Link href="/gds/taxes" className="chip" style={{ fontSize: "0.75rem" }}>Taxes</Link>
          <span className="chip" style={{ fontSize: "0.75rem", fontWeight: 600 }}>{airportCode}</span>
          {currentCount > 0 && (
            <span style={{ ...statusChipStyle("current"), fontSize: "0.75rem", padding: "4px 10px" }}>
              {currentCount} current
            </span>
          )}
          {futureCount > 0 && (
            <span style={{ ...statusChipStyle("future"), fontSize: "0.75rem", padding: "4px 10px" }}>
              {futureCount} future
            </span>
          )}
          {expiredCount > 0 && (
            <span style={{ ...statusChipStyle("expired"), fontSize: "0.75rem", padding: "4px 10px" }}>
              {expiredCount} expired
            </span>
          )}
        </div>
        <h1 className="page-title">{airportCode} Tax Rates</h1>
        <p className="page-copy">
          Airport taxes extracted from Travelport FTAX commands
        </p>
      </div>

      {/* Status tabs */}
      <div className="chip-row" style={{ marginBottom: "24px" }}>
        {tabs.map((t) => (
          <Link
            key={t}
            href={`/gds/taxes/${airportCode}?status=${t}`}
            className="chip"
            style={chipStyle(status === t)}
          >
            {t.charAt(0).toUpperCase() + t.slice(1)}
          </Link>
        ))}
      </div>

      {error ? (
        <div className="card" style={{ padding: "24px", color: "var(--alert)" }}>{error}</div>
      ) : Object.keys(grouped).length === 0 ? (
        <div className="card" style={{ padding: "32px", textAlign: "center", color: "var(--muted)" }}>
          No {status === "all" ? "" : status + " "}tax rates found for {airportCode}.
        </div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: "16px" }}>
          {Object.entries(grouped).map(([code, { name, categories }]) => (
            <div key={code} className="card" style={{ padding: "20px", display: "flex", flexDirection: "column", gap: "16px" }}>
              <h2 style={{ fontWeight: 600, fontSize: "1rem", margin: 0 }}>
                <span style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace" }}>{code}</span>
                {name && (
                  <span style={{ marginLeft: "8px", color: "var(--muted)", fontWeight: 400 }}>
                    — {name}
                  </span>
                )}
              </h2>

              {Object.entries(categories).map(([cat, subs]) => (
                <div key={cat} style={{ display: "flex", flexDirection: "column", gap: "8px" }}>
                  {cat && (
                    <p style={{ fontSize: "0.72rem", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.08em", color: "var(--muted)", margin: 0 }}>
                      {cat}
                    </p>
                  )}
                  {Object.entries(subs).map(([sub, rateList]) => (
                    <div key={sub} style={{ marginLeft: cat ? "12px" : 0, display: "flex", flexDirection: "column", gap: "6px" }}>
                      {sub && (
                        <p style={{ fontSize: "0.78rem", color: "var(--muted)", margin: 0 }}>{sub}</p>
                      )}
                      <div className="data-table-wrap" style={{ borderRadius: "12px", overflow: "hidden" }}>
                        <table>
                          <thead>
                            <tr>
                              <th>Condition</th>
                              <th style={{ textAlign: "right" }}>Amount</th>
                              <th>Currency</th>
                              <th>Status</th>
                            </tr>
                          </thead>
                          <tbody>
                            {rateList.map((r, i) => (
                              <tr key={i} style={{ opacity: r.status === "expired" ? 0.5 : 1 }}>
                                <td style={{ fontSize: "0.8rem" }}>{r.condition || "—"}</td>
                                <td style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace", fontSize: "0.8rem", textAlign: "right" }}>
                                  {r.amount != null ? r.amount.toFixed(2) : "—"}
                                </td>
                                <td style={{ fontSize: "0.8rem" }}>{r.currency ?? "—"}</td>
                                <td>
                                  <span style={statusChipStyle(r.status)}>
                                    {r.status}
                                  </span>
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  ))}
                </div>
              ))}
            </div>
          ))}
        </div>
      )}
    </main>
  );
}

export async function generateMetadata({ params }: Props) {
  const { airport } = await params;
  return {
    title: `${airport.toUpperCase()} Tax Rates — GDS`,
  };
}
