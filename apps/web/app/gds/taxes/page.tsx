/**
 * app/gds/taxes/page.tsx - GDS Airport Tax Viewer
 *
 * Place at: apps/web/app/gds/taxes/page.tsx
 * Accessible via: /gds/taxes
 *
 * Lists all airports with tax data; links to /gds/taxes/[airport].
 */

import Link from "next/link";
import { getGdsTaxAirports, type GdsTaxAirport } from "@/lib/gds";

export default async function GdsTaxesIndexPage() {
  let airports: GdsTaxAirport[] = [];
  try {
    airports = await getGdsTaxAirports();
  } catch {
    // tolerate API unavailability
  }

  return (
    <main className="shell" style={{ paddingBlock: "32px" }}>
      <div style={{ marginBottom: "24px" }}>
        <div className="chip-row" style={{ marginBottom: "12px" }}>
          <Link href="/gds" className="chip" style={{ fontSize: "0.75rem" }}>
            ← GDS
          </Link>
          {airports.length > 0 && (
            <span
              className="chip"
              style={{ fontSize: "0.75rem", background: "rgba(255,255,255,0.56)" }}
            >
              {airports.length} airports
            </span>
          )}
        </div>
        <h1 className="page-title">Airport Tax Rates</h1>
        <p className="page-copy">
          Current, upcoming, and expired tax rates extracted from Travelport FTAX commands
        </p>
      </div>

      {airports.length === 0 ? (
        <div className="card" style={{ padding: "32px", textAlign: "center", color: "var(--muted)" }}>
          No tax data available.
        </div>
      ) : (
        <div className="card" style={{ overflow: "hidden" }}>
          <div className="data-table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Airport</th>
                  <th>Tax Types</th>
                  <th>Last Updated</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {airports.map((ap) => (
                  <tr key={ap.airport_code}>
                    <td>
                      <span style={{ fontFamily: "var(--font-mono)", fontWeight: 600 }}>
                        {ap.airport_code}
                      </span>
                    </td>
                    <td>{ap.tax_count}</td>
                    <td style={{ color: "var(--muted)", fontSize: "0.8rem" }}>
                      {new Date(ap.last_updated).toLocaleDateString()}
                    </td>
                    <td>
                      <Link
                        href={`/gds/taxes/${ap.airport_code}`}
                        className="chip"
                        style={{ fontSize: "0.72rem", padding: "4px 10px", minHeight: "unset" }}
                      >
                        View rates →
                      </Link>
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
  title: "Airport Tax Rates — GDS",
};
