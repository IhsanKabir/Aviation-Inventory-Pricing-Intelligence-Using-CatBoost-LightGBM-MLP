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
    <main className="mx-auto max-w-screen-md space-y-6 p-6">
      <div>
        <h1 className="text-2xl font-bold">Airport Tax Rates</h1>
        <p className="text-sm text-muted-foreground">
          Current, upcoming and expired tax rates extracted from Travelport FTAX commands
        </p>
      </div>

      {airports.length === 0 ? (
        <p className="text-sm text-muted-foreground">No tax data available.</p>
      ) : (
        <div className="overflow-x-auto rounded-lg border">
          <table className="w-full text-sm">
            <thead className="border-b bg-muted/50">
              <tr>
                <th className="px-4 py-2 text-left font-medium">Airport</th>
                <th className="px-4 py-2 text-left font-medium">Tax Types</th>
                <th className="px-4 py-2 text-left font-medium">Last Updated</th>
                <th className="px-4 py-2"></th>
              </tr>
            </thead>
            <tbody className="divide-y">
              {airports.map((ap) => (
                <tr key={ap.airport_code} className="hover:bg-muted/30">
                  <td className="px-4 py-2 font-mono font-semibold">
                    {ap.airport_code}
                  </td>
                  <td className="px-4 py-2">{ap.tax_count}</td>
                  <td className="px-4 py-2 text-xs text-muted-foreground">
                    {new Date(ap.last_updated).toLocaleDateString()}
                  </td>
                  <td className="px-4 py-2">
                    <Link
                      href={`/gds/taxes/${ap.airport_code}`}
                      className="text-xs text-blue-600 hover:underline"
                    >
                      View rates →
                    </Link>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </main>
  );
}

export const metadata = {
  title: "Airport Tax Rates — GDS",
};
