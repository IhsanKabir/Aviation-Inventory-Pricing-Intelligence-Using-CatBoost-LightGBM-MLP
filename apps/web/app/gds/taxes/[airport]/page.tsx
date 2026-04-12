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
  } catch (e: any) {
    error = e?.message ?? "Failed to load tax data";
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

  const statusColor: Record<string, string> = {
    current: "bg-green-100 text-green-800",
    future:  "bg-blue-100 text-blue-800",
    expired: "bg-gray-100 text-gray-500",
  };

  const tabs = ["current", "future", "expired", "all"] as const;

  return (
    <main className="mx-auto max-w-screen-lg space-y-6 p-6">
      {/* Breadcrumb */}
      <nav className="text-sm text-muted-foreground">
        <Link href="/gds" className="hover:underline">GDS</Link>
        {" / "}
        <Link href="/gds/taxes" className="hover:underline">Taxes</Link>
        {" / "}
        <span className="font-medium text-foreground">{airportCode}</span>
      </nav>

      <div>
        <h1 className="text-2xl font-bold">{airportCode} Tax Rates</h1>
        <p className="text-sm text-muted-foreground">
          Airport taxes extracted from Travelport FTAX commands
        </p>
      </div>

      {/* Status tabs */}
      <div className="flex gap-1 rounded-lg border p-1 w-fit">
        {tabs.map((t) => (
          <Link
            key={t}
            href={`/gds/taxes/${airportCode}?status=${t}`}
            className={`rounded px-3 py-1 text-sm capitalize ${
              status === t
                ? "bg-primary text-primary-foreground font-medium"
                : "hover:bg-muted"
            }`}
          >
            {t}
          </Link>
        ))}
      </div>

      {error ? (
        <p className="text-sm text-destructive">{error}</p>
      ) : Object.keys(grouped).length === 0 ? (
        <p className="text-sm text-muted-foreground">
          No {status === "all" ? "" : status + " "}tax rates found for {airportCode}.
        </p>
      ) : (
        Object.entries(grouped).map(([code, { name, categories }]) => (
          <section key={code} className="rounded-lg border p-4 space-y-3">
            <h2 className="font-semibold">
              <span className="font-mono">{code}</span>
              {name && <span className="ml-2 text-muted-foreground font-normal">— {name}</span>}
            </h2>

            {Object.entries(categories).map(([cat, subs]) => (
              <div key={cat} className="space-y-2">
                {cat && (
                  <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                    {cat}
                  </p>
                )}
                {Object.entries(subs).map(([sub, rateList]) => (
                  <div key={sub} className="ml-2 space-y-1">
                    {sub && (
                      <p className="text-xs text-muted-foreground">{sub}</p>
                    )}
                    <div className="overflow-x-auto rounded border">
                      <table className="w-full text-sm">
                        <thead className="border-b bg-muted/40">
                          <tr>
                            <th className="px-3 py-1.5 text-left text-xs font-medium">Condition</th>
                            <th className="px-3 py-1.5 text-right text-xs font-medium">Amount</th>
                            <th className="px-3 py-1.5 text-left text-xs font-medium">Currency</th>
                            <th className="px-3 py-1.5 text-left text-xs font-medium">Status</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y">
                          {rateList.map((r, i) => (
                            <tr key={i} className={r.status === "expired" ? "opacity-50" : ""}>
                              <td className="px-3 py-1.5 text-xs">
                                {r.condition || "—"}
                              </td>
                              <td className="px-3 py-1.5 text-right font-mono text-xs">
                                {r.amount != null ? r.amount.toFixed(2) : "—"}
                              </td>
                              <td className="px-3 py-1.5 text-xs">{r.currency ?? "—"}</td>
                              <td className="px-3 py-1.5">
                                <span
                                  className={`rounded-full px-2 py-0.5 text-xs font-medium ${
                                    statusColor[r.status] ?? "bg-gray-100 text-gray-700"
                                  }`}
                                >
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
          </section>
        ))
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
