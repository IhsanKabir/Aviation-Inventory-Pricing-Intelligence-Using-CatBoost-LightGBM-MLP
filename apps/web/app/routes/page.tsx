import { Suspense } from "react";

import { DataPanel } from "@/components/data-panel";
import { RouteScopeControls } from "@/components/route-scope-controls";
import { getAirlines, getRecentCycles } from "@/lib/api";
import { formatDhakaDateTime } from "@/lib/format";
import { firstParam, manyParams, parseLimit, type RawSearchParams } from "@/lib/query";

import { RouteMonitorSection, RouteMonitorSectionFallback } from "./route-monitor-section";

type PageProps = {
  searchParams?: Promise<RawSearchParams>;
};

function uniqueByKey<T>(items: T[], keyFn: (item: T) => string) {
  const seen = new Set<string>();
  const unique: T[] = [];
  for (const item of items) {
    const key = keyFn(item);
    if (!key || seen.has(key)) {
      continue;
    }
    seen.add(key);
    unique.push(item);
  }
  return unique;
}

function buildTripScopeLabel(
  tripType: string,
  returnScope: string,
  returnDate?: string,
  returnDateStart?: string,
  returnDateEnd?: string
) {
  if (tripType !== "RT") {
    return "One-way observations";
  }
  if (returnScope === "exact" && returnDate) {
    return `Round-trip | return ${returnDate}`;
  }
  if (returnScope === "range" && (returnDateStart || returnDateEnd)) {
    return `Round-trip | return window ${returnDateStart ?? "any"} to ${returnDateEnd ?? "any"}`;
  }
  return "Round-trip | any collected return date";
}

function normalizeAirportCode(value?: string | null) {
  const normalized = value?.trim().toUpperCase();
  return normalized || undefined;
}

export default async function RoutesPage({ searchParams }: PageProps) {
  const params = (await searchParams) ?? {};
  const selectedAirlines = manyParams(params, "airline").map((item) => item.trim().toUpperCase()).filter(Boolean);
  const origin = normalizeAirportCode(firstParam(params, "origin"));
  const destination = normalizeAirportCode(firstParam(params, "destination"));
  const cabin = firstParam(params, "cabin");
  const tripType = firstParam(params, "trip_type") ?? "OW";
  const returnDate = firstParam(params, "return_date");
  const returnDateStart = firstParam(params, "return_date_start");
  const returnDateEnd = firstParam(params, "return_date_end");
  const returnScope =
    firstParam(params, "return_scope") ??
    (returnDateStart || returnDateEnd ? "range" : returnDate ? "exact" : "any");
  const cycleId = firstParam(params, "cycle_id") ?? undefined;
  const routeLimit = parseLimit(firstParam(params, "route_limit"), 5);
  const historyLimit = parseLimit(firstParam(params, "history_limit"), 6);
  const effectiveReturnDate = tripType === "RT" && returnScope === "exact" ? returnDate ?? undefined : undefined;
  const effectiveReturnDateStart =
    tripType === "RT" && returnScope === "range" ? returnDateStart ?? undefined : undefined;
  const effectiveReturnDateEnd =
    tripType === "RT" && returnScope === "range" ? returnDateEnd ?? undefined : undefined;
  const tripScopeLabel = buildTripScopeLabel(
    tripType,
    returnScope,
    effectiveReturnDate,
    effectiveReturnDateStart,
    effectiveReturnDateEnd
  );

  const [airlines, recentCycles] = await Promise.all([
    getAirlines(),
    getRecentCycles(8)
  ]);

  const recentCycleOptions = uniqueByKey(recentCycles.data?.items ?? [], (item) => item.cycle_id ?? "");
  const airlineOptions = uniqueByKey(airlines.data?.items ?? [], (item) => item.airline)
    .map((item) => item.airline)
    .sort((left, right) => left.localeCompare(right));

  return (
    <>
      <h1 className="page-title">Route Monitor</h1>
      <p className="page-copy">
        Report-style route matrix against the reporting API. Hosted reads now prefer the
        BigQuery warehouse path; airline, signal, and capture-history interaction stay in
        the page for workbook-like review without Excel.
      </p>

      <div className="stack">
        <DataPanel
          title="Matrix scope"
          copy="The page shell loads first, while route dates and the matrix stream in behind it. That keeps route review responsive even when the reporting query is heavy."
        >
          <RouteScopeControls
            cycleOptions={recentCycleOptions.map((item) => ({
              cycleId: item.cycle_id ?? null,
              label: item.cycle_completed_at_utc ? formatDhakaDateTime(item.cycle_completed_at_utc) : "Latest"
            }))}
            initialState={{
              cycleId: cycleId ?? "",
              airlines: selectedAirlines,
              origin: origin ?? "",
              destination: destination ?? "",
              cabin: cabin ?? "",
              tripType,
              returnScope,
              returnDate: returnDate ?? "",
              returnDateStart: returnDateStart ?? "",
              returnDateEnd: returnDateEnd ?? "",
              routeLimit: String(routeLimit),
              historyLimit: String(historyLimit)
            }}
            airlineOptions={airlineOptions}
            tripScopeLabel={tripScopeLabel}
          />
        </DataPanel>

        <Suspense fallback={<RouteMonitorSectionFallback />}>
          <RouteMonitorSection
            cabin={cabin ?? undefined}
            cycleId={cycleId}
            destination={destination}
            historyLimit={historyLimit}
            origin={origin}
            recentCycles={recentCycleOptions}
            returnDate={effectiveReturnDate}
            returnDateEnd={effectiveReturnDateEnd}
            returnDateStart={effectiveReturnDateStart}
            routeLimit={routeLimit}
            selectedAirlines={selectedAirlines}
            tripType={tripType}
          />
        </Suspense>
      </div>
    </>
  );
}
