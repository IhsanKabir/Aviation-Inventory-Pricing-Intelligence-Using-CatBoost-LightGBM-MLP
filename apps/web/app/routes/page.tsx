import fs from "node:fs/promises";
import path from "node:path";
import { Suspense, cache } from "react";

import { DataPanel } from "@/components/data-panel";
import { ReportAccessRequestPanel } from "@/components/report-access-request-panel";
import { RouteScopeControls } from "@/components/route-scope-controls";
import { getAirlines, getRecentCycles, getReportAccessRequest } from "@/lib/api";
import { formatDhakaDateTime } from "@/lib/format";
import { firstParam, manyParams, parseLimit, type RawSearchParams } from "@/lib/query";

import { RouteMonitorSection, RouteMonitorSectionFallback } from "./route-monitor-section";

type PageProps = {
  searchParams?: Promise<RawSearchParams>;
};

type ConfiguredRouteEntry = {
  airline?: string;
  origin?: string;
  destination?: string;
  cabins?: string[];
};

type RouteOption = {
  routeKey: string;
  origin: string;
  destination: string;
};

const ROUTES_CONFIG_PATH = path.resolve(process.cwd(), "..", "..", "config", "routes.json");

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
  startDate?: string,
  endDate?: string,
  returnDateStart?: string,
  returnDateEnd?: string
) {
  const outboundLabel = startDate || endDate ? `${startDate ?? "any"} to ${endDate ?? "any"}` : "all collected outbound dates";
  if (tripType !== "RT") {
    return `One-way | outbound ${outboundLabel}`;
  }
  const inboundLabel =
    returnDateStart || returnDateEnd
      ? `${returnDateStart ?? "any"} to ${returnDateEnd ?? "any"}`
      : "all collected inbound dates";
  return `Round-trip | outbound ${outboundLabel} | inbound ${inboundLabel}`;
}

function normalizeAirportCode(value?: string | null) {
  const normalized = value?.trim().toUpperCase();
  return normalized || undefined;
}

function getRouteSuggestionLimit(origin?: string, destination?: string) {
  return origin || destination ? 24 : 16;
}

const loadConfiguredRouteEntries = cache(async (): Promise<ConfiguredRouteEntry[]> => {
  try {
    const payload = JSON.parse(await fs.readFile(ROUTES_CONFIG_PATH, "utf-8")) as unknown;
    return Array.isArray(payload) ? (payload as ConfiguredRouteEntry[]) : [];
  } catch {
    return [];
  }
});

function matchesCabin(entry: ConfiguredRouteEntry, cabin?: string) {
  const normalizedCabin = cabin?.trim().toLowerCase();
  if (!normalizedCabin) {
    return true;
  }
  const cabins = Array.isArray(entry.cabins) ? entry.cabins : [];
  if (!cabins.length) {
    return true;
  }
  return cabins.some((item) => item.trim().toLowerCase() === normalizedCabin);
}

function getConfiguredRouteOptions(
  entries: ConfiguredRouteEntry[],
  {
    airlines,
    cabin,
    origin,
    destination,
    limit
  }: {
    airlines: string[];
    cabin?: string;
    origin?: string;
    destination?: string;
    limit: number;
  }
): RouteOption[] {
  const airlineFilter = new Set(airlines.map((item) => item.trim().toUpperCase()).filter(Boolean));
  const originPrefix = normalizeAirportCode(origin);
  const destinationPrefix = normalizeAirportCode(destination);
  const filtered = entries.filter((entry) => {
    const routeOrigin = normalizeAirportCode(entry.origin);
    const routeDestination = normalizeAirportCode(entry.destination);
    const airline = (entry.airline ?? "").trim().toUpperCase();
    if (!routeOrigin || !routeDestination) {
      return false;
    }
    if (airlineFilter.size && !airlineFilter.has(airline)) {
      return false;
    }
    if (!matchesCabin(entry, cabin)) {
      return false;
    }
    if (originPrefix && !routeOrigin.startsWith(originPrefix)) {
      return false;
    }
    if (destinationPrefix && !routeDestination.startsWith(destinationPrefix)) {
      return false;
    }
    return true;
  });

  return uniqueByKey(
    filtered.map((entry) => {
      const routeOrigin = normalizeAirportCode(entry.origin) ?? "";
      const routeDestination = normalizeAirportCode(entry.destination) ?? "";
      return {
        routeKey: `${routeOrigin}-${routeDestination}`,
        origin: routeOrigin,
        destination: routeDestination
      };
    }),
    (item) => item.routeKey
  ).slice(0, limit);
}

export default async function RoutesPage({ searchParams }: PageProps) {
  const params = (await searchParams) ?? {};
  const selectedAirlines = manyParams(params, "airline").map((item) => item.trim().toUpperCase()).filter(Boolean);
  const origin = normalizeAirportCode(firstParam(params, "origin"));
  const destination = normalizeAirportCode(firstParam(params, "destination"));
  const cabin = firstParam(params, "cabin");
  const tripType = firstParam(params, "trip_type") ?? "OW";
  const departureDate = firstParam(params, "departure_date");
  const startDate = firstParam(params, "start_date") ?? departureDate ?? undefined;
  const endDate = firstParam(params, "end_date") ?? departureDate ?? undefined;
  const returnDate = firstParam(params, "return_date");
  const returnDateStart = firstParam(params, "return_date_start") ?? (returnDate ? returnDate : undefined);
  const returnDateEnd = firstParam(params, "return_date_end") ?? (returnDate ? returnDate : undefined);
  const returnScope =
    firstParam(params, "return_scope") ??
    (returnDateStart || returnDateEnd ? "range" : returnDate ? "exact" : "any");
  const cycleId = firstParam(params, "cycle_id") ?? undefined;
  const requestId = firstParam(params, "request_id") ?? undefined;
  const routeLimit = parseLimit(firstParam(params, "route_limit"), 5);
  const historyLimit = parseLimit(firstParam(params, "history_limit"), 6);
  const routeSelectionReady = Boolean(origin && destination);
  const effectiveReturnDate = undefined;
  const effectiveReturnDateStart =
    tripType === "RT" ? returnDateStart ?? undefined : undefined;
  const effectiveReturnDateEnd =
    tripType === "RT" ? returnDateEnd ?? undefined : undefined;
  const tripScopeLabel = buildTripScopeLabel(
    tripType,
    startDate,
    endDate,
    effectiveReturnDateStart,
    effectiveReturnDateEnd
  );

  const [airlines, recentCycles, configuredRouteEntries, accessRequest] = await Promise.all([
    getAirlines(),
    getRecentCycles(8),
    loadConfiguredRouteEntries(),
    requestId ? getReportAccessRequest(requestId) : Promise.resolve({ ok: true, data: null as null, error: undefined })
  ]);
  const accessGranted = accessRequest.ok && accessRequest.data?.status === "approved";

  const recentCycleOptions = uniqueByKey(recentCycles.data?.items ?? [], (item) => item.cycle_id ?? "");
  const airlineOptions = uniqueByKey(airlines.data?.items ?? [], (item) => item.airline)
    .map((item) => item.airline)
    .sort((left, right) => left.localeCompare(right));
  const initialRouteOptions = getConfiguredRouteOptions(configuredRouteEntries, {
    airlines: selectedAirlines,
    cabin: cabin ?? undefined,
    origin,
    destination,
    limit: getRouteSuggestionLimit(origin, destination)
  });

  return (
    <>
      <h1 className="page-title">Route Monitor</h1>
      <p className="page-copy">
        Review route-level fare activity with airline, cabin, and travel-window filters in a cleaner monitor view.
      </p>

      <div className="stack">
        <DataPanel
          title="Scope"
          copy="Choose the route, travel window, and filters you want to review."
        >
          <RouteScopeControls
            accessGranted={accessGranted}
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
              outboundDateStart: startDate ?? "",
              outboundDateEnd: endDate ?? "",
              returnScope,
              returnDate: returnDate ?? "",
              returnDateStart: returnDateStart ?? "",
              returnDateEnd: returnDateEnd ?? "",
              routeLimit: String(routeLimit),
              historyLimit: String(historyLimit)
            }}
            airlineOptions={airlineOptions}
            requestId={accessGranted ? requestId : undefined}
            routeOptions={initialRouteOptions}
            tripScopeLabel={tripScopeLabel}
          />
        </DataPanel>

        <DataPanel
          title="Data access request"
          copy="Submit the route and travel window you want to unlock, then refresh this page after approval."
        >
          <ReportAccessRequestPanel
            request={accessRequest.ok ? accessRequest.data : null}
            scope={{
              cycleId,
              airlines: selectedAirlines,
              origin,
              destination,
              cabin: cabin ?? undefined,
              tripType,
              returnScope,
              returnDate: effectiveReturnDate,
              startDate,
              endDate,
              returnDateStart: effectiveReturnDateStart,
              returnDateEnd: effectiveReturnDateEnd,
              routeLimit,
              historyLimit
            }}
          />
        </DataPanel>

        {accessGranted && requestId && routeSelectionReady ? (
          <Suspense fallback={<RouteMonitorSectionFallback />}>
            <RouteMonitorSection
              cabin={cabin ?? undefined}
              cycleId={cycleId}
              destination={destination}
              historyLimit={historyLimit}
              origin={origin}
              recentCycles={recentCycleOptions}
              requestId={requestId}
              startDate={startDate}
              endDate={endDate}
              returnDate={effectiveReturnDate}
              returnDateEnd={effectiveReturnDateEnd}
              returnDateStart={effectiveReturnDateStart}
              routeLimit={routeLimit}
              selectedAirlines={selectedAirlines}
              tripType={tripType}
            />
          </Suspense>
        ) : accessGranted && requestId ? (
          <DataPanel
            title="Route flight fare monitor"
            copy="Choose an exact route above, apply the filters, and then the approved route view will load."
          >
            <div className="empty-state">
              Select both origin and destination in the scope panel above. The current approved request is valid, but the route monitor works best with a specific route instead of an open market-wide view.
            </div>
          </DataPanel>
        ) : (
          <DataPanel
            title="Route flight fare monitor"
            copy="This view unlocks after the current request is approved."
          >
            <div className="empty-state">
              {requestId
                ? "This scope is not approved yet. Refresh after manual review, or change the scope and submit a new request."
                : "Submit a route data request above to unlock the live route monitor for this scope."}
            </div>
          </DataPanel>
        )}
      </div>
    </>
  );
}
