"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useEffect, useMemo, useState, useTransition } from "react";

import { buildReportingExportUrl } from "@/lib/export";

import { AvailabilityPreview } from "./availability-preview";
import { useRouteOptions } from "./route-options-loader";
import {
  AirlineChips,
  AirportSuggestionGrid,
  CycleChips,
  ExactRouteChips,
  SelectedRoutePairChips
} from "./scope-chips";
import { ScopeFieldGrid, TravelWindowsCard } from "./scope-fields";
import type { CycleOption, RouteOption, ScopeState } from "./scope-state";
import {
  buildQueryString,
  DEFAULT_ROUTE_HINT_COUNT,
  deriveReturnScope,
  EMPTY_ROUTE_OPTIONS,
  FILTERED_ROUTE_HINT_COUNT,
  filterAirportSuggestions,
  hasExactRouteMatch,
  isAirportScopeReady,
  normalizeAirportCode,
  normalizeRouteKey
} from "./scope-state";

interface RouteScopeFormProps {
  initialState: ScopeState;
  tripScopeLabel: string;
  cycleOptions: CycleOption[];
  airlineOptions: string[];
  routeOptions?: RouteOption[];
  requestId?: string;
  accessGranted: boolean;
}

export function RouteScopeForm({
  initialState,
  tripScopeLabel,
  cycleOptions,
  airlineOptions,
  routeOptions = EMPTY_ROUTE_OPTIONS,
  requestId,
  accessGranted
}: RouteScopeFormProps) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const [isPending, startTransition] = useTransition();
  const [state, setState] = useState<ScopeState>(initialState);
  const [showAllDepartureDates, setShowAllDepartureDates] = useState(false);
  const [showAllReturnDates, setShowAllReturnDates] = useState(false);

  const syncKey = useMemo(() => JSON.stringify(initialState), [initialState]);

  useEffect(() => {
    setState(initialState);
    setShowAllDepartureDates(false);
    setShowAllReturnDates(false);
  }, [syncKey, initialState]);

  const routeOptionsState = useRouteOptions(initialState, routeOptions, state);

  const queryString = useMemo(() => buildQueryString(state), [state]);
  const liveRouteOptions = routeOptionsState.data;
  const airportCodesAreValid = useMemo(
    () => isAirportScopeReady(state.origin) && isAirportScopeReady(state.destination),
    [state.destination, state.origin]
  );
  const airportOptions = useMemo(() => {
    const origins = new Set<string>();
    const destinations = new Set<string>();
    for (const item of liveRouteOptions) {
      origins.add(item.origin);
      destinations.add(item.destination);
    }
    return {
      origins: Array.from(origins).sort(),
      destinations: Array.from(destinations).sort()
    };
  }, [liveRouteOptions]);
  const originSuggestions = useMemo(
    () => filterAirportSuggestions(airportOptions.origins, state.origin),
    [airportOptions.origins, state.origin]
  );
  const destinationSuggestions = useMemo(
    () => filterAirportSuggestions(airportOptions.destinations, state.destination),
    [airportOptions.destinations, state.destination]
  );
  const filteredRouteOptions = useMemo(() => {
    const originFilter = normalizeAirportCode(state.origin);
    const destinationFilter = normalizeAirportCode(state.destination);
    const filtered = liveRouteOptions.filter((item) => {
      if (originFilter && !item.origin.startsWith(originFilter)) {
        return false;
      }
      if (destinationFilter && !item.destination.startsWith(destinationFilter)) {
        return false;
      }
      return true;
    });
    return filtered.slice(0, originFilter || destinationFilter ? FILTERED_ROUTE_HINT_COUNT : DEFAULT_ROUTE_HINT_COUNT);
  }, [liveRouteOptions, state.destination, state.origin]);
  const exactRouteMatch = useMemo(
    () => hasExactRouteMatch(liveRouteOptions, state.origin, state.destination),
    [liveRouteOptions, state.destination, state.origin]
  );
  const normalizedOrigin = useMemo(() => normalizeAirportCode(state.origin), [state.origin]);
  const normalizedDestination = useMemo(() => normalizeAirportCode(state.destination), [state.destination]);
  const normalizedSelectedRoutePairs = useMemo(
    () => state.routePairs.map((item) => normalizeRouteKey(item)).filter(Boolean),
    [state.routePairs]
  );
  const matrixScopeReady = useMemo(() => {
    if (!airportCodesAreValid) {
      return false;
    }
    if (normalizedSelectedRoutePairs.length) {
      return true;
    }
    if (!normalizedOrigin && !normalizedDestination) {
      return true;
    }
    if (normalizedOrigin && normalizedDestination) {
      return exactRouteMatch;
    }
    return false;
  }, [
    airportCodesAreValid,
    exactRouteMatch,
    normalizedDestination,
    normalizedOrigin,
    normalizedSelectedRoutePairs.length,
  ]);
  const availabilityScopeReady = useMemo(() => {
    if (!accessGranted || !airportCodesAreValid) {
      return false;
    }
    if (normalizedSelectedRoutePairs.length) {
      return true;
    }
    return Boolean(normalizedOrigin && normalizedDestination && exactRouteMatch);
  }, [accessGranted, airportCodesAreValid, exactRouteMatch, normalizedDestination, normalizedOrigin, normalizedSelectedRoutePairs.length]);
  const returnScope = useMemo(() => deriveReturnScope(state), [state]);
  const exportHref = useMemo(() => {
    if (!accessGranted || !requestId) {
      return null;
    }
    const params: Record<string, string | string[] | undefined> = {
      request_id: requestId,
      cycle_id: state.cycleId || undefined,
      airline: state.airlines.length ? state.airlines : undefined,
      route_pair: normalizedSelectedRoutePairs.length ? normalizedSelectedRoutePairs : undefined,
      origin: normalizedSelectedRoutePairs.length ? undefined : normalizeAirportCode(state.origin) || undefined,
      destination: normalizedSelectedRoutePairs.length ? undefined : normalizeAirportCode(state.destination) || undefined,
      cabin: state.cabin.trim() || undefined,
      trip_type: state.tripType,
      start_date: state.outboundDateStart || undefined,
      end_date: state.outboundDateEnd || undefined,
      return_scope: state.tripType === "RT" ? returnScope : undefined,
      return_date: state.tripType === "RT" && returnScope === "exact" ? state.returnDate || undefined : undefined,
      return_date_start:
        state.tripType === "RT" && returnScope === "range" ? state.returnDateStart || undefined : undefined,
      return_date_end:
        state.tripType === "RT" && returnScope === "range" ? state.returnDateEnd || undefined : undefined,
      route_limit: state.routeLimit || undefined,
      history_limit: state.historyLimit || undefined
    };
    return buildReportingExportUrl(params, ["routes"]);
  }, [accessGranted, normalizedSelectedRoutePairs, requestId, returnScope, state]);

  const hasPendingMatrixChanges = queryString !== searchParams.toString();

  function updateState(next: Partial<ScopeState>) {
    setState((current) => ({ ...current, ...next }));
  }

  function toggleAirline(airline: string) {
    setState((current) => {
      const normalizedAirline = airline.trim().toUpperCase();
      const exists = current.airlines.includes(normalizedAirline);
      return {
        ...current,
        airlines: exists
          ? current.airlines.filter((item) => item !== normalizedAirline)
          : [...current.airlines, normalizedAirline].sort()
      };
    });
  }

  function setTripType(nextTripType: string) {
    if (nextTripType === "OW") {
      updateState({
        tripType: "OW",
        returnScope: "any",
        returnDate: "",
        returnDateStart: "",
        returnDateEnd: ""
      });
      return;
    }
    updateState({
      tripType: "RT",
      returnScope: "any"
    });
  }

  function selectRoute(option: RouteOption) {
    const routeKey = normalizeRouteKey(option.routeKey);
    if (!routeKey) {
      return;
    }
    setState((current) => {
      const nextRoutePairs = current.routePairs.includes(routeKey)
        ? current.routePairs.filter((item) => item !== routeKey)
        : [...current.routePairs, routeKey];
      return {
        ...current,
        routePairs: nextRoutePairs
      };
    });
  }

  function removeRoutePair(routeKey: string) {
    setState((current) => ({
      ...current,
      routePairs: current.routePairs.filter((item) => item !== routeKey)
    }));
  }

  function selectCycle(cycleId: string | null) {
    updateState({ cycleId: cycleId ?? "" });
  }

  function applyImmediately() {
    if (!matrixScopeReady) {
      return;
    }
    startTransition(() => {
      router.replace(queryString ? `${pathname}?${queryString}` : pathname, { scroll: false });
    });
  }

  function resetScope() {
    setState({
      cycleId: "",
      airlines: [],
      routePairs: [],
      origin: "",
      destination: "",
      cabin: "",
      tripType: "OW",
      outboundDateStart: "",
      outboundDateEnd: "",
      returnScope: "any",
      returnDate: "",
      returnDateStart: "",
      returnDateEnd: "",
      routeLimit: "5",
      historyLimit: "6"
    });
    startTransition(() => {
      router.replace(pathname, { scroll: false });
    });
  }

  return (
    <div className="filter-form">
      <CycleChips isPending={isPending} onSelect={selectCycle} options={cycleOptions} selectedCycleId={state.cycleId} />

      <AirlineChips onToggle={toggleAirline} options={airlineOptions} selected={state.airlines} />

      <ScopeFieldGrid onTripTypeChange={setTripType} onUpdate={updateState} state={state} />

      <SelectedRoutePairChips onRemove={removeRoutePair} routePairs={normalizedSelectedRoutePairs} />

      <ExactRouteChips
        isPending={isPending}
        onSelect={selectRoute}
        options={filteredRouteOptions}
        selectedRoutePairs={normalizedSelectedRoutePairs}
      />

      <TravelWindowsCard onUpdate={updateState} state={state} />

      <AirportSuggestionGrid
        destination={state.destination}
        destinationSuggestions={destinationSuggestions}
        onPickDestination={(option) => updateState({ destination: option })}
        onPickOrigin={(option) => updateState({ origin: option })}
        origin={state.origin}
        originSuggestions={originSuggestions}
      />
      {routeOptionsState.loading ? (
        <p className="mono">Refreshing route suggestions...</p>
      ) : routeOptionsState.error ? (
        <div className="status-banner warn">Route suggestions are temporarily unavailable right now.</div>
      ) : null}

      <div className="button-row">
        <button className="button-link" data-pending={isPending} disabled={!matrixScopeReady || !hasPendingMatrixChanges} onClick={applyImmediately} type="button">
          Apply filters
        </button>
        {exportHref ? (
          <a className="button-link ghost" href={exportHref}>
            Download Excel
          </a>
        ) : (
          <button className="button-link ghost" disabled type="button">
            Download Excel
          </button>
        )}
        <button className="button-link ghost" data-pending={isPending} onClick={resetScope} type="button">
          Reset scope
        </button>
      </div>

      <p className="page-copy" style={{ marginTop: "0.25rem" }}>
        Trip scope: {tripScopeLabel}
      </p>
      {hasPendingMatrixChanges ? (
        <p className="mono">Route suggestions update as you filter. Apply when you are ready to refresh the table.</p>
      ) : null}
      {!accessGranted ? (
        <div className="status-banner">
          Route data and export stay locked until this scope has an approved request.
        </div>
      ) : null}

      <AvailabilityPreview
        onToggleShowAllDepartureDates={() => setShowAllDepartureDates((current) => !current)}
        onToggleShowAllReturnDates={() => setShowAllReturnDates((current) => !current)}
        requestId={requestId}
        scopeReady={availabilityScopeReady}
        showAllDepartureDates={showAllDepartureDates}
        showAllReturnDates={showAllReturnDates}
        state={state}
      />

      {!airportCodesAreValid ? (
        <div className="status-banner warn">
          Enter complete 3-letter airport codes before applying the route filters.
        </div>
      ) : null}
      {normalizedSelectedRoutePairs.length ? (
        <div className="status-banner">
          Exact route selection is active for {normalizedSelectedRoutePairs.length} route{normalizedSelectedRoutePairs.length === 1 ? "" : "s"}.
        </div>
      ) : null}
      {airportCodesAreValid &&
      !normalizedSelectedRoutePairs.length &&
      normalizeAirportCode(state.origin) &&
      normalizeAirportCode(state.destination) &&
      !routeOptionsState.loading &&
      !exactRouteMatch ? (
        <div className="status-banner warn">
          No exact route match found for the entered origin and destination. Pick one of the matching route chips.
        </div>
      ) : null}
    </div>
  );
}
