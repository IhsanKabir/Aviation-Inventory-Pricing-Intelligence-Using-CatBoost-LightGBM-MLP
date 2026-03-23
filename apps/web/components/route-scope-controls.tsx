"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useDeferredValue, useEffect, useMemo, useState, useTransition } from "react";

import { getApiBaseUrl } from "@/lib/api";
import { buildReportingExportUrl } from "@/lib/export";

type CycleOption = {
  cycleId: string | null;
  label: string;
};

type RouteOption = {
  routeKey: string;
  origin: string;
  destination: string;
};

type DateAvailabilityPoint = {
  date: string;
  row_count: number;
};

type AvailabilityPayload = {
  cycle_id: string | null;
  departure_dates: DateAvailabilityPoint[];
  return_dates: DateAvailabilityPoint[];
};

type AvailabilityState = {
  loading: boolean;
  endpointMissing: boolean;
  error?: string;
  data: AvailabilityPayload;
};

type RouteOptionsState = {
  loading: boolean;
  error?: string;
  data: RouteOption[];
};

type ScopeState = {
  cycleId: string;
  airlines: string[];
  origin: string;
  destination: string;
  cabin: string;
  tripType: string;
  returnScope: string;
  returnDate: string;
  returnDateStart: string;
  returnDateEnd: string;
  routeLimit: string;
  historyLimit: string;
};

const AVAILABILITY_PREVIEW_COUNT = 6;
const DEFAULT_ROUTE_HINT_COUNT = 16;
const FILTERED_ROUTE_HINT_COUNT = 24;
const EMPTY_ROUTE_OPTIONS: RouteOption[] = [];
const EMPTY_AVAILABILITY: AvailabilityPayload = {
  cycle_id: null,
  departure_dates: [],
  return_dates: []
};

function normalizeAirportCode(value: string) {
  return value.trim().toUpperCase();
}

function buildQueryString(state: ScopeState) {
  const next = new URLSearchParams();

  if (state.cycleId.trim()) {
    next.set("cycle_id", state.cycleId.trim());
  }
  for (const airline of state.airlines) {
    const normalizedAirline = airline.trim().toUpperCase();
    if (normalizedAirline) {
      next.append("airline", normalizedAirline);
    }
  }
  if (normalizeAirportCode(state.origin)) {
    next.set("origin", normalizeAirportCode(state.origin));
  }
  if (normalizeAirportCode(state.destination)) {
    next.set("destination", normalizeAirportCode(state.destination));
  }
  if (state.cabin.trim()) {
    next.set("cabin", state.cabin.trim());
  }
  next.set("trip_type", state.tripType);
  next.set("route_limit", state.routeLimit.trim() || "5");
  next.set("history_limit", state.historyLimit.trim() || "6");

  if (state.tripType === "RT") {
    next.set("return_scope", state.returnScope);
    if (state.returnScope === "exact" && state.returnDate.trim()) {
      next.set("return_date", state.returnDate.trim());
    }
    if (state.returnScope === "range") {
      if (state.returnDateStart.trim()) {
        next.set("return_date_start", state.returnDateStart.trim());
      }
      if (state.returnDateEnd.trim()) {
        next.set("return_date_end", state.returnDateEnd.trim());
      }
    }
  }

  return next.toString();
}

function buildAvailabilityQueryString(state: ScopeState) {
  const next = new URLSearchParams();

  if (state.cycleId.trim()) {
    next.set("cycle_id", state.cycleId.trim());
  }
  for (const airline of state.airlines) {
    const normalizedAirline = airline.trim().toUpperCase();
    if (normalizedAirline) {
      next.append("airline", normalizedAirline);
    }
  }
  const normalizedOrigin = normalizeAirportCode(state.origin);
  if (normalizedOrigin) {
    next.append("origin", normalizedOrigin);
  }
  const normalizedDestination = normalizeAirportCode(state.destination);
  if (normalizedDestination) {
    next.append("destination", normalizedDestination);
  }
  if (state.cabin.trim()) {
    next.append("cabin", state.cabin.trim());
  }
  if (state.tripType.trim()) {
    next.append("trip_type", state.tripType.trim());
  }

  return next.toString();
}

function buildRouteOptionsQueryString(state: ScopeState) {
  const next = new URLSearchParams();

  if (state.cycleId.trim()) {
    next.set("cycle_id", state.cycleId.trim());
  }
  for (const airline of state.airlines) {
    const normalizedAirline = airline.trim().toUpperCase();
    if (normalizedAirline) {
      next.append("airline", normalizedAirline);
    }
  }
  if (state.cabin.trim()) {
    next.append("cabin", state.cabin.trim());
  }
  if (state.tripType.trim()) {
    next.append("trip_type", state.tripType.trim());
  }

  const normalizedOrigin = normalizeAirportCode(state.origin);
  const normalizedDestination = normalizeAirportCode(state.destination);
  if (normalizedOrigin) {
    next.set("origin_prefix", normalizedOrigin);
  }
  if (normalizedDestination) {
    next.set("destination_prefix", normalizedDestination);
  }
  next.set(
    "limit",
    String(normalizedOrigin || normalizedDestination ? FILTERED_ROUTE_HINT_COUNT : DEFAULT_ROUTE_HINT_COUNT)
  );

  return next.toString();
}

function isAirportScopeReady(value: string) {
  const normalized = normalizeAirportCode(value);
  return !normalized || normalized.length === 3;
}

function filterAirportSuggestions(values: string[], input: string) {
  const normalized = normalizeAirportCode(input);
  if (!normalized) {
    return values.slice(0, 10);
  }
  return values.filter((value) => value.startsWith(normalized)).slice(0, 10);
}

function hasExactRouteMatch(routeOptions: RouteOption[], origin: string, destination: string) {
  const normalizedOrigin = normalizeAirportCode(origin);
  const normalizedDestination = normalizeAirportCode(destination);
  if (!normalizedOrigin || !normalizedDestination) {
    return false;
  }
  return routeOptions.some(
    (item) => item.origin === normalizedOrigin && item.destination === normalizedDestination
  );
}

function buildAvailabilitySummary(items: DateAvailabilityPoint[]) {
  if (!items.length) {
    return {
      totalDates: 0,
      totalRows: 0,
      firstDate: null,
      lastDate: null
    };
  }
  return {
    totalDates: items.length,
    totalRows: items.reduce((sum, item) => sum + item.row_count, 0),
    firstDate: items[0]?.date ?? null,
    lastDate: items[items.length - 1]?.date ?? null
  };
}

function renderAvailabilityTitle(
  label: string,
  summary: { totalDates: number; totalRows: number; firstDate: string | null; lastDate: string | null }
) {
  if (!summary.totalDates) {
    return label;
  }
  const spanLabel =
    summary.firstDate && summary.lastDate && summary.firstDate !== summary.lastDate
      ? `${summary.firstDate} to ${summary.lastDate}`
      : summary.firstDate ?? "Single date";
  return `${label} | ${summary.totalDates} dates | ${summary.totalRows} rows | ${spanLabel}`;
}

export function RouteScopeControls({
  initialState,
  tripScopeLabel,
  cycleOptions,
  airlineOptions,
  routeOptions = EMPTY_ROUTE_OPTIONS
}: {
  initialState: ScopeState;
  tripScopeLabel: string;
  cycleOptions: CycleOption[];
  airlineOptions: string[];
  routeOptions?: RouteOption[];
}) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const [isPending, startTransition] = useTransition();
  const [state, setState] = useState<ScopeState>(initialState);
  const [showAllDepartureDates, setShowAllDepartureDates] = useState(false);
  const [showAllReturnDates, setShowAllReturnDates] = useState(false);
  const [availabilityState, setAvailabilityState] = useState<AvailabilityState>({
    loading: false,
    endpointMissing: false,
    data: EMPTY_AVAILABILITY
  });
  const [routeOptionsState, setRouteOptionsState] = useState<RouteOptionsState>({
    loading: routeOptions.length === 0,
    data: routeOptions
  });

  const syncKey = useMemo(() => JSON.stringify(initialState), [initialState]);

  useEffect(() => {
    setState(initialState);
    setShowAllDepartureDates(false);
    setShowAllReturnDates(false);
  }, [syncKey, initialState]);

  useEffect(() => {
    setRouteOptionsState({
      loading: routeOptions.length === 0,
      data: routeOptions
    });
  }, [routeOptions]);

  const queryString = useMemo(() => buildQueryString(state), [state]);
  const routeOptionsQueryString = useMemo(() => buildRouteOptionsQueryString(state), [state]);
  const deferredRouteOptionsQueryString = useDeferredValue(routeOptionsQueryString);
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
  const scopeIsReady = useMemo(() => {
    if (!airportCodesAreValid) {
      return false;
    }
    const normalizedOrigin = normalizeAirportCode(state.origin);
    const normalizedDestination = normalizeAirportCode(state.destination);
    if (!normalizedOrigin && !normalizedDestination) {
      return true;
    }
    if (normalizedOrigin && normalizedDestination) {
      return exactRouteMatch;
    }
    return false;
  }, [airportCodesAreValid, exactRouteMatch, state.destination, state.origin]);
  const exportHref = useMemo(() => {
    const params: Record<string, string | string[] | undefined> = {
      cycle_id: state.cycleId || undefined,
      airline: state.airlines.length ? state.airlines : undefined,
      origin: normalizeAirportCode(state.origin) || undefined,
      destination: normalizeAirportCode(state.destination) || undefined,
      cabin: state.cabin.trim() || undefined,
      trip_type: state.tripType,
      return_scope: state.tripType === "RT" ? state.returnScope : undefined,
      return_date: state.tripType === "RT" && state.returnScope === "exact" ? state.returnDate || undefined : undefined,
      return_date_start:
        state.tripType === "RT" && state.returnScope === "range" ? state.returnDateStart || undefined : undefined,
      return_date_end:
        state.tripType === "RT" && state.returnScope === "range" ? state.returnDateEnd || undefined : undefined,
      route_limit: state.routeLimit || undefined,
      history_limit: state.historyLimit || undefined
    };
    return buildReportingExportUrl(params, ["routes"]);
  }, [state]);
  const availabilityQueryString = useMemo(
    () => (scopeIsReady ? buildAvailabilityQueryString(state) : null),
    [scopeIsReady, state]
  );
  const deferredAvailabilityQueryString = useDeferredValue(availabilityQueryString);
  const departureDateOptions = availabilityState.data.departure_dates;
  const returnDateOptions = availabilityState.data.return_dates;
  const departureSummary = useMemo(
    () => buildAvailabilitySummary(departureDateOptions),
    [departureDateOptions]
  );
  const returnSummary = useMemo(() => buildAvailabilitySummary(returnDateOptions), [returnDateOptions]);
  const visibleDepartureDates = showAllDepartureDates
    ? departureDateOptions
    : departureDateOptions.slice(0, AVAILABILITY_PREVIEW_COUNT);
  const visibleReturnDates = showAllReturnDates
    ? returnDateOptions
    : returnDateOptions.slice(0, AVAILABILITY_PREVIEW_COUNT);
  const returnDateMap = useMemo(
    () => new Map(returnDateOptions.map((item) => [item.date, item.row_count])),
    [returnDateOptions]
  );
  const availabilityIdle = !scopeIsReady;
  const availabilityOk =
    !availabilityIdle && !availabilityState.loading && !availabilityState.endpointMissing && !availabilityState.error;
  const selectedReturnDateUnavailable =
    availabilityOk &&
    state.tripType === "RT" &&
    state.returnScope === "exact" &&
    Boolean(state.returnDate) &&
    !returnDateMap.has(state.returnDate);
  const selectedReturnRangeUnavailable =
    availabilityOk &&
    state.tripType === "RT" &&
    state.returnScope === "range" &&
    Boolean(state.returnDateStart || state.returnDateEnd) &&
    !returnDateOptions.some((item) => {
      if (state.returnDateStart && item.date < state.returnDateStart) {
        return false;
      }
      if (state.returnDateEnd && item.date > state.returnDateEnd) {
        return false;
      }
      return true;
    });

  useEffect(() => {
    const controller = new AbortController();
    setRouteOptionsState((current) => ({
      ...current,
      loading: true,
      error: undefined
    }));

    const path = deferredRouteOptionsQueryString
      ? `/api/v1/meta/routes?${deferredRouteOptionsQueryString}`
      : "/api/v1/meta/routes";

    fetch(`${getApiBaseUrl()}${path}`, {
      cache: "no-store",
      signal: controller.signal
    })
      .then(async (response) => {
        if (!response.ok) {
          throw new Error(`${response.status} ${response.statusText}`);
        }
        const payload = (await response.json()) as { items?: RouteOption[] };
        const items = Array.isArray(payload.items) ? payload.items : [];
        setRouteOptionsState({
          loading: false,
          error: undefined,
          data: items
        });
      })
      .catch((error: unknown) => {
        if (controller.signal.aborted) {
          return;
        }
        setRouteOptionsState({
          loading: false,
          error: error instanceof Error ? error.message : "Unable to load route suggestions.",
          data: []
        });
      });

    return () => controller.abort();
  }, [deferredRouteOptionsQueryString]);

  useEffect(() => {
    if (!scopeIsReady || deferredAvailabilityQueryString === null) {
      setAvailabilityState({
        loading: false,
        endpointMissing: false,
        data: EMPTY_AVAILABILITY
      });
      return undefined;
    }

    const controller = new AbortController();
    setAvailabilityState((current) => ({
      ...current,
      loading: true,
      endpointMissing: false,
      error: undefined
    }));

    const path = deferredAvailabilityQueryString
      ? `/api/v1/reporting/route-date-availability?${deferredAvailabilityQueryString}`
      : "/api/v1/reporting/route-date-availability";

    fetch(`${getApiBaseUrl()}${path}`, {
      cache: "no-store",
      signal: controller.signal
    })
      .then(async (response) => {
        if (response.status === 404) {
          setAvailabilityState({
            loading: false,
            endpointMissing: true,
            data: EMPTY_AVAILABILITY
          });
          return;
        }
        if (!response.ok) {
          throw new Error(`${response.status} ${response.statusText}`);
        }
        const data = (await response.json()) as AvailabilityPayload;
        setAvailabilityState({
          loading: false,
          endpointMissing: false,
          data
        });
      })
      .catch((error: unknown) => {
        if (controller.signal.aborted) {
          return;
        }
        setAvailabilityState({
          loading: false,
          endpointMissing: false,
          error: error instanceof Error ? error.message : "Unable to inspect collected dates.",
          data: EMPTY_AVAILABILITY
        });
      });

    return () => controller.abort();
  }, [deferredAvailabilityQueryString, scopeIsReady]);

  useEffect(() => {
    if (!scopeIsReady) {
      return undefined;
    }

    const current = searchParams.toString();
    if (queryString === current) {
      return undefined;
    }

    const handle = window.setTimeout(() => {
      startTransition(() => {
        router.replace(queryString ? `${pathname}?${queryString}` : pathname, { scroll: false });
      });
    }, 250);

    return () => window.clearTimeout(handle);
  }, [pathname, queryString, router, scopeIsReady, searchParams, startTransition]);

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
      returnScope: state.returnScope === "any" || state.returnScope === "exact" || state.returnScope === "range"
        ? state.returnScope
        : "any"
    });
  }

  function selectRoute(option: RouteOption) {
    updateState({
      origin: option.origin,
      destination: option.destination
    });
  }

  function selectCycle(cycleId: string | null) {
    updateState({ cycleId: cycleId ?? "" });
  }

  function applyImmediately() {
    if (!scopeIsReady) {
      return;
    }
    startTransition(() => {
      router.replace(queryString ? `${pathname}?${queryString}` : pathname, { scroll: false });
      router.refresh();
    });
  }

  function resetScope() {
    setState({
      cycleId: "",
      airlines: [],
      origin: "",
      destination: "",
      cabin: "",
      tripType: "OW",
      returnScope: "any",
      returnDate: "",
      returnDateStart: "",
      returnDateEnd: "",
      routeLimit: "5",
      historyLimit: "6"
    });
    startTransition(() => {
      router.replace(pathname, { scroll: false });
      router.refresh();
    });
  }

  return (
    <div className="filter-form">
      {cycleOptions.length ? (
        <div className="filter-group">
          <div className="filter-label">Comparable cycles</div>
          <div className="chip-row">
            {cycleOptions.map((item) => (
              <button
                key={item.cycleId ?? "latest-cycle"}
                className="chip"
                data-active={state.cycleId === (item.cycleId ?? "")}
                data-pending={isPending}
                onClick={() => selectCycle(item.cycleId)}
                type="button"
              >
                {item.label}
              </button>
            ))}
          </div>
        </div>
      ) : null}

      {airlineOptions.length ? (
        <div className="filter-group">
          <div className="filter-label">Airlines</div>
          <div className="chip-row">
            {airlineOptions.map((item) => (
              <button
                key={`airline-${item}`}
                className="chip"
                data-active={state.airlines.includes(item)}
                onClick={() => toggleAirline(item)}
                type="button"
              >
                {item}
              </button>
            ))}
          </div>
        </div>
      ) : null}

      <div className="field-grid route-scope-grid">
        <label className="field">
          <span>Origin</span>
          <input
            onChange={(event) => updateState({ origin: normalizeAirportCode(event.target.value) })}
            placeholder="DAC"
            type="text"
            value={state.origin}
          />
        </label>
        <label className="field">
          <span>Destination</span>
          <input
            onChange={(event) => updateState({ destination: normalizeAirportCode(event.target.value) })}
            placeholder="CXB"
            type="text"
            value={state.destination}
          />
        </label>
        <label className="field">
          <span>Cabin</span>
          <input
            onChange={(event) => updateState({ cabin: event.target.value })}
            placeholder="Economy"
            type="text"
            value={state.cabin}
          />
        </label>
        <label className="field">
          <span>Trip type</span>
          <select onChange={(event) => setTripType(event.target.value)} value={state.tripType}>
            <option value="OW">One-way</option>
            <option value="RT">Round-trip</option>
          </select>
        </label>
        {state.tripType === "RT" ? (
          <>
            <label className="field">
              <span>Return scope</span>
              <select onChange={(event) => updateState({ returnScope: event.target.value })} value={state.returnScope}>
                <option value="any">Any collected return</option>
                <option value="exact">Single return date</option>
                <option value="range">Return date range</option>
              </select>
            </label>
            <label className="field">
              <span>Return date</span>
              <input
                disabled={state.returnScope !== "exact"}
                onChange={(event) => updateState({ returnDate: event.target.value })}
                type="date"
                value={state.returnDate}
              />
            </label>
            <label className="field">
              <span>Return start</span>
              <input
                disabled={state.returnScope !== "range"}
                onChange={(event) => updateState({ returnDateStart: event.target.value })}
                type="date"
                value={state.returnDateStart}
              />
            </label>
            <label className="field">
              <span>Return end</span>
              <input
                disabled={state.returnScope !== "range"}
                onChange={(event) => updateState({ returnDateEnd: event.target.value })}
                type="date"
                value={state.returnDateEnd}
              />
            </label>
          </>
        ) : null}
        <label className="field">
          <span>Route blocks</span>
          <input
            inputMode="numeric"
            onChange={(event) => updateState({ routeLimit: event.target.value })}
            pattern="[0-9]*"
            type="text"
            value={state.routeLimit}
          />
        </label>
        <label className="field">
          <span>History depth</span>
          <input
            inputMode="numeric"
            onChange={(event) => updateState({ historyLimit: event.target.value })}
            pattern="[0-9]*"
            type="text"
            value={state.historyLimit}
          />
        </label>
      </div>

      <div className="route-availability-grid">
        <div className="filter-group">
          <div className="filter-label">Matching origins</div>
          <div className="chip-row">
            {originSuggestions.map((option) => (
              <button
                key={`origin-${option}`}
                className="chip"
                data-active={normalizeAirportCode(state.origin) === option}
                onClick={() => updateState({ origin: option })}
                type="button"
              >
                {option}
              </button>
            ))}
          </div>
        </div>
        <div className="filter-group">
          <div className="filter-label">Matching destinations</div>
          <div className="chip-row">
            {destinationSuggestions.map((option) => (
              <button
                key={`destination-${option}`}
                className="chip"
                data-active={normalizeAirportCode(state.destination) === option}
                onClick={() => updateState({ destination: option })}
                type="button"
              >
                {option}
              </button>
            ))}
          </div>
        </div>
      </div>
      {routeOptionsState.loading ? (
        <p className="mono">Refreshing route suggestions for the current airline, cabin, and trip filters...</p>
      ) : routeOptionsState.error ? (
        <div className="status-banner warn">Route suggestions are temporarily unavailable: {routeOptionsState.error}</div>
      ) : null}

      <div className="button-row">
        <button className="button-link" data-pending={isPending} onClick={applyImmediately} type="button">
          Reload matrix
        </button>
        <a className="button-link ghost" href={exportHref}>
          Download Excel
        </a>
        <button className="button-link ghost" data-pending={isPending} onClick={resetScope} type="button">
          Reset scope
        </button>
      </div>

      <p className="page-copy" style={{ marginTop: "0.25rem" }}>
        Trip scope: {tripScopeLabel}
      </p>
      {availabilityState.loading ? (
        <p className="mono">Refreshing collected dates for the current scope...</p>
      ) : null}

      <div className="route-availability-grid">
        <div className="filter-group">
          <div className="filter-label">
            {renderAvailabilityTitle("Collected departure dates", departureSummary)}
          </div>
          {availabilityIdle ? (
            <div className="empty-state">Collected dates will appear once the route scope is ready.</div>
          ) : availabilityOk ? (
            departureDateOptions.length ? (
              <div className="availability-section">
                <div className="chip-row">
                  {visibleDepartureDates.map((item) => (
                    <span className="chip route-date-chip" key={`departure-${item.date}`}>
                      {item.date} ({item.row_count})
                    </span>
                  ))}
                </div>
                {departureDateOptions.length > AVAILABILITY_PREVIEW_COUNT ? (
                  <button
                    className="availability-toggle"
                    onClick={() => setShowAllDepartureDates((current) => !current)}
                    type="button"
                  >
                    {showAllDepartureDates
                      ? "Show fewer departure dates"
                      : `Show all ${departureDateOptions.length} departure dates`}
                  </button>
                ) : null}
              </div>
            ) : (
              <div className="empty-state">No collected departure dates for the current scope.</div>
            )
          ) : availabilityState.loading ? (
            <div className="empty-state">Loading collected departure dates for the current scope...</div>
          ) : availabilityState.endpointMissing ? (
            <div className="empty-state">Date availability is not available on the current API revision yet.</div>
          ) : (
            <div className="empty-state error-state">
              Availability error: {availabilityState.error ?? "Unable to inspect collected dates."}
            </div>
          )}
        </div>

        {state.tripType === "RT" ? (
          <div className="filter-group">
            <div className="filter-label">
              {renderAvailabilityTitle("Collected return dates", returnSummary)}
            </div>
            {availabilityIdle ? (
              <div className="empty-state">Collected return dates will appear once the route scope is ready.</div>
            ) : availabilityOk ? (
              returnDateOptions.length ? (
                <div className="availability-section">
                  <div className="chip-row">
                    {visibleReturnDates.map((item) => (
                      <span className="chip route-date-chip" key={`return-${item.date}`}>
                        {item.date} ({item.row_count})
                      </span>
                    ))}
                  </div>
                  {returnDateOptions.length > AVAILABILITY_PREVIEW_COUNT ? (
                    <button
                      className="availability-toggle"
                      onClick={() => setShowAllReturnDates((current) => !current)}
                      type="button"
                    >
                      {showAllReturnDates
                        ? "Show fewer return dates"
                        : `Show all ${returnDateOptions.length} return dates`}
                    </button>
                  ) : null}
                </div>
              ) : (
                <div className="empty-state">No collected round-trip return dates for the current scope.</div>
              )
            ) : availabilityState.loading ? (
              <div className="empty-state">Loading collected return dates for the current scope...</div>
            ) : availabilityState.endpointMissing ? (
              <div className="empty-state">Date availability is not available on the current API revision yet.</div>
            ) : (
              <div className="empty-state error-state">
                Availability error: {availabilityState.error ?? "Unable to inspect collected return dates."}
              </div>
            )}
          </div>
        ) : null}
      </div>

      {selectedReturnDateUnavailable ? (
        <div className="status-banner warn">
          The selected return date is not currently collected for this route scope and comparable cycle.
        </div>
      ) : null}
      {selectedReturnRangeUnavailable ? (
        <div className="status-banner warn">
          The selected return-date range has no collected matches for this route scope and comparable cycle.
        </div>
      ) : null}

      {filteredRouteOptions.length ? (
        <div className="route-hint-row">
          {filteredRouteOptions.map((item) => (
            <button
              key={item.routeKey}
              className="route-hint-chip"
              data-pending={isPending}
              onClick={() => selectRoute(item)}
              type="button"
            >
              {item.routeKey}
            </button>
          ))}
        </div>
      ) : null}
      {!airportCodesAreValid ? (
        <div className="status-banner warn">
          Enter complete 3-letter airport codes before the matrix refreshes automatically.
        </div>
      ) : null}
      {airportCodesAreValid &&
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

