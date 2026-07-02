export type CycleOption = {
  cycleId: string | null;
  label: string;
};

export type RouteOption = {
  routeKey: string;
  origin: string;
  destination: string;
};

export type DateAvailabilityPoint = {
  date: string;
  row_count: number;
};

export type AvailabilityPayload = {
  cycle_id: string | null;
  departure_dates: DateAvailabilityPoint[];
  return_dates: DateAvailabilityPoint[];
};

export type ScopeState = {
  cycleId: string;
  airlines: string[];
  routePairs: string[];
  origin: string;
  destination: string;
  cabin: string;
  tripType: string;
  outboundDateStart: string;
  outboundDateEnd: string;
  returnScope: string;
  returnDate: string;
  returnDateStart: string;
  returnDateEnd: string;
  routeLimit: string;
  historyLimit: string;
};

export const AVAILABILITY_PREVIEW_COUNT = 6;
export const DEFAULT_ROUTE_HINT_COUNT = 16;
export const FILTERED_ROUTE_HINT_COUNT = 24;
export const EMPTY_ROUTE_OPTIONS: RouteOption[] = [];
export const EMPTY_AVAILABILITY: AvailabilityPayload = {
  cycle_id: null,
  departure_dates: [],
  return_dates: []
};

export function normalizeAirportCode(value: string) {
  return value.trim().toUpperCase();
}

export function normalizeRouteKey(value: string) {
  const cleaned = value.trim().toUpperCase();
  if (!cleaned.includes("-")) {
    return "";
  }
  const [origin, destination] = cleaned.split("-", 2);
  const normalizedOrigin = normalizeAirportCode(origin ?? "");
  const normalizedDestination = normalizeAirportCode(destination ?? "");
  if (!normalizedOrigin || !normalizedDestination) {
    return "";
  }
  return `${normalizedOrigin}-${normalizedDestination}`;
}

export function buildQueryString(state: ScopeState) {
  const next = new URLSearchParams();
  const returnScope = deriveReturnScope(state);

  if (state.cycleId.trim()) {
    next.set("cycle_id", state.cycleId.trim());
  }
  for (const airline of state.airlines) {
    const normalizedAirline = airline.trim().toUpperCase();
    if (normalizedAirline) {
      next.append("airline", normalizedAirline);
    }
  }
  for (const routePair of state.routePairs) {
    const normalizedRoutePair = normalizeRouteKey(routePair);
    if (normalizedRoutePair) {
      next.append("route_pair", normalizedRoutePair);
    }
  }
  if (!state.routePairs.length && normalizeAirportCode(state.origin)) {
    next.set("origin", normalizeAirportCode(state.origin));
  }
  if (!state.routePairs.length && normalizeAirportCode(state.destination)) {
    next.set("destination", normalizeAirportCode(state.destination));
  }
  if (state.cabin.trim()) {
    next.set("cabin", state.cabin.trim());
  }
  next.set("trip_type", state.tripType);
  if (state.outboundDateStart.trim()) {
    next.set("start_date", state.outboundDateStart.trim());
  }
  if (state.outboundDateEnd.trim()) {
    next.set("end_date", state.outboundDateEnd.trim());
  }
  next.set("route_limit", state.routeLimit.trim() || "5");
  next.set("history_limit", state.historyLimit.trim() || "6");

  if (state.tripType === "RT") {
    next.set("return_scope", returnScope);
    if (returnScope === "exact" && state.returnDate.trim()) {
      next.set("return_date", state.returnDate.trim());
    }
    if (returnScope === "range") {
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

export function buildAvailabilityQueryString(state: ScopeState) {
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
  for (const routePair of state.routePairs) {
    const normalizedRoutePair = normalizeRouteKey(routePair);
    if (normalizedRoutePair) {
      next.append("route_pair", normalizedRoutePair);
    }
  }
  const normalizedOrigin = normalizeAirportCode(state.origin);
  if (normalizedOrigin && !state.routePairs.length) {
    next.append("origin", normalizedOrigin);
  }
  const normalizedDestination = normalizeAirportCode(state.destination);
  if (normalizedDestination && !state.routePairs.length) {
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

export function buildRouteOptionsQueryString(state: ScopeState) {
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

export function isAirportScopeReady(value: string) {
  const normalized = normalizeAirportCode(value);
  return !normalized || normalized.length === 3;
}

export function filterAirportSuggestions(values: string[], input: string) {
  const normalized = normalizeAirportCode(input);
  if (!normalized) {
    return values.slice(0, 10);
  }
  return values.filter((value) => value.startsWith(normalized)).slice(0, 10);
}

export function hasExactRouteMatch(routeOptions: RouteOption[], origin: string, destination: string) {
  const normalizedOrigin = normalizeAirportCode(origin);
  const normalizedDestination = normalizeAirportCode(destination);
  if (!normalizedOrigin || !normalizedDestination) {
    return false;
  }
  return routeOptions.some(
    (item) => item.origin === normalizedOrigin && item.destination === normalizedDestination
  );
}

export function deriveReturnScope(state: ScopeState) {
  if (state.tripType !== "RT") {
    return "any";
  }
  if (state.returnDateStart.trim() || state.returnDateEnd.trim()) {
    return "range";
  }
  if (state.returnDate.trim()) {
    return "exact";
  }
  return "any";
}
