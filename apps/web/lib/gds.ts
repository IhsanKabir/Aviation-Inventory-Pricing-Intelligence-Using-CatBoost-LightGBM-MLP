/**
 * gds.ts - TypeScript API client for GDS endpoints
 *
 * Add these types and functions to apps/web/lib/api.ts (or import from here).
 */

// ─────────────────────────────────────────────────────────────────────────────
// Internal fetch helper (mirrors the pattern used in api.ts)
// ─────────────────────────────────────────────────────────────────────────────

const LOCAL_API_BASE_URL = "http://127.0.0.1:8000";

function normalizeApiBaseUrl(raw: string): string {
  return raw.trim().replace(/\s+/g, "").replace(/\/+$/, "");
}

function getConfiguredApiBase(): string | null {
  const base = process.env.API_BASE_URL || process.env.NEXT_PUBLIC_API_BASE_URL || "";
  const normalized = normalizeApiBaseUrl(base);
  return normalized || null;
}

async function fetchJson<T>(path: string): Promise<T> {
  const apiBase = getConfiguredApiBase() ?? (process.env.NODE_ENV === "production" ? null : LOCAL_API_BASE_URL);
  if (!apiBase) {
    throw new Error("API_BASE_URL or NEXT_PUBLIC_API_BASE_URL is not configured");
  }

  const res = await fetch(`${apiBase}${path}`, {
    next: { revalidate: 60 },
  });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json() as Promise<T>;
}

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

export interface GdsFareRun {
  cycle_id: string;
  captured_at_utc: string;
  total_routes: number;
  total_airlines: number;
  total_fares: number;
}

export interface GdsFareRow {
  cycle_id: string;
  captured_at_utc: string;
  airline: string;
  origin: string;
  destination: string;
  route_key: string;
  rbd: string;
  cabin: string | null;
  fare_basis: string | null;
  journey_type: "OW" | "RT";
  base_fare: number | null;
  total_taxes: number | null;
  total_fare: number | null;
  currency: string | null;
  is_sold_out: boolean;
  is_unsaleable: boolean;
}

export interface GdsFareHistoryPoint {
  cycle_id: string;
  captured_at_utc: string;
  base_fare: number | null;
  total_taxes: number | null;
  total_fare: number | null;
  currency: string | null;
  is_sold_out: boolean;
}

export interface GdsChangeEvent {
  detected_at_utc: string;
  report_day: string;
  airline: string;
  origin: string;
  destination: string;
  route_key: string;
  rbd: string;
  cabin: string | null;
  change_type: "new" | "removed" | "price_change" | "sold_out" | "available";
  old_ow_fare: number | null;
  new_ow_fare: number | null;
  old_rt_fare: number | null;
  new_rt_fare: number | null;
}

export interface GdsChangeSummaryPoint {
  report_day: string;
  change_type: string;
  change_count: number;
}

export interface GdsTaxAirport {
  airport_code: string;
  last_updated: string;
  tax_count: number;
}

export interface GdsTaxRate {
  cycle_id: string;
  captured_at_utc: string;
  airport_code: string;
  tax_code: string;
  tax_name: string | null;
  category: string | null;
  subcategory: string | null;
  condition: string | null;
  currency: string | null;
  amount: number | null;
  status: "current" | "future" | "expired";
}

// ─────────────────────────────────────────────────────────────────────────────
// API functions
// ─────────────────────────────────────────────────────────────────────────────

export async function getGdsLatestRun(): Promise<GdsFareRun> {
  return fetchJson<GdsFareRun>("/gds/runs/latest");
}

export async function getGdsRuns(limit = 20): Promise<GdsFareRun[]> {
  return fetchJson<GdsFareRun[]>(`/gds/runs?limit=${limit}`);
}

export async function getGdsFares(params: {
  airline?: string;
  origin?: string;
  destination?: string;
  cabin?: string;
  journey_type?: "OW" | "RT";
  cycle_id?: string;
  limit?: number;
}): Promise<GdsFareRow[]> {
  const qs = new URLSearchParams();
  if (params.airline)       qs.set("airline",       params.airline);
  if (params.origin)        qs.set("origin",         params.origin);
  if (params.destination)   qs.set("destination",    params.destination);
  if (params.cabin)         qs.set("cabin",          params.cabin);
  if (params.journey_type)  qs.set("journey_type",   params.journey_type);
  if (params.cycle_id)      qs.set("cycle_id",       params.cycle_id);
  if (params.limit)         qs.set("limit",          String(params.limit));
  return fetchJson<GdsFareRow[]>(`/gds/fares?${qs.toString()}`);
}

export async function getGdsFareHistory(params: {
  route_key: string;
  airline: string;
  rbd: string;
  journey_type?: "OW" | "RT";
  days?: number;
}): Promise<GdsFareHistoryPoint[]> {
  const qs = new URLSearchParams({
    route_key:    params.route_key,
    airline:      params.airline,
    rbd:          params.rbd,
    journey_type: params.journey_type ?? "OW",
    days:         String(params.days ?? 30),
  });
  return fetchJson<GdsFareHistoryPoint[]>(`/gds/fares/history?${qs.toString()}`);
}

export async function getGdsChanges(params: {
  airline?: string;
  origin?: string;
  destination?: string;
  change_type?: string;
  days?: number;
  limit?: number;
}): Promise<GdsChangeEvent[]> {
  const qs = new URLSearchParams();
  if (params.airline)      qs.set("airline",      params.airline);
  if (params.origin)       qs.set("origin",        params.origin);
  if (params.destination)  qs.set("destination",   params.destination);
  if (params.change_type)  qs.set("change_type",   params.change_type);
  if (params.days)         qs.set("days",          String(params.days));
  if (params.limit)        qs.set("limit",         String(params.limit));
  return fetchJson<GdsChangeEvent[]>(`/gds/changes?${qs.toString()}`);
}

export async function getGdsChangeSummary(days = 7): Promise<GdsChangeSummaryPoint[]> {
  return fetchJson<GdsChangeSummaryPoint[]>(`/gds/changes/summary?days=${days}`);
}

export async function getGdsTaxAirports(): Promise<GdsTaxAirport[]> {
  return fetchJson<GdsTaxAirport[]>("/gds/taxes");
}

export async function getGdsTaxRates(
  airportCode: string,
  status: "current" | "future" | "expired" | "all" = "current"
): Promise<GdsTaxRate[]> {
  return fetchJson<GdsTaxRate[]>(
    `/gds/taxes/${encodeURIComponent(airportCode)}?status=${status}`
  );
}
