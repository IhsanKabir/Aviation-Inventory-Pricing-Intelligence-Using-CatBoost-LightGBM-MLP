export type CycleSummary = {
  cycle_id: string | null;
  cycle_started_at_utc?: string;
  cycle_completed_at_utc?: string;
  offer_rows?: number;
  airline_count?: number;
  route_count?: number;
};

export type HealthPayload = {
  database_ok: boolean;
  latest_cycle_id: string | null;
  latest_cycle_completed_at_utc?: string | null;
};

export type AirlineItem = {
  airline: string;
  first_seen_at_utc?: string;
  last_seen_at_utc?: string;
  offer_rows?: number;
};

export type RouteItem = {
  route_key: string;
  origin: string;
  destination: string;
  offer_rows?: number;
  airlines_present?: number;
};

type FetchResult<T> = {
  ok: boolean;
  data: T | null;
  error?: string;
};

export function getApiBaseUrl(): string {
  return (
    process.env.API_BASE_URL ||
    process.env.NEXT_PUBLIC_API_BASE_URL ||
    "http://127.0.0.1:8000"
  ).replace(/\/+$/, "");
}

async function fetchJson<T>(path: string): Promise<FetchResult<T>> {
  try {
    const response = await fetch(`${getApiBaseUrl()}${path}`, {
      cache: "no-store"
    });

    if (!response.ok) {
      return {
        ok: false,
        data: null,
        error: `${response.status} ${response.statusText}`
      };
    }

    const data = (await response.json()) as T;
    return { ok: true, data };
  } catch (error) {
    return {
      ok: false,
      data: null,
      error: error instanceof Error ? error.message : "Unknown API error"
    };
  }
}

export async function getDashboardPayload() {
  const [health, latestCycle, airlines, routes] = await Promise.all([
    fetchJson<HealthPayload>("/health"),
    fetchJson<CycleSummary>("/api/v1/reporting/cycles/latest"),
    fetchJson<{ items: AirlineItem[] }>("/api/v1/meta/airlines"),
    fetchJson<{ items: RouteItem[] }>("/api/v1/meta/routes")
  ]);

  return { health, latestCycle, airlines, routes };
}
