import "server-only";

import { getAdminApiToken } from "@/lib/admin";
import { getApiBaseUrl } from "@/lib/api";

export interface SystemHealthError {
  occurred_at_utc: string | null;
  method: string | null;
  path: string | null;
  status: number | null;
  error_type: string | null;
  message: string | null;
  instance_id: string | null;
}

export interface SystemHealth {
  version: string;
  instance_id: string;
  uptime_seconds: number;
  services: { api: boolean; database: boolean; bigquery: boolean };
  requests_1h: {
    window_seconds: number;
    total_requests: number;
    error_requests: number;
    error_rate: number;
    latency_p50_ms: number;
    latency_p95_ms: number;
    per_instance: boolean;
    instance_id: string;
  };
  errors_24h: number;
  recent_errors: SystemHealthError[];
  latest_discount_report_date: string | null;
}

/** Fetch live system observability from the API (admin-token gated, server-side). */
export async function getSystemHealth(): Promise<
  { ok: true; data: SystemHealth } | { ok: false; error: string }
> {
  const adminToken = getAdminApiToken();
  if (!adminToken) {
    return { ok: false, error: "Admin API token is not configured." };
  }
  try {
    const response = await fetch(`${getApiBaseUrl()}/api/v1/admin/system-health`, {
      headers: { "X-Admin-Token": adminToken },
      cache: "no-store",
    });
    if (!response.ok) {
      return { ok: false, error: `${response.status} ${response.statusText}` };
    }
    return { ok: true, data: (await response.json()) as SystemHealth };
  } catch (error: unknown) {
    return { ok: false, error: error instanceof Error ? error.message : "API unreachable" };
  }
}
