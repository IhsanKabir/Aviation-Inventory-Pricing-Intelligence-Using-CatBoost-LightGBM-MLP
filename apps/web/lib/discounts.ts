import "server-only";

import { getApiBaseUrl } from "@/lib/api";

/** Per-cell highlight flag computed server-side by discount_engine.highlight. */
export type DiscountCellFlag = "highest" | "second" | "changed" | "none";

export interface DiscountRow {
  label: string;
  kind: "b2b" | "b2c" | "sep" | string;
  cells?: Record<string, string>;
  highlights?: Record<string, DiscountCellFlag>;
}

export interface DiscountBestCell {
  pct: number;
  channel: string;
  short: string;
  display: string;
}

export interface DiscountGridBlock {
  columns: string[];
  rows: DiscountRow[];
  best?: Record<string, DiscountBestCell>;
}

export interface DiscountReportPayload {
  report_date: string;
  report_time: string;
  generated_at: string;
  normalized: boolean;
  true_base: { source: string; airlines_covered: string[]; sample_count: number };
  channel_status: Record<string, string>;
  sources: Record<string, { kinds: string[]; true_base: boolean }>;
  grids: Partial<Record<"DOM" | "INTL", DiscountGridBlock>>;
  prev_report_date?: string | null;
}

export interface StoredDiscountReport {
  report_id: string;
  report_date: string;
  generated_at: string | null;
  submitted_by_email: string | null;
  updated_at_utc: string | null;
  prev_report_date?: string | null;
  report: DiscountReportPayload;
}

export interface DiscountHistoryItem {
  report_id: string;
  report_date: string;
  generated_at: string | null;
  submitted_by_email: string | null;
  updated_at_utc: string | null;
}

export interface DiscountFetchResult<T> {
  ok: boolean;
  status: number;
  data: T | null;
  error?: string;
}

async function fetchDiscountApi<T>(path: string, token: string): Promise<DiscountFetchResult<T>> {
  if (!token) {
    return { ok: false, status: 401, data: null, error: "Sign in is required." };
  }
  try {
    const response = await fetch(`${getApiBaseUrl()}${path}`, {
      headers: { "X-User-Session": token },
      cache: "no-store",
    });
    if (!response.ok) {
      let detail = `${response.status} ${response.statusText}`;
      try {
        const body = (await response.json()) as { detail?: string };
        if (body.detail) {
          detail = body.detail;
        }
      } catch {
        // non-JSON error body — keep the status text
      }
      return { ok: false, status: response.status, data: null, error: detail };
    }
    return { ok: true, status: response.status, data: (await response.json()) as T };
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : "API unreachable";
    return { ok: false, status: 0, data: null, error: message };
  }
}

/** Latest team report, or a specific day when `date` (YYYY-MM-DD) is given. */
export async function getDiscountReport(
  token: string,
  date?: string,
): Promise<DiscountFetchResult<StoredDiscountReport>> {
  const path = date
    ? `/api/v1/discount-reports/by-date?date=${encodeURIComponent(date)}`
    : "/api/v1/discount-reports/latest";
  return fetchDiscountApi<StoredDiscountReport>(path, token);
}

export async function getDiscountHistory(
  token: string,
  limit = 30,
): Promise<DiscountFetchResult<{ items: DiscountHistoryItem[] }>> {
  return fetchDiscountApi<{ items: DiscountHistoryItem[] }>(
    `/api/v1/discount-reports/history?limit=${limit}`,
    token,
  );
}
