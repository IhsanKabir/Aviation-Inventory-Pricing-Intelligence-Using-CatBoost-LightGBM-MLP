/**
 * travelport-feedback.ts - API helpers for Travelport desktop feedback
 */

import { fetchJson } from "./api";

export interface TravelportFeedback {
  feedback_id: string;
  submitted_at_utc: string;
  category: string;
  subject: string;
  message: string;
  status: string;
  app_version: string | null;
  device_id: string | null;
  device_name: string | null;
  hostname: string | null;
  os_version: string | null;
  source: string | null;
  context_json: string | null;
  admin_note: string | null;
}

export async function getTravelportFeedback(params?: {
  limit?: number;
  status?: string;
}): Promise<TravelportFeedback[]> {
  const qs = new URLSearchParams();
  if (params?.limit) qs.set("limit", String(params.limit));
  if (params?.status) qs.set("status", params.status);
  const suffix = qs.toString() ? `?${qs.toString()}` : "";
  return fetchJson<TravelportFeedback[]>(`/travelport-agent/feedback${suffix}`);
}
