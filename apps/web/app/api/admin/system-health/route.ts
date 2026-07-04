import { NextResponse } from "next/server";

import { getAdminApiToken, hasAdminSession } from "@/lib/admin";
import { getApiBaseUrl } from "@/lib/api";

export async function GET() {
  if (!(await hasAdminSession())) {
    return NextResponse.json({ detail: "Admin session required." }, { status: 401 });
  }
  const adminToken = getAdminApiToken();
  if (!adminToken) {
    return NextResponse.json({ detail: "Admin API token is not configured." }, { status: 503 });
  }
  try {
    const response = await fetch(`${getApiBaseUrl()}/api/v1/admin/system-health`, {
      headers: { "X-Admin-Token": adminToken },
      cache: "no-store",
    });
    const text = await response.text();
    return new NextResponse(text, {
      status: response.status,
      headers: { "Content-Type": response.headers.get("Content-Type") || "application/json" },
    });
  } catch (error: unknown) {
    const detail = error instanceof Error ? error.message : "API unreachable";
    return NextResponse.json({ detail }, { status: 502 });
  }
}
