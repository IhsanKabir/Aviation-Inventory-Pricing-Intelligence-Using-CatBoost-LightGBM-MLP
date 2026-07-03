import { NextResponse } from "next/server";

import { getAdminApiToken, hasAdminSession } from "@/lib/admin";
import { getApiBaseUrl } from "@/lib/api";

async function ensureAdmin() {
  if (!(await hasAdminSession())) {
    return NextResponse.json({ detail: "Admin session required." }, { status: 401 });
  }
  return null;
}

export async function GET() {
  const unauthorized = await ensureAdmin();
  if (unauthorized) {
    return unauthorized;
  }
  const adminToken = getAdminApiToken();
  if (!adminToken) {
    return NextResponse.json({ detail: "Admin API token is not configured." }, { status: 503 });
  }
  const response = await fetch(`${getApiBaseUrl()}/api/v1/admin/users`, {
    headers: { "X-Admin-Token": adminToken },
    cache: "no-store"
  });
  const text = await response.text();
  return new NextResponse(text, {
    status: response.status,
    headers: { "Content-Type": response.headers.get("Content-Type") || "application/json" }
  });
}
