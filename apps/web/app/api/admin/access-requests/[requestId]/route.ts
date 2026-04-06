import { NextResponse } from "next/server";

import { hasAdminSession } from "@/lib/admin";
import { getApiBaseUrl } from "@/lib/api";

async function ensureAdmin() {
  if (!(await hasAdminSession())) {
    return NextResponse.json({ detail: "Admin session required." }, { status: 401 });
  }
  return null;
}

function getAdminToken() {
  return (
    process.env.REPORT_ACCESS_ADMIN_TOKEN?.trim() ||
    process.env.WEB_ADMIN_PASSWORD?.trim() ||
    ""
  );
}

export async function PATCH(
  request: Request,
  context: {
    params: Promise<{ requestId: string }>;
  }
) {
  const unauthorized = await ensureAdmin();
  if (unauthorized) {
    return unauthorized;
  }

  const adminToken = getAdminToken();
  if (!adminToken) {
    return NextResponse.json({ detail: "Admin API token is not configured." }, { status: 503 });
  }

  const { requestId } = await context.params;
  const body = await request.text();
  const response = await fetch(`${getApiBaseUrl()}/api/v1/access-requests/${encodeURIComponent(requestId)}`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      "X-Admin-Token": adminToken
    },
    body,
    cache: "no-store"
  });

  const text = await response.text();
  return new NextResponse(text, {
    status: response.status,
    headers: {
      "Content-Type": response.headers.get("content-type") || "application/json"
    }
  });
}
