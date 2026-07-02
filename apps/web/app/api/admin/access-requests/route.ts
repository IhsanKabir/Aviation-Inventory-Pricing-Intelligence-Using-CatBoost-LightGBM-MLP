import { NextResponse } from "next/server";

import { getAdminApiToken, hasAdminSession } from "@/lib/admin";
import { getApiBaseUrl } from "@/lib/api";

async function ensureAdmin() {
  if (!(await hasAdminSession())) {
    return NextResponse.json({ detail: "Admin session required." }, { status: 401 });
  }
  return null;
}

export async function GET(request: Request) {
  const unauthorized = await ensureAdmin();
  if (unauthorized) {
    return unauthorized;
  }

  const adminToken = getAdminApiToken();
  if (!adminToken) {
    return NextResponse.json({ detail: "Admin API token is not configured." }, { status: 503 });
  }

  const url = new URL(request.url);
  const upstreamUrl = `${getApiBaseUrl()}/api/v1/access-requests${url.search}`;
  try {
    const response = await fetch(upstreamUrl, {
      headers: {
        "X-Admin-Token": adminToken
      },
      cache: "no-store"
    });

    const text = await response.text();
    return new NextResponse(text, {
      status: response.status,
      headers: {
        "Content-Type": response.headers.get("content-type") || "application/json"
      }
    });
  } catch {
    return NextResponse.json({ detail: "Admin request service is temporarily unavailable." }, { status: 502 });
  }
}
