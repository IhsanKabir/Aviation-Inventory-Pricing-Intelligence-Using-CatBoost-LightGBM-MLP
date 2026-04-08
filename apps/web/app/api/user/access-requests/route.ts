import { NextResponse } from "next/server";

import { getApiBaseUrl } from "@/lib/api";
import { getCurrentUserSession } from "@/lib/user-auth";

export async function POST(request: Request) {
  const { token } = await getCurrentUserSession();
  if (!token) {
    return NextResponse.json({ detail: "Sign in is required." }, { status: 401 });
  }

  const body = await request.text();
  const response = await fetch(`${getApiBaseUrl()}/api/v1/access-requests`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-User-Session": token
    },
    body,
    cache: "no-store"
  });

  const payload = await response.text();
  return new NextResponse(payload, {
    status: response.status,
    headers: {
      "Content-Type": response.headers.get("Content-Type") || "application/json"
    }
  });
}
