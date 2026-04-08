import { cookies } from "next/headers";
import { NextResponse } from "next/server";

import { getApiBaseUrl, type AuthenticatedUser } from "@/lib/api";
import { getUserSessionCookieName } from "@/lib/user-auth";

type RegisterPayload = {
  user?: AuthenticatedUser;
  session_token?: string;
  detail?: string;
};

export async function POST(request: Request) {
  const payload = (await request.json().catch(() => null)) as
    | {
        fullName?: string;
        email?: string;
        password?: string;
      }
    | null;

  const response = await fetch(`${getApiBaseUrl()}/api/v1/user-auth/register`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      full_name: String(payload?.fullName || ""),
      email: String(payload?.email || ""),
      password: String(payload?.password || "")
    }),
    cache: "no-store"
  });

  const data = (await response.json().catch(() => null)) as RegisterPayload | null;
  if (!response.ok || !data?.session_token || !data.user) {
    return NextResponse.json(
      { detail: data?.detail || "Unable to create account." },
      { status: response.status || 500 }
    );
  }

  const cookieStore = await cookies();
  cookieStore.set({
    name: getUserSessionCookieName(),
    value: data.session_token,
    httpOnly: true,
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production",
    path: "/",
    maxAge: 60 * 60 * 24 * 30
  });

  return NextResponse.json({ ok: true, user: data.user });
}

