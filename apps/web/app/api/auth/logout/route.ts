import { cookies } from "next/headers";
import { getServerSession } from "next-auth";
import { NextResponse } from "next/server";

import { authOptions } from "@/auth";
import { getApiBaseUrl } from "@/lib/api";
import { getUserSessionCookieName } from "@/lib/user-auth";

export async function POST() {
  const oauthSession = await getServerSession(authOptions);
  const cookieStore = await cookies();
  const token = oauthSession?.apiSessionToken || cookieStore.get(getUserSessionCookieName())?.value || "";

  if (token) {
    try {
      await fetch(`${getApiBaseUrl()}/api/v1/user-auth/logout`, {
        method: "POST",
        headers: {
          "X-User-Session": token
        },
        cache: "no-store"
      });
    } catch {
      // Best-effort revoke; local cookie removal still signs the user out here.
    }
  }

  cookieStore.set({
    name: getUserSessionCookieName(),
    value: "",
    httpOnly: true,
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production",
    path: "/",
    maxAge: 0
  });

  return NextResponse.json({ ok: true });
}
