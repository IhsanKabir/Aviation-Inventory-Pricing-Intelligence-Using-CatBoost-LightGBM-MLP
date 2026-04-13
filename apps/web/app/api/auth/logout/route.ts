import { cookies } from "next/headers";
import { NextResponse } from "next/server";

import { getApiBaseUrl } from "@/lib/api";
import { getUserSessionCookieName } from "@/lib/user-auth";

export async function POST() {
  let oauthSession:
    | {
        apiSessionToken?: string | null;
      }
    | null = null;

  const authSecret = process.env.AUTH_SECRET || process.env.NEXTAUTH_SECRET;
  const googleConfigured = Boolean(
    authSecret &&
    process.env.AUTH_GOOGLE_ID &&
    process.env.AUTH_GOOGLE_SECRET
  );

  if (googleConfigured) {
    try {
      const [{ getServerSession }, { authOptions }] = await Promise.all([
        import("next-auth"),
        import("@/auth"),
      ]);
      oauthSession = await getServerSession(authOptions);
    } catch {
      oauthSession = null;
    }
  }

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
