import "server-only";

import { cookies } from "next/headers";
import { getApiBaseUrl, type AuthenticatedUser } from "@/lib/api";

const USER_SESSION_COOKIE = "ap_user_session";

export function getUserSessionCookieName() {
  return USER_SESSION_COOKIE;
}

async function getOAuthSession() {
  const authSecret = process.env.AUTH_SECRET || process.env.NEXTAUTH_SECRET;
  const googleConfigured = Boolean(
    authSecret &&
    process.env.AUTH_GOOGLE_ID &&
    process.env.AUTH_GOOGLE_SECRET
  );

  if (!googleConfigured) {
    return null;
  }

  try {
    const [{ getServerSession }, { authOptions }] = await Promise.all([
      import("next-auth"),
      import("@/auth"),
    ]);
    return await getServerSession(authOptions);
  } catch {
    return null;
  }
}

async function fetchCurrentUser(token: string): Promise<AuthenticatedUser | null> {
  try {
    const response = await fetch(`${getApiBaseUrl()}/api/v1/user-auth/me`, {
      method: "GET",
      headers: {
        "X-User-Session": token
      },
      cache: "no-store"
    });
    if (!response.ok) {
      return null;
    }
    const payload = (await response.json()) as { user?: AuthenticatedUser | null };
    return payload.user ?? null;
  } catch {
    return null;
  }
}

export async function getCurrentUserSession() {
  const oauthSession = await getOAuthSession();
  if (oauthSession?.apiSessionToken && oauthSession.apiUser) {
    return {
      token: oauthSession.apiSessionToken,
      user: oauthSession.apiUser,
      bridgeFailed: false
    };
  }

  const cookieStore = await cookies();
  const token = cookieStore.get(USER_SESSION_COOKIE)?.value || "";
  if (!token) {
    // A Google sign-in that reached NextAuth but never obtained an API session
    // token means the server-side OAuth bridge failed (usually an
    // OAUTH_BRIDGE_SECRET mismatch between Vercel and the API). Signal it so the
    // UI can explain the loop instead of silently bouncing back to "sign in".
    const bridgeFailed = Boolean(oauthSession?.user?.email);
    return { token: "", user: null as AuthenticatedUser | null, bridgeFailed };
  }
  const user = await fetchCurrentUser(token);
  return { token, user, bridgeFailed: false };
}
