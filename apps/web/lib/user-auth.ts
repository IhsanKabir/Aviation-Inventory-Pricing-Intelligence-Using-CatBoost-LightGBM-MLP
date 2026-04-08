import "server-only";

import { cookies } from "next/headers";
import { getServerSession } from "next-auth";

import { authOptions } from "@/auth";
import { getApiBaseUrl, type AuthenticatedUser } from "@/lib/api";

const USER_SESSION_COOKIE = "ap_user_session";

export function getUserSessionCookieName() {
  return USER_SESSION_COOKIE;
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
  const oauthSession = await getServerSession(authOptions);
  if (oauthSession?.apiSessionToken && oauthSession.apiUser) {
    return {
      token: oauthSession.apiSessionToken,
      user: oauthSession.apiUser
    };
  }

  const cookieStore = await cookies();
  const token = cookieStore.get(USER_SESSION_COOKIE)?.value || "";
  if (!token) {
    return { token: "", user: null as AuthenticatedUser | null };
  }
  const user = await fetchCurrentUser(token);
  return { token, user };
}
