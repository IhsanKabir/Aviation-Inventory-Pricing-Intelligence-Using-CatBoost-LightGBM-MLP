import { cookies } from "next/headers";
import { NextResponse } from "next/server";

import {
  buildAdminSessionCookieValue,
  getAdminSessionCookieName,
  isAdminConfigured,
  verifyAdminCredentials
} from "@/lib/admin";

// Per-IP throttle on admin-password attempts. In-process (per serverless
// instance) — not global, but it materially raises the cost of a brute force
// from one source without any external dependency.
const ATTEMPTS = new Map<string, number[]>();
const WINDOW_MS = 60_000;
const MAX_ATTEMPTS = 8;

function tooManyAttempts(ip: string): boolean {
  const now = Date.now();
  const recent = (ATTEMPTS.get(ip) ?? []).filter((t) => now - t < WINDOW_MS);
  if (recent.length >= MAX_ATTEMPTS) {
    ATTEMPTS.set(ip, recent);
    return true;
  }
  recent.push(now);
  ATTEMPTS.set(ip, recent);
  return false;
}

export async function POST(request: Request) {
  if (!isAdminConfigured()) {
    return NextResponse.json({ detail: "Admin login is not configured." }, { status: 503 });
  }

  const ip =
    request.headers.get("x-forwarded-for")?.split(",").pop()?.trim() || "unknown";
  if (tooManyAttempts(ip)) {
    return NextResponse.json(
      { detail: "Too many attempts. Please wait a minute and try again." },
      { status: 429 }
    );
  }

  const payload = (await request.json().catch(() => null)) as
    | {
        username?: string;
        password?: string;
      }
    | null;

  const username = String(payload?.username || "");
  const password = String(payload?.password || "");
  if (!verifyAdminCredentials(username, password)) {
    return NextResponse.json({ detail: "Invalid admin credentials." }, { status: 401 });
  }

  const cookieStore = await cookies();
  cookieStore.set({
    name: getAdminSessionCookieName(),
    value: buildAdminSessionCookieValue(),
    httpOnly: true,
    // Admin area has no cross-site entry flows, so the stricter CSRF posture is free.
    sameSite: "strict",
    secure: process.env.NODE_ENV === "production",
    path: "/",
    maxAge: 60 * 60 * 12
  });

  return NextResponse.json({ ok: true });
}
