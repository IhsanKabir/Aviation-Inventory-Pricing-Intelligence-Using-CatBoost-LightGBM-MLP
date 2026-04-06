import { cookies } from "next/headers";
import { NextResponse } from "next/server";

import {
  buildAdminSessionCookieValue,
  getAdminSessionCookieName,
  isAdminConfigured,
  verifyAdminCredentials
} from "@/lib/admin";

export async function POST(request: Request) {
  if (!isAdminConfigured()) {
    return NextResponse.json({ detail: "Admin login is not configured." }, { status: 503 });
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
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production",
    path: "/",
    maxAge: 60 * 60 * 12
  });

  return NextResponse.json({ ok: true });
}
