import { cookies } from "next/headers";
import { NextResponse } from "next/server";

import { getAdminSessionCookieName } from "@/lib/admin";

export async function POST() {
  const cookieStore = await cookies();
  cookieStore.set({
    name: getAdminSessionCookieName(),
    value: "",
    httpOnly: true,
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production",
    path: "/",
    maxAge: 0
  });
  return NextResponse.json({ ok: true });
}
