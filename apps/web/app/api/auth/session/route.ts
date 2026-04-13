import { NextResponse } from "next/server";

import { hasAdminSession } from "@/lib/admin";
import { getCurrentUserSession } from "@/lib/user-auth";

export async function GET() {
  const [{ user }, showAdminLink] = await Promise.all([
    getCurrentUserSession(),
    hasAdminSession(),
  ]);

  return NextResponse.json(
    {
      showAdminLink,
      user: user
        ? {
            email: user.email ?? null,
            full_name: user.full_name ?? null,
          }
        : null,
    },
    {
      headers: {
        "Cache-Control": "no-store",
      },
    }
  );
}
