import { NextResponse } from "next/server";

import { hasAdminSession } from "@/lib/admin";
import { applySchedulerSettingsOnMachine } from "@/lib/search-config";

export async function POST() {
  if (!(await hasAdminSession())) {
    return NextResponse.json({ detail: "Unauthorized" }, { status: 401 });
  }

  try {
    const payload = await applySchedulerSettingsOnMachine();
    return NextResponse.json(payload, { status: 200 });
  } catch (error) {
    return NextResponse.json(
      {
        detail: error instanceof Error ? error.message : "Unable to apply scheduler settings on this machine.",
      },
      { status: 400 },
    );
  }
}
