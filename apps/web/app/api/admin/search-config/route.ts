import { NextResponse } from "next/server";

import { hasAdminSession } from "@/lib/admin";
import { readAdminSearchConfig, writeAdminSearchConfig } from "@/lib/search-config";

export async function GET() {
  if (!(await hasAdminSession())) {
    return NextResponse.json({ detail: "Unauthorized" }, { status: 401 });
  }

  try {
    const payload = await readAdminSearchConfig();
    return NextResponse.json(payload, { status: 200 });
  } catch (error) {
    return NextResponse.json(
      {
        detail: error instanceof Error ? error.message : "Unable to load search configuration."
      },
      { status: 500 }
    );
  }
}

export async function PUT(request: Request) {
  if (!(await hasAdminSession())) {
    return NextResponse.json({ detail: "Unauthorized" }, { status: 401 });
  }

  try {
    const body = (await request.json()) as Parameters<typeof writeAdminSearchConfig>[0];
    const payload = await writeAdminSearchConfig(body);
    return NextResponse.json(payload, { status: 200 });
  } catch (error) {
    return NextResponse.json(
      {
        detail: error instanceof Error ? error.message : "Unable to save search configuration."
      },
      { status: 400 }
    );
  }
}
