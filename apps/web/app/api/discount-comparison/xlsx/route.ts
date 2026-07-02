import { NextRequest, NextResponse } from "next/server";

import { getApiBaseUrl } from "@/lib/api";
import { getCurrentUserSession } from "@/lib/user-auth";

export const dynamic = "force-dynamic";

/** Stream the regenerated discount-report workbook from the backend.
 *
 * A plain <a download> navigation cannot send the X-User-Session header, so this
 * route bridges: cookie/OAuth session -> header -> backend /discount-reports/xlsx,
 * then streams the workbook bytes back to the browser. */
export async function GET(request: NextRequest) {
  const { token } = await getCurrentUserSession();
  if (!token) {
    return NextResponse.json({ detail: "Sign in is required." }, { status: 401 });
  }

  const date = request.nextUrl.searchParams.get("date");
  const suffix = date ? `?date=${encodeURIComponent(date)}` : "";
  const upstream = await fetch(
    `${getApiBaseUrl()}/api/v1/discount-reports/xlsx${suffix}`,
    { headers: { "X-User-Session": token }, cache: "no-store" },
  );

  if (!upstream.ok) {
    let detail = `${upstream.status} ${upstream.statusText}`;
    try {
      const body = (await upstream.json()) as { detail?: string };
      if (body.detail) {
        detail = body.detail;
      }
    } catch {
      // keep the status text when the error body is not JSON
    }
    return NextResponse.json({ detail }, { status: upstream.status });
  }

  const filename = `OTA_Discount_${date ?? "latest"}.xlsx`;
  return new NextResponse(upstream.body, {
    status: 200,
    headers: {
      "Content-Type":
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      "Content-Disposition": `attachment; filename="${filename}"`,
      "Cache-Control": "no-store",
    },
  });
}
