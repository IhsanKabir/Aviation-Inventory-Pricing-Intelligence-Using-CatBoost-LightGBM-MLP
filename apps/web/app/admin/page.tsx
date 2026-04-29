import { AdminOpsConsole } from "@/components/admin-ops-console";
import { requireAdminSession } from "@/lib/admin";
import type { ReportAccessRequest } from "@/lib/api";
import { getApiBaseUrl } from "@/lib/api";
import { readAdminSearchConfig } from "@/lib/search-config";

async function loadInitialRequests() {
  const adminToken =
    process.env.REPORT_ACCESS_ADMIN_TOKEN?.trim() ||
    process.env.WEB_ADMIN_PASSWORD?.trim() ||
    "";

  if (!adminToken) {
    return [];
  }

  try {
    const response = await fetch(`${getApiBaseUrl()}/api/v1/access-requests?limit=100`, {
      headers: {
        "X-Admin-Token": adminToken
      },
      cache: "no-store"
    });
    if (!response.ok) {
      return [];
    }
    const payload = (await response.json()) as { items?: ReportAccessRequest[] };
    return payload.items || [];
  } catch {
    return [];
  }
}

export default async function AdminPage() {
  await requireAdminSession("/admin");
  const [initialItems, initialConfig] = await Promise.all([
    loadInitialRequests(),
    readAdminSearchConfig(),
  ]);

  return (
    <AdminOpsConsole initialConfig={initialConfig} initialItems={initialItems} />
  );
}
