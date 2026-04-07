import { AdminAccessRequestsDashboard } from "@/components/admin-access-requests-dashboard";
import { AdminSearchConfigPanel } from "@/components/admin-search-config-panel";
import { DataPanel } from "@/components/data-panel";
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
    <>
      <h1 className="page-title">Admin dashboard</h1>
      <p className="page-copy">
        Review route-access requests, approve or reject scopes, and keep sensitive monitoring areas separated from the public user experience.
      </p>

      <div className="stack">
        <DataPanel
          title="Search configuration"
          copy="Use the friendly fields below to control default search behavior without editing JSON by hand. Advanced users can still adjust the raw market-priors block."
        >
          <AdminSearchConfigPanel initialConfig={initialConfig} />
        </DataPanel>

        <DataPanel
          title="Access request review"
          copy="Pending items stay at the top. Use this queue to approve route views, request payment, or reject mismatched scopes."
        >
          <AdminAccessRequestsDashboard initialItems={initialItems} />
        </DataPanel>
      </div>
    </>
  );
}
