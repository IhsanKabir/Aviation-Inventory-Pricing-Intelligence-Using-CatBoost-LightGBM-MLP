import { redirect } from "next/navigation";

import { AdminLoginForm } from "@/components/admin-login-form";
import { hasAdminSession, isAdminConfigured } from "@/lib/admin";

export default async function AdminLoginPage({
  searchParams
}: {
  searchParams?: Promise<{ next?: string }>;
}) {
  if (!isAdminConfigured()) {
    return (
      <>
        <h1 className="page-title">Admin access is not configured</h1>
        <p className="page-copy">
          Set <code>WEB_ADMIN_PASSWORD</code> or <code>REPORT_ACCESS_ADMIN_TOKEN</code> on the web app to enable the admin area.
        </p>
      </>
    );
  }

  const next = (await searchParams)?.next || "/admin";
  if (await hasAdminSession()) {
    redirect(next);
  }

  return (
    <>
      <h1 className="page-title">Admin area</h1>
      <p className="page-copy">
        This area is reserved for internal reviewers who manage access approvals and sensitive monitoring pages.
      </p>
      <div className="stack">
        <AdminLoginForm />
      </div>
    </>
  );
}
