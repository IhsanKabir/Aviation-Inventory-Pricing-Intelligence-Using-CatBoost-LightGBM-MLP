/**
 * app/admin/travelport-feedback/page.tsx - Admin inbox for desktop feedback
 */

import Link from "next/link";
import {
  getTravelportFeedback,
  type TravelportFeedback,
} from "@/lib/travelport-feedback";

interface Props {
  searchParams?: { status?: string };
}

function parseContext(contextJson: string | null): Record<string, string> {
  if (!contextJson) return {};
  try {
    return JSON.parse(contextJson) as Record<string, string>;
  } catch {
    return {};
  }
}

export default async function TravelportFeedbackAdminPage({ searchParams }: Props) {
  const status = searchParams?.status ?? "all";
  let feedbackItems: TravelportFeedback[] = [];
  let error: string | null = null;

  try {
    feedbackItems = await getTravelportFeedback({ limit: 100, status });
  } catch (e: any) {
    error = e?.message ?? "Failed to load feedback";
  }

  const tabs = ["all", "new", "reviewed", "resolved"] as const;
  const badgeColor: Record<string, string> = {
    bug: "bg-red-100 text-red-800",
    suggestion: "bg-blue-100 text-blue-800",
    question: "bg-amber-100 text-amber-800",
    other: "bg-gray-100 text-gray-700",
    general: "bg-gray-100 text-gray-700",
  };

  return (
    <main className="mx-auto max-w-screen-xl space-y-6 p-6">
      <div>
        <h1 className="text-2xl font-bold">Travelport Feedback</h1>
        <p className="text-sm text-muted-foreground">
          Messages submitted from the desktop TravelportAuto GUI.
        </p>
      </div>

      <div className="flex gap-1 rounded-lg border p-1 w-fit">
        {tabs.map((tab) => (
          <Link
            key={tab}
            href={`/admin/travelport-feedback?status=${tab}`}
            className={`rounded px-3 py-1 text-sm capitalize ${
              status === tab ? "bg-primary text-primary-foreground font-medium" : "hover:bg-muted"
            }`}
          >
            {tab}
          </Link>
        ))}
      </div>

      {error ? (
        <p className="text-sm text-destructive">{error}</p>
      ) : feedbackItems.length === 0 ? (
        <p className="text-sm text-muted-foreground">No feedback submitted yet.</p>
      ) : (
        <div className="space-y-4">
          {feedbackItems.map((item) => {
            const context = parseContext(item.context_json);
            return (
              <section key={item.feedback_id} className="rounded-xl border p-4 shadow-sm">
                <div className="flex flex-wrap items-center gap-2">
                  <span
                    className={`rounded-full px-2.5 py-1 text-xs font-medium ${
                      badgeColor[item.category] ?? badgeColor.general
                    }`}
                  >
                    {item.category}
                  </span>
                  <span className="text-xs text-muted-foreground">
                    {new Date(item.submitted_at_utc).toLocaleString()}
                  </span>
                  <span className="rounded-full bg-muted px-2 py-1 text-xs uppercase">
                    {item.status}
                  </span>
                </div>

                <h2 className="mt-3 text-lg font-semibold">{item.subject}</h2>
                <p className="mt-2 whitespace-pre-wrap text-sm">{item.message}</p>

                <div className="mt-4 grid gap-2 text-xs text-muted-foreground md:grid-cols-2">
                  <div>
                    Device: {item.device_name || item.hostname || "Unknown"}
                    {item.device_id ? ` (${item.device_id})` : ""}
                  </div>
                  <div>Version: {item.app_version || "Unknown"}</div>
                  <div>Source: {item.source || "desktop_gui"}</div>
                  <div>OS: {item.os_version || "Unknown"}</div>
                  {context.mode ? <div>Mode: {context.mode}</div> : null}
                  {context.route_filter ? <div>Route Filter: {context.route_filter}</div> : null}
                  {context.airline_filter ? <div>Airline Filter: {context.airline_filter}</div> : null}
                </div>

                {item.admin_note ? (
                  <div className="mt-4 rounded-lg bg-muted/50 p-3 text-sm">
                    <p className="font-medium">Admin Note</p>
                    <p className="mt-1 whitespace-pre-wrap">{item.admin_note}</p>
                  </div>
                ) : null}
              </section>
            );
          })}
        </div>
      )}
    </main>
  );
}

export const metadata = {
  title: "Travelport Feedback Admin",
};
