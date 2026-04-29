"use client";

import { useState } from "react";

import { AdminAccessRequestsDashboard } from "@/components/admin-access-requests-dashboard";
import { AdminSearchConfigPanel } from "@/components/admin-search-config-panel";
import type { ReportAccessRequest } from "@/lib/api";
import type { AdminSearchConfig } from "@/lib/search-config";

function AdminOverviewCard({
  label,
  value,
  note,
  tone = "neutral",
}: {
  label: string;
  value: string | number;
  note: string;
  tone?: "good" | "neutral" | "warn";
}) {
  return (
    <div className="admin-overview-card" data-tone={tone}>
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{note}</small>
    </div>
  );
}

export function AdminOpsConsole({
  initialConfig,
  initialItems,
}: {
  initialConfig: AdminSearchConfig;
  initialItems: ReportAccessRequest[];
}) {
  const [accessItems, setAccessItems] = useState(initialItems);
  const [currentConfig, setCurrentConfig] = useState(initialConfig);
  const [configDirty, setConfigDirty] = useState(false);

  const pendingCount = accessItems.filter((item) => item.status === "pending").length;
  const activeRouteCount = currentConfig.routeProfiles.filter(
    (route) =>
      route.marketTripProfiles.length ||
      route.activeMarketTripProfiles.length ||
      route.trainingMarketTripProfiles.length ||
      route.deepMarketTripProfiles.length,
  ).length;

  return (
    <div className="admin-console">
      <section className="admin-hero">
        <div>
          <div className="admin-section-kicker">Internal operations</div>
          <h1 className="page-title">Admin ops console</h1>
          <p className="page-copy">
            Review access requests first, then tune search behavior, schedules, trip profiles, and route assignments without changing admin APIs.
          </p>
        </div>

        <div className="admin-overview-grid" aria-label="Admin overview">
          <AdminOverviewCard
            label="Pending requests"
            note="Need a decision"
            tone={pendingCount ? "warn" : "good"}
            value={pendingCount}
          />
          <AdminOverviewCard label="Requests loaded" note="Current queue" value={accessItems.length} />
          <AdminOverviewCard
            label="Config state"
            note={configDirty ? "Save before applying" : "Matches saved config"}
            tone={configDirty ? "warn" : "good"}
            value={configDirty ? "Unsaved" : "Clean"}
          />
          <AdminOverviewCard label="Trip profiles" note="Search windows" value={currentConfig.tripProfiles.length} />
          <AdminOverviewCard label="Assigned routes" note="Routes with profiles" value={activeRouteCount} />
        </div>
      </section>

      <section className="admin-section admin-section-priority">
        <div className="admin-section-header">
          <div>
            <div className="admin-section-kicker">Request queue</div>
            <h2>Access request review</h2>
            <p>Pending items stay at the top. Approve route views, request payment, or reject mismatched scopes from this queue.</p>
          </div>
        </div>
        <AdminAccessRequestsDashboard initialItems={initialItems} onItemsChange={setAccessItems} />
      </section>

      <section className="admin-section">
        <div className="admin-section-header">
          <div>
            <div className="admin-section-kicker">Search controls</div>
            <h2>Search configuration</h2>
            <p>Manage defaults, schedules, trip windows, route assignments, holidays, and advanced market-prior settings.</p>
          </div>
        </div>
        <AdminSearchConfigPanel
          initialConfig={initialConfig}
          onConfigChange={setCurrentConfig}
          onDirtyChange={setConfigDirty}
        />
      </section>
    </div>
  );
}
