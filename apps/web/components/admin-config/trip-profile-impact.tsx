"use client";

import { useMemo } from "react";

import type { AdminRouteProfileEntry, AdminTripProfileEntry } from "@/lib/search-config";

interface TripProfileImpactPreviewProps {
  profile: AdminTripProfileEntry | null;
  routeProfiles: AdminRouteProfileEntry[];
}

export function TripProfileImpactPreview({ profile, routeProfiles }: TripProfileImpactPreviewProps) {
  const impact = useMemo(() => {
    if (!profile) {
      return null;
    }
    const airlineSet = new Set<string>();
    const availableRoutes: string[] = [];
    const operationalRoutes: string[] = [];
    const trainingRoutes: string[] = [];
    const deepRoutes: string[] = [];

    for (const route of routeProfiles) {
      const label = `${route.airline} ${route.routeCode}`;
      let used = false;
      if (route.marketTripProfiles.includes(profile.key)) {
        availableRoutes.push(label);
        used = true;
      }
      if (route.activeMarketTripProfiles.includes(profile.key)) {
        operationalRoutes.push(label);
        used = true;
      }
      if (route.trainingMarketTripProfiles.includes(profile.key)) {
        trainingRoutes.push(label);
        used = true;
      }
      if (route.deepMarketTripProfiles.includes(profile.key)) {
        deepRoutes.push(label);
        used = true;
      }
      if (used) {
        airlineSet.add(route.airline);
      }
    }

    return {
      airlineCount: airlineSet.size,
      routeCount: new Set([...availableRoutes, ...operationalRoutes, ...trainingRoutes, ...deepRoutes]).size,
      airlines: Array.from(airlineSet).sort(),
      availableRoutes,
      operationalRoutes,
      trainingRoutes,
      deepRoutes,
    };
  }, [routeProfiles, profile]);

  if (!profile || !impact) {
    return null;
  }

  return (
    <div className="admin-impact-preview">
      <div>
        <strong>Impact preview</strong>
        <div className="panel-copy" style={{ marginBottom: 0 }}>
          This shows where <strong>{profile.key}</strong> is currently assigned before you save or bulk-apply changes.
        </div>
      </div>

      <div className="admin-config-summary-grid">
        <div className="admin-config-summary">
          <strong>Airlines using it</strong>
          <span>{impact.airlineCount}</span>
        </div>
        <div className="admin-config-summary">
          <strong>Routes touched</strong>
          <span>{impact.routeCount}</span>
        </div>
        <div className="admin-config-summary">
          <strong>Operational routes</strong>
          <span>{impact.operationalRoutes.length}</span>
        </div>
        <div className="admin-config-summary">
          <strong>Training or deep routes</strong>
          <span>{impact.trainingRoutes.length + impact.deepRoutes.length}</span>
        </div>
      </div>

      <div className="admin-config-readables">
        <div>
          <strong>Airlines</strong>
          <div className="panel-copy">{impact.airlines.join(", ") || "Not assigned yet"}</div>
        </div>
        {[
          { label: "Available on routes", values: impact.availableRoutes },
          { label: "Operational mode", values: impact.operationalRoutes },
          { label: "Training mode", values: impact.trainingRoutes },
          { label: "Deep mode", values: impact.deepRoutes },
        ].map(({ label, values }) => (
          <details key={label} className="admin-selection-group">
            <summary className="admin-selection-summary">
              <div>
                <strong>{label}</strong>
                <div className="panel-copy" style={{ marginBottom: 0 }}>
                  {Array.isArray(values) && values.length ? `${values.length} routes` : "None"}
                </div>
              </div>
            </summary>
            <div className="admin-selection-body">
              <div className="panel-copy" style={{ marginBottom: 0 }}>
                {Array.isArray(values) && values.length ? values.join(", ") : "None"}
              </div>
            </div>
          </details>
        ))}
      </div>
    </div>
  );
}
