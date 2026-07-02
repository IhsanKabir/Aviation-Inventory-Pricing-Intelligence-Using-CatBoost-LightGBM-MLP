"use client";

import type { AdminRouteProfileEntry, AdminTripProfileEntry } from "@/lib/search-config";

import type { RouteProfileField } from "./shared";
import { summarizeSelection, toggleValue } from "./shared";

interface RouteProfileFieldGroupsProps {
  tripProfiles: AdminTripProfileEntry[];
  selectedRoute: AdminRouteProfileEntry;
  showArchivedProfiles: boolean;
  onSelectAllField: (field: RouteProfileField) => void;
  onClearField: (field: RouteProfileField) => void;
  onUpdateRouteProfile: (
    key: string,
    updater: (profile: AdminRouteProfileEntry) => AdminRouteProfileEntry,
  ) => void;
}

export function RouteProfileFieldGroups({
  tripProfiles,
  selectedRoute,
  showArchivedProfiles,
  onSelectAllField,
  onClearField,
  onUpdateRouteProfile,
}: RouteProfileFieldGroupsProps) {
  return (
    <div className="admin-config-readables">
      {[
        ["Profiles available on this route", "marketTripProfiles"] as const,
        ["Run in operational mode", "activeMarketTripProfiles"] as const,
        ["Extra for training mode", "trainingMarketTripProfiles"] as const,
        ["Extra for deep mode", "deepMarketTripProfiles"] as const,
      ].map(([label, field]) => (
        <details key={field} className="admin-selection-group" open={field === "activeMarketTripProfiles"}>
          <summary className="admin-selection-summary">
            <div>
              <strong>{label}</strong>
              <div className="panel-copy" style={{ marginBottom: 0 }}>
                {summarizeSelection(selectedRoute[field])}
              </div>
            </div>
          </summary>
          <div className="admin-selection-body">
            <div className="button-row">
              <button
                className="button-link ghost"
                type="button"
                onClick={() => onSelectAllField(field)}
              >
                Select all
              </button>
              <button
                className="button-link ghost"
                type="button"
                onClick={() => onClearField(field)}
              >
                Clear all
              </button>
            </div>
            <div className="admin-profile-chip-grid">
              {tripProfiles.map((profile) => {
                const shouldShowProfile =
                  showArchivedProfiles ||
                  !profile.archived ||
                  selectedRoute[field].includes(profile.key);
                if (!shouldShowProfile) {
                  return null;
                }
                const active = selectedRoute[field].includes(profile.key);
                return (
                  <button
                    key={`${field}-${profile.key}`}
                    className="chip"
                    data-active={active}
                    type="button"
                    onClick={() =>
                      onUpdateRouteProfile(selectedRoute.key, (current) => {
                        const nextValues = toggleValue(current[field], profile.key);
                        const nextProfile = { ...current, [field]: nextValues };
                        if (field === "activeMarketTripProfiles" && nextValues.includes(profile.key)) {
                          nextProfile.marketTripProfiles = Array.from(
                            new Set([...nextProfile.marketTripProfiles, profile.key]),
                          );
                        }
                        if (field === "marketTripProfiles" && !nextValues.includes(profile.key)) {
                          nextProfile.activeMarketTripProfiles = nextProfile.activeMarketTripProfiles.filter((item) => item !== profile.key);
                        }
                        return nextProfile;
                      })
                    }
                  >
                    {profile.key}
                  </button>
                );
              })}
            </div>
          </div>
        </details>
      ))}
    </div>
  );
}
