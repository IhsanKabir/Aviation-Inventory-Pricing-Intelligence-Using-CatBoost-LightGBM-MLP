"use client";

import type { Dispatch, SetStateAction } from "react";
import { useMemo, useState } from "react";

import type { AdminRouteProfileEntry, AdminSearchConfig } from "@/lib/search-config";

import { RouteProfileFieldGroups } from "./route-profile-field-groups";
import type { RouteProfileField } from "./shared";
import { AdminAccordionSection } from "./shared";

interface RouteAssignmentPanelProps {
  config: AdminSearchConfig;
  setConfig: Dispatch<SetStateAction<AdminSearchConfig>>;
  selectedRouteKey: string;
  setSelectedRouteKey: Dispatch<SetStateAction<string>>;
  selectedTripProfileKey: string;
  showArchivedProfiles: boolean;
  setMessage: (message: string | null) => void;
  setError: (error: string | null) => void;
  onRevert: () => void;
  open: boolean;
  onToggle: (open: boolean) => void;
}

export function RouteAssignmentPanel({
  config,
  setConfig,
  selectedRouteKey,
  setSelectedRouteKey,
  selectedTripProfileKey,
  showArchivedProfiles,
  setMessage,
  setError,
  onRevert,
  open,
  onToggle,
}: RouteAssignmentPanelProps) {
  const [routeFilter, setRouteFilter] = useState("");

  const filteredRoutes = useMemo(() => {
    const needle = routeFilter.trim().toLowerCase();
    if (!needle) {
      return config.routeProfiles;
    }
    return config.routeProfiles.filter((item) =>
      `${item.airline} ${item.routeCode}`.toLowerCase().includes(needle),
    );
  }, [config.routeProfiles, routeFilter]);

  const selectedRoute = useMemo(
    () => filteredRoutes.find((item) => item.key === selectedRouteKey) || config.routeProfiles.find((item) => item.key === selectedRouteKey) || null,
    [config.routeProfiles, filteredRoutes, selectedRouteKey],
  );

  const selectedTripProfile = useMemo(
    () => config.tripProfiles.find((item) => item.key === selectedTripProfileKey) || null,
    [config.tripProfiles, selectedTripProfileKey],
  );

  const selectedAirlineRoutes = useMemo(() => {
    if (!selectedRoute) {
      return [];
    }
    return config.routeProfiles.filter((item) => item.airline === selectedRoute.airline);
  }, [config.routeProfiles, selectedRoute]);

  function updateRouteProfile(key: string, updater: (profile: AdminRouteProfileEntry) => AdminRouteProfileEntry) {
    setConfig((current) => ({
      ...current,
      routeProfiles: current.routeProfiles.map((item) => (item.key === key ? updater(item) : item)),
    }));
  }

  function selectAllProfilesForField(routeKey: string, field: RouteProfileField) {
    const allVisibleKeys = config.tripProfiles
      .filter((profile) => showArchivedProfiles || !profile.archived)
      .map((profile) => profile.key);
    updateRouteProfile(routeKey, (current) => {
      const nextProfile = { ...current, [field]: allVisibleKeys };
      if (field === "activeMarketTripProfiles") {
        nextProfile.marketTripProfiles = Array.from(
          new Set([...current.marketTripProfiles, ...allVisibleKeys]),
        );
      }
      if (field === "marketTripProfiles") {
        nextProfile.activeMarketTripProfiles = current.activeMarketTripProfiles.filter((item) =>
          allVisibleKeys.includes(item),
        );
      }
      return nextProfile;
    });
    setMessage(`Selected all visible profiles for ${field}. Save configuration to persist it.`);
    setError(null);
  }

  function clearProfilesForField(routeKey: string, field: RouteProfileField) {
    updateRouteProfile(routeKey, (current) => {
      const nextProfile = { ...current, [field]: [] };
      if (field === "marketTripProfiles") {
        nextProfile.activeMarketTripProfiles = [];
      }
      return nextProfile;
    });
    setMessage(`Cleared ${field} for the selected route. Save configuration to persist it.`);
    setError(null);
  }

  function bulkApplyProfileToAirline(field: RouteProfileField, mode: "add" | "remove") {
    if (!selectedRoute || !selectedTripProfile) {
      return;
    }
    setConfig((current) => ({
      ...current,
      routeProfiles: current.routeProfiles.map((item) => {
        if (item.airline !== selectedRoute.airline) {
          return item;
        }
        const nextValues =
          mode === "add"
            ? Array.from(new Set([...item[field], selectedTripProfile.key]))
            : item[field].filter((value) => value !== selectedTripProfile.key);
        const nextItem = { ...item, [field]: nextValues };
        if (field === "activeMarketTripProfiles" && mode === "add") {
          nextItem.marketTripProfiles = Array.from(new Set([...nextItem.marketTripProfiles, selectedTripProfile.key]));
        }
        if (field === "marketTripProfiles" && mode === "remove") {
          nextItem.activeMarketTripProfiles = nextItem.activeMarketTripProfiles.filter(
            (value) => value !== selectedTripProfile.key,
          );
        }
        return nextItem;
      }),
    }));
    setMessage(
      `${mode === "add" ? "Applied" : "Removed"} ${selectedTripProfile.key} ${mode === "add" ? "to" : "from"} all ${selectedRoute.airline} routes in ${field}.`,
    );
    setError(null);
  }

  function clearSelectedRouteAssignments() {
    if (!selectedRoute) {
      return;
    }
    updateRouteProfile(selectedRoute.key, (current) => ({
      ...current,
      marketTripProfiles: [],
      activeMarketTripProfiles: [],
      trainingMarketTripProfiles: [],
      deepMarketTripProfiles: [],
    }));
    setMessage(`Cleared all profile assignments for ${selectedRoute.airline} ${selectedRoute.routeCode}. Save configuration to persist it.`);
    setError(null);
  }

  function copySelectedRouteAssignmentsToAirline() {
    if (!selectedRoute) {
      return;
    }
    setConfig((current) => ({
      ...current,
      routeProfiles: current.routeProfiles.map((item) =>
        item.airline === selectedRoute.airline
          ? {
              ...item,
              marketTripProfiles: [...selectedRoute.marketTripProfiles],
              activeMarketTripProfiles: [...selectedRoute.activeMarketTripProfiles],
              trainingMarketTripProfiles: [...selectedRoute.trainingMarketTripProfiles],
              deepMarketTripProfiles: [...selectedRoute.deepMarketTripProfiles],
            }
          : item,
      ),
    }));
    setMessage(
      `Copied the ${selectedRoute.routeCode} profile layout to all ${selectedRoute.airline} routes. Save configuration to persist it.`,
    );
    setError(null);
  }

  return (
    <AdminAccordionSection
      id="admin-config-route-assignments"
      onToggle={onToggle}
      open={open}
      title="Route assignments"
      summary="This is where scheduling is controlled per airline and per route by assigning which trip profiles are available, operational, training-only, or deep-only."
    >
      <div className="button-row" style={{ justifyContent: "flex-end" }}>
        <button className="button-link ghost" type="button" onClick={onRevert}>
          Revert this section
        </button>
      </div>

      <div className="field-grid three-up">
        <label className="field">
          <span>Find route</span>
          <input type="text" placeholder="DAC-BKK or BG" value={routeFilter} onChange={(event) => setRouteFilter(event.target.value)} />
        </label>
        <label className="field">
          <span>Selected route</span>
          <select value={selectedRouteKey} onChange={(event) => setSelectedRouteKey(event.target.value)}>
            {filteredRoutes.map((item) => (
              <option key={item.key} value={item.key}>
                {item.airline} | {item.routeCode}
              </option>
            ))}
          </select>
        </label>
      </div>

      {selectedRoute ? (
        <div className="admin-route-editor">
          <div className="status-banner" style={{ marginTop: 0 }}>
            Edit the actual date offsets and date ranges in the trip profile section above, then assign those profiles to this route here. That lets non-technical users shape searches by airline, route, and trip-window behavior without touching JSON.
          </div>

          {selectedTripProfile ? (
            <div className="admin-bulk-apply">
              <div>
                <strong>Bulk apply for {selectedRoute.airline}</strong>
                <div className="panel-copy" style={{ marginBottom: 0 }}>
                  Apply <strong>{selectedTripProfile.key}</strong> to all {selectedAirlineRoutes.length} routes of this airline in one click.
                </div>
              </div>
              <div className="button-row">
                <button
                  className="button-link ghost"
                  type="button"
                  onClick={() => bulkApplyProfileToAirline("marketTripProfiles", "add")}
                >
                  Add to all available
                </button>
                <button
                  className="button-link ghost"
                  type="button"
                  onClick={() => bulkApplyProfileToAirline("activeMarketTripProfiles", "add")}
                >
                  Add to all operational
                </button>
                <button
                  className="button-link ghost"
                  type="button"
                  onClick={() => bulkApplyProfileToAirline("trainingMarketTripProfiles", "add")}
                >
                  Add to all training
                </button>
                <button
                  className="button-link ghost"
                  type="button"
                  onClick={() => bulkApplyProfileToAirline("deepMarketTripProfiles", "add")}
                >
                  Add to all deep
                </button>
                <button
                  className="button-link ghost"
                  type="button"
                  onClick={() => bulkApplyProfileToAirline("marketTripProfiles", "remove")}
                >
                  Remove from all available
                </button>
                <button
                  className="button-link ghost"
                  type="button"
                  onClick={() => bulkApplyProfileToAirline("activeMarketTripProfiles", "remove")}
                >
                  Remove from all operational
                </button>
                <button
                  className="button-link ghost"
                  type="button"
                  onClick={() => bulkApplyProfileToAirline("trainingMarketTripProfiles", "remove")}
                >
                  Remove from all training
                </button>
                <button
                  className="button-link ghost"
                  type="button"
                  onClick={() => bulkApplyProfileToAirline("deepMarketTripProfiles", "remove")}
                >
                  Remove from all deep
                </button>
              </div>
            </div>
          ) : null}

          <div className="admin-bulk-apply">
            <div>
              <strong>Selected route cleanup</strong>
              <div className="panel-copy" style={{ marginBottom: 0 }}>
                Use this when one route needs to be reset, or when you want one route to become the template for the rest of the airline.
              </div>
            </div>
            <div className="button-row">
              <button className="button-link ghost" type="button" onClick={clearSelectedRouteAssignments}>
                Clear this route
              </button>
              <button className="button-link ghost" type="button" onClick={copySelectedRouteAssignmentsToAirline}>
                Copy this route to all {selectedRoute.airline} routes
              </button>
            </div>
          </div>

          <RouteProfileFieldGroups
            onClearField={(field) => clearProfilesForField(selectedRoute.key, field)}
            onSelectAllField={(field) => selectAllProfilesForField(selectedRoute.key, field)}
            onUpdateRouteProfile={updateRouteProfile}
            selectedRoute={selectedRoute}
            showArchivedProfiles={showArchivedProfiles}
            tripProfiles={config.tripProfiles}
          />
        </div>
      ) : null}
    </AdminAccordionSection>
  );
}
