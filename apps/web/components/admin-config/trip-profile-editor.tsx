"use client";

import type { Dispatch, SetStateAction } from "react";
import { useMemo } from "react";

import type { AdminSearchConfig, AdminTripProfileEntry } from "@/lib/search-config";

import { AdminAccordionSection, normalizeProfileKey } from "./shared";
import { TripProfileImpactPreview } from "./trip-profile-impact";

interface TripProfileEditorProps {
  config: AdminSearchConfig;
  setConfig: Dispatch<SetStateAction<AdminSearchConfig>>;
  selectedTripProfileKey: string;
  setSelectedTripProfileKey: Dispatch<SetStateAction<string>>;
  showArchivedProfiles: boolean;
  setShowArchivedProfiles: Dispatch<SetStateAction<boolean>>;
  setMessage: (message: string | null) => void;
  setError: (error: string | null) => void;
  onRevert: () => void;
  open: boolean;
  onToggle: (open: boolean) => void;
}

export function TripProfileEditor({
  config,
  setConfig,
  selectedTripProfileKey,
  setSelectedTripProfileKey,
  showArchivedProfiles,
  setShowArchivedProfiles,
  setMessage,
  setError,
  onRevert,
  open,
  onToggle,
}: TripProfileEditorProps) {
  const selectedTripProfile = useMemo(
    () => config.tripProfiles.find((item) => item.key === selectedTripProfileKey) || null,
    [config.tripProfiles, selectedTripProfileKey],
  );

  const visibleTripProfiles = useMemo(
    () => config.tripProfiles.filter((item) => showArchivedProfiles || !item.archived || item.key === selectedTripProfileKey),
    [config.tripProfiles, selectedTripProfileKey, showArchivedProfiles],
  );

  function updateTripProfile(key: string, updater: (profile: AdminTripProfileEntry) => AdminTripProfileEntry) {
    setConfig((current) => ({
      ...current,
      tripProfiles: current.tripProfiles.map((item) => (item.key === key ? updater(item) : item)),
    }));
  }

  function createTripProfile(fromProfile?: AdminTripProfileEntry | null) {
    const typed = typeof window === "undefined" ? "" : window.prompt(
      "Enter a new profile key. Use short readable words like tourism_dxb_rt.",
      fromProfile ? `${fromProfile.key}_copy` : "",
    );
    const key = normalizeProfileKey(String(typed || ""));
    if (!key) {
      return;
    }
    if (config.tripProfiles.some((item) => item.key === key)) {
      setError(`Trip profile "${key}" already exists.`);
      return;
    }
    const nextProfile: AdminTripProfileEntry = fromProfile
      ? { ...fromProfile, key, archived: false }
      : {
          key,
          description: "",
          tripType: "OW",
          archived: false,
          dayOffsets: "",
          dateRangesText: "",
          dayOffsetRangesText: "",
          returnDateOffsets: "",
          returnDateRangesText: "",
          returnDateOffsetRangesText: "",
        };
    setConfig((current) => ({
      ...current,
      tripProfiles: [...current.tripProfiles, nextProfile].sort((left, right) => left.key.localeCompare(right.key)),
    }));
    setSelectedTripProfileKey(key);
    setMessage(`Trip profile "${key}" is ready to edit. Save configuration to persist it.`);
    setError(null);
  }

  function toggleArchiveSelectedProfile(nextArchived: boolean) {
    if (!selectedTripProfile) {
      return;
    }
    updateTripProfile(selectedTripProfile.key, (current) => ({
      ...current,
      archived: nextArchived,
    }));
    setShowArchivedProfiles((current) => current || nextArchived);
    setMessage(`${nextArchived ? "Archived" : "Restored"} ${selectedTripProfile.key}. Save configuration to persist it.`);
    setError(null);
  }

  function deleteSelectedProfile() {
    if (!selectedTripProfile) {
      return;
    }
    if (typeof window !== "undefined") {
      const confirmed = window.confirm(
        `Delete ${selectedTripProfile.key}? This will also remove it from every route assignment.`,
      );
      if (!confirmed) {
        return;
      }
    }
    const deletingKey = selectedTripProfile.key;
    setConfig((current) => {
      const remainingProfiles = current.tripProfiles.filter((item) => item.key !== deletingKey);
      const nextRouteProfiles = current.routeProfiles.map((route) => ({
        ...route,
        marketTripProfiles: route.marketTripProfiles.filter((value) => value !== deletingKey),
        activeMarketTripProfiles: route.activeMarketTripProfiles.filter((value) => value !== deletingKey),
        trainingMarketTripProfiles: route.trainingMarketTripProfiles.filter((value) => value !== deletingKey),
        deepMarketTripProfiles: route.deepMarketTripProfiles.filter((value) => value !== deletingKey),
      }));
      return {
        ...current,
        tripProfiles: remainingProfiles,
        routeProfiles: nextRouteProfiles,
      };
    });
    setSelectedTripProfileKey((current) => {
      if (current !== deletingKey) {
        return current;
      }
      const remainingProfiles = config.tripProfiles.filter((item) => item.key !== deletingKey);
      return (remainingProfiles.find((item) => !item.archived) || remainingProfiles[0])?.key || "";
    });
    setMessage(`Deleted ${deletingKey} and removed it from route assignments. Save configuration to persist it.`);
    setError(null);
  }

  return (
    <AdminAccordionSection
      id="admin-config-trip-profiles"
      onToggle={onToggle}
      open={open}
      title="Trip profiles"
      summary="Define one-way and round-trip search windows here using date ranges, day offsets, and return windows."
    >
      <div className="button-row" style={{ justifyContent: "flex-end" }}>
        <button className="button-link ghost" type="button" onClick={onRevert}>
          Revert this section
        </button>
      </div>

      <div className="field-grid three-up">
        <label className="field">
          <span>Trip profile</span>
          <select value={selectedTripProfileKey} onChange={(event) => setSelectedTripProfileKey(event.target.value)}>
            {visibleTripProfiles.map((item) => (
              <option key={item.key} value={item.key}>
                {item.key}{item.archived ? " (archived)" : ""}
              </option>
            ))}
          </select>
        </label>
        <label className="field checkbox-field">
          <input
            type="checkbox"
            checked={showArchivedProfiles}
            onChange={(event) => setShowArchivedProfiles(event.target.checked)}
          />
          <span>Show archived profiles</span>
        </label>
      </div>

      <div className="button-row">
        <button className="button-link ghost" type="button" onClick={() => createTripProfile(null)}>
          New blank profile
        </button>
        <button
          className="button-link ghost"
          type="button"
          onClick={() => createTripProfile(selectedTripProfile)}
          disabled={!selectedTripProfile}
        >
          Copy selected profile
        </button>
        <button
          className="button-link ghost"
          type="button"
          onClick={() => toggleArchiveSelectedProfile(true)}
          disabled={!selectedTripProfile || selectedTripProfile.archived}
        >
          Archive selected
        </button>
        <button
          className="button-link ghost"
          type="button"
          onClick={() => toggleArchiveSelectedProfile(false)}
          disabled={!selectedTripProfile || !selectedTripProfile.archived}
        >
          Restore profile
        </button>
        <button
          className="button-link ghost"
          type="button"
          onClick={deleteSelectedProfile}
          disabled={!selectedTripProfile}
        >
          Delete selected
        </button>
      </div>

      <TripProfileImpactPreview profile={selectedTripProfile} routeProfiles={config.routeProfiles} />

      {selectedTripProfile ? (
        <div className="admin-route-editor">
          <div className="field-grid three-up">
            <label className="field">
              <span>Description</span>
              <input
                type="text"
                value={selectedTripProfile.description}
                onChange={(event) =>
                  updateTripProfile(selectedTripProfile.key, (current) => ({
                    ...current,
                    description: event.target.value,
                  }))
                }
              />
            </label>
            <label className="field">
              <span>Trip type</span>
              <select
                value={selectedTripProfile.tripType}
                onChange={(event) =>
                  updateTripProfile(selectedTripProfile.key, (current) => ({
                    ...current,
                    tripType: event.target.value,
                  }))
                }
              >
                <option value="OW">One-way</option>
                <option value="RT">Round-trip</option>
              </select>
            </label>
            <label className="field checkbox-field">
              <input
                type="checkbox"
                checked={selectedTripProfile.archived}
                onChange={(event) =>
                  updateTripProfile(selectedTripProfile.key, (current) => ({
                    ...current,
                    archived: event.target.checked,
                  }))
                }
              />
              <span>Archived profile</span>
            </label>
            <label className="field">
              <span>Outbound offsets</span>
              <input
                type="text"
                placeholder="0, 3, 5, 15, 30"
                value={selectedTripProfile.dayOffsets}
                onChange={(event) =>
                  updateTripProfile(selectedTripProfile.key, (current) => ({
                    ...current,
                    dayOffsets: event.target.value,
                  }))
                }
              />
            </label>
          </div>

          <div className="field-grid">
            <label className="field">
              <span>Outbound date ranges</span>
              <textarea
                rows={3}
                placeholder={"2026-04-07 to 2026-04-14\n2026-05-01 to 2026-05-10"}
                value={selectedTripProfile.dateRangesText}
                onChange={(event) =>
                  updateTripProfile(selectedTripProfile.key, (current) => ({
                    ...current,
                    dateRangesText: event.target.value,
                  }))
                }
              />
            </label>
            <label className="field">
              <span>Outbound offset ranges</span>
              <textarea
                rows={3}
                placeholder="0-7, 14-21"
                value={selectedTripProfile.dayOffsetRangesText}
                onChange={(event) =>
                  updateTripProfile(selectedTripProfile.key, (current) => ({
                    ...current,
                    dayOffsetRangesText: event.target.value,
                  }))
                }
              />
            </label>
          </div>

          <div className="field-grid">
            <label className="field">
              <span>Return offsets</span>
              <input
                type="text"
                placeholder="3, 5, 7, 10, 14"
                value={selectedTripProfile.returnDateOffsets}
                onChange={(event) =>
                  updateTripProfile(selectedTripProfile.key, (current) => ({
                    ...current,
                    returnDateOffsets: event.target.value,
                  }))
                }
              />
            </label>
            <label className="field">
              <span>Return date ranges</span>
              <textarea
                rows={3}
                placeholder={"2026-04-20 to 2026-04-30\n2026-05-10 to 2026-05-25"}
                value={selectedTripProfile.returnDateRangesText}
                onChange={(event) =>
                  updateTripProfile(selectedTripProfile.key, (current) => ({
                    ...current,
                    returnDateRangesText: event.target.value,
                  }))
                }
              />
            </label>
            <label className="field">
              <span>Return offset ranges</span>
              <textarea
                rows={3}
                placeholder="2-5, 7-14"
                value={selectedTripProfile.returnDateOffsetRangesText}
                onChange={(event) =>
                  updateTripProfile(selectedTripProfile.key, (current) => ({
                    ...current,
                    returnDateOffsetRangesText: event.target.value,
                  }))
                }
              />
            </label>
          </div>

          <div className="status-banner" style={{ marginTop: 0 }}>
            Use exact date ranges when a market has fixed holiday or event travel dates. Use offset ranges when you want rolling windows like “next 7 days” or “return 3 to 10 days later.”
          </div>
        </div>
      ) : null}
    </AdminAccordionSection>
  );
}
