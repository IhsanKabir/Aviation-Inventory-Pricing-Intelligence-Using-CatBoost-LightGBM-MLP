"use client";

import { useEffect, useMemo, useState, useTransition } from "react";

import type { AdminSearchConfig } from "@/lib/search-config";

import { AdvancedSettings } from "./admin-config/advanced-settings";
import { AdminBasicsSection, AdminOutputSection } from "./admin-config/basics-output-sections";
import { HolidayCalendar } from "./admin-config/holiday-calendar";
import { RouteAssignmentPanel } from "./admin-config/route-assignment-panel";
import { AdminScheduleSection } from "./admin-config/schedule-section";
import {
  ADMIN_CONFIG_SECTIONS,
  cloneValue,
  validateDateRangesText,
  validateOffsetRangesText,
} from "./admin-config/shared";
import { TripProfileEditor } from "./admin-config/trip-profile-editor";

export function AdminSearchConfigPanel({
  initialConfig,
  onConfigChange,
  onDirtyChange,
}: {
  initialConfig: AdminSearchConfig;
  onConfigChange?: (config: AdminSearchConfig) => void;
  onDirtyChange?: (hasChanges: boolean) => void;
}) {
  const [config, setConfig] = useState(initialConfig);
  const [savedConfig, setSavedConfig] = useState(initialConfig);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [applyMessage, setApplyMessage] = useState<string | null>(null);
  const [selectedRouteKey, setSelectedRouteKey] = useState(initialConfig.routeProfiles[0]?.key || "");
  const [selectedTripProfileKey, setSelectedTripProfileKey] = useState(initialConfig.tripProfiles[0]?.key || "");
  const [showArchivedProfiles, setShowArchivedProfiles] = useState(false);
  const [activeConfigSection, setActiveConfigSection] = useState("admin-config-schedule");
  const [openConfigSections, setOpenConfigSections] = useState<Record<string, boolean>>({
    "admin-config-basics": true,
    "admin-config-schedule": true,
    "admin-config-output": false,
    "admin-config-holidays": false,
    "admin-config-trip-profiles": false,
    "admin-config-route-assignments": false,
    "admin-config-advanced": false,
  });
  const [isPending, startTransition] = useTransition();

  const unsavedChangeSummary = useMemo(() => {
    const sections = [
      { label: "Run schedule", changed: JSON.stringify(config.schedule) !== JSON.stringify(savedConfig.schedule) },
      { label: "Passenger defaults", changed: JSON.stringify(config.passengers) !== JSON.stringify(savedConfig.passengers) },
      { label: "Output options", changed: JSON.stringify(config.output) !== JSON.stringify(savedConfig.output) },
      { label: "Holiday calendar", changed: JSON.stringify(config.holidays) !== JSON.stringify(savedConfig.holidays) },
      { label: "Trip profiles", changed: JSON.stringify(config.tripProfiles) !== JSON.stringify(savedConfig.tripProfiles) },
      { label: "Route assignments", changed: JSON.stringify(config.routeProfiles) !== JSON.stringify(savedConfig.routeProfiles) },
      { label: "Advanced market priors JSON", changed: JSON.stringify(config.advanced) !== JSON.stringify(savedConfig.advanced) },
    ];
    const changedSections = sections.filter((item) => item.changed).map((item) => item.label);
    return {
      hasChanges: changedSections.length > 0,
      changedSections,
    };
  }, [config, savedConfig]);

  const activeRouteCount = useMemo(
    () =>
      config.routeProfiles.filter(
        (route) =>
          route.marketTripProfiles.length ||
          route.activeMarketTripProfiles.length ||
          route.trainingMarketTripProfiles.length ||
          route.deepMarketTripProfiles.length,
      ).length,
    [config.routeProfiles],
  );

  const archivedProfileCount = useMemo(
    () => config.tripProfiles.filter((profile) => profile.archived).length,
    [config.tripProfiles],
  );

  useEffect(() => {
    onConfigChange?.(config);
  }, [config, onConfigChange]);

  useEffect(() => {
    onDirtyChange?.(unsavedChangeSummary.hasChanges);
  }, [onDirtyChange, unsavedChangeSummary.hasChanges]);

  async function refreshConfig() {
    const response = await fetch("/api/admin/search-config", { cache: "no-store" });
    const payload = (await response.json().catch(() => null)) as AdminSearchConfig & { detail?: string } | null;
    if (!response.ok || !payload) {
      throw new Error(payload?.detail || "Unable to refresh search configuration.");
    }
    setConfig(payload);
    setSavedConfig(payload);
    if (!payload.routeProfiles.some((item) => item.key === selectedRouteKey)) {
      setSelectedRouteKey(payload.routeProfiles[0]?.key || "");
    }
    if (!payload.tripProfiles.some((item) => item.key === selectedTripProfileKey)) {
      setSelectedTripProfileKey((payload.tripProfiles.find((item) => !item.archived) || payload.tripProfiles[0])?.key || "");
    }
  }

  async function saveConfig() {
    setError(null);
    setMessage(null);
    setApplyMessage(null);
    for (const profile of config.tripProfiles) {
      validateDateRangesText(profile.dateRangesText, `${profile.key} outbound date ranges`);
      validateOffsetRangesText(profile.dayOffsetRangesText, `${profile.key} outbound offset ranges`);
      validateDateRangesText(profile.returnDateRangesText, `${profile.key} return date ranges`);
      validateOffsetRangesText(profile.returnDateOffsetRangesText, `${profile.key} return offset ranges`);
    }
    const response = await fetch("/api/admin/search-config", {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(config),
    });
    const payload = (await response.json().catch(() => null)) as AdminSearchConfig & { detail?: string } | null;
    if (!response.ok || !payload) {
      throw new Error(payload?.detail || "Unable to save search configuration.");
    }
    setConfig(payload);
    setSavedConfig(payload);
    setMessage("Search configuration saved. The next local run will use these file settings.");
  }

  async function saveAndApplyScheduler() {
    await saveConfig();
    const response = await fetch("/api/admin/search-config/apply-scheduler", {
      method: "POST",
    });
    const payload = (await response.json().catch(() => null)) as { detail?: string; steps?: Array<{ name: string; ok: boolean }> } | null;
    if (!response.ok) {
      throw new Error(payload?.detail || "Unable to apply scheduler settings on this machine.");
    }
    const appliedSteps = Array.isArray(payload?.steps) ? payload.steps.filter((step) => step.ok).length : 0;
    setApplyMessage(`Scheduler settings applied on this machine. Updated tasks: ${appliedSteps}.`);
  }

  function revertSection(
    section:
      | "schedule"
      | "passengers"
      | "output"
      | "holidays"
      | "tripProfiles"
      | "routeProfiles"
      | "advanced",
  ) {
    setConfig((current) => {
      const next = {
        ...current,
        [section]: cloneValue(savedConfig[section]),
      } as AdminSearchConfig;

      if (
        section === "tripProfiles" &&
        !next.tripProfiles.some((item) => item.key === selectedTripProfileKey)
      ) {
        setSelectedTripProfileKey((next.tripProfiles.find((item) => !item.archived) || next.tripProfiles[0])?.key || "");
      }

      if (
        section === "routeProfiles" &&
        !next.routeProfiles.some((item) => item.key === selectedRouteKey)
      ) {
        setSelectedRouteKey(next.routeProfiles[0]?.key || "");
      }

      return next;
    });
    setMessage(`Reverted ${section} to the last saved or loaded state.`);
    setError(null);
  }

  function updateConfigSectionOpen(sectionId: string, nextOpen: boolean) {
    setOpenConfigSections((current) => ({
      ...current,
      [sectionId]: nextOpen,
    }));
    if (nextOpen) {
      setActiveConfigSection(sectionId);
    }
  }

  function jumpToConfigSection(sectionId: string) {
    setOpenConfigSections((current) => ({
      ...current,
      [sectionId]: true,
    }));
    setActiveConfigSection(sectionId);
    window.setTimeout(() => {
      document.getElementById(sectionId)?.scrollIntoView({
        behavior: "smooth",
        block: "start",
      });
    }, 0);
  }

  return (
    <div className="admin-config-workbench">
      <div className="status-banner admin-config-note">
        {config.persistenceNote}
      </div>

      {unsavedChangeSummary.hasChanges ? (
        <div className="status-banner warn">
          Unsaved changes are waiting. Updated sections: {unsavedChangeSummary.changedSections.join(", ")}.
        </div>
      ) : (
        <div className="status-banner good">
          No unsaved changes. The editor matches the last loaded or saved configuration.
        </div>
      )}

      <div className="admin-config-snapshot-grid">
        <div className="admin-config-summary">
          <strong>Run mode</strong>
          <span>{config.schedule.mode}</span>
        </div>
        <div className="admin-config-summary">
          <strong>Passenger mix</strong>
          <span>
            {config.passengers.adt} ADT / {config.passengers.chd} CHD / {config.passengers.inf} INF
          </span>
        </div>
        <div className="admin-config-summary">
          <strong>Active routes</strong>
          <span>{activeRouteCount}</span>
        </div>
        <div className="admin-config-summary">
          <strong>Archived profiles</strong>
          <span>{archivedProfileCount}</span>
        </div>
      </div>

      <div className="admin-config-layout">
        <aside className="admin-config-nav" aria-label="Search configuration sections">
          <div className="admin-section-kicker">Jump to section</div>
          <div className="admin-config-nav-list">
            {ADMIN_CONFIG_SECTIONS.map((section) => (
              <button
                className="admin-config-nav-item"
                data-active={activeConfigSection === section.id}
                key={section.id}
                onClick={() => jumpToConfigSection(section.id)}
                type="button"
              >
                {section.label}
              </button>
            ))}
          </div>
        </aside>

        <div className="admin-config-section-stack">
          <AdminScheduleSection
            config={config}
            onRevert={() => revertSection("schedule")}
            onToggle={(open) => updateConfigSectionOpen("admin-config-schedule", open)}
            open={openConfigSections["admin-config-schedule"]}
            setConfig={setConfig}
          />

          <AdminBasicsSection
            config={config}
            onRevert={() => revertSection("passengers")}
            onToggle={(open) => updateConfigSectionOpen("admin-config-basics", open)}
            open={openConfigSections["admin-config-basics"]}
            setConfig={setConfig}
          />

          <AdminOutputSection
            config={config}
            onRevert={() => revertSection("output")}
            onToggle={(open) => updateConfigSectionOpen("admin-config-output", open)}
            open={openConfigSections["admin-config-output"]}
            setConfig={setConfig}
          />

          <HolidayCalendar
            config={config}
            onRevert={() => revertSection("holidays")}
            onToggle={(open) => updateConfigSectionOpen("admin-config-holidays", open)}
            open={openConfigSections["admin-config-holidays"]}
            setConfig={setConfig}
          />

          <TripProfileEditor
            config={config}
            onRevert={() => revertSection("tripProfiles")}
            onToggle={(open) => updateConfigSectionOpen("admin-config-trip-profiles", open)}
            open={openConfigSections["admin-config-trip-profiles"]}
            selectedTripProfileKey={selectedTripProfileKey}
            setConfig={setConfig}
            setError={setError}
            setMessage={setMessage}
            setSelectedTripProfileKey={setSelectedTripProfileKey}
            setShowArchivedProfiles={setShowArchivedProfiles}
            showArchivedProfiles={showArchivedProfiles}
          />

          <RouteAssignmentPanel
            config={config}
            onRevert={() => revertSection("routeProfiles")}
            onToggle={(open) => updateConfigSectionOpen("admin-config-route-assignments", open)}
            open={openConfigSections["admin-config-route-assignments"]}
            selectedRouteKey={selectedRouteKey}
            selectedTripProfileKey={selectedTripProfileKey}
            setConfig={setConfig}
            setError={setError}
            setMessage={setMessage}
            setSelectedRouteKey={setSelectedRouteKey}
            showArchivedProfiles={showArchivedProfiles}
          />

          <AdvancedSettings
            config={config}
            onRevert={() => revertSection("advanced")}
            onToggle={(open) => updateConfigSectionOpen("admin-config-advanced", open)}
            open={openConfigSections["admin-config-advanced"]}
            setConfig={setConfig}
          />
        </div>
      </div>

      <div className="admin-config-action-bar">
        <div className="admin-config-action-feedback">
          {message ? <div className="status-banner good">{message}</div> : null}
          {applyMessage ? <div className="status-banner good">{applyMessage}</div> : null}
          {error ? <div className="status-banner warn">{error}</div> : null}
        </div>

        <div className="button-row admin-config-actions">
          <button
            className="button-link ghost"
            type="button"
            onClick={() =>
              startTransition(() => {
                refreshConfig().catch((refreshError) => {
                  setError(refreshError instanceof Error ? refreshError.message : "Unable to refresh search configuration.");
                });
              })
            }
          >
            Refresh
          </button>
          <button
            className="button-link"
            data-pending={isPending}
            type="button"
            onClick={() =>
              startTransition(() => {
                saveConfig().catch((saveError) => {
                  setError(saveError instanceof Error ? saveError.message : "Unable to save search configuration.");
                });
              })
            }
          >
            Save configuration
          </button>
          <button
            className="button-link"
            data-pending={isPending}
            type="button"
            onClick={() =>
              startTransition(() => {
                saveAndApplyScheduler().catch((saveError) => {
                  setError(saveError instanceof Error ? saveError.message : "Unable to apply scheduler settings.");
                });
              })
            }
          >
            Apply scheduler settings on this machine
          </button>
        </div>
      </div>
    </div>
  );
}
