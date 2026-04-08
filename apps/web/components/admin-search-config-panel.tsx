"use client";

import type { ReactNode } from "react";
import { useMemo, useState, useTransition } from "react";

import type {
  AdminHolidayEntry,
  AdminRouteProfileEntry,
  AdminSearchConfig,
  AdminTripProfileEntry,
} from "@/lib/search-config";

function emptyHoliday(): AdminHolidayEntry {
  return {
    date: "",
    name: "",
    type: "national",
    country: "BD",
    highDemand: false,
  };
}

function summarizeList(values: string[]) {
  if (!values.length) {
    return "None set";
  }
  return values.join(", ");
}

function toggleValue(values: string[], item: string) {
  return values.includes(item) ? values.filter((value) => value !== item) : [...values, item];
}

function cloneValue<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function summarizeSelection(values: string[]) {
  if (!values.length) {
    return "None selected";
  }
  if (values.length <= 3) {
    return values.join(", ");
  }
  return `${values.length} selected`;
}

function normalizeProfileKey(value: string) {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function validateDateRangesText(value: string, label: string) {
  const entries = String(value || "")
    .split(/\r?\n|;/)
    .map((item) => item.trim())
    .filter(Boolean);
  for (const entry of entries) {
    const parts = entry.replace(/\s+/g, " ").split(/\s+to\s+/i);
    if (
      parts.length !== 2 ||
      !/^\d{4}-\d{2}-\d{2}$/.test(parts[0] || "") ||
      !/^\d{4}-\d{2}-\d{2}$/.test(parts[1] || "")
    ) {
      throw new Error(`${label} has an invalid range: "${entry}". Use YYYY-MM-DD to YYYY-MM-DD.`);
    }
  }
}

function validateOffsetRangesText(value: string, label: string) {
  const entries = String(value || "")
    .split(/[\r\n,;]+/)
    .map((item) => item.trim())
    .filter(Boolean);
  for (const entry of entries) {
    if (!/^-?\d+\s*-\s*-?\d+$/.test(entry)) {
      throw new Error(`${label} has an invalid range: "${entry}". Use forms like 0-7 or 14-21.`);
    }
  }
}

function AdminAccordionSection({
  title,
  summary,
  defaultOpen = false,
  children,
}: {
  title: string;
  summary: string;
  defaultOpen?: boolean;
  children: ReactNode;
}) {
  return (
    <details className="card panel admin-config-card admin-accordion" open={defaultOpen}>
      <summary className="admin-accordion-summary">
        <div>
          <h3 style={{ marginBottom: "0.35rem" }}>{title}</h3>
          <div className="panel-copy" style={{ marginBottom: 0 }}>
            {summary}
          </div>
        </div>
      </summary>
      <div className="admin-accordion-body">{children}</div>
    </details>
  );
}

export function AdminSearchConfigPanel({
  initialConfig,
}: {
  initialConfig: AdminSearchConfig;
}) {
  const [config, setConfig] = useState(initialConfig);
  const [savedConfig, setSavedConfig] = useState(initialConfig);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [applyMessage, setApplyMessage] = useState<string | null>(null);
  const [selectedRouteKey, setSelectedRouteKey] = useState(initialConfig.routeProfiles[0]?.key || "");
  const [selectedTripProfileKey, setSelectedTripProfileKey] = useState(initialConfig.tripProfiles[0]?.key || "");
  const [routeFilter, setRouteFilter] = useState("");
  const [showArchivedProfiles, setShowArchivedProfiles] = useState(false);
  const [isPending, startTransition] = useTransition();

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

  const visibleTripProfiles = useMemo(
    () => config.tripProfiles.filter((item) => showArchivedProfiles || !item.archived || item.key === selectedTripProfileKey),
    [config.tripProfiles, selectedTripProfileKey, showArchivedProfiles],
  );

  const selectedAirlineRoutes = useMemo(() => {
    if (!selectedRoute) {
      return [];
    }
    return config.routeProfiles.filter((item) => item.airline === selectedRoute.airline);
  }, [config.routeProfiles, selectedRoute]);

  const selectedProfileImpact = useMemo(() => {
    if (!selectedTripProfile) {
      return null;
    }
    const airlineSet = new Set<string>();
    const availableRoutes: string[] = [];
    const operationalRoutes: string[] = [];
    const trainingRoutes: string[] = [];
    const deepRoutes: string[] = [];

    for (const route of config.routeProfiles) {
      const label = `${route.airline} ${route.routeCode}`;
      let used = false;
      if (route.marketTripProfiles.includes(selectedTripProfile.key)) {
        availableRoutes.push(label);
        used = true;
      }
      if (route.activeMarketTripProfiles.includes(selectedTripProfile.key)) {
        operationalRoutes.push(label);
        used = true;
      }
      if (route.trainingMarketTripProfiles.includes(selectedTripProfile.key)) {
        trainingRoutes.push(label);
        used = true;
      }
      if (route.deepMarketTripProfiles.includes(selectedTripProfile.key)) {
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
  }, [config.routeProfiles, selectedTripProfile]);

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

  function updateHoliday(index: number, nextValue: Partial<AdminHolidayEntry>) {
    setConfig((current) => ({
      ...current,
      holidays: current.holidays.map((item, itemIndex) =>
        itemIndex === index ? { ...item, ...nextValue } : item,
      ),
    }));
  }

  function removeHoliday(index: number) {
    setConfig((current) => ({
      ...current,
      holidays: current.holidays.filter((_, itemIndex) => itemIndex !== index),
    }));
  }

  function updateTripProfile(key: string, updater: (profile: AdminTripProfileEntry) => AdminTripProfileEntry) {
    setConfig((current) => ({
      ...current,
      tripProfiles: current.tripProfiles.map((item) => (item.key === key ? updater(item) : item)),
    }));
  }

  function updateRouteProfile(key: string, updater: (profile: AdminRouteProfileEntry) => AdminRouteProfileEntry) {
    setConfig((current) => ({
      ...current,
      routeProfiles: current.routeProfiles.map((item) => (item.key === key ? updater(item) : item)),
    }));
  }

  function selectAllProfilesForField(
    routeKey: string,
    field:
      | "marketTripProfiles"
      | "activeMarketTripProfiles"
      | "trainingMarketTripProfiles"
      | "deepMarketTripProfiles",
  ) {
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

  function clearProfilesForField(
    routeKey: string,
    field:
      | "marketTripProfiles"
      | "activeMarketTripProfiles"
      | "trainingMarketTripProfiles"
      | "deepMarketTripProfiles",
  ) {
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

  function bulkApplyProfileToAirline(
    field:
      | "marketTripProfiles"
      | "activeMarketTripProfiles"
      | "trainingMarketTripProfiles"
      | "deepMarketTripProfiles",
    mode: "add" | "remove",
  ) {
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
    <div className="stack">
      <div className="status-banner">
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

      <div className="stack">
        <AdminAccordionSection
          title="Run schedule"
          summary="Set the global run cadence and task start times used when local scheduler tasks are installed or reapplied."
        >
          <div className="button-row" style={{ justifyContent: "flex-end" }}>
            <button className="button-link ghost" type="button" onClick={() => revertSection("schedule")}>
              Revert this section
            </button>
          </div>

          <div className="field-grid three-up">
            <label className="field">
              <span>Run mode</span>
              <select
                value={config.schedule.mode}
                onChange={(event) =>
                  setConfig((current) => ({
                    ...current,
                    schedule: { ...current.schedule, mode: event.target.value },
                  }))
                }
              >
                <option value="manual">Manual</option>
                <option value="auto">Auto</option>
              </select>
            </label>

            <label className="field">
              <span>Parallel airline lanes</span>
              <input
                type="number"
                min={1}
                value={config.schedule.concurrency}
                onChange={(event) =>
                  setConfig((current) => ({
                    ...current,
                    schedule: { ...current.schedule, concurrency: Number(event.target.value || 1) },
                  }))
                }
              />
            </label>

            <label className="field">
              <span>Run every X hours</span>
              <input
                type="number"
                min={1}
                value={config.schedule.autoRunIntervalHours}
                onChange={(event) =>
                  setConfig((current) => ({
                    ...current,
                    schedule: { ...current.schedule, autoRunIntervalHours: Number(event.target.value || 1) },
                  }))
                }
              />
            </label>
          </div>

          <div className="field-grid three-up">
            <label className="field">
              <span>Ingestion start time</span>
              <input
                type="time"
                value={config.schedule.ingestionStartTime}
                onChange={(event) =>
                  setConfig((current) => ({
                    ...current,
                    schedule: { ...current.schedule, ingestionStartTime: event.target.value },
                  }))
                }
              />
            </label>

            <label className="field">
              <span>Training enrichment start</span>
              <input
                type="time"
                value={config.schedule.trainingEnrichmentStartTime}
                onChange={(event) =>
                  setConfig((current) => ({
                    ...current,
                    schedule: { ...current.schedule, trainingEnrichmentStartTime: event.target.value },
                  }))
                }
              />
            </label>

            <label className="field">
              <span>Training deep start</span>
              <input
                type="time"
                value={config.schedule.trainingDeepStartTime}
                onChange={(event) =>
                  setConfig((current) => ({
                    ...current,
                    schedule: { ...current.schedule, trainingDeepStartTime: event.target.value },
                  }))
                }
              />
            </label>
          </div>

          <div className="field-grid three-up">
            <label className="field">
              <span>Training deep weekday</span>
              <select
                value={config.schedule.trainingDeepDayOfWeek}
                onChange={(event) =>
                  setConfig((current) => ({
                    ...current,
                    schedule: { ...current.schedule, trainingDeepDayOfWeek: event.target.value },
                  }))
                }
              >
                {["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"].map((day) => (
                  <option key={day} value={day}>
                    {day}
                  </option>
                ))}
              </select>
            </label>
          </div>

          <div className="field-grid three-up">
            <label className="field">
              <span>Default outbound start</span>
              <input
                type="date"
                value={config.schedule.defaultDateStart}
                onChange={(event) =>
                  setConfig((current) => ({
                    ...current,
                    schedule: { ...current.schedule, defaultDateStart: event.target.value },
                  }))
                }
              />
            </label>

            <label className="field">
              <span>Default outbound end</span>
              <input
                type="date"
                value={config.schedule.defaultDateEnd}
                onChange={(event) =>
                  setConfig((current) => ({
                    ...current,
                    schedule: { ...current.schedule, defaultDateEnd: event.target.value },
                  }))
                }
              />
            </label>

            <label className="field">
              <span>Day offsets</span>
              <input
                type="text"
                placeholder="0, 3, 7, 15, 30"
                value={config.schedule.defaultDateOffsets}
                onChange={(event) =>
                  setConfig((current) => ({
                    ...current,
                    schedule: { ...current.schedule, defaultDateOffsets: event.target.value },
                  }))
                }
              />
            </label>
          </div>
        </AdminAccordionSection>

        <AdminAccordionSection
          title="Passenger defaults"
          summary="Set the global passenger mix used when a run does not provide custom ADT, CHD, and INF values."
        >
          <div className="button-row" style={{ justifyContent: "flex-end" }}>
            <button className="button-link ghost" type="button" onClick={() => revertSection("passengers")}>
              Revert this section
            </button>
          </div>

          <div className="field-grid three-up">
            <label className="field">
              <span>Adults</span>
              <input
                type="number"
                min={0}
                value={config.passengers.adt}
                onChange={(event) =>
                  setConfig((current) => ({
                    ...current,
                    passengers: { ...current.passengers, adt: Number(event.target.value || 0) },
                  }))
                }
              />
            </label>

            <label className="field">
              <span>Children</span>
              <input
                type="number"
                min={0}
                value={config.passengers.chd}
                onChange={(event) =>
                  setConfig((current) => ({
                    ...current,
                    passengers: { ...current.passengers, chd: Number(event.target.value || 0) },
                  }))
                }
              />
            </label>

            <label className="field">
              <span>Infants</span>
              <input
                type="number"
                min={0}
                value={config.passengers.inf}
                onChange={(event) =>
                  setConfig((current) => ({
                    ...current,
                    passengers: { ...current.passengers, inf: Number(event.target.value || 0) },
                  }))
                }
              />
            </label>
          </div>
        </AdminAccordionSection>
      </div>

      <AdminAccordionSection
        title="Output options"
        summary="Choose which file formats the scraper writes after a run completes."
      >
        <div className="button-row" style={{ justifyContent: "flex-end" }}>
          <button className="button-link ghost" type="button" onClick={() => revertSection("output")}>
            Revert this section
          </button>
        </div>

        <div className="field-grid three-up">
          <label className="field checkbox-field">
            <input
              type="checkbox"
              checked={config.output.csv}
              onChange={(event) =>
                setConfig((current) => ({
                  ...current,
                  output: { ...current.output, csv: event.target.checked },
                }))
              }
            />
            <span>CSV export</span>
          </label>

          <label className="field checkbox-field">
            <input
              type="checkbox"
              checked={config.output.excel}
              onChange={(event) =>
                setConfig((current) => ({
                  ...current,
                  output: { ...current.output, excel: event.target.checked },
                }))
              }
            />
            <span>Excel export</span>
          </label>

          <label className="field checkbox-field">
            <input
              type="checkbox"
              checked={config.output.json}
              onChange={(event) =>
                setConfig((current) => ({
                  ...current,
                  output: { ...current.output, json: event.target.checked },
                }))
              }
            />
            <span>JSON export</span>
          </label>
        </div>

        <div className="field-grid three-up">
          <label className="field">
            <span>Archive mode</span>
            <input
              type="text"
              value={config.output.archiveMode}
              onChange={(event) =>
                setConfig((current) => ({
                  ...current,
                  output: { ...current.output, archiveMode: event.target.value },
                }))
              }
            />
          </label>

          <label className="field">
            <span>File prefix</span>
            <input
              type="text"
              value={config.output.filePrefix}
              onChange={(event) =>
                setConfig((current) => ({
                  ...current,
                  output: { ...current.output, filePrefix: event.target.value },
                }))
              }
            />
          </label>
        </div>
      </AdminAccordionSection>

      <AdminAccordionSection
        title="Holiday calendar"
        summary="Maintain the high-demand holiday dates that influence holiday-focused search windows."
      >
        <div className="button-row" style={{ justifyContent: "space-between", alignItems: "center" }}>
          <div />
          <div className="button-row">
            <button className="button-link ghost" type="button" onClick={() => revertSection("holidays")}>
              Revert this section
            </button>
            <button
              className="button-link ghost"
              type="button"
              onClick={() =>
                setConfig((current) => ({
                  ...current,
                  holidays: [...current.holidays, emptyHoliday()],
                }))
              }
            >
              Add holiday
            </button>
          </div>
        </div>

        <div className="admin-holiday-list">
          {config.holidays.map((holiday, index) => (
            <div className="admin-holiday-row" key={`${holiday.date}-${holiday.name}-${index}`}>
              <input type="date" value={holiday.date} onChange={(event) => updateHoliday(index, { date: event.target.value })} />
              <input type="text" placeholder="Holiday name" value={holiday.name} onChange={(event) => updateHoliday(index, { name: event.target.value })} />
              <input type="text" placeholder="Type" value={holiday.type} onChange={(event) => updateHoliday(index, { type: event.target.value })} />
              <input type="text" placeholder="Country" value={holiday.country} onChange={(event) => updateHoliday(index, { country: event.target.value })} />
              <label className="holiday-toggle">
                <input type="checkbox" checked={holiday.highDemand} onChange={(event) => updateHoliday(index, { highDemand: event.target.checked })} />
                <span>High demand</span>
              </label>
              <button className="button-link ghost" type="button" onClick={() => removeHoliday(index)}>
                Remove
              </button>
            </div>
          ))}
        </div>
      </AdminAccordionSection>

      <AdminAccordionSection
        title="Trip profile setup"
        summary="Define one-way and round-trip search windows here using date ranges, day offsets, and return windows."
      >
        <div className="button-row" style={{ justifyContent: "flex-end" }}>
          <button className="button-link ghost" type="button" onClick={() => revertSection("tripProfiles")}>
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

        {selectedTripProfile && selectedProfileImpact ? (
          <div className="admin-impact-preview">
            <div>
              <strong>Impact preview</strong>
              <div className="panel-copy" style={{ marginBottom: 0 }}>
                This shows where <strong>{selectedTripProfile.key}</strong> is currently assigned before you save or bulk-apply changes.
              </div>
            </div>

            <div className="admin-config-summary-grid">
              <div className="admin-config-summary">
                <strong>Airlines using it</strong>
                <span>{selectedProfileImpact.airlineCount}</span>
              </div>
              <div className="admin-config-summary">
                <strong>Routes touched</strong>
                <span>{selectedProfileImpact.routeCount}</span>
              </div>
              <div className="admin-config-summary">
                <strong>Operational routes</strong>
                <span>{selectedProfileImpact.operationalRoutes.length}</span>
              </div>
              <div className="admin-config-summary">
                <strong>Training or deep routes</strong>
                <span>{selectedProfileImpact.trainingRoutes.length + selectedProfileImpact.deepRoutes.length}</span>
              </div>
            </div>

            <div className="admin-config-readables">
              <div>
                <strong>Airlines</strong>
                <div className="panel-copy">{selectedProfileImpact.airlines.join(", ") || "Not assigned yet"}</div>
              </div>
              {[
                { label: "Available on routes", values: selectedProfileImpact.availableRoutes },
                { label: "Operational mode", values: selectedProfileImpact.operationalRoutes },
                { label: "Training mode", values: selectedProfileImpact.trainingRoutes },
                { label: "Deep mode", values: selectedProfileImpact.deepRoutes },
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
        ) : null}

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

      <AdminAccordionSection
        title="Airline and route-wise scheduling"
        summary="This is where scheduling is controlled per airline and per route by assigning which trip profiles are available, operational, training-only, or deep-only."
      >
        <div className="button-row" style={{ justifyContent: "flex-end" }}>
          <button className="button-link ghost" type="button" onClick={() => revertSection("routeProfiles")}>
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
                        onClick={() => selectAllProfilesForField(selectedRoute.key, field)}
                      >
                        Select all
                      </button>
                      <button
                        className="button-link ghost"
                        type="button"
                        onClick={() => clearProfilesForField(selectedRoute.key, field)}
                      >
                        Clear all
                      </button>
                    </div>
                    <div className="admin-profile-chip-grid">
                      {config.tripProfiles.map((profile) => {
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
                              updateRouteProfile(selectedRoute.key, (current) => {
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
          </div>
        ) : null}
      </AdminAccordionSection>

      <AdminAccordionSection
        title="Search behavior summary"
        summary="Review the plain-language market behavior groups here, and open the raw JSON only when deeper control is needed."
      >
        <div className="button-row" style={{ justifyContent: "flex-end" }}>
          <button className="button-link ghost" type="button" onClick={() => revertSection("advanced")}>
            Revert advanced JSON
          </button>
        </div>

        <div className="admin-config-summary-grid">
          <div className="admin-config-summary">
            <strong>Trip profiles</strong>
            <span>{config.marketSummary.tripProfileCount}</span>
          </div>
          <div className="admin-config-summary">
            <strong>Default one-way offsets</strong>
            <span>{config.marketSummary.defaultOneWayOffsets.join(", ") || "None"}</span>
          </div>
          <div className="admin-config-summary">
            <strong>Tourism RT offsets</strong>
            <span>{config.marketSummary.tourismRoundTripOffsets.join(", ") || "None"}</span>
          </div>
          <div className="admin-config-summary">
            <strong>Holiday RT offsets</strong>
            <span>{config.marketSummary.holidayReturnOffsets.join(", ") || "None"}</span>
          </div>
        </div>

        <div className="admin-config-readables">
          <div>
            <strong>Labor-flow origins</strong>
            <div className="panel-copy">{summarizeList(config.marketSummary.laborOriginCountries)}</div>
          </div>
          <div>
            <strong>Middle East destinations</strong>
            <div className="panel-copy">{summarizeList(config.marketSummary.middleEastDestinationCountries)}</div>
          </div>
          <div>
            <strong>Tourism airports</strong>
            <div className="panel-copy">{summarizeList(config.marketSummary.tourismAirports)}</div>
          </div>
          <div>
            <strong>Hub-and-spoke airlines</strong>
            <div className="panel-copy">{summarizeList(config.marketSummary.hubSpokeAirlines)}</div>
          </div>
          <div>
            <strong>Low-cost airlines</strong>
            <div className="panel-copy">{summarizeList(config.marketSummary.lccAirlines)}</div>
          </div>
          <div>
            <strong>Return-oriented airlines</strong>
            <div className="panel-copy">{summarizeList(config.marketSummary.returnOrientedAirlines)}</div>
          </div>
        </div>

        <label className="field">
          <span>Advanced market priors JSON</span>
          <textarea
            className="admin-json-textarea"
            rows={20}
            value={config.advanced.marketPriorsJson}
            onChange={(event) =>
              setConfig((current) => ({
                ...current,
                advanced: { marketPriorsJson: event.target.value },
              }))
            }
          />
        </label>
      </AdminAccordionSection>

      <div className="button-row" style={{ justifyContent: "space-between", alignItems: "center" }}>
        <div>
          {message ? <div className="status-banner good">{message}</div> : null}
          {applyMessage ? <div className="status-banner good">{applyMessage}</div> : null}
          {error ? <div className="status-banner warn">{error}</div> : null}
        </div>

        <div className="button-row">
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
