"use client";

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

export function AdminSearchConfigPanel({
  initialConfig,
}: {
  initialConfig: AdminSearchConfig;
}) {
  const [config, setConfig] = useState(initialConfig);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selectedRouteKey, setSelectedRouteKey] = useState(initialConfig.routeProfiles[0]?.key || "");
  const [selectedTripProfileKey, setSelectedTripProfileKey] = useState(initialConfig.tripProfiles[0]?.key || "");
  const [routeFilter, setRouteFilter] = useState("");
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

  async function refreshConfig() {
    const response = await fetch("/api/admin/search-config", { cache: "no-store" });
    const payload = (await response.json().catch(() => null)) as AdminSearchConfig & { detail?: string } | null;
    if (!response.ok || !payload) {
      throw new Error(payload?.detail || "Unable to refresh search configuration.");
    }
    setConfig(payload);
    if (!payload.routeProfiles.some((item) => item.key === selectedRouteKey)) {
      setSelectedRouteKey(payload.routeProfiles[0]?.key || "");
    }
    if (!payload.tripProfiles.some((item) => item.key === selectedTripProfileKey)) {
      setSelectedTripProfileKey(payload.tripProfiles[0]?.key || "");
    }
  }

  async function saveConfig() {
    setError(null);
    setMessage(null);
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
    setMessage("Search configuration saved. The next local run will use these file settings.");
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

  return (
    <div className="stack">
      <div className="status-banner">
        {config.persistenceNote}
      </div>

      <div className="admin-config-grid">
        <section className="card panel admin-config-card">
          <h3>Run schedule</h3>
          <div className="panel-copy">
            These settings control the local scheduler plan. The start times below are the anchor times used when autorun tasks are installed or reinstalled.
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
        </section>

        <section className="card panel admin-config-card">
          <h3>Passenger defaults</h3>
          <div className="panel-copy">
            These values are used when no custom passenger mix is supplied for a run.
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
        </section>
      </div>

      <section className="card panel admin-config-card">
        <h3>Output options</h3>
        <div className="panel-copy">
          Choose which report files are written after a run completes.
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
      </section>

      <section className="card panel admin-config-card">
        <div className="button-row" style={{ justifyContent: "space-between", alignItems: "center" }}>
          <div>
            <h3 style={{ marginBottom: "0.35rem" }}>Holiday calendar</h3>
            <div className="panel-copy" style={{ marginBottom: 0 }}>
              Keep high-demand national and religious travel dates current so holiday-focused search windows make sense.
            </div>
          </div>
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
      </section>

      <section className="card panel admin-config-card">
        <h3>Round-trip and trip profile setup</h3>
        <div className="panel-copy">
          Choose a profile to adjust its trip type, outbound offsets, and return offsets. Return offsets are the core round-trip search setup.
        </div>

        <div className="field-grid three-up">
          <label className="field">
            <span>Trip profile</span>
            <select value={selectedTripProfileKey} onChange={(event) => setSelectedTripProfileKey(event.target.value)}>
              {config.tripProfiles.map((item) => (
                <option key={item.key} value={item.key}>
                  {item.key}
                </option>
              ))}
            </select>
          </label>
        </div>

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
          </div>
        ) : null}
      </section>

      <section className="card panel admin-config-card">
        <h3>Route-wise setup</h3>
        <div className="panel-copy">
          Pick a specific airline route and decide which trip profiles are available, which run in live collection, and which are training-only or deep-only.
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
            <div className="admin-config-readables">
              {[
                ["Profiles available on this route", "marketTripProfiles"] as const,
                ["Run in operational mode", "activeMarketTripProfiles"] as const,
                ["Extra for training mode", "trainingMarketTripProfiles"] as const,
                ["Extra for deep mode", "deepMarketTripProfiles"] as const,
              ].map(([label, field]) => (
                <div key={field}>
                  <strong>{label}</strong>
                  <div className="admin-profile-chip-grid">
                    {config.tripProfiles.map((profile) => {
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
              ))}
            </div>
          </div>
        ) : null}
      </section>

      <section className="card panel admin-config-card">
        <h3>Search behavior summary</h3>
        <div className="panel-copy">
          This translates the heavy market profile file into plain-language groups. Advanced users can still edit the raw JSON below.
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
      </section>

      <div className="button-row" style={{ justifyContent: "space-between", alignItems: "center" }}>
        <div>
          {message ? <div className="status-banner good">{message}</div> : null}
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
        </div>
      </div>
    </div>
  );
}
