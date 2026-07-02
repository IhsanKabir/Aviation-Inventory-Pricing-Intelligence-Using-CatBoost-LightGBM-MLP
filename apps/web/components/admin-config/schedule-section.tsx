"use client";

import type { Dispatch, SetStateAction } from "react";

import type { AdminSearchConfig } from "@/lib/search-config";

import { AdminAccordionSection } from "./shared";

interface AdminScheduleSectionProps {
  config: AdminSearchConfig;
  setConfig: Dispatch<SetStateAction<AdminSearchConfig>>;
  onRevert: () => void;
  open: boolean;
  onToggle: (open: boolean) => void;
}

export function AdminScheduleSection({ config, setConfig, onRevert, open, onToggle }: AdminScheduleSectionProps) {
  return (
    <AdminAccordionSection
      id="admin-config-schedule"
      onToggle={onToggle}
      open={open}
      title="Schedule"
      summary="Set the global run cadence and task start times used when local scheduler tasks are installed or reapplied."
    >
      <div className="button-row" style={{ justifyContent: "flex-end" }}>
        <button className="button-link ghost" type="button" onClick={onRevert}>
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
  );
}
