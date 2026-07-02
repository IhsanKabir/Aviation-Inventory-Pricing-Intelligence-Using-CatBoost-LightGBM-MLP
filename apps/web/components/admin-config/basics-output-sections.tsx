"use client";

import type { Dispatch, SetStateAction } from "react";

import type { AdminSearchConfig } from "@/lib/search-config";

import { AdminAccordionSection } from "./shared";

interface AdminSectionProps {
  config: AdminSearchConfig;
  setConfig: Dispatch<SetStateAction<AdminSearchConfig>>;
  onRevert: () => void;
  open: boolean;
  onToggle: (open: boolean) => void;
}

export function AdminBasicsSection({ config, setConfig, onRevert, open, onToggle }: AdminSectionProps) {
  return (
    <AdminAccordionSection
      id="admin-config-basics"
      onToggle={onToggle}
      open={open}
      title="Basics"
      summary="Set the default passenger mix used when a run does not provide custom ADT, CHD, and INF values."
    >
      <div className="button-row" style={{ justifyContent: "flex-end" }}>
        <button className="button-link ghost" type="button" onClick={onRevert}>
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
  );
}

export function AdminOutputSection({ config, setConfig, onRevert, open, onToggle }: AdminSectionProps) {
  return (
    <AdminAccordionSection
      id="admin-config-output"
      onToggle={onToggle}
      open={open}
      title="Output options"
      summary="Choose which file formats the scraper writes after a run completes."
    >
      <div className="button-row" style={{ justifyContent: "flex-end" }}>
        <button className="button-link ghost" type="button" onClick={onRevert}>
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
  );
}
