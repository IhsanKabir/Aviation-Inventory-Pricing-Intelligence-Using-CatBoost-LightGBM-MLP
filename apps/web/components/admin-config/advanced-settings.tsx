"use client";

import type { Dispatch, SetStateAction } from "react";

import type { AdminSearchConfig } from "@/lib/search-config";

import { AdminAccordionSection, summarizeList } from "./shared";

interface AdvancedSettingsProps {
  config: AdminSearchConfig;
  setConfig: Dispatch<SetStateAction<AdminSearchConfig>>;
  onRevert: () => void;
  open: boolean;
  onToggle: (open: boolean) => void;
}

export function AdvancedSettings({ config, setConfig, onRevert, open, onToggle }: AdvancedSettingsProps) {
  return (
    <AdminAccordionSection
      id="admin-config-advanced"
      onToggle={onToggle}
      open={open}
      title="Advanced JSON"
      summary="Review the plain-language market behavior groups here, and open the raw JSON only when deeper control is needed."
    >
      <div className="button-row" style={{ justifyContent: "flex-end" }}>
        <button className="button-link ghost" type="button" onClick={onRevert}>
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
  );
}
