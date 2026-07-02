"use client";

import type { CSSProperties } from "react";

import type { SignalKey, ViewMode } from "./matrix-support";
import { SIGNAL_LABELS, signalTone, themeForAirline } from "./matrix-support";

interface RouteMonitorToolbarProps {
  availableAirlines: string[];
  selectedAirlines: string[];
  onToggleAirline: (code: string) => void;
  selectedSignals: SignalKey[];
  onToggleSignal: (signal: SignalKey) => void;
  signalCounts: Record<SignalKey, number>;
  viewMode: ViewMode;
  onViewModeChange: (mode: ViewMode) => void;
  onClearFilters: () => void;
}

export function RouteMonitorToolbar({
  availableAirlines,
  selectedAirlines,
  onToggleAirline,
  selectedSignals,
  onToggleSignal,
  signalCounts,
  viewMode,
  onViewModeChange,
  onClearFilters
}: RouteMonitorToolbarProps) {
  return (
    <div className="report-toolbar card">
      <div className="report-toolbar-row">
        <div className="report-label">Airlines</div>
        <div className="report-chip-row">
          {availableAirlines.map((code) => {
            const theme = themeForAirline(code);
            return (
              <button
                key={code}
                className="report-airline-chip"
                data-active={selectedAirlines.includes(code)}
                data-idle={selectedAirlines.length > 0 && !selectedAirlines.includes(code)}
                onClick={() => onToggleAirline(code)}
                style={
                  {
                    "--chip-bg": theme.header,
                    "--chip-fg": theme.headerText
                  } as CSSProperties
                }
                type="button"
              >
                {code}
              </button>
            );
          })}
        </div>
      </div>

      <div className="report-toolbar-row">
        <div className="report-label">Signals</div>
        <div className="report-chip-row">
          {(["increase", "decrease", "new", "sold_out", "unknown"] as SignalKey[]).map((signal) => (
            <button
              key={signal}
              className="report-signal-chip"
              data-active={selectedSignals.includes(signal)}
              data-tone={signalTone(signal)}
              onClick={() => onToggleSignal(signal)}
              type="button"
            >
              <span className="chip-prefix">
                {signal === "increase" ? "↑" : signal === "decrease" ? "↓" : signal === "new" ? "NEW" : signal === "sold_out" ? "S/O" : "—"}
              </span>
              <span>{SIGNAL_LABELS[signal]}</span>
              {signal !== "unknown" ? <span className="chip-count">{signalCounts[signal]}</span> : null}
            </button>
          ))}
        </div>
      </div>

      <div className="report-toolbar-row report-toolbar-meta">
        <div className="report-mode-switch">
          <button
            className="button-link ghost"
            data-active={viewMode === "context"}
            onClick={() => onViewModeChange("context")}
            type="button"
          >
            Context
          </button>
          <button
            className="button-link ghost"
            data-active={viewMode === "strict"}
            onClick={() => onViewModeChange("strict")}
            type="button"
          >
            Strict
          </button>
        </div>
        <button className="button-link ghost" onClick={onClearFilters} type="button">
          Clear interactive filters
        </button>
      </div>
    </div>
  );
}
