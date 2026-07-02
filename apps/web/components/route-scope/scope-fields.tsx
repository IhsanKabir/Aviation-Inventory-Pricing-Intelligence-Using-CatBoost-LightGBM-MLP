"use client";

import type { ScopeState } from "./scope-state";
import { normalizeAirportCode } from "./scope-state";

interface ScopeFieldGridProps {
  state: ScopeState;
  onUpdate: (next: Partial<ScopeState>) => void;
  onTripTypeChange: (nextTripType: string) => void;
}

export function ScopeFieldGrid({ state, onUpdate, onTripTypeChange }: ScopeFieldGridProps) {
  return (
    <div className="field-grid route-scope-grid">
      <label className="field">
        <span>Origin</span>
        <input
          onChange={(event) => onUpdate({ origin: normalizeAirportCode(event.target.value) })}
          placeholder="DAC"
          type="text"
          value={state.origin}
        />
      </label>
      <label className="field">
        <span>Destination</span>
        <input
          onChange={(event) => onUpdate({ destination: normalizeAirportCode(event.target.value) })}
          placeholder="CXB"
          type="text"
          value={state.destination}
        />
      </label>
      <label className="field">
        <span>Cabin</span>
        <input
          onChange={(event) => onUpdate({ cabin: event.target.value })}
          placeholder="Economy"
          type="text"
          value={state.cabin}
        />
      </label>
      <label className="field">
        <span>Trip type</span>
        <select onChange={(event) => onTripTypeChange(event.target.value)} value={state.tripType}>
          <option value="OW">One-way</option>
          <option value="RT">Round-trip</option>
        </select>
      </label>
      <label className="field">
        <span>Route blocks</span>
        <input
          inputMode="numeric"
          onChange={(event) => onUpdate({ routeLimit: event.target.value })}
          pattern="[0-9]*"
          type="text"
          value={state.routeLimit}
        />
      </label>
      <label className="field">
        <span>History depth</span>
        <input
          inputMode="numeric"
          onChange={(event) => onUpdate({ historyLimit: event.target.value })}
          pattern="[0-9]*"
          type="text"
          value={state.historyLimit}
        />
      </label>
    </div>
  );
}

interface TravelWindowsCardProps {
  state: ScopeState;
  onUpdate: (next: Partial<ScopeState>) => void;
}

export function TravelWindowsCard({ state, onUpdate }: TravelWindowsCardProps) {
  return (
    <div className="scope-section-card">
      <div className="scope-section-header">
        <div>
          <div className="scope-section-kicker">Travel windows</div>
          <h3 className="scope-section-title">Set the date window{state.tripType === "RT" ? ", then narrow inbound" : ""}</h3>
        </div>
        <p className="scope-section-copy">Blank edge = open range. Table and Excel will follow this window.</p>
      </div>

      <div className="scope-window-grid">
        <section className="scope-window-card" data-tone="outbound">
          <div className="scope-window-title">Outbound dates</div>
          <div className="scope-window-copy">Departure range for this view.</div>
          <div className="scope-window-fields">
            <label className="field">
              <span>From</span>
              <input
                onChange={(event) => onUpdate({ outboundDateStart: event.target.value })}
                type="date"
                value={state.outboundDateStart}
              />
            </label>
            <label className="field">
              <span>To</span>
              <input
                onChange={(event) => onUpdate({ outboundDateEnd: event.target.value })}
                type="date"
                value={state.outboundDateEnd}
              />
            </label>
          </div>
        </section>

        {state.tripType === "RT" ? (
          <section className="scope-window-card" data-tone="inbound">
            <div className="scope-window-title">Inbound dates</div>
            <div className="scope-window-copy">Return range within the chosen outbound view.</div>
            <div className="scope-window-fields">
              <label className="field">
                <span>From</span>
                <input
                  onChange={(event) =>
                    onUpdate({
                      returnDateStart: event.target.value,
                      returnDate: ""
                    })
                  }
                  type="date"
                  value={state.returnDateStart}
                />
              </label>
              <label className="field">
                <span>To</span>
                <input
                  onChange={(event) =>
                    onUpdate({
                      returnDateEnd: event.target.value,
                      returnDate: ""
                    })
                  }
                  type="date"
                  value={state.returnDateEnd}
                />
              </label>
            </div>
          </section>
        ) : null}
      </div>
    </div>
  );
}
