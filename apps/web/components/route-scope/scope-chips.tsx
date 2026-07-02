"use client";

import type { CycleOption, RouteOption } from "./scope-state";
import { normalizeAirportCode } from "./scope-state";

interface CycleChipsProps {
  options: CycleOption[];
  selectedCycleId: string;
  isPending: boolean;
  onSelect: (cycleId: string | null) => void;
}

export function CycleChips({ options, selectedCycleId, isPending, onSelect }: CycleChipsProps) {
  if (!options.length) {
    return null;
  }
  return (
    <div className="filter-group">
      <div className="filter-label">Saved updates</div>
      <div className="chip-row">
        {options.map((item) => (
          <button
            key={item.cycleId ?? "latest-cycle"}
            className="chip"
            data-active={selectedCycleId === (item.cycleId ?? "")}
            data-pending={isPending}
            onClick={() => onSelect(item.cycleId)}
            type="button"
          >
            {item.label}
          </button>
        ))}
      </div>
    </div>
  );
}

interface AirlineChipsProps {
  options: string[];
  selected: string[];
  onToggle: (airline: string) => void;
}

export function AirlineChips({ options, selected, onToggle }: AirlineChipsProps) {
  if (!options.length) {
    return null;
  }
  return (
    <div className="filter-group">
      <div className="filter-label">Airlines</div>
      <div className="chip-row">
        {options.map((item) => (
          <button
            key={`airline-${item}`}
            className="chip"
            data-active={selected.includes(item)}
            onClick={() => onToggle(item)}
            type="button"
          >
            {item}
          </button>
        ))}
      </div>
    </div>
  );
}

interface SelectedRoutePairChipsProps {
  routePairs: string[];
  onRemove: (routeKey: string) => void;
}

export function SelectedRoutePairChips({ routePairs, onRemove }: SelectedRoutePairChipsProps) {
  return (
    <div className="filter-group">
      <div className="filter-label">Selected routes</div>
      {routePairs.length ? (
        <div className="chip-row">
          {routePairs.map((routePair) => (
            <button
              key={`selected-${routePair}`}
              className="chip"
              data-active={true}
              onClick={() => onRemove(routePair)}
              type="button"
            >
              {routePair} ×
            </button>
          ))}
        </div>
      ) : (
        <div className="empty-state">Choose one or more exact route chips below to build a multi-route view.</div>
      )}
    </div>
  );
}

interface ExactRouteChipsProps {
  options: RouteOption[];
  selectedRoutePairs: string[];
  isPending: boolean;
  onSelect: (option: RouteOption) => void;
}

export function ExactRouteChips({ options, selectedRoutePairs, isPending, onSelect }: ExactRouteChipsProps) {
  if (!options.length) {
    return null;
  }
  return (
    <div className="filter-group">
      <div className="filter-label">Exact routes to choose</div>
      <div className="chip-row">
        {options.map((item) => (
          <button
            key={`exact-route-${item.routeKey}`}
            className="route-hint-chip"
            data-active={selectedRoutePairs.includes(item.routeKey)}
            data-pending={isPending}
            onClick={() => onSelect(item)}
            type="button"
          >
            {item.routeKey}
          </button>
        ))}
      </div>
    </div>
  );
}

interface AirportSuggestionGridProps {
  originSuggestions: string[];
  destinationSuggestions: string[];
  origin: string;
  destination: string;
  onPickOrigin: (option: string) => void;
  onPickDestination: (option: string) => void;
}

export function AirportSuggestionGrid({
  originSuggestions,
  destinationSuggestions,
  origin,
  destination,
  onPickOrigin,
  onPickDestination
}: AirportSuggestionGridProps) {
  return (
    <div className="route-availability-grid">
      <div className="filter-group">
        <div className="filter-label">Matching origins</div>
        <div className="chip-row">
          {originSuggestions.map((option) => (
            <button
              key={`origin-${option}`}
              className="chip"
              data-active={normalizeAirportCode(origin) === option}
              onClick={() => onPickOrigin(option)}
              type="button"
            >
              {option}
            </button>
          ))}
        </div>
      </div>
      <div className="filter-group">
        <div className="filter-label">Matching destinations</div>
        <div className="chip-row">
          {destinationSuggestions.map((option) => (
            <button
              key={`destination-${option}`}
              className="chip"
              data-active={normalizeAirportCode(destination) === option}
              onClick={() => onPickDestination(option)}
              type="button"
            >
              {option}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}
