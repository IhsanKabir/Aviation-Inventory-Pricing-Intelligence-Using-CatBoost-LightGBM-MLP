"use client";

import type { ReactNode } from "react";

import type { AdminHolidayEntry } from "@/lib/search-config";

export type RouteProfileField =
  | "marketTripProfiles"
  | "activeMarketTripProfiles"
  | "trainingMarketTripProfiles"
  | "deepMarketTripProfiles";

export function emptyHoliday(): AdminHolidayEntry {
  return {
    date: "",
    name: "",
    type: "national",
    country: "BD",
    highDemand: false,
  };
}

export function summarizeList(values: string[]) {
  if (!values.length) {
    return "None set";
  }
  return values.join(", ");
}

export function toggleValue(values: string[], item: string) {
  return values.includes(item) ? values.filter((value) => value !== item) : [...values, item];
}

export function cloneValue<T>(value: T): T {
  // structuredClone is V8-native and avoids a full JSON serialize/parse round trip
  // on every config mutation of this large nested object.
  return structuredClone(value);
}

export function summarizeSelection(values: string[]) {
  if (!values.length) {
    return "None selected";
  }
  if (values.length <= 3) {
    return values.join(", ");
  }
  return `${values.length} selected`;
}

export function normalizeProfileKey(value: string) {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

export function validateDateRangesText(value: string, label: string) {
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

export function validateOffsetRangesText(value: string, label: string) {
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

export const ADMIN_CONFIG_SECTIONS = [
  { id: "admin-config-schedule", label: "Schedule" },
  { id: "admin-config-basics", label: "Basics" },
  { id: "admin-config-output", label: "Output Options" },
  { id: "admin-config-holidays", label: "Holiday Calendar" },
  { id: "admin-config-trip-profiles", label: "Trip Profiles" },
  { id: "admin-config-route-assignments", label: "Route Assignments" },
  { id: "admin-config-advanced", label: "Advanced JSON" },
];

export function AdminAccordionSection({
  id,
  title,
  summary,
  open,
  onToggle,
  children,
}: {
  id: string;
  title: string;
  summary: string;
  open: boolean;
  onToggle: (open: boolean) => void;
  children: ReactNode;
}) {
  return (
    <details
      className="card panel admin-config-card admin-accordion"
      id={id}
      onToggle={(event) => onToggle(event.currentTarget.open)}
      open={open}
    >
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
