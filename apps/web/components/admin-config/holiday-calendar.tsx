"use client";

import type { Dispatch, SetStateAction } from "react";

import type { AdminHolidayEntry, AdminSearchConfig } from "@/lib/search-config";

import { AdminAccordionSection, emptyHoliday } from "./shared";

interface HolidayCalendarProps {
  config: AdminSearchConfig;
  setConfig: Dispatch<SetStateAction<AdminSearchConfig>>;
  onRevert: () => void;
  open: boolean;
  onToggle: (open: boolean) => void;
}

export function HolidayCalendar({ config, setConfig, onRevert, open, onToggle }: HolidayCalendarProps) {
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

  return (
    <AdminAccordionSection
      id="admin-config-holidays"
      onToggle={onToggle}
      open={open}
      title="Holiday calendar"
      summary="Maintain the high-demand holiday dates that influence holiday-focused search windows."
    >
      <div className="button-row" style={{ justifyContent: "space-between", alignItems: "center" }}>
        <div />
        <div className="button-row">
          <button className="button-link ghost" type="button" onClick={onRevert}>
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
  );
}
