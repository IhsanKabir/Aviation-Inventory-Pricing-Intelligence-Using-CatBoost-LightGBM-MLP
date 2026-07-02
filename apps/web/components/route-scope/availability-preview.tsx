"use client";

import { useDeferredValue, useEffect, useMemo, useState } from "react";

import { getApiBaseUrl } from "@/lib/api";

import type { DateAvailabilityPoint, ScopeState } from "./scope-state";
import {
  AVAILABILITY_PREVIEW_COUNT,
  buildAvailabilityQueryString,
  deriveReturnScope,
  EMPTY_AVAILABILITY
} from "./scope-state";
import type { AvailabilityPayload } from "./scope-state";

type AvailabilityState = {
  loading: boolean;
  endpointMissing: boolean;
  error?: string;
  data: AvailabilityPayload;
};

function buildAvailabilitySummary(items: DateAvailabilityPoint[]) {
  if (!items.length) {
    return {
      totalDates: 0,
      totalRows: 0,
      firstDate: null,
      lastDate: null
    };
  }
  return {
    totalDates: items.length,
    totalRows: items.reduce((sum, item) => sum + item.row_count, 0),
    firstDate: items[0]?.date ?? null,
    lastDate: items[items.length - 1]?.date ?? null
  };
}

function hasDateMatchInRange(items: DateAvailabilityPoint[], startDate?: string, endDate?: string) {
  return items.some((item) => {
    if (startDate && item.date < startDate) {
      return false;
    }
    if (endDate && item.date > endDate) {
      return false;
    }
    return true;
  });
}

function renderAvailabilityTitle(
  label: string,
  summary: { totalDates: number; totalRows: number; firstDate: string | null; lastDate: string | null }
) {
  if (!summary.totalDates) {
    return label;
  }
  const spanLabel =
    summary.firstDate && summary.lastDate && summary.firstDate !== summary.lastDate
      ? `${summary.firstDate} to ${summary.lastDate}`
      : summary.firstDate ?? "Single date";
  return `${label} | ${summary.totalDates} dates | ${summary.totalRows} rows | ${spanLabel}`;
}

interface AvailabilityPreviewProps {
  state: ScopeState;
  scopeReady: boolean;
  requestId?: string;
  showAllDepartureDates: boolean;
  showAllReturnDates: boolean;
  onToggleShowAllDepartureDates: () => void;
  onToggleShowAllReturnDates: () => void;
}

export function AvailabilityPreview({
  state,
  scopeReady,
  requestId,
  showAllDepartureDates,
  showAllReturnDates,
  onToggleShowAllDepartureDates,
  onToggleShowAllReturnDates
}: AvailabilityPreviewProps) {
  const [availabilityState, setAvailabilityState] = useState<AvailabilityState>({
    loading: false,
    endpointMissing: false,
    data: EMPTY_AVAILABILITY
  });

  const returnScope = useMemo(() => deriveReturnScope(state), [state]);
  const availabilityQueryString = useMemo(
    () => (scopeReady ? buildAvailabilityQueryString(state) : null),
    [scopeReady, state]
  );
  const deferredAvailabilityQueryString = useDeferredValue(availabilityQueryString);

  useEffect(() => {
    if (!scopeReady || deferredAvailabilityQueryString === null) {
      setAvailabilityState({
        loading: false,
        endpointMissing: false,
        data: EMPTY_AVAILABILITY
      });
      return undefined;
    }

    const controller = new AbortController();
    setAvailabilityState((current) => ({
      ...current,
      loading: true,
      endpointMissing: false,
      error: undefined
    }));

    const path = deferredAvailabilityQueryString
      ? `/api/v1/reporting/route-date-availability?${deferredAvailabilityQueryString}${requestId ? `&request_id=${encodeURIComponent(requestId)}` : ""}`
      : "/api/v1/reporting/route-date-availability";

    fetch(`${getApiBaseUrl()}${path}`, {
      cache: "no-store",
      signal: controller.signal
    })
      .then(async (response) => {
        if (response.status === 404) {
          setAvailabilityState({
            loading: false,
            endpointMissing: true,
            data: EMPTY_AVAILABILITY
          });
          return;
        }
        if (!response.ok) {
          throw new Error(`${response.status} ${response.statusText}`);
        }
        const data = (await response.json()) as AvailabilityPayload;
        setAvailabilityState({
          loading: false,
          endpointMissing: false,
          data
        });
      })
      .catch((error: unknown) => {
        if (controller.signal.aborted) {
          return;
        }
        setAvailabilityState({
          loading: false,
          endpointMissing: false,
          error: error instanceof Error ? error.message : "Unable to inspect collected dates.",
          data: EMPTY_AVAILABILITY
        });
      });

    return () => controller.abort();
  }, [scopeReady, deferredAvailabilityQueryString, requestId]);

  const departureDateOptions = availabilityState.data.departure_dates;
  const returnDateOptions = availabilityState.data.return_dates;
  const departureSummary = useMemo(
    () => buildAvailabilitySummary(departureDateOptions),
    [departureDateOptions]
  );
  const returnSummary = useMemo(() => buildAvailabilitySummary(returnDateOptions), [returnDateOptions]);
  const visibleDepartureDates = showAllDepartureDates
    ? departureDateOptions
    : departureDateOptions.slice(0, AVAILABILITY_PREVIEW_COUNT);
  const visibleReturnDates = showAllReturnDates
    ? returnDateOptions
    : returnDateOptions.slice(0, AVAILABILITY_PREVIEW_COUNT);
  const returnDateMap = useMemo(
    () => new Map(returnDateOptions.map((item) => [item.date, item.row_count])),
    [returnDateOptions]
  );
  const availabilityDeferred = !scopeReady;
  const availabilityIdle = !scopeReady;
  const availabilityOk =
    !availabilityIdle && !availabilityState.loading && !availabilityState.endpointMissing && !availabilityState.error;
  const selectedReturnDateUnavailable =
    availabilityOk &&
    state.tripType === "RT" &&
    returnScope === "exact" &&
    Boolean(state.returnDate) &&
    !returnDateMap.has(state.returnDate);
  const selectedDepartureRangeUnavailable =
    availabilityOk &&
    Boolean(state.outboundDateStart || state.outboundDateEnd) &&
    !hasDateMatchInRange(
      departureDateOptions,
      state.outboundDateStart || undefined,
      state.outboundDateEnd || undefined
    );
  const selectedReturnRangeUnavailable =
    availabilityOk &&
    state.tripType === "RT" &&
    returnScope === "range" &&
    Boolean(state.returnDateStart || state.returnDateEnd) &&
    !hasDateMatchInRange(returnDateOptions, state.returnDateStart || undefined, state.returnDateEnd || undefined);

  return (
    <>
      {availabilityState.loading ? (
        <p className="mono">Refreshing collected dates...</p>
      ) : null}

      <div className="route-availability-grid">
        <div className="filter-group">
          <div className="filter-label">
            {renderAvailabilityTitle("Collected departure dates", departureSummary)}
          </div>
          {availabilityDeferred ? (
            <div className="empty-state">Select an exact route to inspect collected dates.</div>
          ) : availabilityOk ? (
            departureDateOptions.length ? (
              <div className="availability-section">
                <div className="chip-row">
                  {visibleDepartureDates.map((item) => (
                    <span className="chip route-date-chip" key={`departure-${item.date}`}>
                      {item.date} ({item.row_count})
                    </span>
                  ))}
                </div>
                {departureDateOptions.length > AVAILABILITY_PREVIEW_COUNT ? (
                  <button
                    className="availability-toggle"
                    onClick={onToggleShowAllDepartureDates}
                    type="button"
                  >
                    {showAllDepartureDates
                      ? "Show fewer departure dates"
                      : `Show all ${departureDateOptions.length} departure dates`}
                  </button>
                ) : null}
              </div>
            ) : (
              <div className="empty-state">No collected departure dates for the current scope.</div>
            )
          ) : availabilityState.loading ? (
            <div className="empty-state">Loading collected departure dates for the current scope...</div>
          ) : availabilityState.endpointMissing ? (
            <div className="empty-state">Date availability is not available right now.</div>
          ) : (
            <div className="empty-state error-state">
              Unable to inspect collected dates right now.
            </div>
          )}
        </div>

        {state.tripType === "RT" ? (
          <div className="filter-group">
            <div className="filter-label">
              {renderAvailabilityTitle("Collected return dates", returnSummary)}
            </div>
            {availabilityDeferred ? (
              <div className="empty-state">Select an exact route to inspect collected return dates.</div>
            ) : availabilityOk ? (
              returnDateOptions.length ? (
                <div className="availability-section">
                  <div className="chip-row">
                    {visibleReturnDates.map((item) => (
                      <span className="chip route-date-chip" key={`return-${item.date}`}>
                        {item.date} ({item.row_count})
                      </span>
                    ))}
                  </div>
                  {returnDateOptions.length > AVAILABILITY_PREVIEW_COUNT ? (
                    <button
                      className="availability-toggle"
                      onClick={onToggleShowAllReturnDates}
                      type="button"
                    >
                      {showAllReturnDates
                        ? "Show fewer return dates"
                        : `Show all ${returnDateOptions.length} return dates`}
                    </button>
                  ) : null}
                </div>
              ) : (
                <div className="empty-state">No collected round-trip return dates for the current scope.</div>
              )
            ) : availabilityState.loading ? (
              <div className="empty-state">Loading collected return dates for the current scope...</div>
            ) : availabilityState.endpointMissing ? (
              <div className="empty-state">Date availability is not available right now.</div>
            ) : (
              <div className="empty-state error-state">
                Unable to inspect collected return dates right now.
              </div>
            )}
          </div>
        ) : null}
      </div>

      {selectedDepartureRangeUnavailable ? (
        <div className="status-banner warn">
          The selected outbound window has no collected departures for this route scope and comparable cycle.
        </div>
      ) : null}
      {selectedReturnDateUnavailable ? (
        <div className="status-banner warn">
          The selected return date is not currently collected for this route scope and comparable cycle.
        </div>
      ) : null}
      {selectedReturnRangeUnavailable ? (
        <div className="status-banner warn">
          The selected return-date range has no collected matches for this route scope and comparable cycle.
        </div>
      ) : null}
    </>
  );
}
