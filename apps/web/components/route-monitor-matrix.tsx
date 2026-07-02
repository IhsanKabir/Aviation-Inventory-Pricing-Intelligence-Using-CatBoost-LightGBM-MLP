"use client";

import { useMemo, useState } from "react";

import type {
  RouteMonitorMatrixDateGroup,
  RouteMonitorMatrixPayload,
  RouteMonitorMatrixRoute
} from "@/lib/api";
import { getRouteMonitorMatrixPayload } from "@/lib/api";

import type { RouteMonitorScopeQuery, SignalKey, ViewMode } from "./route-monitor/matrix-support";
import {
  dateGroupKey,
  hasRenderableCellData,
  routeMatchesDrilldown,
  routeSectionSortValue,
  sortFlightGroups,
  tripDateLabel,
  tripDurationLabel,
  tripWeekdayLabel
} from "./route-monitor/matrix-support";
import { RouteMonitorToolbar } from "./route-monitor/matrix-toolbar";
import { RouteReportBlock } from "./route-monitor/route-report-block";

import "./route-monitor-matrix.css";

export function RouteMonitorMatrix({
  payload,
  initialAirlines = [],
  scopeQuery
}: {
  payload: RouteMonitorMatrixPayload;
  initialAirlines?: string[];
  scopeQuery: RouteMonitorScopeQuery;
}) {
  const [selectedAirlines, setSelectedAirlines] = useState<string[]>(initialAirlines);
  const [selectedSignals, setSelectedSignals] = useState<SignalKey[]>([]);
  const [viewMode, setViewMode] = useState<ViewMode>("context");
  const [expandedRows, setExpandedRows] = useState<Record<string, boolean>>({});
  const [loadedDateGroups, setLoadedDateGroups] = useState<Record<string, RouteMonitorMatrixDateGroup>>({});
  const [loadingRows, setLoadingRows] = useState<Record<string, boolean>>({});
  const [rowErrors, setRowErrors] = useState<Record<string, string>>({});

  const availableAirlines = useMemo(() => {
    const codes = new Set<string>();
    for (const route of payload.routes) {
      for (const flight of route.flight_groups) {
        codes.add(flight.airline);
      }
    }
    return Array.from(codes).sort();
  }, [payload.routes]);

  const visibleRoutes = useMemo(() => {
    return payload.routes
      .map((route) => {
        const flightGroups =
          selectedAirlines.length === 0 || viewMode === "context"
            ? route.flight_groups
            : route.flight_groups.filter((item) => selectedAirlines.includes(item.airline));

        const routeHasSelectedAirline =
          selectedAirlines.length === 0 || route.flight_groups.some((item) => selectedAirlines.includes(item.airline));
        if (!routeHasSelectedAirline || flightGroups.length === 0) {
          return null;
        }

        const sortedFlightGroups = sortFlightGroups(flightGroups);
        const visibleFlightSet = new Set(sortedFlightGroups.map((item) => item.flight_group_id));
        const dateGroups = route.date_groups
          .map((dateGroup) => {
            const effectiveDateGroup = loadedDateGroups[dateGroupKey(route, dateGroup.departure_date)] ?? dateGroup;
            const captures = effectiveDateGroup.captures
              .map((capture) => ({
                ...capture,
                cells: capture.cells.filter(
                  (cell) => visibleFlightSet.has(cell.flight_group_id) && hasRenderableCellData(cell)
                )
              }))
              .filter((capture) => capture.cells.length > 0);

            if (captures.length === 0) {
              return null;
            }

            const latestSignals = new Set(captures[0].cells.map((cell) => cell.signal));
            if (selectedSignals.length > 0 && !selectedSignals.some((signal) => latestSignals.has(signal))) {
              return null;
            }

            return { ...effectiveDateGroup, captures };
          })
          .filter(Boolean) as RouteMonitorMatrixRoute["date_groups"];

        if (dateGroups.length === 0) {
          return null;
        }

        const activeFlightIds = new Set(
          dateGroups.flatMap((dateGroup) =>
            dateGroup.captures.flatMap((capture) => capture.cells.map((cell) => cell.flight_group_id))
          )
        );
        const activeFlightGroups = sortedFlightGroups.filter((item) => activeFlightIds.has(item.flight_group_id));
        if (activeFlightGroups.length === 0) {
          return null;
        }

        return {
          ...route,
          flight_groups: activeFlightGroups,
          date_groups: dateGroups
        };
      })
      .filter(Boolean) as RouteMonitorMatrixRoute[];
  }, [loadedDateGroups, payload.routes, selectedAirlines, selectedSignals, viewMode]);

  const visibleSections = useMemo(() => {
    const sections = new Map<
      string,
      {
        key: string;
        searchTripType: string;
        tripPairKey?: string | null;
        requestedOutboundDate?: string | null;
        requestedReturnDate?: string | null;
        tripDurationDays?: number | null;
        routes: RouteMonitorMatrixRoute[];
      }
    >();

    for (const route of visibleRoutes) {
      const isRoundTrip = (route.search_trip_type ?? "OW") === "RT" && route.trip_pair_key;
      const key = isRoundTrip ? `rt:${route.trip_pair_key}` : `ow:${route.route_key}`;
      if (!sections.has(key)) {
        sections.set(key, {
          key,
          searchTripType: route.search_trip_type ?? "OW",
          tripPairKey: route.trip_pair_key,
          requestedOutboundDate: route.requested_outbound_date,
          requestedReturnDate: route.requested_return_date,
          tripDurationDays: route.trip_duration_days,
          routes: []
        });
      }
      sections.get(key)!.routes.push(route);
    }

    return Array.from(sections.values())
      .map((section) => ({
        ...section,
        routes: section.routes.slice().sort((left, right) => {
          const legSort = routeSectionSortValue(left) - routeSectionSortValue(right);
          if (legSort !== 0) {
            return legSort;
          }
          return left.route_key.localeCompare(right.route_key);
        })
      }))
      .sort((left, right) => left.key.localeCompare(right.key));
  }, [visibleRoutes]);

  const signalCounts = useMemo(() => {
    const counts: Record<SignalKey, number> = {
      increase: 0,
      decrease: 0,
      new: 0,
      sold_out: 0,
      unknown: 0
    };
    for (const route of visibleRoutes) {
      for (const dateGroup of route.date_groups) {
        const latestCapture = dateGroup.captures[0];
        if (!latestCapture) continue;
        for (const cell of latestCapture.cells) {
          counts[cell.signal] += 1;
        }
      }
    }
    return counts;
  }, [visibleRoutes]);

  function toggleAirline(code: string) {
    setSelectedAirlines((current) =>
      current.includes(code) ? current.filter((item) => item !== code) : [...current, code]
    );
  }

  function toggleSignal(signal: SignalKey) {
    setSelectedSignals((current) =>
      current.includes(signal) ? current.filter((item) => item !== signal) : [...current, signal]
    );
  }

  async function loadRowHistory(route: RouteMonitorMatrixRoute, dateGroup: RouteMonitorMatrixDateGroup, rowKey: string) {
    setLoadingRows((current) => ({ ...current, [rowKey]: true }));
    setRowErrors((current) => {
      const next = { ...current };
      delete next[rowKey];
      return next;
    });

    try {
      const drilldown = await getRouteMonitorMatrixPayload({
        requestId: scopeQuery.requestId,
        cycleId: scopeQuery.cycleId,
        airlines: scopeQuery.airlines?.length ? scopeQuery.airlines : undefined,
        routePairKeys: scopeQuery.routePairs?.length ? scopeQuery.routePairs : undefined,
        origins: [route.origin],
        destinations: [route.destination],
        cabins: scopeQuery.cabin ? [scopeQuery.cabin] : undefined,
        tripTypes: [route.search_trip_type ?? scopeQuery.tripType ?? "OW"],
        returnDate:
          (route.search_trip_type ?? scopeQuery.tripType ?? "OW") === "RT" && route.requested_return_date
            ? route.requested_return_date
            : scopeQuery.returnDate,
        returnDateStart:
          (route.search_trip_type ?? scopeQuery.tripType ?? "OW") === "RT" && route.requested_return_date
            ? undefined
            : scopeQuery.returnDateStart,
        returnDateEnd:
          (route.search_trip_type ?? scopeQuery.tripType ?? "OW") === "RT" && route.requested_return_date
            ? undefined
            : scopeQuery.returnDateEnd,
        departureDate: dateGroup.departure_date,
        routeLimit: 1,
        historyLimit: scopeQuery.historyLimit,
        compactHistory: false
      });

      if (!drilldown.ok || !drilldown.data) {
        throw new Error(drilldown.error ?? "Unable to load full capture history.");
      }

      const matchedRoute =
        drilldown.data.routes.find((candidate) => routeMatchesDrilldown(route, candidate)) ??
        drilldown.data.routes.find((candidate) => candidate.route_key === route.route_key);
      const matchedDateGroup = matchedRoute?.date_groups.find((candidate) => candidate.departure_date === dateGroup.departure_date);

      if (!matchedDateGroup) {
        throw new Error("No detailed capture history was returned for this departure date.");
      }

      setLoadedDateGroups((current) => ({
        ...current,
        [rowKey]: matchedDateGroup
      }));
    } catch (error) {
      setRowErrors((current) => ({
        ...current,
        [rowKey]: error instanceof Error ? error.message : "Unable to load full capture history."
      }));
    } finally {
      setLoadingRows((current) => ({ ...current, [rowKey]: false }));
    }
  }

  function toggleRow(route: RouteMonitorMatrixRoute, dateGroup: RouteMonitorMatrixDateGroup) {
    const rowKey = dateGroupKey(route, dateGroup.departure_date);
    const isExpanded = Boolean(expandedRows[rowKey]);
    if (isExpanded) {
      setExpandedRows((current) => ({ ...current, [rowKey]: false }));
      return;
    }

    setExpandedRows((current) => ({ ...current, [rowKey]: true }));
    const effectiveDateGroup = loadedDateGroups[rowKey] ?? dateGroup;
    if (loadingRows[rowKey] || effectiveDateGroup.history_complete) {
      return;
    }
    void loadRowHistory(route, effectiveDateGroup, rowKey);
  }

  function clearInteractiveFilters() {
    setSelectedAirlines([]);
    setSelectedSignals([]);
    setViewMode("context");
    setExpandedRows({});
  }

  return (
    <div className="report-monitor">
      <RouteMonitorToolbar
        availableAirlines={availableAirlines}
        onClearFilters={clearInteractiveFilters}
        onToggleAirline={toggleAirline}
        onToggleSignal={toggleSignal}
        onViewModeChange={setViewMode}
        selectedAirlines={selectedAirlines}
        selectedSignals={selectedSignals}
        signalCounts={signalCounts}
        viewMode={viewMode}
      />

      <div className="route-report-stack">
        {visibleSections.length === 0 ? (
          <div className="empty-state">No route blocks match the current airline or signal selection.</div>
        ) : (
          visibleSections.map((section) => {
            const roundTripShell = section.searchTripType === "RT" && section.tripPairKey;
            return (
              <section className={roundTripShell ? "roundtrip-route-shell" : "roundtrip-route-shell single"} key={section.key}>
                {roundTripShell ? (
                  <div className="roundtrip-route-header">
                    <div>
                      <strong>{section.tripPairKey}</strong>
                      <span>
                        Outbound {tripDateLabel(section.requestedOutboundDate)} ({tripWeekdayLabel(section.requestedOutboundDate)}) | Inbound {tripDateLabel(section.requestedReturnDate)} ({tripWeekdayLabel(section.requestedReturnDate)})
                      </span>
                    </div>
                    <div className="roundtrip-route-meta">
                      <span className="route-type-pill" data-type="RT">RT</span>
                      <span>{tripDurationLabel(section.tripDurationDays)}</span>
                    </div>
                  </div>
                ) : null}

                <div className="route-report-stack nested">
                  {section.routes.map((route) => (
                    <RouteReportBlock
                      expandedRows={expandedRows}
                      key={`${section.key}-${route.route_key}`}
                      loadedDateGroups={loadedDateGroups}
                      loadingRows={loadingRows}
                      onToggleRow={toggleRow}
                      route={route}
                      rowErrors={rowErrors}
                    />
                  ))}
                </div>
              </section>
            );
          })
        )}
      </div>
    </div>
  );
}
