"use client";

import type { CSSProperties } from "react";
import { Fragment, useMemo, useState } from "react";

import type {
  RouteMonitorFlightGroup,
  RouteMonitorMatrixCell,
  RouteMonitorMatrixDateGroup,
  RouteMonitorMatrixPayload,
  RouteMonitorMatrixRoute
} from "@/lib/api";
import { getRouteMonitorMatrixPayload } from "@/lib/api";
import { formatDhakaDate, formatDhakaDateTime, formatMoney, formatPercent, formatRouteGeo, formatRouteType } from "@/lib/format";

type ViewMode = "context" | "strict";
type SignalKey = "increase" | "decrease" | "new" | "sold_out" | "unknown";
type RouteMonitorScopeQuery = {
  requestId?: string;
  cycleId?: string;
  airlines?: string[];
  routePairs?: string[];
  origin?: string;
  destination?: string;
  cabin?: string;
  tripType: string;
  startDate?: string;
  endDate?: string;
  returnDate?: string;
  returnDateStart?: string;
  returnDateEnd?: string;
  historyLimit: number;
};

const SIGNAL_LABELS: Record<SignalKey, string> = {
  increase: "Increase",
  decrease: "Decrease",
  new: "New",
  sold_out: "Sold out",
  unknown: "Unknown"
};

const AIRLINE_THEME: Record<string, { header: string; sub: string; cell: string; text: string; headerText: string }> = {
  BG: { header: "#c8102e", sub: "#fff1f4", cell: "#f9f0f2", text: "#5f1020", headerText: "#ffffff" },
  VQ: { header: "#003a70", sub: "#ffe7d0", cell: "#fff4ea", text: "#123a6e", headerText: "#ffffff" },
  BS: { header: "#00557f", sub: "#d8ebf7", cell: "#eef7fc", text: "#11384d", headerText: "#ffffff" },
  "2A": { header: "#b78700", sub: "#fdefc7", cell: "#fff9ea", text: "#6b4e00", headerText: "#1e1e1e" },
  G9: { header: "#c6282b", sub: "#f9e1e2", cell: "#fdf2f3", text: "#7a1d20", headerText: "#ffffff" },
  "3L": { header: "#c6282b", sub: "#f9e1e2", cell: "#fdf2f3", text: "#7a1d20", headerText: "#ffffff" },
  "6E": { header: "#2b2f86", sub: "#e5e6fb", cell: "#f4f4ff", text: "#1b1f69", headerText: "#ffffff" },
  EK: { header: "#d71920", sub: "#fde7e8", cell: "#fff4f4", text: "#7f1116", headerText: "#ffffff" },
  FZ: { header: "#005b96", sub: "#daeefe", cell: "#f1f8ff", text: "#0f3b66", headerText: "#ffffff" },
  CZ: { header: "#2a9fd8", sub: "#ddf3fd", cell: "#f2fbff", text: "#0d4c69", headerText: "#ffffff" },
  SQ: { header: "#b69100", sub: "#fcf1c7", cell: "#fff9e7", text: "#6c5600", headerText: "#1e1e1e" },
  SV: { header: "#005f7f", sub: "#d8ecf1", cell: "#eef9fc", text: "#15404f", headerText: "#ffffff" },
  MH: { header: "#004990", sub: "#ddeafd", cell: "#f2f7ff", text: "#153a63", headerText: "#ffffff" },
  OD: { header: "#c63a24", sub: "#fde8e2", cell: "#fff5f1", text: "#6f261a", headerText: "#ffffff" },
  QR: { header: "#5c0d45", sub: "#f2ddeb", cell: "#f9eff5", text: "#481038", headerText: "#ffffff" },
  TG: { header: "#6c3c96", sub: "#eadcf6", cell: "#f5effb", text: "#4b2a67", headerText: "#ffffff" },
  UL: { header: "#8c1d4d", sub: "#f6dfe8", cell: "#fbf0f4", text: "#581532", headerText: "#ffffff" },
  WY: { header: "#6d2326", sub: "#f3dddd", cell: "#faefef", text: "#4d191b", headerText: "#ffffff" },
  AK: { header: "#c92026", sub: "#fbe1e3", cell: "#fdf3f4", text: "#7e1a1e", headerText: "#ffffff" },
  "8D": { header: "#2465a4", sub: "#dfeafb", cell: "#f3f8ff", text: "#163d65", headerText: "#ffffff" }
};

function themeForAirline(code: string) {
  return (
    AIRLINE_THEME[code] ?? {
      header: "#194866",
      sub: "#dcecf6",
      cell: "#f4f8fb",
      text: "#163449",
      headerText: "#ffffff"
    }
  );
}

function signalArrow(signal: SignalKey) {
  if (signal === "increase") return "\u2191";
  if (signal === "decrease") return "\u2193";
  return "";
}

function signalTone(signal: SignalKey) {
  if (signal === "increase") return "tone-up";
  if (signal === "decrease") return "tone-down";
  if (signal === "new") return "tone-new";
  if (signal === "sold_out") return "tone-soldout";
  return "tone-neutral";
}

function summarizeCell(cell: RouteMonitorMatrixCell | undefined) {
  if (!cell) {
    return {
      minFare: "N/O",
      maxFare: "\u2014",
      tax: "\u2014",
      seats: "\u2014 / \u2014",
      load: "\u2014",
      minFareMeta: null as string | null,
      maxFareMeta: null as string | null
    };
  }

  const buildFareMeta = (bookingClass?: string | null, seatAvailable?: number | null) => {
    const fareMetaParts: string[] = [];
    if (bookingClass) {
      fareMetaParts.push(String(bookingClass).trim());
    }
    if (seatAvailable != null) {
      fareMetaParts.push(`${seatAvailable} seat${Number(seatAvailable) === 1 ? "" : "s"}`);
    }
    return fareMetaParts.length ? fareMetaParts.join(" | ") : null;
  };

  return {
    minFare: cell.min_total_price_bdt != null ? formatMoney(cell.min_total_price_bdt, "BDT").replace("BDT ", "") : "N/O",
    maxFare: cell.max_total_price_bdt != null ? formatMoney(cell.max_total_price_bdt, "BDT").replace("BDT ", "") : "\u2014",
    tax: cell.tax_amount != null ? formatMoney(cell.tax_amount, "BDT").replace("BDT ", "") : "\u2014",
    seats:
      cell.seat_available != null || cell.seat_capacity != null
        ? `${cell.seat_available ?? "\u2014"} / ${cell.seat_capacity ?? "\u2014"}`
        : "\u2014 / \u2014",
    load: formatPercent(cell.load_factor_pct),
    minFareMeta: buildFareMeta(cell.min_booking_class ?? cell.booking_class, cell.min_seat_available ?? cell.seat_available),
    maxFareMeta: buildFareMeta(cell.max_booking_class, cell.max_seat_available)
  };
}

function fareSignalTag(signal: SignalKey, soldout?: boolean | null) {
  if (soldout || signal === "sold_out") {
    return "SOLD OUT";
  }
  if (signal === "new") {
    return "NEW";
  }
  return null;
}

function routeLeader(route: RouteMonitorMatrixRoute, visibleFlights: RouteMonitorFlightGroup[]) {
  const visibleSet = new Set(visibleFlights.map((item) => item.flight_group_id));
  let best:
    | {
        airline: string;
        flightNumber: string;
        amount: number;
        dates: string[];
      }
    | undefined;

  for (const dateGroup of route.date_groups) {
    const latestCapture = dateGroup.captures[0];
    if (!latestCapture) continue;
    for (const cell of latestCapture.cells) {
      if (!visibleSet.has(cell.flight_group_id) || cell.min_total_price_bdt == null) {
        continue;
      }
      const flight = route.flight_groups.find((item) => item.flight_group_id === cell.flight_group_id);
      if (!flight) continue;
      if (!best || cell.min_total_price_bdt < best.amount) {
        best = {
          airline: flight.airline,
          flightNumber: flight.flight_number,
          amount: Number(cell.min_total_price_bdt),
          dates: [dateGroup.departure_date]
        };
      } else if (cell.min_total_price_bdt === best.amount && !best.dates.includes(dateGroup.departure_date)) {
        best.dates.push(dateGroup.departure_date);
      }
    }
  }

  return best;
}

function formatLeaderDates(values: string[]) {
  return values
    .slice()
    .sort()
    .map((item) => formatDhakaDate(`${item}T00:00:00Z`).replace(",", ""))
    .join(", ");
}

function sortFlightGroups(flightGroups: RouteMonitorFlightGroup[]) {
  return flightGroups.slice().sort((left, right) => {
    const leftLegSequence = left.leg_sequence ?? (left.leg_direction === "inbound" ? 2 : 1);
    const rightLegSequence = right.leg_sequence ?? (right.leg_direction === "inbound" ? 2 : 1);
    if (leftLegSequence !== rightLegSequence) {
      return leftLegSequence - rightLegSequence;
    }

    const leftReturn = (left.requested_return_date ?? "").trim();
    const rightReturn = (right.requested_return_date ?? "").trim();
    if (leftReturn !== rightReturn) {
      if (!leftReturn) return -1;
      if (!rightReturn) return 1;
      return leftReturn.localeCompare(rightReturn);
    }

    const leftTime = (left.departure_time ?? "").trim();
    const rightTime = (right.departure_time ?? "").trim();

    if (leftTime !== rightTime) {
      if (!leftTime) return 1;
      if (!rightTime) return -1;
      return leftTime.localeCompare(rightTime);
    }

    const airlineDiff = left.airline.localeCompare(right.airline);
    if (airlineDiff !== 0) {
      return airlineDiff;
    }

    return left.flight_number.localeCompare(right.flight_number);
  });
}

function tripDateLabel(value?: string | null) {
  if (!value) {
    return "-";
  }
  return formatDhakaDate(`${value}T00:00:00Z`).replace(",", "");
}

function tripWeekdayLabel(value?: string | null) {
  if (!value) {
    return "-";
  }
  return new Intl.DateTimeFormat("en-US", {
    weekday: "long",
    timeZone: "Asia/Dhaka"
  }).format(new Date(`${value}T00:00:00Z`));
}

function tripDurationLabel(value?: number | null) {
  if (value == null || Number.isNaN(value)) {
    return "Linked itinerary view";
  }
  return `${value} Day${value === 1 ? "" : "s"}`;
}

function routeSectionSortValue(route: RouteMonitorMatrixRoute) {
  const outboundOrigin = (route.trip_origin ?? "").trim().toUpperCase();
  const outboundDestination = (route.trip_destination ?? "").trim().toUpperCase();
  const routeOrigin = (route.origin ?? "").trim().toUpperCase();
  const routeDestination = (route.destination ?? "").trim().toUpperCase();
  if (outboundOrigin && outboundDestination && routeOrigin === outboundOrigin && routeDestination === outboundDestination) {
    return 0;
  }
  return 1;
}

function flightLegLabel(flight: RouteMonitorFlightGroup) {
  if ((flight.search_trip_type ?? "OW") !== "RT") {
    return null;
  }
  const direction = flight.leg_direction === "inbound" ? "Inbound" : "Outbound";
  if (flight.requested_return_date) {
    return `${direction} | inbound ${tripDateLabel(flight.requested_return_date)}`;
  }
  return direction;
}

function hasRenderableCellData(cell: RouteMonitorMatrixCell) {
  return (
    cell.min_total_price_bdt != null ||
    cell.max_total_price_bdt != null ||
    cell.tax_amount != null ||
    cell.booking_class != null ||
    cell.seat_available != null ||
    cell.seat_capacity != null ||
    cell.load_factor_pct != null ||
    Boolean(cell.soldout)
  );
}

function routeHasSeatData(route: RouteMonitorMatrixRoute) {
  return route.date_groups.some((dateGroup) =>
    dateGroup.captures.some((capture) =>
      capture.cells.some((cell) => cell.seat_available != null || cell.seat_capacity != null)
    )
  );
}

function routeHasLoadData(route: RouteMonitorMatrixRoute) {
  return route.date_groups.some((dateGroup) =>
    dateGroup.captures.some((capture) =>
      capture.cells.some((cell) => cell.load_factor_pct != null)
    )
  );
}

function dateGroupKey(route: RouteMonitorMatrixRoute, departureDate: string) {
  return [
    route.route_key,
    route.search_trip_type ?? "OW",
    route.trip_pair_key ?? "",
    route.requested_return_date ?? "",
    departureDate
  ].join("|");
}

function routeMatchesDrilldown(target: RouteMonitorMatrixRoute, candidate: RouteMonitorMatrixRoute) {
  return (
    candidate.route_key === target.route_key &&
    (candidate.search_trip_type ?? "OW") === (target.search_trip_type ?? "OW") &&
    (candidate.trip_pair_key ?? "") === (target.trip_pair_key ?? "") &&
    (candidate.requested_return_date ?? "") === (target.requested_return_date ?? "")
  );
}

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
                  onClick={() => toggleAirline(code)}
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
                onClick={() => toggleSignal(signal)}
                type="button"
              >
                <span className="chip-prefix">
                  {signal === "increase" ? "\u2191" : signal === "decrease" ? "\u2193" : signal === "new" ? "NEW" : signal === "sold_out" ? "S/O" : "\u2014"}
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
              onClick={() => setViewMode("context")}
              type="button"
            >
              Context
            </button>
            <button
              className="button-link ghost"
              data-active={viewMode === "strict"}
              onClick={() => setViewMode("strict")}
              type="button"
            >
              Strict
            </button>
          </div>
          <button className="button-link ghost" onClick={clearInteractiveFilters} type="button">
            Clear interactive filters
          </button>
        </div>
      </div>

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
                  {section.routes.map((route) => {
                    const leader = routeLeader(route, route.flight_groups);
                    const showSeatColumn = routeHasSeatData(route);
                    const showLoadColumn = routeHasLoadData(route);
                    return (
                      <section className="route-report-block" key={`${section.key}-${route.route_key}`}>
                        <div className="route-report-title-row">
                          <div className="route-report-title">
                            <strong>{route.route_key}</strong>
                            <div className="route-report-title-meta">
                              <span className="route-type-pill" data-type={formatRouteType(route.route_type)}>
                                {formatRouteType(route.route_type)}
                              </span>
                              <span className="route-geo">{formatRouteGeo(route.origin_country_code, route.destination_country_code)}</span>
                              {route.search_trip_type === "RT" ? <span className="trip-leg-pill">{route.origin === route.trip_origin ? "Outbound" : "Inbound"}</span> : null}
                            </div>
                          </div>
                          <div className="route-report-leader">
                            <span className="route-report-leader-label">Route Price Leader (Lowest Fare):</span>{" "}
                            {leader ? (
                              <>
                                <span
                                  className={`leader-airline ${themeForAirline(leader.airline).headerText === "#1e1e1e" ? "leader-airline-dark" : ""}`}
                                  style={{
                                    background: themeForAirline(leader.airline).header,
                                    color: themeForAirline(leader.airline).headerText
                                  }}
                                >
                                  {leader.airline}
                                  {leader.flightNumber}
                                </span>{" "}
                                <span className="leader-amount">{leader.amount.toLocaleString()}</span>{" "}
                                <span className="leader-dates">(Dates: {formatLeaderDates(leader.dates)})</span>
                              </>
                            ) : (
                              "No visible fare leader"
                            )}
                          </div>
                        </div>

                        <div className="route-report-scroll">
                          <table className="route-report-table">
                            <thead>
                              <tr>
                                <th className="sticky-col sticky-route-meta" rowSpan={3}>
                                  {route.search_trip_type === "RT" ? "Outbound Date" : "Date"}
                                </th>
                                <th className="sticky-col sticky-route-meta second" rowSpan={3}>
                                  {route.search_trip_type === "RT" ? "Outbound Weekday" : "Day"}
                                </th>
                                <th className="sticky-col sticky-route-meta third" rowSpan={3}>
                                  Capture Date/Time
                                </th>
                                {route.flight_groups.map((flight) => {
                                  const theme = themeForAirline(flight.airline);
                                  const metricColumnCount = 3 + (showSeatColumn ? 1 : 0) + (showLoadColumn ? 1 : 0);
                                  return (
                                    <th
                                      className="flight-band"
                                      colSpan={metricColumnCount}
                                      key={flight.flight_group_id}
                                      style={{ background: theme.header, color: theme.headerText }}
                                    >
                                      {flight.airline}
                                      {flight.flight_number} | {flight.aircraft || "Flight"}
                                    </th>
                                  );
                                })}
                              </tr>
                              <tr>
                                {route.flight_groups.map((flight) => {
                                  const theme = themeForAirline(flight.airline);
                                  const metricColumnCount = 3 + (showSeatColumn ? 1 : 0) + (showLoadColumn ? 1 : 0);
                                  return (
                                    <th
                                      className="flight-subband"
                                      colSpan={metricColumnCount}
                                      key={`sub-${flight.flight_group_id}`}
                                      style={{ background: theme.header, color: theme.headerText }}
                                    >
                                      <div className="flight-subband-stack">
                                        <span>{flight.departure_time || "\u2014"}</span>
                                        {flightLegLabel(flight) ? <span>{flightLegLabel(flight)}</span> : null}
                                      </div>
                                    </th>
                                  );
                                })}
                              </tr>
                              <tr>
                                {route.flight_groups.map((flight) => {
                                  const theme = themeForAirline(flight.airline);
                                  return (
                                    <Fragment key={`metrics-${flight.flight_group_id}`}>
                                      <th className="metric-head" key={`${flight.flight_group_id}-min`} style={{ background: theme.sub, color: theme.text }}>
                                        Min Fare
                                      </th>
                                      <th className="metric-head" key={`${flight.flight_group_id}-max`} style={{ background: theme.sub, color: theme.text }}>
                                        Max Fare
                                      </th>
                                      <th className="metric-head" key={`${flight.flight_group_id}-tax`} style={{ background: theme.sub, color: theme.text }}>
                                        Tax Amount
                                      </th>
                                      {showSeatColumn ? (
                                        <th className="metric-head" key={`${flight.flight_group_id}-seat`} style={{ background: theme.sub, color: theme.text }}>
                                          Open/Cap
                                        </th>
                                      ) : null}
                                      {showLoadColumn ? (
                                        <th className="metric-head" key={`${flight.flight_group_id}-load`} style={{ background: theme.sub, color: theme.text }}>
                                          Inv Press
                                        </th>
                                      ) : null}
                                    </Fragment>
                                  );
                                })}
                              </tr>
                            </thead>
                            <tbody>
                              {route.date_groups.map((dateGroup) => {
                                const rowKey = dateGroupKey(route, dateGroup.departure_date);
                                const effectiveDateGroup = loadedDateGroups[rowKey] ?? dateGroup;
                                const expanded = Boolean(expandedRows[rowKey]);
                                const isLoadingHistory = Boolean(loadingRows[rowKey]);
                                const visibleCaptures = expanded ? effectiveDateGroup.captures : effectiveDateGroup.captures.slice(0, 1);
                                const captureCount = effectiveDateGroup.capture_count ?? effectiveDateGroup.captures.length;
                                const canExpandHistory = captureCount > 1;
                                const rowError = rowErrors[rowKey];

                                return visibleCaptures.map((capture, captureIndex) => {
                                  const showDateMeta = captureIndex === 0;
                                  const expandLabel =
                                    canExpandHistory
                                      ? `${expanded ? "[-]" : `[+${Math.max(captureCount - 1, 0)}]`} ${isLoadingHistory ? "Loading history..." : formatDhakaDateTime(capture.captured_at_utc)}`
                                      : formatDhakaDateTime(capture.captured_at_utc);

                                  return (
                                    <tr
                                      className={captureIndex === 0 ? "latest-capture-row" : "history-capture-row"}
                                      data-group-start={captureIndex === 0}
                                      data-group-end={captureIndex === visibleCaptures.length - 1}
                                      key={`${rowKey}-${capture.captured_at_utc}`}
                                    >
                                      <td className="sticky-col sticky-route-meta route-value">{showDateMeta ? dateGroup.departure_date : ""}</td>
                                      <td className="sticky-col sticky-route-meta second route-value">{showDateMeta ? effectiveDateGroup.day_label : ""}</td>
                                      <td className="sticky-col sticky-route-meta third route-value">
                                        {showDateMeta && canExpandHistory ? (
                                          <button
                                            className="history-toggle"
                                            data-expanded={expanded}
                                            disabled={isLoadingHistory}
                                            onClick={() => toggleRow(route, effectiveDateGroup)}
                                            type="button"
                                          >
                                            {expandLabel}
                                          </button>
                                        ) : (
                                          expandLabel
                                        )}
                                        {showDateMeta && rowError ? <div className="fare-cell-meta">{rowError}</div> : null}
                                      </td>
                                      {route.flight_groups.flatMap((flight) => {
                                        const theme = themeForAirline(flight.airline);
                                        const cell = capture.cells.find((item) => item.flight_group_id === flight.flight_group_id);
                                        const summary = summarizeCell(cell);
                                        const signal = (cell?.signal ?? "unknown") as SignalKey;
                                        return [
                                          <td
                                            className={`report-cell signal-${signal}`}
                                            key={`${capture.captured_at_utc}-${flight.flight_group_id}-min`}
                                            style={{ background: theme.cell, color: theme.text }}
                                          >
                                            <div className="fare-cell-stack">
                                              <div className="fare-cell-main">
                                                <span className="metric-value-text">{summary.minFare}</span>
                                                {signalArrow(signal) ? <span className={`metric-arrow ${signal}`}>{signalArrow(signal)}</span> : null}
                                              </div>
                                              {summary.minFareMeta ? <div className="fare-cell-meta">{summary.minFareMeta}</div> : null}
                                              {fareSignalTag(signal, cell?.soldout) ? (
                                                <div className="fare-cell-tag">{fareSignalTag(signal, cell?.soldout)}</div>
                                              ) : null}
                                            </div>
                                          </td>,
                                          <td
                                            className={`report-cell signal-${signal}`}
                                            key={`${capture.captured_at_utc}-${flight.flight_group_id}-max`}
                                            style={{ background: theme.cell, color: theme.text }}
                                          >
                                            <div className="fare-cell-stack">
                                              <div className="fare-cell-main">
                                                <span className="metric-value-text">{summary.maxFare}</span>
                                              </div>
                                              {summary.maxFareMeta ? <div className="fare-cell-meta">{summary.maxFareMeta}</div> : null}
                                            </div>
                                          </td>,
                                          <td
                                            className="report-cell"
                                            key={`${capture.captured_at_utc}-${flight.flight_group_id}-tax`}
                                            style={{ background: theme.cell, color: theme.text }}
                                          >
                                            {summary.tax}
                                          </td>,
                                          ...(showSeatColumn
                                            ? [
                                                <td
                                                  className="report-cell"
                                                  key={`${capture.captured_at_utc}-${flight.flight_group_id}-seat`}
                                                  style={{ background: theme.cell, color: theme.text }}
                                                >
                                                  {summary.seats}
                                                </td>
                                              ]
                                            : []),
                                          ...(showLoadColumn
                                            ? [
                                                <td
                                                  className="report-cell"
                                                  key={`${capture.captured_at_utc}-${flight.flight_group_id}-load`}
                                                  style={{ background: theme.cell, color: theme.text }}
                                                >
                                                  {summary.load}
                                                </td>
                                              ]
                                            : [])
                                        ];
                                      })}
                                    </tr>
                                  );
                                });
                              })}
                            </tbody>
                          </table>
                        </div>
                      </section>
                    );
                  })}
                </div>
              </section>
            );
          })
        )}
      </div>
    </div>
  );
}

