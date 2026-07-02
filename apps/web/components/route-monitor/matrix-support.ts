import type {
  RouteMonitorFlightGroup,
  RouteMonitorMatrixCell,
  RouteMonitorMatrixRoute
} from "@/lib/api";
import { formatDhakaDate, formatMoney, formatPercent } from "@/lib/format";

export type ViewMode = "context" | "strict";
export type SignalKey = "increase" | "decrease" | "new" | "sold_out" | "unknown";
export type RouteMonitorScopeQuery = {
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

export const SIGNAL_LABELS: Record<SignalKey, string> = {
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

export function themeForAirline(code: string) {
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

export function signalArrow(signal: SignalKey) {
  if (signal === "increase") return "↑";
  if (signal === "decrease") return "↓";
  return "";
}

export function signalTone(signal: SignalKey) {
  if (signal === "increase") return "tone-up";
  if (signal === "decrease") return "tone-down";
  if (signal === "new") return "tone-new";
  if (signal === "sold_out") return "tone-soldout";
  return "tone-neutral";
}

export function summarizeCell(cell: RouteMonitorMatrixCell | undefined) {
  if (!cell) {
    return {
      minFare: "N/O",
      maxFare: "—",
      tax: "—",
      seats: "— / —",
      load: "—",
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
    maxFare: cell.max_total_price_bdt != null ? formatMoney(cell.max_total_price_bdt, "BDT").replace("BDT ", "") : "—",
    tax: cell.tax_amount != null ? formatMoney(cell.tax_amount, "BDT").replace("BDT ", "") : "—",
    seats:
      cell.seat_available != null || cell.seat_capacity != null
        ? `${cell.seat_available ?? "—"} / ${cell.seat_capacity ?? "—"}`
        : "— / —",
    load: formatPercent(cell.load_factor_pct),
    minFareMeta: buildFareMeta(cell.min_booking_class ?? cell.booking_class, cell.min_seat_available ?? cell.seat_available),
    maxFareMeta: buildFareMeta(cell.max_booking_class, cell.max_seat_available)
  };
}

export function fareSignalTag(signal: SignalKey, soldout?: boolean | null) {
  if (soldout || signal === "sold_out") {
    return "SOLD OUT";
  }
  if (signal === "new") {
    return "NEW";
  }
  return null;
}

export function routeLeader(route: RouteMonitorMatrixRoute, visibleFlights: RouteMonitorFlightGroup[]) {
  const visibleSet = new Set(visibleFlights.map((item) => item.flight_group_id));
  const flightsByGroupId = new Map(route.flight_groups.map((item) => [item.flight_group_id, item]));
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
      const flight = flightsByGroupId.get(cell.flight_group_id);
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

export function formatLeaderDates(values: string[]) {
  return values
    .slice()
    .sort()
    .map((item) => formatDhakaDate(`${item}T00:00:00Z`).replace(",", ""))
    .join(", ");
}

export function sortFlightGroups(flightGroups: RouteMonitorFlightGroup[]) {
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

export function tripDateLabel(value?: string | null) {
  if (!value) {
    return "-";
  }
  return formatDhakaDate(`${value}T00:00:00Z`).replace(",", "");
}

export function tripWeekdayLabel(value?: string | null) {
  if (!value) {
    return "-";
  }
  return new Intl.DateTimeFormat("en-US", {
    weekday: "long",
    timeZone: "Asia/Dhaka"
  }).format(new Date(`${value}T00:00:00Z`));
}

export function tripDurationLabel(value?: number | null) {
  if (value == null || Number.isNaN(value)) {
    return "Linked itinerary view";
  }
  return `${value} Day${value === 1 ? "" : "s"}`;
}

export function routeSectionSortValue(route: RouteMonitorMatrixRoute) {
  const outboundOrigin = (route.trip_origin ?? "").trim().toUpperCase();
  const outboundDestination = (route.trip_destination ?? "").trim().toUpperCase();
  const routeOrigin = (route.origin ?? "").trim().toUpperCase();
  const routeDestination = (route.destination ?? "").trim().toUpperCase();
  if (outboundOrigin && outboundDestination && routeOrigin === outboundOrigin && routeDestination === outboundDestination) {
    return 0;
  }
  return 1;
}

export function flightLegLabel(flight: RouteMonitorFlightGroup) {
  if ((flight.search_trip_type ?? "OW") !== "RT") {
    return null;
  }
  const direction = flight.leg_direction === "inbound" ? "Inbound" : "Outbound";
  if (flight.requested_return_date) {
    return `${direction} | inbound ${tripDateLabel(flight.requested_return_date)}`;
  }
  return direction;
}

export function hasRenderableCellData(cell: RouteMonitorMatrixCell) {
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

export function routeHasSeatData(route: RouteMonitorMatrixRoute) {
  return route.date_groups.some((dateGroup) =>
    dateGroup.captures.some((capture) =>
      capture.cells.some((cell) => cell.seat_available != null || cell.seat_capacity != null)
    )
  );
}

export function routeHasLoadData(route: RouteMonitorMatrixRoute) {
  return route.date_groups.some((dateGroup) =>
    dateGroup.captures.some((capture) =>
      capture.cells.some((cell) => cell.load_factor_pct != null)
    )
  );
}

export function dateGroupKey(route: RouteMonitorMatrixRoute, departureDate: string) {
  return [
    route.route_key,
    route.search_trip_type ?? "OW",
    route.trip_pair_key ?? "",
    route.requested_return_date ?? "",
    departureDate
  ].join("|");
}

export function routeMatchesDrilldown(target: RouteMonitorMatrixRoute, candidate: RouteMonitorMatrixRoute) {
  return (
    candidate.route_key === target.route_key &&
    (candidate.search_trip_type ?? "OW") === (target.search_trip_type ?? "OW") &&
    (candidate.trip_pair_key ?? "") === (target.trip_pair_key ?? "") &&
    (candidate.requested_return_date ?? "") === (target.requested_return_date ?? "")
  );
}
