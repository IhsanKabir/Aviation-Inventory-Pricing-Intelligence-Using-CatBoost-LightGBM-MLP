"use client";

import { Fragment } from "react";

import type { RouteMonitorMatrixDateGroup, RouteMonitorMatrixRoute } from "@/lib/api";
import { formatDhakaDateTime, formatRouteGeo, formatRouteType } from "@/lib/format";

import type { SignalKey } from "./matrix-support";
import {
  dateGroupKey,
  fareSignalTag,
  flightLegLabel,
  formatLeaderDates,
  routeHasLoadData,
  routeHasSeatData,
  routeLeader,
  signalArrow,
  summarizeCell,
  themeForAirline
} from "./matrix-support";

interface RouteReportBlockProps {
  route: RouteMonitorMatrixRoute;
  loadedDateGroups: Record<string, RouteMonitorMatrixDateGroup>;
  expandedRows: Record<string, boolean>;
  loadingRows: Record<string, boolean>;
  rowErrors: Record<string, string>;
  onToggleRow: (route: RouteMonitorMatrixRoute, dateGroup: RouteMonitorMatrixDateGroup) => void;
}

export function RouteReportBlock({
  route,
  loadedDateGroups,
  expandedRows,
  loadingRows,
  rowErrors,
  onToggleRow
}: RouteReportBlockProps) {
  const leader = routeLeader(route, route.flight_groups);
  const showSeatColumn = routeHasSeatData(route);
  const showLoadColumn = routeHasLoadData(route);

  return (
    <section className="route-report-block">
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

      <div
        className="route-report-scroll"
        role="region"
        aria-label={`Fare matrix for ${route.route_key}`}
        tabIndex={0}
      >
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
                      <span>{flight.departure_time || "—"}</span>
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
                // O(1) lookup per flight column instead of scanning cells per cell render.
                const cellsByFlightGroup = new Map(
                  capture.cells.map((item) => [item.flight_group_id, item])
                );
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
                          onClick={() => onToggleRow(route, effectiveDateGroup)}
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
                      const cell = cellsByFlightGroup.get(flight.flight_group_id);
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
}
