"""Fare Comparison (side by side) and Missing / Re-run sheets.

Fare Comparison is the Fares sheet laid out per route with the airlines across,
plus what the list cannot show: the cheapest flight itself (number, day, class,
baggage) and, when the run used 2+ sites, each site's lowest fare so the cheaper
site is visible.
"""
from __future__ import annotations

from typing import Optional

from openpyxl.styles import Alignment, Font

from market_engine.render import (BEST_FILL, HDR_FILL, HDR_FONT, NOTE_FONT, WARN_FILL,
                                  _header, _widths)
from market_engine.taxes import _outbound, is_domestic, order_key

CENTER = Alignment(horizontal="center", wrap_text=True)
SECTION_FONT = Font(bold=True, size=11, color="1F4E79")


def site_lows(rows: list) -> dict:
    """(route, airline) -> {site: lowest gross}."""
    out: dict = {}
    for r in rows:
        if r.gross_bdt <= 0:
            continue
        sites = out.setdefault((f"{r.origin}-{r.destination}", r.airline), {})
        if r.source not in sites or r.gross_bdt < sites[r.source]:
            sites[r.source] = r.gross_bdt
    return out


def _short(site: str) -> str:
    return site.replace(" (HAR)", "")


def _route_key(route: str) -> tuple:
    o, d = route.split("-", 1)
    return order_key(o, d, "")


def travel_dates(rows: list) -> str:
    """The travel days the run holds, as readable ranges (several when split)."""
    from market_engine.dateranges import describe
    return describe(r.departure_date for r in rows if r.departure_date)


def write_fare_matrix(ws, table, best_by_route: dict, title: str,
                      cheapest: Optional[dict] = None, notes: Optional[dict] = None) -> None:
    """Per route: airlines across, figures down; the cheapest airline is green.

    `cheapest`: (route, airline) -> taxes.CheapestFare; `notes`: (route, airline) ->
    a warning (outliers), shown in a Check row."""
    cheapest, notes = cheapest or {}, notes or {}
    rows = list(getattr(table, "rows", ()) or ())
    lows = site_lows(rows)
    sites = sorted({r.source for r in rows})
    ws.cell(1, 1, title).font = Font(bold=True, size=12)
    when = travel_dates(rows)
    ws.cell(2, 1, ("Travel {}. ".format(when) if when else "")
            + "BDT per adult, one-way. Green = cheapest airline on the route. "
              "Base shows — when the site does not give it.").font = NOTE_FONT
    by_route: dict = {}
    for c_ in table.cells:
        by_route.setdefault(c_.route, []).append(c_)

    lines = [("Flights", "flights"), ("Nonstop", "nonstop"), ("Gross low", "gross_low"),
             ("Gross high", "gross_high"), ("Gross avg", "gross_avg"),
             ("Base low", "base_low"), ("Base avg", "base_avg"),
             ("Tax (cheapest)", "tax"), ("YQ+YR (cheapest)", "surcharge"),
             ("Cheapest flight", "flight"), ("Date", "day"), ("Class (RBD)", "rbd"),
             ("Baggage", "baggage"), ("Refundable", "refundable")]
    if len(sites) > 1:
        lines += [(f"Lowest on {_short(s)}", ("site", s)) for s in sites]
        lines += [("Cheapest site", "cheapest_site")]
    lines += [("Sites", "sources"), ("Check", "note")]
    #: shown only when a column on the route has something to say
    optional = {"note", "flight", "day", "rbd", "baggage", "refundable", "cheapest_site"}

    r, widest, section = 4, 1, None
    for route in sorted(by_route, key=_route_key):
        o, d = route.split("-", 1)
        now = "DOMESTIC" if is_domestic(o, d) else "INTERNATIONAL"
        if now != section:
            section = now
            ws.cell(r, 1, section).font = SECTION_FONT
            r += 2
        cells = sorted(by_route[route], key=lambda c_: order_key(o, d, c_.airline))
        ws.cell(r, 1, "{} {}".format(route, "→" if _outbound(o, d) else "←")).font = Font(bold=True)
        r += 1
        hdr = ws.cell(r, 1, "Airline")
        hdr.fill, hdr.font = HDR_FILL, HDR_FONT
        for i, c_ in enumerate(cells, start=2):
            h = ws.cell(r, i, c_.airline)
            h.fill, h.font, h.alignment = HDR_FILL, HDR_FONT, CENTER
        r += 1
        for label, attr in lines:
            values = [_value(c_, attr, cheapest.get((route, c_.airline)),
                             lows.get((route, c_.airline), {}),
                             notes.get((route, c_.airline))) for c_ in cells]
            if attr in optional and not any(v not in (None, "") for v in values):
                continue
            ws.cell(r, 1, label).font = Font(bold=label == "Gross low")
            for i, (c_, value) in enumerate(zip(cells, values), start=2):
                cell = ws.cell(r, i, round(value) if isinstance(value, float) else value)
                cell.alignment = CENTER
                if isinstance(cell.value, (int, float)) and attr not in ("flights", "nonstop"):
                    cell.number_format = "#,##0"
                if attr == "note" and value:
                    cell.fill = WARN_FILL
                elif best_by_route.get(route) == c_.airline:
                    cell.fill = BEST_FILL
            r += 1
        widest = max(widest, len(cells) + 1)
        r += 1
    _widths(ws, [18] + [14] * (widest - 1))
    ws.freeze_panes = "B4"


def _value(c_, attr, fare, lows: dict, note: Optional[str]):
    if attr == "note":
        return note or None
    if attr == "sources":
        # every site that had the airline here, not only the one whose offer was
        # cheapest per flight (which is all the cell's own sources record)
        return ", ".join(_short(s) for s in sorted(lows) or c_.sources)
    if attr == "cheapest_site":
        return _short(min(lows, key=lows.get)) if len(lows) > 1 else None
    if isinstance(attr, tuple):                     # ("site", name)
        return lows.get(attr[1])
    if attr in ("tax", "surcharge", "flight", "day", "rbd", "baggage", "refundable"):
        if fare is None:
            return None
        value = getattr(fare, attr)
        if attr == "day":
            return value.strftime("%d %b") if value else None
        if attr == "refundable":
            return None if value is None else ("yes" if value else "no")
        return value or (0 if attr == "surcharge" and value == 0 else None)
    if attr.startswith("base") and not c_.base_available:
        return "—"
    return getattr(c_, attr)


def write_missing(ws, gaps: list, title: str) -> None:
    ws.cell(1, 1, title).font = Font(bold=True, size=12)
    ws.cell(2, 1, "What this run could not answer, and what to re-run. Filter the "
                  "columns to work through it.").font = NOTE_FONT
    r = _header(ws, 4, ["Route", "Airline", "Missing", "Detail"])
    first = r
    if not gaps:
        ws.cell(r, 1, "Nothing missing.").font = NOTE_FONT
    for g in gaps:
        ws.cell(r, 1, g.route or "-")
        ws.cell(r, 2, g.airline or "all")
        ws.cell(r, 3, g.kind).fill = WARN_FILL
        ws.cell(r, 4, g.detail)
        r += 1
    if gaps:
        ws.auto_filter.ref = f"A{first - 1}:D{r - 1}"
    _widths(ws, [10, 8, 15, 95])
    ws.freeze_panes = ws.cell(first, 1)


def write_site_check(ws, checks: list, title: str, sites: tuple = ()) -> None:
    """One row per flight seen on 2+ sites; each site's fare and tax side by side."""
    from market_engine import taxes as T

    ws.cell(1, 1, title).font = Font(bold=True, size=12)
    ws.cell(2, 1, "The same flight (number, date, departure time) on 2+ sites: each "
                  "site's cheapest fare and the tax on it. Green = cheapest site. A tax "
                  "difference over {:,.0f} BDT (or {:.0f}%) is explained by the booking "
                  "class where both sites send it.".format(T.OTA_TOLERANCE_BDT,
                                                           T.OTA_TOLERANCE_PCT)
            ).font = NOTE_FONT
    seen: list = []
    for c in checks:
        for s, _ in c.grosses:
            if s not in seen:
                seen.append(s)
    ref = checks[0].reference if checks else "FirstTrip"
    order = [ref] + sorted(s for s in seen if s != ref)
    header = (["Route", "Airline", "Flight", "Date", "Dep"]
              + [x for s in order for x in (f"{_short(s)} fare", f"{_short(s)} tax")]
              + ["Cheapest site", "Saves", "Tax differs on", "Likely cause"])
    r = _header(ws, 4, header)
    if not checks:
        why = ("this run used only {} - tick a second site (or add its HAR) for the same "
               "route and date".format(_short(sites[0])) if len(sites) == 1
               else "no flight (same number, date and departure time) was on two sites")
        ws.cell(r, 1, f"Nothing to compare: {why}.").font = NOTE_FONT
    for c in checks:
        for i, v in enumerate((c.route, c.airline, c.flight_number,
                               c.departure_date.isoformat() if c.departure_date else "",
                               c.departure_time), start=1):
            ws.cell(r, i, v)
        cheapest = c.cheapest
        for k, s in enumerate(order):
            col = 6 + 2 * k
            fare, tax = c.gross(s), c.tax(s)
            fc = ws.cell(r, col, round(fare) if fare is not None else None)
            fc.number_format = "#,##0"
            if s == cheapest:
                fc.fill = BEST_FILL
            tc = ws.cell(r, col + 1, round(tax) if tax is not None else None)
            tc.number_format = "#,##0"
            if s in c.flagged:
                tc.fill = WARN_FILL
        col = 6 + 2 * len(order)
        fares = [g for _, g in c.grosses]
        ws.cell(r, col, _short(cheapest))
        ws.cell(r, col + 1, round(max(fares) - min(fares))).number_format = "#,##0"
        if c.flagged:
            ws.cell(r, col + 2, ", ".join(_short(s) for s in c.flagged)).fill = WARN_FILL
            ws.cell(r, col + 3, "; ".join(f"{_short(s)}: {c.cause(s)}" for s in c.flagged))
        else:
            ws.cell(r, col + 3, "same tax")
        r += 1
    _widths(ws, [10, 8, 9, 11, 7] + [12] * (2 * len(order)) + [14, 10, 16, 44])
    ws.freeze_panes = "F5"
