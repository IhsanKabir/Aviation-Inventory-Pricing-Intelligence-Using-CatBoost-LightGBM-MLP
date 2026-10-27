"""Tax sheets of the Fare Comparison workbook: Tax Comparison, Tax by Class, Tax
Breakdown, YQ-YR Charges and Legend (the OTA Check is in render_compare).

Comparison tables sit SIDE BY SIDE, as in the TravelportAuto fare report: one
block per route, each airline (and each of its routings) in its own column or
table next to the others, so a route reads across instead of down.

A blank code cell means that fare did not carry the code; a fare whose codes
are unknown says so in words ("not captured", or "not sent" for a site that
only ever sends a total), never 0.
"""
from __future__ import annotations

from typing import Iterable, Optional

from openpyxl.styles import Alignment, Font

from market_engine import taxes as T
from market_engine.render import (HDR_FILL, HDR_FONT, NOTE_FONT, WARN_FILL,
                                  _header, _widths)

SECTION_FONT = Font(bold=True, size=11, color="1F4E79")
CENTER = Alignment(horizontal="center", vertical="center", wrap_text=True)
STATUS_NOTES = {
    T.ITEMISED: "",
    T.NOT_CAPTURED: "taxes not sent by the source",
    T.NOT_ITEMISED: "whole tax filed under one code",
    T.CHECK: "codes do not add up to the total - check",
    T.TOTAL_ONLY: "this site sends a tax total only, no codes",
}
#: Code cells of a fare whose codes are unknown: said in words, never 0.
NO_CODES = {T.NOT_CAPTURED: T.NOT_CAPTURED, T.TOTAL_ONLY: "not sent"}


def no_itemising_note(rows: list) -> str:
    """Why the code cells are empty when no site in the run itemises taxes."""
    if not rows or any(r.taxes for r in rows):
        return ""
    sites = ", ".join(sorted({r.source for r in rows}))
    return ("No tax codes in this run: only FirstTrip sends them, and this run used "
            f"{sites}. Tick FirstTrip (or add a FirstTrip search HAR) and run again for "
            "YQ/YR and the code breakdown. Totals shown are as each site gave them.")


def _banner(ws, row: int, note: str) -> int:
    """Write the warning line if there is one; -> the next free row."""
    if not note:
        return row
    cell = ws.cell(row, 1, note)
    cell.font, cell.fill = Font(bold=True, color="9C6500"), WARN_FILL
    return row + 2


def _range(rng: Optional[tuple]):
    """A single amount as a number (sums in Excel); a spread as 'low-high' text."""
    if rng is None:
        return None
    lo, hi = round(rng[0]), round(rng[1])
    return lo if lo == hi else f"{lo:,}-{hi:,}"


def _put(ws, r: int, c: int, value, warn: bool = False):
    cell = ws.cell(r, c, value)
    if isinstance(value, (int, float)):
        cell.number_format = "#,##0"
    if warn:
        cell.fill = WARN_FILL
    return cell


def _head(ws, r: int, c: int, value) -> None:
    cell = ws.cell(r, c, value)
    cell.fill, cell.font, cell.alignment = HDR_FILL, HDR_FONT, CENTER


def _sections(groups: list) -> Iterable[tuple]:
    for title, domestic in (("DOMESTIC", True), ("INTERNATIONAL", False)):
        part = [g for g in groups if g.domestic == domestic]
        if part:
            yield title, part


def _by_route(groups: list) -> list:
    """[(route, [groups...]), ...] in the groups' (already sorted) order."""
    out: list = []
    for g in groups:
        if out and out[-1][0] == g.route:
            out[-1][1].append(g)
        else:
            out.append((g.route, [g]))
    return out


def _arrow(g) -> str:
    return "→" if T._outbound(g.origin, g.destination) else "←"


def _via(g) -> str:
    return g.via.replace("|", "/") if g.via else "Direct"


def _routing(g) -> str:
    """Second header line under the airline: routing, operator, site if total-only."""
    parts = [_via(g) if not g.via else "via " + _via(g)]
    if g.operated_by:
        parts.append("op. " + g.operated_by)
    if getattr(g, "status", None) == T.TOTAL_ONLY and g.sources:
        parts.append("(" + ", ".join(g.sources) + ")")
    return " ".join(parts)


def _partial(g, code: str):
    """A code's amount, saying "(1 of 3)" when only some of the fares carry it."""
    value = _range(g.code_range(code))
    n = g.carried_by(code)
    if value is None or n >= g.fares:
        return value
    shown = f"{value:,}" if isinstance(value, int) else value
    return f"{shown} ({n} of {g.fares})"


def _route_codes(part: list) -> list:
    """Matrix rows for one route: YQ, YR, then only the codes this route carries,
    the most widely charged first (an inbound route has no Bangladesh departure
    taxes, so BD/UT/E5... would be seven empty rows above the ones that matter)."""
    carried: dict = {}
    for g in part:
        for c, *_ in g.codes:
            carried[c] = carried.get(c, 0) + 1
    rest = sorted((c for c in carried if c not in T.SURCHARGES),
                  key=lambda c: (-carried[c], c))
    return list(T.SURCHARGES) + rest


def write_comparison(ws, groups: list, title: str, note: str = "") -> None:
    """Per route: tax codes down, airlines side by side across."""
    ws.cell(1, 1, title).font = Font(bold=True, size=12)
    ws.cell(2, 1, "Adult taxes per fare, BDT. Each column is one airline and routing; "
                  "a range is low-high across its fares, \"(1 of 3)\" = only some of them "
                  "carry it. Blank = not on those fares.").font = NOTE_FONT
    r = _banner(ws, 3, note)
    r = max(r, 4)
    if not groups:
        ws.cell(r, 1, "No fares with taxes in this run.").font = NOTE_FONT
    widest = 1
    for section, part in _sections(groups):
        ws.cell(r, 1, section).font = SECTION_FONT
        r += 2
        for route, gs in _by_route(part):
            ws.cell(r, 1, f"{route} {_arrow(gs[0])}").font = Font(bold=True)
            r += 1
            _head(ws, r, 1, "Airline")
            _head(ws, r + 1, 1, "Routing")
            for i, g in enumerate(gs, start=2):
                _head(ws, r, i, g.airline)
                _head(ws, r + 1, i, _routing(g))
            r += 2
            lines = (["Status", "Fares"] + _route_codes(gs) + ["Total tax"])
            for label in lines:
                ws.cell(r, 1, label).font = Font(bold=label in ("Total tax", "YQ", "YR"))
                for i, g in enumerate(gs, start=2):
                    missing = NO_CODES.get(g.status)
                    if label == "Status":
                        _put(ws, r, i, g.status, warn=g.status != T.ITEMISED)
                    elif label == "Fares":
                        ws.cell(r, i, g.fares)
                    elif label == "Total tax":
                        cell = _put(ws, r, i, _range((g.total_low, g.total_high))
                                    if g.total_high else None)
                        cell.font = Font(bold=True)
                    else:
                        _put(ws, r, i, missing or _partial(g, label), warn=bool(missing))
                    ws.cell(r, i).alignment = Alignment(horizontal="center")
                r += 1
            widest = max(widest, len(gs) + 1)
            r += 1
        r += 1
    _widths(ws, [14] + [15] * (widest - 1))
    ws.freeze_panes = "B4"


#: Side-by-side tables per band: a route with 20 routings wraps onto new bands
#: instead of running 100 columns wide.
TABLES_PER_BAND = 6
TABLE_WIDTH = 4                              # Code | Low | High | Fares


def _bands(items: list) -> list:
    return [items[i:i + TABLES_PER_BAND] for i in range(0, len(items), TABLES_PER_BAND)]


def write_breakdown(ws, groups: list, title: str, note: str = "") -> None:
    """Per route: one table per airline/routing, side by side, a blank column between.

    Every table has the same head (title, routing, status, header row) so the
    tables of a band line up row for row."""
    ws.cell(1, 1, title).font = Font(bold=True, size=12)
    r = _banner(ws, 2, note)
    r = max(r, 3)
    if not groups:
        ws.cell(r, 1, "No fares with taxes in this run.").font = NOTE_FONT
    for section, part in _sections(groups):
        ws.cell(r, 1, section).font = SECTION_FONT
        r += 2
        for route, gs in _by_route(part):
            ws.cell(r, 1, f"{route} {_arrow(gs[0])}").font = Font(bold=True)
            r += 1
            for band in _bands(gs):
                bottom = r
                for k, g in enumerate(band):
                    bottom = max(bottom, _breakdown_table(ws, r, 1 + k * (TABLE_WIDTH + 1), g))
                r = bottom + 2
    _widths(ws, [9, 10, 10, 7, 3] * TABLES_PER_BAND)


def _breakdown_lines(g) -> list:
    """(label, low, high, fares, bold) rows: YQ, YR, their subtotal, then A-Z."""
    lines = []
    for code in T.SURCHARGES:
        rng = g.code_range(code)
        if rng:
            lines.append((code, rng[0], rng[1], g.carried_by(code), False))
    if lines:
        lines.append(("Charges", g.surcharge_low, g.surcharge_high, None, True))
    for code, lo, hi, n in g.codes:
        if code not in T.SURCHARGES:
            lines.append((code, lo, hi, n, False))
    return lines


def _breakdown_table(ws, r: int, c: int, g) -> int:
    """One airline's table at (r, c); -> the last row it used."""
    ws.cell(r, c, f"{g.airline} / {g.route}").font = Font(bold=True)
    ws.cell(r + 1, c, _routing(g)).font = NOTE_FONT
    if STATUS_NOTES.get(g.status):            # always this row, so heads align
        _put(ws, r + 2, c, STATUS_NOTES[g.status], warn=True)
    r += 3
    for i, label in enumerate(("Code", "Low", "High", "Fares")):
        _head(ws, r, c + i, label)
    r += 1
    if g.status in NO_CODES:
        _put(ws, r, c, NO_CODES[g.status], warn=True)
        r += 1
    for label, lo, hi, n, bold in _breakdown_lines(g):
        ws.cell(r, c, label).font = Font(bold=bold)
        if lo is not None:
            _put(ws, r, c + 1, round(lo))
            _put(ws, r, c + 2, round(hi))
        if n is not None:
            ws.cell(r, c + 3, n)
        r += 1
    ws.cell(r, c, "Total").font = Font(bold=True)
    if g.total_high:
        _put(ws, r, c + 1, round(g.total_low), warn=g.status != T.ITEMISED)
        _put(ws, r, c + 2, round(g.total_high), warn=g.status != T.ITEMISED)
    ws.cell(r, c + 3, g.fares)
    if g.rbds:
        r += 1
        ws.cell(r, c, "RBD").font = NOTE_FONT
        ws.cell(r, c + 1, ", ".join(g.rbds)).font = NOTE_FONT
    if g.flight_numbers:
        r += 1
        ws.cell(r, c, "Flights").font = NOTE_FONT
        ws.cell(r, c + 1, ", ".join(g.flight_numbers)).font = NOTE_FONT
    return r


def write_by_class(ws, tables: list, title: str, note: str = "") -> None:
    """Per route: one table per airline/routing, a line per booking class, highest
    fare first - YQ follows the class, which a routing-wide range hides."""
    ws.cell(1, 1, title).font = Font(bold=True, size=12)
    ws.cell(2, 1, "From the site that itemises taxes (FirstTrip). Gross / base = the "
                  "class's cheapest fare; YQ+YR and tax as low-high across its "
                  "fares.").font = NOTE_FONT
    r = _banner(ws, 3, note)
    r = max(r, 4)
    if not tables:
        ws.cell(r, 1, "No booking classes in this run.").font = NOTE_FONT
    width = 5                                  # RBD | Gross | Base | YQ+YR | Tax
    section = None
    by_route: list = []
    for t in tables:
        if by_route and by_route[-1][0] == t.route:
            by_route[-1][1].append(t)
        else:
            by_route.append((t.route, [t]))
    for route, ts in by_route:
        now = "DOMESTIC" if T.is_domestic(ts[0].origin, ts[0].destination) else "INTERNATIONAL"
        if now != section:
            section = now
            ws.cell(r, 1, section).font = SECTION_FONT
            r += 2
        ws.cell(r, 1, f"{route} {_arrow(ts[0])}").font = Font(bold=True)
        r += 1
        for band in _bands(ts):
            bottom = r
            for k, t in enumerate(band):
                bottom = max(bottom, _class_table(ws, r, 1 + k * (width + 1), t))
            r = bottom + 2
    _widths(ws, [6, 10, 10, 11, 12, 3] * TABLES_PER_BAND)


def _class_table(ws, r: int, c: int, t) -> int:
    ws.cell(r, c, f"{t.airline} / {t.route}").font = Font(bold=True)
    ws.cell(r + 1, c, _routing(t)).font = NOTE_FONT
    r += 2
    for i, label in enumerate(("RBD", "Gross", "Base", "YQ+YR", "Tax")):
        _head(ws, r, c + i, label)
    r += 1
    for line in t.lines:
        ws.cell(r, c, line.rbd).alignment = Alignment(horizontal="center")
        _put(ws, r, c + 1, round(line.gross_low))
        _put(ws, r, c + 2, round(line.base_low) if line.base_low else None)
        both = ((line.surcharge_low, line.surcharge_high)
                if line.surcharge_high is not None else None)
        _put(ws, r, c + 3, _range(both))
        taxes = (line.tax_low, line.tax_high) if line.tax_high is not None else None
        _put(ws, r, c + 4, _range(taxes))
        r += 1
    return r - 1


def write_surcharges(ws, groups: list, title: str, note: str = "") -> None:
    """Per route: YQ / YR / share down, airlines side by side across."""
    ws.cell(1, 1, title).font = Font(bold=True, size=12)
    ws.cell(2, 1, "YQ / YR are set by the airline, not by any government - the part "
                  "of the tax an airline controls.").font = NOTE_FONT
    r = _banner(ws, 3, note)
    r = max(r, 4)
    if not groups:
        ws.cell(r, 1, "No fares with taxes in this run.").font = NOTE_FONT
    widest = 1
    for section, part in _sections(groups):
        ws.cell(r, 1, section).font = SECTION_FONT
        r += 2
        for route, gs in _by_route(part):
            ws.cell(r, 1, f"{route} {_arrow(gs[0])}").font = Font(bold=True)
            r += 1
            _head(ws, r, 1, "Airline")
            _head(ws, r + 1, 1, "Routing")
            for i, g in enumerate(gs, start=2):
                _head(ws, r, i, g.airline)
                _head(ws, r + 1, i, _routing(g))
            r += 2
            for label in ("YQ", "YR", "YQ + YR", "Total tax", "YQ + YR share"):
                ws.cell(r, 1, label).font = Font(bold=label == "YQ + YR")
                for i, g in enumerate(gs, start=2):
                    ws.cell(r, i).alignment = Alignment(horizontal="center")
                    if label == "Total tax":
                        _put(ws, r, i, _range((g.total_low, g.total_high))
                             if g.total_high else None)
                    elif g.status != T.ITEMISED:
                        # its codes are unknown or untrustworthy: say which, once
                        if label == "YQ":
                            _put(ws, r, i, g.status, warn=True)
                    elif label in ("YQ", "YR"):
                        _put(ws, r, i, _range(g.code_range(label)))
                    elif label == "YQ + YR":
                        both = ((g.surcharge_low, g.surcharge_high)
                                if g.surcharge_high is not None else None)
                        _put(ws, r, i, _range(both))
                    elif g.share_high is not None:
                        # per fare, then the range: a group's highest YQ and highest
                        # total can come from different fares
                        lo, hi = round(g.share_low * 100), round(g.share_high * 100)
                        ws.cell(r, i, f"{lo}%" if lo == hi else f"{lo}-{hi}%")
                r += 1
            widest = max(widest, len(gs) + 1)
            r += 1
        r += 1
    _widths(ws, [14] + [15] * (widest - 1))
    ws.freeze_panes = "B4"


def write_legend(ws, groups: list, names: dict) -> None:
    ws.cell(1, 1, "Legend").font = Font(bold=True, size=12)
    r = _header(ws, 3, ["Code", "Name"])
    seen = sorted({c for g in groups for c, *_ in g.codes} | set(T.FIXED_CODES))
    for code in seen:
        ws.cell(r, 1, code)
        name = names.get(code)
        cell = ws.cell(r, 2, name or "(not named yet - add it in config/tax_codes.json)")
        if not name:
            cell.font = NOTE_FONT
        r += 1
    r += 1
    ws.cell(r, 1, "Folding").font = Font(bold=True)
    ws.cell(r, 2, "Codes are read by their first two characters (OW2 = OW, YQI = YQ, "
                  "PZ + PZ2 summed). Codes such as E5, P7, P8 are kept as they are.")
    r += 1
    ws.cell(r, 1, "→ / ←").font = Font(bold=True)
    ws.cell(r, 2, "leaving Bangladesh (or DAC) / coming back")
    r += 2
    ws.cell(r, 1, "Status").font = Font(bold=True)
    r += 1
    for status, meaning in ((T.ITEMISED, "codes add up to the fare's total tax"),
                            (T.NOT_CAPTURED, "the source sent a total but no codes - "
                                             "not a zero tax"),
                            (T.NOT_ITEMISED, "the carrier filed its whole tax under one "
                                             "code, so the codes are not comparable"),
                            (T.CHECK, "the codes do not add up to the total tax"),
                            (T.TOTAL_ONLY, "a site that never sends codes (ShareTrip, "
                                           "GoZayaan): its tax total, shown only where "
                                           "FirstTrip had no fare for that airline")):
        ws.cell(r, 1, status)
        ws.cell(r, 2, meaning)
        r += 1
    _widths(ws, [14, 90])


def add_tax_sheets(wb, rows: list, stamp: str, groups: Optional[list] = None) -> None:
    from market_engine.render_compare import write_site_check

    cfg = T.load_config()
    groups = T.group(rows, cfg["limits"]) if groups is None else groups
    note = no_itemising_note(rows)
    write_comparison(wb.create_sheet("Tax Comparison"), groups,
                     f"Tax comparison - {stamp}", note)
    write_by_class(wb.create_sheet("Tax by Class"), T.by_class(rows, cfg["limits"]),
                   f"Fare and tax by booking class - {stamp}", note)
    write_breakdown(wb.create_sheet("Tax Breakdown"), groups,
                    f"Tax breakdown - {stamp}", note)
    write_surcharges(wb.create_sheet("YQ-YR Charges"), groups,
                     f"YQ / YR carrier surcharges - {stamp}", note)
    write_site_check(wb.create_sheet("OTA Check"), T.ota_check(rows),
                     f"Same flight on each site - {stamp}",
                     tuple(sorted({r.source for r in rows})))
    write_legend(wb.create_sheet("Legend"), groups, cfg["names"])
