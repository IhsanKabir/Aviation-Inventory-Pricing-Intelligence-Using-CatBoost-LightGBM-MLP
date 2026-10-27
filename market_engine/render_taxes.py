"""Fares by Class and Tax Comparison (with YQ + YR and a legend) sheets of the
Fare Comparison workbook; the OTA Check is in render_compare.

Comparison tables sit SIDE BY SIDE, as in the TravelportAuto fare report: one
block per route, each airline (and each of its routings) in its own column or
table next to the others, so a route reads across instead of down.

A blank code cell means that fare did not carry the code; a fare whose codes
are unknown says so in words ("not captured", or "not sent" for a site that
only ever sends a total), never 0.
"""
from __future__ import annotations

from typing import Iterable, Optional

from openpyxl.styles import Alignment, Font, PatternFill

from market_engine import taxes as T
from market_engine.render import HDR_FILL, HDR_FONT, NOTE_FONT, WARN_FILL, _widths

SECTION_FONT = Font(bold=True, size=11, color="1F4E79")
CENTER = Alignment(horizontal="center", vertical="center", wrap_text=True)
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


def _matrix_cell(g, label: str) -> tuple:
    """(value, warn) for one Tax Comparison cell."""
    missing = NO_CODES.get(g.status)
    if label == "Status":
        return g.status, g.status != T.ITEMISED
    if label == "Fares":
        return g.fares, False
    if label == "Total tax":
        return (_range((g.total_low, g.total_high)) if g.total_high else None), False
    if label == "YQ + YR":
        if g.status != T.ITEMISED:
            return None, False
        both = (g.surcharge_low, g.surcharge_high) if g.surcharge_high is not None else None
        return _range(both), False
    if label == "YQ + YR share":
        # per fare, then the range: a group's highest YQ and highest total can come
        # from different fares
        if g.status != T.ITEMISED or g.share_high is None:
            return None, False
        lo, hi = round(g.share_low * 100), round(g.share_high * 100)
        return (f"{lo}%" if lo == hi else f"{lo}-{hi}%"), False
    return (missing or _partial(g, label)), bool(missing)


def write_comparison(ws, groups: list, title: str, note: str = "",
                     names: Optional[dict] = None) -> None:
    """Per route: tax codes down, airlines side by side across; YQ + YR and its
    share of the tax under each column, and a short legend at the end."""
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
            lines = (["Status", "Fares"] + _route_codes(gs)
                     + ["Total tax", "YQ + YR", "YQ + YR share"])
            for label in lines:
                ws.cell(r, 1, label).font = Font(bold=label in ("Total tax", "YQ", "YR",
                                                                "YQ + YR"))
                for i, g in enumerate(gs, start=2):
                    _put(ws, r, i, *_matrix_cell(g, label))
                    if label == "Total tax":
                        ws.cell(r, i).font = Font(bold=True)
                    ws.cell(r, i).alignment = Alignment(horizontal="center")
                r += 1
            widest = max(widest, len(gs) + 1)
            r += 1
        r += 1
    _legend(ws, r + 1, groups, names or {})
    _widths(ws, [14] + [15] * (widest - 1))
    ws.freeze_panes = "B4"


#: Side-by-side tables per band: a route with 20 routings wraps onto new bands
#: instead of running 100 columns wide.
TABLES_PER_BAND = 6


def _bands(items: list) -> list:
    return [items[i:i + TABLES_PER_BAND] for i in range(0, len(items), TABLES_PER_BAND)]


#: Base with YQ is the column the GDS report highlights: the fare an airline
#: controls (base + its own surcharges), before government taxes.
YQ_FILL = PatternFill("solid", fgColor="FFFF00")
CLASS_COLUMNS = ("RBD", "Base", "YQ+YR", "Base with YQ", "Tax", "Gross")


def write_fares_by_class(ws, tables: list, title: str, note: str = "") -> None:
    """Per route: one table per airline/routing side by side (6 per band), a line
    per booking class - upper cabins, a blank line, then economy, highest fare
    first - and the routing's tax codes under each table."""
    ws.cell(1, 1, title).font = Font(bold=True, size=12)
    sites = sorted({s for t in tables for s in t.sources})
    ws.cell(2, 1, "BDT per adult, one-way, from {}. Each line is the class's cheapest "
                  "fare. Base with YQ = base + the airline's YQ/YR surcharges; Gross = "
                  "what the passenger pays.".format(", ".join(sites) or "the run")
            ).font = NOTE_FONT
    r = max(_banner(ws, 3, note), 4)
    width = len(CLASS_COLUMNS)
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
            # every table of a band gets as many class lines as its longest one,
            # so the "Taxes (BDT)" blocks start on one row and compare across
            depth = max(len(t.lines) + (1 if t.split_cabins else 0) for t in band)
            bottom = r
            for k, t in enumerate(band):
                bottom = max(bottom, _class_table(ws, r, 1 + k * (width + 1), t, depth))
            r = bottom + 2
    _widths(ws, [6, 10, 9, 12, 9, 10, 3] * TABLES_PER_BAND)
    ws.freeze_panes = "A4"


def _class_table(ws, r: int, c: int, t, depth: int = 0) -> int:
    """One airline's table at (r, c), its class lines padded to `depth`; -> the
    last row it used."""
    width = len(CLASS_COLUMNS)
    ws.merge_cells(start_row=r, start_column=c, end_row=r, end_column=c + width - 1)
    head = ws.cell(r, c, f"{t.airline} / {t.route}")
    head.font, head.fill, head.alignment = HDR_FONT, HDR_FILL, CENTER
    ws.merge_cells(start_row=r + 1, start_column=c, end_row=r + 1, end_column=c + width - 1)
    sub = ws.cell(r + 1, c, "{}  ·  {} fare(s)".format(_routing(t), t.fares))
    sub.font, sub.alignment = NOTE_FONT, CENTER
    r += 2
    for i, label in enumerate(CLASS_COLUMNS):
        _head(ws, r, c + i, label)
        if label == "Base with YQ":
            ws.cell(r, c + i).fill = YQ_FILL
            ws.cell(r, c + i).font = Font(bold=True, size=10)
    r += 1
    lines_end = r + depth
    upper_done = False
    for line in t.lines:
        if t.split_cabins and not upper_done and not T.is_upper(line.cabin):
            upper_done = True
            r += 1                                # the blank line between cabins
        ws.cell(r, c, line.rbd).alignment = Alignment(horizontal="center")
        for i, v in enumerate((line.base, line.surcharge, line.base_with_yq, line.tax),
                              start=1):
            _put(ws, r, c + i, round(v) if v is not None else None)
        gross = _put(ws, r, c + 5, round(line.gross))
        gross.font = Font(bold=True)
        r += 1
    r = max(r, lines_end)
    if t.codes or t.total_high:
        r += 1
        ws.cell(r, c, "Taxes (BDT)").font = Font(bold=True, size=9)
        r += 1
        ordered = ([x for x in t.codes if x[0] in T.SURCHARGES]
                   + [x for x in t.codes if x[0] not in T.SURCHARGES])
        for code, lo, hi, n in ordered:
            ws.cell(r, c, code).font = Font(size=9, bold=code in T.SURCHARGES)
            _put(ws, r, c + 1, _range((lo, hi)))
            if n < t.fares:
                ws.cell(r, c + 2, f"{n} of {t.fares}").font = NOTE_FONT
            r += 1
        ws.cell(r, c, "Total").font = Font(bold=True, size=9)
        if t.total_high is not None:
            _put(ws, r, c + 1, _range((t.total_low, t.total_high)))
        if not t.codes:
            ws.cell(r, c + 2, "no tax codes sent").font = NOTE_FONT
        r += 1
    return r - 1


STATUS_MEANING = {
    T.ITEMISED: "codes add up to the fare's total tax",
    T.NOT_CAPTURED: "the site sent a total but no codes - not a zero tax",
    T.NOT_ITEMISED: "the carrier filed its whole tax under one code - not comparable",
    T.CHECK: "the codes do not add up to the total tax",
    T.TOTAL_ONLY: "a site that never sends codes (ShareTrip, GoZayaan): its total only",
}


def _legend(ws, r: int, groups: list, names: dict) -> None:
    """Only what this sheet uses: its statuses, and the codes that have a name."""
    if not groups:
        return
    ws.cell(r, 1, "Legend").font = Font(bold=True)
    r += 1
    for status in [s for s in STATUS_MEANING if any(g.status == s for g in groups)]:
        ws.cell(r, 1, status)
        ws.cell(r, 2, STATUS_MEANING[status]).font = NOTE_FONT
        r += 1
    used = sorted({c for g in groups for c, *_ in g.codes})
    for code in [c for c in used if names.get(c)]:
        ws.cell(r, 1, code)
        ws.cell(r, 2, names[code]).font = NOTE_FONT
        r += 1
    ws.cell(r, 1, "Codes").font = NOTE_FONT
    ws.cell(r, 2, "read by their first two characters (OW2 = OW, YQI = YQ; E5, P7, P8 kept "
                  "as they are). Name other codes in config/tax_codes.json.").font = NOTE_FONT
    r += 1
    ws.cell(r, 1, "→ / ←").font = NOTE_FONT
    ws.cell(r, 2, "leaving Bangladesh (or DAC) / coming back").font = NOTE_FONT


def add_tax_sheets(wb, rows: list, stamp: str, groups: Optional[list] = None) -> None:
    """Tax Comparison, then the OTA Check only when it has content: a sheet that
    can only say "nothing here" is not worth a tab. (Fares by Class is written
    with the fare sheets.)"""
    from market_engine.render_compare import write_site_check

    cfg = T.load_config()
    groups = T.group(rows, cfg["limits"]) if groups is None else groups
    if not groups:
        return
    write_comparison(wb.create_sheet("Tax Comparison"), groups,
                     f"Tax comparison - {stamp}", no_itemising_note(rows), cfg["names"])
    checks = T.ota_check(rows)
    if checks:
        write_site_check(wb.create_sheet("OTA Check"), checks,
                         f"Same flight on each site - {stamp}")
