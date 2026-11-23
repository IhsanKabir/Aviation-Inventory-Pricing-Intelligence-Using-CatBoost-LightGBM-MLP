"""Excel output for the schedule and fare views.

openpyxl only (already bundled for the discount grid); pandas/xlsxwriter are
excluded from the desktop build.

The sheets carry their own caveats — refused sources, held-back itineraries,
derived bases, data age — because a workbook gets mailed around long after the
screen that explained it is gone.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

HDR_FILL = PatternFill("solid", fgColor="1F4E79")
HDR_FONT = Font(color="FFFFFF", bold=True, size=10)
NOTE_FONT = Font(color="9C6500", size=9, italic=True)
BEST_FILL = PatternFill("solid", fgColor="C6EFCE")
WARN_FILL = PatternFill("solid", fgColor="FFF2CC")
#: A round-trip line, under its one-way line in the same columns.
RT_FONT = Font(color="1F4E79", italic=True)


def _header(ws, row: int, labels: Iterable[str]) -> int:
    for i, text in enumerate(labels, start=1):
        c = ws.cell(row, i, text)
        c.fill, c.font = HDR_FILL, HDR_FONT
        c.alignment = Alignment(horizontal="center")
    return row + 1


def _widths(ws, widths: Iterable[int]) -> None:
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w


def write_schedule(ws, patterns, sched, title: str) -> None:
    ws.cell(1, 1, title).font = Font(bold=True, size=12)
    r = 2
    notes = []
    if sched.itineraries:
        notes.append(f"{sched.itineraries} connecting itineraries held back "
                     "(a marketed origin-destination is not an operated leg)")
    if sched.dropped_no_clock:
        notes.append(f"{sched.dropped_no_clock} rows excluded — no real departure time")
    if sched.dropped_no_flight:
        notes.append(f"{sched.dropped_no_flight} rows excluded — no readable flight number")
    for src, why in (sched.sources_refused or {}).items():
        notes.append(f"source refused: {src} — {why}")
    for n in notes:
        ws.cell(r, 1, n).font = NOTE_FONT
        r += 1
    r += 1

    r = _header(ws, r, ["Route", "Airline", "Flight", "Operates", "Dep", "Arr",
                        "Dates", "Sources", "Flags"])
    for p in patterns:
        flags = []
        if p.varies:
            flags.append("weekday pattern changes in range")
        if p.time_varies:
            flags.append("retimes by day")
        if p.disagreement:
            flags.append("sources disagree on time")
        ws.cell(r, 1, p.route)
        ws.cell(r, 2, p.airline)
        ws.cell(r, 3, p.flight)
        ws.cell(r, 4, p.weekday_label)
        ws.cell(r, 5, p.departure)
        ws.cell(r, 6, p.arrival)
        ws.cell(r, 7, p.dates)
        ws.cell(r, 8, ", ".join(p.sources))
        c = ws.cell(r, 9, "; ".join(flags))
        if flags:
            c.fill = WARN_FILL
        r += 1
    _widths(ws, [10, 8, 8, 24, 7, 7, 7, 18, 34])
    ws.freeze_panes = ws.cell(r - len(patterns), 1)


FARE_COLUMNS = ["Route", "Airline", "Flights", "Nonstop", "Dates",
                "Gross low", "Gross high", "Gross avg",
                "Base low", "Base high", "Base avg",
                "Tax (cheapest)", "YQ+YR (cheapest)", "Data age (h)", "Flags"]


def round_trip_label(table) -> str:
    """'Round trip - return 7 days after departure (price of both flights)'."""
    stay = getattr(table, "stay", None)
    when = f" - return {stay} day(s) after departure" if stay is not None else ""
    return f"ROUND TRIP{when} (price of both flights)"


def write_fares(ws, table, best_by_route: dict, title: str,
                cheapest_tax: Optional[dict] = None, notes: Optional[dict] = None,
                rt_table=None, rt_best: Optional[dict] = None,
                rt_cheapest_tax: Optional[dict] = None) -> None:
    """`cheapest_tax`: (route, airline) -> (tax, YQ+YR) of that pair's cheapest fare
    (market_engine.taxes.cheapest_fare_tax); the columns stay blank without it.
    `notes`: (route, airline) -> a warning for the Flags column (outliers).
    `rt_table` (round trips): each airline's round trip is the line right under its
    one-way line, in the same columns, with a Trip column saying which is which."""
    from market_engine.fares import pair_trips

    cheapest_tax, notes = cheapest_tax or {}, notes or {}
    rt = rt_table is not None and bool(rt_table.cells)
    ws.cell(1, 1, title).font = Font(bold=True, size=12)
    r = 2
    if rt:
        ws.cell(r, 1, "OW = one-way; RT = {}, under the airline's one-way line."
                      .format(round_trip_label(rt_table).lower())).font = NOTE_FONT
        r += 1
    if table.gross_only:
        ws.cell(r, 1, "gross only (base not trustworthy from these sources): "
                      + ", ".join(table.gross_only)).font = NOTE_FONT
        r += 1
    for src, why in (table.refused or {}).items():
        ws.cell(r, 1, f"source refused: {src} — {why}").font = NOTE_FONT
        r += 1
    r += 1

    cols = FARE_COLUMNS[:2] + ["Trip"] + FARE_COLUMNS[2:] if rt else FARE_COLUMNS
    r = _header(ws, r, cols)
    by_trip = {"OW": (best_by_route, cheapest_tax, notes),
               "RT": (rt_best or {}, rt_cheapest_tax or {}, {})}
    for trip, c_ in pair_trips(table.cells, rt_table.cells if rt else ()):
        best, taxes_, notes_ = by_trip[trip]
        _fare_row(ws, r, c_, best, taxes_, notes_, trip if rt else None)
        r += 1
    widths = [10, 8, 8, 9, 7, 11, 11, 11, 10, 10, 10, 13, 15, 13, 50]
    _widths(ws, widths[:2] + [6] + widths[2:] if rt else widths)


def _fare_row(ws, r: int, c_, best_by_route: dict, cheapest_tax: dict, notes: dict,
              trip: Optional[str]) -> None:
    """One (route, airline) cell on row r; `trip` ("OW"/"RT") fills the Trip
    column, None when the sheet has none."""
    flags = []
    if not c_.base_available:
        flags.append("base unavailable")
    elif c_.base_derived:
        flags.append("base partly derived")
    if c_.age_hours > 24:
        flags.append(f"{c_.age_hours/24:.0f}d old")
    if notes.get((c_.route, c_.airline)):
        flags.append(notes[(c_.route, c_.airline)])
    money = lambda v: round(v) if v is not None else None          # noqa: E731
    base = lambda v: round(v) if c_.base_available else "—"  # noqa: E731
    tax, yqyr = cheapest_tax.get((c_.route, c_.airline), (None, None))
    values = ([c_.route, c_.airline] + ([trip] if trip else [])
              + [c_.flights, c_.nonstop, c_.dates,
                 money(c_.gross_low), money(c_.gross_high), money(c_.gross_avg),
                 base(c_.base_low), base(c_.base_high), base(c_.base_avg),
                 money(tax), money(yqyr), round(c_.age_hours, 1), "; ".join(flags)])
    first_money = len(values) - 9            # Gross low .. YQ+YR are the money columns
    for i, v in enumerate(values, start=1):
        cell = ws.cell(r, i, v)
        if first_money <= i < first_money + 8 and isinstance(v, (int, float)):
            cell.number_format = "#,##0"
    if trip == "RT":
        for i in range(1, len(values) + 1):
            ws.cell(r, i).font = RT_FONT
    if flags:
        ws.cell(r, len(values)).fill = WARN_FILL
    if best_by_route.get(c_.route) == c_.airline:
        for i in range(1, len(values) + 1):
            if ws.cell(r, i).fill.patternType is None:
                ws.cell(r, i).fill = BEST_FILL


def write_timetable(ws, rows, title: str, confirmed: Optional[list] = None) -> None:
    """The operator's schedule sheet: sector-grouped, day flags, HHMM times.

    Sector is merged down its block because it labels a market, not a row, and
    both directions of that market sit inside it. Unknown seat capacity is left
    EMPTY rather than filled with a guess -- every load factor built on this
    column would inherit the error.
    """
    from market_engine.timetable import DAY_COLUMNS

    ws.cell(1, 1, title).font = Font(bold=True, size=12)
    # An OTA drops a flight once it is sold out, so an unseen day is only a
    # "no" when it was searched on 2+ dates. Say what ? and – mean whenever used.
    states = {s for row in rows for s in (row.day_state or ())}
    if "?" in states or "-" in states:
        ws.cell(2, 1, "1 = on sale that weekday   ? = not on sale on the ONLY such day "
                      "searched (sold out, or not flying) - widen the date range to 2+ "
                      "weeks to confirm   – = that weekday was not in the date range   "
                      "Freq. counts confirmed days only").font = NOTE_FONT
    header = (["Sector"] + list(DAY_COLUMNS)
              + ["Freq.", "Airline", "Aircraft", "Flight no.", "Org", "1Stop",
                 "Dest", "Dep", "Arr", "Seat Capacity", "Operated by"])
    r = _header(ws, 3, header)

    first_row = r
    for row in rows:
        ws.cell(r, 1, row.sector)
        day_state = row.day_state or tuple("1" if on else "" for on in row.days)
        for i, state in enumerate(day_state):
            value = {"1": 1, "?": "?", "-": "–"}.get(state)
            c = ws.cell(r, 2 + i, value)
            c.alignment = Alignment(horizontal="center")
            if state == "?":
                c.fill = WARN_FILL                # unknown, never a silent "no"
            elif state == "-":
                c.font = NOTE_FONT
        ws.cell(r, 9, row.freq).alignment = Alignment(horizontal="center")
        ws.cell(r, 10, row.airline)
        ws.cell(r, 11, row.aircraft)
        ws.cell(r, 12, row.flight_no)
        ws.cell(r, 13, row.org).alignment = Alignment(horizontal="center")
        ws.cell(r, 14, row.stop or None).alignment = Alignment(horizontal="center")
        ws.cell(r, 15, row.dest).alignment = Alignment(horizontal="center")
        # Times are labels, not numbers: 0745 must keep its leading zero.
        for col, value in ((16, row.dep), (17, row.arr)):
            c = ws.cell(r, col, value or None)
            c.alignment = Alignment(horizontal="center")
            c.number_format = "@"
        cap = ws.cell(r, 18, row.seats if row.seats else None)
        cap.alignment = Alignment(horizontal="center")
        if not row.seats:
            cap.fill = WARN_FILL          # visibly unknown, not silently zero
        # Only a codeshare fills this: naming BG as BG's operator is noise.
        if row.operated_by:
            ws.cell(r, 19, "{} ({})".format(row.operated_by_name or row.operated_by,
                                            row.operated_by))
        r += 1

    # Merge each sector block so the sheet reads as markets, like the original.
    start = first_row
    for i in range(first_row, r + 1):
        at_end = i == r
        changed = (not at_end
                   and ws.cell(i, 1).value != ws.cell(start, 1).value)
        if at_end or changed:
            if i - start > 1:
                ws.merge_cells(start_row=start, start_column=1,
                               end_row=i - 1, end_column=1)
                ws.cell(start, 1).alignment = Alignment(horizontal="center",
                                                        vertical="center")
            start = i
    # Days that were "?" in the range and settled from the same weekday in a
    # nearby week: say so under the table, so a 1 is never unexplained.
    if confirmed:
        ws.cell(r + 1, 1, "Settled from the same weekday in a nearby week "
                          "(the flight was not on sale on the only such day in range):"
                ).font = NOTE_FONT
        for i, line in enumerate(confirmed, start=2):
            ws.cell(r + i, 1, "  " + line).font = NOTE_FONT
    _widths(ws, [10, 4, 4, 4, 4, 4, 4, 4, 7, 22, 18, 11, 6, 7, 6, 7, 7, 14, 22])
    ws.freeze_panes = ws.cell(first_row, 1)


def write_workbook(path: Path, *, patterns=None, sched=None, table=None,
                   best_by_route: Optional[dict] = None, timetable=None,
                   title: str = "", confirmed: Optional[list] = None,
                   rows: Optional[list] = None, rt_table=None) -> Path:
    """`rows`: the fare rows behind `table`; with them the tax sheets are added.
    `rt_table`: the run's round-trip fares (fares.build_fares(trip="RT"))."""
    wb = Workbook()
    wb.remove(wb.active)
    stamp = title or datetime.now().strftime("%d %b %Y %H:%M")
    if sched is not None:
        write_schedule(wb.create_sheet("Schedule"), patterns or [], sched,
                       f"Airline schedule — {stamp}")
    if timetable is not None:
        write_timetable(wb.create_sheet("Timetable"), timetable,
                        f"Schedule timetable — {stamp}", confirmed=confirmed)
    if table is not None:
        _fare_sheets(wb, table, best_by_route or {}, rows, stamp, rt_table)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(str(path))
    return path


def _fare_sheets(wb, table, best_by_route: dict, rows: Optional[list], stamp: str,
                 rt_table=None) -> None:
    """Fares, Fare Comparison, Missing / Re-run, then the tax sheets (with rows).
    Round trips (`rt_table`) go under the one-way figures and into the RT columns
    of Fares by Class; the tax sheets stay one-way (a round trip's codes are both
    flights' added together)."""
    from market_engine import gaps, taxes
    from market_engine.fares import cheapest_by_route
    from market_engine.render_compare import write_fare_matrix, write_missing

    limits = taxes.load_config()["limits"]
    cheapest = taxes.cheapest_fares(rows or [], limits)
    notes = gaps.outliers(table.cells)
    rt_rows = list(rt_table.rows) if rt_table is not None else []
    rt_cheapest = taxes.cheapest_fares(rt_rows, limits) if rt_rows else {}
    rt_best = cheapest_by_route(rt_table.cells) if rt_table is not None else {}
    write_fares(wb.create_sheet("Fares"), table, best_by_route,
                f"Fare comparison — {stamp}",
                cheapest_tax={k: (c.tax, c.surcharge) for k, c in cheapest.items()},
                notes=notes, rt_table=rt_table, rt_best=rt_best,
                rt_cheapest_tax={k: (c.tax, c.surcharge) for k, c in rt_cheapest.items()})
    write_fare_matrix(wb.create_sheet("Fare Comparison"), table, best_by_route,
                      f"Fare comparison side by side — {stamp}", cheapest=cheapest,
                      notes=notes, rt_table=rt_table, rt_best=rt_best,
                      rt_cheapest=rt_cheapest)
    if rows is None:
        return
    from market_engine.render_taxes import add_tax_sheets, write_fares_by_class
    classes = taxes.by_class(rows, limits, rt_rows=rt_rows)
    if classes:
        write_fares_by_class(wb.create_sheet("Fares by Class"), classes,
                             f"Fares by class — {stamp}",
                             stay=getattr(rt_table, "stay", None))
    groups = taxes.group(rows, limits)
    missing = gaps.find(table, rows, groups) + gaps.round_trip(table, rt_table)
    if missing:                       # a sheet that says "nothing missing" is no use
        write_missing(wb.create_sheet("Missing - Re-run"), missing,
                      f"Missing / re-run — {stamp}")
    add_tax_sheets(wb, rows, stamp, groups=groups)
