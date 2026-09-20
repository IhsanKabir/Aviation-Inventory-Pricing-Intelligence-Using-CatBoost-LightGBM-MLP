"""Timetable view: the operator's schedule sheet layout.

One row per flight leg, grouped by SECTOR (the foreign endpoint), with both
directions of a market under the same group -- BKK covers TG339 BKK-DAC and
TG340 DAC-BKK together, because an operator reads a market, not a direction.

Two things are looked up rather than computed, and both stay blank when unknown:

  * Airline NAME. The feed gives a code; the sheet wants "Thai Airways".
  * SEAT CAPACITY. This is per AIRLINE AND AIRCRAFT, never per aircraft alone --
    the same 737 seats 162 at Biman and 189 at US-Bangla. Feeds report seats
    REMAINING (5, 7, 9...), which is not capacity, so it is never substituted.

Aircraft arrives spelled inconsistently ("ATR 72", "ATR 72 - 600", "ATR725"),
so lookups normalize before matching.

A timetable lists every SERVICE on sale, so connections belong in it: on DAC-JED
270 of 271 offers carry a stop, and excluding them would leave the sheet nearly
empty. 1Stop names the via point and is blank only for a genuine nonstop. Build
the schedule with `include_itineraries=True` to feed this view.

Codeshares are listed as sold -- EK 2331 is a real service a passenger can buy --
with the operating carrier named, because who flies the metal decides the cabin,
the capacity and the ground handling.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

#: Monday-first, matching the sheet's M T W T F S S columns.
DAY_COLUMNS = ("M", "T", "W", "T", "F", "S", "S")

#: Home base. The sector is whichever endpoint is NOT home, so both directions
#: of a market group together. Overridable in config/market_settings.json.
HOME = "DAC"

#: Shortest aircraft key allowed to match by prefix. "78" cannot tell a 788 from
#: a 789, and guessing between them is a ~10% capacity error.
_MIN_MATCH = 3


def _config_dirs() -> list:
    """Where a lookup table may live, most specific first.

    The operator's own copy must genuinely win -- these tables are meant to be
    edited (a new carrier, a re-configured cabin) without rebuilding the app.
    A CWD-relative "config" is deliberately NOT searched: the exe's working
    directory is wherever the user launched it from, so it could pick up an
    unrelated folder.
    """
    dirs = []
    try:                                  # the desktop app's editable config dir
        from desktop.backend import config_dir
        dirs.append(Path(config_dir()))
    except Exception:                     # noqa: BLE001 - engine also runs headless
        pass
    import sys
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:                           # bundled inside the exe
        dirs.append(Path(meipass) / "config")
    dirs.append(Path(__file__).resolve().parents[1] / "config")
    return dirs


def _load(name: str) -> dict:
    """Operator's copy wins over the bundled one; a missing file is not fatal."""
    for base in _config_dirs():
        path = base / name
        try:
            if path.is_file():
                data = json.loads(path.read_text(encoding="utf-8"))
                return {k: v for k, v in data.items() if not str(k).startswith("_")}
        except (OSError, ValueError):
            continue
    return {}


def _vias(text: Any) -> str:
    """'SIN|DOH' -> 'SIN/DOH'. Every stop is kept.

    The column is headed 1Stop because one is the common case, but a two-stop
    service must not be silently reported as a one-stop: showing both is the
    only honest option.
    """
    parts = [p.strip().upper() for p in re.split(r"[|,/>\s]+", str(text or "")) if p.strip()]
    return "/".join(parts)


def normalize_aircraft(text: Any) -> str:
    """'ATR 72 - 600' -> 'ATR72600'; 'Boeing-787' -> 'BOEING787'.

    Separators are dropped entirely because feeds place them inconsistently --
    'Boeing-787' and 'Boeing 787-9' would otherwise never meet.
    """
    return re.sub(r"[^A-Z0-9]", "", str(text or "").upper())


def airline_name(code: str, names: Optional[dict] = None) -> str:
    names = _load("airline_names.json") if names is None else names
    return str(names.get(str(code).upper()) or code or "")


def _fleet_cache() -> dict:
    """Capacities scraped from the airlines' own fleet pages.

    modules.fleet_mapping refreshes cache/fleet_capacity_cache.json from each
    carrier's published fleet page (VQ and BG have parsers today). Reading it
    here means anything that pipeline learns appears in the timetable without a
    second copy being maintained by hand.
    """
    for base in (Path(__file__).resolve().parents[1] / "cache", Path("cache")):
        path = base / "fleet_capacity_cache.json"
        try:
            if path.is_file():
                data = json.loads(path.read_text(encoding="utf-8"))
                return data.get("capacity_map") or {}
        except (OSError, ValueError):
            continue
    return {}


def capacity_table() -> dict:
    """Scraped fleet data, overlaid by the operator's file.

    The hand-maintained file wins: it is where a confirmed configuration goes
    when the scraped or general-knowledge figure is wrong for this operator.
    """
    merged: dict = {}
    for source in (_fleet_cache(), _load("seat_capacity.json")):
        for airline, fleet in (source or {}).items():
            if isinstance(fleet, dict):
                merged.setdefault(str(airline).upper(), {}).update(fleet)
    return merged


def seat_capacity(airline: str, aircraft: str,
                  table: Optional[dict] = None) -> Optional[int]:
    """Seats for this operator's configuration of this type, or None.

    None means "not known", and the sheet leaves the cell empty. Guessing a
    capacity would silently corrupt every load/revenue calculation built on it.
    """
    table = capacity_table() if table is None else table
    by_air = {normalize_aircraft(k): v
              for k, v in (table.get(str(airline).upper()) or {}).items()}
    key = normalize_aircraft(aircraft)
    if not key:
        return None
    if key in by_air:
        return int(by_air[key])

    # A sub-variant may match a configured family ("ATR72600" against "ATR72"),
    # but only when the answer is unambiguous. A feed that says merely
    # "Boeing-787" could mean a 787-8, -9 or -10, and those differ by 80+ seats;
    # picking one would put a confident wrong number into every load factor
    # computed from this column. Short keys cannot disambiguate anything.
    # Containment, not just prefix: a scraped "Q400" has to reach a feed's
    # "Dash 8-Q400", and a configured "BOEING737800" a feed's bare "737".
    candidates = [(known, int(seats)) for known, seats in by_air.items()
                  if len(known) >= _MIN_MATCH and len(key) >= _MIN_MATCH
                  and (known in key or key in known)]
    if not candidates:
        return None
    seats = {s for _, s in candidates}
    if len(seats) == 1:                  # every candidate agrees
        return seats.pop()
    return None                          # genuinely ambiguous -> leave it blank


@dataclass(frozen=True)
class TimetableRow:
    sector: str
    days: tuple            # 7 bools, Monday first
    freq: int
    airline: str           # full name
    airline_code: str
    aircraft: str
    flight_no: str         # carrier + number, e.g. TG339
    org: str
    stop: str              # via airport, blank when nonstop
    dest: str
    operated_by: str       # operating carrier on a codeshare, else ""
    operated_by_name: str  # its full name, for the sheet
    dep: str               # HHMM
    arr: str               # HHMM
    seats: Optional[int]


def _hhmm(clock: str) -> str:
    """'07:45' -> '0745'. The sheet uses 4-digit times with no separator.

    Matches the CLOCK rather than stripping punctuation: a full timestamp fed in
    by mistake used to yield '2026' -- a plausible-looking time that is really a
    year, which is far worse than an empty cell.
    """
    text = str(clock or "").strip()
    m = re.search(r"(\d{1,2}):(\d{2})", text)
    if m:
        return "{:02d}{}".format(int(m.group(1)), m.group(2))
    digits = re.sub(r"\D", "", text)
    return digits.rjust(4, "0") if len(digits) in (3, 4) else ""


def _sector(origin: str, destination: str, home: str = HOME,
            bases: Optional[set] = None) -> str:
    """The market, so both directions group together.

    A sector names the FOREIGN point. Keying only on the home airport broke the
    ex-base routes that are already in the route presets: CGP-JED came out as
    "CGP-JED" instead of JED. So any configured home-side airport counts, and
    the sector is the endpoint that is not one of them.
    """
    origin, destination = str(origin).upper(), str(destination).upper()
    if bases is None:
        bases = {str(a).upper() for a in (_load("market_settings.json").get("bases") or [])}
    bases = bases or {home}
    o_home, d_home = origin in bases, destination in bases
    if o_home and not d_home:
        return destination
    if d_home and not o_home:
        return origin
    # Both home-side (a domestic pair) -> the end that is not the home base.
    if o_home and d_home:
        return destination if origin == home else origin
    return "{}-{}".format(origin, destination)   # neither end is home-side


def build_timetable(sched: Any, *, home: str = HOME) -> list:
    """Operated legs -> timetable rows, sorted by sector then departure.

    Built from the LEGS rather than the weekly pattern on purpose. A flight that
    retimes during the range has two real timings, and a timetable shows each on
    its own line with its own day pattern. Collapsing them to one row produced
    impossible pairs -- a 07:30 departure against a 16:00 arrival taken from a
    different day.
    """
    names = _load("airline_names.json")
    caps = capacity_table()
    settings = _load("market_settings.json")
    home = str(settings.get("home") or home).upper()
    bases = {str(a).upper() for a in (settings.get("bases") or [])} or {home}

    groups: dict = {}
    for leg in getattr(sched, "legs", sched):
        # Aircraft is part of the identity: a flight flown by a 777 on Tuesday and
        # a 787 on Thursday has two different seat counts, and collapsing them
        # would attach one aircraft's capacity to both days.
        key = (str(leg.airline).upper(), str(leg.flight).strip(),
               str(leg.origin).upper(), str(leg.destination).upper(),
               leg.departure, leg.arrival, str(getattr(leg, "aircraft", "") or ""),
               _vias(getattr(leg, "via", "")),
               str(getattr(leg, "operated_by", "") or "").upper())
        groups.setdefault(key, []).append(leg)

    rows = []
    for (code, number, org, dest, dep, arr, aircraft, via, operator), legs in groups.items():
        days = tuple(any(l.flight_date.weekday() == i for l in legs) for i in range(7))
        rows.append(TimetableRow(
            sector=_sector(org, dest, home, bases),
            days=days,
            freq=sum(days),
            airline=airline_name(code, names),
            airline_code=code,
            aircraft=aircraft,
            # The feed's number is canonical ('339'); the sheet wants TG339.
            flight_no="{}{}".format(code, number) if number.isdigit() else number,
            org=org,
            stop=via,
            dest=dest,
            operated_by=operator,
            operated_by_name=airline_name(operator, names) if operator else "",
            dep=_hhmm(dep),
            arr=_hhmm(arr),
            seats=seat_capacity(code, aircraft, caps),
        ))
    rows.sort(key=lambda r: (r.sector, r.dep, r.airline_code, r.flight_no))
    return rows


def coverage_gaps(rows: Iterable[TimetableRow]) -> dict:
    """What the sheet could not fill, so the gap is visible and fixable.

    Returns {"seats": [(airline, aircraft), ...], "names": [code, ...]} -- each
    is one line to add to config/seat_capacity.json or airline_names.json.
    """
    seats, names = set(), set()
    for r in rows:
        if r.seats is None and r.aircraft:
            seats.add((r.airline_code, r.aircraft))
        if r.airline == r.airline_code:          # fell back to the raw code
            names.add(r.airline_code)
    return {"seats": sorted(seats), "names": sorted(names)}
