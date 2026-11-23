"""Manual HAR captures as a source for the schedule and fare views.

Why this exists: not every channel can be fetched live, and a teammate on a
restricted network may not be able to fetch anything at all. A HAR they exported
from their own browser still contains real flights, so it should answer the same
questions the live sources do.

What it deliberately does NOT do: pretend a HAR covers a date range. One capture
is one route on one date, so HAR rows fill the days they actually cover and the
caller is told which those were, rather than leaving a silent hole.

Channel detection reuses discount_engine's `detect_channel` (filename hint, then
endpoint signature) so a teammate never has to label files.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, Optional

from market_engine.rows import FlightRow, from_connector
from market_engine.sources import HAR

#: channel key -> (module, function returning repo-standard offer rows).
#: Only channels with a real flight-row parser appear; BDFare and AKIJ have none,
#: which is exactly why they are marked unusable in the source registry.
PARSERS: dict[str, tuple] = {
    "sharetrip": ("modules.sharetrip_har", "parse_har"),
    "gozayaan": ("modules.gozayaan_har", "parse_har"),
    "firsttrip_b2c": ("modules.firsttrip", "parse_b2c_har"),
    "amy": ("modules.amyweb", "parse_agent_har"),
}


def detect(path: Path) -> Optional[str]:
    """Which channel this HAR belongs to, or None when unrecognized."""
    try:
        from discount_engine.grid import detect_channel
        return detect_channel(Path(path))
    except Exception:            # noqa: BLE001 — an undetectable file is just skipped
        return None


def detect_all(path: Path) -> list:
    """Every flight-search channel in a HAR, first-detected first.

    A teammate may search several sites in one browser session and export once
    ("RT Har.har" held FirstTrip, ShareTrip and GoZayaan searches); naming one
    channel per file silently dropped the other two. Each parser reads only its
    own endpoints, so running several on one file is safe. A filename hint still
    names the one channel, as on the Discounts tab."""
    first = detect(path)
    if first is None:
        return []
    try:
        from discount_engine.grid import _SNIFF_BYTES, FILENAME_HINTS, HAR_SIGNATURES
    except Exception:            # noqa: BLE001 — fall back to the single channel
        return [first]
    name = Path(path).name.lower()
    if any(hint in name for _c, hint in FILENAME_HINTS):
        return [first]
    needles = [(c, n) for c, n in HAR_SIGNATURES if c in PARSERS and c != first]
    found = [first]
    overlap = max((len(n) for _c, n in needles), default=0)
    tail = ""
    try:
        with open(path, encoding="utf-8", errors="ignore") as f:
            while needles:
                chunk = f.read(_SNIFF_BYTES)
                if not chunk:
                    break
                text = tail + chunk
                for c, n in list(needles):
                    if n in text:
                        found.append(c)
                        needles = [x for x in needles if x[0] != c]
                tail = text[-overlap:] if overlap else ""
    except OSError:
        pass
    return found


def find_hars(har_dir: Path) -> list:
    """Top-level .har files only; archives live in subfolders and are history."""
    try:
        return sorted(p for p in Path(har_dir).glob("*.har") if p.is_file())
    except OSError:
        return []


def collect_har_rows(har_dir: Path, *, purpose: str = "fare",
                     channels: Optional[Iterable[str]] = None,
                     progress: Optional[Callable[[str, int, int], None]] = None,
                     ) -> tuple:
    """-> (rows, notes). Notes name every file that contributed nothing, and why.

    Each file is parsed in isolation: one truncated capture skips itself and the
    rest of the run still completes, which is how the discount grid already
    behaves and why a corrupt export is survivable.
    """
    rows: list[FlightRow] = []
    notes: list[str] = []
    files = find_hars(har_dir)
    if not files:
        return rows, ["No .har files in the capture folder."]

    wanted = set(channels) if channels is not None else None
    for i, path in enumerate(files, start=1):
        if progress:
            progress("Reading {}".format(path.name), i, len(files))
        channels = detect_all(path)
        if not channels:
            notes.append("{}: not a recognized channel".format(path.name))
            continue
        for channel in channels:
            rows += _rows_of(path, channel, purpose, wanted, notes, len(channels) > 1)
    return rows, notes


def _rows_of(path: Path, channel: str, purpose: str, wanted: Optional[set],
             notes: list, several: bool) -> list:
    """One channel's rows from one file; why it gave none goes to `notes`."""
    source = HAR.get(channel)
    label = source.label.replace(" (HAR)", "") if source else channel
    name = "{} ({})".format(path.name, label) if several else path.name
    if source is None or channel not in PARSERS:
        notes.append("{}: {} captures carry discounts, not flight details, so they are "
                     "used on the Discounts tab only".format(path.name, label))
        return []
    if wanted is not None and channel not in wanted:
        return []
    can = source.can_schedule if purpose == "schedule" else source.can_fare
    if not can:
        notes.append("{}: not used for the {} - {}".format(
            name, purpose, source.note or "this capture cannot answer it"))
        return []

    module_name, func_name = PARSERS[channel]
    try:
        func = getattr(__import__(module_name, fromlist=[func_name]), func_name)
        parsed = func(str(path)) or []
    except Exception as exc:              # noqa: BLE001 — isolate one bad capture
        notes.append("{}: skipped ({}: {})".format(name, type(exc).__name__, exc))
        return []

    stamp = datetime.fromtimestamp(path.stat().st_mtime)
    out = [r for r in (from_connector(raw, source=source.label, cabin="Economy",
                                      fetched_at=stamp) for raw in parsed) if r]
    if not out:
        notes.append("{}: parsed but produced no usable rows".format(name))
    return out


def select_rows(rows: list, *, routes: Optional[Iterable[tuple]] = None,
                date_from=None, date_to=None, dates=None,
                stay: Optional[int] = None, schedule: bool = False) -> tuple:
    """-> (rows asked for, note or None).

    A capture holds whatever its author searched, so HAR rows must be cut to the
    routes and dates the user asked for - otherwise "DAC-DXB, 1-7 Oct" answered
    with every route in the folder, on whatever day it was captured. Blank routes
    or dates mean "everything the captures cover". When the cut leaves nothing,
    the note says what the captures DO cover, so an empty table explains itself.

    Round-trip fares are kept only for a round-trip run (`stay` = days between
    the flights) and only at that stay: a 9-day and a 2-day round trip are not
    the same fare, and neither is a one-way one. What was left out is said. A
    `schedule` keeps them all: the outbound flight is a real flight whatever the
    fare was.
    """
    rows, trip_note = (rows, None) if schedule else _cut_round_trips(rows, stay)
    kept, note = _select(rows, routes=routes, date_from=date_from, date_to=date_to,
                         dates=dates)
    return kept, " ".join(n for n in (note, trip_note) if n) or None


def _cut_round_trips(rows: list, stay: Optional[int]) -> tuple:
    """-> (rows a run with this `stay` can use, note or None)."""
    trips = [r for r in rows if r.is_round_trip]
    if not trips:
        return rows, None
    if stay is None:
        return ([r for r in rows if not r.is_round_trip],
                "{} round-trip fare(s) in your captures were left out of this one-way "
                "comparison - tick Round trip to use them.".format(len(trips)))
    off = sorted({r.stay_days for r in trips if r.stay_days != stay} - {None})
    keep = [r for r in rows if not r.is_round_trip or r.stay_days == stay]
    if not off:
        return keep, None
    return keep, ("Round-trip captures with a different stay were left out: they return "
                  "{} day(s) after departure, this run asks for {}.".format(
                      ", ".join(str(d) for d in off), stay))


def _select(rows: list, *, routes, date_from, date_to, dates) -> tuple:
    wanted = {(o.upper(), d.upper()) for o, d in (routes or [])}
    days = set(dates) if dates else None    # several ranges: only their days
    kept = []
    for r in rows:
        if wanted and (r.origin, r.destination) not in wanted:
            continue
        if days is not None and r.departure_date not in days:
            continue
        if date_from and date_to and (r.departure_date is None
                                      or not date_from <= r.departure_date <= date_to):
            continue
        kept.append(r)
    if kept or not rows:
        return kept, None
    cov = coverage([r for r in rows if not wanted or (r.origin, r.destination) in wanted])
    if not cov:
        return kept, ("Your HAR captures have none of the routes asked for. They cover: "
                      + _describe(coverage(rows)))
    return kept, "Your HAR captures have no flights in those dates. They cover: " + _describe(cov)


def describe_coverage(rows: Iterable[FlightRow]) -> str:
    """'DAC-DXB (31 Oct); DAC-CXB (30 Sep)' - what a set of HAR rows covers."""
    return _describe(coverage(rows))


def _describe(cov: dict, limit: int = 8) -> str:
    parts = []
    for route, days in sorted(cov.items())[:limit]:
        shown = ", ".join(d.strftime("%d %b") for d in days[:3]) + (" …" if len(days) > 3 else "")
        parts.append(f"{route} ({shown})")
    more = len(cov) - limit
    return "; ".join(parts) + (f"; and {more} more route(s)" if more > 0 else "")


def coverage(rows: Iterable[FlightRow]) -> dict:
    """Which routes and dates the HARs actually cover, so the caller can say so
    instead of implying a capture spans the whole requested range."""
    out: dict = {}
    for r in rows:
        if not r.departure_date:
            continue
        key = "{}-{}".format(r.origin, r.destination)
        out.setdefault(key, set()).add(r.departure_date)
    return {k: sorted(v) for k, v in out.items()}
