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
        channel = detect(path)
        if not channel:
            notes.append("{}: not a recognized channel".format(path.name))
            continue
        source = HAR.get(channel)
        if source is None or channel not in PARSERS:
            notes.append("{}: {} has no flight-row parser".format(path.name, channel))
            continue
        if wanted is not None and channel not in wanted:
            continue
        can = source.can_schedule if purpose == "schedule" else source.can_fare
        if not can:
            notes.append("{}: {} cannot answer a {} ({})".format(
                path.name, source.label, purpose, source.note or "unsupported"))
            continue

        module_name, func_name = PARSERS[channel]
        try:
            func = getattr(__import__(module_name, fromlist=[func_name]), func_name)
            parsed = func(str(path)) or []
        except Exception as exc:          # noqa: BLE001 — isolate one bad capture
            notes.append("{}: skipped ({}: {})".format(path.name, type(exc).__name__, exc))
            continue

        stamp = datetime.fromtimestamp(path.stat().st_mtime)
        before = len(rows)
        for raw in parsed:
            row = from_connector(raw, source=source.label, cabin="Economy",
                                 fetched_at=stamp)
            if row:
                rows.append(row)
        if len(rows) == before:
            notes.append("{}: parsed but produced no usable rows".format(path.name))
    return rows, notes


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
