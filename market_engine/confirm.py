"""Confirm "?" weekdays from the same weekday in neighbouring weeks.

An OTA stops listing a flight once it is sold out, so a flight missing on the
ONLY Friday of a range is unknown, not "doesn't fly Fridays" (field case
2026-09-27: VQ921 07:15 DAC-CXB, sold out on 2 Oct, on sale every other Friday).
A timetable describes a WEEKLY pattern, so the same weekday one or two weeks
away is valid evidence - within the same IATA season only, because airlines
retime at the season change (last Sunday of March / of October).

Pure functions; the desktop decides where the extra rows come from (cache,
a short live search for the admin, or the user's own HAR files).
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable, Optional

#: How far to look for evidence, in weeks each way.
WEEKS = 2


def _last_sunday(year: int, month: int) -> date:
    d = date(year, month + 1, 1) - timedelta(days=1) if month < 12 else date(year, 12, 31)
    return d - timedelta(days=(d.weekday() + 1) % 7)


def season(d: date) -> tuple:
    """IATA season key: summer runs from the last Sunday of March to the day
    before the last Sunday of October; winter is the rest."""
    start, end = _last_sunday(d.year, 3), _last_sunday(d.year, 10)
    if start <= d < end:
        return ("S", d.year)
    return ("W", d.year if d >= end else d.year - 1)


def unsure_targets(rows: Iterable, date_from: date, date_to: date, *,
                   today: Optional[date] = None, weeks: int = WEEKS) -> dict:
    """{(origin, dest): sorted dates} to look at for every "?" in the timetable:
    the same weekday up to `weeks` weeks after the range (and before it, if that
    is still in the future), in the same season as the unconfirmed day."""
    today = today or date.today()
    out: dict = {}
    for r in rows:
        for wd, state in enumerate(getattr(r, "day_state", ()) or ()):
            if state != "?":
                continue
            base = next(date_to - timedelta(days=i) for i in range(7)
                        if (date_to - timedelta(days=i)).weekday() == wd)
            first = next(date_from + timedelta(days=i) for i in range(7)
                         if (date_from + timedelta(days=i)).weekday() == wd)
            candidates = [base + timedelta(weeks=k) for k in range(1, weeks + 1)]
            candidates += [first - timedelta(weeks=k) for k in range(1, weeks + 1)]
            keep = {c for c in candidates
                    if c > today and season(c) == season(base)
                    and not date_from <= c <= date_to}
            if keep:
                out.setdefault((r.org, r.dest), set()).update(keep)
    return {k: sorted(v) for k, v in out.items()}


def seen_weekdays(legs: Iterable, *, with_time: bool = True) -> dict:
    """{flight key: weekdays it was on sale}. Key = (airline, flight, origin,
    destination[, departure]) - the timetable matches on departure time too, so a
    retimed service is never confirmed by a different timing."""
    out: dict = {}
    for l in legs:
        key = (str(l.airline).upper(), str(l.flight).strip(),
               str(l.origin).upper(), str(l.destination).upper())
        if with_time:
            key += (l.departure,)
        out.setdefault(key, set()).add(l.flight_date.weekday())
    return out


def confirmations(before: Iterable, after: Iterable) -> list:
    """One note per flight whose "?" days the extra evidence settled, e.g.
    'VQ921 DAC-CXB 0715: flies Fri' / 'BG591 DAC-CXB 0800: no flight Thu/Sun'."""
    names = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
    was = {(r.flight_no, r.org, r.dest, r.dep): r.day_state for r in before}
    notes = []
    for r in after:
        old = was.get((r.flight_no, r.org, r.dest, r.dep))
        if not old:
            continue
        flies = [names[wd] for wd, (a, b) in enumerate(zip(old, r.day_state))
                 if a == "?" and b == "1"]
        no = [names[wd] for wd, (a, b) in enumerate(zip(old, r.day_state))
              if a == "?" and b == ""]
        parts = ([f"flies {'/'.join(flies)}"] if flies else []) \
            + ([f"no flight {'/'.join(no)}"] if no else [])
        if parts:
            notes.append(f"{r.flight_no} {r.org}-{r.dest} {r.dep}: " + "; ".join(parts))
    return notes
