"""Several date ranges in one Schedule / Fare run (e.g. 1-7 Nov and 15-21 Dec).

A run is a set of DAYS, not one span: everything downstream already works from
the days actually searched, and these helpers turn the form's ranges into that
set and back into readable runs for labels.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Iterable, Optional


def _day(text: str) -> date:
    return datetime.strptime(str(text).strip(), "%Y-%m-%d").date()


def parse(pairs: Iterable) -> tuple:
    """[(from, to), ...] as typed -> (sorted unique days, error or None).

    A pair left wholly blank is ignored (an unused extra row); a half-filled one
    is an error, as is an end before its start. Overlapping ranges are fine: a
    day is searched once."""
    days: set = set()
    for n, pair in enumerate(pairs or [], start=1):
        start_text, end_text = (list(pair) + ["", ""])[:2]
        start_text, end_text = str(start_text or "").strip(), str(end_text or "").strip()
        if not start_text and not end_text:
            continue
        label = "" if n == 1 else f" (range {n})"
        try:
            start, end = _day(start_text), _day(end_text)
        except ValueError:
            return [], f"Dates must be YYYY-MM-DD{label}."
        if end < start:
            return [], f"The end date is before the start date{label}."
        days.update(start + timedelta(days=i) for i in range((end - start).days + 1))
    return sorted(days), None


def runs(days: Iterable[date]) -> list:
    """Sorted days -> [(first, last), ...] of consecutive days."""
    out: list = []
    for d in sorted(set(days)):
        if out and d == out[-1][1] + timedelta(days=1):
            out[-1] = (out[-1][0], d)
        else:
            out.append((d, d))
    return out


def describe(days: Iterable[date]) -> str:
    """'01 Nov - 07 Nov 2026, 15 Dec - 21 Dec 2026' (a single day: '24 Nov 2026')."""
    parts = []
    for first, last in runs(days):
        if first == last:
            parts.append(first.strftime("%d %b %Y"))
        else:
            parts.append("{} - {}".format(first.strftime("%d %b"), last.strftime("%d %b %Y")))
    return ", ".join(parts)


def is_split(days: Optional[Iterable[date]]) -> bool:
    return len(runs(days or [])) > 1
