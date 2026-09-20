"""Schedule view: collected rows -> operating pattern per flight.

The heavy lifting (honesty rules, flight-number canonicalisation, source
disagreement, itinerary hold-back) already lives in engines.schedule_view; this
module feeds it correctly and rolls the per-date legs up into the weekly pattern
an operator actually reads.

Two things it refuses to fake:
  * A weekly pattern is only a pattern if the weekdays hold across the weeks in
    range. Airlines retime at the IATA season change (late Oct), so a span that
    crosses one would otherwise average two schedules into a lie -> `varies`.
  * Where sources disagreed on a departure time, the flight is marked rather
    than silently resolved to whichever source was read first.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable, Optional

from core import field_quality as fq
from engines import schedule_view as sv
from market_engine.collect import CollectResult

_DAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


@dataclass(frozen=True)
class Pattern:
    airline: str
    flight: str
    origin: str
    destination: str
    weekdays: tuple                 # ints, 0=Mon
    departure: str
    arrival: str
    aircraft: str
    via: str
    dates: int
    first: date
    last: date
    varies: bool                    # weekday set not constant across FULL weeks
    time_varies: bool               # same flight, different clock on different dates
    disagreement: bool              # sources contradicted each other on one leg
    sources: tuple

    @property
    def weekday_label(self) -> str:
        if len(self.weekdays) == 7:
            return "Daily"
        return "/".join(_DAYS[d] for d in sorted(self.weekdays))

    @property
    def route(self) -> str:
        return f"{self.origin}-{self.destination}"


def assess(result: CollectResult, requested: Optional[Iterable[str]] = None,
           *, purpose: str = "schedule"):
    """Grade the collected sources, then resolve the operator's choice.

    field_quality.judge() wants per-source TALLIES, which CollectResult builds —
    handing it raw rows would make every row look like its own source.
    """
    quality = fq.judge(result.tallies())
    accepted, refused = sv.choose_sources(
        quality, list(requested) if requested is not None else None, purpose=purpose)
    return quality, accepted, refused


def build_schedule(result: CollectResult, *, requested: Optional[Iterable[str]] = None,
                   date_from: Optional[date] = None, date_to: Optional[date] = None,
                   include_itineraries: bool = False) -> sv.ScheduleResult:
    """`include_itineraries=True` keeps connecting services, which a timetable
    needs: on a long-haul market nearly every offer carries a stop, so excluding
    them empties the sheet."""
    quality, accepted, refused = assess(result, requested)
    ok = set(accepted)
    rows = (r.as_schedule_row() for r in result.rows if r.source in ok)
    return sv.build(rows, sources_requested=tuple(accepted), sources_refused=refused,
                    date_from=date_from, date_to=date_to,
                    include_itineraries=include_itineraries)


def _complete_weeks(date_from: Optional[date], date_to: Optional[date]) -> set:
    """ISO (year, week) keys whose whole Mon-Sun sits inside the range.

    A partial week at either edge is an artefact of where the operator cut the
    range, not a schedule change, so it must never feed the variance test.
    """
    if not date_from or not date_to:
        return set()
    seen: dict = {}
    cur = date_from
    while cur <= date_to:
        iso = cur.isocalendar()
        k = (iso[0], iso[1])
        seen[k] = seen.get(k, 0) + 1
        cur += timedelta(days=1)
    return {k for k, n in seen.items() if n == 7}


def weekly_pattern(sched: sv.ScheduleResult, *, date_from: Optional[date] = None,
                   date_to: Optional[date] = None) -> list:
    """Collapse per-date legs into one row per flight, with honest flags."""
    full_weeks = _complete_weeks(date_from or sched.date_from, date_to or sched.date_to)
    groups: dict = {}
    for leg in sched.legs:
        groups.setdefault(
            (leg.airline, leg.flight, leg.origin, leg.destination), []).append(leg)

    out = []
    for (airline, flight, origin, dest), legs in groups.items():
        legs.sort(key=lambda l: l.flight_date)
        weekdays = {l.flight_date.weekday() for l in legs}
        by_week: dict = {}
        for l in legs:
            iso = l.flight_date.isocalendar()
            k = (iso[0], iso[1])
            if k in full_weeks:               # partial edge weeks cannot be judged
                by_week.setdefault(k, set()).add(l.flight_date.weekday())
        varies = len(by_week) > 1 and len({frozenset(w) for w in by_week.values()}) > 1
        times = {l.departure for l in legs if l.departure}
        # Two different facts, kept apart: sources contradicting each other is a
        # data-quality warning; a flight retiming by day is ordinary scheduling.
        disagreement = any(len(l.reported_times) > 1 for l in legs)
        time_varies = len(times) > 1
        srcs = sorted({s for l in legs for s in l.sources})
        out.append(Pattern(
            airline=airline, flight=flight, origin=origin, destination=dest,
            weekdays=tuple(sorted(weekdays)),
            departure=sorted(times)[0] if times else "",
            arrival=next((l.arrival for l in legs if l.arrival), ""),
            aircraft=next((l.aircraft for l in legs if l.aircraft), ""),
            via=next((l.via for l in legs if getattr(l, "via", "")), ""),
            dates=len(legs), first=legs[0].flight_date, last=legs[-1].flight_date,
            varies=varies, time_varies=time_varies, disagreement=disagreement,
            sources=tuple(srcs)))

    out.sort(key=lambda p: (p.route, p.departure, p.airline, p.flight))
    return out
