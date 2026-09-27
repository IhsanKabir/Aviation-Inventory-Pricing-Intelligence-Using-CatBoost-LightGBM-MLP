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

#: Day-cell states. An OTA stops listing a flight once it is SOLD OUT, so "not
#: seen on a searched day" is not proof it doesn't fly - field case 2026-09-27:
#: VQ921 was missing on the one Friday in range (sold out) and the sheet said it
#: never flies Fridays, while it was on sale every other Friday.
SEEN, UNSURE, NOT_OPERATING, NOT_SEARCHED = "1", "?", "", "-"


def searched_dates(rows: Iterable) -> dict:
    """{(origin, destination): dates the route returned ANY offer} - the days
    that were genuinely searched, whichever airline answered."""
    out: dict = {}
    for r in rows:
        day = getattr(r, "departure_date", None)
        if day:
            out.setdefault((r.origin, r.destination), set()).add(day)
    return out


def weekday_states(seen: set, searched: Optional[set]) -> tuple:
    """Seven states, Monday first, for one flight.

    SEEN          on sale on at least one such day;
    UNSURE        not seen, and that weekday was searched only ONCE - sold out
                  or not flying, the data cannot tell;
    NOT_OPERATING not seen on two or more searched days of that weekday;
    NOT_SEARCHED  no day of that weekday was searched.
    With no search record (older callers), unseen days fall back to NOT_OPERATING.
    """
    out = []
    for wd in range(7):
        if wd in seen:
            out.append(SEEN)
        elif searched is None:
            out.append(NOT_OPERATING)
        else:
            n = sum(1 for d in searched if d.weekday() == wd)
            out.append(NOT_SEARCHED if n == 0 else UNSURE if n == 1 else NOT_OPERATING)
    return tuple(out)


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
    #: weekdays not seen but searched only once (sold out or not flying) and
    #: weekdays not searched at all - both unknown, never "doesn't operate".
    unsure: tuple = ()
    unsearched: tuple = ()

    @property
    def weekday_label(self) -> str:
        """'Daily', 'Mon/Wed/Fri', or with unknowns: 'Mon/Tue/Wed/Thu/Sat/Sun
        (Fri?)' - a '?' day was not confirmed either way."""
        if len(self.weekdays) == 7:
            return "Daily"
        base = "/".join(_DAYS[d] for d in sorted(self.weekdays))
        unknown = [f"{_DAYS[d]}?" for d in sorted(set(self.unsure) | set(self.unsearched))]
        return base + (f" ({'/'.join(unknown)})" if unknown else "")

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
                   date_to: Optional[date] = None,
                   searched: Optional[dict] = None,
                   extra_seen: Optional[dict] = None,
                   extra_searched: Optional[dict] = None) -> list:
    """Collapse per-date legs into one row per flight, with honest flags.
    `searched` = searched_dates(rows): lets an unseen weekday read as unknown
    rather than "doesn't operate" when the data can't support that."""
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
        # Evidence from the same weekday in nearby weeks (market_engine.confirm)
        # settles "?" days; keyed without departure time here, as this view is
        # one row per flight number.
        weekdays = weekdays | (extra_seen or {}).get((airline, flight, origin, dest), set())
        states = weekday_states(weekdays, None if searched is None else (
            set(searched.get((origin, dest), set()))
            | set((extra_searched or {}).get((origin, dest), set()))))
        out.append(Pattern(
            airline=airline, flight=flight, origin=origin, destination=dest,
            weekdays=tuple(sorted(weekdays)),
            departure=sorted(times)[0] if times else "",
            arrival=next((l.arrival for l in legs if l.arrival), ""),
            aircraft=next((l.aircraft for l in legs if l.aircraft), ""),
            via=next((l.via for l in legs if getattr(l, "via", "")), ""),
            dates=len(legs), first=legs[0].flight_date, last=legs[-1].flight_date,
            varies=varies, time_varies=time_varies, disagreement=disagreement,
            sources=tuple(srcs),
            unsure=tuple(i for i, s in enumerate(states) if s == UNSURE),
            unsearched=tuple(i for i, s in enumerate(states) if s == NOT_SEARCHED)))

    out.sort(key=lambda p: (p.route, p.departure, p.airline, p.flight))
    return out
