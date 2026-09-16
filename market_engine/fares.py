"""Fare view: collected rows -> what each airline charges on a route.

Deliberate choices, each of which changes the numbers:

  * CHEAPEST PER FLIGHT FIRST. One departure is returned many times, once per
    fare class (a single SQ service came back three times at three prices).
    Averaging those would measure an airline's fare-class ladder, not the
    market. So each distinct flight collapses to its cheapest offer, and only
    then are flights aggregated.
  * BASE IS DERIVED, NOT INVENTED. Some sources hide the base fare. Tax is
    learned from the offers that DO expose one (most specific key first) and
    subtracted; any cell containing a derived base says so, because a derived
    base must never be read as an observed one.
  * NO pandas. The desktop build excludes pandas/numpy to keep the exe small,
    so this is stdlib only.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from statistics import median
from typing import Iterable, Optional

from core import field_quality as fq
from engines import schedule_view as sv
from market_engine.collect import CollectResult
from market_engine.rows import FlightRow


@dataclass
class FareTable:
    """Cells plus WHY each source did or did not contribute."""
    cells: list
    refused: dict               # source -> reason it contributed nothing
    gross_only: tuple           # contributed an observed gross, but no trustworthy base


@dataclass(frozen=True)
class FareCell:
    route: str
    airline: str
    flights: int
    dates: int
    gross_low: float
    gross_high: float
    gross_avg: float
    base_low: float
    base_high: float
    base_avg: float
    base_available: bool        # False when no source here could support a base
    base_derived: bool          # any base in this cell was derived, not observed
    nonstop: int                # how many of the counted flights were nonstop
    sources: tuple
    as_of: datetime             # oldest row behind this cell

    @property
    def age_hours(self) -> float:
        return (datetime.now() - self.as_of).total_seconds() / 3600.0


def learn_tax(rows: Iterable[FlightRow]) -> dict:
    """tax = gross - base, learned only from offers that exposed a real base.

    Keyed most-specific-first so an airline with a route-specific tax is not
    smeared by a global average. Median, not mean: one odd offer should not move
    the model.
    """
    buckets: dict = {}
    for r in rows:
        if not r.base_is_real or r.gross_bdt <= 0:
            continue
        tax = r.gross_bdt - r.base_bdt
        if tax < 0:
            continue
        for key in ((r.origin, r.destination, r.airline, r.cabin),
                    (r.airline, r.cabin), (r.airline,), ("*",)):
            buckets.setdefault(key, []).append(tax)
    return {k: median(v) for k, v in buckets.items()}


def resolve_base(row: FlightRow, model: dict) -> tuple:
    """-> (base_bdt, is_real). Falls back through the key hierarchy."""
    if row.base_is_real:
        return row.base_bdt, True
    for key in ((row.origin, row.destination, row.airline, row.cabin),
                (row.airline, row.cabin), (row.airline,), ("*",)):
        tax = model.get(key)
        if tax is not None:
            return max(0.0, row.gross_bdt - tax), False
    return 0.0, False


def cheapest_per_flight(rows: Iterable[FlightRow]) -> list:
    """Collapse fare classes: one row per (airline, flight, date, route)."""
    best: dict = {}
    for r in rows:
        k = (r.airline, r.flight_number, r.departure_date, r.origin, r.destination)
        cur = best.get(k)
        if cur is None or r.gross_bdt < cur.gross_bdt:
            best[k] = r
    return list(best.values())


def build_fares(result: CollectResult, *, requested: Optional[Iterable[str]] = None,
                direct_only: bool = False) -> FareTable:
    """One cell per (route, airline).

    A source that hides the base fare is NOT discarded: its gross is fully
    observed and gross is the number a market comparison is actually about.
    Such a source contributes gross only, and cells that rest on it say their
    base is unavailable rather than showing a figure modelled from nothing.
    """
    quality = fq.judge(result.tallies())
    accepted, refused = sv.choose_sources(
        quality, list(requested) if requested is not None else None, purpose="fare")
    ok = set(accepted)
    # Refused-but-populated sources still answer "what does it cost".
    gross_only = tuple(sorted(s.source for s in quality.sources
                              if s.rows and s.source not in ok))
    contributing = ok | set(gross_only)
    for name in gross_only:
        refused.pop(name, None)

    usable = [r for r in result.rows if r.source in contributing and r.gross_bdt > 0]
    if direct_only:
        usable = [r for r in usable if (r.stops or 0) == 0]
    # Tax is learned only where base is trustworthy.
    model = learn_tax([r for r in usable if r.source in ok])

    groups: dict = {}
    for r in cheapest_per_flight(usable):
        groups.setdefault((f"{r.origin}-{r.destination}", r.airline), []).append(r)

    cells = []
    for (route, airline), rs in groups.items():
        gross = [r.gross_bdt for r in rs]
        based = [r for r in rs if r.source in ok]
        bases, derived = [], False
        for r in based:
            b, real = resolve_base(r, model)
            if b > 0:
                bases.append(b)
            derived = derived or not real
        base_available = bool(bases)
        bases = bases or [0.0]
        cells.append(FareCell(
            route=route, airline=airline, flights=len(rs),
            dates=len({r.departure_date for r in rs}),
            gross_low=min(gross), gross_high=max(gross),
            gross_avg=round(sum(gross) / len(gross), 2),
            base_low=min(bases), base_high=max(bases),
            base_avg=round(sum(bases) / len(bases), 2),
            base_available=base_available, base_derived=derived,
            nonstop=sum(1 for r in rs if (r.stops or 0) == 0),
            sources=tuple(sorted({r.source for r in rs})),
            as_of=min(r.fetched_at for r in rs)))

    cells.sort(key=lambda c: (c.route, c.gross_low))
    return FareTable(cells=cells, refused=refused, gross_only=gross_only)


def cheapest_by_route(cells: Iterable[FareCell]) -> dict:
    """route -> the airline with the lowest gross_low (for highlighting)."""
    out: dict = {}
    for c in cells:
        cur = out.get(c.route)
        if cur is None or c.gross_low < cur.gross_low:
            out[c.route] = c
    return {r: c.airline for r, c in out.items()}
