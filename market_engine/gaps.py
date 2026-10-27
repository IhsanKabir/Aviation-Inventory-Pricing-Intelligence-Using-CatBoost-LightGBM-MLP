"""What a fare run is missing, for the workbook's "Missing / Re-run" sheet.

Modelled on the GDS fare report's Re-run Needed sheet: every airline + route the
numbers cannot be fully trusted for, with the reason, so a re-run can target it.
Nothing here is guessed: each line points at something absent from the run or
far outside the rest of the route.
"""
from __future__ import annotations

from dataclasses import dataclass
from statistics import median
from typing import Iterable

from market_engine import taxes as T

#: A fare this many times the route's typical cheapest fare is almost always a
#: premium cabin or a broken itinerary (BG DAC-PEN 600,471 against ~45,000: 13x;
#: SQ / MU / CZ PEN-DAC 9-10x). An expensive but ordinary economy fare (EK via
#: DXB, ~5x on JHB-DAC) stays below it.
OUTLIER_RATIO = 6.0
#: The route median needs a few airlines before "far above it" means anything.
OUTLIER_MIN_AIRLINES = 3


@dataclass(frozen=True)
class Gap:
    route: str
    airline: str                 # "" for a route-wide gap
    kind: str                    # short label, e.g. "one direction"
    detail: str


def outliers(cells: Iterable) -> dict:
    """(route, airline) -> note, for fares far above the route's typical fare."""
    by_route: dict = {}
    for c in cells:
        by_route.setdefault(c.route, []).append(c)
    out = {}
    for route, cs in by_route.items():
        if len(cs) < OUTLIER_MIN_AIRLINES:
            continue
        typical = median(c.gross_low for c in cs)
        if typical <= 0:
            continue
        for c in cs:
            if c.gross_low > OUTLIER_RATIO * typical:
                out[(route, c.airline)] = ("fare {:.0f}x the route's typical {:,.0f} - check "
                                           "cabin / itinerary".format(c.gross_low / typical,
                                                                      typical))
            elif c.gross_high > OUTLIER_RATIO * typical:
                out[(route, c.airline)] = ("some fares up to {:,.0f} ({:.0f}x the route) - "
                                           "check cabin / itinerary".format(
                                               c.gross_high, c.gross_high / typical))
    return out


def _reverse(route: str) -> str:
    o, d = route.split("-", 1)
    return f"{d}-{o}"


def find(table, rows: list, groups: list) -> list:
    """Every gap in a fare run, sorted like the other sheets."""
    gaps: list = []
    cells = list(table.cells)
    routes = {c.route for c in cells}
    airlines = {}
    for c in cells:
        airlines.setdefault(c.route, set()).add(c.airline)

    for route in sorted(routes):
        back = _reverse(route)
        if back not in routes:
            continue
        for airline in sorted(airlines[route] - airlines[back]):
            gaps.append(Gap(route, airline, "one direction",
                            f"fares found {route} but none {back} - re-run {back}"))

    sites = sorted({r.source for r in rows})
    if len(sites) == 1:
        for route in sorted(routes):
            gaps.append(Gap(route, "", "one site",
                            f"only {sites[0]} searched - no site-to-site comparison; "
                            "tick a second site or add its HAR"))
    elif len(sites) > 1:
        seen: dict = {}
        for r in rows:
            seen.setdefault((f"{r.origin}-{r.destination}", r.airline), set()).add(r.source)
        alone: dict = {}                    # (route, site) -> airlines only it had
        for (route, airline), on in seen.items():
            if len(on) == 1:
                alone.setdefault((route, next(iter(on))), []).append(airline)
        for (route, site), names in sorted(alone.items()):
            gaps.append(Gap(route, "", "one site",
                            "only {} had {} - the other sites had no fare for "
                            "{}".format(site, ", ".join(sorted(names)),
                                        "it" if len(names) == 1 else "them")))

    for g in groups:
        via = "via " + g.via.replace("|", "/") if g.via else "direct"
        if g.operated_by:
            via += f" (op. {g.operated_by})"
        if g.status == T.NOT_CAPTURED:
            gaps.append(Gap(g.route, g.airline, "no tax codes",
                            f"{via}: {', '.join(g.sources)} sent the total but no tax "
                            f"breakdown ({g.fares} fare(s))"))
        elif g.status == T.NOT_ITEMISED:
            codes = ", ".join(c for c, *_ in g.codes)
            gaps.append(Gap(g.route, g.airline, "not itemised",
                            f"{via}: whole tax filed under {codes} - codes not comparable"))
        elif g.status == T.CHECK:
            gaps.append(Gap(g.route, g.airline, "check",
                            f"{via}: tax codes do not add up to the total"))

    for (route, airline), note in outliers(cells).items():
        gaps.append(Gap(route, airline, "outlier", note))
    for site, why in (table.refused or {}).items():
        gaps.append(Gap("", "", "site refused", f"{site}: {why}"))

    def key(g: Gap) -> tuple:
        if not g.route:
            return (2,)
        o, d = g.route.split("-", 1)
        return (0,) + T.order_key(o, d, g.airline or "") + (g.kind,)
    return sorted(gaps, key=key)
