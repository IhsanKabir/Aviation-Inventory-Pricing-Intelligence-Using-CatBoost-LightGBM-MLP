"""Collection orchestration: routes x dates x sources -> FlightRows.

Three things this layer owes the operator:

  * AN HONEST ESTIMATE FIRST. A month across a few routes is hundreds of live
    queries at ~10s each. Starting that blind is how a tool gets abandoned, so
    `estimate()` reports what will be fetched vs served from cache, and how long
    it should take, before anything runs.
  * PACE PER SOURCE. Rate limits are per-source-family. Each source runs with its
    own worker count and inter-query pause; exceeding those is what draws blocks.
  * PARTIAL RESULTS. A cancelled or half-failed run still returns every row it
    did get, with the failures named — never an all-or-nothing wipe.
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Callable, Optional

from market_engine import sources as S
from market_engine.cache import FRESH_FARE, FRESH_SCHEDULE, CacheStore
from market_engine.rows import FlightRow

#: Measured on this network: one FirstTrip search ~10s. Used only for the
#: pre-run estimate, and refined by what the run actually observes.
ASSUMED_LATENCY_S = {"firsttrip": 10.0, "sharetrip": 12.0, "biman": 15.0, "amy": 8.0}


@dataclass
class CollectPlan:
    routes: list[tuple[str, str]]
    dates: list[date]
    source_keys: list[str]
    cabin: str = "Economy"
    purpose: str = "fare"          # "schedule" | "fare" -> drives cache freshness
    #: Also read manual HAR captures from the operator's capture folder. HARs
    #: cover the days they were captured on, not the requested range, so they
    #: supplement live rows rather than standing in for them.
    use_har: bool = False

    @property
    def max_age(self) -> timedelta:
        return FRESH_SCHEDULE if self.purpose == "schedule" else FRESH_FARE

    def queries(self) -> list[tuple[str, str, str, str]]:
        return [(k, o, d, day.isoformat())
                for k in self.source_keys for (o, d) in self.routes for day in self.dates]


@dataclass
class CollectResult:
    rows: list[FlightRow] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    cancelled: bool = False
    fetched: int = 0
    from_cache: int = 0
    seconds: float = 0.0

    def tallies(self) -> list[dict]:
        """Per-source tallies in the shape core.field_quality.judge() expects.

        judge() wants aggregates, NOT raw offers — feeding it rows would make
        every row look like its own source.
        """
        by: dict[str, dict] = {}
        for r in self.rows:
            t = by.setdefault(r.source, {"source": r.source, "rows": 0, "with_clock": 0,
                                         "with_base": 0, "with_arrival": 0, "with_seats": 0,
                                         "_airlines": set(), "last_departure": None})
            t["rows"] += 1
            t["with_clock"] += 1 if r.has_clock else 0
            t["with_base"] += 1 if r.base_is_real else 0
            t["with_arrival"] += 1 if r.arrival_time else 0
            t["with_seats"] += 1 if r.seats else 0
            t["_airlines"].add(r.airline)
            if r.departure_date and (t["last_departure"] is None
                                     or r.departure_date > t["last_departure"]):
                t["last_departure"] = r.departure_date
        out = []
        for t in by.values():
            t["airlines"] = tuple(sorted(t.pop("_airlines")))
            out.append(t)
        return out


def estimate(plan: CollectPlan, cache: Optional[CacheStore]) -> dict:
    """What this run will cost, before committing to it."""
    qs = plan.queries()
    cached = 0
    if cache is not None:
        for key, o, d, day in qs:
            if cache.get(S.LIVE[key].label, o, d, day, plan.cabin,
                         max_age=plan.max_age) is not None:
                cached += 1
    to_fetch = len(qs) - cached
    secs = 0.0
    for key in plan.source_keys:
        per = len([1 for k, *_ in qs if k == key])
        share = per * (to_fetch / len(qs)) if qs else 0
        src = S.LIVE[key]
        secs += share * (ASSUMED_LATENCY_S.get(key, 10.0) + src.sleep_s) / max(1, src.workers)
    return {"queries": len(qs), "cached": cached, "to_fetch": to_fetch,
            "est_seconds": int(secs)}


def collect(plan: CollectPlan, cache: Optional[CacheStore] = None,
            progress: Optional[Callable[[str, int, int], None]] = None,
            should_cancel: Optional[Callable[[], bool]] = None) -> CollectResult:
    """Run the plan source by source, respecting each source's pace."""
    res = CollectResult()
    started = time.time()
    qs = plan.queries()
    done = 0

    for key in plan.source_keys:
        src = S.LIVE[key]
        fetcher = S.LiveFetcher(key)
        todo = [(o, d, day) for k, o, d, day in qs if k == key]

        def one(job: tuple[str, str, str]) -> None:
            nonlocal done
            o, d, day = job
            if should_cancel and should_cancel():
                return
            hit = cache.get(src.label, o, d, day, plan.cabin,
                            max_age=plan.max_age) if cache else None
            if hit is not None:
                res.rows.extend(hit)
                res.from_cache += 1
            else:
                rows, reason = fetcher(o, d, day, plan.cabin)
                if rows:
                    res.rows.extend(rows)
                    if cache:
                        cache.put(src.label, o, d, day, plan.cabin, rows)
                elif reason:
                    res.errors.append(f"{src.label} {o}-{d} {day}: {reason}")
                res.fetched += 1
                if src.sleep_s:
                    time.sleep(src.sleep_s)
            done += 1
            if progress:
                progress(f"{src.label} {o}-{d} {day}", done, len(qs))

        if src.workers > 1:
            with ThreadPoolExecutor(max_workers=src.workers) as pool:
                list(pool.map(one, todo))
        else:
            for job in todo:
                one(job)

        if should_cancel and should_cancel():
            res.cancelled = True
            break

    res.seconds = time.time() - started
    return res
