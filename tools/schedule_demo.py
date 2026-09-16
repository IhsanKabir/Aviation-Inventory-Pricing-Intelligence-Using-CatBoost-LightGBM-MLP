"""Walkthrough of the source-selected schedule view, against the real store.

    python tools/schedule_demo.py --routes DAC-CGP,DAC-CXB \
        --from 2026-06-01 --to 2026-06-21 --airline BS

Shows, in order: what sources exist and whether each is usable, what happens
when you pick a good one, what happens when you pick the degraded one, and the
schedule itself.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine

from core import field_quality as fq
from db import DATABASE_URL
from engines import schedule_view as sv

RULE = "=" * 78


def _d(text: str) -> date:
    y, m, dd = (int(x) for x in text.split("-"))
    return date(y, m, dd)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--routes", default="DAC-CGP,DAC-CXB,DAC-ZYL")
    ap.add_argument("--from", dest="date_from", default="2026-06-01")
    ap.add_argument("--to", dest="date_to", default="2026-06-21")
    ap.add_argument("--airline", default="BS")
    ap.add_argument("--sources", default="", help="comma list; blank = offer all")
    args = ap.parse_args()

    routes = [r.strip().upper() for r in args.routes.split(",") if r.strip()]
    d0, d1 = _d(args.date_from), _d(args.date_to)
    airlines = [a.strip().upper() for a in args.airline.split(",") if a.strip()]

    engine = create_engine(DATABASE_URL, connect_args={"connect_timeout": 8})
    with engine.connect() as conn:
        # ---- 1. what can we load from, and is it any good? --------------
        print(RULE)
        print("STEP 1  Which sources exist, and can they support a schedule?")
        print(RULE)
        quality = sv.fetch_quality(conn)
        for s in quality.sources:
            mark = "OK " if s.schedule_grade else "NO "
            print(f"  [{mark}] {s.explain()}")
        if quality.stale:
            print(f"  ({len(quality.stale)} usable but not collected recently"
                  f" -- they still describe the dates they hold)")
        if quality.clock_regression:
            print()
            print("  !! sources returning rows with no usable departure clock:")
            for s in quality.clock_regression:
                print(f"     - {s.source}  ({s.with_clock:,} of {s.rows:,} timed)")
            print("     These pass every existing gate: rows arrive, so nothing"
                  " fails.")

        # ---- 2. the operator's choice ------------------------------------
        chosen = ([x.strip() for x in args.sources.split(",") if x.strip()]
                  or None)
        print()
        print(RULE)
        print("STEP 2  Resolve the chosen sources against what they can do")
        print(RULE)
        accepted, refused = sv.choose_sources(quality, chosen,
                                              purpose="schedule")
        print(f"  requested : {chosen if chosen else '(offer all)'}")
        short = [a.split(":")[0].split("?")[0][:34] for a in accepted]
        print(f"  accepted  : {len(accepted)} source(s) -> {short or '(none)'}")
        for name, why in refused.items():
            print(f"  refused   : {name}  --  {why}")
        if not accepted:
            print()
            print("  Nothing usable was selected, so no schedule is produced.")
            print("  That is the point: a blank report beats a wrong one.")
            return 1

        # ---- 3. the schedule ---------------------------------------------
        print()
        print(RULE)
        print(f"STEP 3  Schedule for {airlines} {routes}")
        print(f"        {d0} .. {d1}")
        print(RULE)
        rows = sv.fetch_offers(conn, date_from=d0, date_to=d1,
                               sources=accepted, airlines=airlines,
                               routes=routes)
        res = sv.build(rows, sources_requested=chosen or accepted,
                       sources_refused=refused, date_from=d0, date_to=d1)
        print(f"  {res.summary()}")
        print(f"  rows read {res.rows_read:,} · dropped for no clock "
              f"{res.dropped_no_clock:,} · itineraries held back "
              f"{res.itineraries:,} · unreadable flight no. "
              f"{res.dropped_no_flight:,}")
        if res.missing_dates:
            miss = ", ".join(str(d) for d in res.missing_dates[:6])
            more = "" if len(res.missing_dates) <= 6 else \
                f" (+{len(res.missing_dates) - 6} more)"
            print(f"  dates with NO data: {len(res.missing_dates)} -> {miss}{more}")
        print()
        head = (f"  {'date':<11}{'flt':<8}{'route':<10}{'dep':<7}{'arr':<7}"
                f"{'ac':<7}{'seats':<7}{'confidence':<18}sources")
        print(head)
        print("  " + "-" * (len(head) - 2))
        for leg in res.legs[:18]:
            src = ",".join(sorted(x.split(":")[0] for x in leg.sources))
            print(f"  {str(leg.flight_date):<11}{leg.airline + leg.flight:<8}"
                  f"{leg.route:<10}{leg.departure:<7}{leg.arrival or '-':<7}"
                  f"{(leg.aircraft or '-')[:6]:<7}"
                  f"{str(leg.seats if leg.seats is not None else '-'):<7}"
                  f"{leg.confidence:<18}{src}")
        if len(res.legs) > 18:
            print(f"  ... {len(res.legs) - 18:,} more")

        if res.disagreements:
            print()
            print(f"  {len(res.disagreements)} leg(s) where sources disagree on"
                  f" the departure time -- reported, not silently resolved:")
            for leg in res.disagreements[:5]:
                print(f"     {leg.flight_date} {leg.airline}{leg.flight} "
                      f"{leg.route}: {sorted(leg.reported_times)}")

        # ---- 4. the same picker, for fares -------------------------------
        print()
        print(RULE)
        print("STEP 4  The same selection, judged for FARE comparison")
        print(RULE)
        f_ok, f_no = sv.choose_sources(quality, chosen, purpose="fare")
        print(f"  fare-grade : {list(f_ok) or '(none)'}")
        for name, why in f_no.items():
            print(f"  refused    : {name}  --  {why}")
        print()
        print("  Note a source can be fare-grade and NOT schedule-grade: the")
        print("  degraded channel parses base and tax correctly, it only lost")
        print("  the departure clock.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
