"""
Collect ROUND-TRIP (7-day return) fares for all report routes -> RT gross CSV.
FirstTrip live, tripTypeId=2 (return = depart + gap). The offer's finalTotalPrice is
the combined round-trip fare, so build_df's Avg is the round-trip average per flight-key.

Usage:
  python tools/collect_rt.py --dates 2026-07-01,... --gap 7 --workers 12 --out output/reports/rt_gross.csv
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import tools.ksa_market_report as R
from modules.firsttrip import fetch_flights as ft_fetch


def all_routes() -> list[tuple[str, str]]:
    routes = [(bd, sa) for bd in R.BD_ORIGINS for sa in R.KSA_DESTS] + \
             [(sa, bd) for sa in R.KSA_DESTS for bd in R.BD_ORIGINS]
    routes += [("DAC", d) for d in R.INTL_DESTS] + [(d, "DAC") for d in R.INTL_DESTS]
    return list(dict.fromkeys(routes))


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2026-07-01")
    p.add_argument("--end", default="2026-08-31")
    p.add_argument("--every", type=int, default=3, help="sample every Nth depart date")
    p.add_argument("--dates", default=None, help="explicit comma depart-date list")
    p.add_argument("--gap", type=int, default=7, help="days between depart and return")
    p.add_argument("--workers", type=int, default=12)
    p.add_argument("--out", required=True)
    args = p.parse_args()

    routes = all_routes()
    if args.dates:
        deps = [x.strip() for x in args.dates.split(",") if x.strip()]
    else:
        deps = list(R._date_range(args.start, args.end))[::max(1, args.every)]
    tasks = [(o, d, dep, cab) for (o, d) in routes for dep in deps
             for cab in ("Economy", "Business")]
    print(f"RT collect: {len(routes)} routes x {len(deps)} departs x 2 cabins = {len(tasks)} queries "
          f"(gap {args.gap}d, {args.workers} workers)")

    def q(o, d, dep, cab):
        dep_d = dep if isinstance(dep, date) else date.fromisoformat(str(dep)[:10])
        dep_s = dep_d.isoformat()
        ret = (dep_d + timedelta(days=args.gap)).isoformat()
        try:
            r = ft_fetch(origin=o, destination=d, date=dep_s, cabin=cab, return_date=ret)
            return o, d, cab, [R._offer_from_row(x) for x in (r.get("rows") or [])]
        except Exception as e:  # noqa: BLE001
            print(f"    ERR {o}->{d} {dep}/{cab}: {e}")
            return o, d, cab, []

    groups: dict = defaultdict(list)
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(q, *t) for t in tasks]
        for f in as_completed(futs):
            o, d, cab, offers = f.result()
            for off in offers:
                off["class"] = cab
                groups[(o, d, off["airline"], off["transit"], off["dep"], cab)].append(off)
            done += 1
            if done % 100 == 0 or done == len(tasks):
                print(f"    {done}/{len(tasks)} RT queries — {len(groups)} flight-options")

    df = R.build_df(groups)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    print(f"WROTE {args.out}  ({len(df)} rows, {df.groupby(['Origin','Destination']).ngroups} routes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
