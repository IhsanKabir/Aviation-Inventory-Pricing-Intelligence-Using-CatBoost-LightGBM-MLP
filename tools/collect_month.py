"""
Collect one month of fares for ALL report routes -> gross CSV.
Sources: FirstTrip + Biman live, plus any OTA/ShareTrip cache entries matching the dates.

Usage:
  python tools/collect_month.py --start 2026-07-01 --end 2026-07-31 --out output/reports/july_gross.csv
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import tools.ksa_market_report as R


def all_routes() -> list[tuple[str, str]]:
    routes = [(bd, sa) for bd in R.BD_ORIGINS for sa in R.KSA_DESTS] + \
             [(sa, bd) for sa in R.KSA_DESTS for bd in R.BD_ORIGINS]
    routes += [("DAC", d) for d in R.INTL_DESTS] + [(d, "DAC") for d in R.INTL_DESTS]
    return list(dict.fromkeys(routes))


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2026-07-01")
    p.add_argument("--end", default="2026-07-31")
    p.add_argument("--dates", default=None, help="explicit comma list (overrides start/end range)")
    p.add_argument("--every", type=int, default=0, help="sample every Nth day of the range")
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--no-biman", action="store_true", help="FirstTrip only (faster; BG still via FirstTrip)")
    p.add_argument("--out", required=True)
    args = p.parse_args()

    routes = all_routes()
    if args.dates:
        dates = [x.strip() for x in args.dates.split(",") if x.strip()]
    else:
        dates = list(R._date_range(args.start, args.end))
        if args.every > 1:
            dates = dates[::args.every]
    print(f"collect_month: {len(routes)} routes x {len(dates)} dates x 2 cabins "
          f"(biman={'off' if args.no_biman else 'on'}, workers={args.workers})")
    groups: dict = defaultdict(list)
    for cab in ("Economy", "Business"):
        for k, v in R.collect(routes, dates, workers=args.workers, use_amy="off",
                              cabin=cab, include_biman=not args.no_biman).items():
            groups[k].extend(v)
    df = R.build_df(groups)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    print(f"WROTE {args.out}  ({len(df)} rows, {df.groupby(['Origin','Destination']).ngroups} routes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
