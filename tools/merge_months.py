"""
Blend July into August so route/carrier averages reflect BOTH months.

Inputs:
  - output/reports/july_gross.csv  (FirstTrip+Biman July, from collect_month.py)
  - ShareTrip July cache (output/manual_sessions/sharetrip_cache.json, July keys)
  - latest August combined CSV (gross)

Merge is per flight-key (Origin, Destination, Class, Airline, Transit, Dep):
  Lowest = min, Highest = max, Avg = fare-count-weighted mean, Fares = sum,
  Baggage unioned, RBD of the min/max fare kept. So the combined Avg is the true
  average over every July + August observation. Base is re-derived (gross - tax).

Usage:
  python tools/merge_months.py [--july output/reports/july_gross.csv] [--aug <august.csv>]
"""
from __future__ import annotations

import argparse
import glob
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import tools.ksa_market_report as R
from tools.apply_base_from_tax import apply_base, sample_tax
from tools.merge_biman import _merge   # per-key, fare-count-weighted aggregate merge

JULY_START, JULY_END = "2026-07-01", "2026-07-31"
SAMPLE_DATES = ["2026-08-08", "2026-08-18", "2026-08-28"]  # tax is route/carrier-stable across months


def all_routes() -> list[tuple[str, str]]:
    routes = [(bd, sa) for bd in R.BD_ORIGINS for sa in R.KSA_DESTS] + \
             [(sa, bd) for sa in R.KSA_DESTS for bd in R.BD_ORIGINS]
    routes += [("DAC", d) for d in R.INTL_DESTS] + [(d, "DAC") for d in R.INTL_DESTS]
    return list(dict.fromkeys(routes))


def july_sharetrip_df(routes, july_dates) -> pd.DataFrame:
    """Aggregate July ShareTrip cache offers into a df (same shape as build_df)."""
    cache = R.st_load_cache()
    groups: dict = defaultdict(list)
    n = 0
    for (o, d) in routes:
        for dt in july_dates:
            for cab in ("Economy", "Business"):
                for r in cache.get(R.st_key(o, d, str(dt), cab), []):
                    off = R._offer_from_row(r)
                    off["class"] = cab
                    groups[(o, d, off["airline"], off["transit"], off["dep"], cab)].append(off)
                    n += 1
    print(f"  July ShareTrip offers from cache: {n}")
    return R.build_df(groups) if groups else pd.DataFrame()


def _num(df):
    for c in ("Lowest", "Highest", "Avg", "Fares"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    return df


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--july", default="output/reports/july_gross.csv")
    p.add_argument("--aug", default=None)
    args = p.parse_args()

    routes = all_routes()
    july_dates = list(R._date_range(JULY_START, JULY_END))

    july_ft = _num(pd.read_csv(args.july).fillna(""))
    print(f"July FirstTrip+Biman: {len(july_ft)} rows")
    july_st = july_sharetrip_df(routes, july_dates)
    if len(july_st):
        july_st = _num(july_st)
        july = _merge(july_ft, july_st)
        print(f"July combined (FT+Biman+ShareTrip): {len(july)} rows")
    else:
        july = july_ft
        print("July: no ShareTrip July data found, using FirstTrip+Biman only")

    aug_csv = args.aug
    if not aug_csv:
        cands = [(len(pd.read_csv(c)), c) for c in
                 glob.glob("output/reports/ksa_market_combined_2026-08-01_2026-08-31_*.csv")]
        aug_csv = max(cands)[1]
    print(f"August: {Path(aug_csv).name}")
    aug = _num(pd.read_csv(aug_csv).fillna(""))

    blended = _merge(july, aug)
    blended = _num(blended)
    print(f"BLENDED July+August: {len(blended)} rows, "
          f"{blended.groupby(['Origin','Destination']).ngroups} routes, "
          f"total fares {int(blended['Fares'].sum())}")

    # re-derive base on the blend (gross - tax; tax sampled from FirstTrip, stable across months)
    model = sample_tax(sorted(set(zip(blended['Origin'], blended['Destination']))), SAMPLE_DATES)
    blended = apply_base(blended, model)

    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    out = REPO_ROOT / "output" / "reports" / f"ksa_market_JUL-AUG_blend_{ts}.xlsx"
    R.write_excel(blended, {}, out, "2026-07-01", "2026-08-31")
    blended.to_csv(str(out).replace(".xlsx", ".csv"), index=False)
    print(f"WROTE {out.name}  ({len(blended)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
