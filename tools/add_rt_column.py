"""
Add a "Return 7d" (round-trip, 7-day) average column to the blended report.

Maps the round-trip average per (Origin, Destination, Class, Airline) from rt_gross.csv
onto every matching row of the latest blended one-way report, then rebuilds the workbook
(the route blocks + All Routes show the new column; 0/no-data shows as "—").

Usage:
  python tools/add_rt_column.py [--ow <blend.csv>] [--rt output/reports/rt_gross.csv]
"""
from __future__ import annotations

import argparse
import glob
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import tools.ksa_market_report as R

NUMCOLS = ("Lowest", "Highest", "Avg", "Base_Lowest", "Base_Highest", "Base_Avg", "Fares")


def _num(df):
    for c in NUMCOLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    return df


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--ow", default=None, help="blended one-way CSV (default: latest JUL-AUG_blend)")
    p.add_argument("--rt", default="output/reports/rt_gross.csv")
    args = p.parse_args()

    ow_csv = args.ow or max(glob.glob("output/reports/ksa_market_JUL-AUG_blend_*.csv"),
                            key=lambda c: Path(c).stat().st_mtime)
    print(f"One-way base: {Path(ow_csv).name}")
    ow = _num(pd.read_csv(ow_csv).fillna(""))

    rt = pd.read_csv(args.rt).fillna("")
    for c in ("Lowest", "Highest", "Avg"):
        rt[c] = pd.to_numeric(rt[c], errors="coerce").fillna(0).astype(int)
    rt = rt[rt["Avg"] > 0]
    rt = R.logical_clean(rt)   # strip RT outliers so RT high/avg aren't skewed
    # round-trip Lowest (min) / Highest (max) / Avg (mean) per route+class+airline
    grp = rt.groupby(["Origin", "Destination", "Class", "Airline"])
    rt_low = grp["Lowest"].min().astype(int)
    rt_high = grp["Highest"].max().astype(int)
    rt_avg = grp["Avg"].mean().round().astype(int)
    print(f"RT source: {Path(args.rt).name} ({len(rt)} rows -> {len(rt_avg)} route+class+airline RT triples)")

    keys = list(zip(ow["Origin"], ow["Destination"], ow["Class"], ow["Airline"]))
    ow["RT_Lowest"] = [int(rt_low.get(k, 0)) for k in keys]
    ow["RT_Highest"] = [int(rt_high.get(k, 0)) for k in keys]
    ow["RT_Avg"] = [int(rt_avg.get(k, 0)) for k in keys]
    matched = int((ow["RT_Avg"] > 0).sum())
    print(f"Rows with a return (7d) fare: {matched}/{len(ow)}")

    # logical average: strip high-outlier flights per route+class (skew the mean)
    ow = R.logical_clean(ow)

    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    out = REPO_ROOT / "output" / "reports" / f"ksa_market_JUL-AUG_blend_RT_{ts}.xlsx"
    R.write_excel(ow, {}, out, "2026-07-01", "2026-08-31")
    ow.to_csv(str(out).replace(".xlsx", ".csv"), index=False)
    print(f"WROTE {out.name}  ({len(ow)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
