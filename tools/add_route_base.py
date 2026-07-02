"""
Add one or more DAC<->DEST routes to the month, then re-derive base = gross - tax
for the whole report (no full re-collection).

Usage:
  python tools/add_route_base.py KWI NRT
  python tools/add_route_base.py KWI --start 2026-08-01 --end 2026-08-31

The DEST codes must already be present in tools.ksa_market_report.INTL_DESTS / DEST_NAMES.
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

BASE_COLS = ["Base_Lowest", "Base_Highest", "Base_Avg", "Base_Est"]
SAMPLE_DATES = ["2026-08-08", "2026-08-18", "2026-08-28"]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("dests", nargs="+", help="destination codes to add, e.g. KWI NRT")
    p.add_argument("--start", default="2026-08-01")
    p.add_argument("--end", default="2026-08-31")
    p.add_argument("--workers", type=int, default=8)
    args = p.parse_args()
    dests = [d.upper() for d in args.dests]

    unknown = [d for d in dests if d not in R.DEST_NAMES]
    if unknown:
        print(f"ERROR: {unknown} not in DEST_NAMES — add them to ksa_market_report first.")
        return 1

    # 1) collect DAC<->DEST gross for each new dest (FirstTrip + Biman, both cabins)
    routes = [("DAC", d) for d in dests] + [(d, "DAC") for d in dests]
    dates = list(R._date_range(args.start, args.end))
    groups: dict = defaultdict(list)
    for cab in ("Economy", "Business"):
        for k, v in R.collect(routes, dates, workers=args.workers, use_amy="off", cabin=cab).items():
            groups[k].extend(v)
    new_df = R.build_df(groups)
    print(f"New rows collected for {dests}: {len(new_df)}")
    for d in dests:
        n = len(new_df[((new_df.Origin == 'DAC') & (new_df.Destination == d)) |
                       ((new_df.Origin == d) & (new_df.Destination == 'DAC'))])
        print(f"   {d}: {n} rows")

    # 2) merge into latest full-month combined CSV (gross basis; idempotent on these dests)
    cands = [(len(pd.read_csv(c)), c) for c in
             glob.glob(f"output/reports/ksa_market_combined_{args.start}_{args.end}_*.csv")]
    src = max(cands)[1]
    print(f"Base CSV: {Path(src).name}")
    ex = pd.read_csv(src).fillna("")
    ex = ex.drop(columns=[c for c in BASE_COLS if c in ex.columns])
    new_df = new_df.drop(columns=[c for c in BASE_COLS if c in new_df.columns])
    new_df = new_df[[c for c in ex.columns if c in new_df.columns]]
    mask = pd.Series(False, index=ex.index)
    for d in dests:
        mask |= (((ex["Origin"] == "DAC") & (ex["Destination"] == d)) |
                 ((ex["Origin"] == d) & (ex["Destination"] == "DAC")))
    ex = ex[~mask]
    merged = pd.concat([ex, new_df], ignore_index=True)
    merged["_crank"] = (merged["Class"] == "Business").astype(int)
    merged = merged.sort_values(["Origin", "Destination", "_crank", "Lowest"]).drop(columns="_crank")
    for c in ("Lowest", "Highest", "Avg", "Fares"):
        merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0).astype(int)
    print(f"Merged rows: {len(merged)} | routes: {merged.groupby(['Origin','Destination']).ngroups}")

    # 3) re-derive base = gross - tax for ALL routes (samples incl. the new ones)
    allroutes = sorted(set(zip(merged["Origin"], merged["Destination"])))
    model = sample_tax(allroutes, SAMPLE_DATES, args.workers)
    merged = apply_base(merged, model)

    # 4) write final workbook + CSV
    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    out = REPO_ROOT / "output" / "reports" / f"ksa_market_combined_{args.start}_{args.end}_{ts}.xlsx"
    R.write_excel(merged, {}, out, args.start, args.end)
    merged.to_csv(str(out).replace(".xlsx", ".csv"), index=False)
    print(f"WROTE {out.name}  ({len(merged)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
