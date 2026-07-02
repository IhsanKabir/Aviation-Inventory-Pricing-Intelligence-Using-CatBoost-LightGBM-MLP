"""
Add Biman (BG) direct-source data to the latest combined report WITHOUT re-querying
the other sources. Queries only Biman live for every route/date/cabin, then merges
its BG offers into the existing combined CSV (the FirstTrip + OTA data we already
have) and rewrites the workbook.

Merge is per flight key (Origin, Destination, Class, Airline, Transit, Dep):
overlapping rows combine as Lowest=min, Highest=max, Avg=fare-count-weighted mean,
Fares summed, Baggage tiers unioned; Biman-only flights are added. So BG ends up
compared across Biman + FirstTrip + OTAs with nothing missing.

Usage:  python tools/merge_biman.py [--start 2026-08-01] [--end 2026-08-31] [--workers 8]
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import tools.ksa_market_report as R

KEY = ["Origin", "Destination", "Class", "Airline", "Transit", "Dep"]


def _routes():
    routes = [(bd, sa) for bd in R.BD_ORIGINS for sa in R.KSA_DESTS] + \
             [(sa, bd) for sa in R.KSA_DESTS for bd in R.BD_ORIGINS]
    routes += [("DAC", d) for d in R.INTL_DESTS] + [(d, "DAC") for d in R.INTL_DESTS]
    return list(dict.fromkeys(routes))


def _collect_biman(routes, dates, workers):
    groups: dict = defaultdict(list)

    def q(o, d, dt, cab):
        rows = R.query_source(R.biman_fetch, o, d, dt, cab)
        return o, d, cab, [R._offer_from_row(r) for r in rows]

    tasks = [(o, d, dt, cab) for (o, d) in routes for dt in dates
             for cab in ("Economy", "Business")]
    print(f"  Biman: {len(tasks)} queries ({len(routes)} routes x {len(dates)} dates x 2 cabins)")
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(q, o, d, dt, cab) for (o, d, dt, cab) in tasks]
        for f in as_completed(futs):
            o, d, cab, offers = f.result()
            for off in offers:
                off["class"] = cab
                groups[(o, d, off["airline"], off["transit"], off["dep"], cab)].append(off)
            done += 1
            if done % 200 == 0 or done == len(tasks):
                print(f"    {done}/{len(tasks)} biman queries done — {len(groups)} BG options")
    return R.build_df(groups)


def _merge(ex_df: pd.DataFrame, bm_df: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([ex_df, bm_df], ignore_index=True)
    acc: dict = {}
    for _, r in combined.iterrows():
        k = tuple(str(r[c]) for c in KEY)
        lo, hi, av = int(r["Lowest"]), int(r["Highest"]), int(r["Avg"])
        fa = int(r["Fares"]) or 1
        if k not in acc:
            acc[k] = {
                "Origin": r["Origin"], "Destination": r["Destination"], "Class": r["Class"],
                "Airline": r["Airline"], "Transit": r["Transit"], "Dep": str(r["Dep"]),
                "Lowest": lo, "Highest": hi, "_avgnum": av * fa, "_fa": fa,
                "_lo": (lo, str(r.get("Low_RBD") or "")), "_hi": (hi, str(r.get("High_RBD") or "")),
                "_bag": [r.get("Baggage", "")],
                "Operated By": str(r.get("Operated By") or ""),
                "Aircraft": str(r.get("Aircraft") or ""), "Arr": str(r.get("Arr") or ""),
            }
        else:
            a = acc[k]
            a["Lowest"] = min(a["Lowest"], lo)
            a["Highest"] = max(a["Highest"], hi)
            a["_avgnum"] += av * fa
            a["_fa"] += fa
            if lo < a["_lo"][0]:
                a["_lo"] = (lo, str(r.get("Low_RBD") or ""))
            if hi > a["_hi"][0]:
                a["_hi"] = (hi, str(r.get("High_RBD") or ""))
            a["_bag"].append(r.get("Baggage", ""))
            if not a["Operated By"].strip() and str(r.get("Operated By") or "").strip():
                a["Operated By"] = str(r.get("Operated By"))
            if not a["Aircraft"].strip() and str(r.get("Aircraft") or "").strip():
                a["Aircraft"] = str(r.get("Aircraft"))
            if not a["Arr"].strip() and str(r.get("Arr") or "").strip():
                a["Arr"] = str(r.get("Arr"))

    recs = []
    for a in acc.values():
        recs.append({
            "Origin": a["Origin"], "Destination": a["Destination"], "Class": a["Class"],
            "Airline": a["Airline"], "Operated By": a["Operated By"], "Aircraft": a["Aircraft"],
            "Transit": a["Transit"], "Dep": a["Dep"], "Arr": a["Arr"],
            "Lowest": a["Lowest"], "Highest": a["Highest"],
            "Avg": int(round(a["_avgnum"] / a["_fa"])),
            "Low_RBD": a["_lo"][1], "High_RBD": a["_hi"][1],
            "Fares": a["_fa"], "Baggage": R._merge_bag_cells(a["_bag"]),
        })
    df = pd.DataFrame(recs)
    df["_crank"] = (df["Class"] == "Business").astype(int)
    return df.sort_values(["Origin", "Destination", "_crank", "Lowest"]).drop(columns="_crank")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2026-08-01")
    p.add_argument("--end", default="2026-08-31")
    p.add_argument("--workers", type=int, default=8)
    args = p.parse_args()

    csvs = sorted(glob.glob("output/reports/ksa_market_combined_*.csv"))
    if not csvs:
        print("No existing combined CSV found.")
        return 1
    ex_df = pd.read_csv(csvs[-1]).fillna("")
    print(f"Existing data: {os.path.basename(csvs[-1])} ({len(ex_df)} rows, "
          f"BG={len(ex_df[ex_df['Airline']=='BG'])})")

    dates = list(R._date_range(args.start, args.end))
    bm_df = _collect_biman(_routes(), dates, args.workers)
    print(f"Biman BG rows: {len(bm_df)}")

    merged = _merge(ex_df, bm_df)
    print(f"Merged: {len(merged)} rows (BG now {len(merged[merged['Airline']=='BG'])})")

    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    out = REPO_ROOT / "output" / "reports" / f"ksa_market_combined_{args.start}_{args.end}_{ts}.xlsx"
    R.write_excel(merged, {}, out, args.start, args.end)
    merged.to_csv(str(out).replace(".xlsx", ".csv"), index=False)
    print(f"WROTE {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
