"""
Derive BASE fare for the existing full-month report WITHOUT re-collecting it.

Tax is stable per route+carrier+cabin (set by government + carrier surcharges, not
the OTA or the exact date). So we sample FirstTrip on a few dates to learn the tax
per (route, airline, cabin), then subtract it from the gross fares we already have:

    base = gross - tax(route, airline, cabin)

Fallback chain when a route+airline+cabin wasn't sampled:
    (origin,dest,airline,cabin) -> (origin,dest,cabin) -> (airline,cabin) -> global ratio

Usage:
  python tools/apply_base_from_tax.py [--csv <gross.csv>] [--dates 2026-08-08,2026-08-18,2026-08-28] [--workers 8]
"""
from __future__ import annotations

import argparse
import glob
import statistics
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import tools.ksa_market_report as R

DEFAULT_DATES = ["2026-08-08", "2026-08-18", "2026-08-28"]


def _median(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def sample_tax(routes, dates, workers: int = 8) -> dict:
    """Learn tax = gross - base per (route, airline, cabin) from a small FirstTrip sample.
    Also folds in any real base already sitting in the GoZayaan/ShareTrip HAR caches.
    Tracks BOTH absolute tax and the tax/price ratio per key (ratio rescues cheap fares)."""
    by_rac: dict = defaultdict(list)
    by_rc: dict = defaultdict(list)
    by_ac: dict = defaultdict(list)
    r_rac: dict = defaultdict(list)
    r_rc: dict = defaultdict(list)
    r_ac: dict = defaultdict(list)
    ratios: list = []

    def _add(o, d, air, cab, price, base):
        if price > 0 and 0 < base <= price:
            tax = price - base
            by_rac[(o, d, air, cab)].append(tax)
            by_rc[(o, d, cab)].append(tax)
            by_ac[(air, cab)].append(tax)
            r_rac[(o, d, air, cab)].append(tax / price)
            r_rc[(o, d, cab)].append(tax / price)
            r_ac[(air, cab)].append(tax / price)
            ratios.append(tax / price)

    def q(o, d, dt, cab):
        out = []
        for r in R.query_source(R.ft_fetch, o, d, dt, cab):
            p = float(r.get("price_total_bdt") or 0)
            b = float(r.get("fare_amount") or 0)
            air = R.AIRLINE_ALIAS.get(r.get("airline", ""), r.get("airline", ""))
            out.append((o, d, air, cab, p, b))
        return out

    tasks = [(o, d, dt, cab) for (o, d) in routes for dt in dates
             for cab in ("Economy", "Business")]
    print(f"  Sampling FirstTrip tax: {len(tasks)} queries "
          f"({len(routes)} routes x {len(dates)} dates x 2 cabins), {workers} workers")
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(q, *t) for t in tasks]
        for f in as_completed(futs):
            for (o, d, air, cab, p, b) in f.result():
                _add(o, d, air, cab, p, b)
            done += 1
            if done % 100 == 0 or done == len(tasks):
                print(f"    {done}/{len(tasks)} sampled — {len(by_rac)} route+airline+cabin tax points")

    # Fold in real base/tax already captured in the HAR caches (GoZayaan, ShareTrip)
    for load_cache, key_fn, label in (
        (R.goz_load_cache, R.goz_key, "GoZayaan"),
        (R.st_load_cache, R.st_key, "ShareTrip"),
    ):
        cache = load_cache()
        n = 0
        for k, rows in cache.items():
            for r in rows:
                p = float(r.get("price_total_bdt") or 0)
                b = float(r.get("fare_amount") or 0)
                if not (p > 0 and 0 < b <= p):
                    continue
                o, d = str(r.get("origin")).upper(), str(r.get("destination")).upper()
                air = R.AIRLINE_ALIAS.get(r.get("airline", ""), r.get("airline", ""))
                cab = "Business" if "business" in str(r.get("cabin_class", "")).lower() else "Economy"
                _add(o, d, air, cab, p, b)
                n += 1
        if n:
            print(f"  + {label} HAR real base: {n} extra tax points")

    return {
        "rac": {k: _median(v) for k, v in by_rac.items()},
        "rc": {k: _median(v) for k, v in by_rc.items()},
        "ac": {k: _median(v) for k, v in by_ac.items()},
        "rac_r": {k: _median(v) for k, v in r_rac.items()},
        "rc_r": {k: _median(v) for k, v in r_rc.items()},
        "ac_r": {k: _median(v) for k, v in r_ac.items()},
        "ratio": _median(ratios),
    }


def _tax_for(model, o, d, air, cab, gross):
    """Most-specific (absolute tax, tax ratio, source-tag) available."""
    if (o, d, air, cab) in model["rac"]:
        return model["rac"][(o, d, air, cab)], model["rac_r"].get((o, d, air, cab)), ""
    if (o, d, cab) in model["rc"]:
        return model["rc"][(o, d, cab)], model["rc_r"].get((o, d, cab)), "rc"
    if (air, cab) in model["ac"]:
        return model["ac"][(air, cab)], model["ac_r"].get((air, cab)), "ac"
    if model["ratio"] is not None:
        return gross * model["ratio"], model["ratio"], "ratio"
    return None, None, "n/a"


# Below this base-share an absolute tax is treated as implausible for the fare level,
# and we fall back to the route's tax RATIO so cheap fares never clamp to ~0.
_BASE_FLOOR = 0.30


def _base_one(gross, abs_tax, ratio):
    if abs_tax is None:
        return gross
    base = gross - abs_tax
    if base < gross * _BASE_FLOOR and ratio is not None:
        base = gross * (1 - ratio)           # proportional tax for this route instead
    return max(0, int(round(base)))


def apply_base(df: pd.DataFrame, model: dict) -> pd.DataFrame:
    blo, bhi, bav, bes = [], [], [], []
    tag_counts: dict = defaultdict(int)
    ratio_rescues = 0
    for _, r in df.iterrows():
        o, d, air, cab = r["Origin"], r["Destination"], r["Airline"], r["Class"]
        vals, tag = {}, ""
        for col in ("Lowest", "Highest", "Avg"):
            g = int(r[col])
            abs_tax, ratio, t = _tax_for(model, o, d, air, cab, g)
            b = _base_one(g, abs_tax, ratio)
            if abs_tax is not None and (g - abs_tax) < g * _BASE_FLOOR and ratio is not None:
                ratio_rescues += 1
            vals[col] = b
            tag = t
        blo.append(vals["Lowest"]); bhi.append(vals["Highest"]); bav.append(vals["Avg"])
        bes.append("" if tag == "" else ("n/a" if tag == "n/a" else "est"))
        tag_counts[tag if tag else "rac"] += 1
    out = df.copy()
    out["Base_Lowest"] = blo
    out["Base_Highest"] = bhi
    out["Base_Avg"] = bav
    out["Base_Est"] = bes
    print("  tax source per row:", dict(tag_counts), "| ratio-rescued cells:", ratio_rescues)
    return out


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--csv", default=None, help="gross combined CSV (default: latest 7000+ row one)")
    p.add_argument("--dates", default=",".join(DEFAULT_DATES))
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--start", default="2026-08-01")
    p.add_argument("--end", default="2026-08-31")
    args = p.parse_args()

    csv = args.csv
    if not csv:
        cands = [(len(pd.read_csv(c)), c) for c in
                 glob.glob("output/reports/ksa_market_combined_2026-08-01_2026-08-31_*.csv")]
        csv = max(cands)[1]
    print(f"Gross source: {Path(csv).name}")
    df = pd.read_csv(csv).fillna("")
    for c in ("Lowest", "Highest", "Avg", "Fares"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    df = df.drop(columns=[c for c in ("Base_Lowest", "Base_Highest", "Base_Avg", "Base_Est")
                          if c in df.columns])

    routes = sorted(set(zip(df["Origin"], df["Destination"])))
    dates = [x.strip() for x in args.dates.split(",") if x.strip()]
    model = sample_tax(routes, dates, args.workers)
    df = apply_base(df, model)

    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    out = REPO_ROOT / "output" / "reports" / f"ksa_market_combined_{args.start}_{args.end}_{ts}.xlsx"
    R.write_excel(df, {}, out, args.start, args.end)
    df.to_csv(str(out).replace(".xlsx", ".csv"), index=False)
    print(f"WROTE {out.name}  ({len(df)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
