"""Base-fare alteration audit for the OTA discount channels (DOMESTIC).

Builds the canonical true base per (airline, gross) from the channels whose base/gross
are exact (FirstTrip B2B/B2C, Amy), then for every channel:
  * detects whether its base was ALTERED vs the true base, and how much,
  * keeps the ACTUAL base and the ACTUAL net (agent buy / customer sell / sold total),
  * reports the ACTUAL discount/markup % computed on the TRUE base.

Read-only; never changes the grid. Run alongside the daily report:
  python tools/base_fare_audit.py --routes DAC-CGP --date 2026-07-30

Memory-safe: one HAR parsed at a time (gc between).
"""
from __future__ import annotations

import argparse
import gc
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from modules import firsttrip, amyweb, akijair_har, bdfare_har, true_base as tb_mod

DOM = ["BS", "2A", "BG", "VQ"]


def _pct(numer: float, base: Optional[float]) -> Optional[float]:
    return round(numer / base * 100, 2) if base else None


def _find_hars(har_dir: Path, hint: str) -> List[Path]:
    return [p for p in sorted(har_dir.glob("*.har")) if hint in p.name.lower()]


def build_true_base(har_dir: Path, routes: List[tuple], date: Optional[str]) -> tuple:
    """Build the domestic TrueBase from FT B2B + Amy + live FT B2C. Returns (TrueBase, notes)."""
    notes: List[str] = []
    ft_rows: List[Dict[str, Any]] = []
    for p in _find_hars(har_dir, "booking.firsttrip") + _find_hars(har_dir, "firsttrip_b2b"):
        ft_rows += firsttrip.parse_b2b_commissions(str(p))
    gc.collect()

    amy_rows: List[Dict[str, Any]] = []
    for p in _find_hars(har_dir, "amyweb") + _find_hars(har_dir, "amybd"):
        amy_rows += amyweb.parse_agent_har(str(p))
    gc.collect()

    b2c_rows: List[Dict[str, Any]] = []
    for origin, dest, rdate in routes:
        d = rdate or date
        if not (d and tb_mod.is_domestic(origin, dest)):
            continue
        try:
            b2c_rows += firsttrip.fetch_b2c_discounts(origin, dest, d)
        except Exception as exc:  # noqa: BLE001
            notes.append(f"FT B2C live {origin}-{dest}: {exc}")

    tb = tb_mod.build_from_rows(ft_rows, amy_rows, b2c_rows)
    if tb.disagreements:
        notes.append(f"{len(tb.disagreements)} cross-channel base disagreement(s)")
    del ft_rows, amy_rows, b2c_rows
    gc.collect()
    return tb, notes


def audit_bdfare(har_dir: Path, tb: tb_mod.TrueBase) -> Dict[str, Any]:
    out: Dict[str, Any] = {"channel": "BDFare", "metric": "agent margin (customerNet - agent)", "cells": {}}
    rows: List[Dict[str, Any]] = []
    for p in _find_hars(har_dir, "bdfare"):
        rows += bdfare_har.parse_commissions(str(p))
    gc.collect()
    for air in DOM:
        cand = [r for r in rows if r["domestic"] and r["airline"] == air]
        best = None
        for r in cand:
            tbase, how = tb.base_for(air, r["gross_bdt"])
            if tbase is None:
                continue
            # Agent margin = customerNet - agent; when customerNet is absent the parser
            # already stored the (gross - agent) fallback in commission_bdt — reuse it
            # rather than re-deriving (which would zero out via `None or agent`).
            margin = (r["customer_net_bdt"] - r["agent_bdt"]
                      if r["customer_net_bdt"] is not None else r["commission_bdt"])
            true_pct = _pct(margin, tbase)
            rec = {
                "gross": r["gross_bdt"], "agent": r["agent_bdt"], "customer_net": r["customer_net_bdt"],
                "channel_base_est": r["base_est_bdt"], "true_base": tbase, "base_source": how,
                "base_delta": r["base_est_bdt"] - tbase,
                "reported_pct": r["commission_pct"], "true_pct": true_pct,
                "agent_discount_pct": _pct(r["gross_bdt"] - r["agent_bdt"], tbase),
                "customer_markup_pct": _pct(r["gross_bdt"] - (r["customer_net_bdt"] or r["gross_bdt"]), tbase),
            }
            if best is None or (true_pct or -1) > (best["true_pct"] or -1):
                best = rec
        if best:
            out["cells"][air] = best
    del rows
    gc.collect()
    return out


def audit_akij(har_dir: Path, tb: tb_mod.TrueBase) -> Dict[str, Any]:
    out: Dict[str, Any] = {"channel": "AKIJ", "metric": "realized markdown (gross - total)", "cells": {}}
    rows: List[Dict[str, Any]] = []
    for p in _find_hars(har_dir, "akij"):
        rows += akijair_har.parse_commissions(str(p))
    gc.collect()
    for air in DOM:
        cand = [r for r in rows if tb_mod.is_domestic(r["origin"], r["destination"]) and r["airline"] == air]
        best = None
        for r in cand:
            tbase, how = tb.base_for(air, r["gross_fare_bdt"])
            if tbase is None:
                continue
            true_pct = _pct(r["gross_fare_bdt"] - r["total_fare_bdt"], tbase)
            rec = {
                "gross": r["gross_fare_bdt"], "total": r["total_fare_bdt"],
                "channel_base": r["base_fare_bdt"], "true_base": tbase, "base_source": how,
                "base_delta": r["base_fare_bdt"] - tbase,
                "reported_pct": r["realized_discount_pct"], "true_pct": true_pct,
            }
            if best is None or (true_pct or -1) > (best["true_pct"] or -1):
                best = rec
        if best:
            out["cells"][air] = best
    del rows
    gc.collect()
    return out


def render(tb: tb_mod.TrueBase, bdfare: Dict, akij: Dict, notes: List[str]) -> None:
    print("\n" + "=" * 78)
    print("BASE-FARE ALTERATION AUDIT (domestic)")
    print("=" * 78)
    print("True base per (airline, gross)  [fixed domestic tax = gross - base]:")
    for a in DOM:
        gb = tb.by_gross.get(a, {})
        if gb:
            pts = "  ".join(f"{g}->{b}" for g, b in sorted(gb.items()))
            print(f"  {a}: tax={tb.tax.get(a, '?'):<5} {pts}")
    if notes:
        print("notes:", "; ".join(notes))

    for audit in (bdfare, akij):
        print(f"\n--- {audit['channel']}  (cell metric: {audit['metric']}) ---")
        if not audit["cells"]:
            print("  (no domestic cells matched a true base)")
            continue
        for air in DOM:
            c = audit["cells"].get(air)
            if not c:
                print(f"  {air}: -")
                continue
            altered = abs(c["base_delta"]) > 2
            flag = f"ALTERED base by {c['base_delta']:+d}" if altered else "base OK"
            extra = ""
            if audit["channel"] == "BDFare":
                extra = (f" | agent_disc={c['agent_discount_pct']}%  "
                         f"cust_markup={c['customer_markup_pct']}%")
            print(f"  {air}: reported={c['reported_pct']}%  ->  TRUE={c['true_pct']}%  "
                  f"[{flag}; src={c['base_source']}]{extra}")
    print("=" * 78)


def _parse_routes(value: str) -> List[tuple]:
    routes = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        date = None
        if "@" in item:
            item, date = item.split("@", 1)
        o, d = item.strip().upper().split("-", 1)
        routes.append((o, d, (date or None)))
    return routes


def main() -> int:
    p = argparse.ArgumentParser(description="Domestic base-fare alteration audit.")
    p.add_argument("--har-dir", default="output/discount_hars")
    p.add_argument("--routes", default="DAC-CGP", help="Domestic route(s) for live FT B2C true-base, e.g. DAC-CGP")
    p.add_argument("--date", default=None, help="Travel date YYYY-MM-DD for live FT B2C")
    p.add_argument("--out", default="output/reports")
    args = p.parse_args()

    har_dir = Path(args.har_dir)
    if not har_dir.is_dir():
        raise SystemExit(f"har-dir not found: {har_dir}")
    routes = _parse_routes(args.routes)

    tb, notes = build_true_base(har_dir, routes, args.date)
    bdfare = audit_bdfare(har_dir, tb)
    akij = audit_akij(har_dir, tb)
    render(tb, bdfare, akij, notes)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    payload = {
        "generated_at": datetime.now().isoformat(),
        "true_base": {a: tb.by_gross.get(a, {}) for a in DOM},
        "fixed_tax": tb.tax,
        "disagreements": tb.disagreements,
        "notes": notes,
        "BDFare": bdfare, "AKIJ": akij,
    }
    out_path = out_dir / f"base_fare_audit_{stamp}.json"
    out_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(f"Saved: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
