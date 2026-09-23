"""What's left to click in a GoZayaan capture session — run it between clicks.

GoZayaan only loads an airline's coupon list when that airline's booking page
is opened, so every airline costs a search-and-click. This reads the GoZayaan
HAR(s) in your capture folder and prints, per market, the airlines still
missing, so a session needs ONE route per market and ONE click per airline.

Usage:
  python tools/gozayaan_coverage.py                       # the app's capture folder
  python tools/gozayaan_coverage.py path\\to\\folder_or.har  [more ...]
  python tools/gozayaan_coverage.py --only BS,BG,EK,QR    # just these airlines

By default "still to click" covers the airlines the report has columns for;
--only narrows it when time is short.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from discount_engine.grid import DOM_COLUMNS, INTL_COLUMNS, detect_channel  # noqa: E402
from modules.gozayaan_coverage import coverage_for_hars                      # noqa: E402

LABEL = {"DOM": "Domestic", "OUTBOUND": "International"}
REPORT_COLUMNS = {"DOM": DOM_COLUMNS, "OUTBOUND": INTL_COLUMNS}


def _default_folder() -> Path | None:
    cfg = Path(os.environ.get("APPDATA") or Path.home()) / "OTADiscountReport" / "config.json"
    try:
        folder = json.loads(cfg.read_text(encoding="utf-8")).get("har_dir")
    except (OSError, ValueError):
        return None
    return Path(folder) if folder else None


def _gozayaan_hars(args: list[str]) -> list[Path]:
    targets = [Path(a) for a in args] or [p for p in [_default_folder()] if p]
    hars: list[Path] = []
    for t in targets:
        # Top level only: archive/ holds finished days, not today's session.
        candidates = sorted(t.glob("*.har")) if t.is_dir() else [t]
        hars += [p for p in candidates if p.is_file() and detect_channel(p) == "gozayaan"]
    return hars


def main() -> int:
    p = argparse.ArgumentParser(description="What's left to click in a GoZayaan session")
    p.add_argument("paths", nargs="*", help="capture folder(s) or .har file(s)")
    p.add_argument("--only", default="", help="comma list of airlines to require, e.g. BS,BG,EK")
    args = p.parse_args()
    only = [a.strip().upper() for a in args.only.split(",") if a.strip()]

    hars = _gozayaan_hars(args.paths)
    if not hars:
        folder = ", ".join(args.paths) or str(_default_folder() or "(capture folder not set)")
        print(f"Nothing to check yet - no GoZayaan capture in: {folder}")
        print("This checks a session you are capturing NOW (finished days are in archive\\):")
        print("  1. On gozayaan.com search ONE domestic route, open one airline's booking page.")
        print("  2. DevTools > Network > Save all as HAR, into the folder above.")
        print("  3. Run this again - it lists the airlines still to click.")
        return 1
    print("Reading: " + ", ".join(p.name for p in hars))
    coverage = coverage_for_hars(hars)
    if not coverage:
        print("No GoZayaan searches or coupon lists in these captures yet.")
        return 1
    all_done = True
    for market in ("DOM", "OUTBOUND"):
        cov = coverage.get(market)
        if not cov:
            print(f"\n{LABEL[market]}: nothing captured - search ONE route to start.")
            all_done = False
            continue
        print(f"\n{LABEL[market]}  (routes searched: {', '.join(sorted(cov.routes)) or '-'})")
        tracked = only or REPORT_COLUMNS[market]
        todo = cov.missing_among(tracked)
        untracked = [a for a in cov.missing if a not in set(tracked)]
        print(f"  offered : {', '.join(sorted(cov.offered)) or '-'}")
        print(f"  captured: {', '.join(sorted(cov.captured)) or '-'}")
        if todo:
            all_done = False
            print(f"  STILL TO CLICK ({len(todo)}): {', '.join(todo)}")
        else:
            print("  complete - no more clicks needed for this market")
        if untracked:
            print(f"  (also offered, not required: {', '.join(untracked)})")
        if cov.repeats:
            print("  (clicked more than once - not needed: "
                  + ", ".join(f"{a} x{n}" for a, n in cov.repeats.items()) + ")")
        if len(cov.routes) > 1:
            print("  (more than one route searched - one route per market is enough)")
    print("\nAll markets complete - export the HAR." if all_done else "")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
