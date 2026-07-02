"""
OTA Discount GRID report — CLI wrapper.

The engine lives in the importable `discount_engine` package (see
`discount_engine/grid.py`); this script only parses arguments and calls it, so
the daily command keeps working unchanged:

  python tools/ota_discount_grid.py --auto --routes DAC-CGP,DAC-DXB,DAC-SIN \
      --date 2026-07-30 [--manual config/discount_manual_overrides.json]

Output: console grid + JSON + CSV under output/reports/, plus the colored daily
sheet appended to output/reports/OTA_Discount_Grid.xlsx (one sheet per run date,
overwritten if re-run the same day). Pass --no-true-base to keep each channel's
own (ratio/altered) base instead of the actual % off the canonical base.

Backward compatibility: `import ota_discount_grid as g` still exposes every
engine name (`g.build_report`, `g.collect_bdfare`, `g.bdfare_har`, ...) via a
module-level `__getattr__` that forwards to `discount_engine.grid`.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))  # repo root (script-run bootstrap)

from discount_engine import grid as _grid
from discount_engine.grid import (
    _parse_routes,
    _sheet_name,
    auto_detect_hars,
    build_report,
    render_console,
    write_outputs,
    write_xlsx,
)


def __getattr__(name: str) -> Any:  # PEP 562: forward everything else to the engine
    return getattr(_grid, name)


def main() -> int:
    p = argparse.ArgumentParser(description="OTA discount grid report (25 June format).")
    p.add_argument("--date", default=None,
                   help="Default travel date YYYY-MM-DD for live FirstTrip B2C. Optional; "
                        "HAR channels ignore it. Override per route with DAC-CGP@YYYY-MM-DD.")
    p.add_argument("--routes", default="",
                   help="Comma list for live FirstTrip B2C, each ORIGIN-DEST or ORIGIN-DEST@DATE "
                        "(e.g. DAC-CGP@2026-07-30,DAC-DXB@2026-08-01)")
    p.add_argument("--gozayaan-har", default=None)
    p.add_argument("--amy-har", default=None)
    p.add_argument("--firsttrip-b2b-har", default=None, help="Logged-in booking.firsttrip.com HAR (agent commission)")
    p.add_argument("--sharetrip-har", default=None, help="ShareTrip search HAR (B2C common discount)")
    p.add_argument("--akij-har", default=None, help="AKIJ Air search HAR (agent commission)")
    p.add_argument("--bdfare-har", default=None, help="BDFare searchpad HAR (agent commission)")
    p.add_argument("--auto", nargs="?", const="output/discount_hars", default=None,
                   help="Auto-detect HARs in a folder (default output/discount_hars) and wire them by channel")
    p.add_argument("--combined-har", default=None,
                   help="One HAR containing ALL channels; every parser reads it and extracts its own endpoints")
    p.add_argument("--manual", default=None,
                   help="JSON of manual cell overrides for uncapturable channels: "
                        "{row_label: {DOM|INTL: {airline: \"value\"}}}")
    p.add_argument("--out", default="output/reports")
    p.add_argument("--xlsx", default="output/reports/OTA_Discount_Grid.xlsx",
                   help="Persistent workbook to append today's sheet to "
                        "(one sheet per run date, overwritten if re-run same day)")
    p.add_argument("--no-xlsx", action="store_true", help="Skip the daily Excel sheet")
    p.add_argument("--run-date", default=None,
                   help="Override the report RUN date (YYYY-MM-DD) — sets the sheet name and "
                        "header date. Use to (re)generate a past day's sheet, e.g. after a fix. "
                        "Defaults to today.")
    p.add_argument("--no-true-base", action="store_true",
                   help="Disable the true-base recompute. By DEFAULT the grid reports BDFare "
                        "and AKIJ domestic cells as the ACTUAL %% off the canonical base learned "
                        "from FT B2B/B2C; pass this to keep their own (ratio/altered) base instead.")
    p.add_argument("--quiet", action="store_true")
    args = p.parse_args()

    # Per-channel HAR lists, combined from explicit flags + --combined-har + --auto folder.
    channels = ["gozayaan", "amy", "firsttrip_b2b", "sharetrip", "akij", "bdfare"]
    hars: dict[str, list[str]] = {c: [] for c in channels}

    flag_map = {"gozayaan": args.gozayaan_har, "amy": args.amy_har,
                "firsttrip_b2b": args.firsttrip_b2b_har, "sharetrip": args.sharetrip_har,
                "akij": args.akij_har, "bdfare": args.bdfare_har}
    for c, v in flag_map.items():
        if v:
            hars[c].append(v)

    if args.combined_har:
        for c in channels:
            hars[c].append(args.combined_har)

    if args.auto:
        har_dir = Path(args.auto)
        if not har_dir.is_dir():
            raise SystemExit(f"--auto dir not found: {har_dir}")
        print(f"Auto-detecting HARs in {har_dir} ...")
        for c, paths in auto_detect_hars(har_dir).items():
            hars[c].extend(paths)

    routes = _parse_routes(args.routes)
    if not (routes or any(hars.values())):
        raise SystemExit("Provide at least one source (--routes / --combined-har / --auto / "
                         "--gozayaan-har / --amy-har / --firsttrip-b2b-har / --sharetrip-har "
                         "/ --akij-har / --bdfare-har).")

    manual_overrides = None
    if args.manual:
        manual_path = Path(args.manual)
        if not manual_path.is_file():
            raise SystemExit(f"--manual file not found: {manual_path}")
        manual_overrides = json.loads(manual_path.read_text(encoding="utf-8"))

    run_dt = None
    if args.run_date:
        d = datetime.strptime(args.run_date, "%Y-%m-%d")
        now = datetime.now()
        run_dt = d.replace(hour=now.hour, minute=now.minute, second=now.second)

    report = build_report(args.date, routes,
                          gozayaan_hars=hars["gozayaan"], amy_hars=hars["amy"],
                          firsttrip_b2b_hars=hars["firsttrip_b2b"], sharetrip_hars=hars["sharetrip"],
                          akij_hars=hars["akij"], bdfare_hars=hars["bdfare"],
                          manual_overrides=manual_overrides,
                          use_true_base=not args.no_true_base,
                          run_dt=run_dt)
    if not args.quiet:
        render_console(report)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    for path in write_outputs(report, Path(args.out), stamp):
        print(f"Saved: {path}")

    if not args.no_xlsx and args.xlsx:
        xlsx_path = write_xlsx(report, Path(args.xlsx))
        print(f"Saved: {xlsx_path}  (sheet '{_sheet_name(report)}')")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
