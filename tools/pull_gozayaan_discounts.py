"""Pull GoZayaan published-coupon discounts live — the automatic replacement for
searching each route and clicking each airline by hand.

It mints GoZayaan's own short-lived token (the RSA key the site ships in its own
JS bundle, via modules.gozayaan), registers ONE search per route, and asks for
the coupon list for every carrier that search returned. Because the coupon list
is keyed (carrier, flight_type, product_price) with no route dimension, a single
search per market yields every carrier's discounts.

The result is written as a synthetic GoZayaan HAR into the discount capture
folder, so the desktop report reads it exactly like a browser-exported capture —
no change to the report engine, and no manual capture.

Citizenship: single-threaded, a pause between calls, a fresh token per request,
and it STOPS on the first rate-limit response (a 429 costs a 15-minute cooldown).
Keep --sleep at 3.0 or higher and do not run several copies at once.

Usage:
  python tools/pull_gozayaan_discounts.py --routes DAC-CGP,DAC-JED --date 2026-10-15
  python tools/pull_gozayaan_discounts.py --routes DAC-DXB --date 2026-10-15 \\
         --out output/discount_hars/gozayaan_live.har --sleep 3.0

Then run the desktop report against output/discount_hars/ as usual.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date as _date
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))


def _parse_routes(text: str):
    routes = []
    for chunk in str(text or "").replace(";", ",").split(","):
        chunk = chunk.strip().upper()
        if not chunk:
            continue
        origin, _, dest = chunk.partition("-")
        if len(origin) != 3 or len(dest) != 3 or not origin.isalpha() or not dest.isalpha():
            raise SystemExit(f"Route must look like DAC-CGP (got {chunk!r}).")
        routes.append((origin, dest))
    if not routes:
        raise SystemExit("Give at least one route, e.g. --routes DAC-CGP,DAC-JED")
    return routes


def main() -> int:
    p = argparse.ArgumentParser(description="Live GoZayaan discount pull -> synthetic HAR")
    p.add_argument("--routes", required=True, help="comma list, e.g. DAC-CGP,DAC-JED")
    p.add_argument("--date", default=None, help="YYYY-MM-DD (default: 14 days out)")
    p.add_argument("--out", default="output/discount_hars/gozayaan_live.har",
                   help="synthetic HAR path (default drops it in the capture folder)")
    p.add_argument("--sleep", type=float, default=3.0,
                   help="seconds between calls; do not go below 3.0")
    args = p.parse_args()

    routes = _parse_routes(args.routes)
    day = args.date
    if not day:
        from datetime import timedelta
        day = (_date.today() + timedelta(days=14)).isoformat()

    # Imported here so --help works without touching the network/token layer.
    from modules.gozayaan_discounts_live import pull_to_har

    print(f"GoZayaan live discount pull — {len(routes)} route(s), date {day}, "
          f"sleep {args.sleep}s")
    result = pull_to_har(routes, day, args.out, sleep_s=max(3.0, args.sleep))

    if result.get("cooldown"):
        # Not a failure: nothing was sent, on purpose. Say so plainly, in local time.
        from datetime import datetime
        state = result["cooldown"]
        try:
            until = datetime.fromisoformat(str(state.get("cooldown_until_utc"))).astimezone()
            when = until.strftime("%H:%M")
        except ValueError:
            when = "shortly"
        mins = int(state.get("remaining_cooldown_sec") or 0) // 60 + 1
        print(f"\nNOTHING SENT - GoZayaan's rate-limit cooldown is still running "
              f"(about {mins} min left).")
        print(f"Run this same command again after {when} your time.")
        return 2

    print("\n--- summary ---")
    for route, info in result["routes"].items():
        status = "ok" if info["ok"] else f"NO ({info['reason'] or 'no coupons'})"
        print(f"  {route:<10} {status:<40} carriers={info['carriers']}")
    print(f"\n{len(result['rows'])} coupon rows across {len(result['routes'])} route(s)")
    if result.get("har_written"):
        print(f"wrote {result['har_written']} calls -> {result['har_path']}")
        print("Now run the desktop report against the capture folder.")
        return 0
    print("No HAR written (no discount data collected).")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
