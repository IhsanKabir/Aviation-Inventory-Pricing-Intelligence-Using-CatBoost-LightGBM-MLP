"""
Harvest ShareTrip LIVE into the offer cache the KSA report already merges
(output/manual_sessions/sharetrip_cache.json) — no manual HAR needed.

Mints the short-lived session token itself (see modules/sharetrip_live.py), reuses it
across queries, and re-mints on expiry. ShareTrip rate-limits, so keep workers low.

Usage:
  python tools/harvest_sharetrip_live.py --dates 2026-08-08,2026-08-18,2026-08-28
  python tools/harvest_sharetrip_live.py --routes DAC-JED,DAC-RUH --dates 2026-08-20 --cabins economy,business
  python tools/harvest_sharetrip_live.py --all-report-routes --dates 2026-08-15 --workers 4
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import tools.ksa_market_report as R
from modules.sharetrip_har import _cache_key, load_cache, save_cache
from modules.sharetrip_live import fetch_live, mint_token


def _report_routes() -> list[tuple[str, str]]:
    routes = [(bd, sa) for bd in R.BD_ORIGINS for sa in R.KSA_DESTS] + \
             [(sa, bd) for sa in R.KSA_DESTS for bd in R.BD_ORIGINS]
    routes += [("DAC", d) for d in R.INTL_DESTS] + [(d, "DAC") for d in R.INTL_DESTS]
    return list(dict.fromkeys(routes))


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dates", required=True, help="comma list, e.g. 2026-08-08,2026-08-18")
    p.add_argument("--routes", default=None, help="comma list ORG-DST, e.g. DAC-JED,DAC-RUH")
    p.add_argument("--all-report-routes", action="store_true", help="harvest every route in the report")
    p.add_argument("--cabins", default="economy", help="economy / business / economy,business")
    p.add_argument("--workers", type=int, default=1,
                   help="keep at 1 — ShareTrip rate-limits/truncates results under concurrency")
    args = p.parse_args()

    dates = [x.strip() for x in args.dates.split(",") if x.strip()]
    cabins = [c.strip().capitalize() for c in args.cabins.split(",") if c.strip()]
    if args.routes:
        routes = [tuple(r.upper().split("-")) for r in args.routes.split(",")]
    elif args.all_report_routes:
        routes = _report_routes()
    else:
        routes = [(bd, sa) for bd in R.BD_ORIGINS for sa in R.KSA_DESTS] + \
                 [("DAC", d) for d in R.INTL_DESTS]
    tasks = [(o, d, dt, cab) for (o, d) in routes for dt in dates for cab in cabins]
    print(f"ShareTrip LIVE harvest: {len(tasks)} searches "
          f"({len(routes)} routes x {len(dates)} dates x {len(cabins)} cabin), {args.workers} workers")

    # ShareTrip is per-IP rate-limited -> single-threaded ONLY (family rule). We pace
    # queries, skip already-cached cells (resume), and back off on 429 streaks instead
    # of hammering (aggressive retries cause longer IP bans).
    SLEEP = float(os.getenv("SHARETRIP_INTER_QUERY_SLEEP", "3.0"))
    COOLDOWN = int(os.getenv("SHARETRIP_COOLDOWN_SEC", "600"))
    STREAK_LIMIT = int(os.getenv("SHARETRIP_STREAK_LIMIT", "6"))

    cache = load_cache()
    pending = [(o, d, dt, cab) for (o, d, dt, cab) in tasks
               if not cache.get(_cache_key(o, d, dt, cab))]
    print(f"  resume: {len(tasks) - len(pending)} already cached, {len(pending)} to harvest")
    if not pending:
        print("  nothing to do.")
        return 0

    s = requests.Session()
    token = mint_token(s, *pending[0])
    print("  initial token:", (token or "MINT FAILED")[:20] + "...")

    done = added = streak = 0
    for (o, d, dt, cab) in pending:
        try:
            r = fetch_live(o, d, dt, cab, session=s, token=token)
        except Exception as e:  # network/DNS blip — don't crash the whole run
            print(f"    transient error {o}->{d} {dt}/{cab}: {type(e).__name__}; continuing")
            r = {"ok": False, "rows": [], "reason": "no_searchId"}
        if r.get("token"):
            token = r["token"]
        rows = r["rows"]
        if rows:
            cache[_cache_key(o, d, dt, cab)] = rows
            added += len(rows)
            streak = 0
        elif r.get("reason") in ("no_searchId", "mint_failed"):
            streak += 1                                 # throttle/auth signal (not a no-service route)
        else:
            streak = 0                                  # genuine empty (route not served) — don't cooldown
        done += 1
        if done % 10 == 0 or done == len(pending):
            save_cache(cache)
            print(f"    {done}/{len(pending)} — {added} offers cached (empty streak {streak})")
        if streak >= STREAK_LIMIT:                      # likely a 429 cooldown window
            print(f"    {streak} empties in a row -> ShareTrip cooldown {COOLDOWN}s, then resume")
            save_cache(cache)
            time.sleep(COOLDOWN)
            token = mint_token(s, o, d, dt, cab) or token
            streak = 0
        else:
            time.sleep(SLEEP)
    save_cache(cache)
    print(f"WROTE cache ({len(cache)} keys total): "
          f"{REPO_ROOT / 'output' / 'manual_sessions' / 'sharetrip_cache.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
