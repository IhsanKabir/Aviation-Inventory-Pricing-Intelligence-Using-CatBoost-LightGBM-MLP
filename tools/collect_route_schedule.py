"""Collect a forward schedule for BS's routes from FirstTrip.

FirstTrip answers one POST per route-date with every airline selling that
route, and needs no authentication -- which is why it is the source here
rather than the stored offers, whose last scrape was 17 July and which had
IndiGo missing from DAC-CCU entirely while it was demonstrably flying it.

Two things this writes down that a naive count would get wrong:

* A fare is not a flight. One departure comes back once per fare variant --
  fourteen rows for a single SQ447 -- so frequency is counted on distinct
  (airline, flight number, departure), never on rows.
* What is on sale is not what is scheduled. A sold-out flight can vanish
  from the search entirely, so the frequency this produces is a FLOOR, and
  the number of days actually observed is written beside it so nobody reads
  a gap as an absence.

Output is one parquet per run under the analytics root, so the report is
built from a stored pull rather than re-scraped each time it is opened.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules import firsttrip  # noqa: E402

LOG = logging.getLogger("collect_route_schedule")

#: Politeness between calls. The endpoint is public and unauthenticated;
#: this is a slow sequential walk, not a parallel hammering.
DELAY_SECONDS = 1.5

#: Where the Travel Ops Console reads curated datasets from.
DEFAULT_OUT = Path("E:/Analysis/curated/competitor_schedule")


def _rows_for(origin: str, destination: str, day: str) -> tuple:
    """(rows, error) for one route-date, never raising."""
    try:
        res = firsttrip.fetch_flights(origin=origin, destination=destination,
                                      date=day, cabin="Economy")
    except Exception as exc:                       # noqa: BLE001
        return [], str(exc)[:200]
    if not res.get("ok"):
        return [], str(res.get("error") or "not ok")[:200]
    return (res.get("rows") or []), ""


def collect(pairs, days: int, start: date, delay: float = DELAY_SECONDS):
    """Walk every route-date once, yielding normalised flight rows."""
    out, problems = [], []
    total = len(pairs) * days
    n = 0
    for origin, destination in pairs:
        for offset in range(days):
            day = (start + timedelta(days=offset)).isoformat()
            n += 1
            rows, err = _rows_for(origin, destination, day)
            if err:
                problems.append({"origin": origin, "destination": destination,
                                 "date": day, "error": err})
            for r in rows:
                out.append({
                    "queried_origin": origin,
                    "queried_destination": destination,
                    "queried_date": day,
                    "airline": r.get("airline"),
                    "operating_airline": r.get("operating_airline"),
                    "flight_number": r.get("flight_number"),
                    "origin": r.get("origin"),
                    "destination": r.get("destination"),
                    "departure": r.get("departure"),
                    "arrival": r.get("arrival"),
                    "aircraft": r.get("aircraft"),
                    "seat_available": r.get("seat_available"),
                    "cabin": r.get("cabin"),
                    "price_total_bdt": r.get("price_total_bdt"),
                    "fare_amount": r.get("fare_amount"),
                    "stops": r.get("stops"),
                })
            if n % 25 == 0 or n == total:
                print(f"  {n}/{total} calls · {len(out):,} rows · "
                      f"{len(problems)} problem(s)", flush=True)
            time.sleep(delay)
    return out, problems


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--routes", default="E:/tmp/bs_routes.json")
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--start", default="")
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--delay", type=float, default=DELAY_SECONDS)
    args = ap.parse_args()

    logging.disable(logging.CRITICAL)      # the connector is chatty per call
    pairs = [tuple(p) for p in json.loads(Path(args.routes).read_text())]
    start = (date.fromisoformat(args.start) if args.start
             else date.today() + timedelta(days=1))
    print(f"{len(pairs)} routes x {args.days} days = "
          f"{len(pairs) * args.days} calls, from {start}", flush=True)

    rows, problems = collect(pairs, args.days, start, args.delay)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    # Stamped with the WINDOW, not the run date. Two pulls on one day
    # for different windows share a run date, and the second silently
    # overwrote the first -- which is how a fortnight of collected
    # schedule went missing without anything reporting a problem.
    last = start + timedelta(days=args.days - 1)
    stamp = f"{start.isoformat()}_{last.isoformat()}"
    try:
        import pandas as pd
        path = out_dir / f"firsttrip_{stamp}.parquet"
        pd.DataFrame(rows).to_parquet(path, index=False)
    except Exception:                               # noqa: BLE001
        path = out_dir / f"firsttrip_{stamp}.json"
        path.write_text(json.dumps(rows), encoding="utf-8")
    (out_dir / f"problems_{stamp}.json").write_text(
        json.dumps(problems, indent=1), encoding="utf-8")

    print(f"\nwrote {len(rows):,} rows to {path}")
    print(f"{len(problems)} route-date(s) returned nothing "
          f"-- recorded, not assumed empty")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
