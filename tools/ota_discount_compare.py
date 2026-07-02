"""
OTA Discount Comparison Tool
============================

Builds the channel x airline price/discount comparison grid for a single
route + date, mixing two acquisition modes:

  * LIVE   - channels we can automate (ShareTrip, FirstTrip): fetched directly.
  * HAR    - channels we cannot automate reliably (Agoda, Akbar, GoZayaan,
             Trip.com, ShareTrip-B2C): you search the route/date in the site,
             export a .har, and pass it in. The existing per-OTA HAR parsers
             turn it into the same normalized row schema.

All channels emit a common row shape (airline, origin, destination, departure,
cabin, flight_number, price_total_bdt, fare_amount, tax_amount, ...), so the
grid is just: per (channel, airline) take the cheapest fare on that route/date,
then rank channels within each airline column.

Usage
-----
  python tools/ota_discount_compare.py \
      --origin DAC --destination CGP --date 2026-07-01 --cabin economy \
      --live sharetrip,firsttrip \
      --har agoda=C:\\hars\\agoda.har \
      --har gozayaan=C:\\hars\\gozayaan.har \
      --har sharetrip_b2c=C:\\hars\\sharetrip.har

  # HAR-only run (no live calls):
  python tools/ota_discount_compare.py --origin DAC --destination JFK --date 2026-07-01 \
      --live "" --har agoda=a.har --har trip=t.har

Output: console grid + JSON + CSV under output/reports/.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

# Repo root on path so we can import the modules/ package.
sys.path.insert(0, str(Path(__file__).parent.parent))

# --- Channel registry -------------------------------------------------------
# label is what shows in the grid; loader is filled in lazily so a broken
# module never blocks the whole run.

# Live channels: module + callable that returns {"rows": [...], "ok": bool}.
LIVE_CHANNELS: dict[str, dict[str, str]] = {
    "sharetrip": {"label": "ShareTrip", "module": "modules.sharetrip"},
    "firsttrip": {"label": "Firsttrip", "module": "modules.firsttrip"},
}

# HAR channels: module exposing parse_har(path) -> list[row].
HAR_CHANNELS: dict[str, dict[str, str]] = {
    "sharetrip_b2c": {"label": "ShareTrip-B2C", "module": "modules.sharetrip_har"},
    "gozayaan": {"label": "Go Zayaan", "module": "modules.gozayaan_har"},
    "agoda": {"label": "Agoda", "module": "modules.agoda_har"},
    "trip": {"label": "Trip.Com", "module": "modules.trip_har"},
    "akbar": {"label": "AKBAR", "module": "modules.akbartravels"},
}


def _import(module_path: str):
    """Import a module by dotted path, returning None on failure."""
    try:
        mod = __import__(module_path, fromlist=["*"])
        return mod
    except Exception as exc:  # noqa: BLE001 - report, do not crash the run
        print(f"  ! could not import {module_path}: {exc}")
        return None


def _row_matches(row: dict[str, Any], origin: str, destination: str,
                 date: str, cabin: Optional[str], airline: Optional[str]) -> bool:
    """Keep only rows for the requested O&D / date (/ cabin / airline)."""
    if str(row.get("origin", "")).upper() != origin.upper():
        return False
    if str(row.get("destination", "")).upper() != destination.upper():
        return False
    dep = str(row.get("departure") or row.get("departure_date") or "")
    if date and not dep.startswith(date):
        return False
    if cabin:
        row_cabin = str(row.get("cabin") or row.get("cabin_class") or "").lower()
        if row_cabin and cabin.lower() not in row_cabin and row_cabin not in cabin.lower():
            return False
    if airline and str(row.get("airline", "")).upper() != airline.upper():
        return False
    return True


def fetch_live_rows(channel: str, origin: str, destination: str, date: str,
                    cabin: str, airline: Optional[str]) -> dict[str, Any]:
    """Run a live channel and return {status, offers, rows, error}."""
    info = LIVE_CHANNELS[channel]
    mod = _import(info["module"])
    if mod is None or not hasattr(mod, "fetch_flights"):
        return {"status": "error", "offers": 0, "rows": [], "error": "module/fetch_flights missing"}
    try:
        result = mod.fetch_flights(origin, destination, date, cabin=cabin,
                                   airline_code=airline)
        rows = [r for r in (result.get("rows") or [])
                if _row_matches(r, origin, destination, date, cabin, airline)]
        status = "ok" if rows else ("no_data" if result.get("ok") is not False else "error")
        return {"status": status, "offers": len(rows), "rows": rows,
                "error": (result.get("raw") or {}).get("error")}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "offers": 0, "rows": [], "error": str(exc)}


def parse_har_rows(channel: str, har_path: str, origin: str, destination: str,
                   date: str, cabin: str, airline: Optional[str]) -> dict[str, Any]:
    """Parse a HAR for a channel and return {status, offers, rows, error}."""
    info = HAR_CHANNELS[channel]
    mod = _import(info["module"])
    if mod is None or not hasattr(mod, "parse_har"):
        return {"status": "error", "offers": 0, "rows": [], "error": "module/parse_har missing"}
    path = Path(har_path)
    if not path.exists():
        return {"status": "error", "offers": 0, "rows": [], "error": f"file not found: {har_path}"}
    try:
        all_rows = mod.parse_har(path)
        rows = [r for r in all_rows
                if _row_matches(r, origin, destination, date, cabin, airline)]
        return {"status": "ok" if rows else "no_data", "offers": len(rows),
                "rows": rows, "error": None}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "offers": 0, "rows": [], "error": str(exc)}


def _cheapest_price(rows: list[dict[str, Any]]) -> Optional[float]:
    """Lowest positive total price across a channel's offers for one airline."""
    prices = [float(r["price_total_bdt"]) for r in rows
              if r.get("price_total_bdt") and float(r["price_total_bdt"]) > 0]
    return min(prices) if prices else None


def build_grid(results: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """
    Collapse per-channel rows into a (channel -> airline -> price) grid plus a
    per-airline comparison (cheapest channel, rank, relative discount %).

    Relative discount % = how much cheaper a channel is than the dearest
    channel for that airline: (max_price - price) / max_price * 100.
    NOTE: this is a cross-channel relative metric, NOT an OTA self-reported
    discount. Confirm against the intended manual definition before trusting.
    """
    # channel -> airline -> cheapest price
    price_matrix: dict[str, dict[str, float]] = {}
    airlines: set[str] = set()
    for channel, res in results.items():
        per_airline: dict[str, list[dict[str, Any]]] = {}
        for row in res["rows"]:
            per_airline.setdefault(str(row.get("airline", "")).upper(), []).append(row)
        price_matrix[channel] = {}
        for airline_code, rows in per_airline.items():
            price = _cheapest_price(rows)
            if price is not None:
                price_matrix[channel][airline_code] = price
                airlines.add(airline_code)

    comparison: dict[str, Any] = {}
    for airline_code in sorted(airlines):
        cells = {ch: price_matrix[ch][airline_code]
                 for ch in price_matrix if airline_code in price_matrix[ch]}
        if not cells:
            continue
        max_price = max(cells.values())
        ranked = sorted(cells.items(), key=lambda kv: kv[1])  # cheapest first
        comparison[airline_code] = {
            "cheapest_channel": ranked[0][0],
            "cheapest_price": ranked[0][1],
            "max_price": max_price,
            "channels": {
                ch: {
                    "price": price,
                    "rel_discount_pct": round((max_price - price) / max_price * 100, 2)
                    if max_price else 0.0,
                    "rank": rank + 1,
                }
                for rank, (ch, price) in enumerate(ranked)
            },
        }
    return {"price_matrix": price_matrix, "airlines": sorted(airlines),
            "comparison": comparison}


def _label(channel: str) -> str:
    if channel in LIVE_CHANNELS:
        return LIVE_CHANNELS[channel]["label"]
    if channel in HAR_CHANNELS:
        return HAR_CHANNELS[channel]["label"]
    return channel


def render_console(report: dict[str, Any]) -> None:
    q = report["query"]
    print("\n" + "=" * 78)
    print(f"OTA DISCOUNT COMPARISON  {q['origin']}-{q['destination']}  "
          f"{q['date']}  ({q['cabin']})")
    print("=" * 78)

    print("\nChannels:")
    for channel, res in report["sources"].items():
        note = f" ({res['error']})" if res.get("error") else ""
        print(f"  {_label(channel):<16} {res['status']:<8} {res['offers']:>3} offers{note}")

    grid = report["grid"]
    airlines = grid["airlines"]
    if not airlines:
        print("\nNo overlapping offers found for this query.")
        return

    channels = list(report["sources"].keys())
    width = 11
    header = f"\n{'CHANNEL':<16}" + "".join(f"{a:>{width}}" for a in airlines)
    print("\n" + "-" * len(header))
    print("PRICE GRID (lowest total BDT per airline; * = cheapest channel)")
    print(header)
    for channel in channels:
        cells = grid["price_matrix"].get(channel, {})
        line = f"{_label(channel):<16}"
        for a in airlines:
            if a in cells:
                star = "*" if grid["comparison"][a]["cheapest_channel"] == channel else " "
                line += f"{int(round(cells[a])):>{width-1}}{star}"
            else:
                line += f"{'-':>{width}}"
        print(line)

    print("\nPer-airline cheapest channel:")
    for a in airlines:
        c = grid["comparison"][a]
        print(f"  {a:<4} {_label(c['cheapest_channel']):<16} "
              f"{int(round(c['cheapest_price']))} BDT")
    print("=" * 78)


def write_outputs(report: dict[str, Any], out_dir: Path, stamp: str) -> tuple[Path, Path]:
    q = report["query"]
    base = f"ota_compare_{q['origin']}{q['destination']}_{q['date']}_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / f"{base}.json"
    json_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    csv_path = out_dir / f"{base}.csv"
    grid = report["grid"]
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["channel"] + grid["airlines"])
        for channel in report["sources"]:
            row = [_label(channel)]
            cells = grid["price_matrix"].get(channel, {})
            row += [cells.get(a, "") for a in grid["airlines"]]
            w.writerow(row)
    return json_path, csv_path


def _parse_har_args(har_args: list[str]) -> dict[str, str]:
    """Parse repeated --har channel=path into {channel: path}."""
    out: dict[str, str] = {}
    for item in har_args or []:
        if "=" not in item:
            raise SystemExit(f"--har must be channel=path, got: {item!r}")
        channel, path = item.split("=", 1)
        channel = channel.strip().lower()
        if channel not in HAR_CHANNELS:
            raise SystemExit(f"unknown HAR channel {channel!r}; "
                             f"valid: {', '.join(HAR_CHANNELS)}")
        out[channel] = path.strip()
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="OTA discount comparison grid for a route/date.")
    parser.add_argument("--origin", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--date", required=True, help="Departure date YYYY-MM-DD")
    parser.add_argument("--cabin", default="economy")
    parser.add_argument("--airline", default=None, help="Optional single-airline filter (e.g. BS)")
    parser.add_argument("--live", default="sharetrip,firsttrip",
                        help="Comma list of live channels (sharetrip,firsttrip); empty to skip")
    parser.add_argument("--har", action="append", default=[],
                        help="Repeatable: channel=path.har  "
                             f"(channels: {', '.join(HAR_CHANNELS)})")
    parser.add_argument("--out", default="output/reports", help="Output directory")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    live = [c.strip().lower() for c in args.live.split(",") if c.strip()]
    for c in live:
        if c not in LIVE_CHANNELS:
            raise SystemExit(f"unknown live channel {c!r}; valid: {', '.join(LIVE_CHANNELS)}")
    har_map = _parse_har_args(args.har)

    results: dict[str, dict[str, Any]] = {}
    for channel in live:
        if not args.quiet:
            print(f"-> live fetch {_label(channel)} ...")
        results[channel] = fetch_live_rows(channel, args.origin, args.destination,
                                           args.date, args.cabin, args.airline)
    for channel, path in har_map.items():
        if not args.quiet:
            print(f"-> parse HAR {_label(channel)} <- {path}")
        results[channel] = parse_har_rows(channel, path, args.origin, args.destination,
                                          args.date, args.cabin, args.airline)

    # Strip raw rows out of the persisted "sources" block (kept only for the grid).
    grid = build_grid(results)
    sources = {ch: {k: v for k, v in res.items() if k != "rows"}
               for ch, res in results.items()}

    report = {
        "generated_at": datetime.now().isoformat(),
        "query": {"origin": args.origin.upper(), "destination": args.destination.upper(),
                  "date": args.date, "cabin": args.cabin, "airline": args.airline},
        "sources": sources,
        "grid": grid,
    }

    if not args.quiet:
        render_console(report)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path, csv_path = write_outputs(report, Path(args.out), stamp)
    print(f"\nSaved: {json_path}")
    print(f"Saved: {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
