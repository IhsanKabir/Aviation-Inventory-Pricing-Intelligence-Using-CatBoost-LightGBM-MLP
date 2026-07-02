"""
Automated HAR collector for the OTA discount grid.

Drives a persistent Chrome profile with Playwright, navigates each portal's
flight search for the requested route(s)/date, and records a HAR per channel
that the existing parsers (gozayaan_har, sharetrip_har, firsttrip, amyweb,
akijair_har, bdfare_har) already know how to read.

Auth model: a persistent profile (default output/manual_sessions/discount_profile).
Log in ONCE per portal in that profile (password or Google OAuth); the session
is reused on every run until it expires.

Channels differ in how their search is triggered:
  * url   - navigate to a constructed search URL (GoZayaan, ShareTrip)
  * form  - fill origin/dest/date and submit (Amy, BDFare, AKIJ, FT B2B) [TODO]

Usage:
  python tools/collect_discount_hars.py --date 2026-07-30 --routes DAC-CGP,DAC-DXB
  python tools/collect_discount_hars.py --date 2026-07-30 --routes DAC-CGP --channels gozayaan
  # first time (to log in): add --headed and complete logins in the opened browser
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urlencode

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_PROFILE = REPO_ROOT / "output" / "manual_sessions" / "discount_profile"
DEFAULT_OUT = REPO_ROOT / "output" / "discount_hars"


def _gozayaan_url(origin: str, destination: str, date: str) -> str:
    params = {"adult": 1, "child": 0, "child_age": "", "infant": 0,
              "cabin_class": "Economy", "trips": f"{origin},{destination},{date}"}
    return "https://gozayaan.com/flight/list?" + urlencode(params)


def _sharetrip_url(origin: str, destination: str, date: str) -> str:
    params = {"adult": 1, "child": 0, "child2To5Count": 0, "child6To12Count": 0,
              "class": "Economy", "depart": date, "destination": destination,
              "destinationCode": destination, "origin": origin, "originCode": origin,
              "tripType": "OneWay"}
    return "https://sharetrip.net/flight-search?" + urlencode(params)


# channel -> config. `wait_for` are URL substrings whose responses mark "search done".
CHANNELS: dict[str, dict[str, Any]] = {
    "gozayaan": {
        "mode": "url",
        "url": _gozayaan_url,
        "wait_for": ["/search/legs/", "/business_rules/get_discount_list/"],
        "needs_login": False,
        "settle_ms": 4000,
    },
    "sharetrip": {
        "mode": "url",
        "url": _sharetrip_url,
        "wait_for": ["/flight/search/available-flights"],
        "needs_login": False,
        "settle_ms": 4000,
    },
    # form-based / login channels are added next:
    # "amy", "bdfare", "akij", "firsttrip_b2b"
}


def _collect_channel(context, channel: str, cfg: dict[str, Any],
                     routes: list[tuple[str, str]], date: str, timeout_s: float) -> dict[str, Any]:
    """Navigate every route for one channel; return {seen endpoints, routes done}."""
    page = context.new_page()
    page.set_default_timeout(int(timeout_s * 1000))
    seen: set[str] = set()

    def on_response(resp) -> None:
        url = str(resp.url or "")
        for needle in cfg["wait_for"]:
            if needle in url and resp.status == 200:
                seen.add(needle)

    page.on("response", on_response)

    routes_done = []
    for origin, destination in routes:
        target = cfg["url"](origin, destination, date)
        try:
            page.goto(target, wait_until="domcontentloaded", timeout=int(timeout_s * 1000))
        except Exception as exc:  # noqa: BLE001
            print(f"  [{channel}] {origin}-{destination}: navigation error: {exc}")
            continue
        # wait until the key endpoints for this channel are observed (or timeout)
        deadline = time.time() + timeout_s
        while time.time() < deadline and not all(n in seen for n in cfg["wait_for"]):
            page.wait_for_timeout(250)
        page.wait_for_timeout(cfg.get("settle_ms", 3000))  # let late XHRs flush into the HAR
        routes_done.append(f"{origin}-{destination}")
        print(f"  [{channel}] {origin}-{destination}: captured {sorted(seen)}")

    page.close()
    return {"seen": sorted(seen), "routes": routes_done,
            "ok": all(n in seen for n in cfg["wait_for"])}


def collect(channels: list[str], routes: list[tuple[str, str]], date: str,
            profile_dir: Path, out_dir: Path, headed: bool, timeout_s: float) -> dict[str, Path]:
    from playwright.sync_api import sync_playwright

    out_dir.mkdir(parents=True, exist_ok=True)
    profile_dir.mkdir(parents=True, exist_ok=True)
    har_paths: dict[str, Path] = {}

    with sync_playwright() as p:
        for channel in channels:
            cfg = CHANNELS[channel]
            har_path = out_dir / f"{channel}.har"
            # One persistent context per channel, recording a HAR for that session.
            context = p.chromium.launch_persistent_context(
                str(profile_dir),
                headless=not headed,
                channel="chrome",
                record_har_path=str(har_path),
                record_har_content="embed",
            )
            try:
                print(f"-> collecting {channel} ({'login: yes' if cfg['needs_login'] else 'public'}) ...")
                result = _collect_channel(context, channel, cfg, routes, date, timeout_s)
                if result["ok"]:
                    har_paths[channel] = har_path
                else:
                    print(f"  [{channel}] WARNING: expected endpoints not all seen "
                          f"({result['seen']}); HAR may be incomplete.")
                    har_paths[channel] = har_path
            finally:
                context.close()  # flushes the HAR to disk
            print(f"   saved {har_path}")
    return har_paths


def _parse_routes(value: str) -> list[tuple[str, str]]:
    routes = []
    for item in value.split(","):
        item = item.strip().upper()
        if item and "-" in item:
            o, d = item.split("-", 1)
            routes.append((o, d))
    return routes


def main() -> int:
    p = argparse.ArgumentParser(description="Auto-collect discount HARs via Playwright.")
    p.add_argument("--date", required=True, help="Travel date YYYY-MM-DD")
    p.add_argument("--routes", required=True, help="Comma list ORIGIN-DEST (e.g. DAC-CGP,DAC-DXB)")
    p.add_argument("--channels", default=",".join(CHANNELS),
                   help=f"Comma list; available: {', '.join(CHANNELS)}")
    p.add_argument("--profile-dir", default=str(DEFAULT_PROFILE))
    p.add_argument("--out", default=str(DEFAULT_OUT))
    p.add_argument("--headed", action="store_true", help="Show the browser (needed for first-time login)")
    p.add_argument("--timeout", type=float, default=30.0)
    args = p.parse_args()

    channels = [c.strip().lower() for c in args.channels.split(",") if c.strip()]
    for c in channels:
        if c not in CHANNELS:
            raise SystemExit(f"unknown channel {c!r}; available: {', '.join(CHANNELS)}")
    routes = _parse_routes(args.routes)
    if not routes:
        raise SystemExit("no valid --routes")

    har_paths = collect(channels, routes, args.date, Path(args.profile_dir),
                        Path(args.out), args.headed, args.timeout)
    print("\nRecorded HARs:")
    for channel, path in har_paths.items():
        print(f"  --{channel.replace('_', '-')}-har \"{path}\"")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
