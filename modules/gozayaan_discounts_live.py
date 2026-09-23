"""Live GoZayaan published-coupon discounts — no manual HAR, no per-airline clicking.

Reproduces exactly what the GoZayaan web app does when a user searches a route:
mint the site's own short-lived ``x-kong-segment-id`` (the RSA key GoZayaan ships
in its own JS bundle; minted by ``modules.gozayaan``), register one flight search,
then request the published coupon list for each carrier the search returned.

The coupon list endpoint is keyed ``(plating_carrier, flight_type, product_price)``
with NO route dimension, so a single search per market yields every carrier's
discounts — which is why per-airline clicking was never actually necessary.

Citizenship (non-negotiable — this is why the site tolerates it):
  * single-threaded, one request at a time;
  * ``INTER_QUERY_SLEEP`` between calls;
  * a fresh, short-lived token minted per request (the token lives ~14s);
  * STOP on the first rate-limit signal — a 429 costs a 15-minute cooldown for
    everyone, so the run aborts rather than retrying into a block.

Parsing reuses ``gozayaan_har.rows_from_discount_list`` verbatim, so a live row is
byte-identical to the same coupon read from a captured HAR.
"""
from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional

from modules import gozayaan as gz
from modules.gozayaan_discounts_core import (
    carrier_prices_from_legs, discount_body, error_summary, flight_type_for,
    write_live_har)
from modules.gozayaan_har import rows_from_discount_list
from modules.requester import Requester

DISCOUNT_URL = gz.API_BASE.rstrip("/") + "/business_rules/get_discount_list/"


def _fresh_headers(search_payload: Dict[str, Any]) -> Dict[str, str]:
    """Headers carrying a freshly minted x-kong-segment-id (it expires in ~14s)."""
    ctx = gz._resolve_active_kong_token(min_ttl_sec=0, search_payload=search_payload)
    token = str((ctx or {}).get("token") or "").strip()
    return gz._build_headers(token=token or None)


def fetch_route_discounts(origin: str, destination: str, date: str, *,
                          cabin: str = "Economy", adt: int = 1, chd: int = 0,
                          inf: int = 0, sleep_s: float = 3.0,
                          log: Callable[[str], None] = print) -> Dict[str, Any]:
    """Live published-coupon discounts for one route/date. No HAR.

    Returns {ok, rows, search_id, carriers, flight_type, reason}. `rows` are the
    same shape as gozayaan_har.parse_discounts. Aborts (ok=False) on the first
    rate-limit response rather than retrying into a cooldown.
    """
    ftype = flight_type_for(origin, destination)
    search_payload = gz.build_search_payload(origin=origin, destination=destination,
                                             date=date, cabin=cabin, adt=adt,
                                             chd=chd, inf=inf)
    req = Requester(cookies_path=None, user_agent=gz.USER_AGENT, proxy_url=None)
    out: Dict[str, Any] = {"ok": False, "rows": [], "search_id": None,
                           "carriers": {}, "flight_type": ftype, "reason": "",
                           "calls": []}   # {request_body, response} for the HAR bridge

    def _stop_if_limited(status: int, body: Any, where: str) -> bool:
        if gz._is_rate_limited(body, status):
            # Record it in the SAME cooldown file the pipeline honours, so neither
            # a relaunch nor the BS/2A fallback walks straight back into the limit.
            gz._record_rate_limit_hit(status_code=status, body=body)
            out["reason"] = (f"rate-limited on {where} (status {status}; "
                             f"GoZayaan said: {error_summary(body)}) — stopping")
            log("  ! " + out["reason"])
            return True
        return False

    # 1) register the search -> search_id
    status, body = gz._post_json(req, gz.SEARCH_URL, search_payload,
                                 _fresh_headers(search_payload))
    if _stop_if_limited(status, body, "search"):
        return out
    search_id = None
    if isinstance(body, dict):
        search_id = ((body.get("result") or {}).get("search_id")
                     or body.get("search_id"))
    if not search_id:
        out["reason"] = f"no search_id (status {status})"
        return out
    out["search_id"] = str(search_id)
    time.sleep(sleep_s)

    # 2) poll legs -> per-carrier gross prices
    status, legs = gz._post_json(req, gz.LEGS_URL,
                                 {"search_id": str(search_id), "leg_type": "L1"},
                                 _fresh_headers(search_payload))
    if _stop_if_limited(status, legs, "legs"):
        return out
    prices = carrier_prices_from_legs(legs)
    out["carriers"] = prices
    if not prices:
        out["reason"] = f"search returned no carriers (status {status})"
        return out
    log(f"  {origin}-{destination} {ftype}: {len(prices)} carriers -> {sorted(prices)}")

    # 3) one discount-list call per carrier (fresh token each), parse identically
    rows: List[Dict[str, Any]] = []
    seen: set = set()
    for carrier, price in sorted(prices.items()):
        time.sleep(sleep_s)
        body = discount_body(str(search_id), carrier, ftype, price)
        status, resp = gz._post_json(req, DISCOUNT_URL, body,
                                     _fresh_headers(search_payload))
        if _stop_if_limited(status, resp, f"discounts/{carrier}"):
            out["rows"] = rows          # keep what we already have
            return out
        if not isinstance(resp, dict):
            log(f"  ! {carrier}: non-JSON discount response (status {status})")
            continue
        out["calls"].append({"request_body": body, "response": resp})
        got = rows_from_discount_list(plating_carrier=carrier, flight_type=ftype,
                                      product_price=price, data=resp, seen=seen)
        rows.extend(got)
        log(f"    {carrier}: {len(got)} coupons")

    out["rows"] = rows
    out["ok"] = bool(rows)
    return out


def fetch_many(routes: List[tuple], date: str, *, cabin: str = "Economy",
               sleep_s: float = 3.0, log: Callable[[str], None] = print) -> Dict[str, Any]:
    """Several routes in one run. Stops early if any route hits a rate limit,
    keeping every row collected so far."""
    all_rows: List[Dict[str, Any]] = []
    all_calls: List[Dict[str, Any]] = []
    per_route: Dict[str, Any] = {}
    active = gz._active_rate_limit_state()
    if active:
        return {"rows": [], "calls": [], "routes": {}, "cooldown": active}
    for origin, destination in routes:
        res = fetch_route_discounts(origin, destination, date, cabin=cabin,
                                    sleep_s=sleep_s, log=log)
        per_route[f"{origin}-{destination}"] = {
            "ok": res["ok"], "coupons": len(res["rows"]),
            "carriers": sorted(res["carriers"]), "reason": res["reason"]}
        all_rows.extend(res["rows"])
        all_calls.extend(res.get("calls") or [])
        if res["reason"].startswith("rate-limited"):
            log("  stopping the batch to respect the cooldown")
            break
    return {"rows": all_rows, "calls": all_calls, "routes": per_route}


def pull_to_har(routes: List[tuple], date: str, out_har_path: str, *,
                cabin: str = "Economy", sleep_s: float = 3.0,
                log: Callable[[str], None] = print) -> Dict[str, Any]:
    """Live-pull several routes and write a synthetic GoZayaan HAR the discount
    report reads unchanged. Returns {rows, routes, har_written, har_path}."""
    result = fetch_many(routes, date, cabin=cabin, sleep_s=sleep_s, log=log)
    n = write_live_har(result["calls"], out_har_path) if result["calls"] else 0
    result["har_written"] = n
    result["har_path"] = out_har_path if n else None
    if n:
        log(f"  wrote {n} discount calls -> {out_har_path}")
    return result
