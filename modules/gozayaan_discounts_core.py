"""Pure logic for the live GoZayaan discount pull — no network, no token minting.

Kept apart from the live collector so it can be tested in CI: importing the
minting machinery (modules.gozayaan) is deliberately heavy and network-adjacent,
while everything here is a plain data transform over JSON the search already
returned. The live collector composes these with the minted requests.

The HAR-bridge writer is the integration point: the live pull serializes its
calls into a synthetic GoZayaan HAR, dropped into the capture folder, and the
UNCHANGED discount report reads it exactly like a browser-exported one. Same
idea as the ShareTrip live plugin — nothing in the report engine has to know a
capture was made by hand or by the pull.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List

#: Airports that count as domestic; anything else makes the leg international.
DOMESTIC_AIRPORTS = {"DAC", "CGP", "CXB", "ZYL", "JSR", "SPD", "RJH", "BZL", "IRD"}

#: Relative to the API base (".../api"), exactly as the web app calls it.
DISCOUNT_PATH = "/business_rules/get_discount_list/"


def flight_type_for(origin: str, destination: str) -> str:
    """GoZayaan's flight_type: DOM when both ends are domestic, else OUTBOUND."""
    o, d = str(origin).upper(), str(destination).upper()
    return "DOM" if o in DOMESTIC_AIRPORTS and d in DOMESTIC_AIRPORTS else "OUTBOUND"


def carrier_prices_from_legs(legs_result: Any) -> Dict[str, int]:
    """{carrier -> cheapest gross total} from a search/legs response `result`.

    product_price in a discount request is the gross booking total of the
    carrier's cheapest offer. The carrier is the head of each fare's hash_str
    ('BS|DAC-CXB-...'); one search therefore prices every carrier at once, which
    is why per-airline clicking was never actually required.

    Codes stay exactly as GoZayaan spells them: they go back to GoZayaan as
    plating_carrier, which must be ITS code. Our display alias (3L -> G9) is
    applied once, when rows are parsed (gozayaan_har.rows_from_discount_list).
    Aliasing here once sent Air Arabia Abu Dhabi's price as another carrier's.
    """
    out: Dict[str, int] = {}
    result = legs_result.get("result") if isinstance(legs_result, dict) else None
    fares = (result or {}).get("fares") if isinstance(result, dict) else None
    if not isinstance(fares, list):
        return out
    for fare in fares:
        if not isinstance(fare, dict):
            continue
        price = float(fare.get("total_fare_amount") or 0)
        if price <= 0:
            continue
        hash_str = str(fare.get("hash_str") or "")
        carrier = hash_str.split("|", 1)[0].strip().upper() if hash_str else ""
        if not carrier:
            continue
        cur = out.get(carrier)
        if cur is None or price < cur:
            out[carrier] = round(price)
    return out


def error_summary(body: Any) -> str:
    """GoZayaan's own words from an error envelope, e.g. 'code=429 message=...'.

    GoZayaan can answer HTTP 200 with an `error` object, so the status alone
    says nothing; without this line a stop reads "rate-limited (status 200)"
    and nobody can tell a real limit from a differently worded failure.
    """
    if not isinstance(body, dict):
        return f"non-JSON body: {str(body or '')[:160]!r}"
    err = body.get("error")
    if isinstance(err, dict):
        parts = [f"{k}={str(err.get(k)).strip()[:160]}"
                 for k in ("code", "message", "detail") if err.get(k) not in (None, "")]
        if parts:
            return " ".join(parts)
    if err:
        return f"error={str(err)[:160]}"
    return f"keys={sorted(body)[:8]}"


def discount_body(search_id: str, carrier: str, flight_type: str,
                  product_price: int) -> Dict[str, Any]:
    """The exact body the web app posts to get_discount_list."""
    return {
        "type": "FLIGHT",
        "region": "BD",
        "currency": "BDT",
        "platform_type": "GZ_WEB",
        "search_id": str(search_id),
        "plating_carrier": carrier,
        "flight_type": flight_type,
        "product_price": int(product_price),
    }


def _har_entry(url: str, request_body: Dict[str, Any],
               response_obj: Any) -> Dict[str, Any]:
    """One HAR entry shaped so gozayaan_har.parse_discounts reads it verbatim.

    parse_discounts keys off the request body (plating_carrier / flight_type /
    product_price) and the response text, so both are preserved exactly.
    """
    return {
        "request": {
            "method": "POST", "url": url,
            "postData": {"mimeType": "application/json",
                         "text": json.dumps(request_body)},
        },
        "response": {
            "status": 200,
            "content": {"mimeType": "application/json",
                        "text": json.dumps(response_obj)},
        },
    }


def build_live_har(calls: List[Dict[str, Any]], *,
                   base_url: str = "https://production.gozayaan.com/api") -> Dict[str, Any]:
    """A synthetic GoZayaan HAR from the live discount calls.

    `calls` is a list of {request_body, response} as sent/received by the pull.
    The result is a normal HAR dict the discount report parses unchanged.
    """
    url = base_url.rstrip("/") + DISCOUNT_PATH
    entries = [_har_entry(url, c["request_body"], c["response"]) for c in calls]
    return {
        "log": {
            "version": "1.2",
            "creator": {"name": "gozayaan_discounts_live", "version": "1.0"},
            "_generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "entries": entries,
        }
    }


def write_live_har(calls: List[Dict[str, Any]], out_path: str, *,
                   base_url: str = "https://production.gozayaan.com/api") -> int:
    """Write the synthetic HAR to `out_path`. Returns the number of calls written."""
    from pathlib import Path
    har = build_live_har(calls, base_url=base_url)
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(har), encoding="utf-8")
    return len(calls)
