"""
Learn EXACT checked-baggage per (airline, cabin) from FirstTrip's live BrandedFare
endpoint, and cache it for the KSA report's baggage-fill cascade.

FirstTrip's plain search returns economy baggage but not business; the "View Prices"
modal calls /flight/api/v1/BrandedFare, which returns per-brand baggage for both
cabins. Baggage is airline policy (static per airline+cabin), so we learn each
airline once (first offer seen) and cache the value — a few dozen calls total.

Output: config/baggage_firsttrip.json  {"airlines": {"QR": {"economy": "25-35 KG", ...}}}
Run:    python tools/firsttrip_baggage_probe.py
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules.firsttrip import API_SEARCH, USER_AGENT, _get_sxsrf, _update_sxsrf

BRANDED_URL = "https://b2c-api.firsttrip.com/flight/api/v1/BrandedFare"
OUT_PATH = REPO_ROOT / "config" / "baggage_firsttrip.json"
ROUTES = [("DAC", "JED"), ("DAC", "RUH"), ("DAC", "DMM"), ("DAC", "MED"),
          ("CGP", "JED"), ("ZYL", "JED")]
CABINS = {"economy": 1, "business": 2}
PROBE_DATE = "2026-08-26"


def _headers() -> dict:
    sx = _get_sxsrf()
    h = {"Content-Type": "application/json", "User-Agent": USER_AGENT,
         "Origin": "https://firsttrip.com", "Referer": "https://firsttrip.com/",
         "Accept": "application/json, text/event-stream", "platformtypeid": "1"}
    if sx:
        h["sxsrf"] = sx
    return h


def _search(origin: str, dest: str, cabin_id: int) -> list:
    payload = {"tripTypeId": 1, "routes": [{"origin": origin, "destination": dest,
               "departureDate": PROBE_DATE}], "adults": 1, "childs": 0, "infants": 0,
               "cabinClass": cabin_id, "preferredCarriers": [], "prohibitedCarriers": [],
               "childrenAges": [], "promoCode": "", "fareType": 1, "isComboFare": False}
    try:
        r = requests.post(API_SEARCH, json=payload, headers=_headers(), timeout=90, stream=True)
        _update_sxsrf(r)
    except requests.RequestException:
        return []
    offers = []
    for chunk in r.text.split("\ndata: "):
        c = chunk.replace("data: ", "", 1).strip()
        if not c:
            continue
        try:
            d = json.loads(c)
        except json.JSONDecodeError:
            continue
        offers += (d.get("data") or {}).get("airSearchResponseWithFilters", {}).get("airSearchResponses", [])
    return offers


def _branded(offer: dict) -> list:
    try:
        seg = offer["directions"][0][0]["segments"][0]
    except (KeyError, IndexError, TypeError):
        return []
    body = {"itemCodeRef": offer.get("itemCodeRef"), "uniqueTransID": offer.get("uniqueTransID"),
            "segmentCodeRefs": [seg.get("segmentCodeRef")] if seg.get("segmentCodeRef") else []}
    try:
        r = requests.post(BRANDED_URL, json=body, headers=_headers(), timeout=60)
        if r.status_code != 200:
            return []
        return (r.json().get("data") or {}).get("tripWiseBrandedFare") or []
    except (requests.RequestException, ValueError):
        return []


def _consolidate(kgs: set, pcs: set) -> str | None:
    parts = []
    if kgs:
        lo, hi = min(kgs), max(kgs)
        parts.append(f"{lo} KG" if lo == hi else f"{lo}-{hi} KG")
    parts += [f"{p} Piece" + ("s" if p > 1 else "") for p in sorted(pcs)]
    return " / ".join(parts) if parts else None


def _baggage_from_brands(brands: list) -> str | None:
    kgs: set = set()
    pcs: set = set()
    for f in brands:
        try:
            pb = (f["directions"][0][0]["segments"][0].get("passengerBaggages") or [{}])[0]
        except (KeyError, IndexError, TypeError):
            continue
        if pb.get("checkInBaggageInKg"):
            kgs.add(int(pb["checkInBaggageInKg"]))
        if pb.get("checkInBaggageInPieces"):
            pcs.add(int(pb["checkInBaggageInPieces"]))
    return _consolidate(kgs, pcs)


def main() -> int:
    result: dict = {}
    for cabin, cid in CABINS.items():
        for origin, dest in ROUTES:
            offers = _search(origin, dest, cid)
            seen_air = set()
            for off in offers:
                ac = str(off.get("marketingCarrierCode") or "").upper()
                if not ac or ac in seen_air:
                    continue
                if result.get(ac, {}).get(cabin):  # already learned this airline+cabin
                    seen_air.add(ac)
                    continue
                bag = _baggage_from_brands(_branded(off))
                time.sleep(0.4)
                if bag:
                    result.setdefault(ac, {})[cabin] = bag
                    print(f"  {ac} {cabin}: {bag}  ({origin}->{dest})")
                seen_air.add(ac)
            time.sleep(0.6)
    OUT_PATH.write_text(json.dumps({"airlines": result}, indent=1), encoding="utf-8")
    print(f"\nLearned {len(result)} airlines -> {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
