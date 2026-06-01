"""
FirstTrip OTA connector (b2c-api.firsttrip.com).

No authentication required. Responds as SSE stream with all airlines
on the queried route in a single POST call.

Contract: fetch_flights(...) -> {"ok": bool, "rows": [...], "raw": {...}}

Each row matches the normalised schema expected by saudi_route_scrape.py
and flight_offers / flight_offer_raw_meta.
"""
from __future__ import annotations

import logging
import os
import re
import time
from typing import Any, Dict, List, Optional

import requests

LOG = logging.getLogger(__name__)

API_SEARCH = "https://b2c-api.firsttrip.com/flight/api/v1/Search"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/148.0.0.0 Safari/537.36"
)
CABIN_MAP = {"economy": 1, "business": 2, "first": 3, "premium_economy": 4}

CF_HEADER = "cf-ray-status-id-tn"  # server rotates sxsrf via this response header
ENV_SXSRF = "FIRSTTRIP_SXSRF"    # optional override from env


def _duration_to_min(text: str) -> Optional[int]:
    """'3h 55m' -> 235"""
    if not text:
        return None
    h = re.search(r"(\d+)h", text)
    m = re.search(r"(\d+)m", text)
    return (int(h.group(1)) * 60 if h else 0) + (int(m.group(1)) if m else 0)


def _baggage_label(segment: Dict[str, Any]) -> Optional[str]:
    pax = segment.get("passengerBaggages") or []
    if pax:
        p = pax[0]
        kg = p.get("checkInBaggageInKg")
        pc = p.get("checkInBaggageInPieces")
        if kg:
            return f"{int(kg)} KG"
        if pc:
            return f"{int(pc)} Piece{'s' if int(pc) > 1 else ''}"
    bags = segment.get("baggages") or []
    if bags:
        b = bags[0]
        amt = b.get("cargoAmount")
        unit = b.get("cargoUnit", "")
        if amt:
            return f"{int(amt)} {unit}".strip()
    return None


def _normalize(offer: Dict[str, Any], requested_cabin: str,
                airline_filter: Optional[str]) -> Optional[Dict[str, Any]]:
    carrier = str(offer.get("marketingCarrierCode") or "").upper().strip()
    if not carrier:
        return None
    if airline_filter and carrier != airline_filter.upper():
        return None

    directions = offer.get("directions") or []
    if not directions or not directions[0]:
        return None
    leg = directions[0][0]
    segments: List[Dict] = leg.get("segments") or []
    if not segments:
        return None

    seg0   = segments[0]
    seg_last = segments[-1]

    # Build via_airports from intermediate stops
    if len(segments) > 1:
        via = "|".join(s["destinationAirportCode"] for s in segments[:-1])
    else:
        via = None

    stops        = int(leg.get("stops") or 0)
    dur_text     = leg.get("totalFlightDuration") or seg0.get("flightDuration") or ""
    dur_min      = _duration_to_min(dur_text)
    layover_times = leg.get("layoverTimes") or []   # e.g. ["3h 20m", "1h 45m"]

    origin      = str(seg0.get("originAirportCode") or "").upper()
    destination = str(seg_last.get("destinationAirportCode") or "").upper()
    departure   = seg0.get("departureTime")
    arrival     = seg_last.get("arrivalTime")
    flight_num  = str(seg0.get("flightNumber") or "").strip()
    aircraft    = str(seg0.get("aircraftModel") or "").strip() or None
    fare_basis  = str(seg0.get("fareBasisCode") or offer.get("brandedFare") or "").strip() or None
    cabin       = str(seg0.get("cabinClass") or requested_cabin or "Economy").strip()
    baggage     = _baggage_label(seg0)
    seats       = int(leg.get("availableSeats") or 0) or None

    price      = float(offer.get("finalTotalPrice") or offer.get("totalPrice") or 0)
    base_price = float(offer.get("finalBasePrice") or offer.get("basePrice") or 0)
    tax        = float(offer.get("finalTaxPrice") or offer.get("taxes") or 0)

    if not origin or not destination or not departure or price <= 0:
        return None

    return {
        "airline":              carrier,
        "operating_airline":    str(seg0.get("operatingCarrierCode") or carrier).upper(),
        "flight_number":        flight_num,
        "origin":               origin,
        "destination":          destination,
        "departure":            departure,
        "arrival":              arrival,
        "cabin":                cabin,
        "fare_basis":           fare_basis,
        "brand":                "FIRSTTRIP_OTA",
        "price_total_bdt":      price,
        "fare_amount":          base_price,
        "tax_amount":           tax,
        "currency":             "BDT",
        "duration_min":         dur_min,
        "stops":                stops,
        "via_airports":         via,
        "layover_times":        layover_times,
        "aircraft":             aircraft,
        "baggage":              baggage,
        "seat_available":       seats,
        "seat_capacity":        None,
        "fare_refundable":      bool(offer.get("refundable")),
        "adt_count":            1,
        "chd_count":            0,
        "inf_count":            0,
        "source_endpoint":      API_SEARCH,
        "inventory_confidence": None,
    }


def _bootstrap_sxsrf(timeout: int = 15) -> Optional[str]:
    """
    Get a fresh sxsrf by making a no-auth request to AirportsLastUpdateDate.
    Even a 401 response includes cf-ray-status-id-tn which bootstraps the session.
    Client encodes it with btoa(btoa(value)) to produce the sxsrf token.
    """
    import base64
    try:
        r = requests.get(
            "https://b2c-api.firsttrip.com/flight/api/v1/GeneralPurpose/AirportsLastUpdateDate",
            headers={
                "User-Agent":    USER_AGENT,
                "Origin":        "https://firsttrip.com",
                "Referer":       "https://firsttrip.com/",
                "platformtypeid": "1",
            },
            timeout=timeout,
        )
        cf = r.headers.get(CF_HEADER)
        if cf:
            token = base64.b64encode(
                base64.b64encode(cf.encode()).decode().encode()
            ).decode()
            LOG.debug("[firsttrip] bootstrapped sxsrf from cf-ray header")
            return token
    except Exception as exc:
        LOG.warning("[firsttrip] bootstrap failed: %s", exc)
    return None


# Module-level cached sxsrf (rotated per response)
_current_sxsrf: Optional[str] = None


def _get_sxsrf() -> str:
    global _current_sxsrf
    env_val = os.getenv(ENV_SXSRF)
    if env_val:
        return env_val
    if not _current_sxsrf:
        _current_sxsrf = _bootstrap_sxsrf() or ""
    return _current_sxsrf or ""


def _update_sxsrf(response: requests.Response) -> None:
    global _current_sxsrf
    import base64
    cf = response.headers.get(CF_HEADER)
    if cf:
        _current_sxsrf = base64.b64encode(
            base64.b64encode(cf.encode()).decode().encode()
        ).decode()


def fetch_flights(
    origin: str,
    destination: str,
    date: str,
    cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
    airline_code: Optional[str] = None,
    timeout: int = 90,
) -> Dict[str, Any]:
    cabin_id = CABIN_MAP.get(cabin.lower(), 1)
    payload = {
        "tripTypeId": 1,
        "routes": [{"origin": origin.upper(), "destination": destination.upper(),
                     "departureDate": str(date)}],
        "adults":   max(1, int(adt or 1)),
        "childs":   max(0, int(chd or 0)),
        "infants":  max(0, int(inf or 0)),
        "cabinClass": cabin_id,
        "preferredCarriers":  [],
        "prohibitedCarriers": [],
        "childrenAges": [],
        "promoCode": "",
        "fareType": 1,
        "isComboFare": False,
    }
    sxsrf = _get_sxsrf()
    headers = {
        "Content-Type":  "application/json",
        "User-Agent":    USER_AGENT,
        "Origin":        "https://firsttrip.com",
        "Referer":       "https://firsttrip.com/",
        "Accept":        "application/json, text/event-stream",
        "platformtypeid": "1",
        **({} if not sxsrf else {"sxsrf": sxsrf}),
    }

    out: Dict[str, Any] = {"source": "firsttrip", "ok": False, "rows": [], "raw": {}}
    t0 = time.time()
    try:
        resp = requests.post(API_SEARCH, json=payload, headers=headers,
                             timeout=timeout, stream=True)
        _update_sxsrf(resp)  # rotate token from response header
        out["raw"]["status"] = resp.status_code
        if resp.status_code == 401:
            # sxsrf expired — bootstrap and retry once
            new_sxsrf = _bootstrap_sxsrf()
            if new_sxsrf:
                headers["sxsrf"] = new_sxsrf
                resp = requests.post(API_SEARCH, json=payload, headers=headers,
                                     timeout=timeout, stream=True)
                _update_sxsrf(resp)
                out["raw"]["status"] = resp.status_code
        if resp.status_code != 200:
            out["raw"]["error"] = f"HTTP {resp.status_code}"
            LOG.warning("[firsttrip] %s->%s %s: HTTP %s", origin, destination,
                        date, resp.status_code)
            return out

        raw_text = resp.text
        out["raw"]["response_len"] = len(raw_text)

        # Parse SSE chunks
        rows: List[Dict] = []
        for chunk in raw_text.split("\ndata: "):
            chunk = chunk.replace("data: ", "", 1).strip()
            if not chunk:
                continue
            try:
                data = json_safe_loads(chunk)
                flights = (
                    (data.get("data") or {})
                    .get("airSearchResponseWithFilters", {})
                    .get("airSearchResponses", [])
                )
                for offer in flights:
                    row = _normalize(offer, cabin, airline_code)
                    if row:
                        rows.append(row)
            except Exception:
                pass

        out["rows"] = rows
        out["ok"]   = len(rows) > 0
        out["raw"]["elapsed_sec"] = round(time.time() - t0, 2)
        out["raw"]["total_offers"] = len(rows)

    except requests.exceptions.Timeout:
        out["raw"]["error"] = "timeout"
        LOG.warning("[firsttrip] %s->%s %s: timeout after %ss",
                    origin, destination, date, timeout)
    except Exception as exc:
        out["raw"]["error"] = str(exc)
        LOG.warning("[firsttrip] %s->%s %s: %s", origin, destination, date, exc)

    return out


def json_safe_loads(text: str) -> Any:
    import json
    return json.loads(text)
