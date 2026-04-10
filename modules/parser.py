# modules/parser.py
"""
Parser for Biman (and similar) GraphQL JSON responses.

Features:
- Extracts offers from multiple response shapes (originalResponse, unbundledAlternateDateOffers, brandedResults, etc.)
- Returns list of flat dict rows including fare breakdown, tax components, baggage, equipment mapping,
  seat capacity and an estimated load factor.
- Default behavior: skip sold-out offers (Option A). Pass keep_soldout=True to keep them.
"""

from __future__ import annotations
import json
import logging
import re
from html import unescape
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from modules.fleet_mapping import resolve_seat_capacity
from modules.penalties import parse_bg_category16_penalties, parse_gozayaan_policies

LOG = logging.getLogger("modules.parser")
LOG.addHandler(logging.NullHandler())


# -------------------------
# Config / defaults
# -------------------------

# Default equipment -> human readable name map (editable by placing equipment_map.json next to this file)
DEFAULT_EQUIPMENT_MAP = {
    # codes commonly observed in Biman responses
    "788": "Boeing 787-8 Dreamliner",
    "789": "Boeing 787-9 Dreamliner",
    "77W": "Boeing 777-300ER",
    "773": "Boeing 777-300",
    "738": "Boeing 737-800",
    "737": "Boeing 737-800",
    "DH8": "Dash 8-Q400",
    "DH8Q": "Dash 8-Q400",
    "Q400": "Dash 8-Q400",
    "320": "Airbus A320",
    "321": "Airbus A321",
    # numeric fallbacks
    "738": "Boeing 737-800",
    "787": "Boeing 787 (unknown variant)",
}

# Seat capacity mapping - updated to the table you provided (Biman source)
# keys are equipment codes or common numeric trims; values = total seats
SEAT_CAPACITY_MAP = {
    # exact codes used in payloads
    "788": 271,   # Boeing 787-8 Dreamliner -> 271 (Biman config)
    "789": 298,   # Boeing 787-9 Dreamliner -> 298
    "77W": 419,   # Boeing 777-300ER -> 419
    "773": 419,
    "737": 162,   # Boeing 737-800 -> 162
    "738": 162,
    "DH8": 74,    # Dash 8-Q400 -> 74
    "Q400": 74,
    "DH8Q": 74,
    "DASH8": 74,
    "DASH8Q400": 74,
    "737-800": 162,
    "777-300ER": 419,
    "787-8": 271,
    "787-9": 298,
    # generic fallbacks
    "320": 150,
    "321": 185,
    "333": 277,
    "332": 247,
}

# For aircraft-level class split guidance (if you later want business/economy counts)
# This is optional metadata, not strictly required; included for future expansion
AIRCRAFT_CLASS_SPLIT = {
    "777-300ER": {"business": 35, "premium_economy": 0, "economy": 384, "total": 419},
    "787-9": {"business": 30, "premium_economy": 21, "economy": 247, "total": 298},
    "787-8": {"business": 24, "premium_economy": 0, "economy": 247, "total": 271},
    "737-800": {"business": 12, "premium_economy": 0, "economy": 150, "total": 162},
    "dash8-q400": {"business": 0, "premium_economy": 0, "economy": 74, "total": 74},
}


def _capacity_from_aircraft(equip_code: str | None, equip_desc: str | None) -> Optional[int]:
    """
    Resolve seat capacity from equipment code/name with robust fallbacks.
    """
    dynamic = resolve_seat_capacity("BG", aircraft=equip_desc, equipment_code=equip_code)
    if dynamic is not None:
        return dynamic

    candidates = []
    if equip_code:
        candidates.append(str(equip_code).strip().upper())
    if equip_desc:
        candidates.append(str(equip_desc).strip().upper())

    for c in candidates:
        c_clean = re.sub(r'[^A-Z0-9\-]', '', c)
        if c_clean in SEAT_CAPACITY_MAP:
            return SEAT_CAPACITY_MAP[c_clean]
        # direct known families
        if "787-8" in c_clean or "B7878" in c_clean:
            return 271
        if "787-9" in c_clean or "B7879" in c_clean:
            return 298
        if "777-300" in c_clean or "77W" in c_clean or "773" in c_clean:
            return 419
        if "737-800" in c_clean or "B7378" in c_clean or c_clean == "737" or c_clean == "738":
            return 162
        if "DASH8" in c_clean or "Q400" in c_clean or "DH8" in c_clean:
            return 74

        # numeric fallback
        mnum = re.search(r'(\d{2,4})', c_clean)
        if mnum:
            n = mnum.group(1)
            if n in SEAT_CAPACITY_MAP:
                return SEAT_CAPACITY_MAP[n]

    return None


# -------------------------
# Helpers
# -------------------------
def _safe_get(d: Dict, path: List[str], default=None):
    cur = d
    try:
        for p in path:
            if cur is None:
                return default
            if isinstance(cur, dict):
                cur = cur.get(p)
            else:
                return default
        return cur
    except Exception:
        return default


def load_equipment_map() -> Dict[str, str]:
    """
    Loads equipment_map.json located in same directory as this module if present.
    Merges with DEFAULT_EQUIPMENT_MAP, allowing overrides.
    """
    try:
        here = Path(__file__).resolve().parent
        p = here / "equipment_map.json"
        if p.exists():
            j = json.loads(p.read_text(encoding="utf-8"))
            # Expect mapping code -> name
            merged = DEFAULT_EQUIPMENT_MAP.copy()
            merged.update(j)
            LOG.info("Loaded equipment_map.json (%d entries)", len(j))
            return merged
    except Exception as e:
        LOG.warning("Could not load equipment_map.json: %s", e)
    return DEFAULT_EQUIPMENT_MAP


EQUIP_MAP = load_equipment_map()


def map_equipment(code: Optional[str], equip_map: Optional[Dict[str, str]] = None) -> Tuple[str, str]:
    """
    Return tuple (equipment_code, human_readable_aircraft_name)
    Defensive: normalizes code strings and tries numeric/alpha fallbacks.
    """
    if equip_map is None:
        equip_map = EQUIP_MAP

    if not code:
        return "", ""

    c = str(code).strip().upper()
    # clean out unexpected characters
    c_clean = re.sub(r'[^A-Z0-9\-]', '', c)
    # direct match
    if c_clean in equip_map:
        return c_clean, equip_map[c_clean]
    # numeric match (e.g., '738' -> 'Boeing 737-800' if mapped)
    num = re.search(r'(\d{2,4})', c_clean)
    if num:
        n = num.group(1)
        if n in equip_map:
            return n, equip_map[n]
    # try a few transforms
    if c_clean.startswith("B"):
        tail = c_clean[1:]
        if tail in equip_map:
            return tail, equip_map[tail]
    # fallback
    return c_clean, f"Unknown ({c_clean})"


def extract_baggage_from_fare_families(fare_families: List[Dict]) -> Dict[str, Optional[str]]:
    """
    Parse marketingText fields in fare_families and return brandId -> baggage string
    Example outputs: "30 KG", "0 KG", None
    """
    res: Dict[str, Optional[str]] = {}
    if not fare_families:
        return res

    kg_rx = re.compile(r'(\d{1,3})\s*(?:kg|KG|Kg|kilogram)', re.IGNORECASE)
    no_baggage_rx = re.compile(r'no (?:checked )?baggage|does not allow|zero baggage', re.IGNORECASE)
    for fam in fare_families:
        # brand id may be at different keys
        brand = fam.get("brandId") or (fam.get("brandLabel") or [{}])[0].get("brandId")
        texts = fam.get("marketingTexts") or fam.get("brandLabel") or []
        combined_text = " ".join(
            [unescape(t.get("marketingText", "")) if isinstance(t, dict) else str(t) for t in texts]
        )
        kg = None
        m = kg_rx.search(combined_text)
        if m:
            kg = f"{m.group(1)} KG"
        elif no_baggage_rx.search(combined_text):
            kg = "0 KG"
        else:
            # fallback search
            m2 = re.search(r'(\d+)\s*KG', combined_text, re.IGNORECASE)
            if m2:
                kg = f"{m2.group(1)} KG"
        res[brand] = kg or None
    return res


def _extract_penalty_fields_for_offer(
    offer: Dict[str, Any],
    original: Dict[str, Any],
    fare_basis: str | None = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not isinstance(offer, dict):
        return out
    if not isinstance(original, dict):
        original = {}

    fb = str(fare_basis or offer.get("fareBasis") or "").strip().upper()

    # 1) BG getBookingFareRules shape (category 16 text parsing)
    seg_rules = original.get("segmentFareRules")
    if isinstance(seg_rules, list):
        for item in seg_rules:
            if not isinstance(item, dict):
                continue
            fbr = item.get("fareBasisRules") or {}
            if not isinstance(fbr, dict):
                continue
            fb_item = str(fbr.get("fareBasis") or "").strip().upper()
            if fb and fb_item and fb != fb_item:
                continue
            fare_rules = fbr.get("fareRules") or []
            if not isinstance(fare_rules, list):
                continue
            cat16_texts = []
            for fr in fare_rules:
                if not isinstance(fr, dict):
                    continue
                if str(fr.get("category") or "").strip() == "16":
                    txt = fr.get("ruleText")
                    if txt:
                        cat16_texts.append(str(txt))
            if cat16_texts:
                out.update(parse_bg_category16_penalties("\n\n".join(cat16_texts)))
                break

    # 2) OTA policy list shape (Gozayaan)
    policies = original.get("policies")
    if isinstance(policies, list) and policies:
        pol = parse_gozayaan_policies(policies)
        for k, v in pol.items():
            if out.get(k) is None and v is not None:
                out[k] = v

    # 3) OTA leg-wise fare rule flags (changeable/refundable)
    leg_rules = offer.get("leg_wise_fare_rules")
    if isinstance(leg_rules, dict) and leg_rules:
        first_leg = next(iter(leg_rules.values()), None)
        if isinstance(first_leg, dict):
            adt_rule = first_leg.get("ADT")
            if isinstance(adt_rule, dict):
                if out.get("fare_changeable") is None and adt_rule.get("changeable") is not None:
                    out["fare_changeable"] = bool(adt_rule.get("changeable"))
                if out.get("fare_refundable") is None and adt_rule.get("refundable") is not None:
                    out["fare_refundable"] = bool(adt_rule.get("refundable"))
                if out.get("penalty_currency") is None and adt_rule.get("currency"):
                    out["penalty_currency"] = str(adt_rule.get("currency"))

    return out


def _parse_price_alternatives(alts: Any) -> Tuple[Optional[float], Optional[str], List[Dict]]:
    """
    alts typically looks like: [[{"amount": 123, "currency": "BDT"}, ...], ...]
    Return (sum_amount or first_amount, currency, list_of_components)
    We try to return a single numeric amount (first found) and a components list
    """
    if not alts:
        return None, None, []
    # If alts is a nested list as in examples
    try:
        comps = []
        # alt[0] might be a list of components for total
        for alt_group in alts:
            if isinstance(alt_group, list):
                for comp in alt_group:
                    if isinstance(comp, dict) and "amount" in comp:
                        comps.append({"amount": comp.get("amount"), "currency": comp.get("currency"), "desc": comp.get("description")})
            elif isinstance(alt_group, dict):
                comps.append({"amount": alt_group.get("amount"), "currency": alt_group.get("currency"), "desc": alt_group.get("description")})
        # Pick first numeric for amount
        if comps:
            first = comps[0]
            return first.get("amount"), first.get("currency"), comps
    except Exception:
        pass
    return None, None, []


def pick_price(offer: Dict) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[str], List[Dict]]:
    """
    Extract fare, tax, total and currency from an offer object.
    Returns (fare_amount, tax_amount, total_amount, currency, tax_components)
    """
    currency = None
    fare = None
    tax = None
    total = None
    tax_components: List[Dict] = []

    # total
    try:
        total_alt = _safe_get(offer, ["total", "alternatives"], default=[])
        if total_alt:
            total_amt, total_cur, _ = _parse_price_alternatives(total_alt)
            if total_amt is not None:
                total = total_amt
                currency = total_cur or currency
    except Exception:
        total = None

    # fare (base)
    try:
        fare_alt = _safe_get(offer, ["fare", "alternatives"], default=[])
        if fare_alt:
            fare_amt, fare_cur, _ = _parse_price_alternatives(fare_alt)
            if fare_amt is not None:
                fare = fare_amt
                currency = currency or fare_cur
    except Exception:
        fare = None

    # taxes
    try:
        tax_alt = _safe_get(offer, ["taxes", "alternatives"], default=[])
        if tax_alt:
            tax_amt, tax_cur, comps = _parse_price_alternatives(tax_alt)
            if tax_amt is not None:
                tax = tax_amt
                currency = currency or tax_cur
            tax_components = comps
    except Exception:
        tax = None
        tax_components = []

    # last-resort compute total
    if total is None and fare is not None:
        try:
            total = (fare or 0) + (tax or 0)
        except Exception:
            pass

    # Some Biman/Sabre responses omit explicit tax breakdown while still returning
    # total + base fare. In that case derive tax conservatively.
    if tax is None and total is not None and fare is not None:
        try:
            tax = max(float(total) - float(fare), 0.0)
        except Exception:
            pass

    return fare, tax, total, currency, tax_components


# -------------------------
# Core parsing
# -------------------------

def extract_offers_from_response(resp: Any, keep_soldout: bool = False) -> List[Dict]:
    """
    Main entrypoint.

    - resp: either a dict (decoded JSON) or a requests.Response-like object with .json()
    - keep_soldout: if False (default) skip rows with soldout=True or status != "AVAILABLE".
    """
    rows: List[Dict] = []

    # If Response-like passed, get JSON
    if hasattr(resp, "json") and not isinstance(resp, dict):
        try:
            data = resp.json()
        except Exception as e:
            LOG.exception("Failed to .json() response: %s", e)
            return []
    else:
        data = resp or {}

    # If input already contains 'rows' (pre-parsed), just return it
    if isinstance(data, dict) and "rows" in data:
        return data.get("rows", [])

    # Locate originalResponse (several shapes possible)
    original = None
    if isinstance(data, dict):
        original = _safe_get(data, ["data", "bookingAirSearch", "originalResponse"], default=None) or data.get("originalResponse") or data.get("DigitalConnectOriginalResponse") or data

    if not original:
        LOG.warning("No originalResponse found in provided payload.")
        return []

    # Fare family baggage mapping
    fare_families = original.get("fareFamilies") or []
    fare_family_baggage = extract_baggage_from_fare_families(fare_families)

    # Helper: append a single row with normalized keys
    def append_row(normalized: Dict):
        rows.append(normalized)

    # Helper to decide if an offer should be skipped
    def offer_is_available(offer: Dict) -> bool:
        # Some offers have 'status' with 'AVAILABLE' or 'UNAVAILABLE' or 'NONE_SCHEDULED'
        status = offer.get("status")
        soldout = offer.get("soldout") or False
        if keep_soldout:
            return True
        # if status exists prefer it, otherwise use soldout flag
        if status:
            return str(status).upper() == "AVAILABLE"
        return not bool(soldout)

    def to_bdt(amount: float | None, currency: str | None) -> float | None:
        if amount is None:
            return None
        if currency == "BDT" or currency is None:
            return float(amount)
        FX = {
            "USD": 110.0,
            "EUR": 120.0,
            "AED": 32.5,   # UAE Dirham
            "SAR": 30.5,   # Saudi Riyal
            "OMR": 300.0,  # Omani Rial
            "KWD": 375.0,  # Kuwaiti Dinar
            "QAR": 31.5,   # Qatari Riyal
            "BHD": 305.0,  # Bahraini Dinar
            "JOD": 162.0,  # Jordanian Dinar
            "SGD": 90.0,
            "MYR": 26.0,
            "THB": 3.3,
            "INR": 1.38,
            "MVR": 7.5,
        }
        rate = FX.get(currency)
        return round(amount * rate, 2) if rate else None


    # Generic function to iterate offers lists and yield offers (handles nested list shapes)
    def iter_offers(container: Any):
        if not container:
            return
        if isinstance(container, list):
            for outer in container:
                # outer may itself be a list of offers (alternate-date grouping)
                if isinstance(outer, list):
                    for offer in outer:
                        yield offer
                elif isinstance(outer, dict):
                    yield outer
                else:
                    # unknown item - skip
                    continue
        elif isinstance(container, dict):
            # single offer
            yield container

    # Collect offers from different known keys
    candidate_offer_containers = []
    candidate_offer_containers.append(_safe_get(original, ["unbundledAlternateDateOffers"], default=[]))
    candidate_offer_containers.append(_safe_get(original, ["unbundledOffers"], default=[]))
    candidate_offer_containers.append(_safe_get(original, ["bundledAlternateDateOffers"], default=[]))
    candidate_offer_containers.append(_safe_get(original, ["bundledOffers"], default=[]))

    # Branded results may nest offers differently
    branded = _safe_get(original, ["brandedResults"], default={})
    if branded:
        # itineraryPartBrands -> list -> brandOffers
        ipb = branded.get("itineraryPartBrands") or []
        # convert to similar nested lists so iter_offers can handle
        for part in ipb:
            if isinstance(part, dict):
                offers = part.get("brandOffers") or []
                if offers:
                    candidate_offer_containers.append(offers)

    # Also try legacy keys
    candidate_offer_containers.append(_safe_get(original, ["offers"], default=[]))
    candidate_offer_containers.append(_safe_get(original, ["shoppingOffers"], default=[]))

    # Iterate candidate containers, flatten offers and parse
    for container in candidate_offer_containers:
        if not container:
            continue
        for offer in iter_offers(container):
            try:
                # Some containers may be grouped by departure date; 'offer' may be e.g. {"status":..., "departureDates": [...]} or full offer
                # If an element is not an offer (e.g., it's a date-group wrapper), dive in
                if isinstance(offer, dict) and "departureDates" in offer and "itineraryPart" not in offer and "itineraryPart" not in (offer.get("itineraryPart") or {}):
                    # maybe a date wrapper that contains another nested list - attempt to extract nested offers
                    inner = offer.get("offers") or offer.get("brandOffers") or offer.get("offersList") or []
                    for o2 in iter_offers(inner):
                        yield_from = [o2]
                        for o in yield_from:
                            offer = o
                            # fall through to parsing single offer
                # Now parse single offer
                if not isinstance(offer, dict):
                    continue

                # Apply availability filter
                if not offer_is_available(offer):
                    continue

                soldout_flag = bool(offer.get("soldout") or (offer.get("status") and str(offer.get("status")).upper() != "AVAILABLE"))

                # Brand/fare family
                brand_id = offer.get("brandId") or offer.get("brand") or None

                # Seats
                seats = _safe_get(offer, ["seatsRemaining", "count"], default=None)

                # Itinerary parts -> iterate segments (we flatten multi-seg as separate rows for now — frontend later can group)
                itinerary_parts = offer.get("itineraryPart") or offer.get("itineraryParts") or []
                if not isinstance(itinerary_parts, list):
                    itinerary_parts = [itinerary_parts]

                # If no itineraryPart, some offers include segments in top-level 'segments'
                if not itinerary_parts and "segments" in offer:
                    itinerary_parts = [{"segments": offer.get("segments", [])}]

                # Extract price info based on offer-level fare/taxes/total (pick_price is resilient)
                fare_amt, tax_amt, total_amt, currency, tax_components = pick_price(offer)

                # ---- PRICE NORMALIZATION (RUNTIME SAFE) ----
                raw_total = total_amt
                price_total_bdt = None
                if raw_total is not None:
                    try:
                        price_total_bdt = float(
                            to_bdt(raw_total, currency)
                        )
                    except Exception:
                        price_total_bdt = None

                # ---- SEAT NORMALIZATION (RUNTIME SAFE) ----
                seat_available = None
                if seats is not None:
                    try:
                        seat_available = int(seats)
                    except Exception:
                        seat_available = None
                inventory_confidence = "reported" if seat_available is not None else "unknown"
                fare_search_reference = (
                    original.get("fareSearchReference")
                    or offer.get("fareSearchReference")
                    or _safe_get(offer, ["metadata", "fareSearchReference"], default=None)
                )
                source_endpoint = "api/graphql:bookingAirSearch"
                penalty_fields = _extract_penalty_fields_for_offer(offer, original)

                # Baggage from fare families (map brand -> baggage)
                baggage = None
                if brand_id and brand_id in fare_family_baggage:
                    baggage = fare_family_baggage.get(brand_id)

                # If itinerary parts exist, create a row per first segment (for single-leg search) or per-segment
                for ip in itinerary_parts:
                    try:
                        segments = ip.get("segments") or []
                        if not segments:
                            # If no segments, still produce a row based on offer-level departure/arrival if present
                            row_origin = offer.get("origin") or offer.get("departureAirport")
                            row_destination = offer.get("destination") or offer.get("arrivalAirport")
                            row_dep = offer.get("departure")
                            row_arr = offer.get("arrival")
                            row = {
                                "brand": brand_id,
                                "soldout": soldout_flag,
                                "seats_remaining": seats,
                                "fare_amount": fare_amt,
                                "tax_amount": tax_amt,
                                "total_amount": total_amt,
                                "currency": currency,
                                "tax_components": tax_components,
                                "baggage": baggage,
                                "origin": row_origin,
                                "destination": row_destination,
                                "departure": row_dep,
                                "arrival": row_arr,
                                "duration_min": ip.get("totalDuration") or None,
                                "stops": ip.get("stops") or None,
                                "inventory_confidence": inventory_confidence,
                                "fare_search_reference": fare_search_reference,
                                "source_endpoint": source_endpoint,
                                "raw_offer": offer,
                                "seat_available": seat_available,
                            }
                            row.update(penalty_fields)
                            rows.append(row)
                            continue

                        # For each segment create a row (you can aggregate later for multi-segment)
                        for seg in segments:
                            flight = seg.get("flight") or {}
                            airline_code = flight.get("airlineCode") or seg.get("airlineCode") or None
                            operating_airline = flight.get("operatingAirlineCode") or flight.get("operatingAirline") or None
                            flight_number = flight.get("flightNumber") or seg.get("flightNumber") or None
                            operating_flight_number = flight.get("operatingFlightNumber") or None

                            equipment = seg.get("equipment") or seg.get("aircraft") or seg.get("equipmentCode")
                            equip_code, equip_desc = map_equipment(equipment)

                            origin = seg.get("origin") or seg.get("departureAirport")
                            destination = seg.get("destination") or seg.get("arrivalAirport")
                            dep = seg.get("departure")
                            arr = seg.get("arrival")
                            duration = seg.get("duration") or ip.get("totalDuration")

                            stops = ip.get("stops", seg.get("stops", 0))
                            booking_class = seg.get("bookingClass") or ip.get("bookingClass") or offer.get("bookingClass")
                            fare_basis = seg.get("fareBasis") or offer.get("fareBasis") or None
                            cabin = seg.get("cabinClass") or ip.get("cabinClass") or offer.get("cabinClass") or None

                            # Determine seat capacity based on equipment code/name mapping
                            seat_capacity = _capacity_from_aircraft(equip_code, equip_desc)

                            # estimated load factor: if seats_remaining present and seat_capacity known
                            estimated_load_factor_pct = None
                            try:
                                if seat_capacity and seats is not None:
                                    # if seats_remaining is small -> report load factor = (occupied / capacity)
                                    # seats_remaining is number available -> occupied = capacity - seats_remaining
                                    occupied = max(0, (seat_capacity - (seats or 0)))
                                    estimated_load_factor_pct = round((occupied / seat_capacity) * 100, 1) if seat_capacity else None
                            except Exception:
                                estimated_load_factor_pct = None

                            row = {
                                "brand": brand_id,
                                "soldout": soldout_flag,
                                "airline": airline_code,
                                "operating_airline": operating_airline,
                                "flight_number": flight_number,
                                "operating_flight_number": operating_flight_number,
                                "equipment_code": equip_code,
                                "aircraft": equip_desc,
                                "seat_capacity": seat_capacity,
                                "estimated_load_factor_pct": estimated_load_factor_pct,
                                "origin": origin,
                                "destination": destination,
                                "departure": dep,
                                "arrival": arr,
                                "duration_min": duration,
                                "stops": stops,
                                "cabin": cabin,
                                "booking_class": booking_class,
                                "fare_basis": fare_basis,
                                "fare_amount": fare_amt,
                                "tax_amount": tax_amt,
                                "total_amount": total_amt,
                                "currency": currency,
                                "tax_components": tax_components,
                                "baggage": baggage,
                                "seats_remaining": seats,
                                "raw_offer": offer,
                                "price_total_bdt": price_total_bdt,
                                "seat_available": seat_available,
                                "inventory_confidence": inventory_confidence,
                                "fare_search_reference": fare_search_reference,
                                "source_endpoint": source_endpoint,
                            }
                            row.update(penalty_fields)
                            append_row(row)
                    except Exception as e:
                        LOG.warning("Error parsing itineraryPart: %s", e)
            except Exception as e:
                LOG.exception("Error parsing an offer: %s", e)

    if not rows:
        LOG.warning("No parsed rows produced. Raw keys: %s", type(original))
    else:
        LOG.info("Parsed %d rows", len(rows))

    return rows


# lightweight test hook
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="Path to JSON sample", required=True)
    parser.add_argument("--keep-soldout", action="store_true", help="Keep soldout/UNAVAILABLE offers")
    args = parser.parse_args()
    p = Path(args.input)
    if not p.exists():
        print("File not found:", p)
        raise SystemExit(1)
    sample = json.loads(p.read_text(encoding="utf-8"))
    out = extract_offers_from_response(sample, keep_soldout=args.keep_soldout)
    print(json.dumps({"rows": out}, indent=2, ensure_ascii=False))
