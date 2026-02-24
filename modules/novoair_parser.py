"""
Parser for Novoair JSON responses.

Supported shapes:
1) flight_selection.aspx JSON: flightSelections.flightBlocks...
2) passenger_info.aspx?get=DATA JSON: bookingSummary.Itinerary.travelSegments...
"""
from datetime import datetime
import re
from typing import Any, Dict, List, Optional

from modules.fleet_mapping import resolve_seat_capacity


CABIN_CODE_TO_NAME = {
    "Y": "Economy",
    "J": "Business",
    "C": "Business",
}

NOVO_SEAT_CAPACITY_MAP = {
    "ATR725": 72,
    "ATR72-500": 72,
    "ATR72": 72,
}


def _pick_cabin_name(cabin_code: Optional[str], requested_cabin: str) -> str:
    if cabin_code:
        return CABIN_CODE_TO_NAME.get(str(cabin_code).upper(), str(cabin_code))
    return requested_cabin


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def _duration_minutes(departure_iso: Optional[str], arrival_iso: Optional[str]) -> Optional[int]:
    if not departure_iso or not arrival_iso:
        return None
    try:
        dep = datetime.fromisoformat(departure_iso)
        arr = datetime.fromisoformat(arrival_iso)
        return int((arr - dep).total_seconds() // 60)
    except Exception:
        return None


def _extract_pax_mix_from_flight_search(resp_json: Dict[str, Any]) -> Dict[str, Optional[int]]:
    fsm = (resp_json.get("flightSearchModel") or {}).get("flightSearchData") or {}
    return {
        "adt_count": _safe_int(fsm.get("adultCount"), None),
        "chd_count": _safe_int(fsm.get("childCount"), None),
        "inf_count": _safe_int(fsm.get("infantCount"), None),
    }


def _extract_fare_refs(resp_json: Dict[str, Any]) -> Dict[str, Optional[str]]:
    fs = resp_json.get("flightSelections") or {}
    frm = (resp_json.get("flightResultsModel") or {}) if isinstance(resp_json, dict) else {}
    return {
        "fare_ref_num": fs.get("fareRefNum"),
        "fare_search_reference": frm.get("fareSearchReference"),
    }


def _extract_pax_mix_from_booking_summary(resp_json: Dict[str, Any]) -> Dict[str, Optional[int]]:
    bs = resp_json.get("bookingSummary") or {}
    ptc = bs.get("PaxTypeCount") or bs.get("PaxCodeCount") or {}
    return {
        "adt_count": _safe_int(ptc.get("ADT"), None),
        "chd_count": _safe_int(ptc.get("CHD"), None),
        "inf_count": _safe_int(ptc.get("INF"), None),
    }


def _normalize_stops(value: Any) -> Optional[int]:
    if value is None:
        return 0
    if isinstance(value, list):
        return len(value)
    return _safe_int(value, None)


def _extract_seats(fare_obj: Dict[str, Any]) -> Optional[int]:
    if not isinstance(fare_obj, dict):
        return None

    candidate_keys = (
        "seats",
        "seatsLeft",
        "seats_remaining",
        "seat_available",
        "availableSeats",
        "availSeats",
    )
    for key in candidate_keys:
        seats = _safe_int(fare_obj.get(key), None)
        if seats is not None:
            return seats

    inventory = fare_obj.get("inventory")
    if isinstance(inventory, dict):
        for key in candidate_keys:
            seats = _safe_int(inventory.get(key), None)
            if seats is not None:
                return seats

    hold_value = fare_obj.get("hold")
    hold_text = str(hold_value).upper() if hold_value is not None else ""
    if hold_value is True or hold_text in {"SO", "SOLDOUT"}:
        return 0

    return None


def _extract_baggage(fare_obj: Dict[str, Any], meta: Dict[str, Any]) -> Optional[str]:
    for source in (fare_obj, meta):
        if not isinstance(source, dict):
            continue
        for key in ("baggage", "baggageAllowance", "allowance"):
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    description = str((meta or {}).get("description") or "")
    if not description:
        return None

    kg_match = re.search(r"(\d{1,3})\s*(kg|kgs|kilograms?)", description, re.IGNORECASE)
    if kg_match:
        return f"{kg_match.group(1)} kg"

    piece_match = re.search(r"(\d{1,2})\s*(pc|pcs|piece|pieces)", description, re.IGNORECASE)
    if piece_match:
        return f"{piece_match.group(1)} pc"

    return None


def _seat_capacity_from_aircraft(aircraft: Optional[str]) -> Optional[int]:
    dynamic = resolve_seat_capacity("VQ", aircraft=aircraft, equipment_code=aircraft)
    if dynamic is not None:
        return dynamic

    if not aircraft:
        return None
    a = str(aircraft).strip().upper().replace(" ", "")
    if a in NOVO_SEAT_CAPACITY_MAP:
        return NOVO_SEAT_CAPACITY_MAP[a]
    if "ATR72" in a:
        return 72
    return None


def _estimated_load_factor_pct(seats_remaining: Optional[int], seat_capacity: Optional[int]) -> Optional[float]:
    try:
        if seats_remaining is None or not seat_capacity:
            return None
        occupied = max(0, seat_capacity - int(seats_remaining))
        return round((occupied / float(seat_capacity)) * 100.0, 1)
    except Exception:
        return None


def _parse_flight_selections(
    resp_json: Dict[str, Any],
    requested_date: str,
    requested_cabin: str,
    include_flexible_dates: bool = True,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    fs = resp_json.get("flightSelections") or _find_flight_selections_blob(resp_json) or {}
    currency_code = ((fs.get("currency") or {}).get("code")) or "BDT"
    pax_mix = _extract_pax_mix_from_flight_search(resp_json)
    refs = _extract_fare_refs(resp_json)

    family_meta = {}
    for fam in fs.get("fareFamilies") or []:
        code = fam.get("code")
        if not code:
            continue
        family_meta[code] = {
            "name": fam.get("name") or code,
            "title": fam.get("title") or fam.get("name") or code,
            "cabin_code": fam.get("cabin"),
            "description": fam.get("description"),
        }

    blocks = fs.get("flightBlocks") or []
    seen_offer_keys = set()
    for trip_index, block in enumerate(blocks):
        origin = block.get("from")
        destination = block.get("into")

        for fd in block.get("flightDates") or []:
            block_date = fd.get("date")
            if (not include_flexible_dates) and requested_date and block_date != requested_date:
                continue

            for flight_index, flight in enumerate(fd.get("flights") or []):
                itinerary = flight.get("itinerary") or []
                if not itinerary:
                    continue

                first_seg = itinerary[0]
                flight_no = first_seg.get("flight")
                aircraft = first_seg.get("type")
                seat_capacity = _seat_capacity_from_aircraft(aircraft)
                departure = first_seg.get("TOD")
                arrival = first_seg.get("TOA")
                stops = first_seg.get("stops")

                family_fares = flight.get("familyFares") or {}
                for fare_code, fare_obj in family_fares.items():
                    if not isinstance(fare_obj, dict):
                        continue
                    fare_id = fare_obj.get("id")
                    dedupe_key = (
                        str(origin or ""),
                        str(destination or ""),
                        str(block_date or ""),
                        str(flight_no or ""),
                        str(departure or ""),
                        str(arrival or ""),
                        str(fare_code or ""),
                        str(fare_id or ""),
                    )
                    if dedupe_key in seen_offer_keys:
                        continue
                    seen_offer_keys.add(dedupe_key)

                    meta = family_meta.get(fare_code, {})
                    total_amount = fare_obj.get("all")
                    one_amount = fare_obj.get("one")
                    total_or_one = total_amount if total_amount is not None else one_amount
                    seats_remaining = _extract_seats(fare_obj)
                    baggage = _extract_baggage(fare_obj, meta)

                    tax_amount: Optional[float] = None
                    one_num = _safe_float(one_amount, None)
                    total_num = _safe_float(total_or_one, None)
                    if one_num is not None and total_num is not None:
                        # For one-passenger rows this may be 0; keep it to avoid null tax columns.
                        tax_amount = max(total_num - one_num, 0.0)

                    hold_value = fare_obj.get("hold")
                    hold_text = str(hold_value).upper() if hold_value is not None else ""
                    soldout = (seats_remaining == 0) or (hold_value is True) or (hold_text in {"SO", "SOLDOUT"})

                    row = {
                        "airline": "VQ",
                        "flight_number": str(flight_no) if flight_no is not None else None,
                        "origin": origin,
                        "destination": destination,
                        "departure": departure,
                        "arrival": arrival,
                        "aircraft": aircraft,
                        "equipment_code": aircraft,
                        "duration_min": _duration_minutes(departure, arrival),
                        "stops": _normalize_stops(stops),
                        "seat_capacity": seat_capacity,
                        "estimated_load_factor_pct": _estimated_load_factor_pct(seats_remaining, seat_capacity),
                        "cabin": _pick_cabin_name(meta.get("cabin_code"), requested_cabin),
                        "brand": meta.get("title") or meta.get("name") or fare_code,
                        "fare_basis": fare_code,
                        "fare_id": fare_id,
                        "trip_index": trip_index,
                        "flight_index": flight_index,
                        "booking_class": fare_code,
                        "currency": currency_code,
                        "fare_amount": one_amount,
                        "tax_amount": tax_amount,
                        "total_amount": total_or_one,
                        "price_total_bdt": _safe_float(total_or_one),
                        "seats_remaining": seats_remaining,
                        "seat_available": seats_remaining,
                        "inventory_confidence": "reported" if seats_remaining is not None else "unknown",
                        "soldout": soldout,
                        "baggage": baggage,
                        "adt_count": pax_mix["adt_count"],
                        "chd_count": pax_mix["chd_count"],
                        "inf_count": pax_mix["inf_count"],
                        "fare_ref_num": refs.get("fare_ref_num"),
                        "fare_search_reference": refs.get("fare_search_reference"),
                        "source_endpoint": "flight_selection.aspx?ajax=true&action=flightSearch",
                        "search_date": requested_date,
                        "raw_offer": {
                            "source": "flightSelections",
                            "date": block_date,
                            "trip_index": trip_index,
                            "flight_index": flight_index,
                            "fare_id": fare_id,
                            "itinerary": itinerary,
                            "fare_code": fare_code,
                            "fare": fare_obj,
                            "fare_meta": meta,
                        },
                    }
                    rows.append(row)
    return rows


def _parse_passenger_info(
    resp_json: Dict[str, Any],
    requested_date: str,
    requested_cabin: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    bs = resp_json.get("bookingSummary") or {}
    itinerary = (bs.get("Itinerary") or {}).get("travelSegments") or []
    default_currency = ((bs.get("Currency") or {}).get("code")) or "BDT"
    pax_mix = _extract_pax_mix_from_booking_summary(resp_json)

    for block in itinerary:
        trip_segments = block.get("tripSegments") or []
        cost_summary = block.get("costSummary") or []
        cost0 = cost_summary[0] if cost_summary else {}

        total_amount = cost0.get("totalFare")
        fare_amount = cost0.get("baseFare")
        tax_amount = (
            _safe_float(cost0.get("totalTaxes"), 0.0)
            + _safe_float(cost0.get("totalFees"), 0.0)
            + _safe_float(cost0.get("totalSurcharges"), 0.0)
        ) or None
        fare_basis = cost0.get("fareBasis")
        currency = cost0.get("currencyCode") or default_currency

        for seg in trip_segments:
            departure = seg.get("departing")
            if requested_date:
                try:
                    if not departure or departure[:10] != requested_date:
                        continue
                except Exception:
                    continue

            arrival = seg.get("arriving")
            cabin = _pick_cabin_name(seg.get("cabinClass"), requested_cabin)
            flight_no = seg.get("flightNumber")

            row = {
                "airline": "VQ",
                "flight_number": str(flight_no) if flight_no is not None else None,
                "origin": seg.get("departureCityCode") or block.get("originCityCode"),
                "destination": seg.get("arrivalCityCode") or block.get("destinationCityCode"),
                "departure": departure,
                "arrival": arrival,
                "aircraft": seg.get("aircraftType"),
                "equipment_code": seg.get("aircraftType"),
                "duration_min": _duration_minutes(departure, arrival),
                "stops": _normalize_stops(seg.get("stops")),
                "seat_capacity": _seat_capacity_from_aircraft(seg.get("aircraftType")),
                "estimated_load_factor_pct": None,
                "cabin": cabin,
                "brand": fare_basis or "Standard",
                "fare_basis": fare_basis,
                "booking_class": fare_basis or seg.get("cabinClass"),
                "currency": currency,
                "fare_amount": fare_amount,
                "tax_amount": tax_amount,
                "total_amount": total_amount,
                "price_total_bdt": _safe_float(total_amount),
                "seats_remaining": None,
                "seat_available": None,
                "inventory_confidence": "unknown",
                "soldout": False,
                "baggage": None,
                "adt_count": pax_mix["adt_count"],
                "chd_count": pax_mix["chd_count"],
                "inf_count": pax_mix["inf_count"],
                "source_endpoint": "passenger_info.aspx?get=DATA",
                "raw_offer": {
                    "source": "passenger_info",
                    "segment": seg,
                    "cost_summary": cost0,
                    "itinerary_block": block,
                },
            }
            rows.append(row)
    return rows


def _find_flight_selections_blob(resp_json: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(resp_json, dict):
        return None

    candidates = [
        resp_json.get("flightSelections"),
        (resp_json.get("ResultsData") or {}).get("flightSelections"),
        (resp_json.get("data") or {}).get("flightSelections"),
        (resp_json.get("flightResultsModel") or {}).get("flightSelections"),
        ((resp_json.get("ResultsData") or {}).get("flightResultsModel") or {}).get("flightSelections"),
        ((resp_json.get("ResultsData") or {}).get("flightResultsModel") or {}).get("flightSelectionsData"),
        resp_json.get("flightSelectionsData"),
        resp_json if isinstance(resp_json.get("flightBlocks"), list) else None,
    ]

    for candidate in candidates:
        if isinstance(candidate, dict) and isinstance(candidate.get("flightBlocks"), list):
            return candidate
    return None


def extract_offers_from_response(
    resp_json: Dict[str, Any],
    requested_date: str,
    requested_cabin: str = "Economy",
    include_flexible_dates: bool = True,
) -> List[Dict[str, Any]]:
    if not isinstance(resp_json, dict):
        return []

    flight_selections = _find_flight_selections_blob(resp_json)
    if flight_selections:
        return _parse_flight_selections(
            resp_json,
            requested_date,
            requested_cabin,
            include_flexible_dates=include_flexible_dates,
        )

    if isinstance(resp_json.get("bookingSummary"), dict):
        return _parse_passenger_info(resp_json, requested_date, requested_cabin)

    return []
