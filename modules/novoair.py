"""
Novoair module - form-data search wrapper + normalized rows for run_all.py
"""
import argparse
import datetime as dt
import json
import logging
from typing import Any, Dict, Optional

from core.source_switches import disabled_source_response, source_enabled
from modules.requester import Requester
from modules.novoair_parser import extract_offers_from_response


LOG = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

SEARCH_URL = "https://secure.flynovoair.com/bookings/Vues/flight_selection.aspx?ajax=true&action=flightSearch"
FARE_SELECTION_URL = "https://secure.flynovoair.com/bookings/Vues/flight_selection.aspx?ajax=true&action=fareSelection"
PASSENGER_INFO_URL = "https://secure.flynovoair.com/bookings/Vues/passenger_info.aspx?get=DATA"

NOVO_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://secure.flynovoair.com",
    "Referer": "https://secure.flynovoair.com/bookings/Vues/flight_selection.aspx?=auto",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/144.0.0.0 Safari/537.36"
    ),
}


def _to_yyyymm(date_iso: str) -> str:
    d = dt.date.fromisoformat(date_iso)
    return d.strftime("%Y-%m")


def _to_dd(date_iso: str) -> str:
    d = dt.date.fromisoformat(date_iso)
    return d.strftime("%d")


def build_form_payload(
    origin: str,
    destination: str,
    date: str,
    cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
) -> Dict[str, str]:
    # CC/CR are left blank as observed in the captured browser payload.
    adt_n = max(1, int(adt or 1))
    chd_n = max(0, int(chd or 0))
    inf_n = max(0, int(inf or 0))
    return {
        "ajax": "true",
        "action": "flightSearch",
        "SS": "",
        "RT": "",
        "FL": "on",
        "TT": "OW",
        "DC": origin,
        "AC": destination,
        "AM": _to_yyyymm(date),
        "AD": _to_dd(date),
        "CC": "",
        "CR": "",
        "NS": "false",
        "PA": str(adt_n),
        "PC": str(chd_n) if chd_n > 0 else "",
        "PI": str(inf_n) if inf_n > 0 else "",
        "CX": "",
        "CD": "",
        "RF": "2",
    }


def _safe_json(response_text: str):
    try:
        return json.loads(response_text)
    except Exception:
        return None


def _parse_json_response(response) -> Optional[Dict[str, Any]]:
    try:
        parsed = response.json()
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    fallback = _safe_json(response.text)
    return fallback if isinstance(fallback, dict) else None


def _fetch_passenger_info(req: Requester) -> Optional[Dict[str, Any]]:
    try:
        response = req.session.get(
            PASSENGER_INFO_URL,
            headers=NOVO_HEADERS,
            timeout=req.timeout,
        )
    except Exception as exc:
        LOG.warning("[VQ] passenger_info fallback request failed: %s", exc)
        return None

    if response.status_code != 200:
        LOG.info("[VQ] passenger_info fallback status=%s", response.status_code)
        return None

    parsed = _parse_json_response(response)
    if not parsed:
        LOG.info("[VQ] passenger_info fallback returned non-JSON payload")
        return None
    return parsed


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _extract_taxes_each_values(taxes_each: Any):
    """
    Normalize varying taxesEach shapes into iterable of (tax_code, amount_each).
    """
    if isinstance(taxes_each, dict):
        groups = taxes_each.values()
    elif isinstance(taxes_each, list):
        groups = taxes_each
    else:
        groups = []

    for group in groups:
        if isinstance(group, dict):
            for code, amount in group.items():
                amt = _safe_float(amount, None)
                if amt is None:
                    continue
                yield str(code), float(amt)


def _tax_total_from_fare_selection_payload(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return None

    root = payload
    data_blob = payload.get("data")
    if isinstance(data_blob, dict) and ("paxFares" in data_blob or "fareSelection" in data_blob):
        root = data_blob
    nested = payload.get("fareSelection")
    if isinstance(nested, dict):
        root = nested
    nested2 = root.get("fareSelection") if isinstance(root, dict) else None
    if isinstance(nested2, dict):
        root = nested2

    pax_fares = root.get("paxFares")
    if not isinstance(pax_fares, list):
        return None

    total_tax = 0.0
    code_totals: Dict[str, float] = {}
    for pax in pax_fares:
        if not isinstance(pax, dict):
            continue
        count = _safe_int(pax.get("count"), 1)
        if count <= 0:
            count = 1
        taxes_each = pax.get("taxesEach")
        per_pax_tax = 0.0
        for code, amount_each in _extract_taxes_each_values(taxes_each):
            per_pax_tax += amount_each
            code_totals[code] = code_totals.get(code, 0.0) + (amount_each * count)
        total_tax += per_pax_tax * count

    tax_names = root.get("taxNames") if isinstance(root.get("taxNames"), dict) else {}
    tax_group_names = root.get("taxGroupNames") if isinstance(root.get("taxGroupNames"), dict) else {}
    return {
        "total_tax": round(total_tax, 2),
        "code_totals": {k: round(v, 2) for k, v in code_totals.items()},
        "tax_names": tax_names,
        "tax_group_names": tax_group_names,
    }


def _fetch_fare_selection_tax(
    req: Requester,
    fare_ref_num: str,
    trip_index: int,
    fare_id: int,
) -> Optional[Dict[str, Any]]:
    multipart_payload = {
        "frn": (None, str(fare_ref_num)),
        "trip": (None, str(trip_index)),
        "fareId": (None, str(fare_id)),
    }
    try:
        response = req.session.post(
            FARE_SELECTION_URL,
            files=multipart_payload,
            headers=NOVO_HEADERS,
            timeout=req.timeout,
        )
    except Exception as exc:
        LOG.warning("[VQ] fareSelection request failed trip=%s fareId=%s err=%s", trip_index, fare_id, exc)
        return None

    if response.status_code != 200:
        LOG.info("[VQ] fareSelection non-200 trip=%s fareId=%s status=%s", trip_index, fare_id, response.status_code)
        return None

    parsed = _parse_json_response(response)
    if not isinstance(parsed, dict):
        return None
    return _tax_total_from_fare_selection_payload(parsed)


def _enrich_rows_with_tax_breakdown(req: Requester, rows: list) -> None:
    """
    For flightSelection rows, call fareSelection and set tax_amount to
    sum of tax-code components (e.g., YQ+YR+...).
    """
    if not rows:
        return

    index_map: Dict[tuple, list] = {}
    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        if str(row.get("airline", "")).upper() != "VQ":
            continue

        frn = row.get("fare_ref_num")
        raw_offer = row.get("raw_offer") or {}
        fare_id = row.get("fare_id")
        if fare_id is None and isinstance(raw_offer, dict):
            fare_id = raw_offer.get("fare_id")
            fare = raw_offer.get("fare")
            if fare_id is None and isinstance(fare, dict):
                fare_id = fare.get("id")

        trip_index = row.get("trip_index")
        if trip_index is None and isinstance(raw_offer, dict):
            trip_index = raw_offer.get("trip_index")

        if not frn or fare_id is None or trip_index is None:
            continue

        key = (str(frn), int(trip_index), int(fare_id))
        index_map.setdefault(key, []).append(idx)

    if not index_map:
        return

    tax_cache: Dict[tuple, Optional[Dict[str, Any]]] = {}
    updated = 0
    for key, row_indexes in index_map.items():
        frn, trip_index, fare_id = key
        if key not in tax_cache:
            tax_cache[key] = _fetch_fare_selection_tax(req, frn, trip_index, fare_id)
        tax_info = tax_cache[key]
        if not tax_info:
            continue

        total_tax = tax_info.get("total_tax")
        if total_tax is None:
            continue

        for row_idx in row_indexes:
            row = rows[row_idx]
            row["tax_amount"] = total_tax

            raw_offer = row.get("raw_offer")
            if not isinstance(raw_offer, dict):
                raw_offer = {}
                row["raw_offer"] = raw_offer
            raw_offer["fare_selection_tax_total"] = total_tax
            raw_offer["tax_code_totals"] = tax_info.get("code_totals") or {}
            raw_offer["tax_names"] = tax_info.get("tax_names") or {}
            raw_offer["tax_group_names"] = tax_info.get("tax_group_names") or {}
            raw_offer["tax_source"] = "fareSelection"
            updated += 1

    LOG.info("[VQ] Tax enrichment complete: candidates=%d updated=%d", len(index_map), updated)


def _apply_passenger_mix_defaults(rows: list, form_payload: Dict[str, str]) -> None:
    adt = _safe_int(form_payload.get("PA"), 1)
    chd = _safe_int(form_payload.get("PC"), 0)
    inf = _safe_int(form_payload.get("PI"), 0)
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("adt_count") is None:
            row["adt_count"] = adt
        if row.get("chd_count") is None:
            row["chd_count"] = chd
        if row.get("inf_count") is None:
            row["inf_count"] = inf


def novo_search(
    origin: str,
    dest: str,
    date: str,
    cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
    cookies_path: str = "cookies/novoair.json",
    debug: bool = False,
):
    req = Requester(cookies_path=cookies_path)
    form_payload = build_form_payload(origin, dest, date, cabin, adt=adt, chd=chd, inf=inf)

    if debug:
        with open("inspect_novo_payload.json", "w", encoding="utf-8") as fh:
            json.dump(form_payload, fh, indent=2)
            LOG.info("Wrote payload to inspect_novo_payload.json")

    # Use multipart form-data to match browser behavior.
    multipart_payload = {k: (None, str(v)) for k, v in form_payload.items()}
    response = req.session.post(
        SEARCH_URL,
        files=multipart_payload,
        headers=NOVO_HEADERS,
        timeout=req.timeout,
    )

    ok = response.status_code == 200
    search_json = _parse_json_response(response)

    raw_payload: Dict[str, Any] = {
        "flight_selection": search_json if search_json is not None else response.text
    }
    output = {"raw": raw_payload, "originalResponse": search_json, "rows": [], "ok": ok}
    if not ok:
        LOG.warning("[VQ] Search request failed status=%s", response.status_code)
        return output

    if isinstance(search_json, dict):
        try:
            output["rows"] = extract_offers_from_response(
                search_json,
                requested_date=date,
                requested_cabin=cabin,
                include_flexible_dates=True,
            )
        except Exception as exc:
            LOG.exception("[VQ] Parser failure on flight_selection payload: %s", exc)
    else:
        LOG.info("[VQ] flight_selection response is non-JSON; attempting passenger_info fallback")

    # If the primary payload doesn't provide rows, try the session state endpoint.
    if not output["rows"]:
        passenger_info_json = _fetch_passenger_info(req)
        if passenger_info_json:
            raw_payload["passenger_info"] = passenger_info_json
            try:
                fallback_rows = extract_offers_from_response(
                    passenger_info_json,
                    requested_date=date,
                    requested_cabin=cabin,
                    include_flexible_dates=True,
                )
                if fallback_rows:
                    output["rows"] = fallback_rows
                    output["originalResponse"] = passenger_info_json
                    LOG.info("[VQ] Parsed %d rows from passenger_info fallback", len(fallback_rows))
            except Exception as exc:
                LOG.exception("[VQ] Parser failure on passenger_info fallback payload: %s", exc)

    if not output["rows"]:
        keys = list(search_json.keys())[:12] if isinstance(search_json, dict) else []
        LOG.info("[VQ] No rows extracted. status=%s flight_selection_keys=%s", response.status_code, keys)
    else:
        _enrich_rows_with_tax_breakdown(req, output["rows"])
        _apply_passenger_mix_defaults(output["rows"], form_payload)
    return output


def fetch_flights(
    origin: str,
    destination: str,
    date: str,
    cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
):
    """
    Unified contract for run_all.py:
    { raw, originalResponse, rows, ok }
    """
    if not source_enabled("novoair"):
        return disabled_source_response("novoair")

    return novo_search(origin, destination, date, cabin=cabin, adt=adt, chd=chd, inf=inf)


def check_source_health(*, dry_run: bool = True, **_: Any) -> Dict[str, Any]:
    from core.source_health import ok

    return ok(
        "novoair",
        message="direct web connector configured; live health is measured per extraction attempt",
        endpoint=SEARCH_URL,
    )


def check_session(*, dry_run: bool = True, **kwargs: Any) -> Dict[str, Any]:
    return check_source_health(dry_run=dry_run, **kwargs)


def cli_main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--origin", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--cabin", default="Economy")
    parser.add_argument("--adt", type=int, default=1)
    parser.add_argument("--chd", type=int, default=0)
    parser.add_argument("--inf", type=int, default=0)
    parser.add_argument("--cookies", default="cookies/novoair.json")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    out = novo_search(
        origin=args.origin,
        dest=args.destination,
        date=args.date,
        cabin=args.cabin,
        adt=args.adt,
        chd=args.chd,
        inf=args.inf,
        cookies_path=args.cookies,
        debug=args.debug,
    )
    print(json.dumps(out, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    cli_main()
