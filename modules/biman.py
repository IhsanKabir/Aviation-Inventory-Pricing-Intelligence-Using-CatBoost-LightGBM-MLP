# modules/biman.py
"""
Biman Module – GraphQL wrapper + fetch_flights() for automated engine
"""
import argparse
import json
import logging
from typing import Any, Dict

from core.source_switches import disabled_source_response, source_enabled
from modules.requester import Requester
from modules import parser as parser_mod


LOG = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

GRAPHQL_URL = "https://booking.biman-airlines.com/api/graphql"

BM_HEADERS = {
    "Accept": "application/json",
    "Origin": "https://booking.biman-airlines.com",
    "Referer": "https://booking.biman-airlines.com/dx/BGDX/",
    "Content-Type": "application/json",
    "x-sabre-storefront": "BGDX",
    "ADRUM": "isAjax:true",
    "application-id": "SWS1:SBR-GCPDCShpBk:2ceb6478a8",
    "accept": "*/*",
    "conversation-id": "client-generated",
    "execution": "client-generated"
}


def build_payload(
    origin: str,
    destination: str,
    date: str,
    cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
    trip_type: str = "OW",
    return_date: str | None = None,
):
    passengers = {"ADT": max(1, int(adt or 1))}
    if int(chd or 0) > 0:
        passengers["CHD"] = int(chd)
    if int(inf or 0) > 0:
        passengers["INF"] = int(inf)
    itinerary_parts = [
        {
            "from": {"useNearbyLocations": False, "code": origin},
            "to": {"useNearbyLocations": False, "code": destination},
            "when": {"date": date},
        }
    ]
    if str(trip_type or "OW").strip().upper() in {"RT", "ROUNDTRIP", "ROUND_TRIP"} and return_date:
        itinerary_parts.append(
            {
                "from": {"useNearbyLocations": False, "code": destination},
                "to": {"useNearbyLocations": False, "code": origin},
                "when": {"date": return_date},
            }
        )
    return {
        "operationName": "bookingAirSearch",
        "query": """
            query bookingAirSearch($airSearchInput: CustomAirSearchInput) {
              bookingAirSearch(airSearchInput: $airSearchInput) {
                originalResponse
                __typename
              }
            }
        """,
        "variables": {
            "airSearchInput": {
                "cabinClass": cabin,
                "awardBooking": False,
                "promoCodes": [""],
                "searchType": "BRANDED",
                "passengers": passengers,
                "itineraryParts": itinerary_parts,
                "pointOfSale": "BD",
            }
        },
    }


def _maybe_parse_original_response(original):
    if original is None:
        return None
    if isinstance(original, str):
        try:
            return json.loads(original)
        except:
            return original
    return original


def _extract_original_response(resp_json: Dict[str, Any]):
    if not isinstance(resp_json, dict):
        return None

    data = resp_json.get("data")
    if not isinstance(data, dict):
        return None

    booking = data.get("bookingAirSearch")
    if not isinstance(booking, dict):
        return None

    return booking.get("originalResponse")


def _apply_passenger_mix_defaults(rows: list[dict], payload: Dict[str, Any]) -> None:
    try:
        pax = (
            payload.get("variables", {})
            .get("airSearchInput", {})
            .get("passengers", {})
        )
    except Exception:
        pax = {}
    adt = int(pax.get("ADT") or 0)
    chd = int(pax.get("CHD") or 0)
    inf = int(pax.get("INF") or 0)
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("adt_count") is None:
            row["adt_count"] = adt
        if row.get("chd_count") is None:
            row["chd_count"] = chd
        if row.get("inf_count") is None:
            row["inf_count"] = inf


def _enforce_marketing_airline(rows: list[dict], marketing_airline: str = "BG") -> None:
    """
    Prevent segment-level operating carrier codes from contaminating canonical airline.

    The generic parser may populate row["airline"] from segment flight.airlineCode, which can be
    a codeshare/operating carrier (e.g., "AZ") in Sabre responses. For Biman scrapes we want the
    requested/marketing carrier to remain canonical ("BG"), while preserving any segment operator
    in row["operating_airline"].
    """
    canonical = str(marketing_airline or "").upper().strip()
    if not canonical:
        return
    for row in rows:
        if not isinstance(row, dict):
            continue
        parsed_airline = str(row.get("airline") or "").upper().strip()
        parsed_operating = str(row.get("operating_airline") or "").upper().strip()
        if parsed_airline and parsed_airline != canonical and not parsed_operating:
            row["operating_airline"] = parsed_airline
        row["airline"] = canonical


def biman_search(
    origin: str,
    dest: str,
    date: str,
    cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
    trip_type: str = "OW",
    return_date: str | None = None,
    cookies_path: str = None,
    verbose: bool = False,
    debug: bool = False,
):
    req = Requester(cookies_path=cookies_path)
    payload = build_payload(
        origin,
        dest,
        date,
        cabin,
        adt=adt,
        chd=chd,
        inf=inf,
        trip_type=trip_type,
        return_date=return_date,
    )

    if debug:
        with open("inspect_payload.json", "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
            LOG.info("Wrote payload to inspect_payload.json")

    ok, resp_json, status = req.post(
        GRAPHQL_URL, json_payload=payload, headers=BM_HEADERS
    )

    output = {"raw": resp_json, "originalResponse": None, "rows": [], "ok": ok}

    if not ok or resp_json is None:
        LOG.error(f"[BG] GraphQL error: status={status}")
        return output

    original = _extract_original_response(resp_json)
    original = _maybe_parse_original_response(original)
    output["originalResponse"] = original

    if original:
        try:
            parsed_rows = parser_mod.extract_offers_from_response(original)
            _apply_passenger_mix_defaults(parsed_rows, payload)
            _enforce_marketing_airline(parsed_rows, "BG")
            output["rows"] = parsed_rows
        except Exception as e:
            LOG.exception("Parser failure: %s", e)

    return output


# --- THIS IS WHAT run_all.py CALLS ---
def fetch_flights(
    origin: str,
    destination: str,
    date: str,
    cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
    trip_type: str = "OW",
    return_date: str | None = None,
):
    """
    MUST return a dict with keys:
    { raw, originalResponse, rows, ok }
    """
    if not source_enabled("biman"):
        return disabled_source_response("biman")

    return biman_search(
        origin,
        destination,
        date,
        cabin=cabin,
        adt=adt,
        chd=chd,
        inf=inf,
        trip_type=trip_type,
        return_date=return_date,
        verbose=False,
    )


def check_source_health(*, dry_run: bool = True, **_: Any) -> Dict[str, Any]:
    from core.source_health import ok

    return ok(
        "biman",
        message="direct GraphQL connector configured; live health is measured per extraction attempt",
        endpoint=GRAPHQL_URL,
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
    parser.add_argument("--trip-type", default="OW")
    parser.add_argument("--return-date", default=None)
    parser.add_argument("--cookies", default=None)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    out = biman_search(
        origin=args.origin,
        dest=args.destination,
        date=args.date,
        cabin=args.cabin,
        adt=args.adt,
        chd=args.chd,
        inf=args.inf,
        trip_type=args.trip_type,
        return_date=args.return_date,
        cookies_path=args.cookies,
        verbose=args.verbose,
        debug=args.debug,
    )
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    cli_main()
