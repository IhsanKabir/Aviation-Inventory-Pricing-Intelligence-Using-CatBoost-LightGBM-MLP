"""
Air Astra (2A) TTInteractive bootstrap + fetch_flights() scaffold.

Current limitation:
- Live search endpoints are protected by DataDome and return a captcha challenge
  in this automated environment. This module detects that condition and returns a
  clean failure payload instead of crashing.

Useful today:
- Bootstrap page parsing (routes, airports, service classes, sessionized URLs)
- Route discovery helper for generating/refreshing 2A routes
"""

from __future__ import annotations

import argparse
import copy
import html
import json
import logging
import re
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin

from modules.requester import Requester


LOG = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

BASE_URL = "https://fo-airastra.ttinteractive.com"
INDEX_URL = f"{BASE_URL}/Zenith/FrontOffice/Airastra/en-GB/BookingEngine/IndexRemoveIframe"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

BOOTSTRAP_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "User-Agent": USER_AGENT,
}

SEARCH_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": BASE_URL,
    "Referer": INDEX_URL,
    "User-Agent": USER_AGENT,
    "X-Requested-With": "XMLHttpRequest",
}


def _extract_data_config(html_text: str) -> Dict[str, Any]:
    match = re.search(r"data-config='(\{.*?\})'", html_text, re.S)
    if not match:
        raise ValueError("Air Astra data-config not found in bootstrap HTML")
    raw = html.unescape(match.group(1))
    return json.loads(raw)


def _safe_json_loads(text: str) -> Any:
    try:
        return json.loads(text)
    except Exception:
        return None


def _is_datadome_block(status_code: int, body: Any) -> bool:
    if status_code == 403:
        if isinstance(body, dict):
            url = str(body.get("url") or "")
            if "captcha-delivery.com" in url or "datadome" in url.lower():
                return True
        if isinstance(body, str):
            lower = body.lower()
            if "captcha-delivery.com" in lower or "datadome" in lower:
                return True
    return False


def _cabin_to_service_code(cabin: str) -> str:
    c = (cabin or "").strip().lower()
    if "premium" in c:
        return "P"
    if "business" in c:
        return "C"
    return "Y"


def _service_code_to_label(code: str) -> str:
    code_u = (code or "").upper()
    if code_u == "P":
        return "Premium Economy"
    if code_u == "C":
        return "Business"
    return "Economy"


def _build_service_class_map(config: Dict[str, Any]) -> Dict[str, int]:
    items = (
        config.get("sourceData", {})
        .get("Configuration", {})
        .get("ServiceClasses", {})
        .get("Items", [])
    )
    out: Dict[str, int] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        code = str(item.get("Code") or "").upper()
        data_id = item.get("DataId")
        if code and data_id is not None:
            try:
                out[code] = int(data_id)
            except Exception:
                continue
    return out


def _build_search_model(
    config: Dict[str, Any],
    origin: str,
    destination: str,
    date: str,
    cabin: str,
    adt: int,
    chd: int,
    inf: int,
) -> Dict[str, Any]:
    source = config["sourceData"]
    model = copy.deepcopy(source["SearchModel"])
    airports = source["Configuration"]["Airports"]

    origin_u = origin.upper().strip()
    dest_u = destination.upper().strip()
    if origin_u not in airports or dest_u not in airports:
        raise ValueError(f"Unknown 2A airport code(s): {origin_u}->{dest_u}")

    if not model.get("OriginDestinations"):
        model["OriginDestinations"] = [{}]
    od = copy.deepcopy(model["OriginDestinations"][0]) if model["OriginDestinations"] else {}
    od["DataIdOrigin"] = airports[origin_u]["DataId"]
    od["DataIdDestination"] = airports[dest_u]["DataId"]
    od["DateTime"] = f"{date}T00:00:00"
    od["DisabledDate"] = False
    od["DisabledLeg"] = False
    model["OriginDestinations"] = [od]

    adt_n = max(1, int(adt or 1))
    chd_n = max(0, int(chd or 0))
    inf_n = max(0, int(inf or 0))
    model["TravelerTypes"] = [
        {"Code": "AD", "Quantity": adt_n},
        {"Code": "CHD", "Quantity": chd_n},
        {"Code": "INF", "Quantity": inf_n},
    ]
    model["TripType"] = 0  # one-way
    model["Currency"] = {"Code": "BDT"}
    model["PromoCode"] = None

    service_map = _build_service_class_map(config)
    service_code = _cabin_to_service_code(cabin)
    if service_code in service_map:
        model["CabinClassDataId"] = service_map[service_code]

    return model


def bootstrap_config(cookies_path: Optional[str] = None) -> Dict[str, Any]:
    req = Requester(cookies_path=cookies_path, user_agent=USER_AGENT)
    resp = req.get(INDEX_URL, headers=BOOTSTRAP_HEADERS)
    if resp.status_code != 200:
        raise RuntimeError(f"2A bootstrap failed status={resp.status_code}")
    cfg = _extract_data_config(resp.text)
    cfg["_bootstrap_meta"] = {
        "index_url": str(resp.url),
        "status_code": resp.status_code,
    }
    return cfg


def discover_route_pairs(config: Optional[Dict[str, Any]] = None) -> List[tuple[str, str]]:
    cfg = config or bootstrap_config()
    origin_destinations = (
        cfg.get("sourceData", {})
        .get("Configuration", {})
        .get("OriginDestinations", [])
    )
    pairs: set[tuple[str, str]] = set()
    for item in origin_destinations:
        if not isinstance(item, dict):
            continue
        origin = str(item.get("Origin") or "").upper().strip()
        if not origin:
            continue
        for dest in item.get("Destinations") or []:
            dest_u = str(dest or "").upper().strip()
            if not dest_u or dest_u == origin:
                continue
            pairs.add((origin, dest_u))
    return sorted(pairs)


def discover_route_entries(
    config: Optional[Dict[str, Any]] = None,
    domestic_only: bool = False,
    allowed_origins: Optional[Iterable[str]] = None,
) -> List[Dict[str, Any]]:
    cfg = config or bootstrap_config()
    airports = (
        cfg.get("sourceData", {})
        .get("Configuration", {})
        .get("Airports", {})
    )
    service_map = _build_service_class_map(cfg)
    cabins = [_service_code_to_label(code) for code in ("Y", "P", "C") if code in service_map]
    if not cabins:
        cabins = ["Economy"]

    allowed_set = {str(x).upper().strip() for x in (allowed_origins or []) if str(x).strip()} or None
    entries: List[Dict[str, Any]] = []
    for origin, dest in discover_route_pairs(cfg):
        if allowed_set and origin not in allowed_set:
            continue
        if domestic_only:
            o_country = ((airports.get(origin) or {}).get("ISOCountry") or "").upper()
            d_country = ((airports.get(dest) or {}).get("ISOCountry") or "").upper()
            if not (o_country == "BD" and d_country == "BD"):
                continue
        entries.append(
            {
                "airline": "2A",
                "origin": origin,
                "destination": dest,
                "cabins": cabins,
            }
        )
    return entries


def _post_search(req: Requester, cfg: Dict[str, Any], model: Dict[str, Any]) -> tuple[int, Any]:
    rel_url = cfg.get("sourceData", {}).get("Urls", {}).get("SearchFlightsAction")
    if not rel_url:
        raise RuntimeError("2A SearchFlightsAction URL missing from bootstrap config")
    search_url = urljoin(BASE_URL, rel_url)
    headers = dict(SEARCH_HEADERS)
    headers["Referer"] = cfg.get("_bootstrap_meta", {}).get("index_url") or INDEX_URL
    resp = req.session.post(search_url, json=model, headers=headers, timeout=req.timeout)
    body: Any
    try:
        body = resp.json()
    except Exception:
        body = resp.text
    return resp.status_code, body


def _extract_rows_if_known(_search_body: Any) -> List[Dict[str, Any]]:
    # TODO: Implement TTInteractive SearchFlights parser after anti-bot access is solved.
    return []


def airastra_search(
    origin: str,
    dest: str,
    date: str,
    cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
    cookies_path: Optional[str] = None,
) -> Dict[str, Any]:
    output: Dict[str, Any] = {"raw": {}, "originalResponse": None, "rows": [], "ok": False}
    req = Requester(cookies_path=cookies_path, user_agent=USER_AGENT)

    try:
        bootstrap_resp = req.get(INDEX_URL, headers=BOOTSTRAP_HEADERS)
    except Exception as exc:
        LOG.error("[2A] bootstrap request failed: %s", exc)
        output["raw"] = {"error": "bootstrap_request_failed", "detail": str(exc)}
        return output

    output["raw"]["bootstrap_status"] = bootstrap_resp.status_code
    output["raw"]["bootstrap_url"] = str(bootstrap_resp.url)

    if bootstrap_resp.status_code != 200:
        output["raw"]["error"] = "bootstrap_non_200"
        return output

    try:
        cfg = _extract_data_config(bootstrap_resp.text)
    except Exception as exc:
        LOG.error("[2A] bootstrap parse failed: %s", exc)
        output["raw"]["error"] = "bootstrap_parse_failed"
        output["raw"]["detail"] = str(exc)
        return output

    cfg["_bootstrap_meta"] = {"index_url": str(bootstrap_resp.url)}
    output["raw"]["bootstrap_summary"] = {
        "vendor_date_now": (
            cfg.get("sourceData", {})
            .get("SearchModel", {})
            .get("VendorDateNow")
        ),
        "airports_count": len(
            (cfg.get("sourceData", {}).get("Configuration", {}).get("Airports", {})) or {}
        ),
        "route_pairs_count": len(discover_route_pairs(cfg)),
        "search_url_present": bool(
            cfg.get("sourceData", {}).get("Urls", {}).get("SearchFlightsAction")
        ),
    }

    try:
        model = _build_search_model(cfg, origin, dest, date, cabin, adt, chd, inf)
    except Exception as exc:
        LOG.error("[2A] invalid search inputs: %s", exc)
        output["raw"]["error"] = "invalid_search_inputs"
        output["raw"]["detail"] = str(exc)
        return output

    output["raw"]["request_model"] = model

    try:
        status_code, body = _post_search(req, cfg, model)
    except Exception as exc:
        LOG.error("[2A] SearchFlights request failed: %s", exc)
        output["raw"]["error"] = "search_request_failed"
        output["raw"]["detail"] = str(exc)
        return output

    output["raw"]["search_status"] = status_code
    output["raw"]["search_response"] = body

    if _is_datadome_block(status_code, body):
        LOG.warning("[2A] DataDome challenge detected on SearchFlightsAction (status=%s)", status_code)
        output["raw"]["error"] = "datadome_blocked"
        output["raw"]["hint"] = (
            "Air Astra TTInteractive search is protected by DataDome. "
            "Automated requests are blocked in this environment."
        )
        return output

    output["ok"] = status_code == 200
    if status_code == 200:
        output["originalResponse"] = body if isinstance(body, dict) else None
        output["rows"] = _extract_rows_if_known(body)
        if not output["rows"]:
            LOG.info("[2A] Search response received but parser is not implemented yet")
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
    return airastra_search(
        origin=origin,
        dest=destination,
        date=date,
        cabin=cabin,
        adt=adt,
        chd=chd,
        inf=inf,
    )


def cli_main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--origin")
    parser.add_argument("--destination")
    parser.add_argument("--date")
    parser.add_argument("--cabin", default="Economy")
    parser.add_argument("--adt", type=int, default=1)
    parser.add_argument("--chd", type=int, default=0)
    parser.add_argument("--inf", type=int, default=0)
    parser.add_argument("--discover-routes", action="store_true")
    parser.add_argument("--domestic-only", action="store_true")
    parser.add_argument("--origin-filter", action="append", default=[])
    args = parser.parse_args()

    if args.discover_routes:
        entries = discover_route_entries(
            domestic_only=args.domestic_only,
            allowed_origins=args.origin_filter,
        )
        print(json.dumps(entries, indent=2, ensure_ascii=False))
        return

    if not (args.origin and args.destination and args.date):
        parser.error("--origin, --destination, and --date are required unless --discover-routes is used")

    out = airastra_search(
        origin=args.origin,
        dest=args.destination,
        date=args.date,
        cabin=args.cabin,
        adt=args.adt,
        chd=args.chd,
        inf=args.inf,
    )
    print(json.dumps(out, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    cli_main()
