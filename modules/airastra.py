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
import os
import re
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin

from modules.amybd import fetch_flights_for_airline as fetch_from_amybd
from modules.bdfare import fetch_flights_for_airline as fetch_from_bdfare
from modules.gozayaan import fetch_flights_for_airline as fetch_from_gozayaan
from modules.requester import Requester
from modules.sharetrip import fetch_flights_for_airline as fetch_from_sharetrip
from modules.ttinteractive_flexible_html_parser import extract_flexible_fares_from_search_body


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

ENV_COOKIES_PATH = "AIRASTRA_COOKIES_PATH"
ENV_PROXY_URL = "AIRASTRA_PROXY_URL"
ENV_SOURCE_MODE = "AIRASTRA_SOURCE_MODE"
ENV_FALLBACK_ON_EMPTY = "AIRASTRA_BDFARE_FALLBACK_ON_EMPTY"
ENV_AUTO_SOURCE_CHAIN = "AIRASTRA_AUTO_SOURCE_CHAIN"


def _env_true(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _has_usable_rows(result: Any) -> bool:
    rows = result.get("rows") if isinstance(result, dict) else None
    ok = bool(result.get("ok")) if isinstance(result, dict) else False
    return ok and isinstance(rows, list) and bool(rows)


def _source_attempt_summary(source: str, result: Any) -> Dict[str, Any]:
    raw = result.get("raw") if isinstance(result, dict) else {}
    rows = result.get("rows") if isinstance(result, dict) else None
    return {
        "source": source,
        "ok": bool(result.get("ok")) if isinstance(result, dict) else False,
        "rows": len(rows) if isinstance(rows, list) else None,
        "error": (raw or {}).get("error") if isinstance(raw, dict) else None,
        "message": (raw or {}).get("message") if isinstance(raw, dict) else None,
    }


def _fetch_from_source(
    source: str,
    *,
    origin: str,
    destination: str,
    date: str,
    cabin: str,
    adt: int,
    chd: int,
    inf: int,
) -> Dict[str, Any]:
    source_name = str(source or "").strip().lower()
    if source_name == "bdfare":
        return fetch_from_bdfare(
            airline_code="2A",
            origin=origin,
            destination=destination,
            date=date,
            cabin=cabin,
            adt=adt,
            chd=chd,
            inf=inf,
        )
    if source_name == "gozayaan":
        return fetch_from_gozayaan(
            airline_code="2A",
            origin=origin,
            destination=destination,
            date=date,
            cabin=cabin,
            adt=adt,
            chd=chd,
            inf=inf,
        )
    if source_name == "amybd":
        return fetch_from_amybd(
            airline_code="2A",
            origin=origin,
            destination=destination,
            date=date,
            cabin=cabin,
            adt=adt,
            chd=chd,
            inf=inf,
        )
    if source_name == "sharetrip":
        return fetch_from_sharetrip(
            airline_code="2A",
            origin=origin,
            destination=destination,
            date=date,
            cabin=cabin,
            adt=adt,
            chd=chd,
            inf=inf,
        )
    if source_name == "ttinteractive":
        cookies_path = os.getenv(ENV_COOKIES_PATH) or None
        proxy_url = os.getenv(ENV_PROXY_URL) or None
        return airastra_search(
            origin=origin,
            dest=destination,
            date=date,
            cabin=cabin,
            adt=adt,
            chd=chd,
            inf=inf,
            cookies_path=cookies_path,
            proxy_url=proxy_url,
        )
    raise ValueError(f"Unsupported 2A source mode: {source}")


def _auto_source_chain() -> List[str]:
    raw = str(os.getenv(ENV_AUTO_SOURCE_CHAIN, "") or "").strip()
    if raw:
        parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
        deduped: List[str] = []
        for part in parts:
            if part not in deduped:
                deduped.append(part)
        if deduped:
            return deduped
    return ["bdfare", "sharetrip"]


def _run_auto_source_chain(
    *,
    origin: str,
    destination: str,
    date: str,
    cabin: str,
    adt: int,
    chd: int,
    inf: int,
) -> Dict[str, Any]:
    chain = _auto_source_chain()
    attempts: List[Dict[str, Any]] = []
    first_ok_result: Optional[Dict[str, Any]] = None
    last_result: Optional[Dict[str, Any]] = None
    for source in chain:
        try:
            result = _fetch_from_source(
                source,
                origin=origin,
                destination=destination,
                date=date,
                cabin=cabin,
                adt=adt,
                chd=chd,
                inf=inf,
            )
        except Exception as exc:
            result = {
                "raw": {"error": "source_execution_failed", "message": str(exc)},
                "originalResponse": None,
                "rows": [],
                "ok": False,
            }
        attempts.append(_source_attempt_summary(source, result))
        if isinstance(result, dict):
            raw = result.setdefault("raw", {})
            if isinstance(raw, dict):
                raw["auto_source_chain"] = chain
                raw["auto_source_attempts"] = attempts
            if first_ok_result is None and bool(result.get("ok")):
                first_ok_result = result
            last_result = result
        if _has_usable_rows(result):
            return result
    return first_ok_result or last_result or {
        "raw": {
            "error": "auto_chain_failed",
            "auto_source_chain": chain,
            "auto_source_attempts": attempts,
        },
        "originalResponse": None,
        "rows": [],
        "ok": False,
    }


def _sharetrip_fetch_with_bdfare_fallback(
    origin: str,
    destination: str,
    date: str,
    cabin: str,
    adt: int,
    chd: int,
    inf: int,
) -> Dict[str, Any]:
    sharetrip_out = fetch_from_sharetrip(
        airline_code="2A",
        origin=origin,
        destination=destination,
        date=date,
        cabin=cabin,
        adt=adt,
        chd=chd,
        inf=inf,
    )
    st_rows = sharetrip_out.get("rows") if isinstance(sharetrip_out, dict) else None
    st_ok = bool(sharetrip_out.get("ok")) if isinstance(sharetrip_out, dict) else False
    if st_ok and isinstance(st_rows, list) and st_rows:
        return sharetrip_out

    # Fast-path: ShareTrip returned a clean, successful empty result.
    # Default behavior skips BDFare fallback to avoid slow no-inventory polls.
    if st_ok and isinstance(st_rows, list) and not st_rows and not _env_true(ENV_FALLBACK_ON_EMPTY, default=False):
        return sharetrip_out

    LOG.warning("[2A] ShareTrip returned no usable rows; attempting BDFare fallback")
    bdfare_out = fetch_from_bdfare(
        airline_code="2A",
        origin=origin,
        destination=destination,
        date=date,
        cabin=cabin,
        adt=adt,
        chd=chd,
        inf=inf,
    )
    bd_rows = bdfare_out.get("rows") if isinstance(bdfare_out, dict) else None
    bd_ok = bool(bdfare_out.get("ok")) if isinstance(bdfare_out, dict) else False
    if bd_ok and isinstance(bd_rows, list) and bd_rows:
        return bdfare_out

    return sharetrip_out if isinstance(sharetrip_out, dict) else bdfare_out


def _bdfare_fetch_with_sharetrip_fallback(
    origin: str,
    destination: str,
    date: str,
    cabin: str,
    adt: int,
    chd: int,
    inf: int,
) -> Dict[str, Any]:
    bdfare_out = fetch_from_bdfare(
        airline_code="2A",
        origin=origin,
        destination=destination,
        date=date,
        cabin=cabin,
        adt=adt,
        chd=chd,
        inf=inf,
    )
    bd_rows = bdfare_out.get("rows") if isinstance(bdfare_out, dict) else None
    bd_ok = bool(bdfare_out.get("ok")) if isinstance(bdfare_out, dict) else False
    if bd_ok and isinstance(bd_rows, list) and bd_rows:
        return bdfare_out

    LOG.warning("[2A] BDFare returned no usable rows; attempting ShareTrip fallback")
    sharetrip_out = fetch_from_sharetrip(
        airline_code="2A",
        origin=origin,
        destination=destination,
        date=date,
        cabin=cabin,
        adt=adt,
        chd=chd,
        inf=inf,
    )
    st_rows = sharetrip_out.get("rows") if isinstance(sharetrip_out, dict) else None
    st_ok = bool(sharetrip_out.get("ok")) if isinstance(sharetrip_out, dict) else False
    if st_ok and isinstance(st_rows, list) and st_rows:
        return sharetrip_out
    return bdfare_out if isinstance(bdfare_out, dict) else sharetrip_out


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


def bootstrap_config(
    cookies_path: Optional[str] = None,
    proxy_url: Optional[str] = None,
) -> Dict[str, Any]:
    req = Requester(cookies_path=cookies_path, user_agent=USER_AGENT, proxy_url=proxy_url)
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


def _extract_rows_if_known(
    search_body: Any,
    *,
    cfg: Optional[Dict[str, Any]],
    cabin: str,
    adt: int,
    chd: int,
    inf: int,
) -> List[Dict[str, Any]]:
    return extract_flexible_fares_from_search_body(
        search_body,
        config=cfg,
        airline_code="2A",
        requested_cabin=cabin,
        adt=adt,
        chd=chd,
        inf=inf,
    )


def airastra_search(
    origin: str,
    dest: str,
    date: str,
    cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
    cookies_path: Optional[str] = None,
    proxy_url: Optional[str] = None,
) -> Dict[str, Any]:
    output: Dict[str, Any] = {"raw": {}, "originalResponse": None, "rows": [], "ok": False}
    req = Requester(cookies_path=cookies_path, user_agent=USER_AGENT, proxy_url=proxy_url)
    if cookies_path or proxy_url:
        output["raw"]["access_path"] = {
            "cookies_path": cookies_path,
            "proxy_url": proxy_url,
        }

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
            "Automated requests are blocked in this environment. "
            "Use tools/ttinteractive_browser_assisted_search.py to capture cookies, then set "
            "AIRASTRA_COOKIES_PATH and optionally AIRASTRA_PROXY_URL."
        )
        return output

    output["ok"] = status_code == 200
    if status_code == 200:
        output["originalResponse"] = body if isinstance(body, dict) else None
        output["rows"] = _extract_rows_if_known(
            body,
            cfg=cfg,
            cabin=cabin,
            adt=adt,
            chd=chd,
            inf=inf,
        )
        if not output["rows"]:
            LOG.info("[2A] Search response received but no TTInteractive fare rows were found")
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
    source_mode = (os.getenv(ENV_SOURCE_MODE) or "auto").strip().lower()
    if source_mode in {"auto", "bdfare_first"}:
        return _run_auto_source_chain(
            origin=origin,
            destination=destination,
            date=date,
            cabin=cabin,
            adt=adt,
            chd=chd,
            inf=inf,
        )
    if source_mode == "sharetrip":
        return _sharetrip_fetch_with_bdfare_fallback(
            origin=origin,
            destination=destination,
            date=date,
            cabin=cabin,
            adt=adt,
            chd=chd,
            inf=inf,
        )
    if source_mode == "bdfare":
        return fetch_from_bdfare(
            airline_code="2A",
            origin=origin,
            destination=destination,
            date=date,
            cabin=cabin,
            adt=adt,
            chd=chd,
            inf=inf,
        )
    if source_mode == "amybd":
        amy = fetch_from_amybd(
            airline_code="2A",
            origin=origin,
            destination=destination,
            date=date,
            cabin=cabin,
            adt=adt,
            chd=chd,
            inf=inf,
        )
        if bool(amy.get("ok")):
            return amy
        msg = str(((amy.get("raw") or {}).get("message")) or "")
        err = str(((amy.get("raw") or {}).get("error")) or "")
        if err == "search_not_ok" and "invalid login" in msg.lower():
            LOG.warning("[2A] AMYBD returned Invalid Login; falling back to ShareTrip")
            return fetch_from_sharetrip(
                airline_code="2A",
                origin=origin,
                destination=destination,
                date=date,
                cabin=cabin,
                adt=adt,
                chd=chd,
                inf=inf,
            )
        return amy
    if source_mode == "gozayaan":
        return fetch_from_gozayaan(
            airline_code="2A",
            origin=origin,
            destination=destination,
            date=date,
            cabin=cabin,
            adt=adt,
            chd=chd,
            inf=inf,
        )
    if source_mode != "ttinteractive":
        return _sharetrip_fetch_with_bdfare_fallback(
            origin=origin,
            destination=destination,
            date=date,
            cabin=cabin,
            adt=adt,
            chd=chd,
            inf=inf,
        )

    cookies_path = os.getenv(ENV_COOKIES_PATH) or None
    proxy_url = os.getenv(ENV_PROXY_URL) or None
    return airastra_search(
        origin=origin,
        dest=destination,
        date=date,
        cabin=cabin,
        adt=adt,
        chd=chd,
        inf=inf,
        cookies_path=cookies_path,
        proxy_url=proxy_url,
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
    parser.add_argument("--cookies-path", help=f"Cookie JSON path (Requester-compatible dict) or use {ENV_COOKIES_PATH}")
    parser.add_argument("--proxy-url", help=f"Proxy URL (e.g. http://host:port) or use {ENV_PROXY_URL}")
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

    cookies_path = args.cookies_path or os.getenv(ENV_COOKIES_PATH) or None
    proxy_url = args.proxy_url or os.getenv(ENV_PROXY_URL) or None
    out = airastra_search(
        origin=args.origin,
        dest=args.destination,
        date=args.date,
        cabin=args.cabin,
        adt=args.adt,
        chd=args.chd,
        inf=args.inf,
        cookies_path=cookies_path,
        proxy_url=proxy_url,
    )
    print(json.dumps(out, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    cli_main()
