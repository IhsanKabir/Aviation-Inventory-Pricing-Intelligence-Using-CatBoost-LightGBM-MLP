import json
from pathlib import Path
from typing import Dict


AIRPORT_COUNTRIES_FILE = Path("config/airport_countries.json")

COUNTRY_ALIASES = {
    "BANGLADESH": "BD",
    "INDIA": "IN",
    "UNITED ARAB EMIRATES": "AE",
    "UAE": "AE",
    "SAUDI ARABIA": "SA",
    "UK": "GB",
    "UNITED KINGDOM": "GB",
    "QATAR": "QA",
    "SINGAPORE": "SG",
    "MALAYSIA": "MY",
    "CHINA": "CN",
    "OMAN": "OM",
    "KUWAIT": "KW",
    "SRI LANKA": "LK",
    "MALDIVES": "MV",
    "THAILAND": "TH",
    "BAHRAIN": "BH",
}


def normalize_country_code(raw: str | None) -> str:
    value = str(raw or "").strip().upper()
    if not value:
        return ""
    if len(value) == 2 and value.isalpha():
        return value
    return COUNTRY_ALIASES.get(value, value)


def load_airport_countries(path: Path = AIRPORT_COUNTRIES_FILE) -> Dict[str, str]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    out: Dict[str, str] = {}
    if not isinstance(data, dict):
        return out
    for airport, country in data.items():
        a = str(airport or "").strip().upper()
        c = normalize_country_code(country)
        if a and c:
            out[a] = c
    return out


def classify_route_scope(
    origin: str | None,
    destination: str | None,
    *,
    airport_countries: Dict[str, str],
    market_country: str,
) -> str:
    origin_code = str(origin or "").strip().upper()
    destination_code = str(destination or "").strip().upper()
    origin_country = airport_countries.get(origin_code)
    destination_country = airport_countries.get(destination_code)
    market = normalize_country_code(market_country)

    if not origin_country or not destination_country or not market:
        return "unknown"
    if origin_country == market and destination_country == market:
        return "domestic"
    return "international"


def route_matches_scope(
    origin: str | None,
    destination: str | None,
    *,
    scope: str,
    airport_countries: Dict[str, str],
    market_country: str,
) -> bool:
    scope_norm = str(scope or "all").strip().lower()
    if scope_norm == "all":
        return True
    route_scope = classify_route_scope(
        origin,
        destination,
        airport_countries=airport_countries,
        market_country=market_country,
    )
    if route_scope == "unknown":
        return False
    return route_scope == scope_norm


def parse_csv_upper_codes(raw: str | None) -> list[str]:
    items: list[str] = []
    seen = set()
    for token in str(raw or "").split(","):
        code = token.strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        items.append(code)
    return items
