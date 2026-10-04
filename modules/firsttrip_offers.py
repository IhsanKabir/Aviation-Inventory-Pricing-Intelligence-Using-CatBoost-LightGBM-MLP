"""FirstTrip B2C coupon catalogue: every coupon a fare can take, not just the one the
search auto-applies.

The search response carries ONE auto-applied coupon per fare (couponCode) plus the
dynamic discount. The payment page asks for the full list:

  POST /flight/api/v1/DiscountsAndCoupons/GetOfferList      every coupon for the fare
  POST /flight/api/v1/DiscountsAndCoupons/GetPerkOfferList  telco perk partners

GetOfferList returns one row per (coupon, payment method). Each coupon carries:
  couponDiscountType  'P' = percent of the base fare, 'F' = FLAT BDT amount (the
                      "10000% FTCITYAMEX" bug was a flat 10,000 BDT read as a %)
  couponMaximumDiscountAmount  BDT cap;  minimumSalesAmount  minimum base fare
  dynamicConfig       the coupon's RATE TABLE by airline and route (FTINT26 had 106
                      entries): when isDynamicConfigurationEnabled, the coupon applies
                      only where configured, at that entry's rate
  paymentMethod/bankInfoId  who can use it: a wallet (bKash/Nagad/Upay) or a card
                      with no bank restriction is open to anyone paying that way
  isAllowWithCoupon   false: it REPLACES the dynamic discount, never stacks (a 2026-10
                      RePrice: FTBSDOM 15% applied, FT-Nagad 12% not added on top)

Verified on 2026-10-04 captures (DAC-CXB and DAC-DXB, BS). Telco perks list only the
partners (GP/Robi/Banglalink/Skitto); their rates come after the customer picks an
operator and verifies the number, which these captures don't contain.
"""
from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

API = "https://b2c-api.firsttrip.com/flight/api/v1/DiscountsAndCoupons"
OFFER_LIST = "/DiscountsAndCoupons/GetOfferList"
PERK_LIST = "/DiscountsAndCoupons/GetPerkOfferList"
WALLETS = ("bkash", "nagad", "upay", "rocket", "tap", "ok wallet", "okwallet", "cellfin")
OFFER_SLEEP = 1.0           # live: pause between offer-list calls (one per market x airline)


def _market(flight_type: Any) -> str:
    return "DOM" if str(flight_type) == "1" else "INTL"


def _is_wallet(method: str) -> bool:
    return any(w in method.lower() for w in WALLETS)


def coupons_from_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """GetOfferList data rows (one per coupon x payment method) -> one dict per coupon."""
    by_code: Dict[str, Dict[str, Any]] = {}
    for r in rows or []:
        code = str(r.get("code") or "").strip()
        if not code:
            continue
        c = by_code.get(code)
        if c is None:
            c = by_code[code] = {
                "code": code,
                "type": "F" if str(r.get("couponDiscountType") or "").upper() == "F" else "P",
                "value": float(r.get("couponDiscountValue") or r.get("couponDiscountRate") or 0),
                "cap": float(r.get("couponMaximumDiscountAmount") or 0) or None,
                "min_sales": float(r.get("minimumSalesAmount") or 0),
                "market": _market(r.get("flightType")),
                "description": " ".join(str(r.get("description") or "").split()),
                "title": str(r.get("promotionTitle") or ""),
                "stacks_with_dynamic": bool(r.get("isAllowWithCoupon")),
                "configured_only": bool(r.get("isDynamicConfigurationEnabled")),
                "config": [{"airline": str(d.get("airlineCode") or "").upper(),
                            "origin": (d.get("departureAirportCode") or None),
                            "destination": (d.get("arrivalAirportCode") or None),
                            "type": "F" if str(d.get("discountType") or "").upper() == "F" else "P",
                            "value": float(d.get("discountValue") or d.get("discountRate") or 0),
                            "cap": float(d.get("maximumDiscountAmount") or 0) or None}
                           for d in (r.get("dynamicConfig") or [])],
                "valid_to": str(r.get("validTo") or "")[:10],
                "methods": [], "open_methods": [], "bank_cards": 0,
            }
        method = str(r.get("paymentMethod") or "").strip()
        if method and method not in c["methods"]:
            c["methods"].append(method)
        if method and (r.get("bankInfoId") is None or _is_wallet(method)) and method not in c["open_methods"]:
            c["open_methods"].append(method)
        if r.get("bankInfoId") is not None and not _is_wallet(method):
            c["bank_cards"] += 1
    return list(by_code.values())


def audience(coupon: Dict[str, Any]) -> tuple[str, str]:
    """('common'|'special', who). Open to anyone paying with a wallet or an
    unrestricted card -> common; only certain banks' cards -> special."""
    bank_only = [m for m in coupon["methods"] if m not in coupon["open_methods"]]
    if coupon["open_methods"]:
        extra = f", or {' / '.join(bank_only)} cards of listed banks" if bank_only else ""
        return "common", "Pay with " + " / ".join(coupon["open_methods"]) + extra
    if coupon["methods"]:
        return "special", f"{' / '.join(coupon['methods'])} cards of listed banks"
    return "special", "Card holders"


def rate_for(coupon: Dict[str, Any], airline: str, origin: str, destination: str
             ) -> Optional[Dict[str, Any]]:
    """{type, value, cap} for this airline/route, or None when the coupon doesn't apply.
    A configured coupon applies only where its table lists the airline (route blank =
    any route); the most specific entry wins."""
    airline, origin, destination = airline.upper(), origin.upper(), destination.upper()
    hits = [d for d in coupon["config"] if d["airline"] == airline
            and d["origin"] in (None, origin) and d["destination"] in (None, destination)]
    if hits:
        best = max(hits, key=lambda d: (d["origin"] is not None) + (d["destination"] is not None))
        return {"type": best["type"], "value": best["value"], "cap": best["cap"] or coupon["cap"]}
    if coupon["configured_only"] and coupon["config"]:
        return None
    return {"type": coupon["type"], "value": coupon["value"], "cap": coupon["cap"]}


def worth(coupon: Dict[str, Any], airline: str, origin: str, destination: str,
          base_fare: float) -> Optional[Dict[str, Any]]:
    """What the coupon saves on this fare: {pct (of base), amount, capped, rate}."""
    rate = rate_for(coupon, airline, origin, destination)
    if rate is None or base_fare <= 0 or base_fare < coupon["min_sales"]:
        return None
    raw = rate["value"] if rate["type"] == "F" else math.floor(base_fare * rate["value"] / 100)
    amount = min(raw, rate["cap"]) if rate["cap"] else raw
    if amount <= 0:
        return None
    capped = bool(rate["cap"]) and raw > rate["cap"]
    # an uncapped percent coupon is worth its published rate (FirstTrip floors the taka
    # amount, which would turn 12% into 11.98%); flat or capped ones are worth the amount
    pct = rate["value"] if rate["type"] == "P" and not capped else round(amount / base_fare * 100, 2)
    return {"pct": pct, "amount": round(amount), "capped": capped, "rate": rate}


def dynamic_pct(row: Dict[str, Any]) -> float:
    """The dynamic discount on this fare: its published rate, or less when its cap bit
    (the amount FirstTrip applied is then well under rate x base)."""
    rate = float(row.get("dynamic_rate") or 0)
    base = float(row.get("base_fare_bdt") or 0)
    amount = float(row.get("dynamic_amount_bdt") or 0)
    if rate <= 0 or base <= 0 or amount <= 0:
        return rate
    full = math.floor(base * rate / 100)
    return rate if amount >= full - 1 else round(amount / base * 100, 2)


def _load(path: str | Path) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def parse_har(path: str | Path, *, har: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """{'coupons': [...], 'perks': {market: [partner, ...]}} from a FirstTrip B2C HAR."""
    har = har if har is not None else _load(path)
    coupons: Dict[str, Dict[str, Any]] = {}
    perks: Dict[str, List[str]] = {}
    for e in (har.get("log") or {}).get("entries", []):
        req = e.get("request") or {}
        url = str(req.get("url") or "")
        if req.get("method") != "POST" or "b2c-api.firsttrip.com" not in url:
            continue
        try:
            data = json.loads(((e.get("response") or {}).get("content") or {}).get("text") or "{}")
            body = json.loads((req.get("postData") or {}).get("text") or "{}")
        except (json.JSONDecodeError, TypeError):
            continue
        rows = data.get("data") if isinstance(data, dict) else None
        if url.endswith(OFFER_LIST) and isinstance(rows, list):
            for c in coupons_from_rows(rows):
                prev = coupons.get(c["code"])
                coupons[c["code"]] = c if prev is None else merge(prev, c)
        elif url.endswith(PERK_LIST) and isinstance(rows, list):
            names = perks.setdefault(_market(body.get("flightType")), [])
            for p in rows:
                name = str((p or {}).get("partnerName") or "").strip()
                if name and name not in names:
                    names.append(name)
    return {"coupons": list(coupons.values()), "perks": perks}


def merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """The same coupon seen in two captures (e.g. requested for two airlines): union of
    payment methods and rate-table entries."""
    out = {**a}
    for k in ("methods", "open_methods"):
        out[k] = a[k] + [m for m in b[k] if m not in a[k]]
    seen = {json.dumps(d, sort_keys=True) for d in a["config"]}
    out["config"] = a["config"] + [d for d in b["config"] if json.dumps(d, sort_keys=True) not in seen]
    out["bank_cards"] = max(a["bank_cards"], b["bank_cards"])
    return out


def merge_catalogs(catalogs: List[Dict[str, Any]]) -> Dict[str, Any]:
    coupons: Dict[str, Dict[str, Any]] = {}
    perks: Dict[str, List[str]] = {}
    for cat in catalogs:
        for c in cat.get("coupons") or []:
            coupons[c["code"]] = c if c["code"] not in coupons else merge(coupons[c["code"]], c)
        for m, names in (cat.get("perks") or {}).items():
            have = perks.setdefault(m, [])
            have += [n for n in names if n not in have]
    return {"coupons": list(coupons.values()), "perks": perks}


# --- per-fare options for the grid -----------------------------------------------------

def fare_options(row: Dict[str, Any], catalog: Optional[Dict[str, Any]], *,
                 classify_code=None) -> List[Dict[str, Any]]:
    """Every discount this fare can get: the dynamic discount, the auto-applied coupon
    and each catalogue coupon that applies -> [{code, pct, tier, who, capped, source}].
    classify_code(code, airline) -> (tier, who) labels a coupon not in the catalogue."""
    base = float(row.get("base_fare_bdt") or 0)
    airline, o, d = row["airline"], row.get("origin") or "", row.get("destination") or ""
    out: List[Dict[str, Any]] = []
    if (row.get("dynamic_rate") or 0) > 0:
        pct = dynamic_pct(row)
        out.append({"code": row.get("dynamic_code") or "", "pct": pct, "tier": "common",
                    "who": "Anyone (automatic)", "capped": pct < float(row["dynamic_rate"]),
                    "source": "dynamic"})
    in_catalog = {c["code"] for c in (catalog or {}).get("coupons") or []}
    for c in (catalog or {}).get("coupons") or []:
        w = worth(c, airline, o, d, base)
        if w:
            tier, who = audience(c)
            out.append({"code": c["code"], "pct": w["pct"], "tier": tier, "who": who,
                        "capped": w["capped"], "source": "offer list"})
    code, rate = row.get("coupon_code") or "", float(row.get("headline_rate") or 0)
    if code and rate > 0 and code not in in_catalog:
        tier, who = classify_code(code, airline) if classify_code else ("special", code)
        out.append({"code": code, "pct": rate, "tier": "common" if tier.lower() == "common" else "special",
                    "who": who, "capped": False, "source": "search"})
    slot, srate = row.get("special_code") or "", float(row.get("special_rate") or 0)
    if slot and srate > 0 and slot != code and slot not in in_catalog:
        out.append({"code": slot, "pct": srate, "tier": "special", "who": "Card holders",
                    "capped": False, "source": "search"})
    return out


def summarize(rows: List[Dict[str, Any]], catalog: Optional[Dict[str, Any]] = None, *,
              classify_code=None, label_of=None) -> Dict[str, Dict[str, Any]]:
    """One grid cell per airline. FirstTrip coupons REPLACE the dynamic discount (they
    don't stack), so the customer gets the single best option they qualify for:
      common  = the best option anyone can get (dynamic, or a wallet / open-card coupon)
      special = the best card/bank-only option, shown only when it beats the common
    Each is the best over the airline's fares seen. Shape matches the legacy cell."""
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        cell = out.setdefault(r["airline"], {"common_rate": 0.0, "common_code": None, "common_capped": False,
                                             "special_rate": None, "special_code": None,
                                             "special_label": None, "special_capped": False})
        for opt in fare_options(r, catalog, classify_code=classify_code):
            if opt["tier"] == "common" and opt["pct"] > cell["common_rate"]:
                cell.update(common_rate=opt["pct"], common_code=opt["code"] or None,
                            common_capped=opt["capped"],
                            common_from_coupon=opt["source"] != "dynamic")
            elif opt["tier"] == "special" and opt["pct"] > (cell["special_rate"] or 0):
                cell.update(special_rate=opt["pct"], special_code=opt["code"],
                            special_label=(label_of(opt["code"]) if label_of else None) or opt["code"],
                            special_capped=opt["capped"])
    for cell in out.values():
        if cell["special_rate"] is not None and cell["special_rate"] <= cell["common_rate"]:
            cell.update(special_rate=None, special_code=None, special_label=None, special_capped=False)
        cell["rate"] = max(cell["common_rate"], cell["special_rate"] or 0)
    return out


# --- live (admin runs only; same request the payment page sends) -----------------------

def fetch_catalog(fares: List[Dict[str, Any]], headers: Dict[str, str], post=None,
                  sleep=time.sleep) -> Dict[str, Any]:
    """GetOfferList (+ GetPerkOfferList) for ONE fare per (market, airline): a coupon's
    rate table covers the airline's other routes, so that is all the catalogue needs.
    `fares` are offer rows carrying offer_request (built from the search offer)."""
    import requests
    post = post or requests.post
    catalogs: List[Dict[str, Any]] = []
    done: set = set()
    perk_markets: set = set()
    calls = 0

    def call(path: str, body: Dict[str, Any]) -> Optional[list]:
        nonlocal calls
        if calls:
            sleep(OFFER_SLEEP)
        calls += 1
        r = post(f"{API}/{path}", json=body, headers=headers, timeout=30)
        data = (r.json() or {}).get("data") if r.status_code == 200 else None
        return data if isinstance(data, list) else None

    for row in fares:
        req = row.get("offer_request")
        if not req or (req.get("flightType"), row.get("airline")) in done:
            continue
        done.add((req.get("flightType"), row.get("airline")))
        market = _market(req.get("flightType"))
        try:
            cat: Dict[str, Any] = {"coupons": coupons_from_rows(call("GetOfferList", req) or []),
                                   "perks": {}}
            if market not in perk_markets:              # the partner list is per market
                perk_markets.add(market)
                partners = call("GetPerkOfferList", {**req, "couponType": 4}) or []
                cat["perks"] = {market: [str(x.get("partnerName")) for x in partners
                                         if isinstance(x, dict) and x.get("partnerName")]}
            catalogs.append(cat)
        except Exception:  # noqa: BLE001 — a missing catalogue only drops the extra coupons
            continue
    return merge_catalogs(catalogs)
