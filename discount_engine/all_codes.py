"""Every discount code an OTA offered, not just the one the grid shows.

The summary and per-route grids keep ONE common rate and ONE special coupon per
cell (the best). This list keeps all of them: every promo code each B2C channel
served, with its real code name, who can use it, the published rate, what it was
actually worth at the fares seen (caps applied), the cap, the payment fee and
whether it stacks on the automatic discount.

Tier:
  * Common  - anyone gets it (automatic discount, wallet/any-card promo).
  * Special - needs a specific card, bank or membership (Stellar, AMEX, GPStar...).

Only the coupon channels are listed (FirstTrip B2C, ShareTrip, GoZayaan). The B2B
channels and Amy pay a commission with no promo code; their rates are in the grids.

Rows come from the parses build_report already did (memoized), so nothing is read
or fetched twice. One entry per (route, OTA, airline, code); when the same code was
seen on several fares its effective rate is shown as the lowest-highest range.
"""
from __future__ import annotations

import math
from typing import Any, Optional

from . import grid as g

COMMON, SPECIAL = "Common", "Special"

# Column order for the sheet and the app's table: (key, heading).
COLUMNS = [
    ("market", "Market"), ("route", "Route"), ("ota", "OTA"), ("airline", "Airline"),
    ("tier", "Tier"), ("code", "Promo code"), ("who", "Who can use it"),
    ("published_pct", "Published %"), ("effective", "Effective %"), ("cap_bdt", "Cap (BDT)"),
    ("stacks", "On top of automatic"), ("fee_pct", "Payment fee %"), ("seen", "Fares seen"),
    ("title", "Offer text"),
]


def _market(flight_type: Any, origin: str = "", destination: str = "") -> str:
    if origin and destination:
        return g._route_type(origin, destination)
    return "DOM" if str(flight_type).upper() == "DOM" else "INTL"


def _route(r: dict[str, Any]) -> str:
    o, d = str(r.get("origin") or "").upper(), str(r.get("destination") or "").upper()
    return f"{o}-{d}" if o and d else ""


def _obs(*, market: str, route: str, ota: str, airline: str, tier: str, code: str,
         who: str, published: Optional[float], effective: Optional[float],
         cap: Any = None, stacks: Optional[bool] = None, fee: Any = None,
         title: str = "") -> dict[str, Any]:
    """One sighting of one code on one fare."""
    return {"market": market, "route": route, "ota": ota, "airline": airline,
            "tier": tier, "code": code or "(no code)", "who": who,
            "published_pct": published, "effective_pct": effective,
            "cap_bdt": round(float(cap)) if cap else None, "stacks": stacks,
            "fee_pct": fee, "title": title}


# --- ShareTrip ----------------------------------------------------------------------

def _sharetrip_titles(details: list[dict[str, Any]]) -> dict[str, str]:
    return {str(c.get("couponCode")): str(c.get("title") or "")
            for r in details for c in (r.get("coupon_terms") or []) if c.get("couponCode")}


def _sharetrip_obs(market: str, route: str, airline: str, auto_pct: float,
                   auto_code: Optional[str], cell: dict[str, Any],
                   titles: dict[str, str]) -> list[dict[str, Any]]:
    """Sightings for one judged ShareTrip fare: the automatic discount, the stacking
    wallet coupon (both Common) and every other coupon (Special)."""
    out = []
    ota = "ShareTrip-B2C"
    if auto_pct > 0:
        out.append(_obs(market=market, route=route, ota=ota, airline=airline, tier=COMMON,
                        code=auto_code or "", who="Anyone (automatic)",
                        published=auto_pct, effective=auto_pct,
                        title=titles.get(auto_code or "", "")))
    wallet = cell.get("common_code")
    for j in cell.get("judged") or []:
        is_wallet = bool(wallet) and j["code"] == wallet
        out.append(_obs(market=market, route=route, ota=ota, airline=airline,
                        tier=COMMON if is_wallet else SPECIAL, code=j["code"],
                        who=f"{j['label']} payment" if is_wallet else j["label"],
                        published=j["nominal_pct"], effective=j["effective_pct"],
                        cap=j.get("cap_bdt"), stacks=j["stacks_with_auto"],
                        fee=j.get("fee_pct"), title=titles.get(j["code"], "")))
    return out


def sharetrip(hars: Optional[list[str]]) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    search: list[dict[str, Any]] = []
    for h in hars or []:
        d, s = g._recall("sharetrip_routed", h, ([], []))
        details += d
        search += s
    if not details and not search:
        return []
    gateways = g._recall("sharetrip_gateways", "all", {}) or None
    titles = _sharetrip_titles(details)
    # Coupon terms are market-uniform (see grid.collect_sharetrip_b2c): fares seen only
    # in a search are judged with the market's terms at their own base fare.
    terms: dict[str, list[dict[str, Any]]] = {}
    for r in details:
        terms.setdefault(_market(r["flight_type"]), r.get("coupon_terms") or [])
    auto_code = {(_route(r), r["airline"]): r.get("coupon_code") for r in search}

    out: list[dict[str, Any]] = []
    for r in details:
        route = _route(r)
        out += _sharetrip_obs(_market(r["flight_type"], r.get("origin"), r.get("destination")),
                              route, r["airline"], float(r.get("base_pct") or 0),
                              auto_code.get((route, r["airline"])), r, titles)
    for r in search:
        market = _market(r["flight_type"], r.get("origin"), r.get("destination"))
        auto = float(r.get("discount_pct") or 0)
        cell = g.sharetrip_har.judge_cell(auto, float(r.get("base_fare_bdt") or 0),
                                          terms.get(market) or [], gateways=gateways)
        out += _sharetrip_obs(market, _route(r), r["airline"], auto,
                              r.get("coupon_code"), cell, titles)
    return out


# --- GoZayaan -----------------------------------------------------------------------

def gozayaan(hars: Optional[list[str]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for h in hars or []:
        rows, surcharge = g._recall("gozayaan_routed", h, ([], {}))
        for r in rows:
            market = _market(r["flight_type"], r.get("origin"), r.get("destination"))
            out.append(_obs(
                market=market, route=_route(r), ota="Go Zayaan", airline=r["airline"],
                tier=COMMON if r["eligibility_scope"] == "common" else SPECIAL,
                code=r["coupon_code"], who=r["eligibility"],
                published=r["discount_pct"], effective=r["realized_pct"],
                cap=r.get("cap_bdt"), fee=(surcharge or {}).get(market),
                title=r.get("name") or ""))
    return out


# --- FirstTrip B2C ------------------------------------------------------------------

def _ft_effective(rate: float, base: float, cap: Any) -> float:
    """Rate after its BDT cap at this fare (no cap or no fare: the rate itself)."""
    if not cap or base <= 0:
        return rate
    return round(min(math.floor(base * rate / 100), float(cap)) / base * 100, 2)


def firsttrip_b2c(rows_by_route: Optional[dict], fees: Optional[dict]) -> list[dict[str, Any]]:
    ft = g.firsttrip
    fees = fees or {}
    out: list[dict[str, Any]] = []
    for rows in (rows_by_route or {}).values():
        for r in rows:
            route, airline = _route(r), r["airline"]
            market = _market("", r.get("origin"), r.get("destination"))
            base = float(r.get("base_fare_bdt") or 0)
            common = dict(market=market, route=route, ota="Firsttrip-B2C", airline=airline)
            if (r.get("dynamic_rate") or 0) > 0:
                out.append(_obs(**common, tier=COMMON, code=r.get("dynamic_code") or "",
                                who="Anyone (automatic)", published=r["dynamic_rate"],
                                effective=r["dynamic_rate"], fee=fees.get("common")))
            code, rate = r.get("coupon_code") or "", float(r.get("headline_rate") or 0)
            if rate > 0:
                card = ft._is_ft_card_coupon(code)
                out.append(_obs(**common, tier=SPECIAL if card else COMMON, code=code,
                                who=ft._ft_coupon_label(code) if card else "Anyone",
                                published=rate,
                                effective=_ft_effective(rate, base, r.get("coupon_cap_bdt")),
                                cap=r.get("coupon_cap_bdt"),
                                fee=fees.get("card" if card else "common")))
            slot, slot_rate = r.get("special_code") or "", float(r.get("special_rate") or 0)
            if slot_rate > 0 and slot != code:
                out.append(_obs(**common, tier=SPECIAL, code=slot,
                                who=ft._ft_coupon_label(slot) or "Card holders",
                                published=slot_rate, effective=slot_rate,
                                fee=fees.get("card")))
    return out


# --- merge + entry point ------------------------------------------------------------

def _fmt_range(lo: Optional[float], hi: Optional[float]) -> str:
    if hi is None:
        return ""
    return g._fmt(hi) if lo is None or round(lo, 2) == round(hi, 2) else f"{g._fmt(lo)}-{g._fmt(hi)}"


def merge(observations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One entry per (market, route, OTA, airline, tier, code): the effective rate as a
    lowest-highest range over the fares seen, sorted Common first, best first."""
    by_key: dict[tuple, dict[str, Any]] = {}
    for o in observations:
        key = (o["market"], o["route"], o["ota"], o["airline"], o["tier"], o["code"])
        e = by_key.get(key)
        eff = o["effective_pct"]
        if e is None:
            by_key[key] = {**o, "eff_lo": eff, "eff_hi": eff, "seen": 1}
            continue
        e["seen"] += 1
        if eff is not None:
            e["eff_lo"] = eff if e["eff_lo"] is None else min(e["eff_lo"], eff)
            e["eff_hi"] = eff if e["eff_hi"] is None else max(e["eff_hi"], eff)
        for k in ("who", "title", "cap_bdt", "fee_pct", "published_pct", "stacks"):
            if e.get(k) in (None, "") and o.get(k) not in (None, ""):
                e[k] = o[k]
    entries = []
    for e in by_key.values():
        e.pop("effective_pct", None)
        e["effective"] = _fmt_range(e["eff_lo"], e["eff_hi"])
        e["stacks"] = "" if e["stacks"] is None else ("Yes" if e["stacks"] else "No")
        entries.append(e)
    ota_order = {lab: i for i, (lab, _k) in enumerate(g.ROW_ORDER)}
    entries.sort(key=lambda e: (e["market"] != "DOM", e["route"] == "", e["route"],
                                ota_order.get(e["ota"], 99), e["airline"],
                                e["tier"] != COMMON, -(e["eff_hi"] or 0), e["code"]))
    return entries


_WIDTHS = {"market": 13, "route": 10, "ota": 15, "airline": 8, "tier": 9, "code": 18,
           "who": 22, "published_pct": 11, "effective": 12, "cap_bdt": 10, "stacks": 11,
           "fee_pct": 10, "seen": 8, "title": 60}
_NOTE = ("Every promo code each OTA offered, not just the best one shown in the grids. "
         "Common = anyone gets it; Special = needs that card, bank or membership. "
         "Effective % = what the code was worth at the fares seen (caps applied; a range "
         "when fares differed), including the automatic discount when the code goes on "
         "top of it. ShareTrip and FirstTrip % are of the base fare, GoZayaan's "
         "of the booking total. Use the filter arrows in the header row.")


def _pct(v: Any) -> Any:
    """'12.5' -> 0.125 (shown as a %); a range like '7.41-18' stays text."""
    if v in (None, ""):
        return None
    try:
        return float(v) / 100.0
    except (TypeError, ValueError):
        return f"{v}%"


def write_sheet(ws, report: dict[str, Any]) -> None:
    """The (all codes) sheet: one filterable row per code."""
    from openpyxl.styles import PatternFill
    from openpyxl.utils import get_column_letter
    st = g._detail_styles()
    tier_fill = {COMMON: PatternFill("solid", fgColor=g.HL_GREEN),
                 SPECIAL: PatternFill("solid", fgColor=g.HL_BLUE)}
    ncol = len(COLUMNS)
    t = ws.cell(1, 1, f"{report['report_date']} / {report['report_time']}hrs — all promo codes")
    t.font = st["title"]
    ws.cell(2, 1, _NOTE).font = st["note"]
    for ci, (key, head) in enumerate(COLUMNS, start=1):
        c = ws.cell(3, ci, head)
        c.font, c.fill, c.alignment, c.border = st["head"], st["hdr_fill"], st["center"], st["border"]
        ws.column_dimensions[get_column_letter(ci)].width = _WIDTHS[key]
    r = 4
    for e in report.get("all_codes") or []:
        vals = {**e, "market": "Domestic" if e["market"] == "DOM" else "International",
                "published_pct": _pct(e.get("published_pct")), "effective": _pct(e.get("effective")),
                "fee_pct": _pct(e.get("fee_pct"))}
        for ci, (key, _h) in enumerate(COLUMNS, start=1):
            v = vals.get(key)
            cell = ws.cell(r, ci, v if v != "" else None)
            cell.font = st["label"] if key == "code" else st["data"]
            cell.alignment = st["left"] if key in ("code", "who", "title", "ota") else st["center"]
            cell.border = st["border"]
            if isinstance(v, float) and key in ("published_pct", "effective", "fee_pct"):
                cell.number_format = "0.##%"
            elif key == "cap_bdt" and v:
                cell.number_format = "#,##0"
        ws.cell(r, 5).fill = tier_fill.get(e["tier"], PatternFill())
        r += 1
    ws.auto_filter.ref = f"A3:{get_column_letter(ncol)}{max(r - 1, 3)}"
    ws.freeze_panes = "A4"
    ws.print_title_rows = "3:3"
    ws.page_setup.orientation = "landscape"
    ws.page_setup.fitToWidth, ws.page_setup.fitToHeight = 1, 0
    ws.sheet_properties.pageSetUpPr.fitToPage = True


def collect(*, sharetrip_hars=None, gozayaan_hars=None, b2c_rows_by_route=None,
            b2c_fees=None) -> list[dict[str, Any]]:
    """Every code from every coupon channel. Must run inside build_report (the parse
    memo is live). One channel failing only drops that channel from the list."""
    observations: list[dict[str, Any]] = []
    steps = [("ShareTrip", lambda: sharetrip(sharetrip_hars)),
             ("GoZayaan", lambda: gozayaan(gozayaan_hars)),
             ("FirstTrip B2C", lambda: firsttrip_b2c(b2c_rows_by_route, b2c_fees))]
    for name, step in steps:
        try:
            observations += step()
        except Exception as exc:  # noqa: BLE001 — surfaced in the run log
            print(f"  ! all-codes list: {name} skipped: {exc}")
    return merge(observations)
