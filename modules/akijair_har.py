"""
AKIJ Air (akijair.com) offer source — via MANUAL HAR import.

akijair.com is a Next.js app: the flight search posts to /flight/search and the
response is a React Server Components stream (text/x-component). The useful
payload is the line `N:{"data":[...]}` which is plain JSON. Each item carries:

  * validatingCarrier                              -> airline code
  * fareOptions[].fareSummary{totalBaseFareAmount, totalTaxAmount, totalFareAmount}
  * fareOptions[].grossTotalFare                   -> pre-markdown gross
  * fareOptions[].fareSummary.breakdown.ADT.metadata.commission:
        {adt_in_commission, adt_out_commission, adt_incentive}
  * metaData.isBDDomestic                          -> DOM vs INTL

in_commission is a flat AKIJ rate; out_commission is the airline-specific rate
(and equals the gross->total markdown where one is applied). Auth is Google
OAuth, so this is HAR-only.

Usage:
  python tools/import_akij_har.py <file.har>
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

AIRLINE_ALIAS = {"3L": "G9"}


def _alias(code: Any) -> str:
    return AIRLINE_ALIAS.get(str(code or "").upper(), str(code or "").upper())


def _rsc_data(text: str) -> Optional[list]:
    """Pull the `N:{"data":[...]}` JSON object out of an RSC stream."""
    for line in text.split("\n"):
        match = re.match(r"^[0-9a-f]+:(.*)$", line)
        if not match or '"data"' not in match.group(1):
            continue
        try:
            payload = json.loads(match.group(1))
        except (ValueError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
            return payload["data"]
    return None


def _find_commission(node: Any) -> Dict[str, Any]:
    """Recursively locate the commission dict ({adt_in_commission, ...}) in an item."""
    if isinstance(node, dict):
        commission = node.get("commission")
        if isinstance(commission, dict) and "adt_in_commission" in commission:
            return commission
        for value in node.values():
            found = _find_commission(value)
            if found:
                return found
    elif isinstance(node, list):
        for value in node:
            found = _find_commission(value)
            if found:
                return found
    return {}


def _commission_and_fare(item: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Return (commission dict, first fareOption). Commission path varies, so it
    is located recursively; the fare totals come from fareOptions[0]."""
    fare_options = item.get("fareOptions") or []
    return _find_commission(item), (fare_options[0] if fare_options else {})


def _route(item: Dict[str, Any]) -> tuple[str, str]:
    combos = item.get("flightCombination") or []
    if not combos:
        return "", ""
    details = combos[0].get("flightDetails") or []
    if not details:
        return "", ""
    first = details[0].get("flightInformation") or {}
    last = details[-1].get("flightInformation") or {}
    return (str(first.get("departureAirport") or "").upper(),
            str(last.get("arrivalAirport") or "").upper())


def _commission_row(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    airline = _alias(item.get("validatingCarrier"))
    if not airline:
        return None
    commission, fare = _commission_and_fare(item)
    summary = fare.get("fareSummary") or {}
    base = float(summary.get("totalBaseFareAmount") or 0)
    if base <= 0:
        return None
    total = float(summary.get("totalFareAmount") or 0)
    gross = float(fare.get("grossTotalFare") or total)
    origin, destination = _route(item)
    in_pct = float(commission.get("adt_in_commission") or 0)
    out_pct = float(commission.get("adt_out_commission") or 0)
    incentive = float(commission.get("adt_incentive") or 0)
    # Realized discount = how much below the shown/gross price the fare actually
    # sells, as a percent of base fare: (gross - sold) / base.
    realized_discount = round((gross - total) / base * 100, 2) if base else 0.0
    return {
        "channel": "akijair",
        "persona": "B2B",
        "airline": airline,
        "origin": origin,
        "destination": destination,
        "domestic": bool((item.get("metaData") or {}).get("isBDDomestic")),
        "base_fare_bdt": round(base),
        "total_fare_bdt": round(total),
        "gross_fare_bdt": round(gross),
        "realized_discount_pct": realized_discount,
        "in_commission": in_pct,
        "out_commission": out_pct,
        "incentive": incentive,
        "total_commission": round(in_pct + out_pct + incentive, 2),
    }


def parse_commissions(path: str | Path) -> List[Dict[str, Any]]:
    """Per-offer agent commission from an AKIJ Air search HAR."""
    har = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    rows: List[Dict[str, Any]] = []
    seen: set = set()
    for entry in har.get("log", {}).get("entries", []):
        request = entry.get("request", {})
        if "/flight/search" not in request.get("url", "") or request.get("method") != "POST":
            continue
        text = (entry.get("response", {}).get("content", {}) or {}).get("text", "") or ""
        data = _rsc_data(text)
        if not data:
            continue
        for item in data:
            row = _commission_row(item)
            if not row:
                continue
            sig = (row["airline"], row["origin"], row["destination"],
                   row["base_fare_bdt"], row["out_commission"])
            if sig in seen:
                continue
            seen.add(sig)
            rows.append(row)
    return rows


def summarize_commissions(rows: List[Dict[str, Any]],
                          field: str = "out_commission") -> Dict[tuple[str, str], Dict[str, Any]]:
    """
    One cell per (route_type, airline). `field` chooses which commission number
    drives the cell: in_commission | out_commission | total_commission.
    """
    by_cell: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for r in rows:
        rt = "DOM" if r["domestic"] else "INTL"
        by_cell.setdefault((rt, r["airline"]), []).append(r)
    out: Dict[tuple[str, str], Dict[str, Any]] = {}
    for key, items in by_cell.items():
        best = max(items, key=lambda r: r.get(field, 0))
        out[key] = {
            "value": best.get(field, 0),
            "in_commission": best["in_commission"],
            "out_commission": best["out_commission"],
            "incentive": best["incentive"],
            "total_commission": best["total_commission"],
        }
    return out
