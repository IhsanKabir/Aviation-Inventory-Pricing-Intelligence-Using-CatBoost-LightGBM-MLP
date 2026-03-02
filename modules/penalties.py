from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def _to_amount(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip().replace(",", "")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _collapse_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _extract_amount(patterns: Iterable[str], text: str) -> Optional[float]:
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if not m:
            continue
        val = _to_amount(m.group(1))
        if val is not None:
            return val
    return None


@lru_cache(maxsize=1)
def _airport_country_map() -> Dict[str, str]:
    path = Path("config/airport_countries.json")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return {str(k).upper(): str(v).upper() for k, v in data.items() if k and v}
    except Exception:
        pass
    return {}


def _is_bd_domestic(origin: Any, destination: Any) -> bool:
    amap = _airport_country_map()
    o = str(origin or "").upper().strip()
    d = str(destination or "").upper().strip()
    if not o or not d:
        return False
    return amap.get(o) == "BD" and amap.get(d) == "BD"


def _has_any_penalty_signal(row: Dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    keys = [
        "penalty_source",
        "penalty_currency",
        "penalty_rule_text",
        "fare_change_fee_before_24h",
        "fare_change_fee_within_24h",
        "fare_change_fee_no_show",
        "fare_cancel_fee_before_24h",
        "fare_cancel_fee_within_24h",
        "fare_cancel_fee_no_show",
        "fare_changeable",
        "fare_refundable",
    ]
    for k in keys:
        v = row.get(k)
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return True
    return False


def apply_penalty_inference(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Conservative fallback penalty inference for carriers where explicit penalty fields
    are often absent in source payloads.
    """
    if not isinstance(row, dict):
        return row or {}
    out = dict(row)
    if _has_any_penalty_signal(out):
        return out

    airline = str(out.get("airline") or "").strip().upper()
    brand = str(out.get("brand") or "").strip()
    fare_basis = str(out.get("fare_basis") or "").strip()

    def _set_if_empty(key: str, value: Any) -> None:
        cur = out.get(key)
        if cur is None:
            out[key] = value
            return
        if isinstance(cur, str) and not cur.strip():
            out[key] = value

    # BG domestic baseline from observed Cat-16 policy capture supplied by project.
    if airline == "BG" and _is_bd_domestic(out.get("origin"), out.get("destination")):
        _set_if_empty("penalty_source", "POLICY_BASELINE_BG_DOMESTIC")
        _set_if_empty("penalty_currency", "BDT")
        _set_if_empty("fare_change_fee_before_24h", 1000.0)
        _set_if_empty("fare_change_fee_within_24h", 1500.0)
        _set_if_empty("fare_change_fee_no_show", 1500.0)
        _set_if_empty("fare_cancel_fee_before_24h", 1500.0)
        _set_if_empty("fare_cancel_fee_within_24h", 2000.0)
        _set_if_empty("fare_cancel_fee_no_show", 2000.0)
        _set_if_empty("fare_changeable", True)
        _set_if_empty("fare_refundable", True)
        _set_if_empty(
            "penalty_rule_text",
            "BG domestic baseline policy applied (change: 1000/1500/1500, cancel: 1500/2000/2000 BDT).",
        )
        return out

    # VQ brand-level fallback when explicit penalty API fields are absent.
    if airline == "VQ":
        token = f"{brand} {fare_basis}".upper()
        if "FLEX" in token:
            _set_if_empty("penalty_source", "BRAND_PROFILE_VQ")
            _set_if_empty("fare_changeable", True)
            _set_if_empty("fare_refundable", True)
            _set_if_empty(
                "penalty_rule_text",
                f"VQ brand profile inferred from fare family '{brand or fare_basis or 'FLEX'}' (fees not published in source payload).",
            )
            return out
        if token.strip():
            _set_if_empty("penalty_source", "BRAND_PROFILE_VQ")
            _set_if_empty(
                "penalty_rule_text",
                f"VQ brand profile captured from '{brand or fare_basis}' (explicit penalty fee values unavailable in source payload).",
            )
            return out

    return out


def parse_bg_category16_penalties(rule_text: str) -> Dict[str, Any]:
    """
    Parse Biman Category-16 (PENALTIES) text into structured fields.
    """
    text_raw = str(rule_text or "")
    text_norm = _collapse_space(text_raw).upper()
    if not text_norm:
        return {}

    # Keep block boundaries robust against line breaks and punctuation.
    change_anchor = "VOLUNTARY CHANGES"
    cancel_anchor = "VOLUNTARY CANCEL/REFUND"
    change_block = text_norm
    cancel_block = text_norm
    if change_anchor in text_norm and cancel_anchor in text_norm:
        i1 = text_norm.find(change_anchor)
        i2 = text_norm.find(cancel_anchor)
        if i1 != -1 and i2 != -1 and i2 > i1:
            change_block = text_norm[i1:i2]
            cancel_block = text_norm[i2:]

    out: Dict[str, Any] = {
        "penalty_source": "BG_CATEGORY16",
        "penalty_currency": "BDT",
        "penalty_rule_text": text_raw,
    }

    out["fare_change_fee_before_24h"] = _extract_amount(
        [
            r"24\s*HRS?\s*PRIOR.*?(?:BDT\s*)?([0-9][0-9,]*)\s*PER",
            r"BEFORE\s*24\s*HRS?.*?(?:BDT\s*)?([0-9][0-9,]*)\s*PER",
        ],
        change_block,
    )
    out["fare_change_fee_within_24h"] = _extract_amount(
        [
            r"WITHIN\s*24\s*HRS?.*?(?:BDT\s*)?([0-9][0-9,]*)\s*PER",
        ],
        change_block,
    )
    out["fare_change_fee_no_show"] = _extract_amount(
        [
            r"NO\s*SHOW[^0-9A-Z]{0,60}(?:BDT\s*)?([0-9][0-9,]*)",
            r"AFTER\s*DEPARTURE[^0-9A-Z]{0,60}(?:BDT\s*)?([0-9][0-9,]*)",
        ],
        change_block,
    )

    out["fare_cancel_fee_before_24h"] = _extract_amount(
        [
            r"24\s*HRS?\s*PRIOR.*?(?:BDT\s*)?([0-9][0-9,]*)\s*PER",
            r"BEFORE\s*24\s*HRS?.*?(?:BDT\s*)?([0-9][0-9,]*)\s*PER",
        ],
        cancel_block,
    )
    out["fare_cancel_fee_within_24h"] = _extract_amount(
        [
            r"WITHIN\s*24\s*HRS?.*?(?:BDT\s*)?([0-9][0-9,]*)\s*PER",
        ],
        cancel_block,
    )
    out["fare_cancel_fee_no_show"] = _extract_amount(
        [
            r"NO\s*SHOW[^0-9A-Z]{0,60}(?:BDT\s*)?([0-9][0-9,]*)",
            r"AFTER\s*DEPARTURE[^0-9A-Z]{0,60}(?:BDT\s*)?([0-9][0-9,]*)",
        ],
        cancel_block,
    )

    return out


def parse_gozayaan_policies(policies: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Parse Gozayaan policy rows into normalized penalty fields.
    """
    if not isinstance(policies, list) or not policies:
        return {}

    out: Dict[str, Any] = {
        "penalty_source": "GOZAYAAN_POLICIES",
        "penalty_currency": None,
        "penalty_rule_text": json.dumps(policies, ensure_ascii=False, default=str),
    }

    changeable_vals: List[bool] = []
    refundable_vals: List[bool] = []

    for pol in policies:
        if not isinstance(pol, dict):
            continue
        tf = str(pol.get("time_frame") or "").lower()
        change_fee = _to_amount(pol.get("change_fee"))
        cancel_fee = _to_amount(pol.get("cancellation_fee"))
        currency = pol.get("currency")
        if currency and not out.get("penalty_currency"):
            out["penalty_currency"] = str(currency)
        if pol.get("changeable") is not None:
            changeable_vals.append(bool(pol.get("changeable")))
        if pol.get("refundable") is not None:
            refundable_vals.append(bool(pol.get("refundable")))

        if "no show" in tf:
            out["fare_change_fee_no_show"] = change_fee
            out["fare_cancel_fee_no_show"] = cancel_fee
        elif "within 24" in tf:
            out["fare_change_fee_within_24h"] = change_fee
            out["fare_cancel_fee_within_24h"] = cancel_fee
        elif "before 24" in tf or "prior 24" in tf:
            out["fare_change_fee_before_24h"] = change_fee
            out["fare_cancel_fee_before_24h"] = cancel_fee

    if changeable_vals:
        # conservative: all must agree for true
        out["fare_changeable"] = all(changeable_vals)
    if refundable_vals:
        out["fare_refundable"] = all(refundable_vals)
    return out


def extract_bg_penalties_from_graphql_response(
    payload: Dict[str, Any],
    *,
    fare_basis_filter: str | None = None,
) -> List[Dict[str, Any]]:
    """
    Extract per-fare-basis penalty rows from BG getBookingFareRules GraphQL response.
    """
    if not isinstance(payload, dict):
        return []

    original = (
        payload.get("data", {})
        .get("getBookingFareRules", {})
        .get("originalResponse")
    )
    if not isinstance(original, dict):
        original = payload.get("originalResponse")
    if not isinstance(original, dict):
        return []

    seg_rules = original.get("segmentFareRules")
    if not isinstance(seg_rules, list):
        return []

    rows: List[Dict[str, Any]] = []
    fb_filter = str(fare_basis_filter or "").strip().upper()

    for item in seg_rules:
        if not isinstance(item, dict):
            continue
        fbr = item.get("fareBasisRules") or {}
        if not isinstance(fbr, dict):
            continue
        fare_basis = str(fbr.get("fareBasis") or "").strip().upper()
        if fb_filter and fare_basis != fb_filter:
            continue
        fare_rules = fbr.get("fareRules") or []
        if not isinstance(fare_rules, list):
            continue
        cat16_texts = []
        for r in fare_rules:
            if not isinstance(r, dict):
                continue
            cat = str(r.get("category") or "").strip()
            if cat == "16":
                txt = r.get("ruleText")
                if txt:
                    cat16_texts.append(str(txt))
        if not cat16_texts:
            continue

        parsed = parse_bg_category16_penalties("\n\n".join(cat16_texts))
        parsed["fare_basis"] = fare_basis
        rows.append(parsed)
    return rows
