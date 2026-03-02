"""
Extract Gozayaan OTA fare + penalty data from HAR.

Input HAR: browser export containing
POST /api/flight/v4.0/search/legs/fares/
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.penalties import parse_gozayaan_policies


FARES_URL_TOKEN = "/api/flight/v4.0/search/legs/fares/"


def parse_args():
    p = argparse.ArgumentParser(description="Extract Gozayaan fares + penalties from HAR")
    p.add_argument("--har", required=True, help="Path to gozayaan HAR file")
    p.add_argument("--output-dir", default="output/reports", help="Output directory")
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    return p.parse_args()


def _safe_json_loads(text: str) -> Dict[str, Any] | None:
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _iter_target_entries(har: Dict[str, Any]) -> Iterable[Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]]:
    entries = (har.get("log") or {}).get("entries") or []
    for e in entries:
        if not isinstance(e, dict):
            continue
        req = e.get("request") or {}
        res = e.get("response") or {}
        url = str(req.get("url") or "")
        if FARES_URL_TOKEN not in url:
            continue
        if int(res.get("status") or 0) != 200:
            continue
        yield e, req, res


def _parse_hash_str(hash_str: str) -> Dict[str, Any]:
    """
    Example:
    BS|DAC-CXB-2026-04-13-BS-157-AT7
    """
    out: Dict[str, Any] = {}
    s = str(hash_str or "")
    parts = s.split("|")
    if parts:
        out["airline"] = parts[0].strip().upper() if parts[0] else None
    if len(parts) >= 2:
        m = re.match(r"([A-Z]{3})-([A-Z]{3})-(\d{4}-\d{2}-\d{2})", parts[1].strip().upper())
        if m:
            out["origin"] = m.group(1)
            out["destination"] = m.group(2)
            out["departure_date"] = m.group(3)
    if len(parts) >= 3:
        out["flight_number_hint"] = parts[2].strip().upper() or None
    return out


def _first_adt_rule(leg_wise_fare_rules: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(leg_wise_fare_rules, dict) or not leg_wise_fare_rules:
        return {}
    first_leg = next(iter(leg_wise_fare_rules.values()), None)
    if not isinstance(first_leg, dict):
        return {}
    adt = first_leg.get("ADT")
    return adt if isinstance(adt, dict) else {}


def _now_stamp(tz_mode: str):
    now = datetime.now(timezone.utc) if tz_mode == "utc" else datetime.now().astimezone()
    ts = now.strftime("%Y%m%d_%H%M%S")
    tz = now.strftime("%z") or "0000"
    if tz.startswith("+"):
        tz_token = f"UTCp{tz[1:]}"
    elif tz.startswith("-"):
        tz_token = f"UTCm{tz[1:]}"
    else:
        tz_token = f"UTC{tz}"
    return ts, tz_token


def _write_csv(path: Path, rows: List[Dict[str, Any]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = sorted({k for r in rows for k in r.keys()}) if rows else []
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in cols})


def main():
    args = parse_args()
    har_path = Path(args.har)
    if not har_path.exists():
        raise SystemExit(f"HAR not found: {har_path}")

    har = json.loads(har_path.read_text(encoding="utf-8"))
    fare_rows: List[Dict[str, Any]] = []
    policy_rows: List[Dict[str, Any]] = []

    for entry, req, res in _iter_target_entries(har):
        req_body = _safe_json_loads(((req.get("postData") or {}).get("text") or ""))
        res_body = _safe_json_loads(((res.get("content") or {}).get("text") or ""))
        if not isinstance(res_body, dict):
            continue
        result = res_body.get("result") or {}
        fares = result.get("fares") or []
        policies = result.get("policies") or []
        policy_fields = parse_gozayaan_policies(policies if isinstance(policies, list) else [])
        req_search_id = (req_body or {}).get("search_id")
        req_leg_type = (req_body or {}).get("leg_type")
        req_leg_hash = (req_body or {}).get("leg_hash")

        if isinstance(policies, list):
            for pol in policies:
                if not isinstance(pol, dict):
                    continue
                row = {
                    "search_id": req_search_id,
                    "leg_type": req_leg_type,
                    "leg_hash": req_leg_hash,
                }
                row.update(pol)
                policy_rows.append(row)

        if not isinstance(fares, list):
            continue
        for fare in fares:
            if not isinstance(fare, dict):
                continue
            hash_meta = _parse_hash_str(fare.get("hash_str") or "")
            adt_rule = _first_adt_rule(fare.get("leg_wise_fare_rules") or {})
            baggage_policy = adt_rule.get("baggage_policy") if isinstance(adt_rule, dict) else {}
            fare_row = {
                "search_id": req_search_id,
                "leg_type": req_leg_type,
                "leg_hash": req_leg_hash,
                "fare_id": fare.get("id"),
                "hash": fare.get("hash"),
                "hash_str": fare.get("hash_str"),
                "currency": fare.get("currency"),
                "total_tax_amount": fare.get("total_tax_amount"),
                "total_base_amount": fare.get("total_base_amount"),
                "total_fare_amount": fare.get("total_fare_amount"),
                "fare_type": fare.get("fare_type"),
                "expires_at": fare.get("expires_at"),
                "fare_basis": adt_rule.get("fare_basis"),
                "booking_code": adt_rule.get("booking_code"),
                "fare_family": adt_rule.get("fare_family"),
                "cabin_class": adt_rule.get("cabin_class"),
                "fare_changeable": adt_rule.get("changeable"),
                "fare_refundable": adt_rule.get("refundable"),
                "cancel_fee_ind": adt_rule.get("cancel_fee_ind"),
                "carry_on_quantity": (baggage_policy or {}).get("carry_on_quantity"),
                "check_in_quantity": (baggage_policy or {}).get("check_in_quantity"),
                "policy_count": len(policies) if isinstance(policies, list) else 0,
                "source_url": req.get("url"),
            }
            fare_row.update(hash_meta)
            fare_row.update(policy_fields)
            fare_rows.append(fare_row)

    ts, tz_token = _now_stamp(args.timestamp_tz)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    fares_json = out_dir / f"gozayaan_fares_{ts}_{tz_token}.json"
    fares_csv = out_dir / f"gozayaan_fares_{ts}_{tz_token}.csv"
    policy_json = out_dir / f"gozayaan_policies_{ts}_{tz_token}.json"
    policy_csv = out_dir / f"gozayaan_policies_{ts}_{tz_token}.csv"
    summary_json = out_dir / "gozayaan_extract_latest.json"

    fares_json.write_text(json.dumps(fare_rows, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    _write_csv(fares_csv, fare_rows)
    policy_json.write_text(json.dumps(policy_rows, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    _write_csv(policy_csv, policy_rows)

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "har_path": str(har_path),
        "fare_rows": len(fare_rows),
        "policy_rows": len(policy_rows),
        "fares_json": str(fares_json),
        "fares_csv": str(fares_csv),
        "policies_json": str(policy_json),
        "policies_csv": str(policy_csv),
    }
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"gozayaan_fare_rows={len(fare_rows)} -> {fares_csv}")
    print(f"gozayaan_policy_rows={len(policy_rows)} -> {policy_csv}")
    print(f"summary -> {summary_json}")


if __name__ == "__main__":
    raise SystemExit(main())

