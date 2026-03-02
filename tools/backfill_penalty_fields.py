import argparse
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from sqlalchemy import create_engine, text

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db import DATABASE_URL as DEFAULT_DATABASE_URL
from modules.penalties import (
    apply_penalty_inference,
    extract_bg_penalties_from_graphql_response,
    parse_gozayaan_policies,
)


LOG = logging.getLogger("backfill_penalty_fields")

PENALTY_FIELDS = [
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

_PENALTY_HINT_RE = re.compile(r"(penalt|refund|cancel|change fee|no[ -]?show)", flags=re.IGNORECASE)


def _iter_dicts(node: Any) -> Iterable[Dict[str, Any]]:
    stack = [node]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            yield cur
            for v in cur.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(cur, list):
            for item in cur:
                if isinstance(item, (dict, list)):
                    stack.append(item)


def _coalesce_non_empty(*maps: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for m in maps:
        if not isinstance(m, dict):
            continue
        for k, v in m.items():
            if v is None:
                continue
            if isinstance(v, str) and not v.strip():
                continue
            if out.get(k) is None:
                out[k] = v
    return out


def _extract_bg(payload: Any, fare_basis: Optional[str]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    candidates = [payload]
    if isinstance(payload.get("originalResponse"), dict):
        candidates.append({"originalResponse": payload.get("originalResponse")})
    if isinstance(payload.get("data"), dict):
        candidates.append({"data": payload.get("data")})
    if isinstance(payload.get("segmentFareRules"), list):
        candidates.append({"originalResponse": {"segmentFareRules": payload.get("segmentFareRules")}})

    for cand in candidates:
        try:
            rows = extract_bg_penalties_from_graphql_response(cand, fare_basis_filter=fare_basis)
        except Exception:
            rows = []
        if rows:
            parsed = dict(rows[0])
            parsed.pop("fare_basis", None)
            return parsed
    return {}


def _extract_gozayaan(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}

    parsed_policy: Dict[str, Any] = {}
    for d in _iter_dicts(payload):
        policies = d.get("policies")
        if isinstance(policies, list) and policies:
            try:
                parsed_policy = parse_gozayaan_policies(policies)
            except Exception:
                parsed_policy = {}
            if parsed_policy:
                break

    leg_flags: Dict[str, Any] = {}
    for d in _iter_dicts(payload):
        leg_rules = d.get("leg_wise_fare_rules")
        if not isinstance(leg_rules, dict) or not leg_rules:
            continue
        first_leg = next(iter(leg_rules.values()), None)
        if not isinstance(first_leg, dict):
            continue
        adt = first_leg.get("ADT")
        if not isinstance(adt, dict):
            continue
        if adt.get("changeable") is not None:
            leg_flags["fare_changeable"] = bool(adt.get("changeable"))
        if adt.get("refundable") is not None:
            leg_flags["fare_refundable"] = bool(adt.get("refundable"))
        ccy = adt.get("currency")
        if ccy:
            leg_flags["penalty_currency"] = str(ccy)
        break

    return _coalesce_non_empty(parsed_policy, leg_flags)


def _extract_amybd(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    trip = payload.get("trip")
    if not isinstance(trip, dict):
        return {}
    out: Dict[str, Any] = {}
    refund_text = str(trip.get("fRefund") or "").strip()
    if refund_text:
        out["penalty_source"] = "AMYBD_REFUND_FLAG"
        out["penalty_rule_text"] = refund_text[:600]
        if "REFUND" in refund_text.upper():
            out["fare_refundable"] = True
    if not out:
        serialized = json.dumps(trip, ensure_ascii=False, default=str)
        if _PENALTY_HINT_RE.search(serialized):
            out["penalty_source"] = "AMYBD_TRIP_HINT"
            out["penalty_rule_text"] = serialized[:600]
    return out


def _extract_penalty_fields(payload: Any, airline: str, fare_basis: Optional[str]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    airline_code = str(airline or "").strip().upper()
    parsed = _coalesce_non_empty(
        _extract_bg(payload, fare_basis if airline_code == "BG" else None),
        _extract_gozayaan(payload),
        _extract_amybd(payload),
    )
    return {k: parsed.get(k) for k in PENALTY_FIELDS if parsed.get(k) is not None}


def parse_args():
    p = argparse.ArgumentParser(description="Backfill penalty fields in flight_offer_raw_meta from stored raw payloads")
    p.add_argument("--db-url", default=DEFAULT_DATABASE_URL)
    p.add_argument("--batch-size", type=int, default=1000)
    p.add_argument("--max-rows", type=int, default=0, help="0 = no explicit cap")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    engine = create_engine(args.db_url, pool_pre_ping=True, future=True)
    updated = 0
    scanned = 0
    skipped_no_payload = 0
    skipped_no_parse = 0
    max_rows = max(0, int(args.max_rows or 0))
    payload_cache: Dict[str, Any] = {}

    select_sql = text(
        """
        SELECT
            rm.id,
            rm.raw_offer,
            rm.raw_offer_fingerprint,
            fo.airline,
            fo.fare_basis,
            fo.origin,
            fo.destination,
            fo.brand
        FROM flight_offer_raw_meta rm
        JOIN flight_offers fo
          ON fo.id = rm.flight_offer_id
        WHERE COALESCE(rm.penalty_source, '') = ''
          AND rm.penalty_rule_text IS NULL
          AND rm.fare_change_fee_before_24h IS NULL
          AND rm.fare_change_fee_within_24h IS NULL
          AND rm.fare_change_fee_no_show IS NULL
          AND rm.fare_cancel_fee_before_24h IS NULL
          AND rm.fare_cancel_fee_within_24h IS NULL
          AND rm.fare_cancel_fee_no_show IS NULL
          AND rm.fare_changeable IS NULL
          AND rm.fare_refundable IS NULL
        ORDER BY rm.id
        LIMIT :lim
        """
    )
    payload_sql = text(
        """
        SELECT payload_json
        FROM raw_offer_payload_store
        WHERE fingerprint = :fp
        """
    )
    update_sql = text(
        """
        UPDATE flight_offer_raw_meta
        SET
            penalty_source = COALESCE(:penalty_source, penalty_source),
            penalty_currency = COALESCE(:penalty_currency, penalty_currency),
            penalty_rule_text = COALESCE(:penalty_rule_text, penalty_rule_text),
            fare_change_fee_before_24h = COALESCE(:fare_change_fee_before_24h, fare_change_fee_before_24h),
            fare_change_fee_within_24h = COALESCE(:fare_change_fee_within_24h, fare_change_fee_within_24h),
            fare_change_fee_no_show = COALESCE(:fare_change_fee_no_show, fare_change_fee_no_show),
            fare_cancel_fee_before_24h = COALESCE(:fare_cancel_fee_before_24h, fare_cancel_fee_before_24h),
            fare_cancel_fee_within_24h = COALESCE(:fare_cancel_fee_within_24h, fare_cancel_fee_within_24h),
            fare_cancel_fee_no_show = COALESCE(:fare_cancel_fee_no_show, fare_cancel_fee_no_show),
            fare_changeable = COALESCE(:fare_changeable, fare_changeable),
            fare_refundable = COALESCE(:fare_refundable, fare_refundable)
        WHERE id = :id
        """
    )

    with engine.begin() as conn:
        while True:
            if max_rows and scanned >= max_rows:
                break
            remaining = max_rows - scanned if max_rows else args.batch_size
            lim = min(args.batch_size, remaining) if max_rows else args.batch_size
            if lim <= 0:
                break

            batch = conn.execute(select_sql, {"lim": int(lim)}).mappings().all()
            if not batch:
                break

            for row in batch:
                scanned += 1
                payload = row.get("raw_offer")
                if payload is None:
                    fp = str(row.get("raw_offer_fingerprint") or "").strip()
                    if fp:
                        if fp in payload_cache:
                            payload = payload_cache.get(fp)
                        else:
                            payload = conn.execute(payload_sql, {"fp": fp}).scalar_one_or_none()
                            payload_cache[fp] = payload
                if payload is None:
                    skipped_no_payload += 1
                    continue

                fields = _extract_penalty_fields(payload, row.get("airline"), row.get("fare_basis"))
                fields = apply_penalty_inference(
                    {
                        "airline": row.get("airline"),
                        "origin": row.get("origin"),
                        "destination": row.get("destination"),
                        "brand": row.get("brand"),
                        "fare_basis": row.get("fare_basis"),
                        **fields,
                    }
                )
                fields = {k: fields.get(k) for k in PENALTY_FIELDS if fields.get(k) is not None}
                if not fields:
                    skipped_no_parse += 1
                    continue

                params = {"id": int(row["id"])}
                for key in PENALTY_FIELDS:
                    params[key] = fields.get(key)

                if not args.dry_run:
                    conn.execute(update_sql, params)
                updated += 1

            LOG.info(
                "progress scanned=%d updated=%d skipped_no_payload=%d skipped_no_parse=%d",
                scanned,
                updated,
                skipped_no_payload,
                skipped_no_parse,
            )

    print(
        json.dumps(
            {
                "scanned": scanned,
                "updated": updated,
                "skipped_no_payload": skipped_no_payload,
                "skipped_no_parse": skipped_no_parse,
                "dry_run": bool(args.dry_run),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
