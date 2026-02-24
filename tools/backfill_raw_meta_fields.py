"""
Backfill historical flight_offer_raw_meta fields introduced after initial ingestion.

This script is idempotent and safe to run multiple times.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, Tuple

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import engine, init_db


DEFAULT_AIRPORT_UTC_OFFSET_MINUTES = {
    "DAC": 360,
    "CGP": 360,
    "CXB": 360,
    "JSR": 360,
    "RJH": 360,
    "SPD": 360,
    "ZYL": 360,
    "BZL": 360,
    "DOH": 180,
    "JED": 180,
}


def load_airport_offsets(path: Path) -> Dict[str, int]:
    offsets = dict(DEFAULT_AIRPORT_UTC_OFFSET_MINUTES)
    if not path.exists():
        return offsets
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return offsets
    for k, v in payload.items():
        try:
            offsets[str(k).upper()] = int(v)
        except Exception:
            continue
    return offsets


def to_offset_text(minutes: int) -> str:
    sign = "+" if minutes >= 0 else "-"
    m = abs(int(minutes))
    hh = m // 60
    mm = m % 60
    return f"{sign}{hh:02d}:{mm:02d}"


def build_case_expr(
    column_expr: str,
    value_map: Dict[str, str],
    else_sql: str = "NULL",
) -> str:
    parts = [f"CASE UPPER({column_expr})"]
    for code, value in value_map.items():
        parts.append(f" WHEN '{code}' THEN {value}")
    parts.append(f" ELSE {else_sql} END")
    return "".join(parts)


def fetch_null_stats() -> Dict[str, int]:
    sql = text(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN adt_count IS NULL THEN 1 ELSE 0 END) AS null_adt,
            SUM(CASE WHEN chd_count IS NULL THEN 1 ELSE 0 END) AS null_chd,
            SUM(CASE WHEN inf_count IS NULL THEN 1 ELSE 0 END) AS null_inf,
            SUM(CASE WHEN inventory_confidence IS NULL THEN 1 ELSE 0 END) AS null_inventory_confidence,
            SUM(CASE WHEN source_endpoint IS NULL THEN 1 ELSE 0 END) AS null_source_endpoint,
            SUM(CASE WHEN departure_utc IS NULL THEN 1 ELSE 0 END) AS null_departure_utc,
            SUM(CASE WHEN arrival_utc IS NULL THEN 1 ELSE 0 END) AS null_arrival_utc
        FROM flight_offer_raw_meta
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql).mappings().first() or {}
    return {k: int(v or 0) for k, v in row.items()}


def run_backfill(offsets: Dict[str, int], dry_run: bool = False) -> Dict[str, int]:
    origin_minutes_case = build_case_expr(
        "fo.origin",
        {k: str(v) for k, v in offsets.items()},
        else_sql="NULL",
    )
    dest_minutes_case = build_case_expr(
        "fo.destination",
        {k: str(v) for k, v in offsets.items()},
        else_sql="NULL",
    )
    origin_offset_case = build_case_expr(
        "fo.origin",
        {k: f"'{to_offset_text(v)}'" for k, v in offsets.items()},
        else_sql="NULL",
    )
    dest_offset_case = build_case_expr(
        "fo.destination",
        {k: f"'{to_offset_text(v)}'" for k, v in offsets.items()},
        else_sql="NULL",
    )

    statements: Tuple[Tuple[str, str], ...] = (
        (
            "insert_missing_raw_meta",
            f"""
            INSERT INTO flight_offer_raw_meta (
                flight_offer_id,
                currency,
                fare_amount,
                tax_amount,
                baggage,
                aircraft,
                equipment_code,
                duration_min,
                stops,
                arrival,
                estimated_load_factor_pct,
                inventory_confidence,
                booking_class,
                soldout,
                adt_count,
                chd_count,
                inf_count,
                departure_local,
                departure_utc,
                departure_tz_offset,
                arrival_utc,
                arrival_tz_offset,
                fare_ref_num,
                fare_search_reference,
                source_endpoint,
                raw_offer,
                scraped_at
            )
            SELECT
                fo.id,
                'BDT',
                fo.price_total_bdt,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                CASE WHEN fo.seat_available IS NULL THEN 'unknown' ELSE 'reported' END,
                NULL,
                CASE WHEN fo.seat_available = 0 THEN TRUE ELSE FALSE END,
                1,
                0,
                0,
                fo.departure,
                CASE
                    WHEN ({origin_minutes_case}) IS NOT NULL
                        THEN fo.departure - make_interval(mins => ({origin_minutes_case}))
                    ELSE NULL
                END,
                {origin_offset_case},
                NULL,
                {dest_offset_case},
                NULL,
                NULL,
                CASE
                    WHEN fo.airline = 'BG' THEN 'api/graphql:bookingAirSearch'
                    WHEN fo.airline = 'VQ' THEN 'flight_selection.aspx?ajax=true&action=flightSearch'
                    ELSE 'legacy/unknown'
                END,
                NULL,
                fo.scraped_at
            FROM flight_offers fo
            LEFT JOIN flight_offer_raw_meta rm
              ON rm.flight_offer_id = fo.id
            WHERE rm.id IS NULL
            """,
        ),
        (
            "inventory_confidence",
            """
            UPDATE flight_offer_raw_meta rm
            SET inventory_confidence = CASE
                WHEN fo.seat_available IS NULL THEN 'unknown'
                ELSE 'reported'
            END
            FROM flight_offers fo
            WHERE rm.flight_offer_id = fo.id
              AND rm.inventory_confidence IS NULL
            """,
        ),
        (
            "source_endpoint",
            """
            UPDATE flight_offer_raw_meta rm
            SET source_endpoint = CASE
                WHEN fo.airline = 'BG' THEN 'api/graphql:bookingAirSearch'
                WHEN fo.airline = 'VQ' AND COALESCE(
                    COALESCE(
                        rm.raw_offer,
                        (SELECT ps.payload_json FROM raw_offer_payload_store ps WHERE ps.fingerprint = rm.raw_offer_fingerprint)
                    )->>'source',
                    ''
                ) = 'passenger_info'
                    THEN 'passenger_info.aspx?get=DATA'
                WHEN fo.airline = 'VQ' THEN 'flight_selection.aspx?ajax=true&action=flightSearch'
                ELSE 'legacy/unknown'
            END
            FROM flight_offers fo
            WHERE rm.flight_offer_id = fo.id
              AND rm.source_endpoint IS NULL
            """,
        ),
        (
            "pax_defaults",
            """
            UPDATE flight_offer_raw_meta
            SET adt_count = COALESCE(adt_count, 1),
                chd_count = COALESCE(chd_count, 0),
                inf_count = COALESCE(inf_count, 0)
            WHERE adt_count IS NULL OR chd_count IS NULL OR inf_count IS NULL
            """,
        ),
        (
            "departure_timestamps",
            f"""
            UPDATE flight_offer_raw_meta rm
            SET departure_local = COALESCE(rm.departure_local, fo.departure),
                departure_tz_offset = COALESCE(rm.departure_tz_offset, {origin_offset_case}),
                departure_utc = COALESCE(
                    rm.departure_utc,
                    CASE
                        WHEN ({origin_minutes_case}) IS NOT NULL
                            THEN fo.departure - make_interval(mins => ({origin_minutes_case}))
                        ELSE NULL
                    END
                )
            FROM flight_offers fo
            WHERE rm.flight_offer_id = fo.id
              AND (
                    rm.departure_local IS NULL
                    OR (rm.departure_tz_offset IS NULL AND ({origin_offset_case}) IS NOT NULL)
                    OR (rm.departure_utc IS NULL AND ({origin_minutes_case}) IS NOT NULL)
                  )
            """,
        ),
        (
            "arrival_timestamps",
            f"""
            UPDATE flight_offer_raw_meta rm
            SET arrival_tz_offset = COALESCE(rm.arrival_tz_offset, {dest_offset_case}),
                arrival_utc = COALESCE(
                    rm.arrival_utc,
                    CASE
                        WHEN rm.arrival IS NOT NULL AND ({dest_minutes_case}) IS NOT NULL
                            THEN rm.arrival - make_interval(mins => ({dest_minutes_case}))
                        ELSE NULL
                    END
                )
            FROM flight_offers fo
            WHERE rm.flight_offer_id = fo.id
              AND (
                    (rm.arrival_tz_offset IS NULL AND ({dest_offset_case}) IS NOT NULL)
                    OR (rm.arrival_utc IS NULL AND rm.arrival IS NOT NULL AND ({dest_minutes_case}) IS NOT NULL)
                  )
            """,
        ),
        (
            "fare_references",
            """
            UPDATE flight_offer_raw_meta rm
            SET fare_ref_num = COALESCE(
                    rm.fare_ref_num,
                    COALESCE(
                        rm.raw_offer,
                        (SELECT ps.payload_json FROM raw_offer_payload_store ps WHERE ps.fingerprint = rm.raw_offer_fingerprint)
                    )->>'fare_ref_num'
                ),
                fare_search_reference = COALESCE(
                    rm.fare_search_reference,
                    COALESCE(
                        rm.raw_offer,
                        (SELECT ps.payload_json FROM raw_offer_payload_store ps WHERE ps.fingerprint = rm.raw_offer_fingerprint)
                    )->>'fare_search_reference'
                )
            WHERE COALESCE(
                    rm.raw_offer,
                    (SELECT ps.payload_json FROM raw_offer_payload_store ps WHERE ps.fingerprint = rm.raw_offer_fingerprint)
                  ) IS NOT NULL
              AND (
                    (rm.fare_ref_num IS NULL AND (
                        COALESCE(
                            rm.raw_offer,
                            (SELECT ps.payload_json FROM raw_offer_payload_store ps WHERE ps.fingerprint = rm.raw_offer_fingerprint)
                        )->>'fare_ref_num'
                    ) IS NOT NULL)
                    OR
                    (rm.fare_search_reference IS NULL AND (
                        COALESCE(
                            rm.raw_offer,
                            (SELECT ps.payload_json FROM raw_offer_payload_store ps WHERE ps.fingerprint = rm.raw_offer_fingerprint)
                        )->>'fare_search_reference'
                    ) IS NOT NULL)
                  )
            """,
        ),
    )

    touched = {}
    if dry_run:
        for name, _ in statements:
            touched[name] = -1
        return touched

    with engine.begin() as conn:
        for name, sql in statements:
            result = conn.execute(text(sql))
            touched[name] = int(result.rowcount or 0)
    return touched


def parse_args():
    p = argparse.ArgumentParser(description="Backfill historical raw-meta fields")
    p.add_argument(
        "--airport-timezones",
        default="config/airport_timezones.json",
        help="Path to airport timezone offsets JSON",
    )
    p.add_argument("--dry-run", action="store_true", help="Only print before/after stats without writing")
    return p.parse_args()


def main():
    args = parse_args()
    init_db(create_tables=True)
    offsets = load_airport_offsets(Path(args.airport_timezones))

    before = fetch_null_stats()
    touched = run_backfill(offsets=offsets, dry_run=args.dry_run)
    after = fetch_null_stats()

    print("before:", before)
    print("touched:", touched)
    print("after:", after)


if __name__ == "__main__":
    main()
