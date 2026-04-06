from sqlalchemy import text
import pandas as pd


def _group_apply(grouped, func):
    """
    Compatibility helper for pandas groupby.apply deprecation path.
    """
    try:
        return grouped.apply(func, include_groups=False)
    except TypeError:
        return grouped.apply(func)


def _reset_index_named(applied, value_name: str) -> pd.DataFrame:
    """
    Pandas compatibility helper:
    - Series -> reset_index(name=value_name)
    - DataFrame -> reset_index() + best-effort rename of value column
    """
    if isinstance(applied, pd.Series):
        return applied.reset_index(name=value_name)

    out = applied.reset_index()
    if value_name in out.columns:
        return out
    if 0 in out.columns:
        return out.rename(columns={0: value_name})

    # Fallback: rename the last column when we cannot infer better.
    if len(out.columns) > 0:
        out = out.rename(columns={out.columns[-1]: value_name})
    return out


def finalize_comparison_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # =========================================================
    # 1. VALIDATION
    # =========================================================
    required = ["flight_key", "flight_date"]
    for c in required:
        if c not in df.columns:
            raise RuntimeError(f"{c} missing before finalize_comparison_df")

    # =========================================================
    # 2. SORT + BACKFILL (RBD continuity)
    # =========================================================
    df = df.sort_values(["flight_key", "flight_date", "current_fare_bdt"])

    backfill_cols = [
        "current_fare_bdt",
        "current_tax",
        "current_seats",
        "seat_capacity",
        "rbd",
    ]

    for col in backfill_cols:
        if col in df.columns:
            df[col] = df.groupby("flight_key")[col].ffill()

    # =========================================================
    # 3. CURRENT MIN/MAX FARE (FLIGHT LEVEL)
    # =========================================================
    def compute_current_fare(g):
        g_valid = g.copy()
        g_valid["current_fare_bdt"] = pd.to_numeric(g_valid["current_fare_bdt"], errors="coerce")
        g_valid = g_valid[g_valid["current_fare_bdt"].notna()]

        if g_valid.empty:
            return pd.Series(
                {
                    "min_fare": None,
                    "max_fare": None,
                    "min_rbd": None,
                    "min_rbd_seats": None,
                    "max_rbd": None,
                    "max_rbd_seats": None,
                }
            )

        # Keep rows even when RBD is missing to avoid empty aggregation groups.
        g_valid = g_valid.assign(rbd=g_valid["rbd"].fillna("__UNK__"))
        rbd_level = g_valid.groupby("rbd", as_index=False, dropna=False).agg(
            fare=("current_fare_bdt", "min"),
            seats=("current_seats", "max"),
        )
        rbd_level = rbd_level[rbd_level["fare"].notna()]
        if rbd_level.empty:
            return pd.Series(
                {
                    "min_fare": None,
                    "max_fare": None,
                    "min_rbd": None,
                    "min_rbd_seats": None,
                    "max_rbd": None,
                    "max_rbd_seats": None,
                }
            )

        min_row = rbd_level.loc[rbd_level["fare"].idxmin()]
        max_row = rbd_level.loc[rbd_level["fare"].idxmax()]
        min_rbd = None if min_row["rbd"] == "__UNK__" else min_row["rbd"]
        max_rbd = None if max_row["rbd"] == "__UNK__" else max_row["rbd"]

        return pd.Series(
            {
                "min_fare": rbd_level["fare"].min(),
                "max_fare": rbd_level["fare"].max(),
                "min_rbd": min_rbd,
                "min_rbd_seats": min_row["seats"],
                "max_rbd": max_rbd,
                "max_rbd_seats": max_row["seats"],
            }
        )

    current_info = _group_apply(
        df.groupby(["flight_key", "flight_date"]),
        compute_current_fare,
    ).reset_index()

    df = df.merge(current_info, on=["flight_key", "flight_date"], how="left")

    # =========================================================
    # 4. PREVIOUS MIN/MAX FARE (FLIGHT LEVEL)
    # =========================================================
    def compute_previous_fare(g):
        g_valid = g[g["previous_fare_bdt"].notna()]

        if g_valid.empty:
            return pd.Series({"previous_min_fare": None, "previous_max_fare": None})

        rbd_level = g_valid.groupby("rbd", as_index=False).agg(
            prev_fare=("previous_fare_bdt", "min")
        )

        return pd.Series(
            {
                "previous_min_fare": rbd_level["prev_fare"].min(),
                "previous_max_fare": rbd_level["prev_fare"].max(),
            }
        )

    previous_info = _group_apply(
        df.groupby(["flight_key", "flight_date"]),
        compute_previous_fare,
    ).reset_index()

    df = df.merge(previous_info, on=["flight_key", "flight_date"], how="left")

    # =========================================================
    # 5. CURRENT SEAT TOTAL (FLIGHT LEVEL)
    # =========================================================
    seat_totals = (
        df.groupby(["flight_key", "flight_date", "rbd"], as_index=False)
        .agg(seats=("current_seats", "max"))
        .groupby(["flight_key", "flight_date"], as_index=False)
        .agg(min_seats=("seats", lambda s: s.dropna().sum() if s.notna().any() else None))
    )

    df = df.merge(seat_totals, on=["flight_key", "flight_date"], how="left")

    # =========================================================
    # 6. PREVIOUS SEAT TOTAL
    # =========================================================
    prev_seats = (
        df.groupby(["flight_key", "flight_date", "rbd"], as_index=False)
        .agg(seats=("previous_seats", "max"))
        .groupby(["flight_key", "flight_date"], as_index=False)
        .agg(previous_min_seats=("seats", lambda s: s.dropna().sum() if s.notna().any() else None))
    )

    df = df.merge(prev_seats, on=["flight_key", "flight_date"], how="left")

    # =========================================================
    # 7. NEW FLIGHT FLAG
    # =========================================================
    new_flag = (
        df.groupby(["flight_key", "flight_date"], as_index=False)
        .agg(is_new_flight=("previous_fare_bdt", lambda s: s.isna().all()))
    )

    df = df.merge(new_flag, on=["flight_key", "flight_date"], how="left")

    # =========================================================
    # 8. COLLAPSE TO ONE ROW PER FLIGHT/DAY
    # =========================================================
    df = (
        df.sort_values(["flight_key", "flight_date"])
        .drop_duplicates(["flight_key", "flight_date"], keep="last")
        .copy()
    )

    df["max_seats"] = df["seat_capacity"]

    # =========================================================
    # 9. STATUS LOGIC
    # =========================================================
    df["status"] = "NORMAL"

    df.loc[df["min_seats"].notna() & (df["min_seats"] == 0), "status"] = "SOLD OUT"
    df.loc[(df["is_new_flight"]) & (df["min_seats"] > 0), "status"] = "NEW"

    # =========================================================
    # 10. INVENTORY PRESSURE PROXY (compat aliases kept as load_*)
    # =========================================================
    # min_seats is treated as "total opened seats" (aggregated visible inventory),
    # and seat_capacity is carrier capacity. "Load Factor" was misleading here.
    # We compute a bounded inventory pressure proxy:
    #   pressure = 100 - min(100, open_seats/capacity*100)
    # This preserves a stable 0..100 scarcity-style signal while keeping the raw
    # Open/Cap counts available in the report.
    open_ratio = df["min_seats"] / df["seat_capacity"]
    open_ratio_prev = df["previous_min_seats"] / df["seat_capacity"]

    df["inventory_pressure_pct"] = (1 - open_ratio.clip(lower=0, upper=1)) * 100
    df.loc[df["seat_capacity"].isna() | df["min_seats"].isna() | (df["seat_capacity"] == 0), "inventory_pressure_pct"] = None

    df["previous_inventory_pressure_pct"] = (1 - open_ratio_prev.clip(lower=0, upper=1)) * 100
    df.loc[
        df["seat_capacity"].isna() | df["previous_min_seats"].isna() | (df["seat_capacity"] == 0),
        "previous_inventory_pressure_pct",
    ] = None

    # Backward-compatible aliases used by existing report/export code.
    df["load_pct"] = df["inventory_pressure_pct"]
    df["previous_load_pct"] = df["previous_inventory_pressure_pct"]

    # =========================================================
    # 11. DELTAS (ALL FLIGHT LEVEL)
    # =========================================================
    df["min_fare_delta"] = df["min_fare"] - df["previous_min_fare"]
    df["max_fare_delta"] = df["max_fare"] - df["previous_max_fare"]

    df["seat_delta"] = df["min_seats"] - df["previous_min_seats"]
    df["tax_delta"] = df["current_tax"] - df["previous_tax"]

    df["inventory_pressure_delta"] = df["inventory_pressure_pct"] - df["previous_inventory_pressure_pct"]
    df["load_delta"] = df["inventory_pressure_delta"]

    penalty_fee_bases = [
        "fare_change_fee_before_24h",
        "fare_change_fee_within_24h",
        "fare_change_fee_no_show",
        "fare_cancel_fee_before_24h",
        "fare_cancel_fee_within_24h",
        "fare_cancel_fee_no_show",
    ]
    for base in penalty_fee_bases:
        c_col = f"current_{base}"
        p_col = f"previous_{base}"
        d_col = f"{base}_delta"
        if c_col not in df.columns or p_col not in df.columns:
            continue
        c_val = pd.to_numeric(df[c_col], errors="coerce")
        p_val = pd.to_numeric(df[p_col], errors="coerce")
        df[d_col] = c_val - p_val

    # =========================================================
    # 12. ROUTE LEADER
    # =========================================================
    df["leader"] = df["min_fare"] == df.groupby(["route", "flight_date"])["min_fare"].transform("min")

    # =========================================================
    # 13. VISIBILITY
    # =========================================================
    visibility = (
        df.groupby("flight_key")
        .agg(
            has_fare=("min_fare", lambda s: s.notna().any()),
            has_tax=("current_tax", lambda s: s.notna().any()),
        )
        .reset_index()
    )
    visibility["row_visible"] = visibility["has_fare"] | visibility["has_tax"]
    visibility = visibility[["flight_key", "row_visible"]]

    df = df.merge(visibility, on="flight_key", how="left")
    df["row_visible"] = df["row_visible"].fillna(False)
    df = df[df["row_visible"]].copy()

    return df


class ComparisonEngine:
    def __init__(self, engine):
        self.engine = engine

    def compare_scrapes(
        self,
        current_scrape,
        previous_scrape,
        trip_type: str | None = None,
        return_date: str | None = None,
        return_date_start: str | None = None,
        return_date_end: str | None = None,
    ) -> pd.DataFrame:
        normalized_trip_type = str(trip_type or "").strip().upper()
        if normalized_trip_type not in {"OW", "RT"}:
            normalized_trip_type = None
        return_date_sql = ""
        params = {
            "current": current_scrape,
            "previous": previous_scrape,
        }
        if normalized_trip_type:
            params["trip_type"] = normalized_trip_type
        if return_date:
            params["return_date"] = return_date
            return_date_sql += " AND frm.requested_return_date::text = :return_date"
        if return_date_start:
            params["return_date_start"] = return_date_start
            return_date_sql += " AND frm.requested_return_date::text >= :return_date_start"
        if return_date_end:
            params["return_date_end"] = return_date_end
            return_date_sql += " AND frm.requested_return_date::text <= :return_date_end"
        sql = text(
            f"""
        WITH current_raw AS (
            SELECT
                fo.*,
                COALESCE(frm.search_trip_type, 'OW') AS search_trip_type,
                frm.requested_return_date::text AS requested_return_date
            FROM flight_offers fo
            LEFT JOIN LATERAL (
                SELECT r.search_trip_type, r.requested_return_date
                FROM flight_offer_raw_meta r
                WHERE r.flight_offer_id = fo.id
                ORDER BY r.id DESC
                LIMIT 1
            ) frm ON TRUE
            WHERE scrape_id = :current
              {("AND COALESCE(frm.search_trip_type, 'OW') = :trip_type" if normalized_trip_type else "")}
              {return_date_sql}
        ),
        previous_raw AS (
            SELECT
                fo.*,
                COALESCE(frm.search_trip_type, 'OW') AS search_trip_type,
                frm.requested_return_date::text AS requested_return_date
            FROM flight_offers fo
            LEFT JOIN LATERAL (
                SELECT r.search_trip_type, r.requested_return_date
                FROM flight_offer_raw_meta r
                WHERE r.flight_offer_id = fo.id
                ORDER BY r.id DESC
                LIMIT 1
            ) frm ON TRUE
            WHERE scrape_id = :previous
              {("AND COALESCE(frm.search_trip_type, 'OW') = :trip_type" if normalized_trip_type else "")}
              {return_date_sql}
        ),

        -- Canonical flight per scrape (collapse fare_basis / brand noise)
        current AS (
            SELECT
                MIN(id)               AS id,
                scrape_id,
                airline,
                origin,
                destination,
                flight_number,
                departure,
                cabin,
                brand,
                search_trip_type,
                requested_return_date,
                MIN(price_total_bdt)  AS price_total_bdt,
                MAX(seat_capacity)    AS seat_capacity,
                MAX(seat_available)   AS seat_available
            FROM current_raw
            GROUP BY
                scrape_id,
                airline,
                origin,
                destination,
                flight_number,
                departure,
                cabin,
                brand,
                search_trip_type,
                requested_return_date
        ),
        previous AS (
            SELECT
                MIN(id)               AS id,
                scrape_id,
                airline,
                origin,
                destination,
                flight_number,
                departure,
                cabin,
                brand,
                search_trip_type,
                requested_return_date,
                MIN(price_total_bdt)  AS price_total_bdt,
                MAX(seat_capacity)    AS seat_capacity,
                MAX(seat_available)   AS seat_available
            FROM previous_raw
            GROUP BY
                scrape_id,
                airline,
                origin,
                destination,
                flight_number,
                departure,
                cabin,
                brand,
                search_trip_type,
                requested_return_date
        ),

        current_ranked AS (
            SELECT
                c.*,
                MIN(price_total_bdt) OVER (
                    PARTITION BY origin, destination
                ) AS route_min_price
            FROM current c
        )

        SELECT
            COALESCE(c.airline, p.airline)              AS airline,
            COALESCE(c.origin, p.origin)                AS origin,
            COALESCE(c.destination, p.destination)     AS destination,
            COALESCE(c.flight_number, p.flight_number) AS flight_number,
            COALESCE(c.departure, p.departure)          AS departure,
            COALESCE(c.cabin, p.cabin)                  AS cabin,
            COALESCE(c.brand, p.brand)                  AS brand,
            COALESCE(c.search_trip_type, p.search_trip_type, 'OW') AS search_trip_type,
            COALESCE(c.requested_return_date, p.requested_return_date) AS requested_return_date,

            p.price_total_bdt AS previous_fare_bdt,
            c.price_total_bdt AS current_fare_bdt,

            c.seat_capacity   AS seat_capacity,
            p.seat_capacity   AS previous_seat_capacity,

            p.seat_available  AS previous_seats,
            c.seat_available  AS current_seats,

            COALESCE(cm.aircraft, cm.equipment_code, pm.aircraft, pm.equipment_code) AS aircraft,
            COALESCE(cm.arrival, pm.arrival) AS arrival,
            cm.tax_amount     AS current_tax,
            pm.tax_amount     AS previous_tax,
            COALESCE(cm.booking_class, pm.booking_class) AS rbd,
            cm.penalty_source AS current_penalty_source,
            pm.penalty_source AS previous_penalty_source,
            cm.source_endpoint AS current_source_endpoint,
            pm.source_endpoint AS previous_source_endpoint,
            cm.penalty_currency AS current_penalty_currency,
            pm.penalty_currency AS previous_penalty_currency,
            cm.penalty_rule_text AS current_penalty_rule_text,
            pm.penalty_rule_text AS previous_penalty_rule_text,
            cm.fare_change_fee_before_24h AS current_fare_change_fee_before_24h,
            pm.fare_change_fee_before_24h AS previous_fare_change_fee_before_24h,
            cm.fare_change_fee_within_24h AS current_fare_change_fee_within_24h,
            pm.fare_change_fee_within_24h AS previous_fare_change_fee_within_24h,
            cm.fare_change_fee_no_show AS current_fare_change_fee_no_show,
            pm.fare_change_fee_no_show AS previous_fare_change_fee_no_show,
            cm.fare_cancel_fee_before_24h AS current_fare_cancel_fee_before_24h,
            pm.fare_cancel_fee_before_24h AS previous_fare_cancel_fee_before_24h,
            cm.fare_cancel_fee_within_24h AS current_fare_cancel_fee_within_24h,
            pm.fare_cancel_fee_within_24h AS previous_fare_cancel_fee_within_24h,
            cm.fare_cancel_fee_no_show AS current_fare_cancel_fee_no_show,
            pm.fare_cancel_fee_no_show AS previous_fare_cancel_fee_no_show,
            cm.fare_refundable AS current_fare_refundable,
            pm.fare_refundable AS previous_fare_refundable,
            cm.fare_changeable AS current_fare_changeable,
            pm.fare_changeable AS previous_fare_changeable,

            CASE
                WHEN p.id IS NULL THEN 'NEW_FLIGHT'
                WHEN c.id IS NULL THEN 'REMOVED'
                WHEN c.price_total_bdt > p.price_total_bdt THEN 'INCREASE'
                WHEN c.price_total_bdt < p.price_total_bdt THEN 'DECREASE'
                ELSE 'UNCHANGED'
            END AS signal,

            CASE
                WHEN c.price_total_bdt = c.route_min_price THEN 1
                ELSE 0
            END AS is_price_leader

        FROM current_ranked c
        FULL OUTER JOIN previous p
          ON c.airline = p.airline
         AND c.origin = p.origin
         AND c.destination = p.destination
         AND c.flight_number = p.flight_number
         AND c.departure = p.departure
         AND c.cabin = p.cabin
         AND c.brand = p.brand
         AND COALESCE(c.search_trip_type, 'OW') = COALESCE(p.search_trip_type, 'OW')
         AND COALESCE(c.requested_return_date, '') = COALESCE(p.requested_return_date, '')

        LEFT JOIN flight_offer_raw_meta cm
          ON cm.flight_offer_id = c.id

        LEFT JOIN flight_offer_raw_meta pm
          ON pm.flight_offer_id = p.id

        ORDER BY origin, destination, flight_number;
        """
        )

        with self.engine.connect() as conn:
            df = pd.read_sql(
                sql,
                conn,
                params={
                    **params,
                },
            )
            df["route"] = df["origin"] + "-" + df["destination"]
            df["departure_time"] = pd.to_datetime(df["departure"]).dt.strftime("%H:%M")
            df["flight_date"] = pd.to_datetime(df["departure"]).dt.date
            df["search_trip_type"] = df.get("search_trip_type", pd.Series(index=df.index, dtype=object)).fillna("OW").astype(str).str.upper()
            df["requested_return_date"] = pd.to_datetime(df.get("requested_return_date"), errors="coerce").dt.date

            df["flight_key"] = (
                df["route"] + "|" + df["airline"] + "|" + df["flight_number"] + "|" + df["departure_time"]
                + "|" + df["search_trip_type"].astype(str)
                + "|" + df["requested_return_date"].astype(str)
            )

            aircraft_map = (
                df.groupby("flight_key")["aircraft"]
                .apply(lambda s: " / ".join(sorted({a for a in s.dropna()})))
                .reset_index()
                .rename(columns={"aircraft": "aircraft_label"})
            )

            df = df.merge(aircraft_map, on="flight_key", how="left")
            df = finalize_comparison_df(df)
            return df
