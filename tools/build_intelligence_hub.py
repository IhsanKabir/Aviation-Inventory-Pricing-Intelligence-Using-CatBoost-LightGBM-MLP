"""
Build unified intelligence outputs:
- Forecasting (baseline next-day)
- Competitive intelligence (route/airline leaderboard)
- Reliability snapshot (latest ops/quality statuses)
- Unified artifacts (CSV + XLSX + JSON + Markdown)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from core.runtime_config import get_database_url
except Exception:  # pragma: no cover
    DEFAULT_DB_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/airline_intel"

    def get_database_url():
        return os.getenv("AIRLINE_DB_URL", DEFAULT_DB_URL)


def parse_args():
    p = argparse.ArgumentParser(description="Build unified intelligence hub outputs")
    p.add_argument("--db-url", default=get_database_url())
    p.add_argument("--output-dir", default="output/reports")
    p.add_argument("--lookback-days", type=int, default=14)
    p.add_argument(
        "--forecast-target",
        choices=["min_price_bdt", "avg_seat_available", "offers_count", "soldout_rate"],
        default="min_price_bdt",
    )
    p.add_argument("--forecast-window", type=int, default=3)
    p.add_argument("--forecast-alpha", type=float, default=0.3)
    p.add_argument("--min-history", type=int, default=3)
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    return p.parse_args()


def _now(tz_mode: str):
    return datetime.now(timezone.utc) if tz_mode == "utc" else datetime.now().astimezone()


def _stamp(ts: datetime):
    return ts.strftime("%Y%m%d_%H%M%S")


def _load_daily_snapshot(db_url: str, lookback_days: int) -> pd.DataFrame:
    eng = create_engine(db_url, pool_pre_ping=True, future=True)
    q = text(
        """
        WITH daily AS (
            SELECT
                DATE(fo.scraped_at) AS report_day,
                fo.airline,
                fo.origin,
                fo.destination,
                fo.cabin,
                MIN(fo.price_total_bdt) AS min_price_bdt,
                AVG(fo.seat_available) AS avg_seat_available,
                COUNT(*) AS offers_count,
                SUM(CASE WHEN COALESCE(rm.soldout, FALSE) THEN 1 ELSE 0 END) AS soldout_offers
            FROM flight_offers fo
            LEFT JOIN LATERAL (
                SELECT r.soldout
                FROM flight_offer_raw_meta r
                WHERE r.flight_offer_id = fo.id
                ORDER BY r.id DESC
                LIMIT 1
            ) rm ON TRUE
            WHERE fo.scraped_at >= NOW() - (:lookback_days || ' days')::interval
            GROUP BY DATE(fo.scraped_at), fo.airline, fo.origin, fo.destination, fo.cabin
        )
        SELECT
            report_day,
            airline,
            origin,
            destination,
            cabin,
            min_price_bdt,
            avg_seat_available,
            offers_count,
            soldout_offers,
            CASE
                WHEN offers_count > 0 THEN soldout_offers::double precision / offers_count::double precision
                ELSE 0
            END AS soldout_rate
        FROM daily
        ORDER BY airline, origin, destination, cabin, report_day
        """
    )
    with eng.connect() as conn:
        return pd.read_sql(q, conn, params={"lookback_days": int(max(1, lookback_days))})


def _compute_competitive_intelligence(df: pd.DataFrame):
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    work = df.copy()
    group_cols = ["report_day", "origin", "destination", "cabin"]
    route_min = work.groupby(group_cols)["min_price_bdt"].transform("min")
    work["is_price_leader_day"] = (work["min_price_bdt"] == route_min).astype(int)

    route_sum = work.groupby(group_cols)["min_price_bdt"].transform("sum")
    route_count = work.groupby(group_cols)["min_price_bdt"].transform("count")
    work["competitor_avg_min_price_bdt"] = (
        (route_sum - work["min_price_bdt"]) / (route_count - 1).replace(0, pd.NA)
    )
    work["gap_vs_competitor_bdt"] = work["min_price_bdt"] - work["competitor_avg_min_price_bdt"]

    comp = (
        work.groupby(["airline", "origin", "destination", "cabin"], as_index=False)
        .agg(
            days_observed=("report_day", "nunique"),
            days_leading=("is_price_leader_day", "sum"),
            avg_min_price_bdt=("min_price_bdt", "mean"),
            price_volatility_bdt=("min_price_bdt", "std"),
            avg_seat_available=("avg_seat_available", "mean"),
            avg_soldout_rate=("soldout_rate", "mean"),
            avg_offers_count=("offers_count", "mean"),
            avg_gap_vs_competitor_bdt=("gap_vs_competitor_bdt", "mean"),
        )
    )
    comp["lowest_fare_share_pct"] = (
        comp["days_leading"] / comp["days_observed"].replace(0, pd.NA) * 100.0
    )
    comp["price_volatility_bdt"] = comp["price_volatility_bdt"].fillna(0.0)
    comp["leadership_rank"] = (
        comp.groupby(["origin", "destination", "cabin"])["lowest_fare_share_pct"]
        .rank(ascending=False, method="dense")
        .astype("Int64")
    )
    comp["price_rank"] = (
        comp.groupby(["origin", "destination", "cabin"])["avg_min_price_bdt"]
        .rank(ascending=True, method="dense")
        .astype("Int64")
    )

    route = (
        work.groupby(["origin", "destination", "cabin"], as_index=False)
        .agg(
            observed_days=("report_day", "nunique"),
            airlines_active=("airline", "nunique"),
            route_avg_lowest_fare_bdt=("min_price_bdt", "mean"),
            route_price_volatility_bdt=("min_price_bdt", "std"),
            route_avg_soldout_rate=("soldout_rate", "mean"),
            route_avg_offers_count=("offers_count", "mean"),
        )
    )
    route["route_price_volatility_bdt"] = route["route_price_volatility_bdt"].fillna(0.0)
    route["competition_index"] = (
        route["airlines_active"].fillna(0) * 10.0
        + (100.0 - route["route_avg_soldout_rate"].fillna(0.0) * 100.0) * 0.2
    )
    return comp.sort_values(
        ["origin", "destination", "cabin", "leadership_rank", "price_rank", "airline"]
    ).reset_index(drop=True), route.sort_values(["origin", "destination", "cabin"]).reset_index(drop=True)


def _compute_forecast(
    df: pd.DataFrame,
    target: str,
    min_history: int,
    window: int,
    alpha: float,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    group_cols = ["airline", "origin", "destination", "cabin"]
    rows = []
    alpha = max(0.0001, min(float(alpha), 1.0))
    window = max(int(window), 1)
    min_history = max(int(min_history), 2)

    for key, part in df.groupby(group_cols, dropna=False):
        part = part.sort_values("report_day").copy()
        y = pd.to_numeric(part[target], errors="coerce").dropna()
        if len(y) < min_history:
            continue

        last_day = pd.to_datetime(part["report_day"].iloc[-1], errors="coerce")
        if pd.isna(last_day):
            continue
        last_day = last_day.date()
        pred_last = float(y.iloc[-1])
        pred_roll = float(y.tail(window).mean())
        pred_ewm = float(y.ewm(alpha=alpha, adjust=False).mean().iloc[-1])

        recent = y.tail(max(7, window))
        p10 = float(recent.quantile(0.10)) if len(recent) else None
        p90 = float(recent.quantile(0.90)) if len(recent) else None
        movement = pred_roll - pred_last
        direction = "UP" if movement > 0 else ("DOWN" if movement < 0 else "FLAT")

        rows.append(
            {
                "airline": key[0],
                "origin": key[1],
                "destination": key[2],
                "cabin": key[3],
                "target_column": target,
                "latest_report_day": last_day,
                "predicted_for_day": last_day + timedelta(days=1),
                "history_days": int(len(y)),
                "latest_actual_value": pred_last,
                "pred_last_value": pred_last,
                f"pred_rolling_mean_{window}": pred_roll,
                f"pred_ewm_alpha_{alpha:.2f}": pred_ewm,
                "p10_baseline": p10,
                "p90_baseline": p90,
                "expected_direction": direction,
            }
        )
    return pd.DataFrame(rows).sort_values(["origin", "destination", "airline", "cabin"]).reset_index(drop=True)


def _read_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _ops_status_snapshot(output_dir: Path):
    smoke = _read_json(output_dir / "smoke_check_latest.json") or {}
    sla = _read_json(output_dir / "data_sla_latest.json") or {}
    drift = _read_json(output_dir / "model_drift_latest.json") or {}
    dashboard = _read_json(output_dir / "operator_dashboard_latest.json") or {}

    return pd.DataFrame(
        [
            {"metric": "smoke_check", "status": smoke.get("status", "UNKNOWN"), "source": "smoke_check_latest.json"},
            {"metric": "data_sla", "status": sla.get("status", "UNKNOWN"), "source": "data_sla_latest.json"},
            {
                "metric": "model_drift",
                "status": drift.get("overall_status", "UNKNOWN"),
                "source": "model_drift_latest.json",
            },
            {
                "metric": "operator_dashboard",
                "status": (dashboard.get("statuses") or {}).get("smoke", "UNKNOWN"),
                "source": "operator_dashboard_latest.json",
            },
        ]
    )


def _write_csv_pair(df: pd.DataFrame, path_latest: Path, path_run: Path):
    df.to_csv(path_run, index=False)
    df.to_csv(path_latest, index=False)


def main():
    args = parse_args()
    now = _now(args.timestamp_tz)
    ts = _stamp(now)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    daily = _load_daily_snapshot(args.db_url, args.lookback_days)
    competitive_df, route_df = _compute_competitive_intelligence(daily)
    forecast_df = _compute_forecast(
        daily,
        target=args.forecast_target,
        min_history=args.min_history,
        window=args.forecast_window,
        alpha=args.forecast_alpha,
    )
    ops_df = _ops_status_snapshot(out_dir)

    competitive_run = out_dir / f"intelligence_competitive_{ts}.csv"
    competitive_latest = out_dir / "intelligence_competitive_latest.csv"
    route_run = out_dir / f"intelligence_route_summary_{ts}.csv"
    route_latest = out_dir / "intelligence_route_summary_latest.csv"
    forecast_run = out_dir / f"intelligence_forecast_{args.forecast_target}_{ts}.csv"
    forecast_latest = out_dir / f"intelligence_forecast_{args.forecast_target}_latest.csv"
    ops_run = out_dir / f"intelligence_ops_status_{ts}.csv"
    ops_latest = out_dir / "intelligence_ops_status_latest.csv"

    _write_csv_pair(competitive_df, competitive_latest, competitive_run)
    _write_csv_pair(route_df, route_latest, route_run)
    _write_csv_pair(forecast_df, forecast_latest, forecast_run)
    _write_csv_pair(ops_df, ops_latest, ops_run)

    xlsx_run = out_dir / f"intelligence_hub_{ts}.xlsx"
    xlsx_latest = out_dir / "intelligence_hub_latest.xlsx"
    with pd.ExcelWriter(xlsx_run, engine="openpyxl") as writer:
        forecast_df.to_excel(writer, index=False, sheet_name="Forecast")
        competitive_df.to_excel(writer, index=False, sheet_name="Competitive")
        route_df.to_excel(writer, index=False, sheet_name="RouteSummary")
        ops_df.to_excel(writer, index=False, sheet_name="OpsStatus")
    with pd.ExcelWriter(xlsx_latest, engine="openpyxl") as writer:
        forecast_df.to_excel(writer, index=False, sheet_name="Forecast")
        competitive_df.to_excel(writer, index=False, sheet_name="Competitive")
        route_df.to_excel(writer, index=False, sheet_name="RouteSummary")
        ops_df.to_excel(writer, index=False, sheet_name="OpsStatus")

    overview = {
        "generated_at": now.isoformat(),
        "lookback_days": int(args.lookback_days),
        "forecast_target": args.forecast_target,
        "rows": {
            "daily_snapshot": int(len(daily)),
            "forecast": int(len(forecast_df)),
            "competitive": int(len(competitive_df)),
            "route_summary": int(len(route_df)),
            "ops_status": int(len(ops_df)),
        },
        "artifacts": {
            "forecast_csv": str(forecast_run),
            "competitive_csv": str(competitive_run),
            "route_summary_csv": str(route_run),
            "ops_status_csv": str(ops_run),
            "workbook_xlsx": str(xlsx_run),
        },
    }

    overview_run = out_dir / f"intelligence_overview_{ts}.json"
    overview_latest = out_dir / "intelligence_overview_latest.json"
    overview_run.write_text(json.dumps(overview, indent=2), encoding="utf-8")
    overview_latest.write_text(json.dumps(overview, indent=2), encoding="utf-8")

    md_lines = [
        "# Intelligence Hub",
        "",
        f"- Generated at: `{overview['generated_at']}`",
        f"- Lookback days: `{overview['lookback_days']}`",
        f"- Forecast target: `{overview['forecast_target']}`",
        "",
        "## Row Counts",
        "",
        f"- Daily snapshot: `{overview['rows']['daily_snapshot']}`",
        f"- Forecast rows: `{overview['rows']['forecast']}`",
        f"- Competitive rows: `{overview['rows']['competitive']}`",
        f"- Route summary rows: `{overview['rows']['route_summary']}`",
        f"- Ops status rows: `{overview['rows']['ops_status']}`",
        "",
        "## Artifacts",
        "",
        f"- Workbook: `{overview['artifacts']['workbook_xlsx']}`",
        f"- Forecast CSV: `{overview['artifacts']['forecast_csv']}`",
        f"- Competitive CSV: `{overview['artifacts']['competitive_csv']}`",
        f"- Route Summary CSV: `{overview['artifacts']['route_summary_csv']}`",
        f"- Ops Status CSV: `{overview['artifacts']['ops_status_csv']}`",
        "",
    ]
    md_text = "\n".join(md_lines) + "\n"
    md_run = out_dir / f"intelligence_overview_{ts}.md"
    md_latest = out_dir / "intelligence_overview_latest.md"
    md_run.write_text(md_text, encoding="utf-8")
    md_latest.write_text(md_text, encoding="utf-8")

    print(f"daily_snapshot_rows={len(daily)}")
    print(f"forecast_rows={len(forecast_df)} -> {forecast_run}")
    print(f"competitive_rows={len(competitive_df)} -> {competitive_run}")
    print(f"route_summary_rows={len(route_df)} -> {route_run}")
    print(f"ops_status_rows={len(ops_df)} -> {ops_run}")
    print(f"xlsx={xlsx_run}")
    print(f"overview_json={overview_run}")
    print(f"overview_md={md_run}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
