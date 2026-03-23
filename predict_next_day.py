import argparse
import json
import os
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from core.market_priors import apply_market_priors
from core.holiday_features import add_holiday_features, get_holiday_feature_columns
from core.explainability import compute_shap_feature_importance, format_feature_importance_for_output
from db import DATABASE_URL as DEFAULT_DATABASE_URL


BASE_GROUP_COLS = ["airline", "origin", "destination", "cabin"]
SEARCH_GROUP_COLS = BASE_GROUP_COLS + ["departure_day"]
EVENT_TARGETS = {"total_change_events", "price_events", "availability_events"}
SEARCH_TARGETS = {"min_price_bdt", "avg_seat_available", "offers_count", "soldout_rate"}
SUPPORTED_ML_MODELS = {"catboost", "lightgbm"}
SUPPORTED_DL_MODELS = {"mlp"}
NON_NEGATIVE_TARGETS = EVENT_TARGETS | {"min_price_bdt", "avg_seat_available", "offers_count", "soldout_rate"}
UNIT_INTERVAL_TARGETS = {"soldout_rate"}

MARKET_PRIOR_NUMERIC_COLS = [
    "market_is_middle_east",
    "market_is_ksa",
    "market_is_thailand_tourism",
    "market_is_labor_outbound",
    "market_is_labor_return",
    "airline_is_hub_spoke",
    "airline_is_lcc",
    "airline_is_return_oriented",
    "horizon_is_visa_window",
    "horizon_is_long_window",
]

HOLIDAY_FEATURE_COLS = [
    "is_search_holiday",
    "is_high_demand_holiday",
    "days_to_next_holiday",
    "days_since_last_holiday",
    "is_holiday_week",
    "holiday_type_code",
    "is_departure_holiday",
    "is_departure_high_demand",
]

AIRLINE_MODEL_CODE = {"hybrid": 0.0, "hub_spoke": 1.0, "lcc": 2.0}
TRIP_PURPOSE_CODE = {"general": 0.0, "labor_outbound": 1.0, "labor_return": 2.0, "tourism": 3.0}
YIELD_CLASS_CODE = {"unknown": 0.0, "balanced": 1.0, "medium_high": 2.0, "tourism": 3.0, "high": 4.0}
HORIZON_BUCKET_CODE = {"unknown": 0.0, "D0_visa": 1.0, "D8_30": 2.0, "D31_90": 3.0, "D91_180": 4.0, "D181p": 5.0}


def parse_args():
    parser = argparse.ArgumentParser(description="Baseline next-day prediction + evaluation")
    parser.add_argument("--start-date", help="YYYY-MM-DD")
    parser.add_argument("--end-date", help="YYYY-MM-DD")
    parser.add_argument("--airline")
    parser.add_argument("--origin")
    parser.add_argument("--destination")
    parser.add_argument("--cabin")
    parser.add_argument(
        "--series-mode",
        choices=["event_daily", "search_dynamic"],
        default="event_daily",
        help="event_daily uses change-event daily views; search_dynamic uses search-day x departure-day series from flight_offers",
    )
    parser.add_argument(
        "--target-column",
        choices=[
            "total_change_events",
            "price_events",
            "availability_events",
            "min_price_bdt",
            "avg_seat_available",
            "offers_count",
            "soldout_rate",
        ],
        default="total_change_events",
    )
    parser.add_argument("--departure-start-date", help="YYYY-MM-DD departure lower bound (search_dynamic mode)")
    parser.add_argument("--departure-end-date", help="YYYY-MM-DD departure upper bound (search_dynamic mode)")
    parser.add_argument(
        "--rolling-windows",
        default="3,7",
        help="Comma-separated rolling windows for baseline models (default: 3,7)",
    )
    parser.add_argument("--seasonal-lag", type=int, default=7, help="Seasonal naive lag in days (default: 7)")
    parser.add_argument("--ewm-alpha", type=float, default=0.3, help="EWMA alpha in (0,1], default 0.3")
    parser.add_argument("--min-history", type=int, default=2, help="Minimum history rows per group")
    parser.add_argument("--backtest-train-days", type=int, default=28, help="Train window length in days")
    parser.add_argument("--backtest-val-days", type=int, default=7, help="Validation window length in days")
    parser.add_argument("--backtest-test-days", type=int, default=7, help="Test window length in days")
    parser.add_argument("--backtest-step-days", type=int, default=7, help="Step size between rolling splits (days)")
    parser.add_argument(
        "--backtest-model-min-coverage-ratio",
        type=float,
        default=0.8,
        help="Min n coverage ratio vs best-covered model when selecting split winner (default: 0.8)",
    )
    parser.add_argument(
        "--backtest-selection-metric",
        choices=["mae", "rmse"],
        default="mae",
        help="Metric used to select winning model on validation split (default: mae)",
    )
    parser.add_argument("--disable-backtest", action="store_true", help="Skip rolling-window backtest artifacts")
    parser.add_argument(
        "--ml-models",
        default="catboost,lightgbm",
        help="Comma-separated optional ML models: catboost,lightgbm (default: catboost,lightgbm)",
    )
    parser.add_argument(
        "--ml-quantiles",
        default="0.1,0.5,0.9",
        help="Comma-separated quantiles for ML models (default: 0.1,0.5,0.9)",
    )
    parser.add_argument("--ml-min-history", type=int, default=14, help="Minimum history rows required for ML")
    parser.add_argument("--ml-random-seed", type=int, default=42, help="Random seed for ML models")
    parser.add_argument(
        "--dl-models",
        default="mlp",
        help="Comma-separated optional DL models: mlp (default: mlp)",
    )
    parser.add_argument(
        "--dl-quantiles",
        default="0.1,0.5,0.9",
        help="Comma-separated quantiles for DL models (default: 0.1,0.5,0.9)",
    )
    parser.add_argument("--dl-min-history", type=int, default=8, help="Minimum history rows required for DL")
    parser.add_argument("--dl-random-seed", type=int, default=42, help="Random seed for DL models")
    parser.add_argument("--output-dir", default="output/reports")
    parser.add_argument("--db-url", default=os.getenv("AIRLINE_DB_URL", DEFAULT_DATABASE_URL))
    return parser.parse_args()


def _build_where_clause(args):
    clauses = []
    params = {}

    if args.start_date:
        clauses.append("report_day >= :start_date")
        params["start_date"] = args.start_date
    if args.end_date:
        clauses.append("report_day <= :end_date")
        params["end_date"] = args.end_date
    if args.airline:
        clauses.append("airline = :airline")
        params["airline"] = args.airline.upper()
    if args.origin:
        clauses.append("origin = :origin")
        params["origin"] = args.origin.upper()
    if args.destination:
        clauses.append("destination = :destination")
        params["destination"] = args.destination.upper()
    if args.cabin:
        clauses.append("cabin = :cabin")
        params["cabin"] = args.cabin

    where_sql = ""
    if clauses:
        where_sql = " WHERE " + " AND ".join(clauses)
    return where_sql, params


def _build_offer_where_clause(args, alias: str = "fo"):
    clauses = []
    params = {}
    if args.start_date:
        clauses.append(f"DATE({alias}.scraped_at) >= :start_date")
        params["start_date"] = args.start_date
    if args.end_date:
        clauses.append(f"DATE({alias}.scraped_at) <= :end_date")
        params["end_date"] = args.end_date
    if args.airline:
        clauses.append(f"{alias}.airline = :airline")
        params["airline"] = args.airline.upper()
    if args.origin:
        clauses.append(f"{alias}.origin = :origin")
        params["origin"] = args.origin.upper()
    if args.destination:
        clauses.append(f"{alias}.destination = :destination")
        params["destination"] = args.destination.upper()
    if args.cabin:
        clauses.append(f"{alias}.cabin = :cabin")
        params["cabin"] = args.cabin
    if args.departure_start_date:
        clauses.append(f"DATE({alias}.departure) >= :departure_start_date")
        params["departure_start_date"] = args.departure_start_date
    if args.departure_end_date:
        clauses.append(f"DATE({alias}.departure) <= :departure_end_date")
        params["departure_end_date"] = args.departure_end_date
    where_sql = ""
    if clauses:
        where_sql = " WHERE " + " AND ".join(clauses)
    return where_sql, params


def _load_from_offer_history(engine, args):
    where_sql, params = _build_offer_where_clause(args, alias="fo")
    sql = text(
        f"""
        WITH daily AS (
            SELECT
                DATE(fo.scraped_at) AS report_day,
                fo.airline,
                fo.origin,
                fo.destination,
                fo.cabin,
                MIN(fo.price_total_bdt) AS min_price_bdt,
                AVG(fo.seat_available) AS avg_seat_available
            FROM flight_offers fo
            {where_sql}
            GROUP BY DATE(fo.scraped_at), fo.airline, fo.origin, fo.destination, fo.cabin
        ),
        deltas AS (
            SELECT
                d.*,
                LAG(d.min_price_bdt) OVER (
                    PARTITION BY d.airline, d.origin, d.destination, d.cabin
                    ORDER BY d.report_day
                ) AS prev_min_price_bdt,
                LAG(d.avg_seat_available) OVER (
                    PARTITION BY d.airline, d.origin, d.destination, d.cabin
                    ORDER BY d.report_day
                ) AS prev_avg_seat_available
            FROM daily d
        )
        SELECT
            report_day,
            airline,
            origin,
            destination,
            cabin,
            CASE
                WHEN prev_min_price_bdt IS NULL THEN 0
                WHEN min_price_bdt IS DISTINCT FROM prev_min_price_bdt THEN 1
                ELSE 0
            END AS price_events,
            CASE
                WHEN prev_avg_seat_available IS NULL THEN 0
                WHEN avg_seat_available IS DISTINCT FROM prev_avg_seat_available THEN 1
                ELSE 0
            END AS availability_events
        FROM deltas
        ORDER BY airline, origin, destination, cabin, report_day
        """
    )
    with engine.connect() as conn:
        base = pd.read_sql(sql, conn, params=params)
    if base.empty:
        return base
    base["total_change_events"] = base["price_events"] + base["availability_events"]
    return base[
        [
            "report_day",
            "airline",
            "origin",
            "destination",
            "cabin",
            "total_change_events",
            "price_events",
            "availability_events",
        ]
    ]


def _parse_windows(raw: str):
    out = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    return sorted(set(w for w in out if w > 0))


def _parse_ml_models(raw: str):
    items = []
    for part in str(raw or "").split(","):
        token = part.strip().lower()
        if not token or token == "none":
            continue
        items.append(token)
    return sorted(set(items))


def _parse_quantiles(raw: str):
    values = []
    for part in str(raw or "").split(","):
        token = part.strip()
        if not token:
            continue
        try:
            q = float(token)
        except ValueError:
            continue
        if 0.0 < q < 1.0:
            values.append(q)
    values = sorted(set(values))
    return values if values else [0.1, 0.5, 0.9]


def _resolve_ml_models(requested_models: list[str]):
    available = {}
    skipped = {}
    for model_name in requested_models:
        if model_name not in SUPPORTED_ML_MODELS:
            skipped[model_name] = "unsupported"
            continue
        try:
            if model_name == "catboost":
                from catboost import CatBoostRegressor

                available[model_name] = CatBoostRegressor
            elif model_name == "lightgbm":
                from lightgbm import LGBMRegressor

                available[model_name] = LGBMRegressor
        except Exception as exc:  # pragma: no cover
            skipped[model_name] = str(exc)
    return available, skipped


def _resolve_dl_models(requested_models: list[str]):
    available = {}
    skipped = {}
    for model_name in requested_models:
        if model_name not in SUPPORTED_DL_MODELS:
            skipped[model_name] = "unsupported"
            continue
        try:
            if model_name == "mlp":
                from sklearn.neural_network import MLPRegressor

                available[model_name] = MLPRegressor
        except Exception as exc:  # pragma: no cover
            skipped[model_name] = str(exc)
    return available, skipped


def _quantile_suffix(q: float):
    return f"q{int(round(float(q) * 100)):02d}"


def _apply_market_priors_safe(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    try:
        return apply_market_priors(df)
    except Exception:
        return df


def _apply_holiday_features_safe(df: pd.DataFrame, date_column: str = "report_day", departure_column: str = "departure_day") -> pd.DataFrame:
    """
    Safely apply holiday features to DataFrame.

    Args:
        df: Input DataFrame
        date_column: Column name for search/report date
        departure_column: Column name for departure date (optional)

    Returns:
        DataFrame with holiday features added, or original DataFrame if error occurs
    """
    if df is None or df.empty:
        return df
    try:
        return add_holiday_features(df, date_column=date_column, departure_column=departure_column)
    except Exception as e:
        # If holiday feature extraction fails, continue without them
        print(f"Warning: Holiday features unavailable: {e}")
        return df


def _ml_feature_frame(part: pd.DataFrame, target: str):
    vals = pd.to_numeric(part[target], errors="coerce")
    work = pd.DataFrame(index=part.index)
    work["lag1"] = vals.shift(1)
    work["lag2"] = vals.shift(2)
    work["lag3"] = vals.shift(3)
    work["lag7"] = vals.shift(7)
    work["lag14"] = vals.shift(14)
    work["roll3"] = vals.shift(1).rolling(window=3, min_periods=1).mean()
    work["roll7"] = vals.shift(1).rolling(window=7, min_periods=1).mean()
    work["roll14"] = vals.shift(1).rolling(window=14, min_periods=1).mean()
    work["roll7_std"] = vals.shift(1).rolling(window=7, min_periods=2).std()
    work["diff_1_2"] = work["lag1"] - work["lag2"]
    work["diff_1_7"] = work["lag1"] - work["lag7"]
    work["ewm03"] = vals.shift(1).ewm(alpha=0.3, adjust=False).mean()
    report_day = pd.to_datetime(part["report_day"], errors="coerce")
    work["dow"] = report_day.dt.dayofweek
    work["dom"] = report_day.dt.day
    if "departure_day" in part.columns:
        dep = pd.to_datetime(part["departure_day"], errors="coerce")
        work["days_to_departure"] = (dep - report_day).dt.days
    for col in MARKET_PRIOR_NUMERIC_COLS:
        if col in part.columns:
            work[col] = pd.to_numeric(part[col], errors="coerce")

    # Add holiday features
    for col in HOLIDAY_FEATURE_COLS:
        if col in part.columns:
            work[col] = pd.to_numeric(part[col], errors="coerce")

    if "airline_model_proxy" in part.columns:
        work["airline_model_proxy_code"] = (
            part["airline_model_proxy"].astype(str).str.strip().str.lower().map(AIRLINE_MODEL_CODE)
        )
    if "trip_purpose_proxy" in part.columns:
        work["trip_purpose_proxy_code"] = (
            part["trip_purpose_proxy"].astype(str).str.strip().str.lower().map(TRIP_PURPOSE_CODE)
        )
    if "yield_class_proxy" in part.columns:
        work["yield_class_proxy_code"] = (
            part["yield_class_proxy"].astype(str).str.strip().str.lower().map(YIELD_CLASS_CODE)
        )
    if "horizon_bucket_proxy" in part.columns:
        work["horizon_bucket_proxy_code"] = (
            part["horizon_bucket_proxy"].astype(str).str.strip().map(HORIZON_BUCKET_CODE)
        )
    return work


def _fill_ml_features(train_x: pd.DataFrame, pred_x: pd.DataFrame):
    fill_vals = train_x.median(numeric_only=True)
    train_f = train_x.fillna(fill_vals).fillna(0.0).astype(float)
    pred_f = pred_x.fillna(fill_vals).fillna(0.0).astype(float)
    return train_f, pred_f


def _recency_weights(n: int, min_w: float = 0.5, max_w: float = 1.0):
    n = int(max(n, 1))
    if n == 1:
        return np.array([max_w], dtype=float)
    return np.linspace(float(min_w), float(max_w), n, dtype=float)


def _clip_prediction_value(target: str, value):
    if value is None:
        return None
    try:
        v = float(value)
    except Exception:
        return value
    if target in UNIT_INTERVAL_TARGETS:
        return float(max(0.0, min(1.0, v)))
    if target in NON_NEGATIVE_TARGETS:
        return float(max(0.0, v))
    return float(v)


def _clip_prediction_columns(df: pd.DataFrame, target: str, pred_cols: list[str]):
    if df is None or df.empty:
        return df
    out = df.copy()
    cols = [c for c in pred_cols if c in out.columns]
    if not cols:
        return out
    for col in cols:
        out[col] = out[col].map(lambda v: _clip_prediction_value(target, v))
    return out


def _robust_prediction_bounds(y: pd.Series):
    vals = pd.to_numeric(y, errors="coerce")
    vals = vals[np.isfinite(vals.to_numpy(dtype=float, copy=False))]
    if len(vals) == 0:
        return None, None
    q01 = float(np.quantile(vals, 0.01))
    q25 = float(np.quantile(vals, 0.25))
    q75 = float(np.quantile(vals, 0.75))
    q99 = float(np.quantile(vals, 0.99))
    iqr = max(0.0, q75 - q25)
    lower = q01 - (2.0 * iqr)
    upper = q99 + (2.0 * iqr)
    if upper < lower:
        lower = float(np.min(vals))
        upper = float(np.max(vals))
    return lower, upper


def add_prediction_confidence(
    df: pd.DataFrame,
    target: str,
    route_eval: pd.DataFrame,
    group_cols: list[str]
) -> pd.DataFrame:
    """
    Add prediction confidence bands based on quantile spread and historical accuracy.

    Confidence levels:
        - high: uncertainty < 10% of median prediction AND historical MAE < threshold
        - medium: uncertainty 10-25% of median prediction OR historical MAE moderate
        - low: uncertainty > 25% of median prediction OR historical MAE high

    Args:
        df: Predictions DataFrame with quantile columns
        target: Target column name
        route_eval: Route-level evaluation metrics (contains historical MAE per route)
        group_cols: Grouping columns (e.g., ['airline', 'origin', 'destination', 'cabin'])

    Returns:
        DataFrame with added columns: prediction_uncertainty, prediction_confidence
    """
    if df is None or df.empty:
        return df

    result = df.copy()

    # Find quantile columns
    q10_col = [c for c in result.columns if 'q10' in c.lower()]
    q50_col = [c for c in result.columns if 'q50' in c.lower()]
    q90_col = [c for c in result.columns if 'q90' in c.lower()]

    # If no quantiles available, use simple confidence based on historical MAE only
    if not q10_col or not q50_col or not q90_col:
        result['prediction_confidence'] = 'medium'
        result['prediction_uncertainty'] = 0.0
        return result

    q10_col = q10_col[0]
    q50_col = q50_col[0]
    q90_col = q90_col[0]

    # Calculate uncertainty as quantile spread
    result['prediction_uncertainty'] = abs(result[q90_col] - result[q10_col])

    # Calculate relative uncertainty (uncertainty / median prediction)
    median_pred = result[q50_col].replace(0, 1.0)  # Avoid division by zero
    result['relative_uncertainty'] = result['prediction_uncertainty'] / abs(median_pred)

    # Merge with historical MAE from route_eval
    if not route_eval.empty and 'mae' in route_eval.columns:
        merge_cols = [c for c in group_cols if c in result.columns and c in route_eval.columns]
        if merge_cols:
            # Get best model MAE per route
            route_mae = route_eval.sort_values('mae').groupby(merge_cols, as_index=False).first()[merge_cols + ['mae']]
            result = result.merge(route_mae, on=merge_cols, how='left')
        else:
            result['mae'] = np.nan
    else:
        result['mae'] = np.nan

    # Determine confidence level
    # High: relative_uncertainty < 0.10 AND mae < 0.3 (for change events) or mae < 500 (for prices)
    # Medium: 0.10 <= relative_uncertainty < 0.25 OR moderate mae
    # Low: relative_uncertainty >= 0.25 OR high mae

    # Define MAE thresholds based on target type
    if target in EVENT_TARGETS:
        mae_high_threshold = 0.3
        mae_medium_threshold = 0.5
    elif target == 'min_price_bdt':
        mae_high_threshold = 500
        mae_medium_threshold = 1000
    elif target in ['avg_seat_available', 'offers_count']:
        mae_high_threshold = 5.0
        mae_medium_threshold = 10.0
    elif target == 'soldout_rate':
        mae_high_threshold = 0.15
        mae_medium_threshold = 0.30
    else:
        mae_high_threshold = float('inf')
        mae_medium_threshold = float('inf')

    def determine_confidence(row):
        rel_unc = row.get('relative_uncertainty', 0.5)
        mae = row.get('mae', float('inf'))

        # High confidence: low uncertainty AND low MAE
        if rel_unc < 0.10 and mae < mae_high_threshold:
            return 'high'

        # Low confidence: high uncertainty OR high MAE
        if rel_unc >= 0.25 or mae >= mae_medium_threshold:
            return 'low'

        # Medium confidence: everything else
        return 'medium'

    result['prediction_confidence'] = result.apply(determine_confidence, axis=1)

    # Clean up temporary columns
    if 'relative_uncertainty' in result.columns:
        result = result.drop(columns=['relative_uncertainty'])
    if 'mae' in result.columns:
        result = result.drop(columns=['mae'])

    return result


def _clip_to_bounds(value, lower, upper):
    if value is None:
        return None
    try:
        v = float(value)
    except Exception:
        return value
    if lower is not None and v < lower:
        v = lower
    if upper is not None and v > upper:
        v = upper
    return float(v)


def _fit_predict_quantile(model_name: str, model_cls, quantile: float, train_x: pd.DataFrame, train_y: pd.Series, pred_x: pd.DataFrame, seed: int, return_model=False):
    train_f, pred_f = _fill_ml_features(train_x, pred_x)
    y = pd.to_numeric(train_y, errors="coerce")
    mask = y.notna()
    train_f = train_f.loc[mask]
    y = y.loc[mask]
    if len(y) < 2:
        if return_model:
            return None, None, None
        return None
    sample_weight = _recency_weights(len(y))
    lower, upper = _robust_prediction_bounds(y)

    if model_name == "catboost":
        model = model_cls(
            loss_function=f"Quantile:alpha={float(quantile)}",
            iterations=250,
            learning_rate=0.05,
            depth=6,
            random_seed=int(seed),
            verbose=False,
        )
        model.fit(train_f, y, sample_weight=sample_weight, verbose=False)
        pred = model.predict(pred_f)
        result = _clip_to_bounds(float(np.asarray(pred).reshape(-1)[0]), lower, upper)
        if return_model:
            return result, model, train_f
        return result

    if model_name == "lightgbm":
        model = model_cls(
            objective="quantile",
            alpha=float(quantile),
            n_estimators=220,
            learning_rate=0.05,
            num_leaves=31,
            min_child_samples=5,
            subsample=0.9,
            colsample_bytree=0.9,
            random_state=int(seed),
            verbose=-1,
        )
        model.fit(train_f, y, sample_weight=sample_weight)
        pred = model.predict(pred_f)
        result = _clip_to_bounds(float(np.asarray(pred).reshape(-1)[0]), lower, upper)
        if return_model:
            return result, model, train_f
        return result

    if return_model:
        return None, None, None
    return None


def _fit_predict_dl_quantile(model_name: str, model_cls, quantile: float, train_x: pd.DataFrame, train_y: pd.Series, pred_x: pd.DataFrame, seed: int):
    train_f, pred_f = _fill_ml_features(train_x, pred_x)
    y = pd.to_numeric(train_y, errors="coerce")
    mask = y.notna()
    train_f = train_f.loc[mask]
    y = y.loc[mask]
    if len(y) < 8:
        return None
    lower, upper = _robust_prediction_bounds(y)

    if model_name == "mlp":
        # Scale features for stable neural-network optimization.
        from sklearn.exceptions import ConvergenceWarning
        from sklearn.preprocessing import StandardScaler

        scaler = StandardScaler()
        train_arr = scaler.fit_transform(train_f)
        pred_arr = scaler.transform(pred_f)
        model = model_cls(
            hidden_layer_sizes=(64, 32),
            activation="relu",
            solver="adam",
            alpha=5e-4,
            learning_rate_init=0.01,
            max_iter=700,
            early_stopping=True,
            validation_fraction=0.15,
            n_iter_no_change=25,
            random_state=int(seed),
        )
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=ConvergenceWarning)
            model.fit(train_arr, y)

        pred_mean = float(np.asarray(model.predict(pred_arr)).reshape(-1)[0])
        train_pred = np.asarray(model.predict(train_arr)).reshape(-1)
        residuals = np.asarray(y, dtype=float) - train_pred
        residuals = residuals[np.isfinite(residuals)]
        if residuals.size == 0:
            return _clip_to_bounds(pred_mean, lower, upper)
        q_adj = float(np.quantile(residuals, float(quantile)))
        return _clip_to_bounds(pred_mean + q_adj, lower, upper)

    return None


def _ml_prediction_columns(model_names: list[str], quantiles: list[float]):
    cols = []
    for m in model_names:
        for q in quantiles:
            cols.append(f"pred_ml_{m}_{_quantile_suffix(q)}")
    return cols


def _dl_prediction_columns(model_names: list[str], quantiles: list[float]):
    cols = []
    for m in model_names:
        for q in quantiles:
            cols.append(f"pred_dl_{m}_{_quantile_suffix(q)}")
    return cols


def add_ml_prediction_columns(
    df: pd.DataFrame,
    target: str,
    group_cols: list[str],
    model_classes: dict,
    quantiles: list[float],
    min_history: int,
    random_seed: int,
):
    if not model_classes:
        return df

    out = df.copy()
    out = out.sort_values(group_cols + ["report_day"]).reset_index(drop=True)
    pred_cols = _ml_prediction_columns(list(model_classes.keys()), quantiles)
    for col in pred_cols:
        out[col] = pd.NA

    min_history = max(int(min_history), 3)
    for _, part in out.groupby(group_cols, dropna=False):
        part = part.sort_values("report_day")
        idx_list = part.index.tolist()
        features = _ml_feature_frame(part, target=target)
        y = pd.to_numeric(part[target], errors="coerce")
        for i, row_idx in enumerate(idx_list):
            if pd.isna(y.loc[row_idx]):
                continue
            train_idx = [idx_list[j] for j in range(i) if pd.notna(y.loc[idx_list[j]])]
            if len(train_idx) < min_history:
                continue
            train_x = features.loc[train_idx]
            train_y = y.loc[train_idx]
            pred_x = features.loc[[row_idx]]
            for model_name, model_cls in model_classes.items():
                for q in quantiles:
                    col = f"pred_ml_{model_name}_{_quantile_suffix(q)}"
                    try:
                        pred = _fit_predict_quantile(
                            model_name=model_name,
                            model_cls=model_cls,
                            quantile=q,
                            train_x=train_x,
                            train_y=train_y,
                            pred_x=pred_x,
                            seed=random_seed,
                        )
                    except Exception:
                        pred = None
                    if pred is not None:
                        out.at[row_idx, col] = pred
    return out


def build_next_day_ml_predictions(
    df: pd.DataFrame,
    target: str,
    group_cols: list[str],
    model_classes: dict,
    quantiles: list[float],
    min_history: int,
    random_seed: int,
):
    if not model_classes or df.empty:
        return pd.DataFrame()

    rows = []
    min_history = max(int(min_history), 3)
    for key, part in df.groupby(group_cols, dropna=False):
        part = part.sort_values("report_day").copy()
        y = pd.to_numeric(part[target], errors="coerce")
        train_mask = y.notna()
        if int(train_mask.sum()) < min_history:
            continue

        last_day = pd.to_datetime(part["report_day"].iloc[-1], errors="coerce")
        if pd.isna(last_day):
            continue
        next_day = last_day + timedelta(days=1)

        next_row = part.iloc[-1:].copy()
        next_row["report_day"] = next_day.date()
        next_row[target] = np.nan
        extended = pd.concat([part, next_row], ignore_index=True)

        feature_train = _ml_feature_frame(part, target=target)
        feature_ext = _ml_feature_frame(extended, target=target)
        pred_x = feature_ext.tail(1)

        row = {col: key[idx] for idx, col in enumerate(group_cols)}
        row["latest_report_day"] = last_day.date()
        row["predicted_for_day"] = next_day.date()

        # Track q50 model for SHAP computation (Phase 2 Priority 1)
        q50_model = None
        q50_features = None

        for model_name, model_cls in model_classes.items():
            for q in quantiles:
                col = f"pred_ml_{model_name}_{_quantile_suffix(q)}"
                # Capture model and features for q50 CatBoost to compute SHAP
                return_model = (q == 0.5 and model_name == "catboost")
                try:
                    result = _fit_predict_quantile(
                        model_name=model_name,
                        model_cls=model_cls,
                        quantile=q,
                        train_x=feature_train.loc[train_mask[train_mask].index],
                        train_y=y.loc[train_mask],
                        pred_x=pred_x,
                        seed=random_seed,
                        return_model=return_model,
                    )
                    if return_model and result is not None:
                        pred, q50_model, q50_features = result
                    else:
                        pred = result
                except Exception:
                    pred = None
                row[col] = pred

        # Compute SHAP feature importance for this prediction (Phase 2 Priority 1)
        if q50_model is not None and q50_features is not None:
            try:
                importance_dict = compute_shap_feature_importance(
                    q50_model, q50_features, model_type="tree"
                )
                shap_output = format_feature_importance_for_output(importance_dict, top_n=5)
                row.update(shap_output)
            except Exception:
                # If SHAP fails, add empty columns
                for i in range(1, 6):
                    row[f"shap_feature_{i}"] = None
                    row[f"shap_value_{i}"] = None
        else:
            # No model available, add empty SHAP columns
            for i in range(1, 6):
                row[f"shap_feature_{i}"] = None
                row[f"shap_value_{i}"] = None

        rows.append(row)
    return pd.DataFrame(rows)


def add_dl_prediction_columns(
    df: pd.DataFrame,
    target: str,
    group_cols: list[str],
    model_classes: dict,
    quantiles: list[float],
    min_history: int,
    random_seed: int,
):
    if not model_classes:
        return df

    out = df.copy()
    out = out.sort_values(group_cols + ["report_day"]).reset_index(drop=True)
    pred_cols = _dl_prediction_columns(list(model_classes.keys()), quantiles)
    for col in pred_cols:
        out[col] = pd.NA

    min_history = max(int(min_history), 8)
    for _, part in out.groupby(group_cols, dropna=False):
        part = part.sort_values("report_day")
        idx_list = part.index.tolist()
        features = _ml_feature_frame(part, target=target)
        y = pd.to_numeric(part[target], errors="coerce")
        for i, row_idx in enumerate(idx_list):
            if pd.isna(y.loc[row_idx]):
                continue
            train_idx = [idx_list[j] for j in range(i) if pd.notna(y.loc[idx_list[j]])]
            if len(train_idx) < min_history:
                continue
            train_x = features.loc[train_idx]
            train_y = y.loc[train_idx]
            pred_x = features.loc[[row_idx]]
            for model_name, model_cls in model_classes.items():
                for q in quantiles:
                    col = f"pred_dl_{model_name}_{_quantile_suffix(q)}"
                    try:
                        pred = _fit_predict_dl_quantile(
                            model_name=model_name,
                            model_cls=model_cls,
                            quantile=q,
                            train_x=train_x,
                            train_y=train_y,
                            pred_x=pred_x,
                            seed=random_seed,
                        )
                    except Exception:
                        pred = None
                    if pred is not None:
                        out.at[row_idx, col] = pred
    return out


def build_next_day_dl_predictions(
    df: pd.DataFrame,
    target: str,
    group_cols: list[str],
    model_classes: dict,
    quantiles: list[float],
    min_history: int,
    random_seed: int,
):
    if not model_classes or df.empty:
        return pd.DataFrame()

    rows = []
    min_history = max(int(min_history), 8)
    for key, part in df.groupby(group_cols, dropna=False):
        part = part.sort_values("report_day").copy()
        y = pd.to_numeric(part[target], errors="coerce")
        train_mask = y.notna()
        if int(train_mask.sum()) < min_history:
            continue

        last_day = pd.to_datetime(part["report_day"].iloc[-1], errors="coerce")
        if pd.isna(last_day):
            continue
        next_day = last_day + timedelta(days=1)

        next_row = part.iloc[-1:].copy()
        next_row["report_day"] = next_day.date()
        next_row[target] = np.nan
        extended = pd.concat([part, next_row], ignore_index=True)

        feature_train = _ml_feature_frame(part, target=target)
        feature_ext = _ml_feature_frame(extended, target=target)
        pred_x = feature_ext.tail(1)

        row = {col: key[idx] for idx, col in enumerate(group_cols)}
        row["latest_report_day"] = last_day.date()
        row["predicted_for_day"] = next_day.date()
        for model_name, model_cls in model_classes.items():
            for q in quantiles:
                col = f"pred_dl_{model_name}_{_quantile_suffix(q)}"
                try:
                    pred = _fit_predict_dl_quantile(
                        model_name=model_name,
                        model_cls=model_cls,
                        quantile=q,
                        train_x=feature_train.loc[train_mask[train_mask].index],
                        train_y=y.loc[train_mask],
                        pred_x=pred_x,
                        seed=random_seed,
                    )
                except Exception:
                    pred = None
                row[col] = pred
        rows.append(row)
    return pd.DataFrame(rows)


def load_daily_frame(args):
    engine = create_engine(args.db_url, pool_pre_ping=True, future=True)
    where_sql, params = _build_where_clause(args)

    sql = text(
        f"""
        SELECT
            report_day,
            airline,
            origin,
            destination,
            cabin,
            total_change_events,
            price_events,
            availability_events
        FROM airline_intel.vw_route_airline_summary
        {where_sql}
        ORDER BY airline, origin, destination, cabin, report_day
        """
    )

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)

    # If reporting view has too little history, fallback to flight_offers-derived daily history.
    if df["report_day"].nunique() < 2:
        fallback = _load_from_offer_history(engine, args)
        if not fallback.empty and fallback["report_day"].nunique() >= 2:
            fallback = _apply_market_priors_safe(fallback)
            fallback = _apply_holiday_features_safe(fallback)
            return fallback
    df = _apply_market_priors_safe(df)
    df = _apply_holiday_features_safe(df)
    return df


def load_search_dynamic_frame(args):
    engine = create_engine(args.db_url, pool_pre_ping=True, future=True)
    where_sql, params = _build_offer_where_clause(args, alias="fo")
    sql = text(
        f"""
        WITH daily AS (
            SELECT
                DATE(fo.scraped_at) AS report_day,
                DATE(fo.departure) AS departure_day,
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
            {where_sql}
            GROUP BY
                DATE(fo.scraped_at),
                DATE(fo.departure),
                fo.airline,
                fo.origin,
                fo.destination,
                fo.cabin
        )
        SELECT
            report_day,
            departure_day,
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
            END AS soldout_rate,
            (departure_day - report_day) AS days_to_departure
        FROM daily
        ORDER BY
            airline,
            origin,
            destination,
            cabin,
            departure_day,
            report_day
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)
    df = _apply_market_priors_safe(df)
    df = _apply_holiday_features_safe(df)
    return df


def add_prediction_columns(df: pd.DataFrame, target: str, windows: list[int], group_cols: list[str]):
    df = df.copy()
    df = df.sort_values(group_cols + ["report_day"]).reset_index(drop=True)

    df["prev_actual_value"] = df.groupby(group_cols)[target].shift(1)
    df["pred_last_value"] = df["prev_actual_value"]
    for w in windows:
        col = f"pred_rolling_mean_{w}"
        df[col] = (
            df.groupby(group_cols)[target]
            .transform(lambda s: s.shift(1).rolling(window=w, min_periods=1).mean())
        )
    return df


def add_extra_baselines(df: pd.DataFrame, target: str, seasonal_lag: int, ewm_alpha: float, group_cols: list[str]):
    df = df.copy()
    alpha = max(0.0001, min(float(ewm_alpha), 1.0))
    lag = max(int(seasonal_lag), 1)
    df[f"pred_seasonal_naive_{lag}"] = (
        df.groupby(group_cols)[target]
        .shift(lag)
    )
    df[f"pred_ewm_alpha_{alpha:.2f}"] = (
        df.groupby(group_cols)[target]
        .transform(lambda s: s.shift(1).ewm(alpha=alpha, adjust=False).mean())
    )
    return df


def _safe_f1(y_true: pd.Series, y_pred: pd.Series):
    tp = int(((y_true == 1) & (y_pred == 1)).sum())
    fp = int(((y_true == 0) & (y_pred == 1)).sum())
    fn = int(((y_true == 1) & (y_pred == 0)).sum())
    if tp == 0 and fp == 0 and fn == 0:
        return None
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    if precision + recall == 0:
        return 0.0
    return 2 * precision * recall / (precision + recall)


def _direction_metrics(part: pd.DataFrame, target: str, pred_col: str):
    sub = part[
        part[pred_col].notna()
        & part[target].notna()
        & part["prev_actual_value"].notna()
    ].copy()
    n_dir = len(sub)
    if n_dir == 0:
        return {
            "n_directional": 0,
            "directional_accuracy_pct": None,
            "f1_up": None,
            "f1_down": None,
            "f1_macro": None,
        }

    actual_delta = sub[target] - sub["prev_actual_value"]
    pred_delta = sub[pred_col] - sub["prev_actual_value"]
    actual_sign = actual_delta.apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    pred_sign = pred_delta.apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))

    directional_accuracy = float((actual_sign == pred_sign).mean() * 100.0)

    # F1 for UP class and DOWN class, then macro average.
    actual_up = (actual_sign == 1).astype(int)
    pred_up = (pred_sign == 1).astype(int)
    f1_up = _safe_f1(actual_up, pred_up)

    actual_down = (actual_sign == -1).astype(int)
    pred_down = (pred_sign == -1).astype(int)
    f1_down = _safe_f1(actual_down, pred_down)

    f1_values = [v for v in (f1_up, f1_down) if v is not None]
    f1_macro = float(sum(f1_values) / len(f1_values)) if f1_values else None

    return {
        "n_directional": n_dir,
        "directional_accuracy_pct": directional_accuracy,
        "f1_up": f1_up,
        "f1_down": f1_down,
        "f1_macro": f1_macro,
    }


def _compute_metric_rows(eval_df: pd.DataFrame, target: str, pred_cols: list[str]):
    rows = []
    for col in pred_cols:
        part = eval_df[eval_df[col].notna() & eval_df[target].notna()].copy()
        n = len(part)
        if n == 0:
            rows.append(
                {
                    "model": col,
                    "n": 0,
                    "mae": None,
                    "rmse": None,
                    "mape_pct": None,
                    "smape_pct": None,
                    "n_directional": 0,
                    "directional_accuracy_pct": None,
                    "f1_up": None,
                    "f1_down": None,
                    "f1_macro": None,
                }
            )
            continue

        abs_err = (part[col] - part[target]).abs()
        mae = float(abs_err.mean())
        rmse = float(((part[col] - part[target]) ** 2).mean() ** 0.5)

        non_zero_actual = part[target].replace(0, pd.NA)
        ape = (abs_err / non_zero_actual).dropna()
        mape_pct = float(ape.mean() * 100) if len(ape) > 0 else None

        denom = (part[col].abs() + part[target].abs()) / 2
        smape = (abs_err / denom.replace(0, pd.NA)).dropna()
        smape_pct = float(smape.mean() * 100) if len(smape) > 0 else None

        row = {
            "model": col,
            "n": n,
            "mae": mae,
            "rmse": rmse,
            "mape_pct": mape_pct,
            "smape_pct": smape_pct,
        }
        row.update(_direction_metrics(eval_df, target=target, pred_col=col))
        rows.append(row)
    return rows


def evaluate_predictions(df: pd.DataFrame, target: str, pred_cols: list[str], group_cols: list[str]):
    overall_rows = _compute_metric_rows(df, target, pred_cols)
    overall_eval = pd.DataFrame(overall_rows)

    route_rows = []
    for group_key, part in df.groupby(group_cols, dropna=False):
        metrics = _compute_metric_rows(part, target, pred_cols)
        for m in metrics:
            row = {col: group_key[idx] for idx, col in enumerate(group_cols)}
            row.update(m)
            route_rows.append(row)
    route_eval = pd.DataFrame(route_rows)
    return overall_eval, route_eval


def build_next_day_predictions(
    df: pd.DataFrame,
    target: str,
    windows: list[int],
    min_history: int,
    seasonal_lag: int,
    ewm_alpha: float,
    group_cols: list[str],
):
    rows = []
    for group_key, part in df.groupby(group_cols, dropna=False):
        part = part.sort_values("report_day")
        if len(part) < min_history:
            continue

        y = part[target].astype(float)
        last_day = part["report_day"].iloc[-1]
        if isinstance(last_day, str):
            last_day = datetime.fromisoformat(last_day).date()

        row = {
            "latest_report_day": last_day,
            "predicted_for_day": last_day + timedelta(days=1),
            "history_days": len(part),
            "latest_actual_value": float(y.iloc[-1]),
            "pred_last_value": float(y.iloc[-1]),
        }
        for idx, col in enumerate(group_cols):
            row[col] = group_key[idx]
        if "departure_day" in part.columns and pd.notna(part["departure_day"].iloc[-1]):
            dep_day = part["departure_day"].iloc[-1]
            if isinstance(dep_day, str):
                dep_day = datetime.fromisoformat(dep_day).date()
            row["days_to_departure_latest"] = int((dep_day - last_day).days)
            row["days_to_departure_next_search"] = int((dep_day - (last_day + timedelta(days=1))).days)

        for w in windows:
            row[f"pred_rolling_mean_{w}"] = float(y.tail(w).mean())
        if len(y) >= seasonal_lag:
            row[f"pred_seasonal_naive_{seasonal_lag}"] = float(y.iloc[-seasonal_lag])
        else:
            row[f"pred_seasonal_naive_{seasonal_lag}"] = None
        alpha = max(0.0001, min(float(ewm_alpha), 1.0))
        row[f"pred_ewm_alpha_{alpha:.2f}"] = float(y.ewm(alpha=alpha, adjust=False).mean().iloc[-1])

        rows.append(row)

    return pd.DataFrame(rows)


def _normalize_report_day(df: pd.DataFrame):
    out = df.copy()
    out["report_day"] = pd.to_datetime(out["report_day"], errors="coerce").dt.date
    return out


def build_trend_summary(df: pd.DataFrame, target: str, group_cols: list[str]):
    if df.empty:
        return pd.DataFrame()
    work = _normalize_report_day(df)
    if "departure_day" in work.columns:
        work["departure_day"] = pd.to_datetime(work["departure_day"], errors="coerce").dt.date
    work = work.sort_values(group_cols + ["report_day"])
    agg = (
        work.groupby(group_cols, dropna=False)
        .agg(
            first_report_day=("report_day", "min"),
            last_report_day=("report_day", "max"),
            obs_count=(target, "count"),
            first_value=(target, "first"),
            last_value=(target, "last"),
            min_value=(target, "min"),
            max_value=(target, "max"),
            avg_value=(target, "mean"),
            std_value=(target, "std"),
        )
        .reset_index()
    )
    agg["delta_value"] = agg["last_value"] - agg["first_value"]
    agg["delta_pct"] = (
        (agg["delta_value"] / agg["first_value"].replace(0, pd.NA)) * 100.0
    )
    agg["trend_direction"] = agg["delta_value"].apply(
        lambda v: "UP" if v > 0 else ("DOWN" if v < 0 else "FLAT")
    )
    if "departure_day" in agg.columns:
        agg["days_to_departure_latest"] = (
            pd.to_datetime(agg["departure_day"], errors="coerce")
            - pd.to_datetime(agg["last_report_day"], errors="coerce")
        ).dt.days
    return agg


def _best_model_from_eval(eval_rows: pd.DataFrame, *, metric: str = "mae", min_coverage_ratio: float = 0.8):
    if eval_rows.empty:
        return None
    metric = "rmse" if str(metric or "").lower() == "rmse" else "mae"
    min_coverage_ratio = float(max(0.0, min(1.0, min_coverage_ratio)))
    candidates = eval_rows[(eval_rows["n"] > 0) & eval_rows[metric].notna()].copy()
    if candidates.empty:
        return None
    max_n = int(candidates["n"].max())
    keep_n = int(max_n * min_coverage_ratio)
    covered = candidates[candidates["n"] >= keep_n].copy()
    if not covered.empty:
        candidates = covered
    sort_cols = [metric, "rmse" if metric != "rmse" else "mae", "model"]
    candidates = candidates.sort_values(sort_cols).reset_index(drop=True)
    return str(candidates.iloc[0]["model"])


def build_winner_table(
    eval_rows: pd.DataFrame,
    *,
    scope_cols: list[str],
    metric: str = "mae",
    min_coverage_ratio: float = 0.8,
    extra_group_cols: list[str] | None = None,
):
    if eval_rows is None or eval_rows.empty:
        return pd.DataFrame()

    metric = "rmse" if str(metric or "").lower() == "rmse" else "mae"
    extra_group_cols = extra_group_cols or []
    work = eval_rows.copy()
    required = [col for col in scope_cols + extra_group_cols if col in work.columns]
    if not required or "model" not in work.columns or metric not in work.columns or "n" not in work.columns:
        return pd.DataFrame()

    winners: list[dict[str, Any]] = []
    for group_key, part in work.groupby(required, dropna=False):
        part = part[(part["n"] > 0) & part[metric].notna()].copy()
        if part.empty:
            continue
        max_n = int(part["n"].max())
        keep_n = int(max_n * float(max(0.0, min(1.0, min_coverage_ratio))))
        covered = part[part["n"] >= keep_n].copy()
        if not covered.empty:
            part = covered
        sort_cols = [metric, "rmse" if metric != "rmse" and "rmse" in part.columns else metric, "model"]
        winner = part.sort_values(sort_cols).iloc[0]
        row = {}
        if len(required) == 1:
            row[required[0]] = group_key
        else:
            for idx, col in enumerate(required):
                row[col] = group_key[idx]
        row.update(
            {
                "winner_model": winner["model"],
                "winner_metric": metric,
                "winner_n": winner["n"],
                "winner_mae": winner.get("mae"),
                "winner_rmse": winner.get("rmse"),
                "winner_directional_accuracy_pct": winner.get("directional_accuracy_pct"),
                "winner_f1_macro": winner.get("f1_macro"),
                "max_candidate_n": max_n,
                "coverage_threshold_n": keep_n,
                "candidate_models": int(len(part)),
            }
        )
        winners.append(row)
    return pd.DataFrame(winners)


def _build_backtest_splits(
    min_day,
    max_day,
    train_days: int,
    val_days: int,
    test_days: int,
    step_days: int,
):
    train_days = max(int(train_days), 1)
    val_days = max(int(val_days), 1)
    test_days = max(int(test_days), 1)
    step_days = max(int(step_days), 1)

    first_test_start = min_day + timedelta(days=train_days + val_days)
    splits = []
    split_id = 0
    test_start = first_test_start
    while test_start + timedelta(days=test_days - 1) <= max_day:
        split_id += 1
        train_start = test_start - timedelta(days=train_days + val_days)
        train_end = test_start - timedelta(days=val_days + 1)
        val_start = train_end + timedelta(days=1)
        val_end = test_start - timedelta(days=1)
        test_end = test_start + timedelta(days=test_days - 1)
        splits.append(
            {
                "split_id": split_id,
                "train_start": train_start,
                "train_end": train_end,
                "val_start": val_start,
                "val_end": val_end,
                "test_start": test_start,
                "test_end": test_end,
            }
        )
        test_start = test_start + timedelta(days=step_days)
    return splits


def _derive_auto_backtest_windows(total_days: int):
    total_days = max(int(total_days), 0)
    if total_days < 10:
        return None

    # Keep fixed contiguous train/val/test windows while adapting to available range.
    train_days = max(5, int(total_days * 0.6))
    val_days = max(2, int(total_days * 0.2))
    test_days = max(2, int(total_days * 0.2))

    # Trim down to fit, preserving priority: test >= val >= 2, train >= 5.
    while train_days + val_days + test_days > total_days and train_days > 5:
        train_days -= 1
    while train_days + val_days + test_days > total_days and val_days > 2:
        val_days -= 1
    while train_days + val_days + test_days > total_days and test_days > 2:
        test_days -= 1

    if train_days + val_days + test_days > total_days:
        return None
    return {
        "train_days": int(train_days),
        "val_days": int(val_days),
        "test_days": int(test_days),
        "step_days": int(max(1, test_days)),
    }


def run_rolling_backtest(
    history_df: pd.DataFrame,
    target: str,
    pred_cols: list[str],
    train_days: int,
    val_days: int,
    test_days: int,
    step_days: int,
    group_cols: list[str],
    selection_metric: str = "mae",
    model_min_coverage_ratio: float = 0.8,
):
    df = _normalize_report_day(history_df)
    df = df[df["report_day"].notna()].copy()
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    min_day = df["report_day"].min()
    max_day = df["report_day"].max()
    total_days = int((max_day - min_day).days + 1)
    splits = _build_backtest_splits(
        min_day=min_day,
        max_day=max_day,
        train_days=train_days,
        val_days=val_days,
        test_days=test_days,
        step_days=step_days,
    )
    auto_window = None
    if not splits:
        auto_window = _derive_auto_backtest_windows(total_days)
        if auto_window:
            splits = _build_backtest_splits(
                min_day=min_day,
                max_day=max_day,
                train_days=auto_window["train_days"],
                val_days=auto_window["val_days"],
                test_days=auto_window["test_days"],
                step_days=auto_window["step_days"],
            )
    if not splits:
        meta = {
            "status": "insufficient_range_for_backtest",
            "min_day": str(min_day),
            "max_day": str(max_day),
            "total_days": int(total_days),
            "train_days": int(train_days),
            "val_days": int(val_days),
            "test_days": int(test_days),
            "step_days": int(step_days),
            "split_count": 0,
        }
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), meta

    metric_rows = []
    split_rows = []
    route_metric_rows = []

    for split in splits:
        train_mask = (df["report_day"] >= split["train_start"]) & (df["report_day"] <= split["train_end"])
        val_mask = (df["report_day"] >= split["val_start"]) & (df["report_day"] <= split["val_end"])
        test_mask = (df["report_day"] >= split["test_start"]) & (df["report_day"] <= split["test_end"])

        train_df = df[train_mask].copy()
        val_df = df[val_mask].copy()
        test_df = df[test_mask].copy()

        val_eval, val_route_eval = evaluate_predictions(val_df, target=target, pred_cols=pred_cols, group_cols=group_cols)
        test_eval, test_route_eval = evaluate_predictions(test_df, target=target, pred_cols=pred_cols, group_cols=group_cols)
        selected_model = _best_model_from_eval(
            val_eval,
            metric=selection_metric,
            min_coverage_ratio=model_min_coverage_ratio,
        )

        split_rows.append(
            {
                "split_id": split["split_id"],
                "train_start": split["train_start"],
                "train_end": split["train_end"],
                "val_start": split["val_start"],
                "val_end": split["val_end"],
                "test_start": split["test_start"],
                "test_end": split["test_end"],
                "train_rows": int(len(train_df)),
                "val_rows": int(len(val_df)),
                "test_rows": int(len(test_df)),
                "selected_model": selected_model,
            }
        )

        for dataset_name, eval_frame in (("val", val_eval), ("test", test_eval)):
            if eval_frame.empty:
                continue
            for _, row in eval_frame.iterrows():
                metric_rows.append(
                    {
                        "split_id": split["split_id"],
                        "dataset": dataset_name,
                        "model": row["model"],
                        "selected_on_val": bool(selected_model and row["model"] == selected_model),
                        "n": row["n"],
                        "mae": row["mae"],
                        "rmse": row["rmse"],
                        "mape_pct": row["mape_pct"],
                        "smape_pct": row["smape_pct"],
                        "n_directional": row["n_directional"],
                        "directional_accuracy_pct": row["directional_accuracy_pct"],
                        "f1_up": row["f1_up"],
                        "f1_down": row["f1_down"],
                        "f1_macro": row["f1_macro"],
                        "train_start": split["train_start"],
                        "train_end": split["train_end"],
                        "val_start": split["val_start"],
                        "val_end": split["val_end"],
                        "test_start": split["test_start"],
                        "test_end": split["test_end"],
                    }
                )

        for dataset_name, route_frame in (("val", val_route_eval), ("test", test_route_eval)):
            if route_frame.empty:
                continue
            for _, row in route_frame.iterrows():
                route_metric_rows.append(
                    {
                        "split_id": split["split_id"],
                        "dataset": dataset_name,
                        **{col: row.get(col) for col in group_cols},
                        "model": row["model"],
                        "n": row["n"],
                        "mae": row["mae"],
                        "rmse": row["rmse"],
                        "mape_pct": row["mape_pct"],
                        "smape_pct": row["smape_pct"],
                        "n_directional": row["n_directional"],
                        "directional_accuracy_pct": row["directional_accuracy_pct"],
                        "f1_up": row["f1_up"],
                        "f1_down": row["f1_down"],
                        "f1_macro": row["f1_macro"],
                        "train_start": split["train_start"],
                        "train_end": split["train_end"],
                        "val_start": split["val_start"],
                        "val_end": split["val_end"],
                        "test_start": split["test_start"],
                        "test_end": split["test_end"],
                    }
                )

    split_df = pd.DataFrame(split_rows)
    metric_df = pd.DataFrame(metric_rows)
    route_metric_df = pd.DataFrame(route_metric_rows)
    meta = {
        "status": "ok_auto_window" if auto_window else "ok",
        "min_day": str(min_day),
        "max_day": str(max_day),
        "total_days": int(total_days),
        "requested_train_days": int(train_days),
        "requested_val_days": int(val_days),
        "requested_test_days": int(test_days),
        "requested_step_days": int(step_days),
        "train_days": int(auto_window["train_days"]) if auto_window else int(train_days),
        "val_days": int(auto_window["val_days"]) if auto_window else int(val_days),
        "test_days": int(auto_window["test_days"]) if auto_window else int(test_days),
        "step_days": int(auto_window["step_days"]) if auto_window else int(step_days),
        "split_count": int(len(split_rows)),
        "metric_rows": int(len(metric_rows)),
        "route_metric_rows": int(len(route_metric_rows)),
        "selection_metric": str(selection_metric),
        "model_min_coverage_ratio": float(model_min_coverage_ratio),
    }
    return metric_df, split_df, route_metric_df, meta


def main():
    args = parse_args()
    windows = _parse_windows(args.rolling_windows)
    if not windows:
        raise SystemExit("No valid rolling windows. Example: --rolling-windows 3,7")
    requested_ml_models = _parse_ml_models(args.ml_models)
    ml_quantiles = _parse_quantiles(args.ml_quantiles)
    ml_model_classes, ml_skipped = _resolve_ml_models(requested_ml_models)
    active_ml_models = list(ml_model_classes.keys())
    requested_dl_models = _parse_ml_models(args.dl_models)
    dl_quantiles = _parse_quantiles(args.dl_quantiles)
    dl_model_classes, dl_skipped = _resolve_dl_models(requested_dl_models)
    active_dl_models = list(dl_model_classes.keys())

    if args.series_mode == "event_daily":
        allowed_targets = EVENT_TARGETS
        group_cols = BASE_GROUP_COLS
        df = load_daily_frame(args)
    else:
        allowed_targets = SEARCH_TARGETS
        group_cols = SEARCH_GROUP_COLS
        df = load_search_dynamic_frame(args)

    if args.target_column not in allowed_targets:
        raise SystemExit(
            f"Target '{args.target_column}' is not valid for mode '{args.series_mode}'. "
            f"Allowed: {sorted(allowed_targets)}"
        )

    if df.empty:
        print("No rows found for prediction input.")
        return 0

    target = args.target_column
    if target not in df.columns:
        raise SystemExit(f"Target column not found: {target}")

    history_df = add_prediction_columns(df, target=target, windows=windows, group_cols=group_cols)
    history_df = add_extra_baselines(
        history_df,
        target=target,
        seasonal_lag=args.seasonal_lag,
        ewm_alpha=args.ewm_alpha,
        group_cols=group_cols,
    )
    if ml_model_classes:
        history_df = add_ml_prediction_columns(
            history_df,
            target=target,
            group_cols=group_cols,
            model_classes=ml_model_classes,
            quantiles=ml_quantiles,
            min_history=args.ml_min_history,
            random_seed=args.ml_random_seed,
        )
    if dl_model_classes:
        history_df = add_dl_prediction_columns(
            history_df,
            target=target,
            group_cols=group_cols,
            model_classes=dl_model_classes,
            quantiles=dl_quantiles,
            min_history=args.dl_min_history,
            random_seed=args.dl_random_seed,
        )

    pred_cols = (
        ["pred_last_value"]
        + [f"pred_rolling_mean_{w}" for w in windows]
        + [f"pred_seasonal_naive_{max(args.seasonal_lag, 1)}"]
        + [f"pred_ewm_alpha_{max(0.0001, min(float(args.ewm_alpha), 1.0)):.2f}"]
    )
    if ml_model_classes:
        pred_cols += _ml_prediction_columns(active_ml_models, ml_quantiles)
    if dl_model_classes:
        pred_cols += _dl_prediction_columns(active_dl_models, dl_quantiles)

    history_df = _clip_prediction_columns(history_df, target=target, pred_cols=pred_cols)

    overall_eval, route_eval = evaluate_predictions(history_df, target=target, pred_cols=pred_cols, group_cols=group_cols)
    next_day_df = build_next_day_predictions(
        history_df,
        target=target,
        windows=windows,
        min_history=args.min_history,
        seasonal_lag=max(args.seasonal_lag, 1),
        ewm_alpha=max(0.0001, min(float(args.ewm_alpha), 1.0)),
        group_cols=group_cols,
    )
    if ml_model_classes:
        next_ml_df = build_next_day_ml_predictions(
            history_df,
            target=target,
            group_cols=group_cols,
            model_classes=ml_model_classes,
            quantiles=ml_quantiles,
            min_history=args.ml_min_history,
            random_seed=args.ml_random_seed,
        )
        if not next_ml_df.empty:
            if next_day_df.empty:
                next_day_df = next_ml_df.copy()
            else:
                merge_cols = [c for c in group_cols if c in next_ml_df.columns and c in next_day_df.columns]
                merge_cols += [c for c in ["latest_report_day", "predicted_for_day"] if c in next_ml_df.columns and c in next_day_df.columns]
                next_day_df = next_day_df.merge(next_ml_df, on=merge_cols, how="left", suffixes=("", "_ml"))
    if dl_model_classes:
        next_dl_df = build_next_day_dl_predictions(
            history_df,
            target=target,
            group_cols=group_cols,
            model_classes=dl_model_classes,
            quantiles=dl_quantiles,
            min_history=args.dl_min_history,
            random_seed=args.dl_random_seed,
        )
        if not next_dl_df.empty:
            if next_day_df.empty:
                next_day_df = next_dl_df.copy()
            else:
                merge_cols = [c for c in group_cols if c in next_dl_df.columns and c in next_day_df.columns]
                merge_cols += [c for c in ["latest_report_day", "predicted_for_day"] if c in next_dl_df.columns and c in next_day_df.columns]
                next_day_df = next_day_df.merge(next_dl_df, on=merge_cols, how="left", suffixes=("", "_dl"))
    if not next_day_df.empty:
        next_pred_cols = [c for c in pred_cols if c in next_day_df.columns]
        next_day_df = _clip_prediction_columns(next_day_df, target=target, pred_cols=next_pred_cols)
        # Add prediction confidence bands
        next_day_df = add_prediction_confidence(next_day_df, target=target, route_eval=route_eval, group_cols=group_cols)

    # SHAP feature importance is computed in build_next_day_ml_predictions() (Phase 2 Priority 1)
    # Each prediction row includes shap_feature_1-5 and shap_value_1-5 columns

    trend_df = build_trend_summary(history_df, target=target, group_cols=group_cols)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    history_path = out_dir / f"prediction_history_{target}_{ts}.csv"
    eval_path = out_dir / f"prediction_eval_{target}_{ts}.csv"
    route_eval_path = out_dir / f"prediction_eval_by_route_{target}_{ts}.csv"
    route_winners_path = out_dir / f"prediction_route_winners_{target}_{ts}.csv"
    next_day_path = out_dir / f"prediction_next_day_{target}_{ts}.csv"
    trend_path = out_dir / f"prediction_trend_{target}_{ts}.csv"
    backtest_eval_path = out_dir / f"prediction_backtest_eval_{target}_{ts}.csv"
    backtest_route_eval_path = out_dir / f"prediction_backtest_eval_by_route_{target}_{ts}.csv"
    backtest_route_winners_path = out_dir / f"prediction_backtest_route_winners_{target}_{ts}.csv"
    backtest_splits_path = out_dir / f"prediction_backtest_splits_{target}_{ts}.csv"
    backtest_meta_path = out_dir / f"prediction_backtest_meta_{target}_{ts}.json"

    route_winners_df = build_winner_table(
        route_eval,
        scope_cols=group_cols,
        metric=args.backtest_selection_metric,
        min_coverage_ratio=args.backtest_model_min_coverage_ratio,
    )
    history_df.to_csv(history_path, index=False)
    overall_eval.to_csv(eval_path, index=False)
    route_eval.to_csv(route_eval_path, index=False)
    route_winners_df.to_csv(route_winners_path, index=False)
    next_day_df.to_csv(next_day_path, index=False)
    trend_df.to_csv(trend_path, index=False)

    backtest_metric_rows = 0
    backtest_route_metric_rows = 0
    backtest_split_rows = 0
    backtest_meta = {
        "status": "disabled",
        "reason": "--disable-backtest",
    }
    backtest_route_winners_df = pd.DataFrame()
    if not args.disable_backtest:
        backtest_eval_df, backtest_splits_df, backtest_route_eval_df, backtest_meta = run_rolling_backtest(
            history_df=history_df,
            target=target,
            pred_cols=pred_cols,
            train_days=args.backtest_train_days,
            val_days=args.backtest_val_days,
            test_days=args.backtest_test_days,
            step_days=args.backtest_step_days,
            group_cols=group_cols,
            selection_metric=args.backtest_selection_metric,
            model_min_coverage_ratio=args.backtest_model_min_coverage_ratio,
        )
        backtest_metric_rows = int(len(backtest_eval_df))
        backtest_route_metric_rows = int(len(backtest_route_eval_df))
        backtest_split_rows = int(len(backtest_splits_df))
        backtest_route_winners_df = build_winner_table(
            backtest_route_eval_df,
            scope_cols=group_cols,
            metric=args.backtest_selection_metric,
            min_coverage_ratio=args.backtest_model_min_coverage_ratio,
            extra_group_cols=["dataset"],
        )
        backtest_eval_df.to_csv(backtest_eval_path, index=False)
        backtest_route_eval_df.to_csv(backtest_route_eval_path, index=False)
        backtest_route_winners_df.to_csv(backtest_route_winners_path, index=False)
        backtest_splits_df.to_csv(backtest_splits_path, index=False)
        with backtest_meta_path.open("w", encoding="utf-8") as f:
            json.dump(
                {
                    "created_at_utc": datetime.now(timezone.utc).isoformat(),
                    "target_column": target,
                    "filters": {
                        "start_date": args.start_date,
                        "end_date": args.end_date,
                        "airline": args.airline,
                        "origin": args.origin,
                        "destination": args.destination,
                        "cabin": args.cabin,
                    },
                    "prediction_models": pred_cols,
                    "route_winner_rows": int(len(route_winners_df)),
                    "ml_requested_models": requested_ml_models,
                    "ml_active_models": active_ml_models,
                    "ml_skipped_models": ml_skipped,
                    "ml_quantiles": ml_quantiles,
                    "dl_requested_models": requested_dl_models,
                    "dl_active_models": active_dl_models,
                    "dl_skipped_models": dl_skipped,
                    "dl_quantiles": dl_quantiles,
                    "backtest": backtest_meta,
                    "backtest_route_metric_rows": int(len(backtest_route_eval_df)),
                    "backtest_route_winner_rows": int(len(backtest_route_winners_df)),
                    "backtest_selection_metric": args.backtest_selection_metric,
                    "backtest_model_min_coverage_ratio": args.backtest_model_min_coverage_ratio,
                },
                f,
                indent=2,
            )

    print(f"history_rows={len(history_df)} -> {history_path}")
    print(f"overall_eval_rows={len(overall_eval)} -> {eval_path}")
    print(f"route_eval_rows={len(route_eval)} -> {route_eval_path}")
    print(f"route_winner_rows={len(route_winners_df)} -> {route_winners_path}")
    print(f"next_day_rows={len(next_day_df)} -> {next_day_path}")
    print(f"trend_rows={len(trend_df)} -> {trend_path}")
    print(f"ml_requested_models={requested_ml_models}")
    print(f"ml_active_models={active_ml_models}")
    if ml_skipped:
        print(f"ml_skipped_models={ml_skipped}")
    print(f"dl_requested_models={requested_dl_models}")
    print(f"dl_active_models={active_dl_models}")
    if dl_skipped:
        print(f"dl_skipped_models={dl_skipped}")
    if args.disable_backtest:
        print("backtest=disabled")
    else:
        print(f"backtest_eval_rows={backtest_metric_rows} -> {backtest_eval_path}")
        print(f"backtest_route_eval_rows={backtest_route_metric_rows} -> {backtest_route_eval_path}")
        print(f"backtest_route_winner_rows={len(backtest_route_winners_df)} -> {backtest_route_winners_path}")
        print(f"backtest_split_rows={backtest_split_rows} -> {backtest_splits_path}")
        print(f"backtest_meta -> {backtest_meta_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
