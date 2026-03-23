"""
Booking curve feature engineering for aviation inventory prediction.

Captures booking behavior patterns based on advance purchase windows,
which are critical for pricing and capacity forecasting.
"""

from datetime import datetime, timedelta
import pandas as pd
import numpy as np


def add_booking_curve_features(df: pd.DataFrame, search_date_col: str = "event_day", departure_date_col: str = "departure_day"):
    """
    Add booking curve features based on days to departure.

    Args:
        df: DataFrame with search_date and departure_date columns
        search_date_col: Name of search/event date column
        departure_date_col: Name of departure date column

    Returns:
        DataFrame with added booking curve features:
        - booking_advance_days: Days from search to departure
        - booking_window_0_7: Binary indicator for 0-7 days advance
        - booking_window_8_14: Binary indicator for 8-14 days advance
        - booking_window_15_30: Binary indicator for 15-30 days advance
        - booking_window_31_60: Binary indicator for 31-60 days advance
        - booking_window_61_90: Binary indicator for 61-90 days advance
        - booking_window_91plus: Binary indicator for 90+ days advance
        - is_peak_booking_window: Binary indicator for peak booking (30-45 days)
        - is_late_booking: Binary indicator for last-minute booking (<=7 days)
        - is_early_booking: Binary indicator for early booking (>90 days)
        - booking_curve_phase: Categorical (late/standard/peak/early)
    """
    df = df.copy()

    # Ensure date columns are datetime
    df[search_date_col] = pd.to_datetime(df[search_date_col])
    df[departure_date_col] = pd.to_datetime(df[departure_date_col])

    # Calculate days to departure (booking advance)
    df["booking_advance_days"] = (df[departure_date_col] - df[search_date_col]).dt.days

    # Booking window buckets (inclusive boundaries)
    df["booking_window_0_7"] = (df["booking_advance_days"] <= 7).astype(int)
    df["booking_window_8_14"] = ((df["booking_advance_days"] >= 8) & (df["booking_advance_days"] <= 14)).astype(int)
    df["booking_window_15_30"] = ((df["booking_advance_days"] >= 15) & (df["booking_advance_days"] <= 30)).astype(int)
    df["booking_window_31_60"] = ((df["booking_advance_days"] >= 31) & (df["booking_advance_days"] <= 60)).astype(int)
    df["booking_window_61_90"] = ((df["booking_advance_days"] >= 61) & (df["booking_advance_days"] <= 90)).astype(int)
    df["booking_window_91plus"] = (df["booking_advance_days"] >= 91).astype(int)

    # Special indicators
    df["is_peak_booking_window"] = ((df["booking_advance_days"] >= 30) & (df["booking_advance_days"] <= 45)).astype(int)
    df["is_late_booking"] = (df["booking_advance_days"] <= 7).astype(int)
    df["is_early_booking"] = (df["booking_advance_days"] > 90).astype(int)

    # Booking curve phase (categorical encoded as integer)
    # 0 = late (0-7 days), 1 = standard (8-30 days), 2 = peak (31-60 days), 3 = early (60+ days)
    df["booking_curve_phase"] = 1  # Default: standard
    df.loc[df["booking_advance_days"] <= 7, "booking_curve_phase"] = 0  # late
    df.loc[(df["booking_advance_days"] > 30) & (df["booking_advance_days"] <= 60), "booking_curve_phase"] = 2  # peak
    df.loc[df["booking_advance_days"] > 60, "booking_curve_phase"] = 3  # early

    # Normalized booking progress (0 to 1, where 1 = departure day)
    # Useful for modeling booking curve as time series
    max_advance = df["booking_advance_days"].max()
    if max_advance > 0:
        df["booking_progress"] = 1 - (df["booking_advance_days"] / max_advance)
    else:
        df["booking_progress"] = 1.0

    # Log-transformed advance days (handles long tail better)
    df["log_booking_advance"] = np.log1p(df["booking_advance_days"].clip(lower=0))

    return df


def get_booking_curve_feature_columns():
    """
    Get list of all booking curve feature column names.

    Returns:
        list: Column names for booking curve features
    """
    return [
        "booking_advance_days",
        "booking_window_0_7",
        "booking_window_8_14",
        "booking_window_15_30",
        "booking_window_31_60",
        "booking_window_61_90",
        "booking_window_91plus",
        "is_peak_booking_window",
        "is_late_booking",
        "is_early_booking",
        "booking_curve_phase",
        "booking_progress",
        "log_booking_advance",
    ]


def add_booking_curve_aggregates(df: pd.DataFrame, group_cols: list):
    """
    Add aggregate booking curve statistics per route/group.

    Args:
        df: DataFrame with booking curve features
        group_cols: Columns to group by (e.g., ['airline', 'origin', 'destination'])

    Returns:
        DataFrame with added aggregate features:
        - avg_booking_advance: Average days to departure for this route
        - std_booking_advance: Std dev of days to departure
        - pct_late_bookings: Percentage of bookings within 7 days
        - pct_peak_bookings: Percentage of bookings in peak window (30-45 days)
    """
    df = df.copy()

    if "booking_advance_days" not in df.columns:
        raise ValueError("Must call add_booking_curve_features() first")

    # Calculate aggregates per group
    agg_stats = df.groupby(group_cols).agg({
        "booking_advance_days": ["mean", "std"],
        "is_late_booking": "mean",
        "is_peak_booking_window": "mean",
    }).reset_index()

    # Flatten column names
    agg_stats.columns = group_cols + [
        "avg_booking_advance",
        "std_booking_advance",
        "pct_late_bookings",
        "pct_peak_bookings",
    ]

    # Fill NaN std with 0
    agg_stats["std_booking_advance"] = agg_stats["std_booking_advance"].fillna(0)

    # Merge back to original dataframe
    df = df.merge(agg_stats, on=group_cols, how="left")

    return df


def identify_booking_curve_anomalies(df: pd.DataFrame, group_cols: list, z_threshold: float = 3.0):
    """
    Identify anomalous booking patterns (unusually early or late bookings).

    Args:
        df: DataFrame with booking curve features
        group_cols: Columns to group by
        z_threshold: Z-score threshold for anomaly detection

    Returns:
        DataFrame with added column:
        - is_booking_anomaly: Binary indicator for anomalous booking timing
    """
    df = df.copy()

    if "avg_booking_advance" not in df.columns:
        df = add_booking_curve_aggregates(df, group_cols)

    # Calculate z-score for booking advance relative to route average
    df["booking_z_score"] = 0.0
    mask = df["std_booking_advance"] > 0
    df.loc[mask, "booking_z_score"] = (
        (df.loc[mask, "booking_advance_days"] - df.loc[mask, "avg_booking_advance"]) /
        df.loc[mask, "std_booking_advance"]
    )

    # Mark anomalies
    df["is_booking_anomaly"] = (np.abs(df["booking_z_score"]) > z_threshold).astype(int)

    return df
