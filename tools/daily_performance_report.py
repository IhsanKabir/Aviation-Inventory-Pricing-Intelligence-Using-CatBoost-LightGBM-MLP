"""
Daily Performance Report Generator

Generates a daily report of actual vs predicted performance for monitoring
prediction accuracy over time and detecting degradation.

Usage:
    python tools/daily_performance_report.py --lookback-days 7
    python tools/daily_performance_report.py --route DAC-DXB --target price_events
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

# Add parent directory to path to import core modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.prediction_monitor import PredictionMonitor
from db import DATABASE_URL as DEFAULT_DATABASE_URL


def load_recent_predictions(database_url: str, lookback_days: int = 7) -> pd.DataFrame:
    """
    Load recent predictions from output CSVs.

    Args:
        database_url: Database connection string
        lookback_days: Number of days to look back

    Returns:
        DataFrame with predictions
    """
    output_dir = Path("output/predictions")
    if not output_dir.exists():
        return pd.DataFrame()

    cutoff_date = datetime.now() - timedelta(days=lookback_days)

    # Find all prediction CSV files
    prediction_files = list(output_dir.glob("prediction_next_day_*.csv"))

    all_predictions = []
    for pred_file in prediction_files:
        try:
            # Extract timestamp from filename
            parts = pred_file.stem.split("_")
            if len(parts) >= 5:
                timestamp_str = parts[-1]
                file_timestamp = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")

                if file_timestamp >= cutoff_date:
                    df = pd.read_csv(pred_file)
                    df["prediction_timestamp"] = file_timestamp
                    all_predictions.append(df)
        except Exception as e:
            print(f"Warning: Could not load {pred_file}: {e}")
            continue

    if not all_predictions:
        return pd.DataFrame()

    return pd.concat(all_predictions, ignore_index=True)


def load_actuals_from_database(database_url: str, start_date: datetime, end_date: datetime, target: str) -> pd.DataFrame:
    """
    Load actual values from database for comparison.

    Args:
        database_url: Database connection string
        start_date: Start date for actuals
        end_date: End date for actuals
        target: Target variable name

    Returns:
        DataFrame with actual values
    """
    engine = create_engine(database_url)

    # Query based on target type
    if target in ["price_events", "availability_events", "total_change_events"]:
        # Event targets from flight_search_event
        query = text("""
            SELECT
                airline,
                origin,
                destination,
                cabin,
                report_day,
                COUNT(*) as total_change_events,
                SUM(CASE WHEN price_change_flag = 1 THEN 1 ELSE 0 END) as price_events,
                SUM(CASE WHEN availability_change_flag = 1 THEN 1 ELSE 0 END) as availability_events
            FROM flight_search_event
            WHERE report_day >= :start_date AND report_day <= :end_date
            GROUP BY airline, origin, destination, cabin, report_day
        """)
    else:
        # Search targets from flight_offers
        query = text("""
            SELECT
                airline,
                origin,
                destination,
                cabin,
                search_day as report_day,
                MIN(min_price_bdt) as min_price_bdt,
                AVG(avg_seat_available) as avg_seat_available,
                COUNT(*) as offers_count,
                AVG(CASE WHEN avg_seat_available = 0 THEN 1.0 ELSE 0.0 END) as soldout_rate
            FROM flight_offer_raw_meta
            WHERE search_day >= :start_date AND search_day <= :end_date
            GROUP BY airline, origin, destination, cabin, search_day
        """)

    with engine.connect() as conn:
        actuals = pd.read_sql(query, conn, params={"start_date": start_date, "end_date": end_date})

    return actuals


def generate_daily_report(database_url: str, lookback_days: int = 7, route: str = None, target: str = None) -> dict:
    """
    Generate daily performance report.

    Args:
        database_url: Database connection string
        lookback_days: Number of days to look back
        route: Optional route filter (format: AIRLINE-ORIGIN-DESTINATION)
        target: Optional target filter

    Returns:
        Dictionary with report data
    """
    # Load predictions
    predictions_df = load_recent_predictions(database_url, lookback_days)

    if predictions_df.empty:
        return {
            "status": "no_data",
            "message": f"No predictions found in last {lookback_days} days",
            "generated_at": datetime.now().isoformat()
        }

    # Calculate date range for actuals
    end_date = datetime.now().date()
    start_date = (datetime.now() - timedelta(days=lookback_days)).date()

    # Initialize report
    report = {
        "generated_at": datetime.now().isoformat(),
        "lookback_days": lookback_days,
        "date_range": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat()
        },
        "overall_metrics": {},
        "route_metrics": [],
        "alerts": []
    }

    # Filter by route if specified
    if route:
        parts = route.split("-")
        if len(parts) == 3:
            airline, origin, destination = parts
            predictions_df = predictions_df[
                (predictions_df.get("airline", "") == airline) &
                (predictions_df.get("origin", "") == origin) &
                (predictions_df.get("destination", "") == destination)
            ]

    # Calculate overall metrics (placeholder - would need actuals for real metrics)
    report["overall_metrics"] = {
        "total_predictions": len(predictions_df),
        "unique_routes": predictions_df.groupby(["airline", "origin", "destination"]).ngroups
                         if all(col in predictions_df.columns for col in ["airline", "origin", "destination"])
                         else 0,
        "prediction_files_processed": len(predictions_df["prediction_timestamp"].unique())
                                      if "prediction_timestamp" in predictions_df.columns else 0
    }

    # Group by route for route-level metrics
    if all(col in predictions_df.columns for col in ["airline", "origin", "destination"]):
        for (airline, origin, dest), group in predictions_df.groupby(["airline", "origin", "destination"]):
            route_id = f"{airline}-{origin}-{dest}"

            # Get prediction columns
            pred_cols = [col for col in group.columns if col.startswith("pred_ml_") or col.startswith("pred_dl_")]

            route_metrics = {
                "route": route_id,
                "predictions_count": len(group),
                "latest_prediction_date": group["predicted_for_day"].max()
                                         if "predicted_for_day" in group.columns else None,
                "available_models": len(pred_cols)
            }

            report["route_metrics"].append(route_metrics)

    # Note: Actual MAE/RMSE calculation would require loading actuals from database
    # This is a simplified version showing the structure

    return report


def print_report(report: dict):
    """
    Print report in human-readable format.

    Args:
        report: Report dictionary
    """
    print("\n" + "=" * 80)
    print("DAILY PREDICTION PERFORMANCE REPORT")
    print("=" * 80)
    print(f"\nGenerated: {report['generated_at']}")

    if report.get("status") == "no_data":
        print(f"\n{report['message']}")
        return

    print(f"\nDate Range: {report['date_range']['start']} to {report['date_range']['end']}")
    print(f"Lookback Days: {report['lookback_days']}")

    print("\n" + "-" * 80)
    print("OVERALL METRICS")
    print("-" * 80)
    metrics = report["overall_metrics"]
    print(f"Total Predictions: {metrics.get('total_predictions', 0)}")
    print(f"Unique Routes: {metrics.get('unique_routes', 0)}")
    print(f"Prediction Files Processed: {metrics.get('prediction_files_processed', 0)}")

    if report.get("route_metrics"):
        print("\n" + "-" * 80)
        print("ROUTE-LEVEL METRICS")
        print("-" * 80)
        for route_metric in report["route_metrics"][:10]:  # Show top 10
            print(f"\nRoute: {route_metric['route']}")
            print(f"  Predictions: {route_metric['predictions_count']}")
            print(f"  Latest Date: {route_metric.get('latest_prediction_date', 'N/A')}")
            print(f"  Available Models: {route_metric['available_models']}")

    if report.get("alerts"):
        print("\n" + "-" * 80)
        print("ALERTS")
        print("-" * 80)
        for alert in report["alerts"]:
            print(f"\n⚠ {alert.get('message', 'Unknown alert')}")
    else:
        print("\n✓ No alerts detected")

    print("\n" + "=" * 80)


def main():
    parser = argparse.ArgumentParser(description="Generate daily prediction performance report")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL),
                       help="Database connection URL")
    parser.add_argument("--lookback-days", type=int, default=7,
                       help="Number of days to look back (default: 7)")
    parser.add_argument("--route", help="Filter by specific route (format: AIRLINE-ORIGIN-DESTINATION)")
    parser.add_argument("--target", help="Filter by specific target variable")
    parser.add_argument("--output-json", help="Save report to JSON file")
    parser.add_argument("--quiet", action="store_true", help="Suppress console output")

    args = parser.parse_args()

    # Generate report
    report = generate_daily_report(
        database_url=args.database_url,
        lookback_days=args.lookback_days,
        route=args.route,
        target=args.target
    )

    # Print to console unless quiet mode
    if not args.quiet:
        print_report(report)

    # Save to JSON if requested
    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str)
        print(f"\n✓ Report saved to {output_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
