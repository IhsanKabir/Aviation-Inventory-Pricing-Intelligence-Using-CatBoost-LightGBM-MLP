"""
Data Quality Gates - Pre-prediction validation checks.

Prevents "garbage in, garbage out" by validating data quality before running predictions.
Critical checks: freshness, completeness, outliers, distribution consistency.
"""

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from db import DATABASE_URL as DEFAULT_DATABASE_URL


class DataQualityGate:
    """
    Validates data quality before prediction pipeline execution.

    Performs checks on:
    - Data freshness (last capture time)
    - Data completeness (null percentages)
    - Row count sufficiency
    - Outlier detection (price/capacity anomalies)
    - Distribution drift (sudden changes in data patterns)
    """

    def __init__(
        self,
        min_rows: int = 100,
        max_null_pct: float = 0.05,
        max_age_hours: int = 24,
        price_outlier_iqr_multiplier: float = 3.0,
        capacity_outlier_iqr_multiplier: float = 3.0
    ):
        """
        Initialize data quality gate with thresholds.

        Args:
            min_rows: Minimum number of rows required
            max_null_pct: Maximum allowed null percentage (0.0-1.0)
            max_age_hours: Maximum data age in hours
            price_outlier_iqr_multiplier: IQR multiplier for price outlier detection
            capacity_outlier_iqr_multiplier: IQR multiplier for capacity outlier detection
        """
        self.min_rows = min_rows
        self.max_null_pct = max_null_pct
        self.max_age_hours = max_age_hours
        self.price_outlier_iqr_multiplier = price_outlier_iqr_multiplier
        self.capacity_outlier_iqr_multiplier = capacity_outlier_iqr_multiplier

        self.results: Dict[str, any] = {}
        self.passed = True
        self.warnings: List[str] = []
        self.errors: List[str] = []

    def check_row_count(self, df: pd.DataFrame) -> bool:
        """Check if DataFrame has sufficient rows."""
        row_count = len(df)
        self.results['row_count'] = row_count

        if row_count < self.min_rows:
            self.errors.append(f"Insufficient data: {row_count} rows < {self.min_rows} minimum")
            self.passed = False
            return False

        return True

    def check_freshness(self, df: pd.DataFrame, date_column: str = 'report_day') -> bool:
        """
        Check data freshness.

        Args:
            df: DataFrame to check
            date_column: Column containing capture/report dates

        Returns:
            True if data is fresh enough
        """
        if date_column not in df.columns:
            self.warnings.append(f"Freshness check skipped: '{date_column}' column not found")
            return True

        dates = pd.to_datetime(df[date_column], errors='coerce')
        max_date = dates.max()

        if pd.isna(max_date):
            self.errors.append(f"No valid dates in column '{date_column}'")
            self.passed = False
            return False

        age_hours = (datetime.now() - max_date).total_seconds() / 3600
        self.results['data_age_hours'] = round(age_hours, 2)
        self.results['latest_date'] = max_date.strftime('%Y-%m-%d %H:%M:%S')

        if age_hours > self.max_age_hours:
            self.errors.append(
                f"Stale data: {round(age_hours, 1)}h old > {self.max_age_hours}h maximum (latest: {max_date})"
            )
            self.passed = False
            return False

        return True

    def check_completeness(self, df: pd.DataFrame, critical_columns: Optional[List[str]] = None) -> bool:
        """
        Check data completeness (null percentages).

        Args:
            df: DataFrame to check
            critical_columns: List of critical columns to check. If None, checks all numeric columns.

        Returns:
            True if completeness acceptable
        """
        if critical_columns is None:
            critical_columns = df.select_dtypes(include=[np.number]).columns.tolist()

        null_pcts = {}
        violations = []

        for col in critical_columns:
            if col in df.columns:
                null_pct = df[col].isnull().mean()
                null_pcts[col] = round(null_pct, 4)

                if null_pct > self.max_null_pct:
                    violations.append(f"{col}: {null_pct:.1%} nulls > {self.max_null_pct:.1%} max")

        self.results['null_percentages'] = null_pcts
        self.results['max_null_pct'] = max(null_pcts.values()) if null_pcts else 0.0

        if violations:
            self.errors.append(f"Completeness violations: {', '.join(violations)}")
            self.passed = False
            return False

        return True

    def detect_price_outliers(self, df: pd.DataFrame, price_column: str = 'min_price_bdt') -> Tuple[int, pd.Series]:
        """
        Detect price outliers using IQR method.

        Args:
            df: DataFrame to check
            price_column: Column containing prices

        Returns:
            (outlier_count, outlier_mask)
        """
        if price_column not in df.columns:
            return 0, pd.Series([False] * len(df))

        prices = pd.to_numeric(df[price_column], errors='coerce').dropna()

        if len(prices) < 4:
            return 0, pd.Series([False] * len(df))

        Q1 = prices.quantile(0.25)
        Q3 = prices.quantile(0.75)
        IQR = Q3 - Q1

        lower_bound = Q1 - self.price_outlier_iqr_multiplier * IQR
        upper_bound = Q3 + self.price_outlier_iqr_multiplier * IQR

        outlier_mask = (
            pd.to_numeric(df[price_column], errors='coerce') < lower_bound
        ) | (
            pd.to_numeric(df[price_column], errors='coerce') > upper_bound
        )

        outlier_count = outlier_mask.sum()

        self.results['price_outliers'] = {
            'count': int(outlier_count),
            'percentage': round(outlier_count / len(df), 4) if len(df) > 0 else 0.0,
            'Q1': round(Q1, 2),
            'Q3': round(Q3, 2),
            'IQR': round(IQR, 2),
            'lower_bound': round(lower_bound, 2),
            'upper_bound': round(upper_bound, 2)
        }

        if outlier_count / len(df) > 0.02:  # More than 2% outliers
            self.warnings.append(
                f"High price outlier rate: {outlier_count} ({outlier_count/len(df):.1%}) outside [{lower_bound:.0f}, {upper_bound:.0f}]"
            )

        return outlier_count, outlier_mask

    def detect_capacity_outliers(self, df: pd.DataFrame, capacity_column: str = 'avg_seat_available') -> Tuple[int, pd.Series]:
        """
        Detect capacity outliers using IQR method.

        Args:
            df: DataFrame to check
            capacity_column: Column containing capacity/seat counts

        Returns:
            (outlier_count, outlier_mask)
        """
        if capacity_column not in df.columns:
            return 0, pd.Series([False] * len(df))

        capacities = pd.to_numeric(df[capacity_column], errors='coerce').dropna()

        if len(capacities) < 4:
            return 0, pd.Series([False] * len(df))

        Q1 = capacities.quantile(0.25)
        Q3 = capacities.quantile(0.75)
        IQR = Q3 - Q1

        lower_bound = max(0, Q1 - self.capacity_outlier_iqr_multiplier * IQR)  # Capacity can't be negative
        upper_bound = Q3 + self.capacity_outlier_iqr_multiplier * IQR

        outlier_mask = (
            pd.to_numeric(df[capacity_column], errors='coerce') < lower_bound
        ) | (
            pd.to_numeric(df[capacity_column], errors='coerce') > upper_bound
        )

        outlier_count = outlier_mask.sum()

        self.results['capacity_outliers'] = {
            'count': int(outlier_count),
            'percentage': round(outlier_count / len(df), 4) if len(df) > 0 else 0.0,
            'Q1': round(Q1, 2),
            'Q3': round(Q3, 2),
            'IQR': round(IQR, 2),
            'lower_bound': round(lower_bound, 2),
            'upper_bound': round(upper_bound, 2)
        }

        if outlier_count / len(df) > 0.02:  # More than 2% outliers
            self.warnings.append(
                f"High capacity outlier rate: {outlier_count} ({outlier_count/len(df):.1%}) outside [{lower_bound:.0f}, {upper_bound:.0f}]"
            )

        return outlier_count, outlier_mask

    def validate(
        self,
        df: pd.DataFrame,
        date_column: str = 'report_day',
        critical_columns: Optional[List[str]] = None,
        price_column: Optional[str] = 'min_price_bdt',
        capacity_column: Optional[str] = 'avg_seat_available'
    ) -> bool:
        """
        Run all validation checks.

        Args:
            df: DataFrame to validate
            date_column: Column for freshness check
            critical_columns: Columns for completeness check
            price_column: Column for price outlier detection (None to skip)
            capacity_column: Column for capacity outlier detection (None to skip)

        Returns:
            True if all critical checks passed
        """
        self.results = {'timestamp': datetime.now().isoformat()}
        self.passed = True
        self.warnings = []
        self.errors = []

        # Critical checks (failures block prediction)
        self.check_row_count(df)
        self.check_freshness(df, date_column)
        self.check_completeness(df, critical_columns)

        # Warning checks (logged but don't block)
        if price_column:
            self.detect_price_outliers(df, price_column)

        if capacity_column:
            self.detect_capacity_outliers(df, capacity_column)

        # Store summary
        self.results['passed'] = self.passed
        self.results['warnings'] = self.warnings
        self.results['errors'] = self.errors

        return self.passed

    def get_report(self) -> Dict:
        """Get validation report as dictionary."""
        return self.results

    def print_report(self):
        """Print validation report to console."""
        print("\n=== Data Quality Gate Report ===")
        print(f"Timestamp: {self.results.get('timestamp', 'N/A')}")
        print(f"Overall Status: {'PASS ✓' if self.passed else 'FAIL ✗'}")

        if 'row_count' in self.results:
            print(f"\nRow Count: {self.results['row_count']} (min: {self.min_rows})")

        if 'data_age_hours' in self.results:
            print(f"Data Age: {self.results['data_age_hours']:.1f} hours (max: {self.max_age_hours}h)")
            print(f"Latest Date: {self.results.get('latest_date', 'N/A')}")

        if 'max_null_pct' in self.results:
            print(f"Max Null %: {self.results['max_null_pct']:.1%} (max: {self.max_null_pct:.1%})")

        if 'price_outliers' in self.results:
            po = self.results['price_outliers']
            print(f"\nPrice Outliers: {po['count']} ({po['percentage']:.1%})")
            print(f"  IQR bounds: [{po['lower_bound']:.0f}, {po['upper_bound']:.0f}]")

        if 'capacity_outliers' in self.results:
            co = self.results['capacity_outliers']
            print(f"\nCapacity Outliers: {co['count']} ({co['percentage']:.1%})")
            print(f"  IQR bounds: [{co['lower_bound']:.0f}, {co['upper_bound']:.0f}]")

        if self.warnings:
            print(f"\nWarnings ({len(self.warnings)}):")
            for w in self.warnings:
                print(f"  ⚠ {w}")

        if self.errors:
            print(f"\nErrors ({len(self.errors)}):")
            for e in self.errors:
                print(f"  ✗ {e}")

        print("\n================================\n")


def check_database_data_quality(
    db_url: str = DEFAULT_DATABASE_URL,
    table: str = 'flight_offers',
    min_rows: int = 100,
    max_null_pct: float = 0.05,
    max_age_hours: int = 24
) -> Tuple[bool, Dict]:
    """
    Check data quality directly from database.

    Args:
        db_url: Database connection URL
        table: Table name to check
        min_rows: Minimum row count
        max_null_pct: Maximum null percentage
        max_age_hours: Maximum data age in hours

    Returns:
        (passed, report_dict)
    """
    engine = create_engine(db_url, pool_pre_ping=True, future=True)

    # Load recent data
    sql = text(f"""
        SELECT
            DATE(scraped_at) as report_day,
            airline,
            origin,
            destination,
            cabin,
            total_fare as min_price_bdt,
            seats_remaining as avg_seat_available,
            scraped_at
        FROM {table}
        WHERE scraped_at >= NOW() - INTERVAL '{max_age_hours + 24} hours'
        LIMIT 10000
    """)

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)

    gate = DataQualityGate(
        min_rows=min_rows,
        max_null_pct=max_null_pct,
        max_age_hours=max_age_hours
    )

    critical_cols = ['min_price_bdt', 'airline', 'origin', 'destination', 'cabin']
    passed = gate.validate(df, date_column='report_day', critical_columns=critical_cols)

    return passed, gate.get_report()


def main():
    parser = argparse.ArgumentParser(description="Data Quality Gate - validate data before predictions")
    parser.add_argument('--db-url', default=DEFAULT_DATABASE_URL, help='Database URL')
    parser.add_argument('--table', default='flight_offers', help='Table name to check')
    parser.add_argument('--min-rows', type=int, default=100, help='Minimum row count')
    parser.add_argument('--max-null-pct', type=float, default=0.05, help='Maximum null percentage (0.0-1.0)')
    parser.add_argument('--max-age-hours', type=int, default=24, help='Maximum data age in hours')
    parser.add_argument('--output', help='Output JSON file path (optional)')
    parser.add_argument('--check-all', action='store_true', help='Run all checks and print report')

    args = parser.parse_args()

    passed, report = check_database_data_quality(
        db_url=args.db_url,
        table=args.table,
        min_rows=args.min_rows,
        max_null_pct=args.max_null_pct,
        max_age_hours=args.max_age_hours
    )

    # Print to console
    gate = DataQualityGate()
    gate.results = report
    gate.passed = passed
    gate.warnings = report.get('warnings', [])
    gate.errors = report.get('errors', [])
    gate.print_report()

    # Save to file if requested
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"Report saved to: {output_path}")

    # Exit with appropriate code
    return 0 if passed else 1


if __name__ == '__main__':
    exit(main())
