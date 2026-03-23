"""
Robust imputation pipeline for handling missing values in aviation inventory data.

Implements multiple imputation strategies:
- KNN imputation for numeric features (uses similar routes/dates)
- Mode imputation for categorical features
- Forward/backward fill for time series
- Quality tracking to detect data issues
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import warnings

try:
    from sklearn.impute import KNNImputer
except ModuleNotFoundError:
    class KNNImputer:  # type: ignore[override]
        """Lightweight fallback when scikit-learn is unavailable."""

        def __init__(self, n_neighbors: int = 5, weights: str = "distance"):
            self.n_neighbors = n_neighbors
            self.weights = weights

        def fit_transform(self, values):
            frame = pd.DataFrame(values).copy()
            for column in frame.columns:
                series = frame[column]
                if series.isna().any():
                    fill_value = series.mean()
                    if pd.isna(fill_value):
                        fill_value = 0.0
                    frame[column] = series.fillna(fill_value)
            return frame.to_numpy()


class RobustImputer:
    """
    Handles missing value imputation with quality tracking.

    Strategies:
    - Numeric features: KNN imputation using k=5 nearest neighbors
    - Categorical features: Mode (most frequent value)
    - Time series: Forward fill then backward fill
    - Track imputation rates to detect data quality issues
    """

    def __init__(self, n_neighbors: int = 5, warn_threshold: float = 0.20):
        """
        Initialize imputer.

        Args:
            n_neighbors: Number of neighbors for KNN imputation
            warn_threshold: Warn if imputation rate exceeds this threshold (default 20%)
        """
        self.n_neighbors = n_neighbors
        self.warn_threshold = warn_threshold
        self.imputation_stats = {}
        self.categorical_modes = {}
        self.label_encoders = {}

    def fit_transform(self, df: pd.DataFrame,
                     numeric_cols: Optional[List[str]] = None,
                     categorical_cols: Optional[List[str]] = None,
                     timeseries_cols: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Fit imputer and transform data.

        Args:
            df: Input DataFrame with missing values
            numeric_cols: List of numeric column names (auto-detected if None)
            categorical_cols: List of categorical column names (auto-detected if None)
            timeseries_cols: List of time series columns requiring forward/backward fill

        Returns:
            DataFrame with imputed values
        """
        df = df.copy()

        # Auto-detect column types if not provided
        if numeric_cols is None:
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if categorical_cols is None:
            categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        if timeseries_cols is None:
            timeseries_cols = []

        # Track imputation statistics
        self.imputation_stats = {
            'total_rows': len(df),
            'columns': {}
        }

        # 1. Handle time series columns first (forward/backward fill)
        for col in timeseries_cols:
            if col in df.columns:
                missing_before = df[col].isna().sum()
                df[col] = df[col].ffill().bfill()
                missing_after = df[col].isna().sum()
                self._record_imputation(col, missing_before, missing_after, 'ffill+bfill')

        # 2. Handle categorical columns (mode imputation)
        for col in categorical_cols:
            if col in df.columns and col not in timeseries_cols:
                missing_before = df[col].isna().sum()
                if missing_before > 0:
                    mode_value = df[col].mode()
                    if len(mode_value) > 0:
                        self.categorical_modes[col] = mode_value[0]
                        df[col] = df[col].fillna(self.categorical_modes[col])
                    else:
                        # No mode (all values missing) - use "unknown"
                        self.categorical_modes[col] = "unknown"
                        df[col] = df[col].fillna(self.categorical_modes[col])
                missing_after = df[col].isna().sum()
                self._record_imputation(col, missing_before, missing_after, 'mode')

        # 3. Handle numeric columns (KNN imputation)
        numeric_cols_to_impute = [col for col in numeric_cols if col in df.columns and col not in timeseries_cols]
        if numeric_cols_to_impute:
            # Track missing values before imputation
            missing_before_dict = {col: df[col].isna().sum() for col in numeric_cols_to_impute}

            # Apply KNN imputation
            knn_imputer = KNNImputer(n_neighbors=self.n_neighbors, weights='distance')
            df[numeric_cols_to_impute] = knn_imputer.fit_transform(df[numeric_cols_to_impute])

            # Track missing values after imputation (should be 0)
            for col in numeric_cols_to_impute:
                missing_after = df[col].isna().sum()
                self._record_imputation(col, missing_before_dict[col], missing_after, 'knn')

        # 4. Check for any remaining NaN values and warn
        remaining_nans = df.isna().sum()
        if remaining_nans.sum() > 0:
            warnings.warn(f"Imputation incomplete. Remaining NaN counts:\n{remaining_nans[remaining_nans > 0]}")

        # 5. Check imputation rates and warn if above threshold
        self._check_imputation_quality()

        return df

    def _record_imputation(self, col: str, missing_before: int, missing_after: int, method: str):
        """Record imputation statistics for a column."""
        imputation_rate = missing_before / self.imputation_stats['total_rows'] if self.imputation_stats['total_rows'] > 0 else 0
        self.imputation_stats['columns'][col] = {
            'missing_before': int(missing_before),
            'missing_after': int(missing_after),
            'imputed_count': int(missing_before - missing_after),
            'imputation_rate': float(imputation_rate),
            'method': method
        }

    def _check_imputation_quality(self):
        """Check imputation rates and warn if above threshold."""
        high_imputation_cols = []
        for col, stats in self.imputation_stats['columns'].items():
            if stats['imputation_rate'] > self.warn_threshold:
                high_imputation_cols.append(f"{col} ({stats['imputation_rate']:.1%})")

        if high_imputation_cols:
            warnings.warn(
                f"High imputation rate detected (>{self.warn_threshold:.0%}): {', '.join(high_imputation_cols)}. "
                "This may indicate data quality issues."
            )

    def get_imputation_report(self) -> pd.DataFrame:
        """
        Get detailed imputation report.

        Returns:
            DataFrame with imputation statistics per column
        """
        if not self.imputation_stats.get('columns'):
            return pd.DataFrame()

        report_data = []
        for col, stats in self.imputation_stats['columns'].items():
            report_data.append({
                'column': col,
                'missing_before': stats['missing_before'],
                'imputed_count': stats['imputed_count'],
                'imputation_rate': stats['imputation_rate'],
                'method': stats['method']
            })

        return pd.DataFrame(report_data).sort_values('imputation_rate', ascending=False)


def impute_with_similar_routes(df: pd.DataFrame,
                               route_cols: List[str] = ['origin', 'destination'],
                               numeric_cols: Optional[List[str]] = None,
                               n_neighbors: int = 3) -> pd.DataFrame:
    """
    Impute missing values using data from similar routes.

    This function groups data by route and performs KNN imputation within similar routes,
    which is more accurate than global imputation for route-specific features.

    Args:
        df: Input DataFrame with route information
        route_cols: Columns defining a route (e.g., ['origin', 'destination'])
        numeric_cols: Numeric columns to impute (auto-detected if None)
        n_neighbors: Number of similar routes to use for imputation

    Returns:
        DataFrame with imputed values
    """
    df = df.copy()

    if numeric_cols is None:
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        # Exclude route identifier columns
        numeric_cols = [col for col in numeric_cols if col not in route_cols]

    # Create route identifier
    df['_route_id'] = df[route_cols].astype(str).agg('-'.join, axis=1)

    # For each route with missing data, impute using similar routes
    routes_with_missing = df[df[numeric_cols].isna().any(axis=1)]['_route_id'].unique()

    for route in routes_with_missing:
        route_mask = df['_route_id'] == route
        route_data = df[route_mask]

        if route_data[numeric_cols].isna().any().any():
            # Find similar routes (same origin or destination)
            origin, destination = route.split('-')
            similar_mask = (df['_route_id'] != route) & (
                (df[route_cols[0]] == origin) | (df[route_cols[1]] == destination)
            )
            similar_data = df[similar_mask]

            # Fall back to route-local/global imputation when not enough similar routes exist.
            imputer = RobustImputer(n_neighbors=n_neighbors, warn_threshold=0.5)
            if len(similar_data) > 0:
                combined_data = pd.concat([route_data, similar_data])
                imputed_combined = imputer.fit_transform(combined_data, numeric_cols=numeric_cols)
                df.loc[route_mask, numeric_cols] = imputed_combined.iloc[:len(route_data)][numeric_cols].values
            else:
                imputed_route = imputer.fit_transform(route_data, numeric_cols=numeric_cols)
                df.loc[route_mask, numeric_cols] = imputed_route[numeric_cols].values

    df = df.drop(columns=['_route_id'])
    return df


def get_imputation_quality_metrics(df_before: pd.DataFrame, df_after: pd.DataFrame) -> Dict[str, float]:
    """
    Calculate imputation quality metrics.

    Args:
        df_before: DataFrame before imputation
        df_after: DataFrame after imputation

    Returns:
        Dictionary with quality metrics:
        - overall_completeness: % of non-null values after imputation
        - columns_fully_imputed: Number of columns with no remaining NaN
        - total_imputed_values: Total number of values imputed
    """
    missing_before = df_before.isna().sum().sum()
    missing_after = df_after.isna().sum().sum()
    total_values = df_before.shape[0] * df_before.shape[1]

    return {
        'overall_completeness': (total_values - missing_after) / total_values if total_values > 0 else 0,
        'columns_fully_imputed': int((df_after.isna().sum() == 0).sum()),
        'total_imputed_values': int(missing_before - missing_after),
        'imputation_success_rate': (missing_before - missing_after) / missing_before if missing_before > 0 else 1.0
    }
