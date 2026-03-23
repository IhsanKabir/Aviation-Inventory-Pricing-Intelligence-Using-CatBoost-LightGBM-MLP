"""
Holiday feature engineering module.

Provides functions to compute holiday-related features for aviation demand prediction.
Critical for Bangladesh market where Eid and other major holidays drive significant demand surges.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd


class HolidayCalendar:
    """
    Manages holiday calendar data and provides feature extraction methods.

    Attributes:
        holidays: Dict mapping date strings to holiday info
        high_demand_dates: Set of dates marked as high_demand
        holiday_dates: Set of all holiday dates
    """

    def __init__(self, calendar_path: Optional[str] = None):
        """
        Initialize holiday calendar from JSON file.

        Args:
            calendar_path: Path to holiday_calendar.json. If None, uses default config/holiday_calendar.json
        """
        if calendar_path is None:
            base_dir = Path(__file__).parent.parent
            calendar_path = base_dir / "config" / "holiday_calendar.json"

        self.calendar_path = Path(calendar_path)
        self.holidays: Dict[str, dict] = {}
        self.high_demand_dates: Set[str] = set()
        self.holiday_dates: Set[str] = set()

        self._load_calendar()

    def _load_calendar(self):
        """Load and parse holiday calendar JSON."""
        try:
            with open(self.calendar_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            for holiday in data.get('holidays', []):
                date_str = holiday['date']
                self.holidays[date_str] = holiday
                self.holiday_dates.add(date_str)

                if holiday.get('high_demand', False):
                    self.high_demand_dates.add(date_str)

        except FileNotFoundError:
            print(f"Warning: Holiday calendar not found at {self.calendar_path}. Holiday features will be zero.")
        except Exception as e:
            print(f"Warning: Error loading holiday calendar: {e}. Holiday features will be zero.")

    def is_holiday(self, date: pd.Timestamp) -> bool:
        """Check if a date is a holiday."""
        date_str = date.strftime('%Y-%m-%d')
        return date_str in self.holiday_dates

    def is_high_demand_holiday(self, date: pd.Timestamp) -> bool:
        """Check if a date is a high-demand holiday (e.g., Eid)."""
        date_str = date.strftime('%Y-%m-%d')
        return date_str in self.high_demand_dates

    def get_holiday_info(self, date: pd.Timestamp) -> Optional[dict]:
        """Get full holiday info for a date."""
        date_str = date.strftime('%Y-%m-%d')
        return self.holidays.get(date_str)

    def days_to_next_holiday(self, date: pd.Timestamp, max_days: int = 60) -> int:
        """
        Calculate days until next holiday.

        Args:
            date: Reference date
            max_days: Maximum lookahead window. Returns max_days if no holiday within window.

        Returns:
            Number of days to next holiday (0 if today is a holiday, max_days if none found)
        """
        date_str = date.strftime('%Y-%m-%d')

        # If today is a holiday, return 0
        if date_str in self.holiday_dates:
            return 0

        # Search forward
        for days_ahead in range(1, max_days + 1):
            future_date = date + timedelta(days=days_ahead)
            future_str = future_date.strftime('%Y-%m-%d')
            if future_str in self.holiday_dates:
                return days_ahead

        return max_days

    def days_since_last_holiday(self, date: pd.Timestamp, max_days: int = 60) -> int:
        """
        Calculate days since last holiday.

        Args:
            date: Reference date
            max_days: Maximum lookback window. Returns max_days if no holiday within window.

        Returns:
            Number of days since last holiday (0 if today is a holiday, max_days if none found)
        """
        date_str = date.strftime('%Y-%m-%d')

        # If today is a holiday, return 0
        if date_str in self.holiday_dates:
            return 0

        # Search backward
        for days_back in range(1, max_days + 1):
            past_date = date - timedelta(days=days_back)
            past_str = past_date.strftime('%Y-%m-%d')
            if past_str in self.holiday_dates:
                return days_back

        return max_days

    def is_holiday_week(self, date: pd.Timestamp, window_days: int = 3) -> bool:
        """
        Check if date is within window_days before or after a holiday.

        Args:
            date: Reference date
            window_days: Days before/after holiday to consider "holiday week"

        Returns:
            True if within holiday window
        """
        days_to = self.days_to_next_holiday(date, max_days=window_days)
        days_since = self.days_since_last_holiday(date, max_days=window_days)

        return (days_to <= window_days) or (days_since <= window_days)

    def get_holiday_type(self, date: pd.Timestamp) -> str:
        """
        Get holiday type (religious, national, etc.).

        Returns:
            Holiday type string, or 'none' if not a holiday
        """
        info = self.get_holiday_info(date)
        if info:
            return info.get('type', 'unknown')
        return 'none'


def add_holiday_features(
    df: pd.DataFrame,
    date_column: str = 'report_day',
    departure_column: Optional[str] = 'departure_day',
    calendar_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Add holiday-related features to DataFrame.

    Features added:
        - is_search_holiday: Binary flag if search date is a holiday
        - is_departure_holiday: Binary flag if departure date is a holiday (if departure_column provided)
        - is_high_demand_holiday: Binary flag if holiday marked as high_demand (Eid, etc.)
        - days_to_next_holiday: Days until next holiday from search date
        - days_since_last_holiday: Days since last holiday from search date
        - is_holiday_week: Binary flag if within 3 days of a holiday
        - holiday_type_code: Numeric encoding of holiday type (0=none, 1=religious, 2=national)

    Args:
        df: Input DataFrame (will be copied, not modified in-place)
        date_column: Column name for search/report date
        departure_column: Column name for departure date (optional)
        calendar_path: Path to holiday calendar JSON (optional, uses default if None)

    Returns:
        DataFrame with added holiday features
    """
    result = df.copy()
    calendar = HolidayCalendar(calendar_path)

    # Convert date columns to datetime
    search_dates = pd.to_datetime(result[date_column], errors='coerce')

    # Search date features
    result['is_search_holiday'] = search_dates.apply(lambda d: 1 if pd.notna(d) and calendar.is_holiday(d) else 0)
    result['is_high_demand_holiday'] = search_dates.apply(lambda d: 1 if pd.notna(d) and calendar.is_high_demand_holiday(d) else 0)
    result['days_to_next_holiday'] = search_dates.apply(lambda d: calendar.days_to_next_holiday(d) if pd.notna(d) else 60)
    result['days_since_last_holiday'] = search_dates.apply(lambda d: calendar.days_since_last_holiday(d) if pd.notna(d) else 60)
    result['is_holiday_week'] = search_dates.apply(lambda d: 1 if pd.notna(d) and calendar.is_holiday_week(d) else 0)

    # Holiday type encoding
    type_map = {'none': 0, 'religious': 1, 'national': 2, 'unknown': 0}
    result['holiday_type_code'] = search_dates.apply(lambda d: type_map.get(calendar.get_holiday_type(d), 0) if pd.notna(d) else 0)

    # Departure date features (if provided)
    if departure_column and departure_column in result.columns:
        departure_dates = pd.to_datetime(result[departure_column], errors='coerce')
        result['is_departure_holiday'] = departure_dates.apply(lambda d: 1 if pd.notna(d) and calendar.is_holiday(d) else 0)
        result['is_departure_high_demand'] = departure_dates.apply(lambda d: 1 if pd.notna(d) and calendar.is_high_demand_holiday(d) else 0)

    return result


def get_holiday_feature_columns() -> List[str]:
    """
    Get list of holiday feature column names.

    Returns:
        List of feature column names that will be added by add_holiday_features()
    """
    return [
        'is_search_holiday',
        'is_high_demand_holiday',
        'days_to_next_holiday',
        'days_since_last_holiday',
        'is_holiday_week',
        'holiday_type_code',
        'is_departure_holiday',
        'is_departure_high_demand'
    ]


if __name__ == '__main__':
    # Example usage / testing
    import sys

    # Load calendar
    cal = HolidayCalendar()
    print(f"Loaded {len(cal.holiday_dates)} holidays")
    print(f"High-demand holidays: {len(cal.high_demand_dates)}")

    # Test on specific dates
    test_dates = [
        '2026-04-11',  # Eid
        '2026-04-14',  # Pohela Boishakh
        '2026-03-26',  # Independence Day
        '2026-05-15',  # Regular day
    ]

    for date_str in test_dates:
        date = pd.Timestamp(date_str)
        print(f"\n{date_str}:")
        print(f"  Is holiday: {cal.is_holiday(date)}")
        print(f"  Is high-demand: {cal.is_high_demand_holiday(date)}")
        print(f"  Days to next: {cal.days_to_next_holiday(date)}")
        print(f"  Days since last: {cal.days_since_last_holiday(date)}")
        print(f"  Holiday week: {cal.is_holiday_week(date)}")
        print(f"  Type: {cal.get_holiday_type(date)}")
