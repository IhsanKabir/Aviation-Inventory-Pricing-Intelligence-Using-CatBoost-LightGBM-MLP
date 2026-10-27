"""Schedule / Fare tabs: several date ranges in one run ("+ Add range").

The run searches the days of every range, never the span between them: HAR rows
from the gap are cut, the day limit counts all ranges together, and a week the
gap splits is not a "complete week" for the schedule-change test.
"""
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from market_engine import dateranges as DR  # noqa: E402
from market_engine.har import select_rows  # noqa: E402
from market_engine.rows import FlightRow  # noqa: E402
from market_engine.schedule import _complete_weeks  # noqa: E402


def _row(day: date) -> FlightRow:
    return FlightRow(source="FirstTrip", airline="BS", flight_number="141", origin="DAC",
                     destination="CXB", departure_date=day, departure_time="07:00",
                     arrival_time="08:05", arrival_date=day, stops=0, via="", aircraft="",
                     operated_by="", cabin="Economy", gross_bdt=4849.0, base_bdt=3724.0,
                     base_is_real=True, seats=None, fetched_at=datetime(2026, 10, 27))


def _host():
    from desktop.market_api import MarketApiMixin

    class _Host(MarketApiMixin):
        def __init__(self):
            self._config = {"har_dir": ""}

        def _live_ok(self):
            return True

    return _Host()


def test_ranges_become_their_days_only():
    days, err = DR.parse([("2026-11-01", "2026-11-03"), ("2026-12-15", "2026-12-16")])
    assert err is None
    assert days == [date(2026, 11, 1), date(2026, 11, 2), date(2026, 11, 3),
                    date(2026, 12, 15), date(2026, 12, 16)]
    assert DR.describe(days) == "01 Nov - 03 Nov 2026, 15 Dec - 16 Dec 2026"
    assert DR.is_split(days)


def test_overlaps_count_once_and_blank_extra_rows_are_ignored():
    days, err = DR.parse([("2026-11-01", "2026-11-05"), ("2026-11-04", "2026-11-06"),
                          ("", "")])
    assert err is None and len(days) == 6 and not DR.is_split(days)


def test_a_bad_extra_range_names_which_one():
    _, err = DR.parse([("2026-11-01", "2026-11-02"), ("2026-12-20", "2026-12-10")])
    assert err == "The end date is before the start date (range 2)."
    _, err = DR.parse([("2026-11-01", "2026-11-02"), ("2026-12-20", "")])
    assert "range 2" in err


def test_the_plan_searches_every_range_and_the_limit_counts_them_together():
    host = _host()
    plan, err = host._market_plan("fare", "DAC-CXB", "2026-11-01", "2026-11-02",
                                  ["firsttrip"], "Economy", [["2026-12-24", "2026-12-25"]])
    assert err is None
    assert plan.dates == [date(2026, 11, 1), date(2026, 11, 2),
                          date(2026, 12, 24), date(2026, 12, 25)]
    # each range alone is within the limit; together they are not
    ranges = [["2027-03-01", "2027-03-31"], ["2027-05-01", "2027-05-31"]]
    assert 31 < host.MARKET_MAX_DAYS < 93
    _, err = host._market_plan("fare", "DAC-CXB", "2027-01-01", "2027-01-31",
                               ["firsttrip"], "Economy", ranges)
    assert err and "all ranges together" in err["error"]


def test_one_range_still_works_as_before():
    plan, err = _host()._market_plan("schedule", "DAC-CXB", "2026-11-01", "2026-11-07",
                                     ["firsttrip"], "Economy")
    assert err is None and len(plan.dates) == 7


def test_har_rows_from_the_gap_between_ranges_are_cut():
    rows = [_row(date(2026, 11, 1)), _row(date(2026, 11, 20)), _row(date(2026, 12, 15))]
    kept, _ = select_rows(rows, routes=[("DAC", "CXB")],
                          dates=[date(2026, 11, 1), date(2026, 12, 15)])
    assert [r.departure_date for r in kept] == [date(2026, 11, 1), date(2026, 12, 15)]


def test_a_week_split_by_the_gap_is_not_complete():
    # 2-8 Nov 2026 is Mon-Sun; ranges 2-5 Nov and 7-8 Nov leave the 6th out
    days, _ = DR.parse([("2026-11-02", "2026-11-05"), ("2026-11-07", "2026-11-15")])
    weeks = _complete_weeks(None, None, days)
    assert (2026, 45) not in weeks and (2026, 46) in weeks
