"""Whether a source's rows are usable, not merely present."""

from datetime import date, timedelta

from core import field_quality as fq


def _tally(**kw):
    base = {"source": "s", "rows": 100, "with_clock": 100, "with_base": 100}
    base.update(kw)
    return base


def test_a_healthy_source_passes():
    q = fq.judge([_tally(source="sharetrip:v2/search",
                         last_scraped=date.today())])
    (s,) = q.sources
    assert s.verdict == fq.OK
    assert s.schedule_grade and s.fare_grade
    assert q.failing == []


def test_the_failure_that_ran_for_five_months_is_now_caught():
    """A channel served BS and 2A with a synthetic midnight clock from April.
    Rows kept arriving, so every existing gate stayed green."""
    q = fq.judge([_tally(source="api/flight/v4.0/search/legs/fares",
                         rows=33079, with_clock=14, with_base=33079,
                         last_scraped=date.today())])
    (s,) = q.sources
    assert s.verdict == fq.NO_CLOCK
    assert not s.schedule_grade
    assert s.fare_grade               # its fares are fine; only the clock is not
    assert q.clock_regression == [s]
    assert q.schedule_grade == []


def test_a_source_with_no_base_fare_cannot_support_fare_comparison():
    q = fq.judge([_tally(with_base=0, last_scraped=date.today())])
    (s,) = q.sources
    assert not s.fare_grade
    assert s.schedule_grade           # the clock is still fine
    assert fq.NO_BASE in s.problems


def test_staleness_is_reported_but_does_not_disqualify_history():
    """Old rows still describe the days they cover; refusing them would hide
    history. The reader is told the period is not current instead."""
    old = date.today() - timedelta(days=60)
    q = fq.judge([_tally(last_scraped=old)])
    (s,) = q.sources
    assert fq.STALE in s.problems
    assert s.age_days == 60
    assert s.schedule_grade           # still usable for the dates it holds


def test_an_empty_source_is_empty_not_broken():
    q = fq.judge([_tally(rows=0, with_clock=0, with_base=0)])
    (s,) = q.sources
    assert s.verdict == fq.EMPTY
    assert not s.schedule_grade and not s.fare_grade
    assert s.clock_share == 0.0       # no division by zero


def test_the_clock_threshold_sits_where_the_real_failure_sat():
    """Observed: the degraded channel at 0%, every healthy one at 100%. A
    source just under the bar must fail, just over must pass."""
    under = fq.judge([_tally(rows=100, with_clock=79)]).sources[0]
    over = fq.judge([_tally(rows=100, with_clock=81)]).sources[0]
    assert not under.schedule_grade
    assert over.schedule_grade


def test_sources_are_ordered_by_size_so_the_big_ones_are_seen_first():
    q = fq.judge([_tally(source="small", rows=10),
                  _tally(source="big", rows=1000)])
    assert [s.source for s in q.sources] == ["big", "small"]


def test_explain_is_readable_and_names_the_problem():
    s = fq.judge([_tally(source="goz", rows=33079, with_clock=14,
                         last_scraped=date.today())]).sources[0]
    line = s.explain()
    assert "NOT usable" in line
    assert fq.NO_CLOCK in line
    assert "clock 0%" in line


def test_a_source_never_collected_reports_no_age_rather_than_zero():
    s = fq.judge([_tally(last_scraped=None)]).sources[0]
    assert s.age_days is None
    assert fq.STALE not in s.problems     # unknown is not the same as stale
