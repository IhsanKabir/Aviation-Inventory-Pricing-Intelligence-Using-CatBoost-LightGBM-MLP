"""Tests for market_engine — the schedule/fare collection layer.

Weighted toward the traps found while building it: a synthetic midnight must not
become a departure, a cached fare must not be reused as today's price, a partial
week at the edge of a range must not read as a schedule change, and a flight that
retimes by day must not be reported as sources contradicting each other.
"""
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from engines import schedule_view as sv                     # noqa: E402
from market_engine import sources as S                      # noqa: E402
from market_engine.cache import FRESH_FARE, FRESH_SCHEDULE, CacheStore  # noqa: E402
from market_engine.collect import CollectResult             # noqa: E402
from market_engine.rows import from_connector, _split_dt    # noqa: E402
from market_engine.schedule import _complete_weeks, weekly_pattern  # noqa: E402


def _row(**kw):
    base = {"airline": "BS", "flight_number": "BS 101", "origin": "DAC",
            "destination": "CGP", "departure": "2026-10-07T07:00:00",
            "arrival": "2026-10-07T07:55:00", "stops": 0,
            "price_total_bdt": 5000, "fare_amount": 4000}
    base.update(kw)
    return from_connector(base, source="FirstTrip", cabin="Economy")


def test_midnight_is_not_a_departure():
    assert _split_dt("2026-10-07T00:00:00") == (date(2026, 10, 7), "00:00")
    r = _row(departure="2026-10-07T00:00:00")
    assert r.departure_time == "" and not r.has_clock     # neutralized, not trusted


def test_derived_base_and_overnight_are_flagged():
    assert _row().base_is_real is True
    assert _row(fare_amount=0).base_is_real is False      # source hid the base
    assert _row(arrival="2026-10-08T06:00:00").arrives_next_day is True


def test_cache_freshness_is_per_purpose():
    store = CacheStore(Path(tempfile.mkdtemp()) / "m.sqlite")
    store.put("FirstTrip", "DAC", "CGP", "2026-10-07", "Economy", [_row()])
    # A fare must not be served from a stale entry; the same entry is fine for a schedule.
    assert store.get("FirstTrip", "DAC", "CGP", "2026-10-07", "Economy",
                     max_age=timedelta(seconds=0)) is None
    assert store.get("FirstTrip", "DAC", "CGP", "2026-10-07", "Economy",
                     max_age=FRESH_SCHEDULE) is not None
    assert FRESH_FARE < FRESH_SCHEDULE


def test_source_capability_is_declared_not_assumed():
    """Flags come from what the parsers actually emit, checked on real captures."""
    # ShareTrip and GoZayaan HARs carry flight number + clock + price.
    for key in ("sharetrip", "gozayaan"):
        assert S.HAR[key].can_schedule and S.HAR[key].can_fare
    # Amy's agent rows have a clock and a price but no flight number.
    assert S.HAR["amy"].can_fare and not S.HAR["amy"].can_schedule
    assert "amy" not in {s.key for s in S.usable(S.HAR, purpose="schedule")}
    assert "amy" in {s.key for s in S.usable(S.HAR, purpose="fare")}
    # BDFare and AKIJ have no flight-row parser at all.
    for key in ("bdfare", "akij"):
        assert not S.HAR[key].can_schedule and not S.HAR[key].can_fare


def test_tallies_match_field_quality_contract():
    """judge() wants per-source aggregates, not raw offers."""
    res = CollectResult(rows=[_row(), _row(flight_number="BS 105", fare_amount=0)])
    t = res.tallies()
    assert len(t) == 1                                  # one source, not one per row
    assert t[0]["source"] == "FirstTrip" and t[0]["rows"] == 2
    assert t[0]["with_clock"] == 2 and t[0]["with_base"] == 1
    assert t[0]["airlines"] == ("BS",)


def test_partial_edge_weeks_never_count_as_variance():
    assert _complete_weeks(date(2026, 10, 1), date(2026, 10, 7)) == set()   # straddles 2
    assert len(_complete_weeks(date(2026, 10, 5), date(2026, 10, 11))) == 1  # Mon..Sun


def _sched(rows, d0, d1):
    return sv.build((r.as_schedule_row() for r in rows), sources_requested=("FirstTrip",),
                    date_from=d0, date_to=d1)


def test_retiming_by_day_is_not_a_source_disagreement():
    d0, d1 = date(2026, 10, 5), date(2026, 10, 11)
    rows = [_row(departure="2026-10-05T07:00:00", arrival="2026-10-05T07:55:00"),
            _row(departure="2026-10-08T09:30:00", arrival="2026-10-08T10:25:00")]
    pats = weekly_pattern(_sched(rows, d0, d1), date_from=d0, date_to=d1)
    assert len(pats) == 1
    assert pats[0].time_varies is True          # same flight, different clock by day
    assert pats[0].disagreement is False        # but no source contradicted another


def test_weekly_pattern_labels_days():
    d0, d1 = date(2026, 10, 5), date(2026, 10, 11)
    rows = [_row(departure=f"2026-10-{d:02d}T07:00:00", arrival=f"2026-10-{d:02d}T07:55:00")
            for d in range(5, 12)]
    p = weekly_pattern(_sched(rows, d0, d1), date_from=d0, date_to=d1)[0]
    assert p.weekday_label == "Daily" and p.dates == 7 and p.varies is False


def test_cheapest_per_flight_collapses_fare_classes():
    """One departure returned at three fare levels is ONE flight, at its lowest."""
    from market_engine.fares import cheapest_per_flight
    rows = [_row(price_total_bdt=p) for p in (9000, 5000, 7000)]
    kept = cheapest_per_flight(rows)
    assert len(kept) == 1 and kept[0].gross_bdt == 5000


def test_tax_model_learns_per_airline():
    from market_engine.fares import learn_tax, resolve_base
    rows = [_row(airline="BS", price_total_bdt=5174, fare_amount=4049),   # tax 1125
            _row(airline="BG", flight_number="BG 611",
                 price_total_bdt=5974, fare_amount=4749)]                 # tax 1225
    model = learn_tax(rows)
    assert model[("BS",)] == 1125 and model[("BG",)] == 1225
    # A source that hides the base gets it derived from its OWN airline's tax.
    hidden = _row(airline="BG", flight_number="BG 611",
                  price_total_bdt=6974, fare_amount=0)
    base, is_real = resolve_base(hidden, model)
    assert base == 5749 and is_real is False


def test_derived_base_is_flagged_in_the_cell():
    from market_engine.fares import build_fares
    res = CollectResult(rows=[
        _row(price_total_bdt=5174, fare_amount=4049),
        _row(flight_number="BS 105", price_total_bdt=6174, fare_amount=0)])
    t = build_fares(res)
    # 50% base coverage fails the 80% bar, so base is NOT presented -- but the
    # observed gross still is, rather than the whole source being thrown away.
    assert len(t.cells) == 1
    assert t.cells[0].base_available is False
    assert t.cells[0].gross_low == 5174
    assert "FirstTrip" in t.gross_only


def test_cheapest_by_route_picks_lowest():
    from market_engine.fares import build_fares, cheapest_by_route
    res = CollectResult(rows=[
        _row(airline="BS", price_total_bdt=5000, fare_amount=4000),
        _row(airline="BG", flight_number="BG 611", price_total_bdt=4000, fare_amount=3000)])
    t = build_fares(res)
    assert cheapest_by_route(t.cells)["DAC-CGP"] == "BG"


def test_market_plan_validates_before_any_network_work():
    """Bad input must fail fast with a message a teammate can act on."""
    from desktop.market_api import MarketApiMixin

    class _Api(MarketApiMixin):
        pass

    api = _Api()
    ok, err = api._market_plan("fare", "DAC-CGP", "2026-10-01", "2026-10-07",
                               ["firsttrip"], "Economy")
    assert err is None and len(ok.dates) == 7 and ok.routes == [("DAC", "CGP")]

    for routes, d0, d1, srcs, expect in (
            ("DACCGP", "2026-10-01", "2026-10-07", ["firsttrip"], "DAC-CGP"),
            ("DAC-CGP", "01-10-2026", "2026-10-07", ["firsttrip"], "YYYY-MM-DD"),
            ("DAC-CGP", "2026-10-07", "2026-10-01", ["firsttrip"], "before"),
            ("DAC-CGP", "2026-10-01", "2026-10-07", [], "at least one source"),
            ("", "2026-10-01", "2026-10-07", ["firsttrip"], "at least one route"),
            ("DAC-CGP", "2026-01-01", "2026-12-31", ["firsttrip"], "limit")):
        plan, err = api._market_plan("fare", routes, d0, d1, srcs, "Economy")
        assert plan is None and expect in err["error"], (routes, d0, err)


def test_schedule_purpose_rejects_sources_that_cannot_answer_it():
    from desktop.market_api import MarketApiMixin
    from market_engine import sources as S

    class _Api(MarketApiMixin):
        pass

    # Every LIVE source can do both today; the guard is exercised by asking for a
    # purpose no picked source supports.
    assert all(s.can_schedule for s in S.LIVE.values())
    plan, err = _Api()._market_plan("schedule", "DAC-CGP", "2026-10-01",
                                    "2026-10-07", ["nope"], "Economy")
    assert plan is None and "at least one source" in err["error"]


def test_har_capability_reflects_the_parsers(tmp_path):
    """Declared capability must match what a parser can actually produce."""
    from market_engine import sources as S
    from market_engine.har import PARSERS
    # Channels with no flight-row parser must be unusable for BOTH views.
    for key in ("bdfare", "akij"):
        assert key not in PARSERS
        assert not S.HAR[key].can_schedule and not S.HAR[key].can_fare
    # Amy's agent rows carry no flight number, so fares only.
    assert "amy" in PARSERS
    assert S.HAR["amy"].can_fare and not S.HAR["amy"].can_schedule


def test_har_collector_explains_every_skipped_file(tmp_path):
    """A file that contributes nothing must say why, never fail silently."""
    from market_engine.har import collect_har_rows
    (tmp_path / "mystery.har").write_text("{}", encoding="utf-8")
    rows, notes = collect_har_rows(tmp_path, purpose="fare")
    assert rows == []
    assert any("mystery.har" in n for n in notes)

    empty, why = collect_har_rows(tmp_path / "nope", purpose="fare")
    assert empty == [] and why and "No .har files" in why[0]


def test_har_only_selection_is_allowed():
    """A teammate who cannot fetch live may run on captures alone."""
    from desktop.market_api import MarketApiMixin

    class _Api(MarketApiMixin):
        pass

    plan, err = _Api()._market_plan("fare", "DAC-CGP", "2026-10-01", "2026-10-07",
                                    ["har"], "Economy")
    assert err is None and plan.use_har is True and plan.source_keys == []


def test_har_coverage_reports_actual_days(tmp_path):
    from market_engine.har import coverage
    rows = [_row(departure="2026-10-07T07:00:00"), _row(departure="2026-10-09T07:00:00")]
    cov = coverage(rows)
    assert cov["DAC-CGP"] == [date(2026, 10, 7), date(2026, 10, 9)]


if __name__ == "__main__":
    import subprocess
    raise SystemExit(subprocess.call([sys.executable, "-m", "pytest", __file__, "-q"]))
