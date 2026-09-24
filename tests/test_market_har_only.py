"""Schedule / Fare from HAR captures only - the path every non-admin user takes."""
from __future__ import annotations

import sys
import tempfile
from datetime import date, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from desktop import backend as backend_mod  # noqa: E402
from market_engine import har as har_mod  # noqa: E402
from market_engine.rows import FlightRow  # noqa: E402


def _row(route: str, day: date, airline: str = "EK", number: str = "585",
         gross: float = 40000.0) -> FlightRow:
    o, d = route.split("-")
    return FlightRow(source="ShareTrip (HAR)", airline=airline, flight_number=number,
                     origin=o, destination=d, departure_date=day, departure_time="01:00",
                     arrival_time="04:25", arrival_date=day, stops=0, via="", aircraft="",
                     operated_by="", cabin="Economy", gross_bdt=gross, base_bdt=gross * 0.7,
                     base_is_real=True, seats=None, fetched_at=datetime(2026, 9, 24))


ROWS = [_row("DAC-DXB", date(2026, 10, 31)), _row("DAC-DXB", date(2026, 10, 31), "BS", "343"),
        _row("DAC-CXB", date(2026, 9, 30), "BS", "141", 4349.0), _row("AUH-CGP", date(2026, 10, 31))]


def test_rows_are_cut_to_the_routes_and_dates_asked_for():
    kept, note = har_mod.select_rows(ROWS, routes=[("DAC", "DXB")],
                                     date_from=date(2026, 10, 31), date_to=date(2026, 10, 31))
    assert {(r.origin, r.destination) for r in kept} == {("DAC", "DXB")} and len(kept) == 2
    assert note is None


def test_blank_routes_and_dates_mean_everything_the_captures_hold():
    kept, note = har_mod.select_rows(ROWS)
    assert kept == ROWS and note is None


def test_an_empty_answer_names_what_the_captures_do_cover():
    _, note = har_mod.select_rows(ROWS, routes=[("DAC", "DXB")],
                                  date_from=date(2026, 10, 1), date_to=date(2026, 10, 7))
    assert note == ("Your HAR captures have no flights in those dates. "
                    "They cover: DAC-DXB (31 Oct)")
    _, note = har_mod.select_rows(ROWS, routes=[("DAC", "HKG")])
    assert note.startswith("Your HAR captures have none of the routes asked for. "
                           "They cover: AUH-CGP (31 Oct); DAC-CXB (30 Sep); DAC-DXB (31 Oct)")


@pytest.fixture()
def teammate(monkeypatch):
    """A signed-in, approved, NON-admin user with a capture folder."""
    tmp = Path(tempfile.mkdtemp())
    monkeypatch.setattr(backend_mod, "config_dir", lambda: tmp)
    monkeypatch.setattr(backend_mod, "_HAS_KEYRING", False)
    api = backend_mod.DesktopApi()
    api._store_token("tok")
    api._config.update(har_dir=str(tmp), is_admin=False)
    monkeypatch.setattr(api, "check_access",
                        lambda: {"status": "approved", "allowed": True, "is_admin": False})
    monkeypatch.setattr(api, "_log_usage", lambda *a, **k: None)
    monkeypatch.setattr(har_mod, "collect_har_rows",
                        lambda d, purpose="fare", progress=None, channels=None:
                        (list(ROWS), ["bdfare.har: BDFare captures carry discounts, not "
                                      "flight details, so they are used on the Discounts tab only"]))
    return api


def test_har_only_needs_no_route_or_dates_but_live_does(teammate):
    plan, err = teammate._market_plan("fare", "", "", "", ["har"], "Economy")
    assert err is None and plan.routes == [] and plan.dates == []
    teammate._config["is_admin"] = True
    _plan, err = teammate._market_plan("fare", "", "", "", ["firsttrip"], "Economy")
    assert "Enter at least one route" in err["error"]


def test_teammate_fare_run_answers_only_the_question_asked(teammate):
    r = teammate.run_market("fare", "DAC-DXB", "2026-10-31", "2026-10-31", ["har"])
    assert r["ok"] and {c["route"] for c in r["cells"]} == {"DAC-DXB"}
    assert r["har_notes"][0] == "Your HAR files cover: DAC-DXB (31 Oct)"
    assert any("Discounts tab only" in n for n in r["har_notes"])        # notes reach the page
    assert teammate.get_progress("fare")["state"] == "complete"


def test_teammate_blank_run_shows_all_captures_over_their_own_dates(teammate):
    r = teammate.run_market("schedule", "", "", "", ["har"])
    assert r["ok"] and r["rows"] == len(ROWS)
    assert (r["date_from"], r["date_to"]) == ("2026-09-30", "2026-10-31")
    assert teammate.market_estimate("schedule", "", "", "", ["har"])["har_only"] is True


def test_teammate_empty_answer_is_explained(teammate):
    r = teammate.run_market("fare", "DAC-DXB", "2026-10-01", "2026-10-07", ["har"])
    assert r["ok"] and r["cells"] == []
    assert r["har_notes"][0].startswith("Your HAR captures have no flights in those dates")
