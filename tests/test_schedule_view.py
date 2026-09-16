"""A route schedule over a date range, from sources the operator chose."""

from datetime import date

from core import field_quality as fq
from engines import schedule_view as sv


def _row(**kw):
    base = {"airline": "BS", "flight_number": "BS 109",
            "departure_date": date(2026, 6, 1), "departure_time": "15:10",
            "arrival_time": "16:05", "origin": "DAC", "destination": "CGP",
            "source": "bdfare:v2/Search/GetAirSearch", "aircraft": "AT7",
            "seats": 70, "stops": 0, "via": None}
    base.update(kw)
    return base


# --- the midnight rule ----------------------------------------------------

def test_a_synthetic_midnight_is_dropped_and_counted_never_placed():
    """A channel served five months of rows with departure at 00:00. Any
    schedule built from them would put every flight at the start of the day."""
    res = sv.build([_row(departure_time="00:00"), _row()])
    assert len(res.legs) == 1
    assert res.dropped_no_clock == 1
    assert res.legs[0].departure == "15:10"


def test_a_row_with_no_time_at_all_is_dropped_too():
    res = sv.build([_row(departure_time=""), _row(departure_time=None)])
    assert res.legs == []
    assert res.dropped_no_clock == 2


# --- disagreement is reported, not resolved -------------------------------

def test_sources_disagreeing_on_a_time_are_reported_not_silently_merged():
    """'First non-empty wins' hides this; 1,403 collected flight-days carry
    more than one departure time."""
    res = sv.build([_row(source="a", departure_time="15:10"),
                    _row(source="b", departure_time="15:40")])
    (leg,) = res.legs
    assert leg.reported_times == {"15:10", "15:40"}
    assert leg.sources_disagree
    assert leg.confidence == "sources disagree"
    assert res.disagreements == [leg]


def test_two_sources_agreeing_are_marked_corroborated():
    res = sv.build([_row(source="a"), _row(source="b")])
    (leg,) = res.legs
    assert leg.sources == {"a", "b"}
    assert not leg.sources_disagree
    assert leg.confidence == "corroborated"


def test_one_source_says_so():
    (leg,) = sv.build([_row()]).legs
    assert leg.confidence == "single source"


# --- marketed O&D is not an operated leg ----------------------------------

def test_a_row_with_a_stop_is_an_itinerary_and_kept_out_of_the_legs():
    """An OTA sells DAC->DXB for a flight that operates DAC-CGP then
    CGP-DXB. Treating that as a leg mis-states the routing."""
    res = sv.build([_row(destination="DXB", stops=1, via="CGP"), _row()])
    assert len(res.legs) == 1
    assert res.itineraries == 1
    assert res.legs[0].route == "DAC-CGP"


def test_itineraries_can_be_asked_for_explicitly():
    res = sv.build([_row(destination="DXB", stops=1, via="CGP")],
                   include_itineraries=True)
    (leg,) = res.legs
    assert leg.kind == "itinerary"
    assert not leg.is_direct
    assert leg.via == "CGP"


# --- the join key ---------------------------------------------------------

def test_the_prefixed_and_bare_flight_forms_merge_into_one_leg():
    res = sv.build([_row(flight_number="BS 109", source="bdfare"),
                    _row(flight_number="109", source="goz")])
    assert len(res.legs) == 1
    assert res.legs[0].sources == {"bdfare", "goz"}


def test_a_row_with_no_readable_flight_number_is_dropped_and_counted():
    res = sv.build([_row(flight_number=""), _row(airline="")])
    assert res.legs == []
    assert res.dropped_no_flight == 2


# --- source selection -----------------------------------------------------

def _quality():
    return fq.judge([
        {"source": "healthy", "rows": 100, "with_clock": 100, "with_base": 100},
        {"source": "noclock", "rows": 100, "with_clock": 0, "with_base": 100},
        {"source": "nobase", "rows": 100, "with_clock": 100, "with_base": 0},
    ])


def test_the_operator_choice_is_honoured():
    accepted, refused = sv.choose_sources(_quality(), ["healthy"])
    assert accepted == ("healthy",)
    assert refused == {}


def test_choosing_a_degraded_source_refuses_it_with_a_reason():
    accepted, refused = sv.choose_sources(_quality(), ["healthy", "noclock"])
    assert accepted == ("healthy",)
    assert refused == {"noclock": fq.NO_CLOCK}


def test_choosing_all_still_cannot_readmit_a_degraded_source():
    """'All' is the dangerous default -- it is how the midnight clock reached
    five months of data. It accepts only what can support the purpose."""
    accepted, refused = sv.choose_sources(_quality(), None)
    assert "noclock" not in accepted
    assert refused["noclock"] == fq.NO_CLOCK


def test_a_source_that_was_never_collected_is_named_as_such():
    _accepted, refused = sv.choose_sources(_quality(), ["ghost"])
    assert refused == {"ghost": "not collected"}


def test_fare_purpose_judges_on_base_not_on_the_clock():
    accepted, refused = sv.choose_sources(_quality(), None, purpose="fare")
    assert "noclock" in accepted          # its fares are fine
    assert refused["nobase"] == fq.NO_BASE


# --- gaps are stated -----------------------------------------------------

def test_dates_with_no_legs_are_listed_rather_than_left_to_be_noticed():
    """A gap is a fact about collection, not an empty day of flying."""
    res = sv.build([_row(departure_date=date(2026, 6, 1)),
                    _row(departure_date=date(2026, 6, 3))],
                   date_from=date(2026, 6, 1), date_to=date(2026, 6, 4))
    assert res.missing_dates == [date(2026, 6, 2), date(2026, 6, 4)]


def test_no_range_means_no_gap_claim():
    res = sv.build([_row()])
    assert res.missing_dates == []


# --- shape ---------------------------------------------------------------

def test_legs_are_ordered_by_date_then_departure():
    res = sv.build([
        _row(departure_date=date(2026, 6, 2), departure_time="09:00"),
        _row(departure_date=date(2026, 6, 1), departure_time="18:00"),
        _row(departure_date=date(2026, 6, 1), departure_time="07:00",
             flight_number="101"),
    ])
    assert [(l.flight_date.day, l.departure) for l in res.legs] == [
        (1, "07:00"), (1, "18:00"), (2, "09:00")]


def test_the_summary_counts_what_it_says():
    res = sv.build([_row(), _row(flight_number="101", departure_time="07:00")])
    assert res.flights == 2
    assert res.routes == {"DAC-CGP"}
    assert res.dates_covered == 1
    assert "2 leg(s)" in res.summary()


def test_nothing_in_is_not_an_error():
    res = sv.build([])
    assert res.legs == [] and res.rows_read == 0
    assert res.summary()
