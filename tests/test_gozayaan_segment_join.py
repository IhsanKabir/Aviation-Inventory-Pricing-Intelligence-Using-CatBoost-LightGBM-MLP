"""The segment join that silently produced five months of midnight clocks.

From April to August this connector served BS and 2A with `departure` set to
midnight -- 33,079 rows, 14 of them with a real time -- because the segment
join accepted exactly one payload shape and returned `[]` for anything else.
The row builder then invented a departure from the date in `hash_str` (which
carries no time) and a stop count of zero from an empty list.
"""

from modules.gozayaan import _normalize_fare_row, _resolve_segments

FARE = {"id": "f-1", "hash": "h1", "leg_hashes": ["h1"],
        "hash_str": "BS|DAC-CXB-2026-04-13-BS-157-AT7",
        "currency": "BDT", "total_base_amount": 4024,
        "total_fare_amount": 5149}
SEG = {"hash": "s1", "flight_number": "157", "origin": "DAC",
       "destination": "CXB", "departure_date_time": "2026-04-13T09:40:00",
       "arrival_date_time": "2026-04-13T10:35:00", "equipment": "AT7"}


def _row(segments, reason=""):
    return _normalize_fare_row(
        airline_code="BS", search_id="s-1", leg_hash="h1", fare=FARE,
        leg={"hash": "h1"}, segments=segments, segment_reason=reason,
        policies=[], requested_cabin="Economy", adt=1, chd=0, inf=0)


# --- the resolver accepts every shape the payload has been seen to use ----

def test_the_documented_shape_resolves():
    segs, reason = _resolve_segments(
        FARE, {"hash": "h1", "segment_hashes": ["s1"]}, {}, {"s1": SEG})
    assert segs == [SEG] and reason == ""


def test_a_leg_naming_the_field_segments_also_resolves():
    segs, reason = _resolve_segments(
        FARE, {"hash": "h1", "segments": ["s1"]}, {}, {"s1": SEG})
    assert segs == [SEG] and reason == ""


def test_a_leg_carrying_whole_segments_resolves_without_a_lookup():
    segs, reason = _resolve_segments(
        FARE, {"hash": "h1", "segments": [SEG]}, {}, {})
    assert segs == [SEG] and reason == ""


def test_hashes_on_the_fare_itself_resolve():
    fare = dict(FARE, segment_hashes=["s1"])
    segs, reason = _resolve_segments(fare, {"hash": "h1"}, {}, {"s1": SEG})
    assert segs == [SEG] and reason == ""


# --- and when it genuinely cannot, it says why ---------------------------

def test_a_missing_leg_is_named_not_returned_as_an_empty_list():
    segs, reason = _resolve_segments(FARE, None, {}, {})
    assert segs == []
    assert reason == "leg hash not in payload"


def test_a_leg_with_no_segment_reference_is_named():
    segs, reason = _resolve_segments(FARE, {"hash": "h1"}, {}, {})
    assert segs == []
    assert "no segment reference" in reason


def test_unmatched_segment_hashes_are_named_with_a_count():
    segs, reason = _resolve_segments(
        FARE, {"hash": "h1", "segment_hashes": ["nope"]}, {}, {"s1": SEG})
    assert segs == []
    assert "none of 1 segment hashes matched" in reason


def test_a_partial_match_still_returns_what_it_found_and_says_so():
    segs, reason = _resolve_segments(
        FARE, {"hash": "h1", "segment_hashes": ["s1", "nope"]}, {},
        {"s1": SEG})
    assert segs == [SEG]
    assert "1 of 2 segment hashes unmatched" in reason


# --- the row never invents a time or a stop count ------------------------

def test_a_resolved_segment_gives_a_real_departure_time():
    row = _row([SEG])
    assert row["departure"] == "2026-04-13T09:40:00"
    assert row["departure_time_known"] is True
    assert row["stops"] == 0              # one segment really is direct


def test_no_segments_means_the_time_is_declared_unknown_not_midnight():
    """The date is still emitted -- storage needs one and the offer is real --
    but the clock is flagged, so a schedule can exclude the row instead of
    placing the flight at the start of the day."""
    row = _row([], reason="leg carries no segment reference")
    assert row["departure"].endswith("T00:00:00")
    assert row["departure_time_known"] is False
    assert row["segment_reason"] == "leg carries no segment reference"


def test_no_segments_means_the_stop_count_is_unknown_not_zero():
    """Reporting 0 asserted 'direct flight, no via' about a row that carried
    no routing information at all."""
    row = _row([])
    assert row["stops"] is None
    assert row["via_airports"] is None


def test_a_two_segment_leg_reports_one_stop_and_the_via():
    seg2 = dict(SEG, hash="s2", origin="CXB", destination="CGP",
                departure_date_time="2026-04-13T11:30:00",
                arrival_date_time="2026-04-13T12:20:00")
    row = _row([SEG, seg2])
    assert row["stops"] == 1
    assert row["via_airports"] == "CXB"
    assert row["destination"] == "CGP"


def test_the_flight_number_comes_from_the_segment_when_it_resolves():
    """The bare '157' in the live column is the hash-hint fallback firing.
    With a segment present the number is read from the flight itself."""
    row = _row([SEG])
    assert row["flight_number"] == "157"
    assert row["departure_time_known"] is True
