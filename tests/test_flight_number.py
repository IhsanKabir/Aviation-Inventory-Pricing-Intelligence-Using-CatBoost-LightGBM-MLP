"""One canonical flight number, whatever a channel called it."""

from core.flight_number import (canonical, carrier, key, numbers, same_flight)


def test_the_three_shapes_that_actually_occur_collapse_to_one_key():
    """Measured in the live column: BDFare writes 'BS 109', GoZayaan writes
    '101' from its hash hint, and one cell held two flights at once."""
    assert canonical("BS 109") == "109"
    assert canonical("101") == "101"
    assert canonical("BS 341,BS 344") == "341"


def test_a_cell_holding_two_flights_yields_both_when_asked():
    assert numbers("BS 341,BS 344") == ["341", "344"]
    assert numbers("BS 109") == ["109"]


def test_the_prefixed_and_bare_forms_join_to_the_same_key():
    """This is the bug that returned 0 matches against the airline's own
    schedule and read as 'the OTA data is worthless'."""
    assert key("BS", "BS 109") == key("BS", "109") == "BS109"


def test_the_airline_comes_from_the_row_not_the_cell():
    """GoZayaan rows are bare, and a number alone is not unique across
    carriers -- '101' is a real flight for more than one airline."""
    assert key("BS", "101") == "BS101"
    assert key("BG", "101") == "BG101"
    assert key("BS", "101") != key("BG", "101")


def test_a_flight_number_with_a_letter_suffix_survives():
    assert canonical("BS 341A") == "341A"
    assert numbers("341A") == ["341A"]


def test_leading_zeros_do_not_split_one_flight_into_two():
    assert canonical("BS 0109") == canonical("BS 109") == "109"


def test_the_same_flight_written_twice_is_not_two_flights():
    assert numbers("BS109 / BS 109") == ["109"]


def test_carrier_is_read_when_written_and_blank_when_not():
    assert carrier("BS 109") == "BS"
    assert carrier("101") == ""          # bare is normal, not an error


def test_nothing_readable_yields_nothing_rather_than_a_guess():
    for junk in ("", None, "n/a", "--"):
        assert canonical(junk) == ""
        assert numbers(junk) == []
        assert key("BS", junk) == ""


def test_same_flight_matches_across_shapes_including_the_packed_cell():
    assert same_flight("BS", "BS 341,BS 344", "BS", "341")
    assert same_flight("BS", "BS 109", "BS", "109")


def test_same_flight_refuses_a_different_carrier_on_the_same_number():
    assert not same_flight("BS", "101", "BG", "101")
    assert not same_flight("BS", "101", "BS", "102")
    assert not same_flight("", "101", "", "101")     # no airline, no match
