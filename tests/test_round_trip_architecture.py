import unittest

from core.trip_context import (
    TRIP_TYPE_ONE_WAY,
    TRIP_TYPE_ROUND_TRIP,
    apply_trip_context,
    build_trip_context,
    build_trip_search_windows,
    expand_iso_date_range,
    normalize_trip_type,
)
from modules import biman
from tools import export_bigquery_stage


class RoundTripArchitectureTests(unittest.TestCase):
    def test_normalize_trip_type_maps_common_values(self):
        self.assertEqual(TRIP_TYPE_ONE_WAY, normalize_trip_type("ow"))
        self.assertEqual(TRIP_TYPE_ROUND_TRIP, normalize_trip_type("round_trip"))
        self.assertEqual(TRIP_TYPE_ROUND_TRIP, normalize_trip_type("RT"))

    def test_build_trip_context_requires_return_date_for_round_trip(self):
        with self.assertRaises(ValueError):
            build_trip_context(
                origin="DAC",
                destination="DXB",
                departure_date="2026-03-10",
                cabin="Economy",
                adt=1,
                chd=0,
                inf=0,
                trip_type="RT",
            )

    def test_build_trip_context_generates_request_id_and_duration(self):
        context = build_trip_context(
            origin="DAC",
            destination="DXB",
            departure_date="2026-03-10",
            return_date="2026-03-15",
            cabin="Economy",
            adt=1,
            chd=0,
            inf=0,
            trip_type="RT",
        )

        self.assertEqual(TRIP_TYPE_ROUND_TRIP, context["search_trip_type"])
        self.assertEqual("2026-03-10", context["requested_outbound_date"])
        self.assertEqual("2026-03-15", context["requested_return_date"])
        self.assertEqual(5, context["trip_duration_days"])
        self.assertEqual(24, len(context["trip_request_id"]))

    def test_expand_iso_date_range_is_inclusive(self):
        self.assertEqual(
            ["2026-03-10", "2026-03-11", "2026-03-12"],
            expand_iso_date_range("2026-03-10", "2026-03-12"),
        )

    def test_build_trip_search_windows_supports_return_offsets(self):
        windows = build_trip_search_windows(
            outbound_dates=["2026-03-10", "2026-03-11"],
            trip_type="RT",
            return_offsets=[2, 4],
        )

        self.assertEqual(
            [
                {"departure_date": "2026-03-10", "return_date": "2026-03-12"},
                {"departure_date": "2026-03-10", "return_date": "2026-03-14"},
                {"departure_date": "2026-03-11", "return_date": "2026-03-13"},
                {"departure_date": "2026-03-11", "return_date": "2026-03-15"},
            ],
            windows,
        )

    def test_build_trip_search_windows_supports_absolute_return_ranges(self):
        windows = build_trip_search_windows(
            outbound_dates=["2026-03-10", "2026-03-12"],
            trip_type="RT",
            return_dates=["2026-03-09", "2026-03-12", "2026-03-13", "2026-03-15"],
        )

        self.assertEqual(
            [
                {"departure_date": "2026-03-10", "return_date": "2026-03-12"},
                {"departure_date": "2026-03-10", "return_date": "2026-03-13"},
                {"departure_date": "2026-03-10", "return_date": "2026-03-15"},
                {"departure_date": "2026-03-12", "return_date": "2026-03-12"},
                {"departure_date": "2026-03-12", "return_date": "2026-03-13"},
                {"departure_date": "2026-03-12", "return_date": "2026-03-15"},
            ],
            windows,
        )

    def test_apply_trip_context_defaults_outbound_leg_shape(self):
        context = build_trip_context(
            origin="DAC",
            destination="CXB",
            departure_date="2026-03-10",
            cabin="Economy",
            adt=1,
            chd=0,
            inf=0,
        )

        row = apply_trip_context({"airline": "BG", "flight_number": "BG121"}, context)

        self.assertEqual("outbound", row["leg_direction"])
        self.assertEqual(1, row["leg_sequence"])
        self.assertEqual(1, row["itinerary_leg_count"])
        self.assertEqual(context["trip_request_id"], row["trip_request_id"])

    def test_biman_payload_builds_two_itinerary_parts_for_round_trip(self):
        payload = biman.build_payload(
            "DAC",
            "DXB",
            "2026-03-10",
            cabin="Economy",
            adt=1,
            chd=0,
            inf=0,
            trip_type="RT",
            return_date="2026-03-15",
        )

        parts = payload["variables"]["airSearchInput"]["itineraryParts"]
        self.assertEqual(2, len(parts))
        self.assertEqual("DAC", parts[0]["from"]["code"])
        self.assertEqual("DXB", parts[1]["from"]["code"])
        self.assertEqual("2026-03-15", parts[1]["when"]["date"])

    def test_bigquery_offer_snapshot_export_includes_round_trip_columns(self):
        sql = export_bigquery_stage._query_fact_offer_snapshot()

        self.assertIn("search_trip_type", sql)
        self.assertIn("trip_request_id", sql)
        self.assertIn("requested_return_date", sql)
        self.assertIn("trip_pair_key", sql)
        self.assertIn("leg_direction", sql)


if __name__ == "__main__":
    unittest.main()
