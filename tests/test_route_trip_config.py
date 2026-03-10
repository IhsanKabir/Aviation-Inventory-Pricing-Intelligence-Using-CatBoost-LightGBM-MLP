import json
import unittest
from datetime import date
from pathlib import Path
from uuid import uuid4

from core.trip_config import (
    load_route_trip_overrides,
    match_route_trip_override,
    resolve_route_trip_plan,
)


class RouteTripConfigTests(unittest.TestCase):
    def test_load_route_trip_overrides_supports_route_string_and_offsets(self):
        payload = {
            "routes": [
                {
                    "airline": "BG",
                    "route": "DAC-CXB",
                    "trip_type": "RT",
                    "return_date_offsets": [2, 4],
                },
                {
                    "route": "DAC-SPD",
                    "trip_type": "OW",
                },
            ]
        }
        temp_dir = Path("output/test_tmp")
        temp_dir.mkdir(parents=True, exist_ok=True)
        path = temp_dir / f"route_trip_windows_{uuid4().hex}.json"
        try:
            path.write_text(json.dumps(payload), encoding="utf-8")
            overrides = load_route_trip_overrides(path, today=date(2026, 3, 10))
        finally:
            if path.exists():
                path.unlink()

        self.assertEqual(2, len(overrides))
        self.assertEqual("BG", overrides[0]["airline"])
        self.assertEqual("DAC", overrides[0]["origin"])
        self.assertEqual("CXB", overrides[0]["destination"])
        self.assertEqual("RT", overrides[0]["trip_type"])
        self.assertEqual([2, 4], overrides[0]["return_offsets"])
        self.assertIsNone(overrides[1]["airline"])

    def test_load_route_trip_overrides_supports_named_profiles(self):
        payload = {
            "profiles": {
                "bg_domestic_rt": {
                    "trip_type": "RT",
                    "return_date_offsets": [1, 2, 3],
                }
            },
            "routes": [
                {
                    "airline": "BG",
                    "route": "DAC-CXB",
                    "profile": "bg_domestic_rt",
                }
            ],
        }
        temp_dir = Path("output/test_tmp")
        temp_dir.mkdir(parents=True, exist_ok=True)
        path = temp_dir / f"route_trip_windows_{uuid4().hex}.json"
        try:
            path.write_text(json.dumps(payload), encoding="utf-8")
            overrides = load_route_trip_overrides(path, today=date(2026, 3, 10))
        finally:
            if path.exists():
                path.unlink()

        self.assertEqual(1, len(overrides))
        self.assertEqual("RT", overrides[0]["trip_type"])
        self.assertEqual([1, 2, 3], overrides[0]["return_offsets"])

    def test_load_route_trip_overrides_supports_grouped_airline_blocks(self):
        payload = {
            "profiles": {
                "ow_default": {"trip_type": "OW"},
                "bg_domestic_rt": {"trip_type": "RT", "return_date_offsets": [1, 2, 3]},
            },
            "airlines": {
                "BG": {
                    "default_profile": "ow_default",
                    "routes": {
                        "DAC-CXB": {"profile": "bg_domestic_rt"},
                        "DAC-CGP": {},
                    },
                }
            },
        }
        temp_dir = Path("output/test_tmp")
        temp_dir.mkdir(parents=True, exist_ok=True)
        path = temp_dir / f"route_trip_windows_{uuid4().hex}.json"
        try:
            path.write_text(json.dumps(payload), encoding="utf-8")
            overrides = load_route_trip_overrides(path, today=date(2026, 3, 10))
        finally:
            if path.exists():
                path.unlink()

        self.assertEqual(2, len(overrides))
        first = next(item for item in overrides if item["destination"] == "CXB")
        second = next(item for item in overrides if item["destination"] == "CGP")
        self.assertEqual("RT", first["trip_type"])
        self.assertEqual([1, 2, 3], first["return_offsets"])
        self.assertEqual("OW", second["trip_type"])

    def test_match_route_trip_override_prefers_airline_specific_rule(self):
        overrides = [
            {
                "airline": None,
                "origin": "DAC",
                "destination": "CXB",
                "trip_type": "OW",
                "outbound_dates": [],
                "return_dates": [],
                "return_offsets": [],
                "source": "wildcard",
            },
            {
                "airline": "BG",
                "origin": "DAC",
                "destination": "CXB",
                "trip_type": "RT",
                "outbound_dates": [],
                "return_dates": [],
                "return_offsets": [3],
                "source": "specific",
            },
        ]

        matched = match_route_trip_override(overrides, airline="BG", origin="DAC", destination="CXB")
        self.assertEqual("specific", matched["source"])

    def test_resolve_route_trip_plan_applies_route_override_over_global(self):
        route_override = {
            "airline": "BG",
            "origin": "DAC",
            "destination": "CXB",
            "trip_type": "RT",
            "outbound_dates": ["2026-03-12", "2026-03-13"],
            "return_dates": [],
            "return_offsets": [2, 5],
            "source": "route_trip_windows.json[0]",
        }

        plan = resolve_route_trip_plan(
            base_outbound_dates=["2026-03-10"],
            base_trip_type="OW",
            base_return_dates=[],
            base_return_offsets=[],
            route_override=route_override,
            limit_dates=None,
        )

        self.assertEqual("RT", plan["trip_type"])
        self.assertEqual(["2026-03-12", "2026-03-13"], plan["outbound_dates"])
        self.assertEqual(
            [
                {"departure_date": "2026-03-12", "return_date": "2026-03-14"},
                {"departure_date": "2026-03-12", "return_date": "2026-03-17"},
                {"departure_date": "2026-03-13", "return_date": "2026-03-15"},
                {"departure_date": "2026-03-13", "return_date": "2026-03-18"},
            ],
            plan["search_windows"],
        )

    def test_resolve_route_trip_plan_can_force_one_way_on_specific_route(self):
        route_override = {
            "airline": "VQ",
            "origin": "DAC",
            "destination": "SPD",
            "trip_type": "OW",
            "outbound_dates": [],
            "return_dates": [],
            "return_offsets": [],
            "source": "route_trip_windows.json[1]",
        }

        plan = resolve_route_trip_plan(
            base_outbound_dates=["2026-03-10", "2026-03-12"],
            base_trip_type="RT",
            base_return_dates=["2026-03-15"],
            base_return_offsets=[],
            route_override=route_override,
            limit_dates=None,
        )

        self.assertEqual("OW", plan["trip_type"])
        self.assertEqual(
            [
                {"departure_date": "2026-03-10", "return_date": None},
                {"departure_date": "2026-03-12", "return_date": None},
            ],
            plan["search_windows"],
        )


if __name__ == "__main__":
    unittest.main()
