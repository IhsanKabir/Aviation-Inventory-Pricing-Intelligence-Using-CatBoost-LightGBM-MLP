import json
import unittest
from datetime import date
from pathlib import Path
from uuid import uuid4
from unittest.mock import patch

from core.trip_config import (
    load_route_trip_overrides,
    match_route_trip_override,
    match_route_trip_overrides,
    resolve_route_trip_plan,
)


class RouteTripConfigTests(unittest.TestCase):
    def test_load_route_trip_overrides_drops_past_absolute_outbound_dates(self):
        payload = {
            "routes": [
                {
                    "airline": "BG",
                    "route": "DAC-CXB",
                    "trip_type": "OW",
                    "dates": ["2026-03-09", "2026-03-10", "2026-03-11", "2026-03-15"],
                }
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

        self.assertEqual(1, len(overrides))
        self.assertEqual(["2026-03-10", "2026-03-11", "2026-03-15"], overrides[0]["outbound_dates"])

    def test_load_route_trip_overrides_drops_past_absolute_return_dates(self):
        payload = {
            "routes": [
                {
                    "airline": "BG",
                    "route": "DAC-CXB",
                    "trip_type": "RT",
                    "dates": ["2026-03-10"],
                    "return_dates": ["2026-03-09", "2026-03-10", "2026-03-12"],
                }
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

        self.assertEqual(1, len(overrides))
        self.assertEqual(["2026-03-10", "2026-03-12"], overrides[0]["return_dates"])

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

    def test_load_route_trip_overrides_supports_market_trip_profiles(self):
        payload = {
            "routes": [
                {
                    "airline": "BS",
                    "route": "DAC-JED",
                    "market_trip_profile": "labor_me_rt",
                }
            ]
        }
        priors = {
            "trip_date_profiles": {
                "labor_me_rt": {
                    "trip_type": "RT",
                    "day_offsets": [7, 14, 21],
                    "return_date_offsets": [7, 14, 21],
                }
            }
        }
        temp_dir = Path("output/test_tmp")
        temp_dir.mkdir(parents=True, exist_ok=True)
        path = temp_dir / f"route_trip_windows_{uuid4().hex}.json"
        try:
            path.write_text(json.dumps(payload), encoding="utf-8")
            with patch("core.trip_config.load_market_priors", return_value=priors):
                overrides = load_route_trip_overrides(path, today=date(2026, 3, 10))
        finally:
            if path.exists():
                path.unlink()

        self.assertEqual(1, len(overrides))
        self.assertEqual("RT", overrides[0]["trip_type"])
        self.assertEqual(["2026-03-17", "2026-03-24", "2026-03-31"], overrides[0]["outbound_dates"])
        self.assertEqual([7, 14, 21], overrides[0]["return_offsets"])

    def test_load_route_trip_overrides_supports_market_trip_profiles_with_offset_ranges(self):
        payload = {
            "routes": [
                {
                    "airline": "BS",
                    "route": "DAC-JED",
                    "market_trip_profile": "worker_visa_outbound_to_middle_east_one_way",
                }
            ]
        }
        priors = {
            "trip_date_profiles": {
                "worker_visa_outbound_to_middle_east_one_way": {
                    "trip_type": "OW",
                    "day_offset_ranges": [{"start": 7, "end": 10}],
                }
            }
        }
        temp_dir = Path("output/test_tmp")
        temp_dir.mkdir(parents=True, exist_ok=True)
        path = temp_dir / f"route_trip_windows_{uuid4().hex}.json"
        try:
            path.write_text(json.dumps(payload), encoding="utf-8")
            with patch("core.trip_config.load_market_priors", return_value=priors):
                overrides = load_route_trip_overrides(path, today=date(2026, 3, 10))
        finally:
            if path.exists():
                path.unlink()

        self.assertEqual(1, len(overrides))
        self.assertEqual("OW", overrides[0]["trip_type"])
        self.assertEqual(
            ["2026-03-17", "2026-03-18", "2026-03-19", "2026-03-20"],
            overrides[0]["outbound_dates"],
        )

    def test_load_route_trip_overrides_supports_market_trip_profiles_with_date_ranges(self):
        payload = {
            "routes": [
                {
                    "airline": "BG",
                    "route": "DAC-CXB",
                    "market_trip_profile": "bangladesh_domestic_eid_round_trip_2026",
                }
            ]
        }
        priors = {
            "trip_date_profiles": {
                "bangladesh_domestic_eid_round_trip_2026": {
                    "trip_type": "RT",
                    "date_ranges": [{"start": "2026-03-11", "end": "2026-03-20"}],
                    "return_date_ranges": [{"start": "2026-03-20", "end": "2026-03-30"}],
                }
            }
        }
        temp_dir = Path("output/test_tmp")
        temp_dir.mkdir(parents=True, exist_ok=True)
        path = temp_dir / f"route_trip_windows_{uuid4().hex}.json"
        try:
            path.write_text(json.dumps(payload), encoding="utf-8")
            with patch("core.trip_config.load_market_priors", return_value=priors):
                overrides = load_route_trip_overrides(path, today=date(2026, 3, 10))
        finally:
            if path.exists():
                path.unlink()

        self.assertEqual(1, len(overrides))
        self.assertEqual("RT", overrides[0]["trip_type"])
        self.assertEqual("2026-03-11", overrides[0]["outbound_dates"][0])
        self.assertEqual("2026-03-20", overrides[0]["outbound_dates"][-1])
        self.assertEqual("2026-03-20", overrides[0]["return_dates"][0])
        self.assertEqual("2026-03-30", overrides[0]["return_dates"][-1])

    def test_load_route_trip_overrides_supports_multiple_market_trip_profiles(self):
        payload = {
            "routes": [
                {
                    "airline": "BG",
                    "route": "DAC-CXB",
                    "market_trip_profiles": [
                        "bangladesh_domestic_round_trip_short",
                        "bangladesh_domestic_eid_round_trip_2026",
                    ],
                }
            ]
        }
        priors = {
            "trip_date_profiles": {
                "bangladesh_domestic_round_trip_short": {
                    "trip_type": "RT",
                    "return_date_offsets": [2],
                },
                "bangladesh_domestic_eid_round_trip_2026": {
                    "trip_type": "RT",
                    "date_ranges": [{"start": "2026-03-11", "end": "2026-03-20"}],
                    "return_date_ranges": [{"start": "2026-03-20", "end": "2026-03-30"}],
                },
            }
        }
        temp_dir = Path("output/test_tmp")
        temp_dir.mkdir(parents=True, exist_ok=True)
        path = temp_dir / f"route_trip_windows_{uuid4().hex}.json"
        try:
            path.write_text(json.dumps(payload), encoding="utf-8")
            with patch("core.trip_config.load_market_priors", return_value=priors):
                overrides = load_route_trip_overrides(path, today=date(2026, 3, 10))
        finally:
            if path.exists():
                path.unlink()

        self.assertEqual(2, len(overrides))
        matches = match_route_trip_overrides(overrides, airline="BG", origin="DAC", destination="CXB")
        self.assertEqual(2, len(matches))
        self.assertEqual([2], matches[0]["return_offsets"])
        self.assertEqual("2026-03-11", matches[1]["outbound_dates"][0])

    def test_load_route_trip_overrides_supports_directional_eid_one_way_profile(self):
        payload = {
            "routes": [
                {
                    "airline": "BG",
                    "route": "DAC-CXB",
                    "market_trip_profiles": [
                        "bangladesh_domestic_round_trip_short",
                        "bangladesh_domestic_eid_round_trip_2026",
                        "bangladesh_domestic_eid_capital_outbound_one_way_2026",
                    ],
                }
            ]
        }
        priors = {
            "trip_date_profiles": {
                "bangladesh_domestic_round_trip_short": {
                    "trip_type": "RT",
                    "return_date_offsets": [2],
                },
                "bangladesh_domestic_eid_round_trip_2026": {
                    "trip_type": "RT",
                    "date_ranges": [{"start": "2026-03-11", "end": "2026-03-20"}],
                    "return_date_ranges": [{"start": "2026-03-20", "end": "2026-03-30"}],
                },
                "bangladesh_domestic_eid_capital_outbound_one_way_2026": {
                    "trip_type": "OW",
                    "date_ranges": [{"start": "2026-03-11", "end": "2026-03-20"}],
                },
            }
        }
        temp_dir = Path("output/test_tmp")
        temp_dir.mkdir(parents=True, exist_ok=True)
        path = temp_dir / f"route_trip_windows_{uuid4().hex}.json"
        try:
            path.write_text(json.dumps(payload), encoding="utf-8")
            with patch("core.trip_config.load_market_priors", return_value=priors):
                overrides = load_route_trip_overrides(path, today=date(2026, 3, 10))
        finally:
            if path.exists():
                path.unlink()

        self.assertEqual(3, len(overrides))
        matches = match_route_trip_overrides(overrides, airline="BG", origin="DAC", destination="CXB")
        self.assertEqual(3, len(matches))
        self.assertEqual(["RT", "RT", "OW"], [m["trip_type"] for m in matches])

    def test_load_route_trip_overrides_can_limit_active_market_trip_profiles(self):
        payload = {
            "routes": [
                {
                    "airline": "BG",
                    "route": "DAC-CXB",
                    "market_trip_profiles": [
                        "bangladesh_domestic_round_trip_short",
                        "bangladesh_domestic_eid_round_trip_2026",
                        "bangladesh_domestic_eid_capital_outbound_one_way_2026",
                    ],
                    "active_market_trip_profiles": [
                        "bangladesh_domestic_round_trip_short",
                    ],
                }
            ]
        }
        priors = {
            "trip_date_profiles": {
                "bangladesh_domestic_round_trip_short": {
                    "trip_type": "RT",
                    "return_date_offsets": [2],
                },
                "bangladesh_domestic_eid_round_trip_2026": {
                    "trip_type": "RT",
                    "date_ranges": [{"start": "2026-03-11", "end": "2026-03-20"}],
                    "return_date_ranges": [{"start": "2026-03-20", "end": "2026-03-30"}],
                },
                "bangladesh_domestic_eid_capital_outbound_one_way_2026": {
                    "trip_type": "OW",
                    "date_ranges": [{"start": "2026-03-11", "end": "2026-03-20"}],
                },
            }
        }
        temp_dir = Path("output/test_tmp")
        temp_dir.mkdir(parents=True, exist_ok=True)
        path = temp_dir / f"route_trip_windows_{uuid4().hex}.json"
        try:
            path.write_text(json.dumps(payload), encoding="utf-8")
            with patch("core.trip_config.load_market_priors", return_value=priors):
                overrides = load_route_trip_overrides(path, today=date(2026, 3, 10))
        finally:
            if path.exists():
                path.unlink()

        self.assertEqual(1, len(overrides))
        self.assertEqual("RT", overrides[0]["trip_type"])
        self.assertEqual([2], overrides[0]["return_offsets"])

    def test_load_route_trip_overrides_training_mode_uses_active_profiles_only_by_default(self):
        payload = {
            "routes": [
                {
                    "airline": "BG",
                    "route": "DAC-CXB",
                    "market_trip_profiles": [
                        "bangladesh_domestic_round_trip_short",
                        "bangladesh_domestic_eid_capital_outbound_one_way_2026",
                    ],
                    "active_market_trip_profiles": [
                        "bangladesh_domestic_round_trip_short",
                    ],
                }
            ]
        }
        priors = {
            "trip_date_profiles": {
                "bangladesh_domestic_round_trip_short": {
                    "trip_type": "RT",
                    "return_date_offsets": [2],
                },
                "bangladesh_domestic_eid_capital_outbound_one_way_2026": {
                    "trip_type": "OW",
                    "date_ranges": [{"start": "2026-03-11", "end": "2026-03-20"}],
                },
            }
        }
        temp_dir = Path("output/test_tmp")
        temp_dir.mkdir(parents=True, exist_ok=True)
        path = temp_dir / f"route_trip_windows_{uuid4().hex}.json"
        try:
            path.write_text(json.dumps(payload), encoding="utf-8")
            with patch("core.trip_config.load_market_priors", return_value=priors):
                overrides = load_route_trip_overrides(path, today=date(2026, 3, 10), trip_plan_mode="training")
        finally:
            if path.exists():
                path.unlink()

        self.assertEqual(1, len(overrides))
        self.assertEqual(["RT"], [item["trip_type"] for item in overrides])

    def test_load_route_trip_overrides_training_mode_adds_training_only_profiles(self):
        payload = {
            "routes": [
                {
                    "airline": "BG",
                    "route": "DAC-CXB",
                    "market_trip_profiles": [
                        "bangladesh_domestic_round_trip_short",
                    ],
                    "active_market_trip_profiles": [
                        "bangladesh_domestic_round_trip_short",
                    ],
                    "training_market_trip_profiles": [
                        "inventory_anchor_departure_tracking_default",
                    ],
                }
            ]
        }
        priors = {
            "trip_date_profiles": {
                "bangladesh_domestic_round_trip_short": {
                    "trip_type": "RT",
                    "return_date_offsets": [2],
                },
                "inventory_anchor_departure_tracking_default": {
                    "trip_type": "OW",
                    "day_offsets": [7],
                },
            }
        }
        temp_dir = Path("output/test_tmp")
        temp_dir.mkdir(parents=True, exist_ok=True)
        path = temp_dir / f"route_trip_windows_{uuid4().hex}.json"
        try:
            path.write_text(json.dumps(payload), encoding="utf-8")
            with patch("core.trip_config.load_market_priors", return_value=priors):
                overrides = load_route_trip_overrides(path, today=date(2026, 3, 10), trip_plan_mode="training")
        finally:
            if path.exists():
                path.unlink()

        self.assertEqual(2, len(overrides))
        self.assertEqual(["RT", "OW"], [item["trip_type"] for item in overrides])
        self.assertIn("2026-03-17", overrides[1]["outbound_dates"])

    def test_load_route_trip_overrides_deep_mode_uses_full_candidate_profiles_and_deep_only_profiles(self):
        payload = {
            "routes": [
                {
                    "airline": "BG",
                    "route": "DAC-CXB",
                    "market_trip_profiles": [
                        "bangladesh_domestic_round_trip_short",
                        "bangladesh_domestic_eid_capital_outbound_one_way_2026",
                    ],
                    "active_market_trip_profiles": [
                        "bangladesh_domestic_round_trip_short",
                    ],
                    "training_market_trip_profiles": [
                        "inventory_anchor_departure_tracking_default",
                    ],
                    "deep_market_trip_profiles": [
                        "bangladesh_domestic_eid_round_trip_2026",
                    ],
                }
            ]
        }
        priors = {
            "trip_date_profiles": {
                "bangladesh_domestic_round_trip_short": {
                    "trip_type": "RT",
                    "return_date_offsets": [2],
                },
                "bangladesh_domestic_eid_capital_outbound_one_way_2026": {
                    "trip_type": "OW",
                    "date_ranges": [{"start": "2026-03-11", "end": "2026-03-20"}],
                },
                "inventory_anchor_departure_tracking_default": {
                    "trip_type": "OW",
                    "day_offsets": [7],
                },
                "bangladesh_domestic_eid_round_trip_2026": {
                    "trip_type": "RT",
                    "date_ranges": [{"start": "2026-03-11", "end": "2026-03-20"}],
                    "return_date_ranges": [{"start": "2026-03-20", "end": "2026-03-30"}],
                },
            }
        }
        temp_dir = Path("output/test_tmp")
        temp_dir.mkdir(parents=True, exist_ok=True)
        path = temp_dir / f"route_trip_windows_{uuid4().hex}.json"
        try:
            path.write_text(json.dumps(payload), encoding="utf-8")
            with patch("core.trip_config.load_market_priors", return_value=priors):
                overrides = load_route_trip_overrides(path, today=date(2026, 3, 10), trip_plan_mode="deep")
        finally:
            if path.exists():
                path.unlink()

        self.assertEqual(4, len(overrides))
        self.assertEqual(["RT", "OW", "OW", "RT"], [item["trip_type"] for item in overrides])

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
            today=date(2026, 3, 10),
            base_outbound_dates=["2026-03-10"],
            base_trip_type="OW",
            base_return_dates=[],
            base_return_offsets=[],
            route_override=route_override,
            limit_dates=None,
        )

        self.assertEqual("RT", plan["trip_type"])
        self.assertEqual(
            [
                "2026-03-12",
                "2026-03-13",
                "2026-03-14",
                "2026-03-15",
                "2026-03-16",
                "2026-03-17",
                "2026-03-18",
            ],
            plan["outbound_dates"],
        )
        self.assertEqual(
            [
                {"departure_date": "2026-03-12", "return_date": "2026-03-14", "trip_type": "RT"},
                {"departure_date": "2026-03-12", "return_date": "2026-03-17", "trip_type": "RT"},
                {"departure_date": "2026-03-13", "return_date": "2026-03-15", "trip_type": "RT"},
                {"departure_date": "2026-03-13", "return_date": "2026-03-18", "trip_type": "RT"},
                {"departure_date": "2026-03-14", "return_date": "2026-03-16", "trip_type": "RT"},
                {"departure_date": "2026-03-14", "return_date": "2026-03-19", "trip_type": "RT"},
                {"departure_date": "2026-03-15", "return_date": "2026-03-17", "trip_type": "RT"},
                {"departure_date": "2026-03-15", "return_date": "2026-03-20", "trip_type": "RT"},
                {"departure_date": "2026-03-16", "return_date": "2026-03-18", "trip_type": "RT"},
                {"departure_date": "2026-03-16", "return_date": "2026-03-21", "trip_type": "RT"},
                {"departure_date": "2026-03-17", "return_date": "2026-03-19", "trip_type": "RT"},
                {"departure_date": "2026-03-17", "return_date": "2026-03-22", "trip_type": "RT"},
                {"departure_date": "2026-03-18", "return_date": "2026-03-20", "trip_type": "RT"},
                {"departure_date": "2026-03-18", "return_date": "2026-03-23", "trip_type": "RT"},
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
            today=date(2026, 3, 10),
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
                {"departure_date": "2026-03-10", "return_date": None, "trip_type": "OW"},
                {"departure_date": "2026-03-12", "return_date": None, "trip_type": "OW"},
                {"departure_date": "2026-03-13", "return_date": None, "trip_type": "OW"},
                {"departure_date": "2026-03-14", "return_date": None, "trip_type": "OW"},
                {"departure_date": "2026-03-15", "return_date": None, "trip_type": "OW"},
                {"departure_date": "2026-03-16", "return_date": None, "trip_type": "OW"},
                {"departure_date": "2026-03-18", "return_date": None, "trip_type": "OW"},
            ],
            plan["search_windows"],
        )

    def test_resolve_route_trip_plan_adds_future_anchor_when_only_today_remains(self):
        plan = resolve_route_trip_plan(
            today=date(2026, 3, 31),
            base_outbound_dates=["2026-03-31"],
            base_trip_type="OW",
            base_return_dates=[],
            base_return_offsets=[],
            route_override=None,
            limit_dates=1,
        )

        self.assertEqual(
            [
                "2026-03-31",
                "2026-04-01",
                "2026-04-02",
                "2026-04-03",
                "2026-04-04",
                "2026-04-05",
                "2026-04-06",
            ],
            plan["outbound_dates"],
        )
        self.assertEqual(
            [
                {"departure_date": "2026-03-31", "return_date": None, "trip_type": "OW"},
                {"departure_date": "2026-04-01", "return_date": None, "trip_type": "OW"},
                {"departure_date": "2026-04-02", "return_date": None, "trip_type": "OW"},
                {"departure_date": "2026-04-03", "return_date": None, "trip_type": "OW"},
                {"departure_date": "2026-04-04", "return_date": None, "trip_type": "OW"},
                {"departure_date": "2026-04-05", "return_date": None, "trip_type": "OW"},
                {"departure_date": "2026-04-06", "return_date": None, "trip_type": "OW"},
            ],
            plan["search_windows"],
        )

    def test_resolve_route_trip_plan_drops_past_override_dates_and_keeps_future_anchor(self):
        route_override = {
            "airline": "BG",
            "origin": "DAC",
            "destination": "CXB",
            "trip_type": "OW",
            "outbound_dates": ["2026-03-10", "2026-03-31"],
            "return_dates": [],
            "return_offsets": [],
            "source": "route_trip_windows.json[0]",
        }

        plan = resolve_route_trip_plan(
            today=date(2026, 3, 31),
            base_outbound_dates=["2026-03-31"],
            base_trip_type="OW",
            base_return_dates=[],
            base_return_offsets=[],
            route_override=route_override,
            limit_dates=None,
        )

        self.assertEqual(
            [
                "2026-03-31",
                "2026-04-01",
                "2026-04-02",
                "2026-04-03",
                "2026-04-04",
                "2026-04-05",
                "2026-04-06",
            ],
            plan["outbound_dates"],
        )

    def test_resolve_route_trip_plan_extends_missing_weekdays(self):
        plan = resolve_route_trip_plan(
            today=date(2026, 3, 31),
            base_outbound_dates=["2026-04-01", "2026-04-03"],
            base_trip_type="OW",
            base_return_dates=[],
            base_return_offsets=[],
            route_override=None,
            limit_dates=None,
        )

        self.assertEqual(
            [
                "2026-04-01",
                "2026-04-03",
                "2026-04-04",
                "2026-04-05",
                "2026-04-06",
                "2026-04-07",
                "2026-04-09",
            ],
            plan["outbound_dates"],
        )


if __name__ == "__main__":
    unittest.main()
