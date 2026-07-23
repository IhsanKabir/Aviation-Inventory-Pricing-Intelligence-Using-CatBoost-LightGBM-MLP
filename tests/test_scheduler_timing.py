import json
from pathlib import Path

import pytest

from core.scheduler_timing import find_timing_entry, load_scheduler_timing_plan, pipeline_filter_args


ARTIFACT_DIR = Path("output/test_artifacts/scheduler_timing")


def _write_json(name: str, payload: dict) -> Path:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    path = ARTIFACT_DIR / name
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_current_schedule_timing_plan_loads():
    schedule_file = Path("config/schedule.json")
    plan = load_scheduler_timing_plan(
        schedule_file=schedule_file,
        airlines_file=Path("config/airlines.json"),
        source_switches_file=Path("config/source_switches.json"),
    )

    global_entry = find_timing_entry(plan, scope_type="global", scope_id="global")

    # Assert the loaded plan faithfully mirrors the CURRENT config rather than a hard-coded
    # time — the schedule is retuned from time to time (e.g. moved to 01:00), and a pinned
    # literal here silently goes stale and fails for no real reason.
    cfg_global = json.loads(schedule_file.read_text(encoding="utf-8"))["scheduler_timing"]["global"]
    assert global_entry is not None
    assert global_entry.start_time == cfg_global["start_time"]
    assert global_entry.repeat_minutes == cfg_global["repeat_minutes"]


def test_source_airline_and_route_timing_filters():
    schedule_file = _write_json(
        "schedule_scoped.json",
        {
            "scheduler_timing": {
                "enabled": True,
                "global": {"enabled": True, "start_time": "00:05", "repeat_minutes": 360},
                "sources": {"biman": {"enabled": True, "start_time": "01:00", "repeat_minutes": 720}},
                "airlines": {"VQ": {"enabled": True, "start_time": "02:00", "repeat_minutes": 360}},
                "routes": [
                    {
                        "enabled": True,
                        "airline": "BG",
                        "origin": "DAC",
                        "destination": "CXB",
                        "start_time": "03:00",
                        "repeat_minutes": 1440,
                    }
                ],
            }
        },
    )
    plan = load_scheduler_timing_plan(
        schedule_file=schedule_file,
        airlines_file=Path("config/airlines.json"),
        source_switches_file=Path("config/source_switches.json"),
    )

    source_entry = find_timing_entry(plan, scope_type="source", scope_id="biman")
    airline_entry = find_timing_entry(plan, scope_type="airline", scope_id="VQ")
    route_entry = find_timing_entry(plan, scope_type="route", scope_id="BG_DAC_CXB")

    assert source_entry is not None
    assert pipeline_filter_args(source_entry) == ["--airline", "BG"]
    assert airline_entry is not None
    assert pipeline_filter_args(airline_entry) == ["--airline", "VQ"]
    assert route_entry is not None
    assert pipeline_filter_args(route_entry) == ["--airline", "BG", "--origin", "DAC", "--destination", "CXB"]


def test_invalid_scheduler_time_is_rejected():
    schedule_file = _write_json(
        "schedule_invalid_time.json",
        {"scheduler_timing": {"global": {"start_time": "25:99", "repeat_minutes": 360}}},
    )

    with pytest.raises(ValueError, match="Invalid scheduler time"):
        load_scheduler_timing_plan(
            schedule_file=schedule_file,
            airlines_file=Path("config/airlines.json"),
            source_switches_file=Path("config/source_switches.json"),
        )
