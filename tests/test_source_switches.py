import json
from pathlib import Path

from core.source_switches import load_source_switches, source_switch_status
from modules.sharetrip import fetch_flights_for_airline
from tools.pre_flight_session_check import run_preflight


ARTIFACT_DIR = Path("output/test_artifacts/source_switches")


def _write_switches(filename: str, payload: dict) -> Path:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    path = ARTIFACT_DIR / filename
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_source_switch_file_disables_named_source(monkeypatch):
    monkeypatch.delenv("SHARETRIP_ENABLED", raising=False)
    path = _write_switches(
        "source_switches_disabled_sharetrip.json",
        {"sources": {"sharetrip": {"enabled": False, "reason": "maintenance"}}},
    )

    switches = load_source_switches(path)
    status = source_switch_status("ShareTrip", switches=switches)

    assert status["enabled"] is False
    assert "maintenance" in " ".join(status["reasons"])
    assert source_switch_status("unknown-source", switches=switches)["enabled"] is True


def test_legacy_sharetrip_env_still_disables_source(monkeypatch):
    monkeypatch.setenv("SHARETRIP_ENABLED", "false")

    status = source_switch_status("sharetrip", switches={"sharetrip": {"enabled": True}})

    assert status["enabled"] is False
    assert "SHARETRIP_ENABLED=false" in " ".join(status["reasons"])


def test_preflight_respects_source_switch_file(monkeypatch):
    monkeypatch.delenv("SHARETRIP_ENABLED", raising=False)
    switches_path = _write_switches(
        "source_switches_preflight_sharetrip_off.json",
        {"sources": {"sharetrip": {"enabled": False, "reason": "pause sharetrip"}}},
    )

    rc, report = run_preflight(
        airlines_config=Path("config/airlines.json"),
        source_switches_file=switches_path,
        output_dir=ARTIFACT_DIR / "preflight",
        dry_run=True,
        strict=False,
    )

    assert rc == 0
    assert report["airline_count"] > 0
    assert all(row["module"] != "sharetrip" for row in report["results"])


def test_sharetrip_fetch_returns_disabled_response_from_source_switch_file(monkeypatch):
    monkeypatch.delenv("SHARETRIP_ENABLED", raising=False)
    switches_path = _write_switches(
        "source_switches_sharetrip_fetch_off.json",
        {"sources": {"sharetrip": {"enabled": False, "reason": "pause sharetrip"}}},
    )
    monkeypatch.setenv("AIRLINE_SOURCE_SWITCHES_FILE", str(switches_path))

    resp = fetch_flights_for_airline(
        airline_code="QR",
        origin="DAC",
        destination="DOH",
        date="2026-05-01",
    )

    assert resp["ok"] is False
    assert resp["raw"]["error"] == "source_disabled"
    assert "pause sharetrip" in resp["raw"]["message"]
