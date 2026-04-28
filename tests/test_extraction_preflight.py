import importlib
import json
from pathlib import Path

from tools.pre_flight_session_check import run_preflight


def test_enabled_modules_expose_health_contract():
    airlines = json.loads(Path("config/airlines.json").read_text(encoding="utf-8-sig"))
    modules = sorted({row["module"] for row in airlines if row.get("enabled")})
    assert modules
    for module_name in modules:
        module = importlib.import_module(f"modules.{module_name}")
        checker = getattr(module, "check_source_health", None) or getattr(module, "check_session", None)
        assert callable(checker), module_name
        result = checker(dry_run=True)
        assert isinstance(result, dict), module_name
        assert result.get("status") in {"ok", "warn", "fail"}, module_name


def test_preflight_dry_run_writes_report():
    out_dir = Path("output/test_artifacts/preflight")
    rc, report = run_preflight(
        airlines_config=Path("config/airlines.json"),
        output_dir=out_dir,
        dry_run=True,
        strict=False,
    )
    assert rc == 0
    assert report["airline_count"] > 0
    assert (out_dir / "preflight_session_check_latest.json").exists()
    assert (out_dir / "preflight_session_check_latest.md").exists()
