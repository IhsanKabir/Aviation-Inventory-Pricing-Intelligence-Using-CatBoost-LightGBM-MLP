import json
from pathlib import Path

from core.extraction_health import classify_attempt, summarize_attempts, write_health_reports


def test_classifies_success_and_no_inventory():
    assert classify_attempt({"ok": True, "rows": [{"flight_number": "BG123"}]}, row_count=1)["error_class"] == "success"
    clean_empty = classify_attempt({"ok": True, "raw": {"source": "biman"}, "rows": []}, row_count=0)
    assert clean_empty["error_class"] == "no_inventory"
    assert clean_empty["manual_action_required"] is False


def test_classifies_retryable_and_manual_failures():
    rate_limited = classify_attempt(
        {"ok": False, "raw": {"error": "initialize_failed", "initialize_status": 429}, "rows": []},
        row_count=0,
    )
    assert rate_limited["error_class"] == "rate_limit"
    assert rate_limited["retry_recommended"] is True

    stale = classify_attempt({"ok": False, "raw": {"error": "stale_capture"}, "rows": []}, row_count=0)
    assert stale["error_class"] == "stale_capture"
    assert stale["manual_action_required"] is True

    waf = classify_attempt({"ok": False, "raw": {"error": "datadome_blocked"}, "rows": []}, row_count=0)
    assert waf["error_class"] == "waf_blocked"
    assert waf["manual_action_required"] is True


def test_summary_gate_fails_on_missing_or_source_failure():
    attempts = [
        {"airline": "BG", "error_class": "success", "row_count": 2, "inserted_core_count": 2},
        {"airline": "VQ", "error_class": "rate_limit", "row_count": 0, "retry_recommended": True},
    ]
    summary = summarize_attempts(attempts, expected_airlines=["BG", "VQ", "BS"])
    assert summary["status"] == "FAIL"
    assert summary["missing_airlines"] == ["BS"]
    assert summary["failure_count"] == 1


def test_write_health_reports():
    out_dir = Path("output/test_artifacts/extraction_health")
    attempts = [{"airline": "BG", "error_class": "success", "row_count": 1, "inserted_core_count": 1}]
    report = write_health_reports(attempts, output_dir=out_dir, cycle_id="cycle-1", expected_airlines=["BG"])
    assert report["status"] == "PASS"
    payload = json.loads((out_dir / "extraction_health_latest.json").read_text(encoding="utf-8"))
    assert payload["summary"]["cycle_id"] == "cycle-1"
    assert (out_dir / "extraction_health_latest.md").exists()
    assert (out_dir / "extraction_health_latest.csv").exists()
