from run_pipeline import _extraction_gate_allows_bigquery


def test_extraction_gate_blocks_bigquery_only_on_fail():
    assert _extraction_gate_allows_bigquery("PASS") is True
    assert _extraction_gate_allows_bigquery("WARN") is True
    assert _extraction_gate_allows_bigquery("UNKNOWN") is True
    assert _extraction_gate_allows_bigquery("FAIL") is False
