"""Guard: the sync payload may NEVER carry secrets, filenames, paths, or route intel.

sanitize_report_for_sync builds the payload by whitelist; these tests are the durable
enforcement — they fail if a future report field leaks through, and they also sweep
the LATEST REAL report on disk, not just synthetic fixtures.
"""
import json
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from discount_engine.sanitize import sanitize_report_for_sync  # noqa: E402

SECRET_RE = re.compile(
    r"cookie|token|sxsrf|authorization|bearer|password|set-cookie|jwt|accesstoken",
    re.IGNORECASE)
PATHY_RE = re.compile(r"\.har|[A-Za-z]:\\|/Users/|/home/|\\\\")

ALLOWED_TOP = {"report_date", "report_time", "generated_at", "normalized", "true_base",
               "channel_status", "sources", "grids", "prev_report_date"}


def _nasty_report() -> dict:
    """A report deliberately carrying everything that must NOT sync."""
    return {
        "generated_at": "2026-07-02T12:00:00",
        "report_date": "02/07/2026",
        "report_time": "1200",
        "default_date": "2026-07-30",                       # travel date -> dropped
        "routes": ["DAC-CGP", "DAC-DXB@2026-08-01"],        # route intel -> dropped
        "sources": {"BDFare": r"HAR: C:\Users\agent\bdfare TriploverSecret.har  [true-base]",
                    "Firsttrip-B2C": "live: 3 route(s)"},
        "true_base": {"source": "ft_b2b_har", "airlines_covered": ["BS"], "sample_count": 3},
        "normalized": True,
        "channel_status": {"BDFare": "ok"},
        "debug_session_cookie": "SECRET-COOKIE-VALUE",      # unknown field -> dropped
        "grids": {"DOM": {"columns": ["BS"], "rows": [
            {"label": "BDFare", "kind": "b2b", "cells": {"BS": "8.02"},
             "highlights": {"BS": "highest"}},
            {"label": "__sep__", "kind": "sep", "cells": {}},
        ], "best": {"BS": {"pct": 8.02, "channel": "BDFare", "short": "BDFare",
                           "display": "8.02% · BDFare"}}}},
    }


def test_whitelist_top_level_keys_only():
    payload = sanitize_report_for_sync(_nasty_report())
    assert set(payload) <= ALLOWED_TOP
    assert "routes" not in payload and "default_date" not in payload
    assert "debug_session_cookie" not in payload


def test_no_secrets_no_filenames_no_paths():
    blob = json.dumps(sanitize_report_for_sync(_nasty_report()))
    assert not SECRET_RE.search(blob), SECRET_RE.search(blob).group()
    assert not PATHY_RE.search(blob), PATHY_RE.search(blob).group()


def test_sources_reduced_to_provenance_kinds():
    payload = sanitize_report_for_sync(_nasty_report())
    assert payload["sources"]["BDFare"] == {"kinds": ["har"], "true_base": True}
    assert payload["sources"]["Firsttrip-B2C"] == {"kinds": ["live"], "true_base": False}


def test_grid_content_and_flags_survive():
    payload = sanitize_report_for_sync(_nasty_report())
    dom = payload["grids"]["DOM"]
    assert dom["rows"][0]["cells"]["BS"] == "8.02"
    assert dom["rows"][0]["highlights"]["BS"] == "highest"
    assert dom["best"]["BS"]["display"] == "8.02% · BDFare"
    assert dom["rows"][1] == {"label": "__sep__", "kind": "sep"}   # sep carries nothing


def test_sanitize_is_idempotent():
    # The server re-runs sanitize on ingest; a double pass must be a no-op.
    once = sanitize_report_for_sync(_nasty_report())
    twice = sanitize_report_for_sync(once)
    assert twice == once


def test_latest_real_report_sanitizes_clean():
    reports = sorted(Path(__file__).parent.parent.glob("output/reports/ota_discount_grid_*.json"))
    if not reports:
        pytest.skip("no real report on disk")
    real = json.loads(reports[-1].read_text(encoding="utf-8"))
    blob = json.dumps(sanitize_report_for_sync(real))
    assert not SECRET_RE.search(blob)
    assert not PATHY_RE.search(blob)
    assert "DAC-" not in blob            # no route intel
    assert len(blob) < 512 * 1024        # under the server-side size cap


if __name__ == "__main__":
    import subprocess
    raise SystemExit(subprocess.call([sys.executable, "-m", "pytest", __file__, "-q"]))
