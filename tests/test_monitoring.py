"""Tests for the system-observability repository (error tracking + metrics)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from apps.api.app.repositories import monitoring  # noqa: E402


def _sqlite_db():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    engine = create_engine("sqlite://")
    monitoring.ensure_tables(engine)
    return Session(engine)


def test_error_recorded_and_fetched_from_db():
    db = _sqlite_db()
    monitoring.record_error(db, method="POST", path="/x", status=500,
                            error_type="ValueError", message="boom", request_id="r1")
    errs = monitoring.recent_errors(db, 10)
    assert len(errs) == 1
    assert errs[0]["status"] == 500 and errs[0]["error_type"] == "ValueError"
    assert monitoring.error_count(db, 24) == 1


def test_error_falls_back_to_memory_without_db():
    before = len(monitoring.recent_errors(None, 200))
    monitoring.record_error(None, method="GET", path="/y", status=502,
                            error_type="Timeout", message="slow", request_id=None)
    after = monitoring.recent_errors(None, 200)
    assert len(after) == before + 1
    assert after[0]["path"] == "/y"          # newest first


def test_request_stats_windowing_and_percentiles():
    # fresh deque state is process-global; just assert the shape + that our adds count
    monitoring.record_request(200, 10.0)
    monitoring.record_request(200, 20.0)
    monitoring.record_request(500, 30.0)
    stats = monitoring.request_stats(3600)
    assert stats["total_requests"] >= 3
    assert stats["error_requests"] >= 1
    assert 0.0 <= stats["error_rate"] <= 1.0
    assert stats["latency_p95_ms"] >= stats["latency_p50_ms"]
    assert stats["per_instance"] is True


def test_record_error_never_raises_on_bad_db():
    class Boom:
        def execute(self, *a, **k): raise RuntimeError("db down")
        def rollback(self): pass
        def commit(self): pass
    # must swallow and fall back to memory, never propagate
    monitoring.record_error(Boom(), method="GET", path="/z", status=500,
                            error_type="X", message="y", request_id=None)


if __name__ == "__main__":
    import subprocess
    raise SystemExit(subprocess.call([sys.executable, "-m", "pytest", __file__, "-q"]))
