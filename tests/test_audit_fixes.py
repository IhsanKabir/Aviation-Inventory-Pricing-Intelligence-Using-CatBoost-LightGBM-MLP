"""Regression tests for the six-lens audit fixes (2026-07-03)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def test_version_tag_parse_handles_desktop_prefix():
    # The bug: lstrip('v') left 'desktop-v0.1.5' non-numeric -> updates never showed.
    from apps.api.app.routers.app_release import _version_from_tag
    assert _version_from_tag("desktop-v0.1.5") == "0.1.5"
    assert _version_from_tag("v1.1.0") == "1.1.0"
    assert _version_from_tag("2.3") == "2.3"


def test_client_version_compare_is_per_segment():
    import re

    def _ver(v: str) -> tuple[int, ...]:
        return tuple(int(x) for x in re.findall(r"\d+", str(v))) or (0,)

    assert _ver("0.1.6") > _ver("0.1.5")
    assert _ver("desktop-v0.1.6") > _ver("0.1.5")   # must not collapse to (0,)
    assert not _ver("0.1.5") > _ver("0.1.5")


def test_corrupt_har_does_not_kill_the_run(monkeypatch, tmp_path=None):
    import tempfile
    from discount_engine import grid

    good = str(Path(tempfile.mkdtemp()) / "bdfare_good.har")
    bad = str(Path(good).parent / "bdfare_bad.har")
    Path(good).write_text("{}", encoding="utf-8")
    Path(bad).write_text("{}", encoding="utf-8")

    def flaky_collector(path):
        if path == bad:
            raise ValueError("truncated HAR")
        return {("DOM", "BS"): "8"}

    monkeypatch.setattr(grid, "collect_bdfare",
                        lambda h, tb=None, bi=None: flaky_collector(h))
    report = grid.build_report(None, [], bdfare_hars=[good, bad], use_true_base=False)
    dom = {r["label"]: r["cells"] for r in report["grids"]["DOM"]["rows"]}
    assert dom["BDFare"]["BS"] == "8"                       # good HAR survived
    assert report["channel_status"]["BDFare"] == "ok"      # partial success -> ok

    # both bad -> parse_failed, run still completes
    def all_bad(h, tb=None, bi=None):
        raise ValueError("truncated")
    monkeypatch.setattr(grid, "collect_bdfare", all_bad)
    report = grid.build_report(None, [], bdfare_hars=[good, bad], use_true_base=False)
    assert report["channel_status"]["BDFare"] == "parse_failed"


def test_consume_use_is_atomic_and_idempotent():
    """SQLite in-memory stand-in for the metering table exercises the real SQL."""
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import Session
    from apps.api.app.repositories import access_requests as ar

    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text(
            """CREATE TABLE report_access_request_usage (
                usage_id TEXT PRIMARY KEY, request_id TEXT, action TEXT,
                user_id TEXT, email TEXT, sync_id TEXT, created_at_utc TIMESTAMP)"""))
    db = Session(engine)
    req = {"request_id": "r1", "use_quota": 2}

    a = ar.consume_use(db, request=req, action="sync", user_id="u", email="e", sync_id="s1")
    b = ar.consume_use(db, request=req, action="sync", user_id="u", email="e", sync_id="s2")
    assert a["allowed"] and b["allowed"] and b["remaining"] == 0

    # third distinct sync exceeds quota 2 -> blocked, nothing inserted
    c = ar.consume_use(db, request=req, action="sync", user_id="u", email="e", sync_id="s3")
    assert not c["allowed"]
    assert ar.count_usage(db, "r1", "sync") == 2

    # retry of s1 is idempotent -> no double-bill, flagged duplicate
    d = ar.consume_use(db, request=req, action="sync", user_id="u", email="e", sync_id="s1")
    assert d["allowed"] and d["duplicate"] is True
    assert ar.count_usage(db, "r1", "sync") == 2


def test_b2c_failsafe_unions_multiple_hars(monkeypatch):
    from discount_engine import grid
    from modules import firsttrip

    calls = {"bs": [{"airline": "BS", "origin": "DAC", "destination": "CGP",
                     "gross_total_bdt": 5000, "base_fare_bdt": 4000,
                     "headline_rate": 16.0, "realized_pct": 16.0, "coupon_code": "X",
                     "dynamic_rate": None, "coupon_cap_bdt": None}]}

    def fake_parse(path):
        return calls["bs"] if "one" in path else [
            {"airline": "2A", "origin": "DAC", "destination": "CGP",
             "gross_total_bdt": 5200, "base_fare_bdt": 4100, "headline_rate": 12.0,
             "realized_pct": 12.0, "coupon_code": "Y", "dynamic_rate": None,
             "coupon_cap_bdt": None}]

    monkeypatch.setattr(firsttrip, "parse_b2c_har", fake_parse)
    monkeypatch.setattr(firsttrip, "summarize_b2c_discounts",
                        lambda rows: {r["airline"]: {"rate": r["headline_rate"], "code": r["coupon_code"],
                                                     "source": "coupon", "realized_pct": r["realized_pct"],
                                                     "cap_bdt": None} for r in rows})
    report = grid.build_report(None, [], firsttrip_b2c_hars=["one.har", "two.har"],
                               use_true_base=False)
    dom = {r["label"]: r["cells"] for r in report["grids"]["DOM"]["rows"]}
    # both HARs' airlines present -> unioned, not overwritten
    assert dom["Firsttrip-B2C"]["BS"] == "16"
    assert dom["Firsttrip-B2C"]["2A"] == "12"


if __name__ == "__main__":
    import subprocess
    raise SystemExit(subprocess.call([sys.executable, "-m", "pytest", __file__, "-q"]))


def test_app_latest_is_public_and_https(monkeypatch):
    """/app/latest must serve WITHOUT auth (broken old installs need it to find
    their fix) and its download_url must be https even behind a proxy that
    hands the app an http base URL."""
    from fastapi.testclient import TestClient
    from apps.api.app import main
    from apps.api.app.routers import app_release

    monkeypatch.setattr(app_release, "_fetch_latest_release", lambda k, c: {
        "tag_name": "desktop-v9.9.9", "body": "notes", "published_at": "2026-07-07T00:00:00Z",
        "assets": [{"name": c["asset"], "browser_download_url": "https://x/y.exe"}]})
    monkeypatch.setattr(app_release, "_sha256_from_release", lambda c, d: "abc")
    r = TestClient(main.app).get("/api/v1/app/latest?app=discount-report")  # NO session header
    assert r.status_code == 200
    body = r.json()
    assert body["version"] == "9.9.9"
    assert body["download_url"].startswith("https://")
    assert body["published_at"].startswith("2026-07-07")
