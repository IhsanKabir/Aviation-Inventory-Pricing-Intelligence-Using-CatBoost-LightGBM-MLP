"""OTA Discount Report — desktop entry point.

pywebview shell (embedded WebView2, NO listening socket → no firewall prompt,
no port conflicts, no local HTTP attack surface). Run from source with
`python -m desktop.app`; ships as a PyInstaller one-folder build (build.spec).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _resource_path(name: str) -> Path:
    """Bundle-aware resource lookup (PyInstaller extracts datas under _MEIPASS)."""
    base = Path(getattr(sys, "_MEIPASS", Path(__file__).parent))
    candidate = base / name
    return candidate if candidate.exists() else Path(__file__).parent / name


def _selftest() -> int:
    """Prove the bundle from inside the built exe: `OTADiscountReport.exe --selftest`.

    The Schedule/Fare tabs import their engine lazily, so a missing package shows
    up only when a user clicks Run — after shipping. This exercises those imports
    (and the bundled data files) at build time instead, which is exactly the class
    of failure that shipped a crashing exe once before.
    """
    checks: list[tuple[str, bool, str]] = []

    def check(name, fn):
        try:
            detail = fn() or ""
            checks.append((name, True, str(detail)))
        except Exception as exc:                      # noqa: BLE001
            checks.append((name, False, f"{type(exc).__name__}: {exc}"))

    def _engine():
        from discount_engine import build_report          # noqa: F401
        return "discount engine"

    def _market():
        from market_engine import (cache, collect, fares, har, render, rows,
                                    schedule, sources, timetable)
        names = timetable._load("airline_names.json")
        caps = timetable._load("seat_capacity.json")
        return (f"{len(sources.LIVE)} live sources, {len(har.PARSERS)} HAR parsers, "
                f"{len(names)} airline names, {len(caps)} fleets")

    def _sched_engine():
        from core import field_quality, flight_number      # noqa: F401
        from engines import schedule_view                  # noqa: F401
        return "schedule view"

    def _presets():
        from .market_api import _bundled
        path = _bundled("config/route_presets.json")
        if not path:
            raise FileNotFoundError("route_presets.json not bundled")
        import json
        return f"{len(json.loads(path.read_text(encoding='utf-8'))) - 1} preset groups"

    def _ui():
        page = _resource_path("ui.html")
        if not page.exists():
            raise FileNotFoundError("ui.html not bundled")
        return f"{page.stat().st_size // 1024} KB"

    check("discount engine", _engine)
    check("market engine", _market)
    check("schedule engine", _sched_engine)
    check("route presets", _presets)
    check("ui", _ui)

    failed = [c for c in checks if not c[1]]
    lines = [f"[{'OK ' if ok else 'FAIL'}] {name}: {detail}" for name, ok, detail in checks]
    lines.append(f"selftest: {len(checks) - len(failed)}/{len(checks)} passed")
    report = "\n".join(lines)
    print(report)
    # The shipped exe is windowed, so stdout goes nowhere; write the result where
    # a build step (or a support call) can actually read it. The exit code is the
    # machine-readable answer.
    try:
        out = Path(os.environ.get("TEMP") or ".") / "ota_selftest.txt"
        out.write_text(report, encoding="utf-8")
    except OSError:
        pass
    return 1 if failed else 0


def main() -> int:
    # Windowed (no-console) PyInstaller builds run with sys.stdout/err = None;
    # the engine prints progress/warnings, which would raise. Route to devnull.
    if sys.stdout is None:
        sys.stdout = open(os.devnull, "w", encoding="utf-8")  # noqa: SIM115
    if sys.stderr is None:
        sys.stderr = open(os.devnull, "w", encoding="utf-8")  # noqa: SIM115

    if "--selftest" in sys.argv:
        return _selftest()

    import webview

    from .backend import DesktopApi

    api = DesktopApi()
    window = webview.create_window(
        "OTA Discount Comparison",
        url=str(_resource_path("ui.html")),
        js_api=api,
        width=1360,
        height=860,
        min_size=(1000, 640),
    )
    api.attach_window(window)
    webview.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
