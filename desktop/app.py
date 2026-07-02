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


def main() -> int:
    # Windowed (no-console) PyInstaller builds run with sys.stdout/err = None;
    # the engine prints progress/warnings, which would raise. Route to devnull.
    if sys.stdout is None:
        sys.stdout = open(os.devnull, "w", encoding="utf-8")  # noqa: SIM115
    if sys.stderr is None:
        sys.stderr = open(os.devnull, "w", encoding="utf-8")  # noqa: SIM115

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
