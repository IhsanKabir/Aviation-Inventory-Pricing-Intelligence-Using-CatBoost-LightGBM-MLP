"""discount_engine — OTA discount comparison report as an importable library.

Public API
----------
- run_report(har_dir, ...)   one-call convenience: auto-detect HARs in a folder,
                             build the report dict (optionally write the xlsx sheet).
- build_report(...)          the orchestrator (explicit per-channel HAR lists).
- write_xlsx(report, path)   append the colored daily sheet to a rolling workbook.
- write_outputs(report, ...) JSON + CSV snapshots.
- auto_detect_hars(dir)      map {channel: [har paths]} by filename hint / signature.
- render_console(report)     print the grid to stdout.

Used by the CLI (`tools/ota_discount_grid.py`), the desktop app, and `apps/api`.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from .grid import (  # noqa: F401  (re-exported public API)
    DOM_COLUMNS,
    DOMESTIC_AIRPORTS,
    INTL_COLUMNS,
    ROW_ORDER,
    auto_detect_hars,
    build_report,
    detect_channel,
    render_console,
    write_outputs,
    write_single_sheet_xlsx,
    write_xlsx,
)
from .highlight import (  # noqa: F401
    apply_highlights,
    compute_highlights,
    leading_number,
)
from .sanitize import sanitize_report_for_sync  # noqa: F401

__version__ = "0.1.0"

#: build_report kwarg name per auto-detected channel key.
_CHANNEL_KWARGS = {
    "gozayaan": "gozayaan_hars",
    "amy": "amy_hars",
    "firsttrip_b2b": "firsttrip_b2b_hars",
    "sharetrip": "sharetrip_hars",
    "akij": "akij_hars",
    "bdfare": "bdfare_hars",
}


def run_report(
    har_dir: str | Path,
    routes: list[tuple[str, str, Optional[str]]] | None = None,
    date: Optional[str] = None,
    channels: Optional[list[str]] = None,
    manual_overrides: Optional[dict[str, Any]] = None,
    use_true_base: bool = True,
    run_dt: Optional[datetime] = None,
    xlsx_path: str | Path | None = None,
) -> dict[str, Any]:
    """One-call report: auto-detect HARs in `har_dir`, build the report dict.

    `channels` optionally restricts which auto-detected channels are used (keys:
    gozayaan, amy, firsttrip_b2b, sharetrip, akij, bdfare). `routes` feeds the live
    FirstTrip B2C fetch (each (origin, dest, date-or-None)). If `xlsx_path` is given,
    the colored daily sheet is also written there.
    """
    har_dir = Path(har_dir)
    if not har_dir.is_dir():
        raise FileNotFoundError(f"HAR directory not found: {har_dir}")

    detected = auto_detect_hars(har_dir)
    if channels is not None:
        detected = {c: paths for c, paths in detected.items() if c in set(channels)}

    kwargs: dict[str, Any] = {kw: [] for kw in _CHANNEL_KWARGS.values()}
    for channel, paths in detected.items():
        kw = _CHANNEL_KWARGS.get(channel)
        if kw:
            kwargs[kw].extend(paths)

    report = build_report(
        date,
        routes or [],
        manual_overrides=manual_overrides,
        use_true_base=use_true_base,
        run_dt=run_dt,
        **kwargs,
    )
    if xlsx_path:
        write_xlsx(report, Path(xlsx_path))
    return report
