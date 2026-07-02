"""sanitize_report_for_sync — the ONLY shape of a report that may leave the machine.

HAR captures come from LOGGED-IN agent sessions, and the raw report dict carries
identifying metadata: `sources` embeds local HAR filenames (which can encode
usernames, account hints, capture habits) and `routes` reveals exactly what the team
searched. The sync payload is therefore built by WHITELIST — every field below is
constructed fresh; anything not listed here simply does not exist in the payload,
so a future field added to the report can never leak by accident.

Server side re-runs this on ingest (never trust the client) and rejects oversized
payloads. Guarded by tests/test_sync_payload_no_secrets.py.
"""

from __future__ import annotations

from typing import Any, Optional

#: sources whose provenance is keepable, as coarse kinds (never filenames).
_SOURCE_KINDS = (("live", "live"), ("FT-B2B HAR", "har"), ("HAR", "har"), ("manual", "manual"))


def _provenance(source: Any) -> dict[str, Any]:
    """'HAR: akijair.com.har  [true-base]' -> {'kinds': ['har'], 'true_base': True}.

    IDEMPOTENT: the server re-runs sanitize on ingest, so an already-sanitized
    provenance dict must pass through unchanged (not get str()-mangled)."""
    if isinstance(source, dict) and "kinds" in source:
        return {"kinds": [str(k) for k in source.get("kinds", [])] or ["unknown"],
                "true_base": bool(source.get("true_base", False))}
    text = str(source)
    kinds = []
    for needle, kind in _SOURCE_KINDS:
        if needle in text and kind not in kinds:
            kinds.append(kind)
    return {"kinds": kinds or ["unknown"], "true_base": "[true-base]" in text}


def _clean_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        clean: dict[str, Any] = {"label": row.get("label", ""), "kind": row.get("kind", "")}
        if row.get("kind") != "sep":
            clean["cells"] = {str(a): str(v) for a, v in (row.get("cells") or {}).items()}
            if "highlights" in row:
                clean["highlights"] = {str(a): str(f) for a, f in row["highlights"].items()}
        out.append(clean)
    return out


def sanitize_report_for_sync(report: dict[str, Any]) -> dict[str, Any]:
    """Whitelisted copy of a report, safe to POST to the backend.

    Keeps: grids (cells + highlight flags + best), report date/time, generated_at,
    true-base health, normalized flag, channel status, prev_report_date.
    Strips: HAR filenames (sources -> coarse provenance kinds), searched routes,
    default travel date, and every unknown field.
    """
    grids: dict[str, Any] = {}
    for rt, grid in (report.get("grids") or {}).items():
        clean_grid: dict[str, Any] = {
            "columns": [str(c) for c in grid.get("columns", [])],
            "rows": _clean_rows(grid.get("rows", [])),
        }
        if "best" in grid:
            clean_grid["best"] = {
                str(a): {"pct": float(b["pct"]), "channel": str(b["channel"]),
                         "short": str(b["short"]), "display": str(b["display"])}
                for a, b in grid["best"].items()
            }
        grids[str(rt)] = clean_grid

    payload: dict[str, Any] = {
        "report_date": str(report.get("report_date", "")),
        "report_time": str(report.get("report_time", "")),
        "generated_at": str(report.get("generated_at", "")),
        "normalized": bool(report.get("normalized", False)),
        "true_base": {
            "source": str((report.get("true_base") or {}).get("source", "unknown")),
            "airlines_covered": [str(a) for a in
                                 (report.get("true_base") or {}).get("airlines_covered", [])],
            "sample_count": int((report.get("true_base") or {}).get("sample_count", 0)),
        },
        "channel_status": {str(k): str(v) for k, v in
                           (report.get("channel_status") or {}).items()},
        "sources": {str(k): _provenance(v) for k, v in
                    (report.get("sources") or {}).items()},
        "grids": grids,
    }
    prev_date: Optional[str] = report.get("prev_report_date")
    if prev_date:
        payload["prev_report_date"] = str(prev_date)
    return payload
