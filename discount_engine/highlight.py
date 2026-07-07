"""Highlight computation for the discount grid — the single source of truth.

Both renderers (the xlsx writer and the web viewer) color cells from THESE flags,
so they can never drift apart. Semantics (locked in 2026-07-02):

- Cells rank per airline WITHIN each B2B / B2C group by the leading COMMON rate —
  a plain cell ("12") by its number, a coupon TEXT cell ("9(Bkash), 18 (EBL)") by
  its leading common number (9), so text cells participate fully.
- "changed" (differs from the previous report's common rate) takes precedence,
  then "highest" (green), then "second" (blue).
- The "Best (OTA)" summary row shows, per airline, the top COMMON rate across ALL
  channels (B2B + B2C) and the winning channel.

All rates here are in PERCENT units (12 == 12%).
"""

from __future__ import annotations

import copy
import re
from typing import Any, Optional

# Short OTA labels for the "Best" row.
BEST_SHORT = {"USBA OTA B2B": "USBA", "SHARETRIP-B2B": "ST-B2B", "BDFare": "BDFare",
              "TLN": "TLN", "AKIJ AIR-B2B": "AKIJ", "Firsttrip-B2C": "FT-B2C",
              "ShareTrip-B2C": "ST-B2C", "Go Zayaan": "GoZ", "Amy": "Amy"}

_NUM = re.compile(r"\s*~?\s*(-?\d+(?:\.\d+)?)")   # "~7.5" = estimated base, ranks as 7.5


def leading_number(text: Any) -> Optional[float]:
    """Leading numeric of a cell -> float (the common/headline rate). Handles
    '9(Bkash), 18 (EBL)' -> 9.0, '-6.49' -> -6.49, '12' -> 12.0; blanks/pure text -> None."""
    if text is None or text == "":
        return None
    m = _NUM.match(str(text))
    return float(m.group(1)) if m else None


def prev_lookup_from_report(prev_report: Optional[dict[str, Any]],
                            ) -> dict[tuple[str, str, str], float]:
    """{(route_type, ota_label, airline): common rate %} from a stored report dict."""
    out: dict[tuple[str, str, str], float] = {}
    for rt, grid in (prev_report or {}).get("grids", {}).items():
        for row in grid.get("rows", []):
            if row.get("kind") == "sep":
                continue
            for airline, raw in (row.get("cells") or {}).items():
                n = leading_number(raw)
                if n is not None:
                    out[(rt, row["label"], airline)] = n
    return out


def compute_highlights(report: dict[str, Any],
                       prev_lookup: Optional[dict[tuple[str, str, str], float]] = None,
                       ) -> dict[str, dict[str, Any]]:
    """Per route_type: {"flags": {(label, airline): changed|highest|second},
    "best": {airline: {pct, channel, short, display}}}."""
    prev_lookup = prev_lookup or {}
    out: dict[str, dict[str, Any]] = {}
    for rt, grid in report.get("grids", {}).items():
        cols = grid.get("columns", [])
        rows = [r for r in grid.get("rows", []) if r.get("kind") != "sep"]

        # Common rate per (label, airline), in ROW_ORDER (insertion order matters for
        # the Best row's first-wins tie-break, matching the original xlsx behavior).
        val: dict[tuple[str, str], float] = {}
        group_labels: dict[str, list[str]] = {}
        for row in rows:
            group_labels.setdefault(row.get("kind", "b2b"), []).append(row["label"])
            for airline in cols:
                n = leading_number((row.get("cells") or {}).get(airline))
                if n is not None:
                    val[(row["label"], airline)] = n

        flags: dict[tuple[str, str], str] = {}
        for labels in group_labels.values():
            for airline in cols:
                present = [(lab, val[(lab, airline)]) for lab in labels
                           if (lab, airline) in val]
                if not present:
                    continue
                ranked = sorted({v for _, v in present}, reverse=True)
                hi = ranked[0]
                second = ranked[1] if len(ranked) > 1 else None
                for lab, v in present:
                    prev = prev_lookup.get((rt, lab, airline))
                    if prev is not None and abs(prev - v) > 1e-6:
                        flags[(lab, airline)] = "changed"       # change wins
                    elif v == hi:
                        flags[(lab, airline)] = "highest"
                    elif second is not None and v == second:
                        flags[(lab, airline)] = "second"

        best: dict[str, dict[str, Any]] = {}
        for airline in cols:
            cand = [(v, lab) for (lab, a), v in val.items() if a == airline]
            if cand:
                bv, blab = max(cand, key=lambda x: x[0])
                short = BEST_SHORT.get(blab, blab)
                best[airline] = {"pct": bv, "channel": blab, "short": short,
                                 "display": f"{bv:g}% · {short}"}
        out[rt] = {"flags": flags, "best": best}
    return out


def apply_highlights(report: dict[str, Any],
                     prev_report: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """Return a COPY of `report` with per-cell `highlights` embedded in each row and a
    `best` entry per grid — the shape the web viewer renders directly. The red/changed
    diff comes from `prev_report` (the stored previous report), never a local xlsx."""
    hl = compute_highlights(report, prev_lookup_from_report(prev_report))
    new = copy.deepcopy(report)
    for rt, grid in new.get("grids", {}).items():
        for row in grid.get("rows", []):
            if row.get("kind") == "sep":
                continue
            row["highlights"] = {
                airline: hl[rt]["flags"].get((row["label"], airline), "none")
                for airline in grid.get("columns", [])
                if (row.get("cells") or {}).get(airline)
            }
        grid["best"] = hl[rt]["best"]
    new["prev_report_date"] = (prev_report or {}).get("report_date")
    return new
