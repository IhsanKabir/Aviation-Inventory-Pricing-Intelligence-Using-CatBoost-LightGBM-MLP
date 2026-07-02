"""Tests for discount_engine.highlight — the single source of truth for grid coloring.

Semantics locked in: rank per airline WITHIN each B2B/B2C group by the leading COMMON
rate (text coupon cells included); changed-vs-previous wins over highest/second; the
Best row is the max common rate across ALL channels.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from discount_engine.highlight import (  # noqa: E402
    apply_highlights,
    compute_highlights,
    leading_number,
    prev_lookup_from_report,
)


def _report(cells_by_label: dict[str, dict[str, str]], cols=("BS", "2A")) -> dict:
    kinds = {"USBA OTA B2B": "b2b", "BDFare": "b2b",
             "Firsttrip-B2C": "b2c", "ShareTrip-B2C": "b2c"}
    rows = [{"label": lab, "kind": kinds[lab],
             "cells": {c: cells_by_label.get(lab, {}).get(c, "") for c in cols}}
            for lab in kinds]
    return {"report_date": "02/07/2026", "report_time": "1200",
            "grids": {"DOM": {"columns": list(cols), "rows": rows}}}


BASE = {
    "USBA OTA B2B": {"BS": "12", "2A": "13.2"},
    "BDFare": {"BS": "8.02", "2A": "13"},
    "Firsttrip-B2C": {"BS": "16", "2A": "16"},
    "ShareTrip-B2C": {"BS": "9(Bkash), 18 (Stellar)", "2A": "8.5(Bkash), 18 (Stellar)"},
}


def test_leading_number_variants():
    assert leading_number("9(Bkash), 18 (EBL)") == 9.0
    assert leading_number("12") == 12.0
    assert leading_number("-6.49") == -6.49
    assert leading_number("") is None and leading_number(None) is None


def test_highest_and_second_within_each_group():
    hl = compute_highlights(_report(BASE))["DOM"]
    f = hl["flags"]
    # B2B group: USBA 12 > BDFare 8.02 for BS
    assert f[("USBA OTA B2B", "BS")] == "highest"
    assert f[("BDFare", "BS")] == "second"
    # B2C group ranks by the COMMON rate of the coupon TEXT cell (16 > 9)
    assert f[("Firsttrip-B2C", "BS")] == "highest"
    assert f[("ShareTrip-B2C", "BS")] == "second"


def test_changed_wins_over_highest():
    prev = _report({**BASE, "USBA OTA B2B": {"BS": "11", "2A": "13.2"}})
    hl = compute_highlights(_report(BASE), prev_lookup_from_report(prev))["DOM"]
    assert hl["flags"][("USBA OTA B2B", "BS")] == "changed"     # 11 -> 12, beats green
    assert hl["flags"][("USBA OTA B2B", "2A")] == "highest"     # unchanged keeps rank


def test_coupon_text_change_detected_by_common_rate():
    prev = _report({**BASE, "ShareTrip-B2C": {"BS": "8(Bkash), 18 (Stellar)",
                                              "2A": "8.5(Bkash), 18 (Stellar)"}})
    hl = compute_highlights(_report(BASE), prev_lookup_from_report(prev))["DOM"]
    assert hl["flags"][("ShareTrip-B2C", "BS")] == "changed"    # common 8 -> 9
    assert hl["flags"][("ShareTrip-B2C", "2A")] == "second"     # unchanged


def test_best_row_across_all_channels():
    best = compute_highlights(_report(BASE))["DOM"]["best"]
    assert best["BS"] == {"pct": 16.0, "channel": "Firsttrip-B2C",
                          "short": "FT-B2C", "display": "16% · FT-B2C"}
    assert best["2A"]["pct"] == 16.0


def test_apply_highlights_embeds_flags_best_and_prev_date():
    prev = _report({**BASE, "USBA OTA B2B": {"BS": "11", "2A": "13.2"}})
    out = apply_highlights(_report(BASE), prev)
    dom = out["grids"]["DOM"]
    row = {r["label"]: r for r in dom["rows"]}["USBA OTA B2B"]
    assert row["highlights"]["BS"] == "changed"
    assert dom["best"]["BS"]["display"] == "16% · FT-B2C"
    assert out["prev_report_date"] == "02/07/2026"
    # original report untouched (copy semantics)
    src = _report(BASE)
    assert "highlights" not in src["grids"]["DOM"]["rows"][0]


def test_write_single_sheet_xlsx_colors_from_prev_report():
    import tempfile
    import openpyxl
    from discount_engine import write_single_sheet_xlsx

    tmp_dir = Path(tempfile.mkdtemp(prefix="hl_test_"))
    prev = _report({**BASE, "USBA OTA B2B": {"BS": "11", "2A": "13.2"}})
    out = write_single_sheet_xlsx(_report(BASE), prev, tmp_dir / "single.xlsx")
    ws = openpyxl.load_workbook(out)["02 July"]

    def fill(label, col_idx):
        for ri in range(1, ws.max_row + 1):
            if ws.cell(ri, 1).value == label:
                c = ws.cell(ri, col_idx)
                return str(c.fill.fgColor.rgb)[-6:] if c.fill.patternType else None
        return None

    assert fill("USBA OTA B2B", 2) == "FFC7CE"       # changed -> red
    assert fill("Firsttrip-B2C", 2) == "C6EFCE"      # highest -> green
    assert any(ws.cell(ri, 1).value == "Best (OTA)" for ri in range(1, ws.max_row + 1))


if __name__ == "__main__":
    import subprocess
    raise SystemExit(subprocess.call([sys.executable, "-m", "pytest", __file__, "-q"]))
