"""Regression tests for the --true-base recompute in ota_discount_grid.

Covers the two HIGH bugs found in review:
  1. BDFare margin must reuse the parser's commission_bdt (correct gross-agent fallback)
     instead of `(customer_net or agent) - agent`, which zeros out when customerNet is None.
  2. Unmatched domestic rows must be DROPPED in true-base mode, not left on their
     ratio-estimated value where summarize()'s max() could pick them.
Also asserts flag-OFF behaviour is unchanged.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))

import ota_discount_grid as g  # noqa: E402
from modules import true_base as tb_mod  # noqa: E402


def _oracle_bs():
    # BS gross 5549 -> base 4424 (implied fixed tax 1125)
    return tb_mod.build_from_rows(ft_b2b_rows=[
        {"airline": "BS", "origin": "DAC", "destination": "CGP",
         "gross_total_bdt": 5549, "base_fare_bdt": 4424}])


def _bd_row(airline, gross, agent, cust, comm_bdt, pct, dom=True):
    return {"channel": "bdfare", "persona": "B2B", "airline": airline,
            "origin": "DAC", "destination": "CGP", "domestic": dom,
            "gross_bdt": gross, "agent_bdt": agent, "customer_net_bdt": cust,
            "base_est_bdt": round(gross * 0.767), "commission_bdt": comm_bdt,
            "commission_pct": pct}


def test_bdfare_truebase_is_agent_discount_off_gross(monkeypatch):
    # Unified model: (actual gross 5549 - agent 5211) / true base 4424 = 7.64%.
    rows = [_bd_row("BS", 5549, 5211, 5566, 355, 8.34)]
    monkeypatch.setattr(g.bdfare_har, "parse_commissions", lambda p: rows)
    cells = g.collect_bdfare("x.har", true_base=_oracle_bs())
    assert cells[("DOM", "BS")] == "7.64"   # NOT the 8.02 margin, NOT 0


def test_bdfare_truebase_drops_oracle_absent_domestic(monkeypatch):
    rows = [
        _bd_row("BS", 5549, 5211, 5566, 355, 8.34),
        _bd_row("QR", 5000, 4000, 4500, 500, 13.04),   # QR not in oracle -> must drop
    ]
    monkeypatch.setattr(g.bdfare_har, "parse_commissions", lambda p: rows)
    cells = g.collect_bdfare("x.har", true_base=_oracle_bs())
    assert cells[("DOM", "BS")] == "7.64"          # (5549-5211)/4424 agent discount
    assert ("DOM", "QR") not in cells              # dropped, not contaminated


def test_bdfare_flag_off_is_unchanged(monkeypatch):
    rows = [_bd_row("BS", 5549, 5211, 5566, 355, 8.34)]
    monkeypatch.setattr(g.bdfare_har, "parse_commissions", lambda p: rows)
    cells = g.collect_bdfare("x.har")              # no true_base -> original value
    assert cells[("DOM", "BS")] == "8.34"


def test_bdfare_intl_passthrough_in_truebase_mode(monkeypatch):
    # Intl row must be untouched (true base is domestic-only).
    rows = [_bd_row("EK", 60000, 55000, 58000, 3000, 6.5, dom=False)]
    monkeypatch.setattr(g.bdfare_har, "parse_commissions", lambda p: rows)
    cells = g.collect_bdfare("x.har", true_base=_oracle_bs())
    assert cells[("INTL", "EK")] == "6.5"


if __name__ == "__main__":
    import subprocess
    raise SystemExit(subprocess.call([sys.executable, "-m", "pytest", __file__, "-q"]))
