"""Unit tests for modules.true_base (domestic true-base oracle)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from modules import true_base as tb_mod  # noqa: E402


def _bs_rows():
    # FT B2B / Amy / FT B2C shapes for BS domestic: base = gross - 1125
    ftb2b = [{"airline": "BS", "origin": "DAC", "destination": "CGP",
              "gross_total_bdt": 5549, "base_fare_bdt": 4424}]
    amy = [{"airline": "BS", "origin": "DAC", "destination": "CGP",
            "tot_fare": 5849, "base_fare": 4724}]
    ftb2c = [{"airline": "BS", "origin": "DAC", "destination": "CGP",
              "gross_total_bdt": 6149, "base_fare_bdt": 5024}]
    return ftb2b, amy, ftb2c


def test_exact_match_returns_known_base():
    ftb2b, amy, ftb2c = _bs_rows()
    tb = tb_mod.build_from_rows(ftb2b, amy, ftb2c)
    base, how = tb.base_for("BS", 5549)
    assert base == 4424 and how == "exact"


def test_fixed_tax_fallback_for_unseen_gross():
    # All three points imply a fixed tax of 1125 -> an unseen gross uses gross - 1125.
    ftb2b, amy, ftb2c = _bs_rows()
    tb = tb_mod.build_from_rows(ftb2b, amy, ftb2c)
    base, how = tb.base_for("BS", 7049)        # unseen
    assert base == 7049 - 1125
    assert how == "tax:1125"


def test_near_gross_within_tolerance():
    ftb2b, amy, ftb2c = _bs_rows()
    tb = tb_mod.build_from_rows(ftb2b, amy, ftb2c)
    base, how = tb.base_for("BS", 5560)        # within 50 of 5549
    assert base == 4424 and how.startswith("near")


def test_unknown_airline_returns_none():
    ftb2b, amy, ftb2c = _bs_rows()
    tb = tb_mod.build_from_rows(ftb2b, amy, ftb2c)
    base, how = tb.base_for("QR", 30000)
    assert base is None and how == "none"


def test_international_rows_are_ignored():
    intl = [{"airline": "EK", "origin": "DAC", "destination": "DXB",
             "gross_total_bdt": 60000, "base_fare_bdt": 45000}]
    tb = tb_mod.build_from_rows(ft_b2b_rows=intl)
    assert not tb.has("EK")
    assert tb.base_for("EK", 60000) == (None, "none")


def test_markdown_gross_picks_smallest_gross_at_or_above_net():
    # BG market grosses 5249/5749 (tax 1225). A net of 4963 was marked down from 5249.
    tb = tb_mod.build_from_rows(ft_b2b_rows=[
        {"airline": "BG", "origin": "DAC", "destination": "CGP", "gross_total_bdt": 5249, "base_fare_bdt": 4024},
        {"airline": "BG", "origin": "DAC", "destination": "CGP", "gross_total_bdt": 5749, "base_fare_bdt": 4524},
    ])
    gross, base = tb.markdown_gross("BG", 4963)
    assert gross == 5249 and base == 4024            # (5249-4963)/4024 = 7.11%
    gross2, base2 = tb.markdown_gross("BG", 5425)
    assert gross2 == 5749 and base2 == 4524          # marked down from the next fare up


def test_markdown_gross_none_when_net_above_all_or_unknown():
    tb = tb_mod.build_from_rows(ft_b2b_rows=[
        {"airline": "BG", "origin": "DAC", "destination": "CGP", "gross_total_bdt": 5249, "base_fare_bdt": 4024}])
    assert tb.markdown_gross("BG", 9999) == (None, None)   # above all grosses
    assert tb.markdown_gross("QR", 5000) == (None, None)   # unknown airline


def _bs_bg_oracle():
    # BS tax 1125 (grosses 5549/6149), BG tax 1225 (grosses 5249/5749)
    return tb_mod.build_from_rows(ft_b2b_rows=[
        {"airline": "BS", "origin": "DAC", "destination": "CGP", "gross_total_bdt": 5549, "base_fare_bdt": 4424},
        {"airline": "BS", "origin": "DAC", "destination": "CGP", "gross_total_bdt": 6149, "base_fare_bdt": 5024},
        {"airline": "BG", "origin": "DAC", "destination": "CGP", "gross_total_bdt": 5249, "base_fare_bdt": 4024},
        {"airline": "BG", "origin": "DAC", "destination": "CGP", "gross_total_bdt": 5749, "base_fare_bdt": 4524},
    ])


def test_discount_trusts_site_gross_via_fixed_tax_when_markdown_shown():
    # AKIJ BS: gross 5849 (NOT in oracle) > net 5560 -> trust it; base via fixed tax 1125.
    tb = _bs_bg_oracle()
    pct, gross, base = tb.discount("BS", 5849, 5560)
    assert gross == 5849 and base == 4724            # 5849 - 1125
    assert pct == round((5849 - 5560) / 4724 * 100, 2)   # 6.12


def test_discount_overrides_gross_when_site_hid_markdown():
    # AKIJ BG: gross == net (4963) -> mark down from market gross 5249.
    tb = _bs_bg_oracle()
    pct, gross, base = tb.discount("BG", 4963, 4963)
    assert gross == 5249 and base == 4024
    assert pct == round((5249 - 4963) / 4024 * 100, 2)   # 7.11


def test_discount_exact_gross_uses_oracle_base():
    tb = _bs_bg_oracle()
    pct, gross, base = tb.discount("BS", 5549, 5018)     # FT-B2B-like agent net
    assert gross == 5549 and base == 4424 and pct == 12.0


def test_discount_unknown_airline_is_none():
    tb = _bs_bg_oracle()
    assert tb.discount("QR", 5000, 4000) == (None, None, None)


def test_cross_channel_disagreement_is_recorded():
    a = [{"airline": "BS", "origin": "DAC", "destination": "CGP",
          "gross_total_bdt": 5549, "base_fare_bdt": 4424}]
    b = [{"airline": "BS", "origin": "DAC", "destination": "CGP",
          "tot_fare": 5549, "base_fare": 4200}]   # disagrees by >2
    tb = tb_mod.build_from_rows(ft_b2b_rows=a, amy_rows=b)
    assert tb.disagreements and tb.disagreements[0][0] == "BS"
    # first writer wins for the stored value
    assert tb.base_for("BS", 5549)[0] == 4424


if __name__ == "__main__":
    import subprocess
    raise SystemExit(subprocess.call([sys.executable, "-m", "pytest", __file__, "-q"]))
