"""Canonical DOMESTIC base fare, learned from channels whose base/gross are exact.

FirstTrip B2B, FirstTrip B2C and Amy all report the SAME base for the same
(airline, gross) flight, and domestically the tax is a FIXED per-airline amount
(base = gross - tax, e.g. BS/2A/VQ tax 1125, BG tax 1225) — it does NOT scale with
price. So a channel that estimates base as `gross * ratio` (BDFare) or reclassifies
part of base as tax (AKIJ) ends up with an ALTERED base and a skewed discount %.

This module builds the true base per (airline, gross) from the agreeing channels and
lets other code re-derive any channel's discount on that true base. INTERNATIONAL is
out of scope here (intl tax varies by route, so there is no fixed-tax law to fall back
on) — base_for() returns (None, "none") when it has no exact/near match.
"""
from __future__ import annotations

from collections import defaultdict
from statistics import median
from typing import Any, Dict, List, Optional, Tuple

DOMESTIC_AIRPORTS = {"DAC", "CGP", "CXB", "ZYL", "SPD", "BZL", "RJH", "JSR", "SAH", "TKR", "IRD", "KMI"}

_NEAR_GROSS_TOL = 50   # BDT; treat a gross within this of a known one as the same fare bucket
_AGREE_TOL = 2         # BDT; bases within this are "the same" across channels


def is_domestic(origin: str, destination: str) -> bool:
    return origin in DOMESTIC_AIRPORTS and destination in DOMESTIC_AIRPORTS


class TrueBase:
    """Domestic base-fare oracle: exact (airline,gross)->base, with a fixed per-airline
    tax fallback for grosses not seen, and a record of any cross-channel disagreements."""

    def __init__(self) -> None:
        self.by_gross: Dict[str, Dict[int, int]] = defaultdict(dict)   # airline -> {gross: base}
        self.tax: Dict[str, int] = {}                                  # airline -> fixed domestic tax
        self.disagreements: List[Tuple[str, int, str, int, int]] = []  # (airline, gross, src, new, prev)

    def add(self, airline: str, gross: Any, base: Any, src: str) -> None:
        if not airline or not gross or not base:
            return
        g, b = round(float(gross)), round(float(base))
        if g <= 0 or b <= 0 or b >= g:
            return
        prev = self.by_gross[airline].get(g)
        if prev is None:
            self.by_gross[airline][g] = b
        elif abs(prev - b) > _AGREE_TOL:
            self.disagreements.append((airline, g, src, b, prev))

    def finalize(self) -> "TrueBase":
        for airline, gb in self.by_gross.items():
            taxes = [g - b for g, b in gb.items()]
            if taxes:
                self.tax[airline] = round(median(taxes))
        return self

    def base_for(self, airline: str, gross: Any) -> Tuple[Optional[int], str]:
        """Return (true_base, source). source: 'exact' | 'near:<g>' | 'tax:<t>' | 'none'."""
        if not gross:
            return None, "none"
        g = round(float(gross))
        gb = self.by_gross.get(airline, {})
        if g in gb:
            return gb[g], "exact"
        if gb:
            ng = min(gb, key=lambda x: abs(x - g))
            if abs(ng - g) <= _NEAR_GROSS_TOL:
                return gb[ng], f"near:{ng}"
        if airline in self.tax:
            return g - self.tax[airline], f"tax:{self.tax[airline]}"
        return None, "none"

    def markdown_gross(self, airline: str, net: Any) -> Tuple[Optional[int], Optional[int]]:
        """The market gross a NET price was marked down from: the smallest known gross
        >= net, with its base. Use when a channel reports a net/discounted price as its
        own 'gross' (e.g. AKIJ on some carriers), so (own_gross - total) understates the
        discount. Returns (gross, base) or (None, None) if unknown / net above all grosses."""
        gb = self.by_gross.get(airline)
        if not gb or not net:
            return None, None
        n = round(float(net))
        candidates = [g for g in gb if g >= n - 2]   # small tolerance for rounding
        if not candidates:
            return None, None
        g = min(candidates)
        return g, gb[g]

    def discount(self, airline: str, site_gross: Any, net: Any
                 ) -> Tuple[Optional[float], Optional[int], Optional[int]]:
        """Unified discount %: (actual market gross - the site's NET price) / actual base.

        Uses the site's own gross when it is a real market gross (matches the oracle
        exactly / nearly); otherwise the site's gross is untrusted (e.g. AKIJ reporting
        the net as its gross) and the net is marked down from the smallest market gross
        >= net. Returns (pct, gross_used, base_used); (None, None, None) if unresolvable.
        """
        if not net:
            return None, None, None
        n = round(float(net))
        sg = round(float(site_gross)) if site_gross else 0
        if sg > n + 1:
            # The site shows a real markdown from its own gross -> trust that gross; the
            # true base comes from the fixed per-airline tax (robust even if this exact
            # gross was never seen in the oracle).
            gross = sg
            base, _how = self.base_for(airline, sg)
        else:
            # The site hid the markdown (gross ~= net, e.g. AKIJ on BG) -> mark the net
            # down from the smallest known market gross at/above it.
            gross, base = self.markdown_gross(airline, n)
        if gross is None or not base:
            return None, None, None
        return round((gross - n) / base * 100, 2), gross, base

    def has(self, airline: str) -> bool:
        return airline in self.by_gross

    def is_empty(self) -> bool:
        """True when nothing was learned (no exact-base source) — callers should then
        fall back to the channel's own base rather than dropping all domestic rows."""
        return not self.by_gross


def build_from_rows(ft_b2b_rows: Optional[List[Dict[str, Any]]] = None,
                    amy_rows: Optional[List[Dict[str, Any]]] = None,
                    ft_b2c_rows: Optional[List[Dict[str, Any]]] = None) -> TrueBase:
    """Build the domestic TrueBase from the parser row shapes of the agreeing channels.
    Only domestic offers contribute (intl tax is not fixed)."""
    tb = TrueBase()
    for r in (ft_b2b_rows or []):
        if is_domestic(r.get("origin", ""), r.get("destination", "")):
            tb.add(r["airline"], r.get("gross_total_bdt"), r.get("base_fare_bdt"), "FTB2B")
    for r in (amy_rows or []):
        if is_domestic(r.get("origin", ""), r.get("destination", "")):
            tb.add(r["airline"], r.get("tot_fare"), r.get("base_fare"), "Amy")
    for r in (ft_b2c_rows or []):
        # FT B2C rows are already the searched route; treat all as the route they came from.
        if is_domestic(r.get("origin", ""), r.get("destination", "")):
            tb.add(r["airline"], r.get("gross_total_bdt"), r.get("base_fare_bdt"), "FTB2C")
    return tb.finalize()
