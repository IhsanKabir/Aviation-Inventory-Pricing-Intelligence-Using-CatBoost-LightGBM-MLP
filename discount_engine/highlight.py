"""Highlight computation for the discount grid — the single source of truth.

Both renderers (the xlsx writer and the web viewer) color cells from THESE flags,
so they can never drift apart. Semantics (net-of-fee refactor, 2026-07):

- Cells rank per airline WITHIN each B2B / B2C group by the NET common rate —
  the common tier's pct minus its convenience fee (parse_cell_tiers()[0]["net"]).
  A plain cell ("12") nets 12; a coupon TEXT cell ("9(Bkash, 2% fee), 18 (EBL)")
  nets 7 on its common tier, so text cells participate fully.
- "changed" (differs from the previous report's NET common rate) takes precedence,
  then "highest" (green), then "second" (blue) — all by NET, so a fee-only move flags too.
- The "Best (net)" summary row shows, per airline: `universal` = the best NET rate
  anyone gets (common tiers), plus a `gated` tier = the best NET card/loyalty special.

All rates here are in PERCENT units (12 == 12%); net = pct - convenience-fee %.
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


def _split_top_level(s: str) -> list[str]:
    """Split on top-level commas only (labels inside (...) may contain commas)."""
    parts, depth, cur = [], 0, ""
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        if ch == "," and depth == 0:
            parts.append(cur)
            cur = ""
        else:
            cur += ch
    if cur.strip():
        parts.append(cur)
    return [p.strip() for p in parts if p.strip()]


def parse_cell_tiers(raw: Any) -> list[dict[str, Any]]:
    """Parse a discount cell into ordered tiers. tiers[0] = the COMMON rate anyone gets;
    the rest are card/loyalty specials. Each tier: {pct, label, fee_pct, capped, net}.
    net = pct - fee_pct (the value a customer actually keeps after the convenience fee).
      '9(Bkash, 2% fee), 18 (Stellar Signature, 2% fee)'
        -> [{9,'Bkash',2,F,7}, {18,'Stellar Signature',2,F,16}]
      '7.1%, 8.1% (GPStar, 1.5% fee), 7.4% (Stellar Signature, capped, 2% fee)'
        -> [{7.1,'',0,F,7.1}, {8.1,'GPStar',1.5,F,6.6}, {7.4,'Stellar Signature',2,T,5.4}]"""
    text = str(raw or "").strip()
    tiers: list[dict[str, Any]] = []
    for seg in _split_top_level(text):
        m = re.match(r"~?\s*(-?\d+(?:\.\d+)?)", seg)
        if not m:
            continue
        pct = float(m.group(1))
        paren = re.search(r"\(([^)]*)\)", seg)
        label, fee, capped = "", 0.0, False
        if paren:
            body = paren.group(1)
            capped = "capped" in body.lower()
            fm = re.search(r"(\d+(?:\.\d+)?)\s*%\s*fee", body)
            if fm:
                fee = float(fm.group(1))
            lab = re.sub(r",?\s*\d+(?:\.\d+)?\s*%\s*fee", "", body)
            lab = re.sub(r",?\s*capped", "", lab, flags=re.I)
            label = lab.strip().strip(",").strip()
        tiers.append({"pct": pct, "label": label, "fee_pct": fee,
                      "capped": capped, "net": round(pct - fee, 2)})
    return tiers


def prev_lookup_from_report(prev_report: Optional[dict[str, Any]],
                            ) -> dict[tuple[str, str, str], float]:
    """{(route_type, ota_label, airline): NET common rate %} from a stored report dict.
    NET (common pct - fee) so change detection matches how ranking/Best rank cells — a
    fee-only move (gross flat, net changed) still counts as a change. The stored cell text
    keeps the fee annotation, so prev net reconstructs with no schema change."""
    out: dict[tuple[str, str, str], float] = {}
    for rt, grid in (prev_report or {}).get("grids", {}).items():
        for row in grid.get("rows", []):
            if row.get("kind") == "sep":
                continue
            for airline, raw in (row.get("cells") or {}).items():
                tiers = parse_cell_tiers(raw)
                if tiers:
                    out[(rt, row["label"], airline)] = tiers[0]["net"]
    return out


def compute_highlights(report: dict[str, Any],
                       prev_lookup: Optional[dict[tuple[str, str, str], float]] = None,
                       ) -> dict[str, dict[str, Any]]:
    """Per route_type: {"flags": {(label, airline): changed|highest|second},
    "best": {airline: {pct, channel, short, display, universal, gated}}}. `universal` is
    the anyone-gets tier dict and `gated` is the best card/loyalty tier dict (or None);
    pct/channel/short/display mirror `universal` for backward compatibility."""
    prev_lookup = prev_lookup or {}
    out: dict[str, dict[str, Any]] = {}
    for rt, grid in report.get("grids", {}).items():
        cols = grid.get("columns", [])
        rows = [r for r in grid.get("rows", []) if r.get("kind") != "sep"]

        # Per (label, airline): NET common (pct - fee, the real value used for ranking AND
        # change detection) and the parsed tiers.
        net_common: dict[tuple[str, str], float] = {}
        tiers_by: dict[tuple[str, str], list] = {}
        group_labels: dict[str, list[str]] = {}
        for row in rows:
            group_labels.setdefault(row.get("kind", "b2b"), []).append(row["label"])
            for airline in cols:
                tiers = parse_cell_tiers((row.get("cells") or {}).get(airline))
                if not tiers:
                    continue
                key = (row["label"], airline)
                net_common[key] = tiers[0]["net"]
                tiers_by[key] = tiers

        # highest/second rank by NET common; "changed" (net vs prev net) still wins.
        flags: dict[tuple[str, str], str] = {}
        for labels in group_labels.values():
            for airline in cols:
                present = [(lab, net_common[(lab, airline)]) for lab in labels
                           if (lab, airline) in net_common]
                if not present:
                    continue
                ranked = sorted({v for _, v in present}, reverse=True)
                hi = ranked[0]
                second = ranked[1] if len(ranked) > 1 else None
                for lab, v in present:
                    prev = prev_lookup.get((rt, lab, airline))
                    if prev is not None and abs(prev - net_common[(lab, airline)]) > 1e-6:
                        flags[(lab, airline)] = "changed"       # change wins (net vs net)
                    elif v == hi:
                        flags[(lab, airline)] = "highest"
                    elif second is not None and v == second:
                        flags[(lab, airline)] = "second"

        # Best per airline, NET and TIER-AWARE:
        #   universal = best net rate anyone gets (common tiers)
        #   gated     = best net rate needing a specific card/loyalty (special tiers)
        best: dict[str, dict[str, Any]] = {}
        for airline in cols:
            uni = []                      # (net, gross, short, full_label)
            gated = []                    # (net, gross, short, full_label, card_label, capped)
            for lab in [l for labs in group_labels.values() for l in labs]:
                tiers = tiers_by.get((lab, airline))
                if not tiers:
                    continue
                short = BEST_SHORT.get(lab, lab)
                uni.append((tiers[0]["net"], tiers[0]["pct"], short, lab))
                for t in tiers[1:]:
                    gated.append((t["net"], t["pct"], short, lab, t["label"], t["capped"]))
            if not uni:
                continue
            un = max(uni, key=lambda x: x[0])
            universal = {"net": un[0], "gross": un[1], "short": un[2], "channel": un[3],
                         "display": f"{un[0]:g}% net · {un[2]}"}
            gated_best = None
            if gated:
                # KNOWN LIMITATION (deferred): a CAPPED tier ranks by its nominal net here,
                # so a high-% capped coupon can outrank a lower-% uncapped one whose effective
                # saving on a real fare is larger. The cap AMOUNT isn't carried into the cell,
                # so we can't compute the true effective net at this layer — the display flags
                # ", capped" so the operator can judge. Proper fix = plumb an eff_net (cap-aware,
                # from grid's min(pct*base, maxDiscount)) through parse_cell_tiers, then rank on it.
                gb = max(gated, key=lambda x: x[0])
                lbl = f", {gb[4]}" if gb[4] else ""
                cap = ", capped" if gb[5] else ""
                gated_best = {"net": gb[0], "gross": gb[1], "short": gb[2], "channel": gb[3],
                              "label": gb[4], "capped": gb[5],
                              "display": f"{gb[0]:g}% net · {gb[2]}{lbl}{cap}"}
            # Primary (backward-compat) = the UNIVERSAL best (what anyone gets), matching
            # the historical Best row; the card-gated tier is carried alongside.
            best[airline] = {"pct": universal["gross"], "channel": universal["channel"],
                             "short": universal["short"], "display": universal["display"],
                             "universal": universal, "gated": gated_best}
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
