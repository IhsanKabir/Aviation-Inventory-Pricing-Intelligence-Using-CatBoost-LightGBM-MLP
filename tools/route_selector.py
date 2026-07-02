"""
Build the interactive "Route Selector" sheet for the KSA combined report.

Driven by the hidden _Data sheet (A=Origin B=Destination C=Class D=Airline
E=OperatedBy F=Lowest G=Highest H=Avg). Only non-interline rows (E="") are ever
counted/listed. Fares/baggage detail are computed from `df` (same source as the
Baggage sheet) and written as live formulas (recalc on load) + cached values.

Public: write_route_selector(workbook, worksheet, df)
"""
from __future__ import annotations

import re
import statistics
from typing import Optional

BD = {"DAC", "CGP", "ZYL", "CXB"}
KSA = {"JED", "RUH", "DMM", "MED"}

NAVY = "#1F3864"
SLATE = "#2E5496"
YELLOW = "#FFF2CC"
BAND = "#F2F5FA"
WHITE = "#FFFFFF"
GREEN_TXT = "#1B5E20"
GREY_TXT = "#777777"
BLUE_TXT = "#1F3864"

# Exclusion rule map (both directions). ("ex", set) or ("only", set).
_RULES_RAW = {
    ("DAC", "JED"): ("ex", ["TK", "MH"]),
    ("DAC", "RUH"): ("ex", ["MH", "X1", "G9", "TK", "ET"]),
    ("DAC", "DMM"): ("ex", ["FZ", "EY", "MS", "ET", "X1"]),
    ("DAC", "MED"): ("ex", ["EY", "MH", "TK", "X1", "FZ"]),
    ("DAC", "LHR"): ("ex", ["WY", "MH", "SQ", "CX", "TG"]),
    ("DAC", "SYD"): ("ex", ["QR", "EK"]),
    ("DAC", "CAN"): ("ex", ["EK", "TK", "QR", "UL", "CX", "CA"]),
    ("DAC", "KMG"): ("only", ["CZ"]),
    ("DAC", "AMM"): ("ex", ["WY", "TK"]),
    ("DAC", "BEY"): ("ex", ["SV", "EY"]),
    ("DAC", "FCO"): ("ex", ["CA", "CX"]),
    ("DAC", "MXP"): ("ex", ["CA", "CX", "TG"]),
}


def _route_str(o, d):
    return f"{o} → {d}"


def _rules_by_route():
    """route_str -> ('ex'|'only', [codes]); both directions."""
    out = {}
    for (o, d), (kind, codes) in _RULES_RAW.items():
        out[_route_str(o, d)] = (kind, codes)
        out[_route_str(d, o)] = (kind, codes)
    return out


def _comma(codes):
    return "," + ",".join(codes) + "," if codes else ""


def _merge_bag(values):
    """Merge baggage strings -> '0kg / 30kg / 40kg' (kg sorted) + pieces; '—' if none."""
    toks = []
    for v in values:
        if not v or str(v).strip() in ("--", "", "—"):
            continue
        for part in str(v).replace("*", "").replace("–", "-").split("/"):
            t = part.strip()
            if not t:
                continue
            m = re.match(r"(\d+(?:-\d+)?)\s*KG", t, re.I)
            if m:
                toks.append(f"{m.group(1)}kg"); continue
            m = re.match(r"(\d+)\s*Piece", t, re.I)
            if m:
                toks.append(f"{m.group(1)}pc"); continue
            toks.append(t.lower().replace(" ", ""))
    seen = []
    for t in toks:
        if t not in seen:
            seen.append(t)

    def _k(x):
        m = re.match(r"(\d+)", x)
        return (0, int(m.group(1))) if (m and x.endswith("kg")) else (1, x)

    return " / ".join(sorted(seen, key=_k)) if seen else "—"


def write_route_selector(wb, ws, df):  # noqa: C901 - one cohesive sheet builder
    n = len(df) + 1                       # _Data last row (data is rows 2..n)
    RULES = _rules_by_route()
    ni = df[df["Operated By"].fillna("") == ""].copy()   # non-interline only
    # The Route Selector works on BASE fare when available (_Data col K), else gross (col H).
    use_base = "Base_Avg" in df.columns
    AVG, LOW, HIGH = (("Base_Avg", "Base_Lowest", "Base_Highest") if use_base
                      else ("Avg", "Lowest", "Highest"))
    DCOL = "K" if use_base else "H"       # _Data column with the avg to key formulas on
    FARE_BASIS = "base fare" if use_base else "gross fare"

    # ---- formats ----
    def F(**kw):
        return wb.add_format(kw)

    f_title = F(bold=True, font_size=16, font_color=NAVY)
    f_lbl = F(bold=True, font_color="#212121")
    f_drop = F(font_color="#0000CC", bold=True, bg_color=YELLOW, border=1,
               border_color="#BFA100", align="left", valign="vcenter")
    f_banner = F(bold=True, font_color=WHITE, bg_color=NAVY, align="left", valign="vcenter", border=1, border_color="#cccccc")
    f_hdr = F(bold=True, font_color=WHITE, bg_color=SLATE, align="center", valign="vcenter", border=1, border_color="#cccccc")
    f_hdr_l = F(bold=True, font_color=WHITE, bg_color=SLATE, align="left", valign="vcenter", border=1, border_color="#cccccc")
    f_cell = F(border=1, border_color="#cccccc", align="left", valign="vcenter")
    f_num = F(border=1, border_color="#cccccc", align="right", valign="vcenter", num_format="#,##0;(#,##0);—")
    f_band = F(border=1, border_color="#cccccc", align="left", valign="vcenter", bg_color=BAND)
    f_num_band = F(border=1, border_color="#cccccc", align="right", valign="vcenter", num_format="#,##0;(#,##0);—", bg_color=BAND)
    f_flag = F(border=1, border_color="#cccccc", align="center", valign="vcenter", bold=True, font_color=BLUE_TXT)
    f_green = F(border=1, border_color="#cccccc", align="left", valign="top", text_wrap=True, font_color=GREEN_TXT)
    f_bagkg = F(border=1, border_color="#cccccc", align="left", valign="top", text_wrap=True, font_color=GREY_TXT, font_size=9)
    f_note = F(italic=True, font_color=GREY_TXT, font_size=9)
    f_title_route = F(bold=True, font_color=WHITE, bg_color=NAVY, align="left", valign="vcenter", border=1, border_color="#cccccc", text_wrap=True)
    # KSA accent variants (thick navy left border; divider = thick bottom)
    f_route_ksa = F(border=1, border_color="#cccccc", left=5, left_color=NAVY, align="left", valign="vcenter")

    def _fx(formula):
        formula = re.sub(r"\bFILTER\(", "_xlfn._xlws.FILTER(", formula)
        formula = re.sub(r"\bSORT\(", "_xlfn._xlws.SORT(", formula)
        formula = re.sub(r"\bUNIQUE\(", "_xlfn.UNIQUE(", formula)
        formula = re.sub(r"\bLET\(", "_xlfn.LET(", formula)
        formula = re.sub(r"\bTEXTJOIN\(", "_xlfn.TEXTJOIN(", formula)
        return formula

    # ---------- Python computations (for ordering + cached values + static cols) ----------
    present = sorted(set(zip(df["Origin"], df["Destination"])))

    def all_air(o, d):
        return sorted(set(ni[(ni.Origin == o) & (ni.Destination == d)].Airline))

    def included(o, d):
        aa = all_air(o, d)
        r = RULES.get(_route_str(o, d))
        if not r:
            return aa
        kind, s = r
        s = set(s)
        return [a for a in aa if (a in s)] if kind == "only" else [a for a in aa if a not in s]

    def avg_of(o, d, cls):
        inc = set(included(o, d))
        sub = ni[(ni.Origin == o) & (ni.Destination == d) & (ni.Class == cls) & (ni.Airline.isin(inc))]
        return int(round(sub[AVG].mean())) if len(sub) else None

    def rt_of(o, d, cls):
        if "RT_Avg" not in ni.columns:
            return None
        inc = set(included(o, d))
        sub = ni[(ni.Origin == o) & (ni.Destination == d) & (ni.Class == cls) & (ni.Airline.isin(inc))]
        r = sub["RT_Avg"][sub["RT_Avg"] > 0]
        return int(round(r.mean())) if len(r) else None

    def rt_lohiav(o, d, cls):
        """(lowest, highest, avg) round-trip over non-interline carriers on route+cabin."""
        if "RT_Avg" not in ni.columns:
            return (None, None, None)
        sub = ni[(ni.Origin == o) & (ni.Destination == d) & (ni.Class == cls)]
        lo = sub["RT_Lowest"][sub["RT_Lowest"] > 0]
        hi = sub["RT_Highest"][sub["RT_Highest"] > 0]
        av = sub["RT_Avg"][sub["RT_Avg"] > 0]
        return (int(lo.min()) if len(lo) else None,
                int(hi.max()) if len(hi) else None,
                int(round(av.mean())) if len(av) else None)

    def records(o, d):
        inc = set(included(o, d))
        return int(len(ni[(ni.Origin == o) & (ni.Destination == d) & (ni.Airline.isin(inc))]))

    # baggage / RBD map per (o,d,airline,class)
    bagmap = {}
    for (o, d, air, cls), g in ni.groupby(["Origin", "Destination", "Airline", "Class"]):
        lo_i = g[LOW].idxmin(); hi_i = g[HIGH].idxmax()
        bagmap[(o, d, air, cls)] = {
            "lo": int(g[LOW].min()), "hi": int(g[HIGH].max()),
            "lo_rbd": str(g.at[lo_i, "Low_RBD"] or ""), "hi_rbd": str(g.at[hi_i, "High_RBD"] or ""),
            "avg": int(round(g[AVG].mean())), "bag": _merge_bag(g["Baggage"].tolist()),
        }

    def _inc_items(o, d):
        """Included (airline,class) entries sorted Business-first then lowest desc."""
        inc = included(o, d)
        items = []
        for air in inc:
            for cls in ("Business", "Economy"):
                m = bagmap.get((o, d, air, cls))
                if m:
                    items.append((air, cls, m))
        items.sort(key=lambda t: (0 if t[1] == "Business" else 1, -t[2]["lo"]))
        return items

    def baggage_kg(o, d):
        kgs, raws = [], []
        for air, cls, m in _inc_items(o, d):
            if m["bag"] in ("", "—"):
                continue
            for tok in m["bag"].split("/"):
                tok = tok.strip()
                mm = re.match(r"(\d+)kg$", tok)
                if mm:
                    kgs.append(int(mm.group(1)))
                elif tok:
                    raws.append(tok)
        if kgs:
            return f"{min(kgs)} / {int(round(statistics.median(kgs)))} / {max(kgs)} kg"
        if raws:
            seen = []
            for x in raws:
                if x not in seen:
                    seen.append(x)
            return " / ".join(seen)
        return "—"

    def fare_detail(o, d):
        parts = []
        for air, cls, m in _inc_items(o, d):
            ci = "B" if cls == "Business" else "E"
            parts.append(f"{air}({ci}): {m['lo']:,}{m['lo_rbd']}–{m['hi']:,}{m['hi_rbd']}")
        return " | ".join(parts)

    def baggage_detail(o, d):
        parts = []
        for air, cls, m in _inc_items(o, d):
            ci = "B" if cls == "Business" else "E"
            parts.append(f"{air}({ci}): {m['bag']}")
        return " | ".join(parts)

    # ---------- 64-route ordering: KSA group first, paired (out then return), sorted by pair's lowest eco ----------
    def is_out(o, d):
        return o in BD
    seen_p, pairs = set(), []
    for (o, d) in present:
        if (o, d) in seen_p:
            continue
        rev = (d, o)
        fwd, other = ((o, d), rev) if is_out(o, d) else (rev, (o, d))
        seen_p.add((o, d)); seen_p.add(rev)
        pairs.append((fwd, other))

    def pair_eco(p):
        vals = [avg_of(*p[0], "Economy"), avg_of(*p[1], "Economy")]
        vals = [v for v in vals if v is not None]
        return min(vals) if vals else 10 ** 12

    def pair_ksa(p):
        return (set(p[0]) & KSA) or (set(p[1]) & KSA)

    ksa_pairs = sorted([p for p in pairs if pair_ksa(p)], key=pair_eco)
    non_pairs = sorted([p for p in pairs if not pair_ksa(p)], key=pair_eco)
    ordered = []
    for p in ksa_pairs + non_pairs:
        if p[0] in present:
            ordered.append(p[0])
        if p[1] in present:
            ordered.append(p[1])
    nroutes = len(ordered)
    n_ksa = sum(1 for od in ordered if (set(od) & KSA))

    default_route = "DAC → JED" if ("DAC", "JED") in present else _route_str(*ordered[0])

    def count_cls(o, d, cls, inc_set):
        return int(len(ni[(ni.Origin == o) & (ni.Destination == d) &
                          (ni.Class == cls) & (ni.Airline.isin(inc_set))]))

    fm = {"title": f_title, "lbl": f_lbl, "drop": f_drop, "banner": f_banner, "hdr": f_hdr,
          "hdr_l": f_hdr_l, "cell": f_cell, "num": f_num, "band": f_band, "num_band": f_num_band,
          "flag": f_flag, "note": f_note}

    _build_helper(ws, _fx, RULES, n)
    _build_left(ws, wb, _fx, fm, n, default_route, included, avg_of, bagmap, all_air, count_cls,
                DCOL, FARE_BASIS, nroutes, rt_lohiav)
    _build_routewise(ws, wb, _fx, n, ordered, n_ksa, default_route, RULES, all_air, included,
                     avg_of, records, baggage_kg, fare_detail, baggage_detail, DCOL, FARE_BASIS, rt_of)
    _finalize_columns(ws, nroutes)


# ============================ helper area (N:U + S:U map) ============================
def _build_helper(ws, _fx, RULES, n):
    """Hidden/grouped helper: route list, origin/dest split, airline list,
    include/exclude strings, and the static exclusion-rule map."""
    # route-list spill at AB2 (col 27) — far hidden col, frees M/N for route-wise return columns
    ws.write_dynamic_array_formula(
        1, 27, 1, 27,
        _fx(f'=IFERROR(SORT(UNIQUE(FILTER(_Data!$A$2:$A${n}&" → "&_Data!$B$2:$B${n},'
            f'_Data!$A$2:$A${n}<>""))),"")'),
        None, "")
    ws.write_formula(3, 15, "=LEFT($C$4,3)", None, "")
    ws.write_formula(5, 15, "=MID($C$4,7,3)", None, "")
    ws.write_dynamic_array_formula(
        1, 16, 1, 16,
        _fx(f'=IFERROR(SORT(UNIQUE(FILTER(_Data!$D$2:$D${n},'
            f'(_Data!$A$2:$A${n}=$P$4)*(_Data!$B$2:$B${n}=$P$6)*(_Data!$E$2:$E${n}="")))),"")'),
        None, "")
    # dynamic-array so TEXTJOIN(IF(range)) array-evaluates (recomputes on TRUE/FALSE toggle)
    ws.write_dynamic_array_formula(8, 15, 8, 15, _fx(
        '=","&TEXTJOIN(",",TRUE,IF(($B$23:$B$46=TRUE)*($C$23:$C$46<>""),$C$23:$C$46,""))&","'), None, "")
    ws.write_dynamic_array_formula(11, 15, 11, 15, _fx(
        '=","&TEXTJOIN(",",TRUE,IF(($B$23:$B$46=FALSE)*($C$23:$C$46<>""),$C$23:$C$46,""))&","'), None, "")
    ws.write_string(0, 18, "RouteKey")
    ws.write_string(0, 19, "ExcludeList")
    ws.write_string(0, 20, "OnlyList")
    r = 1
    for route_str, (kind, codes) in RULES.items():
        ws.write_string(r, 18, route_str)
        if kind == "ex":
            ws.write_string(r, 19, _comma(codes))
        else:
            ws.write_string(r, 20, _comma(codes))
        r += 1


# ============================ left block (selector + results + checkboxes) ============================
def _build_left(ws, wb, _fx, fm, n, default_route, included, avg_of, bagmap, all_air, count_cls,
                dcol="H", fare_basis="gross fare", nroutes=64, rt_lohiav=None):
    o0, d0 = default_route.split(" → ")
    inc0 = included(o0, d0)
    inc0_set = set(inc0)
    aa0 = all_air(o0, d0)

    def pa_avg(air, cls):
        m = bagmap.get((o0, d0, air, cls))
        return m["avg"] if m else None

    biz_av = [v for v in (pa_avg(a, "Business") for a in inc0) if v is not None]
    eco_av = [v for v in (pa_avg(a, "Economy") for a in inc0) if v is not None]

    ws.merge_range(1, 1, 1, 4, "✈  Route Selector", fm["title"])
    ws.write_string(3, 1, "Route:", fm["lbl"])
    ws.write_string(3, 2, default_route, fm["drop"])
    ws.merge_range(3, 3, 3, 4, "← pick any route (dropdown)", fm["note"])
    ws.data_validation(3, 2, 3, 2, {"validate": "list", "source": f"=$AB$2:$AB${1 + nroutes}"})
    ws.merge_range(5, 1, 5, 4,
                   "Toggle TRUE/FALSE in the Incl? column below to include/exclude carriers; "
                   "the summary updates live.", fm["note"])

    ws.merge_range(8, 1, 8, 3, f"Selected Route — {fare_basis.title()} Summary (non-interline, included carriers)", fm["banner"])
    ws.write_string(10, 1, "Metric", fm["hdr_l"])
    ws.write_string(10, 2, "Business", fm["hdr"])
    ws.write_string(10, 3, "Economy", fm["hdr"])

    # Results aggregate the checkbox table DIRECTLY via *IFS (recalc cleanly on TRUE/FALSE
    # toggle; no fragile array-IF/TEXTJOIN). D=Business avg col, E=Economy avg col.
    def avgifs(col):
        return f'=IFERROR(ROUND(AVERAGEIFS(${col}$23:${col}$46,$B$23:$B$46,TRUE),0),"—")'

    def minifs(col):
        return f'=_xlfn.MINIFS(${col}$23:${col}$46,$B$23:$B$46,TRUE)'   # 0 (no match) shows as — via num_format

    def maxifs(col):
        return f'=_xlfn.MAXIFS(${col}$23:${col}$46,$B$23:$B$46,TRUE)'

    def cnt_formula(cls):
        # fare records over _Data for the checked carriers (P9 is a dynamic-array string)
        c = (f'(_Data!$A$2:$A${n}=$P$4)*(_Data!$B$2:$B${n}=$P$6)*(_Data!$E$2:$E${n}="")*'
             f'ISNUMBER(SEARCH(","&_Data!$D$2:$D${n}&",",$P$9))*(_Data!$C$2:$C${n}="{cls}")')
        return f'=SUMPRODUCT({c})'

    # cached = per-airline mean (matches AVERAGEIFS over the checkbox table)
    avg0_b = int(round(sum(biz_av) / len(biz_av))) if biz_av else "—"
    avg0_e = int(round(sum(eco_av) / len(eco_av))) if eco_av else "—"
    ws.write_string(11, 1, "Average fare (BDT)", fm["band"])
    ws.write_formula(11, 2, avgifs("D"), fm["num_band"], avg0_b)
    ws.write_formula(11, 3, avgifs("E"), fm["num_band"], avg0_e)
    ws.write_string(12, 1, "Lowest carrier avg", fm["cell"])
    ws.write_formula(12, 2, minifs("D"), fm["num"], (min(biz_av) if biz_av else 0))
    ws.write_formula(12, 3, minifs("E"), fm["num"], (min(eco_av) if eco_av else 0))
    ws.write_string(13, 1, "Highest carrier avg", fm["band"])
    ws.write_formula(13, 2, maxifs("D"), fm["num_band"], (max(biz_av) if biz_av else 0))
    ws.write_formula(13, 3, maxifs("E"), fm["num_band"], (max(eco_av) if eco_av else 0))
    ws.write_string(14, 1, "Fare records", fm["cell"])
    ws.write_formula(14, 2, cnt_formula("Business"), fm["num"], count_cls(o0, d0, "Business", inc0_set))
    ws.write_formula(14, 3, cnt_formula("Economy"), fm["num"], count_cls(o0, d0, "Economy", inc0_set))
    ws.write_string(15, 1, "Carriers included", fm["band"])
    nb = sum(1 for a in inc0 if pa_avg(a, "Business") is not None)
    ne = sum(1 for a in inc0 if pa_avg(a, "Economy") is not None)
    ws.write_formula(15, 2, '=SUMPRODUCT(--($B$23:$B$46=TRUE),--ISNUMBER($D$23:$D$46))', fm["num_band"], nb)
    ws.write_formula(15, 3, '=SUMPRODUCT(--($B$23:$B$46=TRUE),--ISNUMBER($E$23:$E$46))', fm["num_band"], ne)
    cheap = "—"
    if eco_av:
        cheap = min((pa_avg(a, "Economy"), a) for a in inc0 if pa_avg(a, "Economy") is not None)[1]
    ws.write_string(16, 1, "Cheapest carrier (economy)", fm["cell"])
    ws.merge_range(16, 2, 16, 3, "", fm["cell"])
    ws.write_formula(16, 2,
        '=IFERROR(INDEX($C$23:$C$46,MATCH(_xlfn.MINIFS($E$23:$E$46,$B$23:$B$46,TRUE),$E$23:$E$46,0)),"—")',
        fm["cell"], cheap)

    # Return (round-trip, 7-day) Lowest / Avg / Highest — gross, per cabin, over non-interline
    # carriers on the route (_Data cols L=RT_Lowest, M=RT_Highest, N=RT_Avg; 0-rows excluded).
    def ret_formula(fn, vcol, cls):
        pre = "_xlfn." if fn in ("MINIFS", "MAXIFS") else ""
        body = (f'{pre}{fn}(_Data!${vcol}$2:${vcol}${n},'
                f'_Data!$A$2:$A${n},$P$4,_Data!$B$2:$B${n},$P$6,'
                f'_Data!$C$2:$C${n},"{cls}",_Data!$E$2:$E${n},"",'
                f'_Data!${vcol}$2:${vcol}${n},">0")')
        return f'=IFERROR(ROUND({body},0),"—")'

    rlo_b, rhi_b, rav_b = (rt_lohiav(o0, d0, "Business") if rt_lohiav else (None, None, None))
    rlo_e, rhi_e, rav_e = (rt_lohiav(o0, d0, "Economy") if rt_lohiav else (None, None, None))
    for row, label, fn, vcol, cb, ce in (
            (17, "Return lowest (7d)", "MINIFS", "L", rlo_b, rlo_e),
            (18, "Return avg (7d)", "AVERAGEIFS", "N", rav_b, rav_e),
            (19, "Return highest (7d)", "MAXIFS", "M", rhi_b, rhi_e)):
        ws.write_string(row, 1, label, fm["band"])
        ws.write_formula(row, 2, ret_formula(fn, vcol, "Business"), fm["num_band"], (cb if cb else "—"))
        ws.write_formula(row, 3, ret_formula(fn, vcol, "Economy"), fm["num_band"], (ce if ce else "—"))

    ws.merge_range(20, 1, 20, 4, "Include / Exclude Carriers (edit TRUE/FALSE)", fm["banner"])
    ws.write_string(21, 1, "Incl?", fm["hdr"])
    ws.write_string(21, 2, "Airline", fm["hdr_l"])
    ws.write_string(21, 3, "Business", fm["hdr"])
    ws.write_string(21, 4, "Economy", fm["hdr"])

    for i in range(24):
        rr = 22 + i
        x1 = rr + 1
        air = aa0[i] if i < len(aa0) else None
        band = (i % 2 == 1)
        cfmt = fm["band"] if band else fm["cell"]
        nfmt = fm["num_band"] if band else fm["num"]
        ws.write_formula(rr, 2,
            '=IFERROR(IF(INDEX($Q$2:$Q$40,ROW()-22)="","",INDEX($Q$2:$Q$40,ROW()-22)),"")',
            cfmt, (air or ""))
        bflag = _fx(
            f'=IF($C{x1}="","",LET('
            f'ex,IFERROR(IF(INDEX($T$2:$T$25,MATCH($C$4,$S$2:$S$25,0))=0,"",INDEX($T$2:$T$25,MATCH($C$4,$S$2:$S$25,0))),""),'
            f'on,IFERROR(IF(INDEX($U$2:$U$25,MATCH($C$4,$S$2:$S$25,0))=0,"",INDEX($U$2:$U$25,MATCH($C$4,$S$2:$S$25,0))),""),'
            f'IF(on<>"",ISNUMBER(SEARCH(","&$C{x1}&",",on)),NOT(ISNUMBER(SEARCH(","&$C{x1}&",",ex))))))')
        bval = "" if air is None else bool(air in inc0_set)
        ws.write_formula(rr, 1, bflag, fm["flag"], bval)
        df_b = (f'=IF($C{x1}="","",IFERROR(ROUND(AVERAGEIFS(_Data!${dcol}:${dcol},_Data!$A:$A,$P$4,_Data!$B:$B,$P$6,'
                f'_Data!$D:$D,$C{x1},_Data!$C:$C,"Business",_Data!$E:$E,""),0),"—"))')
        df_e = (f'=IF($C{x1}="","",IFERROR(ROUND(AVERAGEIFS(_Data!${dcol}:${dcol},_Data!$A:$A,$P$4,_Data!$B:$B,$P$6,'
                f'_Data!$D:$D,$C{x1},_Data!$C:$C,"Economy",_Data!$E:$E,""),0),"—"))')
        vb = pa_avg(air, "Business") if air else ""
        ve = pa_avg(air, "Economy") if air else ""
        ws.write_formula(rr, 3, df_b, nfmt, (vb if vb is not None else "—"))
        ws.write_formula(rr, 4, df_e, nfmt, (ve if ve is not None else "—"))


# ============================ route-wise table (G:L visible + V,W,X,Y hidden) ============================
def _build_routewise(ws, wb, _fx, n, ordered, n_ksa, default_route, RULES, all_air, included,
                     avg_of, records, baggage_kg, fare_detail, baggage_detail,
                     dcol="H", fare_basis="gross fare", rt_of=None):
    NAVY = "#1F3864"

    def mk(extra):
        base = {"border": 1, "border_color": "#cccccc", "valign": "vcenter"}
        base.update(extra)
        return wb.add_format(base)

    title = wb.add_format({"bold": True, "font_size": 13, "font_color": NAVY})
    hdr = mk({"bold": True, "font_color": "#FFFFFF", "bg_color": "#2E5496", "align": "center"})
    hdr_l = mk({"bold": True, "font_color": "#FFFFFF", "bg_color": "#2E5496", "align": "left"})
    g_norm = mk({"align": "left"})
    g_ksa = mk({"align": "left", "left": 5, "left_color": NAVY})
    num = mk({"align": "right", "num_format": "#,##0;(#,##0);—"})
    green = mk({"align": "left", "valign": "top", "text_wrap": True, "font_color": "#1B5E20"})
    bagkg = mk({"align": "left", "valign": "top", "text_wrap": True, "font_color": "#777777", "font_size": 9})
    hidden = wb.add_format({"font_size": 8})

    def div(fmt_kwargs):
        d = dict(fmt_kwargs)
        d["bottom"] = 5
        d["bottom_color"] = NAVY
        return wb.add_format(d)

    g_norm_d = div({"border": 1, "border_color": "#cccccc", "valign": "vcenter", "align": "left"})
    g_ksa_d = div({"border": 1, "border_color": "#cccccc", "valign": "vcenter", "align": "left", "left": 5, "left_color": NAVY})
    num_d = div({"border": 1, "border_color": "#cccccc", "valign": "vcenter", "align": "right", "num_format": "#,##0;(#,##0);—"})
    green_d = div({"border": 1, "border_color": "#cccccc", "valign": "top", "align": "left", "text_wrap": True, "font_color": "#1B5E20"})
    bagkg_d = div({"border": 1, "border_color": "#cccccc", "valign": "top", "align": "left", "text_wrap": True, "font_color": "#777777", "font_size": 9})

    KSA = {"JED", "RUH", "DMM", "MED"}

    def rule_strs(route_str):
        r = RULES.get(route_str)
        if not r:
            return "", ""
        kind, codes = r
        s = _comma(codes)
        return (s, "") if kind == "ex" else ("", s)

    # default route's unchecked (excluded) string -> V cached for the selected row
    do0, dd0 = default_route.split(" → ")
    def_excl = _comma([a for a in all_air(do0, dd0) if a not in set(included(do0, dd0))])

    ws.merge_range(1, 6, 1, 13, f"Route-wise Average — {fare_basis.upper()}  (one-way base + 7-day round-trip · non-interline · KSA routes grouped on top)", title)
    for col, txt, f in ((6, "Route", hdr_l), (7, "Business Avg", hdr), (8, "Economy Avg", hdr),
                        (9, "Baggage (kg)", hdr), (10, "Included Airlines", hdr_l), (11, "Fare Records", hdr),
                        (12, "Return Biz 7d", hdr), (13, "Return Eco 7d", hdr)):
        ws.write_string(2, col, txt, f)
    ws.write_string(2, 21, "ExcludeStr", hidden)
    ws.write_string(2, 22, "OnlyStr", hidden)
    ws.write_string(2, 23, "All Airlines Lowest-Highest (RBD)", hidden)
    ws.write_string(2, 24, "All Airlines Baggage", hidden)

    last_ksa = 2 + n_ksa  # 0-indexed row of last KSA data row (header at row idx 2)

    for i, (o, d) in enumerate(ordered):
        rr = 3 + i
        x1 = rr + 1
        route = f"{o} → {d}"
        is_ksa = bool(set((o, d)) & KSA)
        is_div = (rr == last_ksa)
        gf = (g_ksa_d if is_div else g_ksa) if is_ksa else (g_norm_d if is_div else g_norm)
        nf = num_d if is_div else num
        grf = green_d if is_div else green
        bgf = bagkg_d if is_div else bagkg

        exc_c, only_c = rule_strs(route)
        if route == default_route:
            exc_c, only_c = def_excl, ""

        ws.write_string(rr, 6, route, gf)
        # Business / Economy avg (SUMPRODUCT honoring exclude/only)
        for col, cls in ((7, "Business"), (8, "Economy")):
            c = (f'(_Data!$A$2:$A${n}=LEFT($G{x1},3))*(_Data!$B$2:$B${n}=MID($G{x1},7,3))*'
                 f'(_Data!$C$2:$C${n}="{cls}")*(_Data!$E$2:$E${n}="")*'
                 f'IF($W{x1}="",ISNUMBER(SEARCH(","&_Data!$D$2:$D${n}&",",$V{x1}))=FALSE,'
                 f'ISNUMBER(SEARCH(","&_Data!$D$2:$D${n}&",",$W{x1})))')
            f = f'=IFERROR(ROUND(SUMPRODUCT({c}*_Data!${dcol}$2:${dcol}${n})/SUMPRODUCT({c}),0),"—")'
            v = avg_of(o, d, cls)
            ws.write_formula(rr, col, f, nf, (v if v is not None else "—"))
        # Baggage (kg) — static
        ws.write_string(rr, 9, baggage_kg(o, d), bgf)
        # Included Airlines — dynamic LET (FILTER inside)
        kf = _fx(
            f'=LET(o,LEFT($G{x1},3),d,MID($G{x1},7,3),ex,$V{x1},on,$W{x1},'
            f'air,SORT(UNIQUE(FILTER(_Data!$D$2:$D${n},(_Data!$A$2:$A${n}=o)*(_Data!$B$2:$B${n}=d)*(_Data!$E$2:$E${n}=""),""))),'
            f'inc,IF(air="","",IF(on<>"",IF(ISNUMBER(SEARCH(","&air&",",on)),air,""),IF(ISNUMBER(SEARCH(","&air&",",ex)),"",air))),'
            f'TEXTJOIN(", ",TRUE,inc))')
        ws.write_dynamic_array_formula(rr, 10, rr, 10, kf, grf, ", ".join(included(o, d)))
        # Fare Records — SUMPRODUCT count
        cc = (f'(_Data!$A$2:$A${n}=LEFT($G{x1},3))*(_Data!$B$2:$B${n}=MID($G{x1},7,3))*(_Data!$E$2:$E${n}="")*'
              f'IF($W{x1}="",ISNUMBER(SEARCH(","&_Data!$D$2:$D${n}&",",$V{x1}))=FALSE,'
              f'ISNUMBER(SEARCH(","&_Data!$D$2:$D${n}&",",$W{x1})))')
        ws.write_formula(rr, 11, f'=SUMPRODUCT({cc})', nf, records(o, d))
        # Return Biz / Eco (7-day round-trip AVG, gross) over included carriers (_Data col N=RT_Avg)
        for col, cls in ((12, "Business"), (13, "Economy")):
            rc = (f'(_Data!$A$2:$A${n}=LEFT($G{x1},3))*(_Data!$B$2:$B${n}=MID($G{x1},7,3))*'
                  f'(_Data!$C$2:$C${n}="{cls}")*(_Data!$E$2:$E${n}="")*(_Data!$N$2:$N${n}>0)*'
                  f'IF($W{x1}="",ISNUMBER(SEARCH(","&_Data!$D$2:$D${n}&",",$V{x1}))=FALSE,'
                  f'ISNUMBER(SEARCH(","&_Data!$D$2:$D${n}&",",$W{x1})))')
            rf = f'=IFERROR(ROUND(SUMPRODUCT({rc}*_Data!$N$2:$N${n})/SUMPRODUCT({rc}),0),"—")'
            rv = rt_of(o, d, cls) if rt_of else None
            ws.write_formula(rr, col, rf, nf, (rv if rv else "—"))
        # hidden V exclude-str / W only-str
        ws.write_formula(rr, 21,
            f'=IF($G{x1}=$C$4,$P$12,IFERROR(IF(INDEX($T$2:$T$25,MATCH($G{x1},$S$2:$S$25,0))=0,"",'
            f'INDEX($T$2:$T$25,MATCH($G{x1},$S$2:$S$25,0))),""))', hidden, exc_c)
        ws.write_formula(rr, 22,
            f'=IF($G{x1}=$C$4,"",IFERROR(IF(INDEX($U$2:$U$25,MATCH($G{x1},$S$2:$S$25,0))=0,"",'
            f'INDEX($U$2:$U$25,MATCH($G{x1},$S$2:$S$25,0))),""))', hidden, only_c)
        # hidden X fare detail / Y baggage detail (static snapshots)
        ws.write_string(rr, 23, fare_detail(o, d), hidden)
        ws.write_string(rr, 24, baggage_detail(o, d), hidden)


def _finalize_columns(ws, nroutes=64):
    ws.hide_gridlines(2)
    # G..N visible route-wise (incl Return Biz=12, Return Eco=13); O(14) gap
    widths = {1: 24, 2: 14, 3: 13, 4: 13, 5: 3, 6: 16, 7: 13, 8: 13, 9: 17, 10: 42, 11: 13,
              12: 13, 13: 13, 14: 3}
    for col, w in widths.items():
        ws.set_column(col, col, w)
    # helper area P:U grouped + hidden
    ws.set_column(15, 20, 12, None, {"level": 1, "hidden": True})
    # route-wise hidden detail columns V,W,X,Y + route-list spill AB
    ws.set_column(21, 22, 13, None, {"hidden": True})
    ws.set_column(23, 24, 60, None, {"hidden": True})
    ws.set_column(27, 27, 18, None, {"hidden": True})
    # route-wise data rows: wrap-friendly height
    for rr in range(3, 3 + nroutes):
        ws.set_row(rr, 28)
