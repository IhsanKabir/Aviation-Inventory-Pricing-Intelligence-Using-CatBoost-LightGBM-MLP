"""
KSA Market Comparison — all BD hubs to/from JED/RUH/DMM/MED.

Features:
- DAC, CGP, ZYL, CXB  x  JED, RUH, DMM, MED  — both directions
- Outbound | Return side-by-side on each worksheet
- Transit time shown as "DXB (~3h20m layover)" or "Direct"
- Aug 1-14 from FirstTrip; ShareTrip gap-fill for missing airlines
- Per-cell colour coding (green cheap → red expensive) per column

Run:
    python tools/ksa_market_report.py
    python tools/ksa_market_report.py --start 2026-08-01 --end 2026-08-14
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
load_dotenv(REPO_ROOT / ".env")

from modules.firsttrip import fetch_flights as ft_fetch
from modules.sharetrip import fetch_flights as st_fetch

BD_ORIGINS  = ["DAC", "CGP", "ZYL", "CXB"]
KSA_DESTS   = ["JED", "RUH", "DMM", "MED"]
DEST_NAMES  = {"JED": "Jeddah", "RUH": "Riyadh", "DMM": "Dammam", "MED": "Medina"}
BD_NAMES    = {"DAC": "Dhaka", "CGP": "Chittagong", "ZYL": "Sylhet", "CXB": "Cox's Bazar"}

# Colour palette
COLOURS = {
    "header_dark":  "#1a237e",
    "header_mid":   "#e65100",
    "col_header":   "#b71c1c",
    "green_dark":   "#1b5e20",
    "green":        "#388e3c",
    "green_light":  "#81c784",
    "lime":         "#c5e1a5",
    "yellow":       "#fff9c4",
    "amber":        "#ffe082",
    "orange":       "#ffb74d",
    "red_light":    "#ef9a9a",
    "red":          "#e53935",
    "red_dark":     "#b71c1c",
    "white":        "#ffffff",
    "text_white":   "#ffffff",
    "text_dark":    "#212121",
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--start",      default="2026-08-01")
    p.add_argument("--end",        default="2026-08-14")
    p.add_argument("--output-dir", default="output/reports")
    return p.parse_args()


def _date_range(start: str, end: str):
    d, e = date.fromisoformat(start), date.fromisoformat(end)
    while d <= e:
        yield d
        d += timedelta(days=1)


def _transit_label(via: str | None, layover_times: list, dur_min: int | None) -> str:
    if not via or via == "None":
        return "Direct"
    hubs = via.replace("|", "+")
    if layover_times:
        # Show first layover time
        lt = layover_times[0] if isinstance(layover_times, list) else str(layover_times)
        return f"{hubs} (~{lt} layover)"
    if dur_min:
        h, m = dur_min // 60, dur_min % 60
        return f"{hubs} ({h}h{m:02d}m total)"
    return hubs


def _baggage_tiers(price_bag: list) -> tuple:
    if not price_bag:
        return "--", "--", "--"
    srt = sorted(price_bag, key=lambda x: x[0])
    n = len(srt)
    return srt[0][1] or "--", srt[n // 2][1] or "--", srt[-1][1] or "--"


def query_source(fn, origin, dest, dep_date, airline_code=None):
    try:
        r = fn(origin=origin, destination=dest, date=str(dep_date),
               cabin="Economy", adt=1, chd=0, inf=0, airline_code=airline_code)
        return r.get("rows") or []
    except Exception as e:
        print(f"      ERROR {fn.__name__}: {e}")
        return []


def collect(routes, dates) -> dict:
    """Return {(o,d,airline,aircraft,transit_label,dep): [offers]}"""
    groups: dict = defaultdict(list)

    for origin, dest in routes:
        print(f"\n  {origin}->{dest}")
        seen_airlines: set = set()

        for dep_date in dates:
            rows = query_source(ft_fetch, origin, dest, dep_date)
            print(f"    FirstTrip {dep_date}: {len(rows)} offers")
            for r in rows:
                seen_airlines.add(r.get("airline", ""))
                dep_t  = (r.get("departure") or "")[-8:][:5]
                arr_t  = (r.get("arrival") or "")[-8:][:5]
                via    = r.get("via_airports")
                lt     = r.get("layover_times") or []
                dur    = r.get("duration_min")
                transit = _transit_label(via, lt, dur)
                key = (origin, dest, r.get("airline",""), r.get("aircraft",""),
                       transit, dep_t)
                groups[key].append({
                    "price":   float(r.get("price_total_bdt") or 0),
                    "baggage": r.get("baggage") or "--",
                    "arr":     arr_t,
                    "dur":     dur,
                })

        # Gap-fill with ShareTrip for any missing major airlines
        # Use first date as probe
        probe = dates[0]
        st_rows = query_source(st_fetch, origin, dest, probe)
        st_airlines = {r.get("airline","") for r in st_rows}
        new_airlines = st_airlines - seen_airlines - {""}
        if new_airlines:
            print(f"    ShareTrip gap-fill: {sorted(new_airlines)}")
            for dep_date in dates:
                st_day = query_source(st_fetch, origin, dest, dep_date)
                for r in st_day:
                    if r.get("airline","") not in new_airlines:
                        continue
                    seen_airlines.add(r.get("airline",""))
                    dep_t  = (r.get("departure") or "")[-8:][:5]
                    arr_t  = (r.get("arrival") or "")[-8:][:5]
                    via    = r.get("via_airports")
                    transit = _transit_label(via, [], r.get("duration_min"))
                    key = (origin, dest, r.get("airline",""), r.get("aircraft",""),
                           transit, dep_t)
                    groups[key].append({
                        "price":   float(r.get("price_total_bdt") or 0),
                        "baggage": r.get("baggage") or "--",
                        "arr":     arr_t,
                        "dur":     r.get("duration_min"),
                    })

    return groups


def build_df(groups: dict) -> pd.DataFrame:
    records = []
    for (origin, dest, airline, aircraft, transit, dep), offers in groups.items():
        prices = [o["price"] for o in offers if o["price"] > 0]
        if not prices:
            continue
        arr_list = [o["arr"] for o in offers if o.get("arr")]
        arr = max(set(arr_list), key=arr_list.count) if arr_list else "--"
        bag_s, bag_m, bag_h = _baggage_tiers([(o["price"], o["baggage"]) for o in offers])
        records.append({
            "Origin":      origin,
            "Destination": dest,
            "Airline":     airline,
            "Aircraft":    aircraft,
            "Transit":     transit,
            "Dep":         dep,
            "Arr":         arr,
            "Lowest":      int(min(prices)),
            "Highest":     int(max(prices)),
            "Avg":         int(sum(prices) / len(prices)),
            "Bag_Start":   bag_s,
            "Bag_Mid":     bag_m,
            "Bag_High":    bag_h,
        })
    df = pd.DataFrame(records)
    if not df.empty:
        df = df.sort_values(["Origin", "Destination", "Lowest"])
    return df


def _cell_colour(val: int, col_min: int, col_max: int) -> tuple[str, str]:
    if col_max == col_min:
        return COLOURS["green"], COLOURS["text_white"]
    pct = (val - col_min) / (col_max - col_min)
    if pct < 0.10: return COLOURS["green_dark"],  COLOURS["text_white"]
    if pct < 0.25: return COLOURS["green"],        COLOURS["text_white"]
    if pct < 0.40: return COLOURS["green_light"],  COLOURS["text_dark"]
    if pct < 0.55: return COLOURS["lime"],         COLOURS["text_dark"]
    if pct < 0.65: return COLOURS["yellow"],       COLOURS["text_dark"]
    if pct < 0.75: return COLOURS["amber"],        COLOURS["text_dark"]
    if pct < 0.85: return COLOURS["orange"],       COLOURS["text_dark"]
    if pct < 0.93: return COLOURS["red_light"],    COLOURS["text_dark"]
    return COLOURS["red_dark"], COLOURS["text_white"]


def _write_route_block(ws, wb, fmt_cache, start_row: int, start_col: int,
                       sub: pd.DataFrame, origin: str, dest: str,
                       start_dt: str, end_dt: str) -> int:
    """Write one route block. Returns next available row."""
    END_COL = start_col + 10  # 11 columns wide

    def _fmt(bg, fc=None, bold=False, num=False, sz=10, align="left"):
        key = (bg, fc or COLOURS["text_dark"], bold, num, sz, align)
        if key not in fmt_cache:
            d = {"bg_color": bg, "font_color": fc or COLOURS["text_dark"],
                 "font_size": sz, "align": align, "valign": "vcenter",
                 "border": 1, "border_color": "#cccccc"}
            if bold: d["bold"] = True
            if num:  d["num_format"] = "#,##0"
            fmt_cache[key] = wb.add_format(d)
        return fmt_cache[key]

    dest_name = DEST_NAMES.get(dest, dest)
    orig_name = BD_NAMES.get(origin, origin)
    n_opts = len(sub)
    route_low  = sub["Lowest"].min()
    route_high = sub["Highest"].max()
    route_avg  = int(sub["Avg"].mean())

    # -- Route header --
    hdr_txt = f"{origin} → {dest} ({orig_name} → {dest_name})  ·  {n_opts} options  ·  {start_dt} to {end_dt}"
    ws.merge_range(start_row, start_col, start_row, start_col + 4, hdr_txt,
                   _fmt(COLOURS["header_dark"], COLOURS["text_white"], bold=True, sz=11))
    ws.write(start_row, start_col + 5, "Route range",
             _fmt(COLOURS["header_mid"], COLOURS["text_white"], bold=True))
    ws.write_number(start_row, start_col + 6, route_low,
                    _fmt(COLOURS["header_mid"], COLOURS["text_white"], bold=True, num=True, align="right"))
    ws.write_number(start_row, start_col + 7, route_high,
                    _fmt(COLOURS["header_mid"], COLOURS["text_white"], bold=True, num=True, align="right"))
    ws.write_number(start_row, start_col + 8, route_avg,
                    _fmt(COLOURS["header_mid"], COLOURS["text_white"], bold=True, num=True, align="right"))
    ws.merge_range(start_row, start_col + 9, start_row, start_col + 10, "",
                   _fmt(COLOURS["header_mid"], COLOURS["text_white"]))
    ws.set_row(start_row, 22)
    start_row += 1

    # -- Column headers --
    col_hdrs = ["Airline", "Aircraft", "Transit / Stops", "Dep", "Arr",
                "Lowest", "Highest", "Avg", "Start Bag", "Mid Bag", "High Bag"]
    for ci, h in enumerate(col_hdrs):
        ws.write(start_row, start_col + ci, h,
                 _fmt(COLOURS["col_header"], COLOURS["text_white"], bold=True))
    ws.set_row(start_row, 18)
    start_row += 1

    # Column min/max for per-cell colouring
    col_ranges = {
        "Lowest":  (sub["Lowest"].min(),  sub["Lowest"].max()),
        "Highest": (sub["Highest"].min(), sub["Highest"].max()),
        "Avg":     (sub["Avg"].min(),     sub["Avg"].max()),
    }

    for _, rec in sub.iterrows():
        # Text cells use row colour based on Lowest
        row_bg, row_fc = _cell_colour(rec["Lowest"],
                                      col_ranges["Lowest"][0], col_ranges["Lowest"][1])
        f_txt = _fmt(row_bg, row_fc)

        ws.write(start_row, start_col + 0, rec["Airline"],  f_txt)
        ws.write(start_row, start_col + 1, rec["Aircraft"],  f_txt)
        ws.write(start_row, start_col + 2, rec["Transit"],   f_txt)
        ws.write(start_row, start_col + 3, rec["Dep"],       f_txt)
        ws.write(start_row, start_col + 4, rec["Arr"],       f_txt)

        # Each fare cell coloured independently
        for ci, col in enumerate(["Lowest", "Highest", "Avg"]):
            bg, fc = _cell_colour(rec[col], col_ranges[col][0], col_ranges[col][1])
            ws.write_number(start_row, start_col + 5 + ci, int(rec[col]),
                            _fmt(bg, fc, num=True, align="right"))

        ws.write(start_row, start_col + 8,  rec["Bag_Start"], f_txt)
        ws.write(start_row, start_col + 9,  rec["Bag_Mid"],   f_txt)
        ws.write(start_row, start_col + 10, rec["Bag_High"],  f_txt)
        ws.set_row(start_row, 15)
        start_row += 1

    return start_row + 1  # blank spacer


def write_excel(df: pd.DataFrame, path: Path, start_dt: str, end_dt: str) -> None:
    with pd.ExcelWriter(str(path), engine="xlsxwriter") as writer:
        wb = writer.book
        fmt_cache: dict = {}

        # One sheet per BD origin (DAC, CGP, ZYL, CXB)
        for bd_origin in BD_ORIGINS:
            sub_orig = df[df["Origin"].isin([bd_origin] + KSA_DESTS)]
            if sub_orig.empty:
                continue

            ws = wb.add_worksheet(bd_origin)
            writer.sheets[bd_origin] = ws

            # Column widths — left block + gap + right block
            ws.set_column(0, 0, 10)   # Airline
            ws.set_column(1, 1, 22)   # Aircraft
            ws.set_column(2, 2, 32)   # Transit
            ws.set_column(3, 4, 6)    # Dep/Arr
            ws.set_column(5, 7, 11)   # Fares
            ws.set_column(8, 10, 13)  # Baggage
            ws.set_column(11, 11, 3)  # Gap
            ws.set_column(12, 12, 10)
            ws.set_column(13, 13, 22)
            ws.set_column(14, 14, 32)
            ws.set_column(15, 16, 6)
            ws.set_column(17, 19, 11)
            ws.set_column(20, 22, 13)

            cur_row = 0
            for dest in KSA_DESTS:
                # Left: outbound
                out_df = df[(df["Origin"] == bd_origin) & (df["Destination"] == dest)]
                # Right: return
                ret_df = df[(df["Origin"] == dest) & (df["Destination"] == bd_origin)]

                if out_df.empty and ret_df.empty:
                    continue

                # Find the taller block
                max_rows = max(len(out_df), len(ret_df))

                if not out_df.empty:
                    next_r = _write_route_block(ws, wb, fmt_cache, cur_row, 0,
                                                out_df, bd_origin, dest, start_dt, end_dt)
                if not ret_df.empty:
                    _write_route_block(ws, wb, fmt_cache, cur_row, 12,
                                       ret_df, dest, bd_origin, start_dt, end_dt)

                cur_row = (cur_row + max_rows + 4) if not out_df.empty else cur_row + max_rows + 4


def main() -> int:
    args  = parse_args()
    dates = list(_date_range(args.start, args.end))
    print(f"\n[ksa_market_report]  {args.start} -> {args.end}  ({len(dates)} days)")

    # All route pairs: BD origin × KSA dest + KSA dest × BD origin
    routes = [(bd, sa) for bd in BD_ORIGINS for sa in KSA_DESTS] + \
             [(sa, bd) for sa in KSA_DESTS for bd in BD_ORIGINS]
    routes = list(dict.fromkeys(routes))  # dedupe preserving order

    groups = collect(routes, dates)
    df = build_df(groups)

    if df.empty:
        print("No data returned.")
        return 1

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts   = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    xlsx = out_dir / f"ksa_market_{args.start}_{args.end}_{ts}.xlsx"
    csv  = out_dir / f"ksa_market_{args.start}_{args.end}_{ts}.csv"

    write_excel(df, xlsx, args.start, args.end)
    df.to_csv(str(csv), index=False)

    print(f"\nSummary:")
    for bd in BD_ORIGINS:
        for dest in KSA_DESTS:
            sub = df[(df["Origin"] == bd) & (df["Destination"] == dest)]
            if not sub.empty:
                print(f"  {bd}->{dest}: {len(sub):3d} options  "
                      f"Low:{sub['Lowest'].min():>8,}  High:{sub['Highest'].max():>8,}")

    print(f"\n[ksa_market_report] Excel -> {xlsx}")
    print(f"[ksa_market_report] CSV   -> {csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
