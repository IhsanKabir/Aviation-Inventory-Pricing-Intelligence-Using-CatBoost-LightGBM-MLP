"""
KSA Market Comparison — all BD hubs to/from JED/RUH/DMM/MED.

Features:
- DAC, CGP, ZYL, CXB  x  JED, RUH, DMM, MED  — both directions
- Outbound | Return side-by-side on each worksheet
- Transit time shown as "DXB (~3h20m layover)" or "Direct"
- FirstTrip + AMY (amyweb) merged per route/date; AMY auto-skips if its
  session token is stale (set AMYWEB_TOKEN from a fresh capture to enable)
- Parallel collection across source x route x date (--workers)
- Air Arabia unified: 3L is aliased to G9 wherever a source returns it
- Per-cell colour coding (green cheap → red expensive) per column

Run:
    python tools/ksa_market_report.py
    python tools/ksa_market_report.py --start 2026-08-01 --end 2026-08-31
    python tools/ksa_market_report.py --workers 8 --amy off
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
load_dotenv(REPO_ROOT / ".env")

from modules.firsttrip import fetch_flights as ft_fetch
from modules.amyweb import fetch_flights as amy_fetch
from modules.biman import fetch_flights as _biman_fetch
from tools.route_selector import write_route_selector


def biman_fetch(origin, destination, date, cabin="Economy", adt=1, chd=0, inf=0,
                airline_code=None, **_kw):
    """Adapter so the Biman direct connector matches the report's source signature.
    Biman is an ADDITIONAL BG source (direct) — its data merges with FirstTrip/OTA BG, not replaces it."""
    return _biman_fetch(origin=origin, destination=destination, date=date,
                        cabin=cabin, adt=adt, chd=chd, inf=inf)
from modules.akbartravels import load_cache as akbar_load_cache, _cache_key as akbar_key
from modules.gozayaan_har import load_cache as goz_load_cache, _cache_key as goz_key
from modules.agoda_har import load_cache as agoda_load_cache, _cache_key as agoda_key
from modules.trip_har import load_cache as trip_load_cache, _cache_key as trip_key
from modules.sharetrip_har import load_cache as st_load_cache, _cache_key as st_key

BD_ORIGINS  = ["DAC", "CGP", "ZYL", "CXB"]
KSA_DESTS   = ["JED", "RUH", "DMM", "MED"]
# Long-haul / other international destinations — DAC only, both directions.
INTL_DESTS  = ["LHR", "LGW", "SYD", "CAN", "KMG", "AMM", "BEY", "FCO", "MXP",
               "CDG", "FRA", "MAD", "LIS", "BRU", "DUB", "CWL", "KTM", "KWI", "NRT", "HYD",
               "SLL", "PEN", "JHB", "CMB", "ICN", "BWN", "BKK",
               "CCU", "MAA", "AUH", "SHJ", "DXB", "KUL", "SIN"]
ALL_DESTS   = KSA_DESTS + INTL_DESTS
DEST_NAMES  = {"JED": "Jeddah", "RUH": "Riyadh", "DMM": "Dammam", "MED": "Medina",
               "LHR": "London Heathrow", "LGW": "London Gatwick", "SYD": "Sydney",
               "CAN": "Guangzhou", "KMG": "Kunming", "AMM": "Amman", "BEY": "Beirut",
               "FCO": "Rome", "MXP": "Milan", "CDG": "Paris", "FRA": "Frankfurt",
               "MAD": "Madrid", "LIS": "Lisbon", "BRU": "Brussels", "DUB": "Dublin",
               "CWL": "Cardiff", "KTM": "Kathmandu", "KWI": "Kuwait City",
               "NRT": "Tokyo Narita", "HYD": "Hyderabad", "SLL": "Salalah",
               "PEN": "Penang", "JHB": "Johor Bahru", "CMB": "Colombo",
               "ICN": "Seoul Incheon", "BWN": "Brunei", "BKK": "Bangkok",
               "CCU": "Kolkata", "MAA": "Chennai", "AUH": "Abu Dhabi",
               "SHJ": "Sharjah", "DXB": "Dubai", "KUL": "Kuala Lumpur",
               "SIN": "Singapore"}
BD_NAMES    = {"DAC": "Dhaka", "CGP": "Chittagong", "ZYL": "Sylhet", "CXB": "Cox's Bazar"}


def _origins_for(dest: str) -> list[str]:
    """KSA destinations are served from all BD hubs; the rest are DAC-only."""
    return BD_ORIGINS if dest in KSA_DESTS else ["DAC"]

# Air Arabia is sold under both 3L (Abu Dhabi) and G9 (Sharjah) — unify to G9.
AIRLINE_ALIAS = {"3L": "G9"}

# Restrained blue palette (no green->red heatmap; data stays calm with zebra banding).
COLOURS = {
    "title":       "#1f3864",  # route header — deep blue
    "col_header":  "#2e5496",  # column headers — slate blue
    "label":       "#8497b0",  # route-range label band — blue-grey
    "fare_cheap":  "#9dc3e6",  # cheapest fares — mid blue
    "fare_mid":    "#bdd7ee",  # light blue
    "fare_high":   "#deebf7",  # pale blue
    "band":        "#f2f5fa",  # zebra band for alternate data rows
    "white":       "#ffffff",
    "text_white":  "#ffffff",
    "text_dark":   "#212121",
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--start",      default="2026-08-01")
    p.add_argument("--end",        default="2026-08-31")
    p.add_argument("--output-dir", default="output/reports")
    p.add_argument("--workers",    type=int, default=6,
                   help="parallel query workers across source x route x date")
    p.add_argument("--amy",        choices=["auto", "on", "off"], default="auto",
                   help="AMY enrichment: auto=probe token then decide, on=force, off=FirstTrip only")
    p.add_argument("--cabin",      choices=["economy", "business", "both"], default="both",
                   help="cabin class: economy, business, or both (one combined workbook with a Class column)")
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
        lt = layover_times[0] if isinstance(layover_times, list) else str(layover_times)
        return f"{hubs} (~{lt} layover)"
    return hubs


def _norm_bag(b) -> str | None:
    """'30 KG' -> '30kg', '2 Pieces' -> '2pc'. Returns None for unknown."""
    if not b or str(b).strip() in ("--", ""):
        return None
    s = str(b).strip()
    import re
    m = re.match(r"(\d+)\s*KG", s, re.I)
    if m:
        return f"{int(m.group(1))}kg"
    m = re.match(r"(\d+)\s*Piece", s, re.I)
    if m:
        return f"{int(m.group(1))}pc"
    return s


def _baggage_all(baggages: list) -> str:
    """All distinct baggage tiers across an option's fares, e.g. '20kg/30kg'."""
    seen: dict[str, None] = {}
    for b in baggages:
        nb = _norm_bag(b)
        if nb:
            seen.setdefault(nb, None)
    if not seen:
        return "--"

    def _key(v: str):
        import re
        m = re.match(r"(\d+)", v)
        return (0, int(m.group(1))) if (m and v.endswith("kg")) else (1, v)

    return "/".join(sorted(seen, key=_key))


def query_source(fn, origin, dest, dep_date, cabin="Economy", airline_code=None):
    try:
        r = fn(origin=origin, destination=dest, date=str(dep_date),
               cabin=cabin, adt=1, chd=0, inf=0, airline_code=airline_code)
        return r.get("rows") or []
    except Exception as e:
        print(f"      ERROR {fn.__name__}: {e}")
        return []


def _offer_from_row(r: dict) -> dict:
    """Normalize one source row into the report's internal offer shape."""
    airline = AIRLINE_ALIAS.get(r.get("airline", ""), r.get("airline", ""))
    transit = _transit_label(r.get("via_airports"), r.get("layover_times") or [],
                             r.get("duration_min"))
    ops = [AIRLINE_ALIAS.get(c, c) for c in (r.get("operating_airlines") or [])]
    if not ops and r.get("operating_airline"):
        ops = [AIRLINE_ALIAS.get(r["operating_airline"], r["operating_airline"])]
    return {
        "airline":  airline,
        "operating": [c for c in ops if c],
        "transit":  transit,
        "flight":   str(r.get("flight_number") or "").strip(),
        "dep":      (r.get("departure") or "")[-8:][:5],
        "arr":      (r.get("arrival") or "")[-8:][:5],
        "aircraft": r.get("aircraft") or "",
        "price":    float(r.get("price_total_bdt") or 0),
        # base fare (pre-tax) in BDT where the source exposes it; 0/None -> derived later via tax model
        "base":     float(r.get("fare_amount") or 0),
        "baggage":  r.get("baggage") or "--",
        "rbd":      (r.get("rbd") or "").upper().strip(),
        "dur":      r.get("duration_min"),
    }


def _query(fn, origin, dest, dep_date, cabin):
    """Worker: returns (origin, dest, dep_date, [offer dicts]) for one source/route/date."""
    rows = query_source(fn, origin, dest, dep_date, cabin)
    return origin, dest, str(dep_date), [_offer_from_row(r) for r in rows]


def _amy_enabled(routes, dates, use_amy: str, cabin: str) -> bool:
    """Decide whether to include AMY: 'off' skips, 'on' forces, 'auto' probes the token."""
    if use_amy == "off":
        print("  AMY enrichment: OFF (FirstTrip only)")
        return False
    if use_amy == "on":
        print("  AMY enrichment: ON (forced)")
        return True
    probe = query_source(amy_fetch, routes[0][0], routes[0][1], dates[0], cabin)
    if probe:
        print("  AMY enrichment: ENABLED (token valid)")
        return True
    print("  AMY enrichment: DISABLED — amyweb token stale/empty. "
          "Capture a fresh one and set AMYWEB_TOKEN to enable.")
    return False


def collect(routes, dates, workers: int = 6, use_amy: str = "auto",
            cabin: str = "Economy", include_biman: bool = True) -> dict:
    """Return {(origin, dest, airline, transit_label, dep): [offers]} merged across sources.

    include_biman=False drops the (slow Sabre) Biman leg — FirstTrip already returns BG,
    so Biman is only additive; skipping it ~halves queries for a fast run.
    """
    # Live sources: FirstTrip (all airlines) + Biman (BG direct, additive — merges with FirstTrip/OTA BG).
    sources = [("FirstTrip", ft_fetch)]
    if include_biman:
        sources.append(("Biman", biman_fetch))
    if _amy_enabled(routes, dates, use_amy, cabin):
        sources.append(("AMY", amy_fetch))

    tasks = [(label, fn, o, d, dt)
             for (o, d) in routes for dt in dates for (label, fn) in sources]
    total = len(tasks)
    print(f"  {total} queries  ({len(routes)} routes x {len(dates)} dates x "
          f"{len(sources)} source(s)), {cabin}, {workers} workers")

    groups: dict = defaultdict(list)
    done = 0
    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futures = {ex.submit(_query, fn, o, d, dt, cabin): label
                   for (label, fn, o, d, dt) in tasks}
        for fut in as_completed(futures):
            origin, dest, qdate, offers = fut.result()
            for o in offers:
                o["class"] = cabin
                o["date"] = qdate
                key = (origin, dest, o["airline"], o["transit"], o["dep"], cabin)
                groups[key].append(o)
            done += 1
            if done % 100 == 0 or done == total:
                print(f"    {done}/{total} queries done — {len(groups)} flight-options so far")

    # Merge manually-captured OTA offers (HAR caches) — all airlines (BG included; Biman is additive)
    for label, load_fn, key_fn in (("AkbarTravels", akbar_load_cache, akbar_key),
                                   ("GoZayaan", goz_load_cache, goz_key),
                                   ("Agoda", agoda_load_cache, agoda_key),
                                   ("Trip.com", trip_load_cache, trip_key),
                                   ("ShareTrip", st_load_cache, st_key)):
        cache = load_fn()
        if not cache:
            continue
        added = 0
        for (origin, dest) in routes:
            for dt in dates:
                for r in cache.get(key_fn(origin, dest, str(dt), cabin), []):
                    o = _offer_from_row(r)
                    o["class"] = cabin
                    o["date"] = str(dt)
                    groups[(origin, dest, o["airline"], o["transit"], o["dep"], cabin)].append(o)
                    added += 1
        if added:
            print(f"  {label} (manual HAR cache, {cabin}): +{added} offers merged")

    return groups


_REF_BAGGAGE = None
_FT_BAGGAGE = None


def _firsttrip_baggage(airline: str, cabin: str) -> str | None:
    """Live-confirmed FirstTrip BrandedFare baggage (from tools/firsttrip_baggage_probe.py)."""
    global _FT_BAGGAGE
    if _FT_BAGGAGE is None:
        try:
            _FT_BAGGAGE = json.loads(
                (REPO_ROOT / "config" / "baggage_firsttrip.json").read_text(encoding="utf-8")
            ).get("airlines", {})
        except Exception:  # noqa: BLE001
            _FT_BAGGAGE = {}
    return (_FT_BAGGAGE.get(airline) or {}).get(str(cabin).lower())


def _ref_baggage(airline: str, cabin: str) -> str | None:
    """Curated policy-level baggage fallback (flagged with '*'). None if unknown."""
    global _REF_BAGGAGE
    if _REF_BAGGAGE is None:
        try:
            _REF_BAGGAGE = json.loads(
                (REPO_ROOT / "config" / "baggage_reference.json").read_text(encoding="utf-8")
            ).get("airlines", {})
        except Exception:  # noqa: BLE001
            _REF_BAGGAGE = {}
    v = (_REF_BAGGAGE.get(airline) or {}).get(str(cabin).lower())
    return f"{v} *" if v else None


def _observed_baggage(groups: dict) -> dict:
    """{(airline, cabin): consolidated baggage} from every offer that reported baggage."""
    obs: dict = defaultdict(list)
    for key, offers in groups.items():
        airline = key[2]
        cabin = key[5] if len(key) > 5 else "Economy"
        for o in offers:
            b = o.get("baggage")
            if b and str(b).strip() not in ("--", ""):
                obs[(airline, cabin)].append(b)
    return {k: _baggage_all(v) for k, v in obs.items()}


def _fill_baggage(group_bag: str, airline: str, cabin: str, observed: dict) -> str:
    """Cascade: this flight's baggage -> FirstTrip live policy -> same airline+cabin observed -> reference*."""
    if group_bag and group_bag != "--":
        return group_bag
    return (_firsttrip_baggage(airline, cabin) or observed.get((airline, cabin))
            or _ref_baggage(airline, cabin) or "--")


def _median(xs) -> float | None:
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def _build_tax_model(groups: dict) -> dict:
    """Tax = gross - base, learned ONLY from offers that expose a real base fare.
    Keyed most-specific first so a missing-base offer (Akbar/Trip/Biman) can borrow
    the tax another source reported for the same route+airline+cabin."""
    by_rac: dict = defaultdict(list)   # (origin, dest, airline, cabin)
    by_rc: dict = defaultdict(list)    # (origin, dest, cabin)
    by_ac: dict = defaultdict(list)    # (airline, cabin)
    ratios: list = []
    for key, offers in groups.items():
        origin, dest, airline, transit, dep = key[:5]
        cabin = key[5] if len(key) > 5 else "Economy"
        for off in offers:
            p = off.get("price") or 0
            b = off.get("base") or 0
            if p > 0 and 0 < b <= p:
                tax = p - b
                by_rac[(origin, dest, airline, cabin)].append(tax)
                by_rc[(origin, dest, cabin)].append(tax)
                by_ac[(airline, cabin)].append(tax)
                ratios.append(tax / p)
    return {
        "rac": {k: _median(v) for k, v in by_rac.items()},
        "rc": {k: _median(v) for k, v in by_rc.items()},
        "ac": {k: _median(v) for k, v in by_ac.items()},
        "ratio": _median(ratios),
    }


def _offer_base(off: dict, origin: str, dest: str, airline: str, cabin: str,
                model: dict) -> tuple[float | None, bool]:
    """Return (base_bdt, is_real). Real base when the source exposed it; otherwise
    base = gross - tax(route+airline+cabin) borrowed from the tax model."""
    p = off.get("price") or 0
    b = off.get("base") or 0
    if p > 0 and 0 < b <= p:
        return b, True
    tax = (model["rac"].get((origin, dest, airline, cabin))
           or model["rc"].get((origin, dest, cabin))
           or model["ac"].get((airline, cabin)))
    if tax is not None and tax < p:
        return p - tax, False
    if model["ratio"] is not None and p > 0:
        return p * (1 - model["ratio"]), False
    return None, False


_WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _dow_freq(offers: list) -> tuple[str, int]:
    """Day-of-week operating pattern from the set of dates an option was observed on.
    Airline schedules repeat weekly, so the union of weekdays seen across the sampled
    dates is the operating pattern. Returns (pattern_text, distinct_weekday_count).
    'Daily' when all 7 weekdays appear; '' when no usable dates."""
    import datetime as _dt
    wds: set[int] = set()
    for o in offers:
        s = str(o.get("date") or "")[:10]
        try:
            y, m, d = (int(x) for x in s.split("-"))
            wds.add(_dt.date(y, m, d).weekday())   # 0=Mon .. 6=Sun
        except (ValueError, TypeError):
            continue
    if not wds:
        return "", 0
    if len(wds) == 7:
        return "Daily", 7
    return " ".join(_WEEKDAYS[i] for i in sorted(wds)), len(wds)


def build_df(groups: dict) -> pd.DataFrame:
    observed = _observed_baggage(groups)
    tax_model = _build_tax_model(groups)
    records = []
    for key, offers in groups.items():
        origin, dest, airline, transit, dep = key[:5]
        cabin = key[5] if len(key) > 5 else "Economy"
        priced = [o for o in offers if o["price"] > 0]
        prices = [o["price"] for o in priced]
        if not prices:
            continue
        low_rbd  = min(priced, key=lambda o: o["price"]).get("rbd", "")
        high_rbd = max(priced, key=lambda o: o["price"]).get("rbd", "")
        # Base fares (pre-tax): real where the source exposed it, else tax-model derived
        bases, n_real = [], 0
        for off in priced:
            bv, real = _offer_base(off, origin, dest, airline, cabin, tax_model)
            if bv is not None:
                bases.append(bv)
                n_real += int(real)
        base_low = int(min(bases)) if bases else 0
        base_high = int(max(bases)) if bases else 0
        base_avg = int(sum(bases) / len(bases)) if bases else 0
        if not bases:
            base_est = "n/a"          # no base derivable at all
        elif n_real == len(bases):
            base_est = ""             # fully real base fares
        elif n_real == 0:
            base_est = "est"          # fully tax-model estimated
        else:
            base_est = "mix"          # blend of real + estimated
        arr_list = [o["arr"] for o in offers if o.get("arr")]
        arr = max(set(arr_list), key=arr_list.count) if arr_list else "--"
        ac_list = [o["aircraft"] for o in offers if o.get("aircraft")]
        aircraft = max(set(ac_list), key=ac_list.count) if ac_list else "--"
        baggage = _fill_baggage(_baggage_all([o.get("baggage") for o in offers]),
                                airline, cabin, observed)
        # Codeshare: operating carriers that differ from the marketing airline
        ops = {c for o in offers for c in o.get("operating", []) if c and c != airline}
        operated_by = ",".join(sorted(ops)) if ops else ""
        fl_list = [o["flight"] for o in offers if o.get("flight")]
        flight = max(set(fl_list), key=fl_list.count) if fl_list else "--"
        days, days_wk = _dow_freq(offers)
        records.append({
            "Origin":      origin,
            "Destination": dest,
            "Class":       cabin,
            "Airline":     airline,
            "Flight":      flight,
            "Operated By": operated_by,
            "Aircraft":    aircraft,
            "Transit":     transit,
            "Dep":         dep,
            "Arr":         arr,
            "Days":        days,
            "Days/Wk":     days_wk,
            "Lowest":      int(min(prices)),
            "Highest":     int(max(prices)),
            "Avg":         int(sum(prices) / len(prices)),
            "Base_Lowest": base_low,
            "Base_Highest": base_high,
            "Base_Avg":    base_avg,
            "Base_Est":    base_est,
            "Low_RBD":     low_rbd,
            "High_RBD":    high_rbd,
            "Fares":       len(prices),
            "Baggage":     baggage,
        })
    df = pd.DataFrame(records)
    if not df.empty:
        # Economy options before Business within each route, each cheapest-first
        df["_crank"] = (df["Class"] == "Business").astype(int)
        df = df.sort_values(["Origin", "Destination", "_crank", "Lowest"]).drop(columns="_crank")
    return df


def logical_clean(df: pd.DataFrame, min_n: int = 8, k: float = 1.5) -> pd.DataFrame:
    """Drop high-outlier flight rows per (route, class) so route averages are 'logical'.

    For each Origin+Destination+Class group with >= min_n rows, compute the IQR fence
    (Q3 + k*IQR) on the gross Avg and drop rows above it — the 600k-type multi-stop /
    last-seat / error fares that skew the mean. Cheap fares are kept; only the expensive
    tail is trimmed. Small groups are left untouched (IQR unstable on few points).
    """
    if df.empty or "Avg" not in df.columns:
        return df
    parts, removed = [], 0
    for _, g in df.groupby(["Origin", "Destination", "Class"], sort=False):
        if len(g) < min_n:
            parts.append(g)
            continue
        a = pd.to_numeric(g["Avg"], errors="coerce")
        q1, q3 = a.quantile(0.25), a.quantile(0.75)
        fence = q3 + k * (q3 - q1)
        kept = g[a <= fence]
        removed += len(g) - len(kept)
        parts.append(kept)
    out = pd.concat(parts).reset_index(drop=True)
    print(f"  logical_clean: dropped {removed} high-outlier rows ({len(df)} -> {len(out)})")
    return out


def _fare_colour(val: int, col_min: int, col_max: int, band: bool) -> str:
    """Gentle blue emphasis for fare cells only — cheaper = more blue. No warm colours."""
    if col_max == col_min:
        return COLOURS["fare_cheap"]
    pct = (val - col_min) / (col_max - col_min)
    if pct < 0.20: return COLOURS["fare_cheap"]   # cheapest -> mid blue
    if pct < 0.45: return COLOURS["fare_mid"]      # light blue
    if pct < 0.70: return COLOURS["fare_high"]     # pale blue
    return COLOURS["band"] if band else COLOURS["white"]  # priciest stays calm


def _sub_fmt(wb, fmt_cache):
    """Subscript font run (for RBD shown small after a fare)."""
    key = ("__subscript__",)
    if key not in fmt_cache:
        fmt_cache[key] = wb.add_format({"font_script": 2, "font_size": 9, "font_color": "#555555"})
    return fmt_cache[key]


def _cell_fmt(wb, fmt_cache, bg, fc=None, bold=False, num=False, sz=10, align="left",
              valign="vcenter", wrap=False):
    """Shared, cached cell format — thin #cccccc borders."""
    key = (bg, fc or COLOURS["text_dark"], bold, num, sz, align, valign, wrap)
    if key not in fmt_cache:
        d = {"bg_color": bg, "font_color": fc or COLOURS["text_dark"],
             "font_size": sz, "align": align, "valign": valign,
             "border": 1, "border_color": "#cccccc"}
        if bold: d["bold"] = True
        if num:  d["num_format"] = "#,##0"
        if wrap: d["text_wrap"] = True
        fmt_cache[key] = wb.add_format(d)
    return fmt_cache[key]


BLOCK_W = 19          # per block: schedule + gross + base Low/High/Avg + 7-day round-trip Low/High/Avg
RIGHT_OFFSET = 20     # right (return) block starts here; col 19 is the gap
COL_HDRS = ["Airline", "Flight", "Class", "Operated By", "Aircraft", "Transit / Stops",
            "Dep", "Arr", "Days",
            "Lowest", "Highest", "Avg", "Base Low", "Base High", "Base Avg",
            "Ret Low", "Ret High", "Ret Avg", "Baggage"]
COL_WIDTHS = [9, 8, 9, 11, 22, 30, 6, 6, 16, 11, 11, 11, 10, 10, 10, 11, 11, 11, 15]
_FARE_COL0 = 9        # first numeric column (gross Lowest); text/schedule cols are 0.._FARE_COL0-1


def _write_route_block(ws, wb, fmt_cache, start_row: int, start_col: int,
                       sub: pd.DataFrame, origin: str, dest: str,
                       start_dt: str, end_dt: str, opt_label: str = "options") -> dict:
    """Write one route block (calm blue theme, zebra banding).

    Returns {next_row, first_data_row, last_data_row, start_col} (rows 0-based, for formula refs).
    Dep/Arr are written as text so '15:55' never converts to a time/decimal.
    """

    def _fmt(bg, fc=None, bold=False, num=False, sz=10, align="left"):
        return _cell_fmt(wb, fmt_cache, bg, fc, bold, num, sz, align)

    dest_name = DEST_NAMES.get(dest, BD_NAMES.get(dest, dest))
    orig_name = BD_NAMES.get(origin, DEST_NAMES.get(origin, origin))
    n_opts = len(sub)
    has_base = "Base_Avg" in sub.columns
    route_low  = int(sub["Lowest"].min())
    route_high = int(sub["Highest"].max())
    route_avg  = int(sub["Avg"].mean())
    route_blow = int(sub["Base_Lowest"].min()) if has_base else route_low
    route_bhigh = int(sub["Base_Highest"].max()) if has_base else route_high
    route_bavg = int(sub["Base_Avg"].mean()) if has_base else route_avg
    has_rt = "RT_Avg" in sub.columns
    _rtl = sub["RT_Lowest"][sub["RT_Lowest"] > 0] if has_rt else []
    _rth = sub["RT_Highest"][sub["RT_Highest"] > 0] if has_rt else []
    _rta = sub["RT_Avg"][sub["RT_Avg"] > 0] if has_rt else []
    route_rtl = int(_rtl.min()) if len(_rtl) else 0
    route_rth = int(_rth.max()) if len(_rth) else 0
    route_rta = int(_rta.mean()) if len(_rta) else 0

    # -- Route header -- (Airline..Days cols 0-8; gross 9-11, base 12-14, round-trip 15-17, baggage 18)
    hdr_txt = f"{origin} → {dest}  ({orig_name} → {dest_name})  ·  {n_opts} {opt_label}  ·  {start_dt} to {end_dt}"
    ws.merge_range(start_row, start_col, start_row, start_col + 8, hdr_txt,
                   _fmt(COLOURS["title"], COLOURS["text_white"], bold=True, sz=11))
    for off, val in zip(range(9, 18),
                        (route_low, route_high, route_avg, route_blow, route_bhigh, route_bavg,
                         route_rtl, route_rth, route_rta)):
        ws.write_number(start_row, start_col + off, val,
                        _fmt(COLOURS["label"], COLOURS["text_white"], bold=True, num=True, align="right"))
    ws.write(start_row, start_col + 18, "", _fmt(COLOURS["label"], COLOURS["text_white"]))
    ws.set_row(start_row, 22)
    start_row += 1

    # -- Column headers --
    for ci, h in enumerate(COL_HDRS):
        ws.write(start_row, start_col + ci, h,
                 _fmt(COLOURS["col_header"], COLOURS["text_white"], bold=True,
                      align="center" if ci >= _FARE_COL0 else "left"))
    ws.set_row(start_row, 18)
    start_row += 1

    first_data_row = start_row
    _range_cols = ["Lowest", "Highest", "Avg"]
    if has_base:
        _range_cols += ["Base_Lowest", "Base_Highest", "Base_Avg"]
    col_ranges = {c: (int(sub[c].min()), int(sub[c].max())) for c in _range_cols}

    for di, (_, rec) in enumerate(sub.iterrows()):
        band = (di % 2 == 1)
        base = COLOURS["band"] if band else COLOURS["white"]
        f_txt = _fmt(base)
        biz = rec.get("Class") == "Business"

        ws.write(start_row, start_col + 0, rec["Airline"], _fmt(base, bold=True))
        ws.write(start_row, start_col + 1, str(rec.get("Flight", "") or ""),
                 _fmt(base, align="center"))
        # Class — tint business rows so the two cabins are easy to tell apart
        ws.write(start_row, start_col + 2, rec.get("Class", ""),
                 _fmt(COLOURS["fare_mid"] if biz else base, align="center", bold=biz))
        opby = rec.get("Operated By", "") or ""
        if opby:  # codeshare — flag with a subtle highlight
            ws.write(start_row, start_col + 3, opby,
                     _fmt(COLOURS["fare_high"], COLOURS["text_dark"], bold=True))
        else:
            ws.write(start_row, start_col + 3, "", f_txt)
        ws.write(start_row, start_col + 4, rec["Aircraft"], f_txt)
        ws.write(start_row, start_col + 5, rec["Transit"], f_txt)
        ws.write_string(start_row, start_col + 6, str(rec["Dep"]), _fmt(base, align="center"))
        ws.write_string(start_row, start_col + 7, str(rec["Arr"]), _fmt(base, align="center"))
        ws.write(start_row, start_col + 8, str(rec.get("Days", "") or ""),
                 _fmt(base, align="center"))

        for ci, col in enumerate(("Lowest", "Highest", "Avg")):
            val = int(rec[col])
            cellfmt = _fmt(_fare_colour(val, col_ranges[col][0], col_ranges[col][1], band),
                           num=True, align="right")
            rbd = str(rec.get("Low_RBD") if col == "Lowest"
                      else rec.get("High_RBD") if col == "Highest" else "") or ""
            if rbd:  # show the booking class as a small subscript after the fare
                ws.write_rich_string(start_row, start_col + _FARE_COL0 + ci,
                                     f"{val:,}", _sub_fmt(wb, fmt_cache), rbd, cellfmt)
            else:
                ws.write_number(start_row, start_col + _FARE_COL0 + ci, val, cellfmt)

        # Base fare (pre-tax) columns 12-14 — plain numbers in the same calm blue scale
        for ci, col in enumerate(("Base_Lowest", "Base_Highest", "Base_Avg")):
            val = int(rec[col]) if has_base and rec.get(col) else int(rec[("Lowest", "Highest", "Avg")[ci]])
            cr = col_ranges.get(col) or col_ranges[("Lowest", "Highest", "Avg")[ci]]
            ws.write_number(start_row, start_col + 12 + ci, val,
                            _fmt(_fare_colour(val, cr[0], cr[1], band), num=True, align="right"))

        # Return (round-trip, 7-day) Lowest / Highest / Avg — cols 15/16/17
        for ci, col in enumerate(("RT_Lowest", "RT_Highest", "RT_Avg")):
            rv = int(rec[col]) if (has_rt and rec.get(col)) else 0
            cellbg = COLOURS["band"] if band else COLOURS["white"]
            if rv > 0:
                ws.write_number(start_row, start_col + 15 + ci, rv,
                                _fmt(cellbg, num=True, align="right"))
            else:
                ws.write(start_row, start_col + 15 + ci, "—", _fmt(cellbg, align="right"))
        ws.write(start_row, start_col + 18, rec["Baggage"], _fmt(base, align="center"))
        ws.set_row(start_row, 15)
        start_row += 1

    return {"next_row": start_row + 1, "first_data_row": first_data_row,
            "last_data_row": start_row - 1, "start_col": start_col}


def _band_blocks(dest: str) -> list[tuple[str, str]]:
    """Blocks for one destination band: origin->dest, dest->origin for each serving origin."""
    blocks: list[tuple[str, str]] = []
    for o in _origins_for(dest):
        blocks.append((o, dest))
        blocks.append((dest, o))
    return blocks


def _write_all_routes_sheet(writer, wb, fmt_cache, df, start_dt, end_dt,
                            sheet_name: str = "All Routes", direct_only: bool = False) -> dict:
    """Every route block side by side, banded by destination.

    direct_only=True keeps only direct-operated flights (blank Operated By).
    Returns {(origin,dest): {sheet,start_col,first,last}} for formula references.
    """
    ws = wb.add_worksheet(sheet_name)
    writer.sheets[sheet_name] = ws
    stride = BLOCK_W + 1
    for b in range(8):                      # up to 8 blocks across (KSA bands)
        base = b * stride
        for i, w in enumerate(COL_WIDTHS):
            ws.set_column(base + i, base + i, w)
        ws.set_column(base + BLOCK_W, base + BLOCK_W, 2)  # spacer

    opt_label = "direct-operated options" if direct_only else "options"
    meta: dict = {}
    band_row = 0
    for dest in ALL_DESTS:
        max_rows = 0
        for bi, (o, d) in enumerate(_band_blocks(dest)):
            sub = df[(df["Origin"] == o) & (df["Destination"] == d)]
            if direct_only:
                sub = sub[sub["Operated By"].fillna("") == ""]
            if sub.empty:
                continue
            m = _write_route_block(ws, wb, fmt_cache, band_row, bi * stride,
                                   sub, o, d, start_dt, end_dt, opt_label)
            meta[(o, d)] = {"sheet": sheet_name, "start_col": bi * stride,
                            "first": m["first_data_row"], "last": m["last_data_row"]}
            max_rows = max(max_rows, len(sub))
        if max_rows:
            band_row += 2 + max_rows + 2    # title + header + data + gap
    return meta


def _merge_bag_cells(values) -> str:
    """Merge per-option baggage strings into one cell, e.g. '0kg / 30kg / 40kg'."""
    import re
    toks: list[str] = []
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
    seen: list[str] = []
    for t in toks:
        if t not in seen:
            seen.append(t)

    def _k(x: str):
        m = re.match(r"(\d+)", x)
        return (0, int(m.group(1))) if (m and x.endswith("kg")) else (1, x)

    return " / ".join(sorted(seen, key=_k)) if seen else "—"


def _rbd_of(g, fare_col: str, rbd_col: str, want_min: bool) -> str:
    """RBD of the row holding the min (or max) fare in a groupby group."""
    if rbd_col not in g.columns or g.empty:
        return ""
    idx = g[fare_col].idxmin() if want_min else g[fare_col].idxmax()
    v = g.at[idx, rbd_col]
    return "" if pd.isna(v) else str(v).strip()


def _ordered_routes(df) -> list[tuple[str, str]]:
    """All (origin,dest) routes present in df, in All-Routes band order."""
    present = set(zip(df["Origin"], df["Destination"]))
    out: list[tuple[str, str]] = []
    for dest in ALL_DESTS:
        for od in _band_blocks(dest):
            if od in present:
                out.append(od)
    return out


def _write_data_sheet(writer, wb, df) -> None:
    """Hidden flat numeric table the baggage formulas key on (decoupled from the rich All Routes cells)."""
    ws = wb.add_worksheet("_Data")
    writer.sheets["_Data"] = ws
    for ci, h in enumerate(("Origin", "Destination", "Class", "Airline", "OperatedBy",
                            "Lowest", "Highest", "Avg",
                            "Base_Lowest", "Base_Highest", "Base_Avg",
                            "RT_Lowest", "RT_Highest", "RT_Avg",
                            "Flight", "Days")):   # cols O/P — appended so A..N formula refs are unaffected
        ws.write_string(0, ci, h)
    has_base = "Base_Avg" in df.columns
    has_rt = "RT_Avg" in df.columns
    for ri, (_, r) in enumerate(df.iterrows(), start=1):
        ws.write_string(ri, 0, str(r["Origin"]))
        ws.write_string(ri, 1, str(r["Destination"]))
        ws.write_string(ri, 2, str(r["Class"]))
        ws.write_string(ri, 3, str(r["Airline"]))
        opby = str(r.get("Operated By") or "")
        if opby:
            ws.write_string(ri, 4, opby)   # leave blank for direct so MINIFS(...,"") matches
        ws.write_number(ri, 5, int(r["Lowest"]))
        ws.write_number(ri, 6, int(r["Highest"]))
        ws.write_number(ri, 7, int(r["Avg"]))
        # Base fare columns (I/J/K) — fall back to gross if base not derivable
        ws.write_number(ri, 8, int(r["Base_Lowest"]) if has_base and r["Base_Lowest"] else int(r["Lowest"]))
        ws.write_number(ri, 9, int(r["Base_Highest"]) if has_base and r["Base_Highest"] else int(r["Highest"]))
        ws.write_number(ri, 10, int(r["Base_Avg"]) if has_base and r["Base_Avg"] else int(r["Avg"]))
        # Round-trip 7-day Lowest/Highest/Avg (L/M/N) — 0 when no RT data (formulas exclude 0s)
        ws.write_number(ri, 11, int(r["RT_Lowest"]) if (has_rt and r.get("RT_Lowest")) else 0)
        ws.write_number(ri, 12, int(r["RT_Highest"]) if (has_rt and r.get("RT_Highest")) else 0)
        ws.write_number(ri, 13, int(r["RT_Avg"]) if (has_rt and r.get("RT_Avg")) else 0)
        ws.write_string(ri, 14, str(r.get("Flight") or ""))
        ws.write_string(ri, 15, str(r.get("Days") or ""))
    ws.hide()


def _write_baggage_sheet(writer, wb, fmt_cache, df, sheet_name, direct_only=False) -> None:
    """Baggage by airline x class x route in a 4-wide grid, with live MIN/MAX/AVERAGEIFS
    formulas keyed on Origin+Dest+Airline+Class against the hidden _Data sheet
    (plus OperatedBy="" when direct_only)."""
    ws = wb.add_worksheet(sheet_name)
    writer.sheets[sheet_name] = ws
    BW, GRID = 9, 4                       # 9 cols/block (gross 3 + RT Low/High/Avg + Baggage), 4 wide
    stride = BW + 1
    for b in range(GRID):
        base = b * stride
        ws.set_column(base + 0, base + 0, 9)    # Airline
        ws.set_column(base + 1, base + 1, 10)   # Class
        ws.set_column(base + 2, base + 4, 11)   # Lowest/Highest/Average
        ws.set_column(base + 5, base + 7, 11)   # Return Low/High/Avg (7d RT)
        ws.set_column(base + 8, base + 8, 20)   # Baggage
        ws.set_column(base + BW, base + BW, 2)  # spacer

    def _fmt(bg, fc=None, bold=False, num=False, sz=10, align="left"):
        return _cell_fmt(wb, fmt_cache, bg, fc, bold, num, sz, align)

    def _expr(o, d, fn, vcol, airline, cls):
        D = "'_Data'!"
        # MINIFS/MAXIFS are post-2007 "future functions" — xlsxwriter needs the _xlfn. prefix
        # or Excel shows #NAME?. (AVERAGEIFS is a 2007 function and needs no prefix.)
        fn_str = f"_xlfn.{fn}" if fn in ("MINIFS", "MAXIFS") else fn
        crit = (f'{D}$A:$A,"{o}",{D}$B:$B,"{d}",'
                f'{D}$D:$D,"{airline}",{D}$C:$C,"{cls}"')
        if direct_only:
            crit += f',{D}$E:$E,""'   # OperatedBy = "" -> direct-operated only
        return f"{fn_str}({D}${vcol}:${vcol},{crit})"

    def _rt_expr(o, d, airline, cls, fn="AVERAGEIFS", vcol="N"):
        # 7-day round-trip Lowest(L)/Highest(M)/Avg(N) from _Data, excluding 0 (no-RT) rows
        D = "'_Data'!"
        pre = "_xlfn." if fn in ("MINIFS", "MAXIFS") else ""
        crit = (f'{D}$A:$A,"{o}",{D}$B:$B,"{d}",{D}$D:$D,"{airline}",'
                f'{D}$C:$C,"{cls}",{D}${vcol}:${vcol},">0"')
        if direct_only:
            crit += f',{D}$E:$E,""'
        return f'{pre}{fn}({D}${vcol}:${vcol},{crit})'

    crank = {"Business": 0, "Economy": 1}
    routes = _ordered_routes(df)
    grid_row = 0
    for i in range(0, len(routes), GRID):
        chunk = routes[i:i + GRID]
        chunk_max = 0
        for j, (o, d) in enumerate(chunk):
            col = j * stride
            sub = df[(df["Origin"] == o) & (df["Destination"] == d)]
            if direct_only:
                sub = sub[sub["Operated By"].fillna("") == ""]
            # combos: (airline, class, avg, baggage, low, high, low_rbd, high_rbd) — biz first then avg desc
            combos = []
            for (a, c), g in sub.groupby(["Airline", "Class"]):
                rtl = g["RT_Lowest"][g["RT_Lowest"] > 0] if "RT_Lowest" in g.columns else []
                rth = g["RT_Highest"][g["RT_Highest"] > 0] if "RT_Highest" in g.columns else []
                rta = g["RT_Avg"][g["RT_Avg"] > 0] if "RT_Avg" in g.columns else []
                combos.append((a, c, int(round(g["Avg"].mean())),
                               _merge_bag_cells(g["Baggage"].tolist()),
                               int(g["Lowest"].min()), int(g["Highest"].max()),
                               _rbd_of(g, "Lowest", "Low_RBD", True),
                               _rbd_of(g, "Highest", "High_RBD", False),
                               int(rtl.min()) if len(rtl) else 0,
                               int(rth.max()) if len(rth) else 0,
                               int(round(rta.mean())) if len(rta) else 0))
            combos.sort(key=lambda r: (crank.get(r[1], 1), -r[2]))

            orig_name = BD_NAMES.get(o, DEST_NAMES.get(o, o))
            dest_name = DEST_NAMES.get(d, BD_NAMES.get(d, d))
            title = f"{o} → {d}  ({orig_name} → {dest_name})"
            # Append 3 live fare metrics off _Data (codes extracted from the title text).
            D = "'_Data'!"
            e_crit = f',{D}$E:$E,""' if direct_only else ""  # no-interline filter where applicable
            tl = title.replace('"', '""')

            def _avgifs(vcol, cls, _tl=tl, _e=e_crit):
                return (f'AVERAGEIFS({D}${vcol}:${vcol},'
                        f'{D}$A:$A,LEFT("{_tl}",3),{D}$B:$B,MID("{_tl}",7,3)'
                        f'{_e},{D}$C:$C,"{cls}")')

            head_formula = (
                f'="{tl}"'
                f'&IFERROR(" | Lowest (Economy): "&TEXT({_avgifs("F", "Economy")},"#,##0"),"")'
                f'&IFERROR(" | Highest (Economy): "&TEXT({_avgifs("G", "Economy")},"#,##0"),"")'
                f'&IFERROR(" | Average (Business): "&TEXT({_avgifs("H", "Business")},"#,##0"),"")'
            )
            # cached display value (matches the formula; sub is already no-interline if direct_only)
            eco, biz = sub[sub["Class"] == "Economy"], sub[sub["Class"] == "Business"]
            cached = title
            if len(eco):
                cached += f" | Lowest (Economy): {int(round(eco['Lowest'].mean())):,}"
                cached += f" | Highest (Economy): {int(round(eco['Highest'].mean())):,}"
            if len(biz):
                cached += f" | Average (Business): {int(round(biz['Avg'].mean())):,}"
            hfmt = _cell_fmt(wb, fmt_cache, COLOURS["title"], COLOURS["text_white"],
                             bold=True, sz=11, align="left", valign="top", wrap=True)
            ws.merge_range(grid_row, col, grid_row, col + BW - 1, "", hfmt)
            ws.write_formula(grid_row, col, head_formula, hfmt, cached)
            for ci, h in enumerate(("Airline", "Class", "Lowest", "Highest", "Average",
                                    "Ret Low", "Ret High", "Ret Avg", "Baggage")):
                ws.write(grid_row + 1, col + ci, h,
                         _fmt(COLOURS["col_header"], COLOURS["text_white"], bold=True,
                              align="left" if ci in (0, 8) else "center"))
            econ_i = 0
            for ri, (airline, cls, av, bag, lo, hi, lo_rbd, hi_rbd, rt_lo, rt_hi, rt_av) in enumerate(combos):
                biz = cls == "Business"
                base = COLOURS["fare_mid"] if biz else (COLOURS["band"] if econ_i % 2 else COLOURS["white"])
                if not biz:
                    econ_i += 1
                r = grid_row + 2 + ri
                fnum = _fmt(base, num=True, align="right")
                ws.write(r, col + 0, airline, _fmt(base, bold=True))
                ws.write(r, col + 1, cls, _fmt(base, align="center", bold=biz))
                # Lowest/Highest: fare + RBD as a small subscript (rich text). Average stays a live formula.
                if lo_rbd:
                    ws.write_rich_string(r, col + 2, f"{lo:,}", _sub_fmt(wb, fmt_cache), lo_rbd, fnum)
                else:
                    ws.write_number(r, col + 2, lo, fnum)
                if hi_rbd:
                    ws.write_rich_string(r, col + 3, f"{hi:,}", _sub_fmt(wb, fmt_cache), hi_rbd, fnum)
                else:
                    ws.write_number(r, col + 3, hi, fnum)
                ws.write_formula(r, col + 4,
                                 "=ROUND(IFERROR(" + _expr(o, d, "AVERAGEIFS", "H", airline, cls) + ",0),0)",
                                 fnum, av)
                for k, (fn, vcol, rtv) in enumerate((("MINIFS", "L", rt_lo),
                                                     ("MAXIFS", "M", rt_hi),
                                                     ("AVERAGEIFS", "N", rt_av))):
                    ws.write_formula(r, col + 5 + k,
                                     '=IFERROR(ROUND(' + _rt_expr(o, d, airline, cls, fn, vcol) + ',0),"—")',
                                     fnum, (rtv if rtv else "—"))
                ws.write(r, col + 8, bag, _fmt(base, align="center"))
            ws.set_row(grid_row, 32)   # tall headline row so wrapped title + metrics show fully
            chunk_max = max(chunk_max, 2 + len(combos))
        grid_row += chunk_max + 1


def write_excel(df: pd.DataFrame, groups: dict, path: Path, start_dt: str, end_dt: str) -> None:
    with pd.ExcelWriter(str(path), engine="xlsxwriter") as writer:
        wb = writer.book
        fmt_cache: dict = {}

        # Route Selector — interactive tool, placed FIRST (references the hidden _Data sheet)
        rs_ws = wb.add_worksheet("Route Selector")
        writer.sheets["Route Selector"] = rs_ws
        write_route_selector(wb, rs_ws, df)

        for bd_origin in BD_ORIGINS:
            # DAC also carries the long-haul/intl destinations
            dests = ALL_DESTS if bd_origin == "DAC" else KSA_DESTS
            sub_orig = df[df["Origin"].isin([bd_origin] + dests)]
            if sub_orig.empty:
                continue

            ws = wb.add_worksheet(bd_origin)
            writer.sheets[bd_origin] = ws

            # Mirror column widths for left (0..) and right (RIGHT_OFFSET..) blocks
            for blk in (0, RIGHT_OFFSET):
                for i, w in enumerate(COL_WIDTHS):
                    ws.set_column(blk + i, blk + i, w)
            ws.set_column(BLOCK_W, BLOCK_W, 3)  # centre gap

            cur_row = 0
            for dest in dests:
                out_df = df[(df["Origin"] == bd_origin) & (df["Destination"] == dest)]
                ret_df = df[(df["Origin"] == dest) & (df["Destination"] == bd_origin)]
                if out_df.empty and ret_df.empty:
                    continue

                max_rows = max(len(out_df), len(ret_df))
                if not out_df.empty:
                    _write_route_block(ws, wb, fmt_cache, cur_row, 0,
                                       out_df, bd_origin, dest, start_dt, end_dt)
                if not ret_df.empty:
                    _write_route_block(ws, wb, fmt_cache, cur_row, RIGHT_OFFSET,
                                       ret_df, dest, bd_origin, start_dt, end_dt)
                cur_row = cur_row + max_rows + 4

        # Summary sheets + hidden numeric _Data table that the baggage formulas key on.
        _write_all_routes_sheet(writer, wb, fmt_cache, df, start_dt, end_dt, "All Routes")
        _write_all_routes_sheet(writer, wb, fmt_cache, df, start_dt, end_dt,
                                "All Routes (No Interline)", direct_only=True)
        _write_data_sheet(writer, wb, df)
        _write_baggage_sheet(writer, wb, fmt_cache, df, "Baggage", direct_only=False)
        _write_baggage_sheet(writer, wb, fmt_cache, df, "Baggage (No Interline)", direct_only=True)


def main() -> int:
    args  = parse_args()
    dates = list(_date_range(args.start, args.end))
    print(f"\n[ksa_market_report]  {args.start} -> {args.end}  ({len(dates)} days)")

    # KSA: every BD hub <-> each KSA city (both directions)
    routes = [(bd, sa) for bd in BD_ORIGINS for sa in KSA_DESTS] + \
             [(sa, bd) for sa in KSA_DESTS for bd in BD_ORIGINS]
    # Intl/long-haul: DAC <-> each intl destination (both directions)
    routes += [("DAC", d) for d in INTL_DESTS] + [(d, "DAC") for d in INTL_DESTS]
    routes = list(dict.fromkeys(routes))  # dedupe preserving order

    cabins = ["Economy", "Business"] if args.cabin == "both" else [args.cabin.capitalize()]
    groups: dict = {}
    for cab in cabins:
        print(f"\n--- Collecting {cab} ---")
        groups.update(collect(routes, dates, workers=args.workers, use_amy=args.amy, cabin=cab))
    df = build_df(groups)

    if df.empty:
        print("No data returned.")
        return 1

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts    = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    label = "combined" if args.cabin == "both" else args.cabin
    xlsx  = out_dir / f"ksa_market_{label}_{args.start}_{args.end}_{ts}.xlsx"
    csv   = out_dir / f"ksa_market_{label}_{args.start}_{args.end}_{ts}.csv"

    write_excel(df, groups, xlsx, args.start, args.end)
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
