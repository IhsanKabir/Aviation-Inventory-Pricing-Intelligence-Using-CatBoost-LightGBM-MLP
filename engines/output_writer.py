import pandas as pd
import hashlib
from xlsxwriter.utility import xl_range

from engines.route_scope import load_airport_countries
from modules.fleet_mapping import get_fleet_capacity_map, get_fleet_inventory


class OutputWriter:
    """
    Excel renderer ONLY.
    No comparisons, no pruning, no business logic.
    """

    def __init__(self, style: str = "compact"):
        style_norm = str(style or "compact").strip().lower()
        self.style = style_norm if style_norm in {"compact", "presentation"} else "compact"

    def _style_cfg(self):
        if self.style == "presentation":
            return {
                "title": 17,
                "section": 13,
                "header": 12,
                "body": 11,
                "sub": 10,
                "legend_row": 22,
                "route_title_row": 28,
                "default_row": 20,
                "zoom": 115,
            }
        return {
            "title": 15,
            "section": 11,
            "header": 10,
            "body": 10,
            "sub": 9,
            "legend_row": 18,
            "route_title_row": 22,
            "default_row": 18,
            "zoom": 100,
        }

    @staticmethod
    def _is_na(value) -> bool:
        return value is None or (isinstance(value, float) and pd.isna(value)) or pd.isna(value)

    @staticmethod
    def _to_int(value):
        if OutputWriter._is_na(value):
            return None
        try:
            return int(value)
        except Exception:
            return None

    @staticmethod
    def _dominant_integer(values) -> int:
        cleaned = []
        for value in values or []:
            if OutputWriter._is_na(value):
                continue
            try:
                cleaned.append(int(value))
            except Exception:
                continue
        if not cleaned:
            return 0
        freq = pd.Series(cleaned, dtype="int64").value_counts()
        if freq.empty:
            return 0
        max_freq = int(freq.max())
        candidates = [int(idx) for idx, count in freq.items() if int(count) == max_freq]
        return max(candidates) if candidates else 0

    @staticmethod
    def _daily_flight_counts(frame: pd.DataFrame) -> pd.Series:
        if frame is None or frame.empty or "flight_date" not in frame.columns or "flight_key" not in frame.columns:
            return pd.Series(dtype="int64")
        base = frame[["flight_date", "flight_key"]].dropna(subset=["flight_date", "flight_key"]).copy()
        if base.empty:
            return pd.Series(dtype="int64")
        return base.groupby("flight_date")["flight_key"].nunique().sort_index()

    @staticmethod
    def _typical_weekday_count_sum(frame: pd.DataFrame, day_order: list[str]) -> int:
        if (
            frame is None
            or frame.empty
            or "flight_date" not in frame.columns
            or "flight_key" not in frame.columns
            or "day_name" not in frame.columns
        ):
            return 0
        base = frame[["day_name", "flight_date", "flight_key"]].dropna(subset=["day_name", "flight_date", "flight_key"]).copy()
        if base.empty:
            return 0
        daily = (
            base.groupby(["day_name", "flight_date"])["flight_key"]
            .nunique()
            .reset_index(name="flight_count")
        )
        weekday_typical = (
            daily.groupby("day_name")["flight_count"]
            .agg(lambda s: OutputWriter._dominant_integer(list(s)))
            .to_dict()
        )
        return int(sum(int(weekday_typical.get(day, 0) or 0) for day in day_order))

    @staticmethod
    def _date_span_summary(frame: pd.DataFrame) -> str:
        if frame is None or frame.empty or "flight_date" not in frame.columns:
            return "--"
        dates = pd.to_datetime(frame["flight_date"], errors="coerce").dropna()
        if dates.empty:
            return "--"
        dep_min = dates.min()
        dep_max = dates.max()
        day_count = int(dates.dt.normalize().nunique())
        if pd.isna(dep_min) or pd.isna(dep_max):
            return "--"
        if dep_min.date() == dep_max.date():
            return f"{dep_min.strftime('%d %b %Y')} ({day_count} date)"
        return f"{dep_min.strftime('%d %b')} to {dep_max.strftime('%d %b')} ({day_count} dates)"

    @staticmethod
    def _future_pattern_signal(frame: pd.DataFrame, day_order: list[str]) -> str:
        required = {"day_name", "flight_date", "flight_key", "departure_time"}
        if frame is None or frame.empty or not required.issubset(set(frame.columns)):
            return "--"

        base = frame[["day_name", "flight_date", "flight_key", "departure_time"]].copy()
        base["flight_date"] = pd.to_datetime(base["flight_date"], errors="coerce")
        base["departure_time"] = base["departure_time"].astype(str).str.strip().str.slice(0, 5)
        base.loc[base["departure_time"].isin({"", "None", "nan", "NaT"}), "departure_time"] = pd.NA
        base = base.dropna(subset=["day_name", "flight_date", "flight_key"])
        if base.empty:
            return "--"

        daily_counts = (
            base.groupby(["day_name", "flight_date"])["flight_key"]
            .nunique()
            .reset_index(name="flight_count")
        )
        count_var_days = []
        for day_name, grp in daily_counts.groupby("day_name", sort=False):
            values = sorted({int(v) for v in pd.to_numeric(grp["flight_count"], errors="coerce").dropna().tolist()})
            if len(values) > 1:
                count_var_days.append(str(day_name))

        timing_view = base.dropna(subset=["departure_time"]).copy()
        time_var_days = []
        if not timing_view.empty:
            timing_sets = (
                timing_view.groupby(["day_name", "flight_date"])["departure_time"]
                .agg(lambda s: tuple(sorted({str(v) for v in s if str(v).strip()})))
                .reset_index(name="timings")
            )
            for day_name, grp in timing_sets.groupby("day_name", sort=False):
                timing_patterns = {tuple(v) for v in grp["timings"].tolist() if isinstance(v, tuple)}
                if len(timing_patterns) > 1:
                    time_var_days.append(str(day_name))

        day_rank = {day: idx for idx, day in enumerate(day_order or [])}

        def _abbr(days):
            ordered = sorted({str(d) for d in days}, key=lambda d: day_rank.get(d, 999))
            return ", ".join([d[:3] for d in ordered])

        count_text = "Count: Stable"
        time_text = "Times: Stable"
        if count_var_days:
            count_text = f"Count: {_abbr(count_var_days)}"
        if time_var_days:
            time_text = f"Times: {_abbr(time_var_days)}"
        return f"{count_text} | {time_text}"

    @staticmethod
    def _build_ops_baseline_frame(
        current_df: pd.DataFrame,
        history_df: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame, str]:
        """
        Build a deduplicated flight-occurrence frame for ops reporting.

        Prefer historical capture observations when available so schedule-style
        counts reflect the usual route pattern rather than only the latest
        comparison slice. Each flight/date/time is counted once regardless of
        how many captures saw it.
        """
        candidates = []
        if isinstance(history_df, pd.DataFrame) and not history_df.empty:
            candidates.append(("historical capture dates", history_df.copy()))
        candidates.append(("current comparison slice", current_df.copy()))

        required = {"airline", "route", "flight_number", "flight_date", "departure_time"}
        for source_label, frame in candidates:
            if frame is None or frame.empty or not required.issubset(set(frame.columns)):
                continue
            work = frame.copy()
            work["airline"] = work["airline"].astype(str).str.upper().str.strip()
            work["route"] = work["route"].astype(str).str.strip()
            work["flight_number"] = work["flight_number"].astype(str).str.strip()
            work["flight_date"] = pd.to_datetime(work["flight_date"], errors="coerce")
            work["departure_time"] = work["departure_time"].astype(str).str.strip().str.slice(0, 5)
            work.loc[work["departure_time"].isin({"", "None", "nan", "NaT"}), "departure_time"] = pd.NA
            work = work.dropna(subset=["airline", "route", "flight_number", "flight_date", "departure_time"]).copy()
            if work.empty:
                continue

            work["day_name"] = work["flight_date"].dt.day_name()
            if "aircraft" not in work.columns:
                work["aircraft"] = pd.NA
            work["aircraft"] = work["aircraft"].fillna("Aircraft NA").astype(str)
            if "departure" in work.columns:
                work["departure"] = pd.to_datetime(work["departure"], errors="coerce")
            if "arrival" in work.columns:
                work["arrival"] = pd.to_datetime(work["arrival"], errors="coerce")

            work["flight_key"] = (
                work["airline"].astype(str)
                + "|"
                + work["flight_number"].astype(str)
                + "|"
                + work["departure_time"].astype(str)
            )
            work = work.drop_duplicates(
                subset=["route", "airline", "flight_number", "flight_date", "departure_time"],
                keep="last",
            ).copy()
            return work, source_label

        return pd.DataFrame(columns=["airline", "route", "flight_number", "flight_date", "departure_time", "day_name", "aircraft", "flight_key"]), "no usable ops baseline"

    @staticmethod
    def _bool_label(value):
        if OutputWriter._is_na(value):
            return "--"
        try:
            return "Yes" if bool(value) else "No"
        except Exception:
            return "--"

    @staticmethod
    def _delta_sign(value) -> int:
        if OutputWriter._is_na(value):
            return 0
        try:
            v = float(value)
        except Exception:
            return 0
        if v > 0:
            return 1
        if v < 0:
            return -1
        return 0

    @staticmethod
    def _flight_code_label(airline, flight_number) -> str:
        airline_s = str(airline or "").strip().upper()
        flight_s = str(flight_number or "").strip()
        if not flight_s:
            return airline_s
        upper = flight_s.upper()
        if airline_s and (upper.startswith(airline_s + "-") or upper.startswith(airline_s)):
            return flight_s
        return f"{airline_s}{flight_s}"

    @staticmethod
    def _join_limited(values, limit=8):
        vals = [str(v) for v in values if str(v).strip()]
        if limit is None:
            return ", ".join(vals)
        if len(vals) <= limit:
            return ", ".join(vals)
        return ", ".join(vals[:limit]) + f" (+{len(vals) - limit})"

    @staticmethod
    def _airline_theme_map(airlines=None):
        # Curated themes for known carriers + deterministic palette for new ones.
        themes = {
            "BG": {
                "header_bg": "#C8102E",
                "subheader_bg": "#FFFFFF",
                "cell_bg": "#F7F7F8",
                "header_font": "#FFFFFF",
                "text_font": "#2F2F2F",
            },
            "VQ": {
                "header_bg": "#003A70",
                "subheader_bg": "#F58220",
                "cell_bg": "#FFF3E8",
                "header_font": "#FFFFFF",
                "text_font": "#123A6E",
            },
            "BS": {
                "header_bg": "#00557F",
                "subheader_bg": "#D8EBF7",
                "cell_bg": "#F1F8FC",
                "header_font": "#FFFFFF",
                "text_font": "#12384D",
            },
            "2A": {
                "header_bg": "#B78700",
                "subheader_bg": "#FDEFC7",
                "cell_bg": "#FFF9EA",
                "header_font": "#1E1E1E",
                "text_font": "#6B4E00",
            },
            "G9": {
                "header_bg": "#C6282B",
                "subheader_bg": "#F9E1E2",
                "cell_bg": "#FDF2F3",
                "header_font": "#FFFFFF",
                "text_font": "#7A1D20",
            },
            "3L": {
                "header_bg": "#C6282B",
                "subheader_bg": "#F9E1E2",
                "cell_bg": "#FDF2F3",
                "header_font": "#FFFFFF",
                "text_font": "#7A1D20",
            },
            "8D": {
                "header_bg": "#1B5E9A",
                "subheader_bg": "#DDEBFA",
                "cell_bg": "#F2F8FE",
                "header_font": "#FFFFFF",
                "text_font": "#0E3A66",
            },
            "F8": {
                "header_bg": "#1B5E9A",
                "subheader_bg": "#DDEBFA",
                "cell_bg": "#F2F8FE",
                "header_font": "#FFFFFF",
                "text_font": "#0E3A66",
            },
            "CZ": {
                "header_bg": "#2A9FD8",
                "subheader_bg": "#DDF3FD",
                "cell_bg": "#F2FBFF",
                "header_font": "#FFFFFF",
                "text_font": "#0C4E6A",
            },
            "EK": {
                "header_bg": "#C61E23",
                "subheader_bg": "#F8DFE1",
                "cell_bg": "#FEF2F3",
                "header_font": "#FFFFFF",
                "text_font": "#7A1A1E",
            },
            "MH": {
                "header_bg": "#00408C",
                "subheader_bg": "#DEE9FA",
                "cell_bg": "#F2F7FE",
                "header_font": "#FFFFFF",
                "text_font": "#112F66",
            },
            "QR": {
                "header_bg": "#5A0F3A",
                "subheader_bg": "#F2E2EA",
                "cell_bg": "#FAF1F6",
                "header_font": "#FFFFFF",
                "text_font": "#4A1633",
            },
            "SQ": {
                "header_bg": "#B78A00",
                "subheader_bg": "#FEEEC0",
                "cell_bg": "#FFF9E4",
                "header_font": "#1F1F1F",
                "text_font": "#6D5200",
            },
            "TG": {
                "header_bg": "#5E2D82",
                "subheader_bg": "#EDE0F9",
                "cell_bg": "#F7F1FD",
                "header_font": "#FFFFFF",
                "text_font": "#3E2559",
            },
            "UL": {
                "header_bg": "#8B2042",
                "subheader_bg": "#F4E0E8",
                "cell_bg": "#FDF3F7",
                "header_font": "#FFFFFF",
                "text_font": "#5C2135",
            },
            "WY": {
                "header_bg": "#7E2D35",
                "subheader_bg": "#F2E4E6",
                "cell_bg": "#FBF4F5",
                "header_font": "#FFFFFF",
                "text_font": "#4C2126",
            },
            "AK": {
                "header_bg": "#C4202F",
                "subheader_bg": "#F8DEE2",
                "cell_bg": "#FDF3F5",
                "header_font": "#FFFFFF",
                "text_font": "#7D1E2A",
            },
            "6E": {
                "header_bg": "#2B2F83",
                "subheader_bg": "#E2E4F6",
                "cell_bg": "#F3F4FD",
                "header_font": "#FFFFFF",
                "text_font": "#272A67",
            },
            "FZ": {
                "header_bg": "#0E5F9D",
                "subheader_bg": "#DCEAF7",
                "cell_bg": "#F3F8FD",
                "header_font": "#FFFFFF",
                "text_font": "#11456B",
            },
            "SV": {
                "header_bg": "#0B5A7A",
                "subheader_bg": "#D7EAF1",
                "cell_bg": "#EFF8FC",
                "header_font": "#FFFFFF",
                "text_font": "#11435A",
            },
            "OD": {
                "header_bg": "#B52025",
                "subheader_bg": "#F6DEE0",
                "cell_bg": "#FDF2F3",
                "header_font": "#FFFFFF",
                "text_font": "#7A2025",
            },
            "Q2": {
                "header_bg": "#2D4F8B",
                "subheader_bg": "#DEE8F7",
                "cell_bg": "#F3F7FD",
                "header_font": "#FFFFFF",
                "text_font": "#1F365C",
            },
        }
        themes["DEFAULT"] = {
            "header_bg": "#EAEAEA",
            "subheader_bg": "#F5F5F5",
            "cell_bg": "#FFFFFF",
            "header_font": "#333333",
            "text_font": "#333333",
        }

        palette = [
            ("#1565C0", "#DCEBFA", "#F3F8FE", "#FFFFFF", "#103D6B"),
            ("#6A1B9A", "#ECDDFA", "#F8F2FD", "#FFFFFF", "#4C2171"),
            ("#AD1457", "#FADBE8", "#FDF2F7", "#FFFFFF", "#7A1F4B"),
            ("#EF6C00", "#FDE8D3", "#FFF6EE", "#FFFFFF", "#8A4D19"),
            ("#2E7D32", "#E3F1E4", "#F5FBF6", "#FFFFFF", "#1F4E28"),
            ("#455A64", "#E4E9EC", "#F5F7F8", "#FFFFFF", "#2B3B42"),
        ]
        for code in sorted({str(a or "").strip().upper() for a in (airlines or []) if str(a or "").strip()}):
            if code in themes:
                continue
            idx = int(hashlib.md5(code.encode("utf-8")).hexdigest(), 16) % len(palette)
            hbg, sbg, cbg, hfont, tfont = palette[idx]
            themes[code] = {
                "header_bg": hbg,
                "subheader_bg": sbg,
                "cell_bg": cbg,
                "header_font": hfont,
                "text_font": tfont,
            }
        return themes

    @staticmethod
    def _has_inventory_signal(frame: pd.DataFrame) -> bool:
        if frame is None or frame.empty:
            return False
        inv_cols = [
            "min_seats",
            "max_seats",
            "load_pct",
            "previous_min_seats",
            "previous_max_seats",
            "previous_load_pct",
        ]
        for col in inv_cols:
            if col not in frame.columns:
                continue
            vals = pd.to_numeric(frame[col], errors="coerce")
            if vals.notna().any():
                return True
        return False

    @staticmethod
    def _collect_route_signals(route_df: pd.DataFrame):
        if route_df is None or route_df.empty:
            return ["UNKNOWN"]

        signals = set()
        for _, rec in route_df.iterrows():
            status = str(rec.get("status") or "").strip().upper()
            if status == "NEW":
                signals.add("NEW")
            elif status == "SOLD OUT":
                signals.add("SOLD OUT")

            for dcol in ("min_fare_delta", "max_fare_delta", "seat_delta", "tax_delta", "load_delta"):
                val = rec.get(dcol)
                if OutputWriter._is_na(val):
                    continue
                try:
                    fv = float(val)
                except Exception:
                    continue
                if fv > 0:
                    signals.add("INCREASE")
                elif fv < 0:
                    signals.add("DECREASE")

        if not signals:
            signals.add("UNKNOWN")
        return sorted(signals)

    @staticmethod
    def _peak_concurrent_flights_lower_bound(sub: pd.DataFrame):
        """
        Lower bound on aircraft needed from observed search results.
        Uses overlap of departure/arrival intervals. This is not total fleet size.
        """
        if sub is None or sub.empty or "departure" not in sub.columns or "arrival" not in sub.columns:
            return None

        dep = pd.to_datetime(sub["departure"], errors="coerce")
        arr = pd.to_datetime(sub["arrival"], errors="coerce")
        intervals = pd.DataFrame({"dep": dep, "arr": arr}).dropna()
        intervals = intervals[intervals["arr"] > intervals["dep"]]
        if intervals.empty:
            return None

        events = []
        for r in intervals.itertuples(index=False):
            events.append((r.dep, 1))
            events.append((r.arr, -1))

        # Conservative lower bound: arrivals processed before departures on ties.
        events.sort(key=lambda x: (x[0], 0 if x[1] == -1 else 1))
        running = 0
        peak = 0
        for _, delta in events:
            running += delta
            if running > peak:
                peak = running
        return int(peak)

    def _write_methodology_note(self, sheet, start_row: int, start_col: int, workbook):
        cfg = self._style_cfg()
        fmt_box_title = workbook.add_format(
            {"bold": True, "font_size": cfg["header"], "font_name": "Segoe UI", "bg_color": "#FFF2CC", "border": 1, "align": "left"}
        )
        fmt_box = workbook.add_format(
            {"font_size": cfg["body"], "font_name": "Segoe UI", "bg_color": "#FFFBE6", "border": 1, "text_wrap": True, "align": "left", "valign": "top"}
        )
        sheet.write(start_row, start_col, "Methodology", fmt_box_title)
        note = (
            "Arrows compare with previous snapshot for same route+flight+date.\n"
            "NEW = first seen in latest snapshot.\n"
            "SOLD OUT = explicit zero seats.\n"
            "N/O = flight not operating on that date.\n"
            "-- means source did not provide value.\n"
            "Open/Cap and Inv Press are shown only when inventory data exists.\n"
            "Open/Cap = total opened seats (visible fare buckets) / carrier capacity.\n"
            "Inv Press = 100 x (1 - min(1, Open/Cap)); proxy only, not exact load.\n"
            "Compare only runs with same passenger mix (ADT/CHD/INF)."
        )
        # Avoid tall merged regions on the main monitor sheet because row hide/show
        # operations used by interactive XLSM filtering fail on merged-row intersections.
        note_inline = note.replace("\n", " | ")
        sheet.write(start_row + 1, start_col, note_inline, fmt_box)
        sheet.set_column(start_col, start_col + 4, 24)

    def _write_changes_summary(self, workbook, df: pd.DataFrame):
        sheet = workbook.add_worksheet("What Changed Since Last Run")
        cfg = self._style_cfg()
        sheet.set_zoom(cfg["zoom"])
        sheet.set_default_row(cfg["default_row"])

        fmt_title = workbook.add_format({"bold": True, "font_size": cfg["title"], "font_name": "Segoe UI", "font_color": "#1F4E78"})
        fmt_note = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "font_color": "#555555"})
        fmt_section = workbook.add_format({"bold": True, "font_size": cfg["section"], "font_name": "Segoe UI", "font_color": "#1F4E78"})
        fmt_header = workbook.add_format({"bold": True, "font_size": cfg["header"], "font_name": "Segoe UI", "bg_color": "#D9E1F2", "border": 1, "align": "center"})
        fmt_cell = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "center"})
        fmt_cell_left = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "left"})
        fmt_date = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "center", "num_format": "yyyy-mm-dd"})
        fmt_num = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "right", "num_format": "#,##0"})
        fmt_num_signed = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "right", "num_format": "+#,##0;-#,##0;0"})
        fmt_pct = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "right", "num_format": "0.0"})
        fmt_pct_signed = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "right", "num_format": "+0.0;-0.0;0.0"})

        required = [
            "airline",
            "route",
            "flight_number",
            "flight_date",
            "status",
            "min_fare",
            "max_fare",
            "previous_min_fare",
            "previous_max_fare",
            "min_fare_delta",
            "max_fare_delta",
            "min_seats",
            "previous_min_seats",
            "seat_delta",
            "current_tax",
            "previous_tax",
            "tax_delta",
            "load_pct",
            "previous_load_pct",
            "load_delta",
        ]
        if any(c not in df.columns for c in required):
            sheet.write(0, 0, "What Changed Since Last Run", fmt_title)
            sheet.write(2, 0, "Insufficient columns to build changes summary.", fmt_note)
            return

        work = df.copy()
        work["airline"] = work["airline"].astype(str).str.upper()
        work["route"] = work["route"].astype(str)
        work["flight_number"] = work["flight_number"].astype(str)
        work["status"] = work["status"].astype(str).str.upper()
        work["flight_date"] = pd.to_datetime(work["flight_date"], errors="coerce")
        if "search_trip_type" not in work.columns:
            work["search_trip_type"] = "OW"
        work["search_trip_type"] = work["search_trip_type"].fillna("OW").astype(str).str.upper()
        if "requested_return_date" not in work.columns:
            work["requested_return_date"] = pd.NaT
        work["requested_return_date"] = pd.to_datetime(work["requested_return_date"], errors="coerce")
        stay_days = (work["requested_return_date"] - work["flight_date"]).dt.days
        work["stay_label"] = stay_days.where(stay_days.notna() & (stay_days >= 0)).apply(
            lambda v: f"{int(v)} Days" if pd.notna(v) else "--"
        )
        if "current_capture_label" not in work.columns:
            work["current_capture_label"] = "Latest snapshot"
        if "previous_capture_label" not in work.columns:
            work["previous_capture_label"] = "Previous snapshot"

        numeric_cols = [
            "min_fare",
            "max_fare",
            "previous_min_fare",
            "previous_max_fare",
            "min_fare_delta",
            "max_fare_delta",
            "min_seats",
            "previous_min_seats",
            "seat_delta",
            "current_tax",
            "previous_tax",
            "tax_delta",
            "load_pct",
            "previous_load_pct",
            "load_delta",
        ]
        for col in numeric_cols:
            work[col] = pd.to_numeric(work[col], errors="coerce")

        event_mask = (
            work["status"].isin(["NEW", "SOLD OUT"])
            | work["min_fare_delta"].fillna(0).ne(0)
            | work["max_fare_delta"].fillna(0).ne(0)
            | work["seat_delta"].fillna(0).ne(0)
            | work["tax_delta"].fillna(0).ne(0)
            | work["load_delta"].fillna(0).ne(0)
        )
        work = work.loc[event_mask].copy()
        if work.empty:
            sheet.write(0, 0, "What Changed Since Last Run", fmt_title)
            sheet.write(1, 0, "Simple route and airline view of exact changes from the previous snapshot", fmt_note)
            sheet.write(3, 0, "No change rows were detected for this run.", fmt_note)
            return

        work["flight_label"] = work.apply(lambda r: self._flight_code_label(r.get("airline"), r.get("flight_number")), axis=1)
        work = work.sort_values(
            ["route", "airline", "flight_date", "flight_number"],
            ascending=[True, True, True, True],
            kind="stable",
        )

        sheet.write(0, 0, "What Changed Since Last Run", fmt_title)
        sheet.write(1, 0, "Simple route and airline view of exact changes from the previous snapshot", fmt_note)
        self._write_methodology_note(sheet, 0, 20, workbook)

        row = 3
        sheet.write(row, 0, "A) Flat Change View", fmt_section)
        row += 1
        cols_flat = [
            "route",
            "airline",
            "search_trip_type",
            "flight_label",
            "flight_date",
            "requested_return_date",
            "stay_label",
            "status",
            "previous_min_fare",
            "min_fare",
            "min_fare_delta",
            "previous_max_fare",
            "max_fare",
            "max_fare_delta",
            "previous_min_seats",
            "min_seats",
            "seat_delta",
            "previous_tax",
            "current_tax",
            "tax_delta",
            "previous_load_pct",
            "load_pct",
            "load_delta",
            "previous_capture_label",
            "current_capture_label",
        ]
        labels_flat = [
            "Route",
            "Airline",
            "Trip Type",
            "Flight",
            "Date",
            "Return Date",
            "Stay",
            "Status",
            "Prev Min Fare",
            "Curr Min Fare",
            "Min Fare Change",
            "Prev Max Fare",
            "Curr Max Fare",
            "Max Fare Change",
            "Prev Seats",
            "Curr Seats",
            "Seat Change",
            "Prev Tax",
            "Curr Tax",
            "Tax Change",
            "Prev Load %",
            "Curr Load %",
            "Load Change",
            "Previous Snapshot",
            "Current Snapshot",
        ]
        for c_idx, label in enumerate(labels_flat):
            sheet.write(row, c_idx, label, fmt_header)
        row += 1
        for _, rec in work.iterrows():
            for c_idx, col in enumerate(cols_flat):
                val = rec.get(col)
                if col in {"route", "flight_label", "previous_capture_label", "current_capture_label"}:
                    sheet.write(row, c_idx, val if pd.notna(val) else "--", fmt_cell_left)
                elif col in {"airline", "status", "search_trip_type", "stay_label"}:
                    sheet.write(row, c_idx, val if pd.notna(val) else "--", fmt_cell)
                elif col in {"flight_date", "requested_return_date"}:
                    if pd.notna(val):
                        sheet.write_datetime(row, c_idx, pd.Timestamp(val).to_pydatetime(), fmt_date)
                    else:
                        sheet.write(row, c_idx, "--", fmt_cell)
                elif col in {"previous_load_pct", "load_pct"}:
                    sheet.write(row, c_idx, float(val) if pd.notna(val) else "--", fmt_pct if pd.notna(val) else fmt_cell)
                elif col == "load_delta":
                    sheet.write(row, c_idx, float(val) if pd.notna(val) else "--", fmt_pct_signed if pd.notna(val) else fmt_cell)
                elif col in {"min_fare_delta", "max_fare_delta", "seat_delta", "tax_delta"}:
                    sheet.write(row, c_idx, float(val) if pd.notna(val) else "--", fmt_num_signed if pd.notna(val) else fmt_cell)
                elif col in {
                    "previous_min_fare",
                    "min_fare",
                    "previous_max_fare",
                    "max_fare",
                    "previous_min_seats",
                    "min_seats",
                    "previous_tax",
                    "current_tax",
                }:
                    sheet.write(row, c_idx, float(val) if pd.notna(val) else "--", fmt_num if pd.notna(val) else fmt_cell)
                else:
                    sheet.write(row, c_idx, val if pd.notna(val) else "--", fmt_cell)
            row += 1

        last_data_row = max(row - 1, 4)
        sheet.autofilter(4, 0, last_data_row, len(cols_flat) - 1)
        sheet.set_column(0, 0, 14)
        sheet.set_column(1, 1, 9)
        sheet.set_column(2, 2, 10)
        sheet.set_column(3, 3, 12)
        sheet.set_column(4, 5, 12)
        sheet.set_column(6, 6, 8)
        sheet.set_column(7, 22, 13)
        sheet.set_column(23, 24, 20)
        if hasattr(sheet, "autofit"):
            try:
                sheet.autofit()
            except Exception:
                pass
        sheet.freeze_panes(5, 0)

    def _write_fare_trend_sparklines(self, workbook, df: pd.DataFrame):
        sheet = workbook.add_worksheet("Fare Trend Sparklines")
        cfg = self._style_cfg()
        sheet.set_zoom(cfg["zoom"])
        sheet.set_default_row(cfg["default_row"])

        fmt_title = workbook.add_format({"bold": True, "font_size": cfg["title"], "font_name": "Segoe UI", "font_color": "#1F4E78"})
        fmt_note = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "font_color": "#555555"})
        fmt_header = workbook.add_format({"bold": True, "font_size": cfg["header"], "font_name": "Segoe UI", "bg_color": "#D9E1F2", "border": 1, "align": "center"})
        fmt_cell = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "center"})
        fmt_left = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "left"})
        fmt_currency = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "right", "num_format": "#,##0"})

        required = ["route", "airline", "flight_key", "flight_number", "flight_date", "min_fare"]
        if any(c not in df.columns for c in required):
            sheet.write(0, 0, "Fare Trend Sparklines", fmt_title)
            sheet.write(2, 0, "Insufficient columns to build sparkline sheet.", fmt_note)
            return

        work = df.copy()
        work["airline"] = work["airline"].astype(str).str.upper()
        work["route"] = work["route"].astype(str)
        work["flight_number"] = work["flight_number"].astype(str)
        work["flight_date"] = pd.to_datetime(work["flight_date"], errors="coerce")
        work = work[work["flight_date"].notna()].copy()
        if work.empty:
            sheet.write(0, 0, "Fare Trend Sparklines", fmt_title)
            sheet.write(2, 0, "No valid dated rows available for sparklines.", fmt_note)
            return

        work["flight_label"] = work.apply(lambda r: self._flight_code_label(r.get("airline"), r.get("flight_number")), axis=1)
        piv = (
            work.pivot_table(
                index=["route", "airline", "flight_key", "flight_label"],
                columns="flight_date",
                values="min_fare",
                aggfunc="min",
            )
            .sort_index(axis=1)
            .reset_index()
        )

        date_cols = [c for c in piv.columns if isinstance(c, pd.Timestamp)]
        if not date_cols:
            sheet.write(0, 0, "Fare Trend Sparklines", fmt_title)
            sheet.write(2, 0, "No date columns available for sparklines.", fmt_note)
            return

        sheet.write(0, 0, "Fare Trend Sparklines", fmt_title)
        sheet.write(1, 0, "Min-fare trend by route and flight across searched dates", fmt_note)
        max_flights_any_route = int(df.groupby("route")["flight_key"].nunique().max()) if not df.empty else 1
        max_flight_cols = max(1, max_flights_any_route * 5)
        note_start_col = max(9, 2 + max_flight_cols + 2)
        self._write_methodology_note(sheet, 0, note_start_col, workbook)

        row = 3
        headers = ["Route", "Airline", "Flight", "Trend", "Latest Min Fare", "Lowest", "Highest", "Points"]
        for c_idx, label in enumerate(headers):
            sheet.write(row, c_idx, label, fmt_header)
        row += 1

        data_start_col = 8
        for d_idx, dt in enumerate(date_cols):
            sheet.write(row - 1, data_start_col + d_idx, dt.strftime("%Y-%m-%d"), fmt_header)

        for _, rec in piv.iterrows():
            vals = [self._to_int(rec[c]) for c in date_cols]
            numeric_vals = [v for v in vals if v is not None]
            latest = numeric_vals[-1] if numeric_vals else None
            low = min(numeric_vals) if numeric_vals else None
            high = max(numeric_vals) if numeric_vals else None
            points = len(numeric_vals)

            sheet.write(row, 0, rec["route"], fmt_left)
            sheet.write(row, 1, rec["airline"], fmt_cell)
            sheet.write(row, 2, rec["flight_label"], fmt_left)
            sheet.write(row, 4, latest if latest is not None else "--", fmt_currency if latest is not None else fmt_cell)
            sheet.write(row, 5, low if low is not None else "--", fmt_currency if low is not None else fmt_cell)
            sheet.write(row, 6, high if high is not None else "--", fmt_currency if high is not None else fmt_cell)
            sheet.write(row, 7, points, fmt_cell)

            for d_idx, v in enumerate(vals):
                sheet.write(row, data_start_col + d_idx, v if v is not None else "", fmt_cell)

            if points >= 2:
                spark_rng = xl_range(row, data_start_col, row, data_start_col + len(date_cols) - 1)
                sheet.add_sparkline(
                    row,
                    3,
                    {
                        "range": f"'{sheet.name}'!{spark_rng}",
                        "type": "line",
                        "markers": True,
                        "high_point": True,
                        "low_point": True,
                    },
                )
            row += 1

        sheet.set_column(0, 0, 13)
        sheet.set_column(1, 1, 9)
        sheet.set_column(2, 2, 16)
        sheet.set_column(3, 3, 20)
        sheet.set_column(4, 6, 14)
        sheet.set_column(7, 7, 8)
        sheet.set_column(data_start_col, data_start_col + len(date_cols) - 1, 10, None, {"hidden": True})
        sheet.freeze_panes(3, 0)

    def _write_airline_ops_compare(
        self,
        workbook,
        df: pd.DataFrame,
        full_capture_history: pd.DataFrame | None = None,
    ):
        sheet = workbook.add_worksheet("Airline Ops Compare")
        day_order = ["Saturday", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
        cfg = self._style_cfg()
        sheet.set_zoom(cfg["zoom"])
        sheet.set_default_row(cfg["default_row"])

        fmt_title = workbook.add_format({"bold": True, "font_name": "Segoe UI", "font_size": cfg["title"], "font_color": "#1F4E78"})
        fmt_note = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "font_color": "#555555"})
        fmt_section = workbook.add_format({"bold": True, "font_name": "Segoe UI", "font_size": cfg["section"], "font_color": "#1F4E78"})
        fmt_header = workbook.add_format({"bold": True, "font_name": "Segoe UI", "font_size": cfg["header"], "bg_color": "#D9E1F2", "border": 1, "align": "center", "text_wrap": True})
        fmt_metric = workbook.add_format({"bold": True, "font_name": "Segoe UI", "font_size": cfg["header"], "border": 1, "bg_color": "#F2F2F2"})
        fmt_cell = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "align": "left"})
        fmt_center = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "align": "center"})
        fmt_gray = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "align": "center", "font_color": "#777777"})
        fmt_legend_key = workbook.add_format({"bold": True, "font_name": "Segoe UI", "font_size": cfg["header"], "border": 1, "bg_color": "#F2F2F2", "align": "center"})

        required = ["airline", "route", "flight_key", "flight_number", "departure_time", "aircraft", "flight_date"]
        if any(c not in df.columns for c in required):
            sheet.write(0, 0, "Airline Ops Compare", fmt_title)
            sheet.write(2, 0, "Insufficient columns to build comparison sheet.", fmt_cell)
            return

        working, ops_source_label = self._build_ops_baseline_frame(df, full_capture_history)
        if working.empty:
            sheet.write(0, 0, "Airline Ops Compare", fmt_title)
            sheet.write(2, 0, "No historical/current flight observations available to build operations sheet.", fmt_cell)
            return

        airlines = sorted([a for a in working["airline"].dropna().unique() if str(a).strip()])
        airline_theme = self._airline_theme_map(airlines)
        capacity_map = get_fleet_capacity_map(airlines=airlines) if airlines else {}
        inventory_map = get_fleet_inventory(airlines=airlines) if airlines else {}

        fmt_airline_header = {}
        fmt_airline_subheader = {}
        fmt_airline_cell = {}
        fmt_airline_center = {}
        for airline in airlines or ["DEFAULT"]:
            t = airline_theme.get(airline, airline_theme["DEFAULT"])
            fmt_airline_header[airline] = workbook.add_format(
                {"bold": True, "font_name": "Segoe UI", "font_size": cfg["header"], "bg_color": t["header_bg"], "font_color": t["header_font"], "border": 1, "align": "center"}
            )
            fmt_airline_subheader[airline] = workbook.add_format(
                {"bold": True, "font_name": "Segoe UI", "font_size": cfg["header"], "bg_color": t["subheader_bg"], "font_color": t["text_font"], "border": 1, "align": "center"}
            )
            fmt_airline_cell[airline] = workbook.add_format(
                {"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "align": "left", "bg_color": t["cell_bg"], "font_color": t["text_font"]}
            )
            fmt_airline_center[airline] = workbook.add_format(
                {"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "align": "center", "bg_color": t["cell_bg"], "font_color": t["text_font"]}
            )

        sheet.write(0, 0, "Airline Ops Compare", fmt_title)
        sheet.write(1, 0, "Side-by-side airline comparison: fleet, route coverage, frequency, and timings", fmt_note)
        sheet.write(2, 0, "Legend", fmt_legend_key)
        legend_col = 1
        for airline in airlines:
            sheet.write(2, legend_col, airline, fmt_airline_header.get(airline, fmt_header))
            legend_col += 1
        sheet.write(2, legend_col, "Heatmap columns: Daily / Weekly", fmt_cell)
        legend_note = (
            "LB = peak concurrent flights from observed search results (lower bound, not exact fleet). "
            "Fleet Coverage = LB / Website Fleet Count. Daily/Weekly counts use typical integer flights from observed dates, not fractional averages. "
            f"Ops baseline source: {ops_source_label}."
        )
        sheet.merge_range(3, 0, 3, max(6, legend_col + 2), legend_note, fmt_note)
        self._write_methodology_note(sheet, 0, max(10, legend_col + 4), workbook)

        # -------------------------
        # Section A: Fleet snapshot side-by-side
        # -------------------------
        row = 4
        sheet.write(row, 0, "A) Fleet Snapshot (Side-by-Side)", fmt_section)
        row += 1
        sheet.write(row, 0, "Metric", fmt_header)
        for idx, airline in enumerate(airlines):
            sheet.write(row, 1 + idx, airline, fmt_airline_header.get(airline, fmt_header))
        row += 1

        metric_rows = [
            "Known Fleet Types",
            "Website Fleet Count",
            "Search Fleet (LB)",
            "Fleet Coverage (LB/Website)",
            "Known Seat Capacities",
            "Observed Routes",
            "Date Span",
            "Typical Daily Flights",
            "Typical Weekly Flights",
            "Future Pattern",
            "First/Last Departure",
        ]
        fleet_start_row = row
        fleet_metric_row_index = {}
        for metric in metric_rows:
            fleet_metric_row_index[metric] = row
            sheet.write(row, 0, metric, fmt_metric)
            for idx, airline in enumerate(airlines):
                sub = working[working["airline"] == airline]
                dep_times = sorted(t for t in sub["departure_time"].dropna().unique() if t and t != "nan")
                first_dep = dep_times[0] if dep_times else "--"
                last_dep = dep_times[-1] if dep_times else "--"
                route_n = int(sub["route"].nunique())
                daily_counts = self._daily_flight_counts(sub)
                typical_daily = self._dominant_integer(daily_counts.tolist())
                typical_weekly = self._typical_weekday_count_sum(sub, day_order)
                date_span = self._date_span_summary(sub)
                future_pattern = self._future_pattern_signal(sub, day_order)

                known_rows = inventory_map.get(airline, [])
                known_types = self._join_limited([r.get("aircraft_type") for r in known_rows], limit=8) if known_rows else "--"
                known_count = sum(int(r.get("aircraft_count") or 0) for r in known_rows) if known_rows else None
                search_fleet_lb = self._peak_concurrent_flights_lower_bound(sub)
                fleet_coverage_pct = None
                if known_count and known_count > 0 and search_fleet_lb is not None:
                    fleet_coverage_pct = round((float(search_fleet_lb) / float(known_count)) * 100.0, 2)
                known_caps = self._join_limited(
                    sorted({str(r.get("seats_per_aircraft")) for r in known_rows if r.get("seats_per_aircraft")}),
                    limit=8,
                ) if known_rows else "--"

                value = "--"
                if metric == "Known Fleet Types":
                    value = known_types
                elif metric == "Website Fleet Count":
                    value = known_count if known_count is not None else "--"
                elif metric == "Search Fleet (LB)":
                    value = search_fleet_lb if search_fleet_lb is not None else "?"
                elif metric == "Fleet Coverage (LB/Website)":
                    value = f"{fleet_coverage_pct:.2f}%" if fleet_coverage_pct is not None else "?"
                elif metric == "Known Seat Capacities":
                    value = known_caps
                elif metric == "Observed Routes":
                    value = route_n
                elif metric == "Date Span":
                    value = date_span
                elif metric == "Typical Daily Flights":
                    value = typical_daily
                elif metric == "Typical Weekly Flights":
                    value = typical_weekly
                elif metric == "Future Pattern":
                    value = future_pattern
                elif metric == "First/Last Departure":
                    value = f"{first_dep} - {last_dep}"

                if metric in {"Website Fleet Count", "Search Fleet (LB)", "Fleet Coverage (LB/Website)", "Observed Routes", "Typical Daily Flights", "Typical Weekly Flights"}:
                    fmt = fmt_airline_center.get(airline, fmt_center)
                else:
                    fmt = fmt_airline_cell.get(airline, fmt_cell)
                sheet.write(row, 1 + idx, value, fmt)
            row += 1

        fleet_end_row = row - 1
        for idx in range(len(airlines)):
            col = 1 + idx
            # Highlight comparative magnitude on frequency rows
            daily_row = fleet_metric_row_index.get("Typical Daily Flights")
            weekly_row = fleet_metric_row_index.get("Typical Weekly Flights")
            if daily_row is not None and weekly_row is not None:
                sheet.conditional_format(daily_row, col, weekly_row, col, {"type": "3_color_scale"})

        # -------------------------
        # Section B: Route comparison side-by-side
        # -------------------------
        row += 2
        sheet.write(row, 0, "B) Route Operations Baseline (Historical + Future-Dated)", fmt_section)
        row += 1
        sheet.merge_range(
            row,
            0,
            row,
            max(6, len(airlines) * 5),
            "Daily/Weekly show the usual pattern for each airline-route from historical observations. Pattern highlights whether future departure dates stay stable or vary.",
            fmt_note,
        )
        row += 1

        route_daily_counts = (
            working.dropna(subset=["flight_date"])
            .groupby(["airline", "route", "flight_date", "day_name"], as_index=False)
            .agg(daily_flights=("flight_key", "nunique"))
        )
        route_typical_weekday = (
            route_daily_counts.groupby(["airline", "route", "day_name"], as_index=False)
            .agg(typical_flights=("daily_flights", lambda s: self._dominant_integer(list(s))))
        )
        route_weekly = (
            route_typical_weekday.groupby(["airline", "route"], as_index=False)
            .agg(typical_weekly_flights=("typical_flights", "sum"))
        )
        route_timings = (
            working.groupby(["airline", "route"], as_index=False)
            .agg(
                timings=("departure_time", lambda s: sorted({str(v) for v in s if str(v).strip()})),
                aircraft_types=("aircraft", lambda s: sorted({str(v) for v in s if str(v).strip()})),
            )
        )
        route_span_pattern_rows = []
        for (airline_code, route_code), grp in working.groupby(["airline", "route"], sort=False):
            route_span_pattern_rows.append(
                {
                    "airline": airline_code,
                    "route": route_code,
                    "date_span": self._date_span_summary(grp),
                    "future_pattern": self._future_pattern_signal(grp, day_order),
                }
            )
        route_span_pattern = pd.DataFrame(route_span_pattern_rows)
        grp = (
            route_daily_counts.groupby(["airline", "route"], as_index=False)
            .agg(
                operating_days=("flight_date", lambda s: int(s.dropna().nunique())),
                typical_daily_flights=("daily_flights", lambda s: self._dominant_integer(list(s))),
            )
            .merge(route_weekly, on=["airline", "route"], how="left")
            .merge(route_span_pattern, on=["airline", "route"], how="left")
            .merge(route_timings, on=["airline", "route"], how="left")
        )
        route_rows = sorted(grp["route"].dropna().unique().tolist())

        # two header rows: airline group + metric
        sheet.write(row, 0, "Route", fmt_header)
        col = 1
        for airline in airlines:
            sheet.merge_range(row, col, row, col + 4, airline, fmt_airline_header.get(airline, fmt_header))
            col += 5
        row += 1
        sheet.write(row, 0, "Route", fmt_header)
        col = 1
        metric_labels = ["Daily", "Weekly", "Date Span", "Pattern Detail", "Timings"]
        for airline in airlines:
            for m in metric_labels:
                sheet.write(row, col, m, fmt_airline_subheader.get(airline, fmt_header))
                col += 1
        row += 1

        routes_start_row = row
        for route in route_rows:
            sheet.write(row, 0, route, fmt_cell)
            col = 1
            for airline in airlines:
                m = grp[(grp["airline"] == airline) & (grp["route"] == route)]
                if m.empty:
                    daily_avg = "--"
                    weekly_est = "--"
                    date_span = "--"
                    future_pattern = "--"
                    timings = "--"
                else:
                    rec = m.iloc[0]
                    daily_avg = int(rec["typical_daily_flights"]) if pd.notna(rec.get("typical_daily_flights")) else 0
                    weekly_est = int(rec["typical_weekly_flights"]) if pd.notna(rec.get("typical_weekly_flights")) else 0
                    date_span = str(rec.get("date_span") or "--")
                    future_pattern = str(rec.get("future_pattern") or "--")
                    timings = self._join_limited(rec["timings"], limit=8)
                sheet.write(row, col, daily_avg, fmt_airline_center.get(airline, fmt_center))
                sheet.write(row, col + 1, weekly_est, fmt_airline_center.get(airline, fmt_center))
                sheet.write(row, col + 2, date_span, fmt_airline_cell.get(airline, fmt_cell))
                sheet.write(row, col + 3, future_pattern, fmt_airline_cell.get(airline, fmt_cell))
                sheet.write(row, col + 4, timings, fmt_airline_cell.get(airline, fmt_cell))
                col += 5
            row += 1
        routes_end_row = row - 1

        # color-scale per airline daily/weekly columns
        for idx in range(len(airlines)):
            base = 1 + idx * 5
            if routes_end_row >= routes_start_row:
                sheet.conditional_format(routes_start_row, base, routes_end_row, base, {"type": "3_color_scale"})
                sheet.conditional_format(routes_start_row, base + 1, routes_end_row, base + 1, {"type": "3_color_scale"})

        # -------------------------
        # Section C: Route x Day operations (typical flights + timings)
        # -------------------------
        row += 2
        sheet.write(row, 0, "C) Route-Day Total Operations Across Airlines (Historical Baseline + Timings)", fmt_section)
        row += 1
        sheet.merge_range(
            row,
            0,
            row,
            max(6, len(day_order) + 1),
            "Σ = route total across all listed airlines for that weekday. Each cell shows the airline breakdown and departure times from the historical ops baseline.",
            fmt_note,
        )
        row += 1

        day_timings = (
            working.groupby(["route", "day_name", "airline"], as_index=False)
            .agg(timings=("departure_time", lambda s: sorted({str(v) for v in s if str(v).strip()})))
        )
        day_grp = route_typical_weekday.merge(day_timings, on=["route", "day_name", "airline"], how="left")
        route_list = sorted(day_grp["route"].dropna().astype(str).unique().tolist())

        sheet.write(row, 0, "Route", fmt_header)
        for i, dname in enumerate(day_order):
            sheet.write(row, 1 + i, dname, fmt_header)
        row += 1

        day_start_row = row
        for route in route_list:
            sheet.write(row, 0, route, fmt_cell)
            for i, dname in enumerate(day_order):
                block = day_grp[(day_grp["route"] == route) & (day_grp["day_name"] == dname)]
                if block.empty:
                    value = "--"
                    fmt = fmt_gray
                else:
                    total = int(pd.to_numeric(block["typical_flights"], errors="coerce").fillna(0).sum())
                    entries = []
                    for _, rec in block.sort_values("airline").iterrows():
                        ac = str(rec.get("airline") or "").upper()
                        fl = int(rec.get("typical_flights") or 0)
                        tms = self._join_limited(rec.get("timings") or [], limit=None)
                        entries.append(f"{ac}:{fl}[{tms}]")
                    details = self._join_limited(entries, limit=None)
                    value = f"\u03A3{total} | {details}"
                    fmt = fmt_cell
                sheet.write(row, 1 + i, value, fmt)
            row += 1
        day_end_row = row - 1

        for i in range(len(day_order)):
            col_idx = 1 + i
            if day_end_row >= day_start_row:
                sheet.conditional_format(day_start_row, col_idx, day_end_row, col_idx, {"type": "text", "criteria": "not containing", "value": "--", "format": fmt_cell})

        # Column sizing and freeze
        sheet.set_column(0, 0, 16)  # Route
        for i in range(len(day_order)):
            sheet.set_column(1 + i, 1 + i, 42)  # route-day summary value
        sheet.freeze_panes(3, 1)

    def _write_penalty_comparison(self, workbook, df: pd.DataFrame):
        sheet = workbook.add_worksheet("Penalty Comparison")
        cfg = self._style_cfg()
        sheet.set_zoom(cfg["zoom"])
        sheet.set_default_row(cfg["default_row"])

        fmt_title = workbook.add_format({"bold": True, "font_size": cfg["title"], "font_name": "Segoe UI", "font_color": "#1F4E78"})
        fmt_note = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "font_color": "#555555"})
        fmt_header = workbook.add_format({"bold": True, "font_size": cfg["header"], "font_name": "Segoe UI", "bg_color": "#D9E1F2", "border": 1, "align": "center", "text_wrap": True})
        fmt_cell = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "center"})
        fmt_cell_left = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "left"})

        sheet.write(0, 0, "Penalty Change Timeline (Route + Airline)", fmt_title)
        sheet.write(1, 0, "Shows only dates where route+airline penalty state changed vs previous scrape.", fmt_note)

        if df is None or df.empty:
            sheet.write(3, 0, "No rows available.", fmt_note)
            return

        fee_fields = [
            "fare_change_fee_before_24h",
            "fare_change_fee_within_24h",
            "fare_change_fee_no_show",
            "fare_cancel_fee_before_24h",
            "fare_cancel_fee_within_24h",
            "fare_cancel_fee_no_show",
        ]
        required = ["route", "airline", "flight_date", "current_capture_label", "previous_capture_label"]
        required += [f"current_{f}" for f in fee_fields]
        required += [f"previous_{f}" for f in fee_fields]
        required += [
            "current_penalty_currency",
            "previous_penalty_currency",
            "current_penalty_rule_text",
            "previous_penalty_rule_text",
            "current_fare_refundable",
            "previous_fare_refundable",
            "current_fare_changeable",
            "previous_fare_changeable",
        ]
        work = df.copy()
        for col in required:
            if col not in work.columns:
                work[col] = pd.NA
        work["flight_date"] = pd.to_datetime(work["flight_date"], errors="coerce")

        def _first_text(series):
            for v in series:
                if v is None:
                    continue
                s = str(v).strip()
                if s:
                    return s
            return None

        def _agg_bool(series):
            vals = [v for v in series if pd.notna(v)]
            if not vals:
                return None
            bools = [bool(v) for v in vals]
            return True if any(bools) else False

        def _agg_fee(series):
            vals = pd.to_numeric(series, errors="coerce").dropna()
            if vals.empty:
                return None
            return float(vals.min())

        def _norm_rule(text):
            s = str(text or "").strip()
            if not s:
                return ""
            return " ".join(s.split()).lower()

        def _state(rec, prefix):
            out = {}
            for f in fee_fields:
                out[f] = rec.get(f"{prefix}_{f}")
            out["currency"] = rec.get(f"{prefix}_penalty_currency")
            out["rule"] = rec.get(f"{prefix}_penalty_rule_text")
            out["fare_refundable"] = rec.get(f"{prefix}_fare_refundable")
            out["fare_changeable"] = rec.get(f"{prefix}_fare_changeable")
            return out

        def _has_state(st):
            if not isinstance(st, dict):
                return False
            for f in fee_fields:
                if st.get(f) is not None and pd.notna(st.get(f)):
                    return True
            if st.get("fare_refundable") is not None:
                return True
            if st.get("fare_changeable") is not None:
                return True
            if str(st.get("rule") or "").strip():
                return True
            return False

        def _state_key(st):
            return (
                tuple(st.get(f) for f in fee_fields),
                st.get("fare_refundable"),
                st.get("fare_changeable"),
                str(st.get("currency") or "").strip().upper(),
                _norm_rule(st.get("rule")),
            )

        def _state_snapshot(st):
            def _fee_txt(v):
                if v is None or pd.isna(v):
                    return "--"
                return f"{float(v):,.0f}"

            chg = "/".join(
                [
                    _fee_txt(st.get("fare_change_fee_before_24h")),
                    _fee_txt(st.get("fare_change_fee_within_24h")),
                    _fee_txt(st.get("fare_change_fee_no_show")),
                ]
            )
            can = "/".join(
                [
                    _fee_txt(st.get("fare_cancel_fee_before_24h")),
                    _fee_txt(st.get("fare_cancel_fee_within_24h")),
                    _fee_txt(st.get("fare_cancel_fee_no_show")),
                ]
            )
            ccy = str(st.get("currency") or "").strip().upper()
            ccy_tok = ccy if ccy else "--"
            note = str(st.get("rule") or "").strip()
            if len(note) > 80:
                note = note[:77] + "..."
            return (
                f"{ccy_tok} | Chg:{chg} | Can:{can} | "
                f"Refundable:{self._bool_label(st.get('fare_refundable'))} | "
                f"Changeable:{self._bool_label(st.get('fare_changeable'))}"
                + (f" | Note:{note}" if note else "")
            )

        records = []
        group_cols = ["route", "airline", "flight_date"]
        for (route, airline, fdate), grp in work.groupby(group_cols, sort=True):
            rec = {
                "route": route,
                "airline": str(airline or "").upper(),
                "flight_date": fdate,
                "current_capture_label": _first_text(grp["current_capture_label"]) or "Current snapshot",
                "previous_capture_label": _first_text(grp["previous_capture_label"]) or "Previous snapshot",
                "current_penalty_currency": _first_text(grp["current_penalty_currency"]),
                "previous_penalty_currency": _first_text(grp["previous_penalty_currency"]),
                "current_penalty_rule_text": _first_text(grp["current_penalty_rule_text"]),
                "previous_penalty_rule_text": _first_text(grp["previous_penalty_rule_text"]),
                "current_fare_refundable": _agg_bool(grp["current_fare_refundable"]),
                "previous_fare_refundable": _agg_bool(grp["previous_fare_refundable"]),
                "current_fare_changeable": _agg_bool(grp["current_fare_changeable"]),
                "previous_fare_changeable": _agg_bool(grp["previous_fare_changeable"]),
            }
            for f in fee_fields:
                rec[f"current_{f}"] = _agg_fee(grp[f"current_{f}"])
                rec[f"previous_{f}"] = _agg_fee(grp[f"previous_{f}"])
            records.append(rec)

        summary = pd.DataFrame(records)
        if summary.empty:
            sheet.write(3, 0, "No penalty summary rows available.", fmt_note)
            return

        changes = []
        for _, rec in summary.iterrows():
            curr = _state(rec, "current")
            prev = _state(rec, "previous")
            curr_has = _has_state(curr)
            prev_has = _has_state(prev)
            if not curr_has and not prev_has:
                continue
            if curr_has and not prev_has:
                change_type = "NEW"
            elif prev_has and not curr_has:
                change_type = "REMOVED"
            elif _state_key(curr) != _state_key(prev):
                change_type = "UPDATED"
            else:
                continue
            changes.append(
                {
                    "route": rec["route"],
                    "airline": rec["airline"],
                    "flight_date": rec["flight_date"],
                    "previous_capture_label": rec["previous_capture_label"],
                    "current_capture_label": rec["current_capture_label"],
                    "change_type": change_type,
                    "previous_snapshot": _state_snapshot(prev),
                    "current_snapshot": _state_snapshot(curr),
                }
            )

        row = 3
        headers = [
            "Route",
            "Airline",
            "Date",
            "Previous Capture",
            "Current Capture",
            "Change Type",
            "Previous Penalty Snapshot",
            "Current Penalty Snapshot",
        ]
        for i, h in enumerate(headers):
            sheet.write(row, i, h, fmt_header)
        row += 1

        if not changes:
            sheet.write(row, 0, "No route+airline penalty changes detected for selected scrapes.", fmt_note)
            row += 1
        else:
            for rec in sorted(changes, key=lambda x: (str(x["route"]), str(x["airline"]), str(x["flight_date"]))):
                date_txt = pd.to_datetime(rec["flight_date"], errors="coerce")
                date_str = date_txt.strftime("%Y-%m-%d") if pd.notna(date_txt) else "--"
                sheet.write(row, 0, rec["route"], fmt_cell_left)
                sheet.write(row, 1, rec["airline"], fmt_cell)
                sheet.write(row, 2, date_str, fmt_cell)
                sheet.write(row, 3, rec["previous_capture_label"], fmt_cell)
                sheet.write(row, 4, rec["current_capture_label"], fmt_cell)
                sheet.write(row, 5, rec["change_type"], fmt_cell)
                sheet.write(row, 6, rec["previous_snapshot"], fmt_cell_left)
                sheet.write(row, 7, rec["current_snapshot"], fmt_cell_left)
                row += 1

        sheet.set_column(0, 0, 14)
        sheet.set_column(1, 1, 9)
        sheet.set_column(2, 2, 12)
        sheet.set_column(3, 4, 19)
        sheet.set_column(5, 5, 11)
        sheet.set_column(6, 7, 58)
        sheet.freeze_panes(4, 0)

    def _write_tax_comparison(self, workbook, df: pd.DataFrame):
        sheet = workbook.add_worksheet("Tax Comparison")
        cfg = self._style_cfg()
        sheet.set_zoom(cfg["zoom"])
        sheet.set_default_row(cfg["default_row"])

        fmt_title = workbook.add_format({"bold": True, "font_size": cfg["title"], "font_name": "Segoe UI", "font_color": "#1F4E78"})
        fmt_note = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "font_color": "#555555"})
        fmt_header = workbook.add_format({"bold": True, "font_size": cfg["header"], "font_name": "Segoe UI", "bg_color": "#D9E1F2", "border": 1, "align": "center", "text_wrap": True})
        fmt_cell = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "center"})
        fmt_cell_left = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "left"})

        sheet.write(0, 0, "Tax Snapshot by Country Flow (Current)", fmt_title)
        sheet.write(
            1,
            0,
            "Simple country-flow view (BD-first): BD->BD, BD->Outside, Outside->BD. "
            "Shows current tax only and a change flag vs previous scrape.",
            fmt_note,
        )

        if df is None or df.empty:
            sheet.write(3, 0, "No rows available.", fmt_note)
            return

        work = df.copy()
        for col in [
            "route",
            "airline",
            "flight_date",
            "current_tax",
            "previous_tax",
            "current_capture_label",
            "previous_capture_label",
        ]:
            if col not in work.columns:
                work[col] = pd.NA
        work["flight_date"] = pd.to_datetime(work["flight_date"], errors="coerce")
        work["current_tax"] = pd.to_numeric(work["current_tax"], errors="coerce")
        work["previous_tax"] = pd.to_numeric(work["previous_tax"], errors="coerce")

        airport_countries = load_airport_countries()

        def _first_text(series):
            for v in series:
                if v is None:
                    continue
                s = str(v).strip()
                if s:
                    return s
            return None

        def _tax_text(mn, mx):
            if mn is None and mx is None:
                return "--"
            if mn is None:
                return f"-- to {mx:,.0f}"
            if mx is None:
                return f"{mn:,.0f} to --"
            if float(mn) == float(mx):
                return f"{mn:,.0f}"
            return f"{mn:,.0f} to {mx:,.0f}"

        def _flow_from_route(route_value: str):
            r = str(route_value or "").strip().upper()
            if "-" not in r:
                return "Unknown", 99
            parts = r.split("-", 1)
            org = parts[0].strip().upper()
            dst = parts[1].strip().upper()
            oc = airport_countries.get(org, "")
            dc = airport_countries.get(dst, "")
            if oc == "BD" and dc == "BD":
                return "BD->BD", 1
            if oc == "BD" and dc and dc != "BD":
                return "BD->Outside", 2
            if dc == "BD" and oc and oc != "BD":
                return "Outside->BD", 3
            if oc and dc:
                return "Outside->Outside", 4
            return "Unknown", 99

        rows = []
        for (route, airline, fdate), grp in work.groupby(["route", "airline", "flight_date"], sort=True):
            curr = grp["current_tax"].dropna()
            prev = grp["previous_tax"].dropna()
            curr_count = int(curr.shape[0])
            prev_count = int(prev.shape[0])
            curr_min = float(curr.min()) if curr_count > 0 else None
            curr_max = float(curr.max()) if curr_count > 0 else None
            prev_min = float(prev.min()) if prev_count > 0 else None
            prev_max = float(prev.max()) if prev_count > 0 else None

            if curr_count <= 0:
                continue

            if prev_count <= 0:
                change_flag = "NEW"
            else:
                changed = bool(
                    curr_min != prev_min
                    or curr_max != prev_max
                    or curr_count != prev_count
                )
                change_flag = "CHANGED" if changed else "NO_CHANGE"

            flow_label, flow_order = _flow_from_route(route)
            rows.append(
                {
                    "flow_label": flow_label,
                    "flow_order": flow_order,
                    "route": str(route or ""),
                    "airline": str(airline or "").upper(),
                    "flight_date": fdate,
                    "current_capture_label": _first_text(grp["current_capture_label"]) or "Current snapshot",
                    "current_tax_text": _tax_text(curr_min, curr_max),
                    "curr_count": curr_count,
                    "change_flag": change_flag,
                }
            )

        row = 3
        headers = [
            "Flow",
            "Route",
            "Airline",
            "Date",
            "Current Capture",
            "Current Tax",
            "Flights w/Tax",
            "Change Flag",
        ]
        for i, h in enumerate(headers):
            sheet.write(row, i, h, fmt_header)
        row += 1

        if not rows:
            sheet.write(row, 0, "No current route+airline tax rows found for selected scrapes.", fmt_note)
            row += 1
        else:
            rows_sorted = sorted(
                rows,
                key=lambda x: (int(x["flow_order"]), str(x["route"]), str(x["airline"]), str(x["flight_date"])),
            )
            for rec in rows_sorted:
                date_txt = pd.to_datetime(rec["flight_date"], errors="coerce")
                date_str = date_txt.strftime("%Y-%m-%d") if pd.notna(date_txt) else "--"
                sheet.write(row, 0, rec["flow_label"], fmt_cell)
                sheet.write(row, 1, rec["route"], fmt_cell_left)
                sheet.write(row, 2, rec["airline"], fmt_cell)
                sheet.write(row, 3, date_str, fmt_cell)
                sheet.write(row, 4, rec["current_capture_label"], fmt_cell)
                sheet.write(row, 5, rec["current_tax_text"], fmt_cell_left)
                sheet.write(row, 6, int(rec["curr_count"] or 0), fmt_cell)
                sheet.write(row, 7, rec["change_flag"], fmt_cell)
                row += 1

        sheet.set_column(0, 0, 15)
        sheet.set_column(1, 1, 14)
        sheet.set_column(2, 2, 9)
        sheet.set_column(3, 3, 12)
        sheet.set_column(4, 4, 20)
        sheet.set_column(5, 5, 18)
        sheet.set_column(6, 6, 13)
        sheet.set_column(7, 7, 12)
        sheet.freeze_panes(4, 0)

    def _write_route_filter_view(self, workbook, df: pd.DataFrame):
        sheet = workbook.add_worksheet("Route Filter View")
        cfg = self._style_cfg()
        sheet.set_zoom(cfg["zoom"])
        sheet.set_default_row(cfg["default_row"])

        fmt_title = workbook.add_format({"bold": True, "font_size": cfg["title"], "font_name": "Segoe UI", "font_color": "#1F4E78"})
        fmt_note = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "font_color": "#555555"})
        fmt_header = workbook.add_format({"bold": True, "font_size": cfg["header"], "font_name": "Segoe UI", "bg_color": "#D9E1F2", "border": 1, "align": "center"})
        fmt_cell = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "center"})
        fmt_left = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "left"})
        fmt_chip_label = workbook.add_format({"bold": True, "font_size": cfg["header"], "font_name": "Segoe UI", "bg_color": "#F2F2F2", "border": 1, "align": "center"})
        fmt_sig_inc = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "center"})
        fmt_sig_dec = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "center"})
        fmt_sig_new = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "bold": True, "italic": True, "font_color": "#1F4BD8", "border": 1, "align": "center"})
        fmt_sig_sold = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "bold": True, "italic": True, "font_color": "#777777", "border": 1, "align": "center"})
        fmt_sig_unk = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "font_color": "#777777", "border": 1, "align": "center"})

        sheet.write(0, 0, "Route Monitor Filter View", fmt_title)
        sheet.write(1, 0, "Normalized table for filtering by airline, status, route, date, and deltas.", fmt_note)

        if df is None or df.empty:
            sheet.write(3, 0, "No rows available.", fmt_note)
            return

        cols = [
            "route",
            "flight_date",
            "day_name",
            "current_capture_label",
            "previous_capture_label",
            "airline",
            "flight_number",
            "departure_time",
            "status",
            "signal_primary",
            "min_fare",
            "max_fare",
            "current_tax",
            "min_seats",
            "max_seats",
            "load_pct",
            "min_fare_delta",
            "max_fare_delta",
            "tax_delta",
            "seat_delta",
            "load_delta",
        ]
        work = df.copy()
        for col in cols:
            if col not in work.columns:
                work[col] = pd.NA
        work = work[cols].copy()
        work["flight_date"] = pd.to_datetime(work["flight_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        work["airline"] = work["airline"].astype(str).str.upper()
        work["status"] = work["status"].astype(str).str.upper()

        def _signal_primary(rec):
            status = str(rec.get("status") or "").upper()
            if status == "NEW":
                return "NEW"
            if status == "SOLD OUT":
                return "SOLD OUT"
            deltas = []
            for c in ["min_fare_delta", "max_fare_delta", "tax_delta", "seat_delta", "load_delta"]:
                v = pd.to_numeric(rec.get(c), errors="coerce")
                if pd.notna(v):
                    deltas.append(float(v))
            has_pos = any(v > 0 for v in deltas)
            has_neg = any(v < 0 for v in deltas)
            if has_pos and has_neg:
                return "MIXED"
            if has_pos:
                return "INCREASE"
            if has_neg:
                return "DECREASE"
            measure_cols = ["min_fare", "max_fare", "current_tax", "min_seats", "max_seats", "load_pct"]
            all_missing = True
            for c in measure_cols:
                v = pd.to_numeric(rec.get(c), errors="coerce")
                if pd.notna(v):
                    all_missing = False
                    break
            return "UNKNOWN" if all_missing else "STABLE"

        work["signal_primary"] = work.apply(_signal_primary, axis=1)
        work = work.sort_values(["route", "flight_date", "airline", "departure_time"], na_position="last")

        # Visual chips section (airlines + signals) for quick scan before filtering.
        chip_airlines = sorted([a for a in work["airline"].dropna().unique() if str(a).strip()])
        airline_theme = self._airline_theme_map(chip_airlines)
        sheet.write(3, 0, "Airlines", fmt_chip_label)
        c = 1
        for code in chip_airlines:
            t = airline_theme.get(code, airline_theme["DEFAULT"])
            fmt_chip = workbook.add_format(
                {
                    "bold": True,
                    "font_size": cfg["header"],
                    "font_name": "Segoe UI",
                    "bg_color": t["header_bg"],
                    "font_color": t["header_font"],
                    "border": 1,
                    "align": "center",
                }
            )
            sheet.write(3, c, code, fmt_chip)
            c += 1

        sheet.write(4, 0, "Signals", fmt_chip_label)
        sheet.write(4, 1, "\u2191 Increase", fmt_sig_inc)
        sheet.write(4, 2, "\u2193 Decrease", fmt_sig_dec)
        sheet.write(4, 3, "NEW", fmt_sig_new)
        sheet.write(4, 4, "SOLD OUT", fmt_sig_sold)
        sheet.write(4, 5, "\u2014 Unknown", fmt_sig_unk)

        header_row = 6
        for idx, col in enumerate(cols):
            sheet.write(header_row, idx, col, fmt_header)

        row = header_row + 1
        for rec in work.itertuples(index=False):
            for idx, val in enumerate(rec):
                if cols[idx] in {"route", "current_capture_label", "previous_capture_label"}:
                    sheet.write(row, idx, "" if pd.isna(val) else str(val), fmt_left)
                else:
                    sheet.write(row, idx, "" if pd.isna(val) else val, fmt_cell)
            row += 1

        if row > header_row + 1:
            sheet.autofilter(header_row, 0, row - 1, len(cols) - 1)
        widths = [14, 12, 10, 16, 16, 8, 12, 9, 10, 11, 10, 10, 10, 10, 10, 9, 10, 10, 10, 10, 10]
        for idx, w in enumerate(widths):
            sheet.set_column(idx, idx, w)
        sheet.set_column(0, max(6, len(chip_airlines)), 14)
        sheet.freeze_panes(header_row + 1, 0)

    def _write_full_capture_history(self, workbook, history_df: pd.DataFrame):
        sheet = workbook.add_worksheet("Full Capture History")
        cfg = self._style_cfg()
        sheet.set_zoom(cfg["zoom"])
        sheet.set_default_row(cfg["default_row"])

        fmt_title = workbook.add_format({"bold": True, "font_size": cfg["title"], "font_name": "Segoe UI", "font_color": "#1F4E78"})
        fmt_note = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "font_color": "#555555"})
        fmt_header = workbook.add_format({"bold": True, "font_size": cfg["header"], "font_name": "Segoe UI", "bg_color": "#D9E1F2", "border": 1, "align": "center"})
        fmt_cell = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "center"})
        fmt_left = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "left"})
        fmt_changed = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "center", "bg_color": "#FFF2CC"})
        fmt_nochange = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "center", "font_color": "#777777"})

        sheet.write(0, 0, "Full Capture History", fmt_title)
        sheet.write(
            1,
            0,
            "All available capture timestamps for selected route/flight/date scope, with per-capture deltas.",
            fmt_note,
        )

        columns = [
            "route",
            "airline",
            "flight_number",
            "flight_date",
            "day_name",
            "departure_time",
            "scrape_id",
            "captured_at_utc",
            "capture_label",
            "previous_capture_label",
            "state_changed_flag",
            "status",
            "min_fare",
            "max_fare",
            "tax_amount",
            "min_seats",
            "max_seats",
            "seat_capacity",
            "load_pct",
            "min_fare_delta",
            "max_fare_delta",
            "tax_amount_delta",
            "min_seats_delta",
            "max_seats_delta",
            "load_pct_delta",
            "offer_rows",
        ]

        row = 3
        for i, c in enumerate(columns):
            sheet.write(row, i, c, fmt_header)
        row += 1

        if history_df is None or history_df.empty:
            sheet.write(row, 0, "No capture history rows found for selected scope.", fmt_note)
            sheet.set_column(0, 0, 60)
            return

        work = history_df.copy()
        for c in columns:
            if c not in work.columns:
                work[c] = pd.NA

        work["flight_date"] = pd.to_datetime(work["flight_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        work["captured_at_utc"] = pd.to_datetime(work["captured_at_utc"], errors="coerce", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
        work = work.sort_values(
            ["route", "airline", "flight_number", "flight_date", "departure_time", "captured_at_utc"],
            na_position="last",
        )

        for rec in work[columns].itertuples(index=False):
            changed_flag = str(rec[10] or "").upper()
            flag_fmt = fmt_changed if changed_flag == "CHANGED/NEW" else fmt_nochange
            for i, val in enumerate(rec):
                cell_fmt = fmt_left if columns[i] in {"route"} else fmt_cell
                if columns[i] == "state_changed_flag":
                    cell_fmt = flag_fmt
                sheet.write(row, i, "" if pd.isna(val) else val, cell_fmt)
            row += 1

        sheet.autofilter(3, 0, row - 1, len(columns) - 1)
        widths = [14, 8, 12, 11, 10, 10, 14, 20, 17, 17, 14, 10, 10, 10, 10, 9, 9, 11, 9, 12, 12, 12, 12, 12, 11, 9]
        for i, w in enumerate(widths):
            sheet.set_column(i, i, w)
        sheet.freeze_panes(4, 0)

    def _write_route_block_index(self, workbook, blocks):
        sheet = workbook.add_worksheet("Route Block Index")
        sheet.hide()

        headers = ["route", "start_row", "end_row", "airlines_csv", "signals_csv"]
        for c, h in enumerate(headers):
            sheet.write(0, c, h)

        row = 1
        for b in blocks or []:
            sheet.write(row, 0, str(b.get("route") or ""))
            sheet.write(row, 1, int(b.get("start_row") or 0))
            sheet.write(row, 2, int(b.get("end_row") or 0))
            sheet.write(row, 3, str(b.get("airlines_csv") or ""))
            sheet.write(row, 4, str(b.get("signals_csv") or ""))
            row += 1

    def _write_route_row_index(self, workbook, rows):
        sheet = workbook.add_worksheet("Route Row Index")
        sheet.hide()

        headers = [
            "route",
            "row_number",
            "variant_key",
            "flight_date",
            "airlines_csv",
            "signals_csv",
            "airline_signals_csv",
            "is_primary_variant",
            "has_history_stack",
        ]
        for c, h in enumerate(headers):
            sheet.write(0, c, h)

        row = 1
        for rec in rows or []:
            sheet.write(row, 0, str(rec.get("route") or ""))
            sheet.write(row, 1, int(rec.get("row_number") or 0))
            sheet.write(row, 2, str(rec.get("variant_key") or ""))
            sheet.write(row, 3, str(rec.get("flight_date") or ""))
            sheet.write(row, 4, str(rec.get("airlines_csv") or ""))
            sheet.write(row, 5, str(rec.get("signals_csv") or ""))
            sheet.write(row, 6, str(rec.get("airline_signals_csv") or ""))
            sheet.write(row, 7, int(rec.get("is_primary_variant") or 0))
            sheet.write(row, 8, int(rec.get("has_history_stack") or 0))
            row += 1

    def _write_route_column_index(self, workbook, cols):
        sheet = workbook.add_worksheet("Route Column Index")
        sheet.hide()

        headers = [
            "route",
            "start_row",
            "end_row",
            "airline",
            "start_col",
            "end_col",
            "data_start_row",
            "data_end_row",
        ]
        for c, h in enumerate(headers):
            sheet.write(0, c, h)

        row = 1
        for rec in cols or []:
            sheet.write(row, 0, str(rec.get("route") or ""))
            sheet.write(row, 1, int(rec.get("start_row") or 0))
            sheet.write(row, 2, int(rec.get("end_row") or 0))
            sheet.write(row, 3, str(rec.get("airline") or ""))
            sheet.write(row, 4, int(rec.get("start_col") or 0))
            sheet.write(row, 5, int(rec.get("end_col") or 0))
            sheet.write(row, 6, int(rec.get("data_start_row") or 0))
            sheet.write(row, 7, int(rec.get("data_end_row") or 0))
            row += 1

    def _write_execution_plan_status(self, workbook, execution_plan_status):
        if not isinstance(execution_plan_status, dict) or not execution_plan_status:
            return

        sheet = workbook.add_worksheet("Execution Plan Status")
        cfg = self._style_cfg()
        sheet.set_zoom(cfg["zoom"])
        sheet.set_default_row(cfg["default_row"])

        fmt_title = workbook.add_format({"bold": True, "font_size": cfg["title"], "font_name": "Segoe UI", "font_color": "#1F4E78"})
        fmt_note = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "font_color": "#555555"})
        fmt_section = workbook.add_format({"bold": True, "font_size": cfg["section"], "font_name": "Segoe UI", "font_color": "#1F4E78"})
        fmt_header = workbook.add_format({"bold": True, "font_size": cfg["header"], "font_name": "Segoe UI", "bg_color": "#D9E1F2", "border": 1, "align": "center"})
        fmt_cell = workbook.add_format({"font_size": cfg["body"], "font_name": "Segoe UI", "border": 1, "align": "left"})

        coverage = execution_plan_status.get("coverage_summary")
        if not isinstance(coverage, dict):
            coverage = {}

        sheet.write(0, 0, "Execution Plan Status", fmt_title)
        source = str(execution_plan_status.get("_source") or "unknown")
        sheet.write(1, 0, f"Source: {source}", fmt_note)

        summary_rows = [
            ("generated_at_utc", execution_plan_status.get("generated_at_utc")),
            ("ultimate_priority_goal", execution_plan_status.get("ultimate_priority_goal")),
            ("current_phase", execution_plan_status.get("current_phase")),
            ("recommended_next_phase", execution_plan_status.get("recommended_next_phase")),
            ("pipeline_rc", execution_plan_status.get("pipeline_rc")),
            ("coverage_gate_passed", coverage.get("coverage_gate_passed")),
            ("coverage_pct", coverage.get("coverage_pct")),
            ("expected_airlines_count", len(coverage.get("expected_airlines") or [])),
            ("covered_airlines_count", len(coverage.get("covered_airlines") or [])),
            ("missing_airlines_count", len(coverage.get("missing_airlines") or [])),
            ("missing_airlines", ", ".join(coverage.get("missing_airlines") or [])),
        ]

        row = 3
        sheet.write(row, 0, "Field", fmt_header)
        sheet.write(row, 1, "Value", fmt_header)
        row += 1
        for k, v in summary_rows:
            sheet.write(row, 0, str(k), fmt_cell)
            sheet.write(row, 1, "" if v is None else str(v), fmt_cell)
            row += 1

        row += 1
        sheet.write(row, 0, "Phase Sequence", fmt_section)
        row += 1
        sheet.write(row, 0, "id", fmt_header)
        sheet.write(row, 1, "status", fmt_header)
        sheet.write(row, 2, "description", fmt_header)
        row += 1

        phases = execution_plan_status.get("phase_sequence")
        if isinstance(phases, list) and phases:
            for p in phases:
                if not isinstance(p, dict):
                    continue
                sheet.write(row, 0, str(p.get("id") or ""), fmt_cell)
                sheet.write(row, 1, str(p.get("status") or ""), fmt_cell)
                sheet.write(row, 2, str(p.get("description") or ""), fmt_cell)
                row += 1
        else:
            sheet.write(row, 0, "no_phase_sequence_data", fmt_cell)
            row += 1

        sheet.set_column(0, 0, 28)
        sheet.set_column(1, 1, 30)
        sheet.set_column(2, 2, 80)
        sheet.freeze_panes(4, 0)

    def write_route_flight_fare_monitor(
        self,
        writer,
        df: pd.DataFrame,
        full_capture_history: pd.DataFrame | None = None,
        execution_plan_status=None,
    ):
        required_cols = [
            "min_seats",
            "max_seats",
            "min_fare",
            "max_fare",
            "min_rbd",
            "min_rbd_seats",
            "max_rbd",
            "max_rbd_seats",
            "seat_delta",
            "min_fare_delta",
            "max_fare_delta",
            "tax_delta",
            "load_delta",
            "status",
        ]
        for c in required_cols:
            if c not in df.columns:
                raise RuntimeError(f"{c} missing in dataframe")

        workbook = writer.book
        trip_types_present = sorted(
            {
                str(v).strip().upper()
                for v in df.get("search_trip_type", pd.Series(dtype=object)).dropna().astype(str).tolist()
                if str(v).strip()
            }
        )
        if trip_types_present == ["RT"]:
            sheet_name = "Route Fare Monitor - RT"
        else:
            sheet_name = "Route Flight Fare Monitor"
        sheet = workbook.add_worksheet(sheet_name)
        cfg = self._style_cfg()
        sheet.set_zoom(cfg["zoom"])
        sheet.set_default_row(cfg["default_row"])
        block_border = 2

        airline_codes = sorted([a for a in df.get("airline", pd.Series(dtype=str)).dropna().astype(str).str.upper().unique() if str(a).strip()])
        airline_theme = self._airline_theme_map(airline_codes)

        fmt_arrow_up = workbook.add_format({"font_name": "Segoe UI", "font_color": "green", "bold": True, "font_size": cfg["sub"] + 2})
        fmt_arrow_down = workbook.add_format({"font_name": "Segoe UI", "font_color": "red", "bold": True, "font_size": cfg["sub"] + 2})
        fmt_sub = workbook.add_format({"font_name": "Segoe UI", "font_script": 2, "font_size": cfg["sub"], "bold": True})
        fmt_sub_soldout = workbook.add_format({"font_name": "Segoe UI", "font_script": 2, "font_size": cfg["sub"], "bold": True, "italic": True, "font_color": "#777777"})
        fmt_sub_new = workbook.add_format({"font_name": "Segoe UI", "font_script": 2, "font_size": cfg["sub"], "bold": True, "italic": True, "font_color": "#1F4BD8"})
        fmt_sig_count_sub = workbook.add_format({"font_name": "Segoe UI", "font_script": 2, "font_size": max(cfg["sub"] - 1, 8), "bold": True})
        arrow_up = "\u2191"
        arrow_down = "\u2193"
        emdash = "\u2014"

        fmt_sheet_title = workbook.add_format({"font_name": "Segoe UI", "bold": True, "font_size": cfg["title"] + 1, "align": "center", "valign": "vcenter", "bg_color": "#F2F2F2", "border": 1})
        fmt_route = workbook.add_format({"font_name": "Segoe UI", "bold": True, "font_size": cfg["title"]})
        fmt_note = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "font_color": "#555555"})
        fmt_route_leader_default = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "bold": True, "align": "left", "valign": "vcenter", "text_wrap": True, "bg_color": "#F2F2F2", "border": 1})
        fmt_header = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["header"], "bold": True, "border": 1, "align": "center", "valign": "vcenter"})
        fmt_cell = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "align": "center"})
        fmt_gray = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "align": "center", "font_color": "#777777"})
        fmt_date_row = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "align": "center", "valign": "vcenter", "bg_color": "#FAFAFA"})
        fmt_date_group_top = workbook.add_format(
            {
                "font_name": "Segoe UI",
                "font_size": cfg["body"],
                "align": "center",
                "valign": "vcenter",
                "bg_color": "#FAFAFA",
                "left": 1,
                "right": 1,
                "top": block_border,
                "bottom": 0,
            }
        )
        fmt_date_group_bottom = workbook.add_format(
            {
                "font_name": "Segoe UI",
                "font_size": cfg["body"],
                "align": "center",
                "valign": "vcenter",
                "bg_color": "#FAFAFA",
                "left": 1,
                "right": 1,
                "top": 0,
                "bottom": block_border,
            }
        )
        fmt_legend_key = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["header"], "bold": True, "border": 1, "bg_color": "#F2F2F2", "align": "center"})
        fmt_tag_new = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "bold": True, "italic": True, "font_color": "#1F4BD8", "border": 1, "align": "center"})
        fmt_tag_soldout = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "bold": True, "italic": True, "font_color": "#777777", "border": 1, "align": "center"})
        fmt_date_row_bottom = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "align": "center", "valign": "vcenter", "bg_color": "#FAFAFA", "bottom": block_border})
        # Softer pastel change cues for better readability on dense sheets.
        fmt_change_up_bg = workbook.add_format({"bg_color": "#F1FBF4"})
        fmt_change_down_bg = workbook.add_format({"bg_color": "#FFF4F6"})
        fmt_change_new_bg = workbook.add_format({"bg_color": "#F1F4FF"})
        fmt_change_sold_bg = workbook.add_format({"bg_color": "#F6F6F6"})

        fmt_header_airline = {}
        fmt_metric_airline = {}
        fmt_metric_airline_left = {}
        fmt_metric_airline_right = {}
        fmt_cell_airline = {}
        fmt_cell_airline_left = {}
        fmt_cell_airline_right = {}
        fmt_gray_airline = {}
        fmt_gray_airline_left = {}
        fmt_gray_airline_right = {}
        fmt_cell_airline_bottom = {}
        fmt_cell_airline_left_bottom = {}
        fmt_cell_airline_right_bottom = {}
        fmt_gray_airline_bottom = {}
        fmt_gray_airline_left_bottom = {}
        fmt_gray_airline_right_bottom = {}
        fmt_route_leader_airline = {}
        for code, t in airline_theme.items():
            fmt_header_airline[code] = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["header"], "bold": True, "border": 1, "left": block_border, "right": block_border, "top": block_border, "align": "center", "bg_color": t["header_bg"], "font_color": t["header_font"]})
            fmt_metric_airline[code] = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["header"], "bold": True, "border": 1, "align": "center", "bg_color": t["subheader_bg"], "font_color": t["text_font"]})
            fmt_metric_airline_left[code] = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["header"], "bold": True, "border": 1, "left": block_border, "align": "center", "bg_color": t["subheader_bg"], "font_color": t["text_font"]})
            fmt_metric_airline_right[code] = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["header"], "bold": True, "border": 1, "right": block_border, "align": "center", "bg_color": t["subheader_bg"], "font_color": t["text_font"]})
            fmt_cell_airline[code] = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "align": "center", "bg_color": t["cell_bg"], "font_color": t["text_font"]})
            fmt_cell_airline_left[code] = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "left": block_border, "align": "center", "bg_color": t["cell_bg"], "font_color": t["text_font"]})
            fmt_cell_airline_right[code] = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "right": block_border, "align": "center", "bg_color": t["cell_bg"], "font_color": t["text_font"]})
            fmt_gray_airline[code] = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "align": "center", "bg_color": t["cell_bg"], "font_color": "#777777"})
            fmt_gray_airline_left[code] = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "left": block_border, "align": "center", "bg_color": t["cell_bg"], "font_color": "#777777"})
            fmt_gray_airline_right[code] = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "right": block_border, "align": "center", "bg_color": t["cell_bg"], "font_color": "#777777"})
            fmt_cell_airline_bottom[code] = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "bottom": block_border, "align": "center", "bg_color": t["cell_bg"], "font_color": t["text_font"]})
            fmt_cell_airline_left_bottom[code] = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "left": block_border, "bottom": block_border, "align": "center", "bg_color": t["cell_bg"], "font_color": t["text_font"]})
            fmt_cell_airline_right_bottom[code] = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "right": block_border, "bottom": block_border, "align": "center", "bg_color": t["cell_bg"], "font_color": t["text_font"]})
            fmt_gray_airline_bottom[code] = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "bottom": block_border, "align": "center", "bg_color": t["cell_bg"], "font_color": "#777777"})
            fmt_gray_airline_left_bottom[code] = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "left": block_border, "bottom": block_border, "align": "center", "bg_color": t["cell_bg"], "font_color": "#777777"})
            fmt_gray_airline_right_bottom[code] = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "right": block_border, "bottom": block_border, "align": "center", "bg_color": t["cell_bg"], "font_color": "#777777"})
            fmt_route_leader_airline[code] = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "bold": True, "align": "left", "valign": "vcenter", "text_wrap": True, "bg_color": t["subheader_bg"], "font_color": t["text_font"], "border": 1})

        df = df.sort_values(["route", "flight_date", "departure_time"])
        if "day_name" not in df.columns:
            df["day_name"] = pd.to_datetime(df["flight_date"]).dt.day_name()
        if "search_trip_type" not in df.columns:
            df["search_trip_type"] = "OW"
        df["search_trip_type"] = df["search_trip_type"].fillna("OW").astype(str).str.upper()
        if "requested_return_date" not in df.columns:
            df["requested_return_date"] = pd.NaT
        df["requested_return_date"] = pd.to_datetime(df["requested_return_date"], errors="coerce").dt.date
        df["requested_return_date_label"] = df["requested_return_date"].apply(
            lambda v: pd.Timestamp(v).strftime("%d %b %Y") if pd.notna(v) else "--"
        )
        df["requested_return_date_key"] = df["requested_return_date"].apply(
            lambda v: pd.Timestamp(v).strftime("%Y-%m-%d") if pd.notna(v) else ""
        )
        df["requested_return_day_name"] = pd.to_datetime(df["requested_return_date"], errors="coerce").dt.day_name()
        df["requested_return_day_name"] = df["requested_return_day_name"].fillna("--")
        dep_dates = pd.to_datetime(df["flight_date"], errors="coerce")
        ret_dates = pd.to_datetime(df["requested_return_date"], errors="coerce")
        stay_days = (ret_dates - dep_dates).dt.days
        df["stay_nights"] = stay_days.where(stay_days.notna() & (stay_days >= 0))
        df["stay_label"] = df["stay_nights"].apply(
            lambda v: f"{int(v)} Days" if pd.notna(v) else "--"
        )

        curr_cap = str((df.get("current_capture_label", pd.Series(["Current snapshot"])).iloc[0] if len(df) else "Current snapshot") or "Current snapshot")
        prev_cap = str((df.get("previous_capture_label", pd.Series(["Previous snapshot"])).iloc[0] if len(df) else "Previous snapshot") or "Previous snapshot")
        workbook_trip_types = sorted(
            {
                str(v).strip().upper()
                for v in df.get("search_trip_type", pd.Series(dtype=object)).dropna().astype(str).tolist()
                if str(v).strip()
            }
        )
        workbook_return_dates = sorted(
            {
                pd.Timestamp(v).strftime("%d %b %Y")
                for v in df.get("requested_return_date", pd.Series(dtype=object)).dropna().tolist()
                if pd.notna(v)
            }
        )

        history_day_variants = {}
        history_offer_map = {}
        history_day_capture_rows = {}
        if isinstance(full_capture_history, pd.DataFrame) and not full_capture_history.empty:
            hist = full_capture_history.copy()
            need_cols = {
                "route",
                "airline",
                "flight_number",
                "flight_date",
                "departure_time",
                "capture_label",
                "captured_at_utc",
                "state_changed_flag",
            }
            if need_cols.issubset(set(hist.columns)):
                hist["route"] = hist["route"].astype(str)
                hist["airline"] = hist["airline"].astype(str).str.upper().str.strip()
                hist["flight_number"] = hist["flight_number"].astype(str).str.strip()
                hist["flight_date"] = pd.to_datetime(hist["flight_date"], errors="coerce").dt.strftime("%Y-%m-%d")
                if "requested_return_date" not in hist.columns:
                    hist["requested_return_date"] = ""
                hist["requested_return_date"] = pd.to_datetime(hist["requested_return_date"], errors="coerce").dt.strftime("%Y-%m-%d")
                hist["requested_return_date"] = hist["requested_return_date"].fillna("")
                hist["departure_time"] = hist["departure_time"].astype(str).str.strip().str.slice(0, 5)
                hist["capture_label"] = hist["capture_label"].fillna("").astype(str)
                hist["captured_at_utc"] = pd.to_datetime(hist["captured_at_utc"], errors="coerce", utc=True)
                hist["cap_key"] = hist["captured_at_utc"].dt.strftime("%Y-%m-%d %H:%M:%S")
                hist["cap_key"] = hist["cap_key"].fillna(hist["capture_label"])
                hist = hist.sort_values(["route", "flight_date", "captured_at_utc", "cap_key"], na_position="last")

                for _, rr in hist.iterrows():
                    route_key = str(rr.get("route") or "")
                    date_key = str(rr.get("flight_date") or "")
                    cap_key = str(rr.get("cap_key") or "")
                    if not route_key or not date_key or not cap_key:
                        continue
                    offer_key = (
                        route_key,
                        date_key,
                        str(rr.get("requested_return_date") or ""),
                        str(rr.get("airline") or "").upper().strip(),
                        str(rr.get("flight_number") or "").strip(),
                        str(rr.get("departure_time") or "").strip(),
                        cap_key,
                    )
                    row_dict = rr.to_dict()
                    history_offer_map[offer_key] = row_dict
                    history_day_capture_rows.setdefault((route_key, date_key, str(rr.get("requested_return_date") or ""), cap_key), []).append(row_dict)

                changed = hist[hist["state_changed_flag"].astype(str).str.upper() != "NO_CHANGE"].copy()
                if not changed.empty:
                    for (route_key, date_key, return_date_key), grp in changed.groupby(["route", "flight_date", "requested_return_date"], sort=False):
                        uniq = (
                            grp[["cap_key", "capture_label", "captured_at_utc"]]
                            .dropna(subset=["cap_key"])
                            .drop_duplicates(subset=["cap_key"], keep="first")
                            .sort_values(["captured_at_utc", "cap_key"], na_position="last")
                        )
                        vals = [
                            (str(r.cap_key), str(r.capture_label or r.cap_key))
                            for r in uniq.itertuples(index=False)
                        ]
                        if vals:
                            history_day_variants[(str(route_key), str(date_key), str(return_date_key or ""))] = vals

        sheet.set_column(0, 0, 12)
        sheet.set_column(1, 1, 16)
        sheet.set_column(2, 2, 14)
        sheet.set_column(3, 3, 16)
        sheet.set_column(4, 4, 12)
        sheet.set_column(5, 5, 18)

        legend_airlines = [str(a).strip().upper() for a in airline_codes if str(a).strip()]
        signal_specs = [
            ("INCREASE", f"{arrow_up} Increase"),
            ("DECREASE", f"{arrow_down} Decrease"),
            ("NEW", "NEW"),
            ("SOLD OUT", "SOLD OUT"),
            ("UNKNOWN", f"{emdash} Unknown"),
        ]
        legend_airline_end_col = len(legend_airlines)
        legend_status_end_col = len(signal_specs)
        title_end_col = max(12, 1 + max(legend_airline_end_col, legend_status_end_col))
        sheet.merge_range(0, 0, 0, title_end_col, "Aero Pulse Intelligence Monitor", fmt_sheet_title)

        sheet.write(1, 0, "Airlines", fmt_legend_key)
        lc = 1
        for a in legend_airlines:
            theme = a if a in fmt_header_airline else "DEFAULT"
            sheet.write(1, lc, a, fmt_header_airline[theme])
            lc += 1

        sheet.write(2, 0, "Signals", fmt_legend_key)
        signal_col_map = {}
        signal_fmt_map = {}
        signal_label_map = {}
        for idx, (signal_key, signal_label) in enumerate(signal_specs):
            col = 1 + idx
            if signal_key == "NEW":
                fmt = fmt_tag_new
            elif signal_key == "SOLD OUT":
                fmt = fmt_tag_soldout
            elif signal_key == "UNKNOWN":
                fmt = fmt_gray
            else:
                fmt = fmt_cell
            signal_col_map[signal_key] = col
            signal_fmt_map[signal_key] = fmt
            signal_label_map[signal_key] = signal_label
            sheet.write(2, col, signal_label, fmt)

        sheet.set_row(1, cfg["legend_row"])
        sheet.set_row(2, cfg["legend_row"])
        max_flight_cols = 1
        if not df.empty:
            for _, route_df_for_cols in df.groupby("route", sort=False):
                route_col_width = 0
                for _, flight_df_for_cols in route_df_for_cols.groupby("flight_key", sort=False):
                    route_col_width += 5 if self._has_inventory_signal(flight_df_for_cols) else 3
                max_flight_cols = max(max_flight_cols, route_col_width)
        note_start_col = max(10, 6 + max_flight_cols + 2)
        self._write_methodology_note(sheet, 0, note_start_col, workbook)

        def _changed(day_frame: pd.DataFrame) -> bool:
            if day_frame is None or day_frame.empty:
                return False
            for _, rec in day_frame.iterrows():
                if str(rec.get("status") or "").upper() in {"NEW", "SOLD OUT"}:
                    return True
                for dcol in ("min_fare_delta", "max_fare_delta", "seat_delta", "tax_delta", "load_delta"):
                    v = rec.get(dcol)
                    if self._is_na(v):
                        continue
                    try:
                        if float(v) != 0.0:
                            return True
                    except Exception:
                        pass
            return False

        trip_label = ", ".join(workbook_trip_types) if workbook_trip_types else "OW"
        return_label = ", ".join(workbook_return_dates[:8]) if workbook_return_dates else "--"
        if len(workbook_return_dates) > 8:
            return_label += f" (+{len(workbook_return_dates) - 8})"
        sheet.merge_range(4, 0, 4, title_end_col, f"Trip Type: {trip_label} | Return Dates: {return_label}", fmt_note)
        row = 6
        route_blocks = []
        route_row_entries = []
        route_col_entries = []
        route_sep = "\u2013"
        leader_sep = "\u2014"

        for route, route_df in df.groupby("route", sort=False):
            route_block_start = row
            route_display = str(route).replace("-", route_sep)
            sheet.write(row, 0, route_display, fmt_route)

            leader_df = route_df[route_df["leader"] & route_df["min_fare"].notna()]
            flights = (
                route_df.groupby("flight_key", as_index=False)
                .first()[["flight_key", "airline", "flight_number", "aircraft", "departure_time"]]
                .sort_values("departure_time")
            )
            flight_metrics = {}
            for _, f in flights.iterrows():
                flight_slice = route_df[route_df["flight_key"] == f.flight_key]
                if self._has_inventory_signal(flight_slice):
                    flight_metrics[f.flight_key] = ["Min Fare", "Max Fare", "Tax Amount", "Open/Cap", "Inv Press"]
                else:
                    flight_metrics[f.flight_key] = ["Min Fare", "Max Fare", "Tax Amount"]

            total_flight_cols = max(1, sum(len(flight_metrics.get(f.flight_key, [])) for _, f in flights.iterrows()))
            leader_end_col = 5 + total_flight_cols
            if leader_df.empty:
                leader_txt = "Route Price Leader (Lowest Fare): \u2014"
                leader_fmt = fmt_route_leader_default
            else:
                lr = leader_df.sort_values("min_fare").iloc[0]
                code = self._flight_code_label(lr.airline, lr.flight_number)
                leader_dates = leader_df[(leader_df["flight_key"] == lr["flight_key"]) & (pd.to_numeric(leader_df["min_fare"], errors="coerce") == float(lr.min_fare))]["flight_date"].dropna().astype(str).tolist()
                leader_dates = sorted(set(leader_dates))
                date_tokens = pd.to_datetime(leader_dates, errors="coerce").strftime("%d %b").tolist() if leader_dates else []
                date_label = ", ".join(date_tokens)
                leader_return_dates = leader_df[(leader_df["flight_key"] == lr["flight_key"]) & (pd.to_numeric(leader_df["min_fare"], errors="coerce") == float(lr.min_fare))]["requested_return_date"].dropna().astype(str).tolist() if "requested_return_date" in leader_df.columns else []
                leader_return_dates = sorted(set(leader_return_dates))
                leader_return_tokens = pd.to_datetime(leader_return_dates, errors="coerce").strftime("%d %b").tolist() if leader_return_dates else []
                leader_return_label = ", ".join(leader_return_tokens)
                leader_txt = f"Route Price Leader (Lowest Fare): {code} {leader_sep} {int(lr.min_fare):,}" + (f" (Dates: {date_label})" if date_label else "")
                if leader_return_label:
                    leader_txt += f" | Returns: {leader_return_label}"
                leader_airline = str(getattr(lr, "airline", "") or "").upper()
                leader_fmt = fmt_route_leader_airline.get(leader_airline, fmt_route_leader_default)
            sheet.merge_range(row, 1, row, leader_end_col, leader_txt, leader_fmt)
            sheet.set_row(row, cfg["route_title_row"])
            row += 1

            sheet.merge_range(row, 0, row + 2, 0, "Outbound Date", fmt_header)
            sheet.merge_range(row, 1, row + 2, 1, "Outbound Weekday", fmt_header)
            sheet.merge_range(row, 2, row + 2, 2, "Inbound Date", fmt_header)
            sheet.merge_range(row, 3, row + 2, 3, "Inbound Weekday", fmt_header)
            sheet.merge_range(row, 4, row + 2, 4, "Length of Stay", fmt_header)
            sheet.merge_range(row, 5, row + 2, 5, "Capture Date/Time", fmt_header)
            col_map = {}
            col_airline = {}
            col_flight_number = {}
            col_departure_time = {}
            route_col_groups = []
            col = 6
            for _, f in flights.iterrows():
                aircraft = f.aircraft if pd.notna(f.aircraft) else "Aircraft NA"
                code = self._flight_code_label(f.airline, f.flight_number)
                airline_code = str(f.airline or "").upper()
                theme = airline_code if airline_code in fmt_header_airline else "DEFAULT"
                metric_cols = flight_metrics.get(f.flight_key, ["Min Fare", "Max Fare", "Tax Amount"])
                span = max(1, len(metric_cols))
                sheet.merge_range(row, col, row, col + span - 1, f"{code} | {aircraft}", fmt_header_airline[theme])
                col_map[f.flight_key] = col
                col_airline[f.flight_key] = airline_code
                col_flight_number[f.flight_key] = str(f.flight_number or "").strip()
                col_departure_time[f.flight_key] = str(f.departure_time or "").strip()[:5]
                for wcol in range(col, col + span):
                    sheet.set_column(wcol, wcol, 13)
                route_col_groups.append(
                    {
                        "airline": airline_code,
                        "start_col": int(col + 1),
                        "end_col": int(col + span),
                    }
                )
                col += span
            row += 1

            for _, f in flights.iterrows():
                start_col = col_map[f.flight_key]
                dep_txt = str(f.departure_time) if pd.notna(f.departure_time) else ""
                airline_code = str(f.airline or "").upper()
                theme = airline_code if airline_code in fmt_header_airline else "DEFAULT"
                span = len(flight_metrics.get(f.flight_key, ["Min Fare", "Max Fare", "Tax Amount"]))
                sheet.merge_range(row, start_col, row, start_col + span - 1, dep_txt, fmt_header_airline[theme])
            row += 1

            for fk, start_col in col_map.items():
                airline_code = col_airline.get(fk, "")
                theme = airline_code if airline_code in fmt_metric_airline else "DEFAULT"
                metrics = flight_metrics.get(fk, ["Min Fare", "Max Fare", "Tax Amount"])
                for i, m in enumerate(metrics):
                    if i == 0:
                        metric_fmt = fmt_metric_airline_left[theme]
                    elif i == len(metrics) - 1:
                        metric_fmt = fmt_metric_airline_right[theme]
                    else:
                        metric_fmt = fmt_metric_airline[theme]
                    sheet.write(row, start_col + i, m, metric_fmt)
            row += 1

            day_groups = list(route_df.groupby(["flight_date", "day_name", "requested_return_date_key", "requested_return_date_label", "requested_return_day_name", "stay_label"], sort=True, dropna=False))
            for day_idx, ((date, day, return_date_key, return_date_label, return_day_name, stay_label), day_df) in enumerate(day_groups):
                is_last_day = day_idx == (len(day_groups) - 1)
                date_fmt = fmt_date_row_bottom if is_last_day else fmt_date_row
                day_curr_cap = str(
                    day_df.get("current_capture_label", pd.Series(dtype=object))
                    .dropna()
                    .astype(str)
                    .replace("", pd.NA)
                    .dropna()
                    .iloc[0]
                    if "current_capture_label" in day_df.columns
                    and not day_df.get("current_capture_label", pd.Series(dtype=object)).dropna().empty
                    else curr_cap
                )
                day_prev_cap = str(
                    day_df.get("previous_capture_label", pd.Series(dtype=object))
                    .dropna()
                    .astype(str)
                    .replace("", pd.NA)
                    .dropna()
                    .iloc[0]
                    if "previous_capture_label" in day_df.columns
                    and not day_df.get("previous_capture_label", pd.Series(dtype=object)).dropna().empty
                    else prev_cap
                )
                variants = [("current", day_curr_cap)]
                if _changed(day_df):
                    day_key = (str(route), str(date), str(return_date_key or ""))
                    hist_variants = history_day_variants.get(day_key, [])
                    if hist_variants:
                        variants = [("history", cap_label, cap_key) for cap_key, cap_label in hist_variants]
                    else:
                        variants = [("previous", day_prev_cap), ("current", day_curr_cap)]
                span = len(variants)

                # Keep Date/Day unmerged on data rows so interactive row hiding in XLSM
                # mode can safely hide a single variant row without merged-cell errors.
                variant_date_fmts = []
                for vidx in range(span):
                    row_i = row + vidx
                    if span > 1 and vidx == 0:
                        date_group_fmt = fmt_date_group_top
                    elif span > 1 and vidx == span - 1:
                        date_group_fmt = fmt_date_group_bottom
                    else:
                        date_group_fmt = date_fmt
                    variant_date_fmts.append(date_group_fmt)
                    sheet.write(row_i, 0, str(date), date_group_fmt)
                    sheet.write(row_i, 1, day, date_group_fmt)
                    sheet.write(row_i, 2, str(return_date_label or "--"), date_group_fmt)
                    sheet.write(row_i, 3, str(return_day_name or "--"), date_group_fmt)
                    sheet.write(row_i, 4, str(stay_label or "--"), date_group_fmt)

                has_history_stack = span > 1
                for vidx, variant in enumerate(variants):
                    if isinstance(variant, tuple) and len(variant) >= 3:
                        vkey, vlabel, vcap_key = variant[0], variant[1], str(variant[2] or "")
                    else:
                        vkey, vlabel = variant[0], variant[1]
                        vcap_key = ""
                    row_i = row + vidx
                    is_primary_variant = vidx == (span - 1)
                    capture_label = str(vlabel or "")
                    if has_history_stack and is_primary_variant:
                        capture_label = f"[+] {capture_label}"
                    sheet.write(row_i, 5, capture_label, variant_date_fmts[vidx])
                    if has_history_stack:
                        if is_primary_variant:
                            sheet.set_row(
                                row_i,
                                None,
                                None,
                                {
                                    "level": 1,
                                    "collapsed": True,
                                },
                            )
                        else:
                            sheet.set_row(
                                row_i,
                                None,
                                None,
                                {
                                    "level": 1,
                                    "hidden": True,
                                },
                            )

                    row_airline_signals = {}
                    if str(vkey).lower() == "history":
                        cap_rows = history_day_capture_rows.get((str(route), str(date), str(return_date_key or ""), vcap_key), [])
                        for rr in cap_rows:
                            ac = str(rr.get("airline") or "").upper().strip()
                            if not ac:
                                continue
                            sigs = set()
                            status_up = str(rr.get("status") or "").upper()
                            if status_up == "NEW":
                                sigs.add("NEW")
                            elif status_up == "SOLD OUT":
                                sigs.add("SOLD OUT")
                            for dc in ("min_fare_delta", "max_fare_delta", "tax_amount_delta", "min_seats_delta", "max_seats_delta", "load_pct_delta"):
                                vv = rr.get(dc)
                                if self._is_na(vv):
                                    continue
                                try:
                                    fv = float(vv)
                                except Exception:
                                    continue
                                if fv > 0:
                                    sigs.add("INCREASE")
                                elif fv < 0:
                                    sigs.add("DECREASE")
                            if not sigs:
                                sigs.add("UNKNOWN")
                            row_airline_signals.setdefault(ac, set()).update(sigs)
                    elif str(vkey).lower() == "previous":
                        row_airlines = sorted(
                            {
                                str(a).strip().upper()
                                for a in day_df.get("airline", pd.Series(dtype=str)).dropna().astype(str).tolist()
                                if str(a).strip()
                            }
                        )
                        for ac in row_airlines:
                            row_airline_signals[ac] = {"PREVIOUS"}
                    else:
                        for _, rr in day_df.iterrows():
                            ac = str(rr.get("airline") or "").strip().upper()
                            if not ac:
                                continue
                            sigs = set()
                            status_up = str(rr.get("status") or "").upper()
                            if status_up == "NEW":
                                sigs.add("NEW")
                            elif status_up == "SOLD OUT":
                                sigs.add("SOLD OUT")
                            for dc in ("min_fare_delta", "max_fare_delta", "tax_delta", "seat_delta", "load_delta"):
                                vv = rr.get(dc)
                                if self._is_na(vv):
                                    continue
                                try:
                                    fv = float(vv)
                                except Exception:
                                    continue
                                if fv > 0:
                                    sigs.add("INCREASE")
                                elif fv < 0:
                                    sigs.add("DECREASE")
                            if not sigs:
                                sigs.add("UNKNOWN")
                            row_airline_signals.setdefault(ac, set()).update(sigs)
                        if not row_airline_signals:
                            row_airlines = sorted(
                                {
                                    str(a).strip().upper()
                                    for a in day_df.get("airline", pd.Series(dtype=str)).dropna().astype(str).tolist()
                                    if str(a).strip()
                                }
                            )
                            for ac in row_airlines:
                                row_airline_signals[ac] = {"UNKNOWN"}
                    row_airlines = sorted(row_airline_signals.keys())
                    row_signals = sorted({sig for sigs in row_airline_signals.values() for sig in sigs})
                    airline_signals_csv = ";".join(
                        f"{ac}:{'|'.join(sorted(row_airline_signals.get(ac, set())))}"
                        for ac in sorted(row_airline_signals.keys())
                    )
                    route_row_entries.append(
                        {
                            "route": str(route),
                            "row_number": int(row_i + 1),
                            "variant_key": str(vkey),
                            "flight_date": str(date),
                            "requested_return_date": str(return_date_key or ""),
                            "airlines_csv": ",".join(row_airlines),
                            "signals_csv": ",".join(row_signals),
                            "airline_signals_csv": airline_signals_csv,
                            "is_primary_variant": int(1 if is_primary_variant else 0),
                            "has_history_stack": int(1 if has_history_stack else 0),
                        }
                    )

                    for fk, start_col in col_map.items():
                        airline_code = str(col_airline.get(fk, "") or "").upper()
                        theme = airline_code if airline_code in fmt_cell_airline else "DEFAULT"
                        metrics = flight_metrics.get(fk, ["Min Fare", "Max Fare", "Tax Amount"])
                        use_bottom = is_last_day and vidx == span - 1
                        n_o_left = fmt_gray_airline_left_bottom[theme] if use_bottom else fmt_gray_airline_left[theme]
                        n_o_mid = fmt_gray_airline_bottom[theme] if use_bottom else fmt_gray_airline[theme]
                        n_o_right = fmt_gray_airline_right_bottom[theme] if use_bottom else fmt_gray_airline_right[theme]
                        for idx, metric_name in enumerate(metrics):
                            if idx == 0:
                                metric_fmt = n_o_left
                            elif idx == len(metrics) - 1:
                                metric_fmt = n_o_right
                            else:
                                metric_fmt = n_o_mid
                            if metric_name == "Min Fare":
                                default_text = "N/O"
                            elif metric_name == "Open/Cap":
                                default_text = f"{emdash} / {emdash}"
                            else:
                                default_text = emdash
                            sheet.write(row_i, start_col + idx, default_text, metric_fmt)

                    for _, r in day_df.iterrows():
                        base = col_map.get(r.flight_key)
                        if base is None:
                            continue
                        airline_code = str(r.airline or col_airline.get(r.flight_key, "")).upper()
                        theme = airline_code if airline_code in fmt_cell_airline else "DEFAULT"
                        use_bottom = is_last_day and vidx == span - 1
                        cell_fmt_left = fmt_cell_airline_left_bottom[theme] if use_bottom else fmt_cell_airline_left[theme]
                        cell_fmt_mid = fmt_cell_airline_bottom[theme] if use_bottom else fmt_cell_airline[theme]
                        cell_fmt_right = fmt_cell_airline_right_bottom[theme] if use_bottom else fmt_cell_airline_right[theme]
                        gray_fmt_left = fmt_gray_airline_left_bottom[theme] if use_bottom else fmt_gray_airline_left[theme]
                        gray_fmt_mid = fmt_gray_airline_bottom[theme] if use_bottom else fmt_gray_airline[theme]
                        gray_fmt_right = fmt_gray_airline_right_bottom[theme] if use_bottom else fmt_gray_airline_right[theme]
                        metrics = flight_metrics.get(r.flight_key, ["Min Fare", "Max Fare", "Tax Amount"])
                        metric_index = {name: idx for idx, name in enumerate(metrics)}

                        def _pick_fmt(idx, left_fmt, mid_fmt, right_fmt):
                            if idx <= 0:
                                return left_fmt
                            if idx >= len(metrics) - 1:
                                return right_fmt
                            return mid_fmt

                        min_idx = metric_index.get("Min Fare", 0)
                        max_idx = metric_index.get("Max Fare", 1 if len(metrics) > 1 else 0)
                        tax_idx = metric_index.get("Tax Amount", len(metrics) - 1)
                        open_cap_idx = metric_index.get("Open/Cap")
                        inv_press_idx = metric_index.get("Inv Press")

                        min_cell_fmt = _pick_fmt(min_idx, cell_fmt_left, cell_fmt_mid, cell_fmt_right)
                        max_gray_fmt = _pick_fmt(max_idx, gray_fmt_left, gray_fmt_mid, gray_fmt_right)
                        tax_gray_fmt = _pick_fmt(tax_idx, gray_fmt_left, gray_fmt_mid, gray_fmt_right)
                        seat_cell_fmt = _pick_fmt(open_cap_idx, cell_fmt_left, cell_fmt_mid, cell_fmt_right) if open_cap_idx is not None else None
                        seat_gray_fmt = _pick_fmt(open_cap_idx, gray_fmt_left, gray_fmt_mid, gray_fmt_right) if open_cap_idx is not None else None
                        load_gray_fmt = _pick_fmt(inv_press_idx, gray_fmt_left, gray_fmt_mid, gray_fmt_right) if inv_press_idx is not None else None

                        if str(vkey).lower() == "history":
                            route_key = str(route)
                            date_key = str(date)
                            return_key = str(return_date_key or "")
                            flight_no = str(col_flight_number.get(r.flight_key) or "").strip()
                            dep_key = str(col_departure_time.get(r.flight_key) or "").strip()
                            offer_key = (route_key, date_key, return_key, airline_code, flight_no, dep_key, vcap_key)
                            hr = history_offer_map.get(offer_key)
                            if not isinstance(hr, dict):
                                continue

                            min_fare_int = self._to_int(hr.get("min_fare"))
                            base_price = f"{min_fare_int:,}" if min_fare_int is not None else emdash
                            min_sign = self._delta_sign(hr.get("min_fare_delta"))
                            min_arrow = arrow_up if min_sign > 0 else (arrow_down if min_sign < 0 else "")
                            min_arrow_fmt = fmt_arrow_up if min_sign > 0 else (fmt_arrow_down if min_sign < 0 else min_cell_fmt)
                            status_up = str(hr.get("status") or "").upper()
                            status_txt = ""
                            status_fmt = fmt_sub
                            if status_up == "SOLD OUT":
                                status_txt = " SOLD OUT"
                                status_fmt = fmt_sub_soldout
                            elif status_up == "NEW":
                                status_txt = " NEW"
                                status_fmt = fmt_sub_new
                            min_parts = [min_cell_fmt, base_price]
                            if status_txt:
                                min_parts += [status_fmt, status_txt]
                            if min_arrow:
                                min_parts += [min_arrow_fmt, f" {min_arrow}"]
                            min_parts += [min_cell_fmt]
                            if len(min_parts) <= 3:
                                sheet.write(row_i, base + min_idx, base_price, min_cell_fmt)
                            else:
                                sheet.write_rich_string(row_i, base + min_idx, *min_parts)

                            max_fare_int = self._to_int(hr.get("max_fare"))
                            max_sign = self._delta_sign(hr.get("max_fare_delta"))
                            max_arrow = arrow_up if max_sign > 0 else (arrow_down if max_sign < 0 else "")
                            max_arrow_fmt = fmt_arrow_up if max_sign > 0 else (fmt_arrow_down if max_sign < 0 else max_gray_fmt)
                            if max_fare_int is None:
                                sheet.write(row_i, base + max_idx, emdash, max_gray_fmt)
                            elif max_arrow:
                                sheet.write_rich_string(row_i, base + max_idx, max_gray_fmt, f"{max_fare_int:,}", max_arrow_fmt, f" {max_arrow}", max_gray_fmt)
                            else:
                                sheet.write(row_i, base + max_idx, f"{max_fare_int:,}", max_gray_fmt)

                            tax_int = self._to_int(hr.get("tax_amount"))
                            tax_sign = self._delta_sign(hr.get("tax_amount_delta"))
                            tax_arrow = arrow_up if tax_sign > 0 else (arrow_down if tax_sign < 0 else "")
                            tax_arrow_fmt = fmt_arrow_up if tax_sign > 0 else (fmt_arrow_down if tax_sign < 0 else tax_gray_fmt)
                            if tax_int is None:
                                sheet.write(row_i, base + tax_idx, emdash, tax_gray_fmt)
                            elif tax_arrow:
                                sheet.write_rich_string(row_i, base + tax_idx, tax_gray_fmt, f"{tax_int:,}", tax_arrow_fmt, f" {tax_arrow}", tax_gray_fmt)
                            else:
                                sheet.write(row_i, base + tax_idx, f"{tax_int:,}", tax_gray_fmt)

                            if open_cap_idx is not None:
                                min_seat_int = self._to_int(hr.get("min_seats"))
                                max_seat_int = self._to_int(hr.get("max_seats"))
                                seat_sign = self._delta_sign(hr.get("min_seats_delta"))
                                if seat_sign == 0:
                                    seat_sign = self._delta_sign(hr.get("max_seats_delta"))
                                seat_arrow = arrow_up if seat_sign > 0 else (arrow_down if seat_sign < 0 else "")
                                seat_arrow_fmt = fmt_arrow_up if seat_sign > 0 else (fmt_arrow_down if seat_sign < 0 else seat_cell_fmt)
                                if min_seat_int is None and max_seat_int is None:
                                    sheet.write(row_i, base + open_cap_idx, f"{emdash} / {emdash}", seat_gray_fmt)
                                elif min_seat_int is None:
                                    sheet.write(row_i, base + open_cap_idx, f"{emdash} / {max_seat_int}", seat_gray_fmt)
                                elif max_seat_int is None:
                                    sheet.write(row_i, base + open_cap_idx, f"{min_seat_int} / {emdash}", seat_gray_fmt)
                                elif seat_arrow:
                                    sheet.write_rich_string(row_i, base + open_cap_idx, seat_cell_fmt, f"{min_seat_int} / {max_seat_int}", seat_arrow_fmt, f" {seat_arrow}", seat_cell_fmt)
                                else:
                                    sheet.write(row_i, base + open_cap_idx, f"{min_seat_int} / {max_seat_int}", seat_cell_fmt)

                            if inv_press_idx is not None:
                                load_int = self._to_int(hr.get("load_pct"))
                                load_sign = self._delta_sign(hr.get("load_pct_delta"))
                                load_arrow = arrow_up if load_sign > 0 else (arrow_down if load_sign < 0 else "")
                                load_arrow_fmt = fmt_arrow_up if load_sign > 0 else (fmt_arrow_down if load_sign < 0 else load_gray_fmt)
                                if load_int is None:
                                    sheet.write(row_i, base + inv_press_idx, emdash, load_gray_fmt)
                                elif load_arrow:
                                    sheet.write_rich_string(row_i, base + inv_press_idx, load_gray_fmt, f"{load_int}%", load_arrow_fmt, f" {load_arrow}", load_gray_fmt)
                                else:
                                    sheet.write(row_i, base + inv_press_idx, f"{load_int}%", load_gray_fmt)
                            continue

                        if str(vkey).lower() == "previous":
                            min_fare_int = self._to_int(r.get("previous_min_fare"))
                            max_fare_int = self._to_int(r.get("previous_max_fare"))
                            tax_int = self._to_int(r.get("previous_tax"))
                            min_seat_int = self._to_int(r.get("previous_min_seats"))
                            max_seat_int = self._to_int(r.get("previous_max_seats"))
                            if max_seat_int is None:
                                max_seat_int = self._to_int(r.get("max_seats"))
                            load_int = self._to_int(r.get("previous_load_pct"))
                            min_fmt = min_cell_fmt if min_fare_int is not None else _pick_fmt(min_idx, gray_fmt_left, gray_fmt_mid, gray_fmt_right)
                            sheet.write(row_i, base + min_idx, f"{min_fare_int:,}" if min_fare_int is not None else emdash, min_fmt)
                            sheet.write(row_i, base + max_idx, f"{max_fare_int:,}" if max_fare_int is not None else emdash, max_gray_fmt)
                            sheet.write(row_i, base + tax_idx, f"{tax_int:,}" if tax_int is not None else emdash, tax_gray_fmt)
                            if open_cap_idx is not None:
                                if min_seat_int is None and max_seat_int is None:
                                    sheet.write(row_i, base + open_cap_idx, f"{emdash} / {emdash}", seat_gray_fmt)
                                elif min_seat_int is None:
                                    sheet.write(row_i, base + open_cap_idx, f"{emdash} / {max_seat_int}", seat_gray_fmt)
                                elif max_seat_int is None:
                                    sheet.write(row_i, base + open_cap_idx, f"{min_seat_int} / {emdash}", seat_gray_fmt)
                                else:
                                    sheet.write(row_i, base + open_cap_idx, f"{min_seat_int} / {max_seat_int}", seat_cell_fmt)
                            if inv_press_idx is not None:
                                sheet.write(row_i, base + inv_press_idx, f"{load_int}%" if load_int is not None else emdash, load_gray_fmt)
                            continue

                        min_fare_int = self._to_int(r.min_fare)
                        min_rbd = str(r.min_rbd)[:1] if pd.notna(r.min_rbd) else ""
                        min_rbd_seats = self._to_int(r.min_rbd_seats)
                        sub = f"{min_rbd}-{min_rbd_seats}" if min_rbd and min_rbd_seats is not None else min_rbd
                        base_price = f"{min_fare_int:,}" if min_fare_int is not None else emdash

                        min_sign = self._delta_sign(r.min_fare_delta)
                        min_arrow = arrow_up if min_sign > 0 else (arrow_down if min_sign < 0 else "")
                        min_arrow_fmt = fmt_arrow_up if min_sign > 0 else (fmt_arrow_down if min_sign < 0 else min_cell_fmt)

                        status_txt = ""
                        status_fmt = fmt_sub
                        if r.status == "SOLD OUT":
                            status_txt = " SOLD OUT"
                            status_fmt = fmt_sub_soldout
                        elif r.status == "NEW":
                            status_txt = " NEW"
                            status_fmt = fmt_sub_new

                        rich_parts = [min_cell_fmt, base_price]
                        if sub:
                            rich_parts += [fmt_sub, f"({sub})"]
                        if status_txt:
                            rich_parts += [status_fmt, status_txt]
                        if min_arrow:
                            rich_parts += [min_arrow_fmt, f" {min_arrow}"]
                        rich_parts += [min_cell_fmt]
                        if len(rich_parts) <= 3:
                            sheet.write(row_i, base + min_idx, base_price, min_cell_fmt)
                        else:
                            sheet.write_rich_string(row_i, base + min_idx, *rich_parts)

                        max_fare_int = self._to_int(r.max_fare)
                        if max_fare_int is not None:
                            max_rbd = str(r.max_rbd)[:1] if pd.notna(r.max_rbd) else ""
                            max_rbd_seats = self._to_int(r.max_rbd_seats)
                            max_sub = f"{max_rbd}-{max_rbd_seats}" if max_rbd and max_rbd_seats is not None else max_rbd
                            max_sign = self._delta_sign(r.max_fare_delta)
                            max_arrow = arrow_up if max_sign > 0 else (arrow_down if max_sign < 0 else "")
                            max_arrow_fmt = fmt_arrow_up if max_sign > 0 else (fmt_arrow_down if max_sign < 0 else max_gray_fmt)
                            parts = [max_gray_fmt, f"{max_fare_int:,}"]
                            if max_sub:
                                parts += [fmt_sub, f"({max_sub})"]
                            if max_arrow:
                                parts += [max_arrow_fmt, f" {max_arrow}"]
                            parts += [max_gray_fmt]
                            if len(parts) <= 3:
                                sheet.write(row_i, base + max_idx, f"{max_fare_int:,}", max_gray_fmt)
                            else:
                                sheet.write_rich_string(row_i, base + max_idx, *parts)
                        else:
                            sheet.write(row_i, base + max_idx, emdash, max_gray_fmt)

                        tax_int = self._to_int(r.current_tax)
                        tax_sign = self._delta_sign(r.tax_delta)
                        tax_arrow = arrow_up if tax_sign > 0 else (arrow_down if tax_sign < 0 else "")
                        tax_arrow_fmt = fmt_arrow_up if tax_sign > 0 else (fmt_arrow_down if tax_sign < 0 else tax_gray_fmt)
                        if tax_int is None:
                            sheet.write(row_i, base + tax_idx, emdash, tax_gray_fmt)
                        elif tax_arrow:
                            sheet.write_rich_string(row_i, base + tax_idx, tax_gray_fmt, f"{tax_int:,}", tax_arrow_fmt, f" {tax_arrow}", tax_gray_fmt)
                        else:
                            sheet.write(row_i, base + tax_idx, f"{tax_int:,}", tax_gray_fmt)

                        if open_cap_idx is not None:
                            min_seat_int = self._to_int(r.min_seats)
                            max_seat_int = self._to_int(r.max_seats)
                            seat_sign = self._delta_sign(r.seat_delta)
                            seat_arrow = arrow_up if seat_sign > 0 else (arrow_down if seat_sign < 0 else "")
                            seat_arrow_fmt = fmt_arrow_up if seat_sign > 0 else (fmt_arrow_down if seat_sign < 0 else seat_cell_fmt)
                            if min_seat_int is None and max_seat_int is None:
                                sheet.write(row_i, base + open_cap_idx, f"{emdash} / {emdash}", seat_gray_fmt)
                            elif min_seat_int is None:
                                sheet.write(row_i, base + open_cap_idx, f"{emdash} / {max_seat_int}", seat_gray_fmt)
                            elif max_seat_int is None:
                                sheet.write(row_i, base + open_cap_idx, f"{min_seat_int} / {emdash}", seat_gray_fmt)
                            elif seat_arrow:
                                sheet.write_rich_string(row_i, base + open_cap_idx, seat_cell_fmt, f"{min_seat_int} / {max_seat_int}", seat_arrow_fmt, f" {seat_arrow}", seat_cell_fmt)
                            else:
                                sheet.write(row_i, base + open_cap_idx, f"{min_seat_int} / {max_seat_int}", seat_cell_fmt)

                        if inv_press_idx is not None:
                            load_int = self._to_int(r.load_pct)
                            load_sign = self._delta_sign(r.load_delta)
                            load_arrow = arrow_up if load_sign > 0 else (arrow_down if load_sign < 0 else "")
                            load_arrow_fmt = fmt_arrow_up if load_sign > 0 else (fmt_arrow_down if load_sign < 0 else load_gray_fmt)
                            if load_int is None:
                                sheet.write(row_i, base + inv_press_idx, emdash, load_gray_fmt)
                            elif load_arrow:
                                sheet.write_rich_string(row_i, base + inv_press_idx, load_gray_fmt, f"{load_int}%", load_arrow_fmt, f" {load_arrow}", load_gray_fmt)
                            else:
                                sheet.write(row_i, base + inv_press_idx, f"{load_int}%", load_gray_fmt)

                row += span

            route_airlines = sorted(
                {
                    str(a).strip().upper()
                    for a in route_df.get("airline", pd.Series(dtype=str)).dropna().astype(str).tolist()
                    if str(a).strip()
                }
            )
            route_signals = self._collect_route_signals(route_df)
            route_blocks.append(
                {
                    "route": str(route),
                    "start_row": int(route_block_start + 1),
                    "end_row": int(max(route_block_start + 1, row)),
                    "airlines_csv": ",".join(route_airlines),
                    "signals_csv": ",".join(route_signals),
                }
            )
            route_end_row_excel = int(max(route_block_start + 1, row))
            # 1-based Excel rows for route block sections.
            # route title row = route_block_start + 1
            # flight header stack starts next row and spans 3 rows.
            header_start_row_excel = int(route_block_start + 2)
            header_end_row_excel = int(route_block_start + 4)
            data_start_row_excel = int(route_block_start + 5)
            for grp in route_col_groups:
                route_col_entries.append(
                    {
                        "route": str(route),
                        "start_row": header_start_row_excel,
                        "end_row": header_end_row_excel,
                        "airline": str(grp.get("airline") or ""),
                        "start_col": int(grp.get("start_col") or 0),
                        "end_col": int(grp.get("end_col") or 0),
                        "data_start_row": data_start_row_excel,
                        "data_end_row": route_end_row_excel,
                    }
                )

        def _normalize_signal_token(raw_token: str) -> str:
            token = str(raw_token or "").strip().upper()
            if not token:
                return ""
            if "INCREASE" in token:
                return "INCREASE"
            if "DECREASE" in token:
                return "DECREASE"
            if token == "NEW":
                return "NEW"
            if "SOLD" in token:
                return "SOLD OUT"
            if "UNKNOWN" in token or token == "STABLE":
                return "UNKNOWN"
            return token

        signal_row_counts = {"INCREASE": 0, "DECREASE": 0, "NEW": 0, "SOLD OUT": 0}
        for rec in route_row_entries:
            sig_csv = str(rec.get("signals_csv") or "")
            uniq = set()
            for part in sig_csv.split(","):
                norm = _normalize_signal_token(part)
                if norm in signal_row_counts:
                    uniq.add(norm)
            for norm in uniq:
                signal_row_counts[norm] += 1

        for signal_key, signal_label in signal_specs:
            col = signal_col_map.get(signal_key)
            fmt = signal_fmt_map.get(signal_key, fmt_cell)
            if col is None:
                continue
            if signal_key == "UNKNOWN":
                sheet.write(2, col, signal_label, fmt)
                continue
            count_val = int(signal_row_counts.get(signal_key, 0))
            sheet.write_rich_string(
                2,
                col,
                fmt,
                f"{signal_label} ",
                fmt_sig_count_sub,
                f"({count_val})",
                fmt,
            )

        for col_rec in route_col_entries:
            try:
                start_row = int(col_rec.get("data_start_row") or 0) - 1
                end_row = int(col_rec.get("data_end_row") or 0) - 1
                start_col = int(col_rec.get("start_col") or 0) - 1
                end_col = int(col_rec.get("end_col") or 0) - 1
            except Exception:
                continue
            if start_row < 0 or end_row < start_row or start_col < 0 or end_col < start_col:
                continue
            sheet.conditional_format(
                start_row,
                start_col,
                end_row,
                end_col,
                {"type": "text", "criteria": "containing", "value": arrow_up, "format": fmt_change_up_bg},
            )
            sheet.conditional_format(
                start_row,
                start_col,
                end_row,
                end_col,
                {"type": "text", "criteria": "containing", "value": arrow_down, "format": fmt_change_down_bg},
            )
            sheet.conditional_format(
                start_row,
                start_col,
                end_row,
                end_col,
                {"type": "text", "criteria": "containing", "value": "NEW", "format": fmt_change_new_bg},
            )
            sheet.conditional_format(
                start_row,
                start_col,
                end_row,
                end_col,
                {"type": "text", "criteria": "containing", "value": "SOLD OUT", "format": fmt_change_sold_bg},
            )

        if hasattr(sheet, "autofit"):
            try:
                sheet.autofit()
            except Exception:
                pass
        sheet.freeze_panes(5, 6)
        self._write_airline_ops_compare(workbook, df, full_capture_history=full_capture_history)
        self._write_changes_summary(workbook, df)
        self._write_fare_trend_sparklines(workbook, df)
        self._write_penalty_comparison(workbook, df)
        self._write_tax_comparison(workbook, df)
        self._write_route_filter_view(workbook, df)
        self._write_route_block_index(workbook, route_blocks)
        self._write_route_row_index(workbook, route_row_entries)
        self._write_route_column_index(workbook, route_col_entries)
        self._write_execution_plan_status(workbook, execution_plan_status)
