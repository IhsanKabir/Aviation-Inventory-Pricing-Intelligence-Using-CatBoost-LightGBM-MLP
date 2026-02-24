import pandas as pd
from xlsxwriter.utility import xl_range

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
                "legend_row": 24,
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
            "legend_row": 20,
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
        if len(vals) <= limit:
            return ", ".join(vals)
        return ", ".join(vals[:limit]) + f" (+{len(vals) - limit})"

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
            "Open/Cap = total opened seats (visible fare buckets) / carrier capacity.\n"
            "Inv Press = bounded inventory pressure proxy from Open/Cap (not actual load factor).\n"
            "Formula: Inv Press = 100 x (1 - min(1, Open/Cap)).\n"
            "Compare only runs with same passenger mix (ADT/CHD/INF)."
        )
        sheet.merge_range(start_row + 1, start_col, start_row + 9, start_col + 4, note, fmt_box)

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

        required = ["airline", "route", "flight_number", "flight_date", "status", "min_fare_delta", "max_fare_delta", "seat_delta", "tax_delta", "load_delta"]
        if any(c not in df.columns for c in required):
            sheet.write(0, 0, "What Changed Since Last Run", fmt_title)
            sheet.write(2, 0, "Insufficient columns to build changes summary.", fmt_note)
            return

        work = df.copy()
        work["airline"] = work["airline"].astype(str).str.upper()
        work["route"] = work["route"].astype(str)
        work["flight_number"] = work["flight_number"].astype(str)
        work["status"] = work["status"].astype(str).str.upper()

        def pos(x):
            try:
                return float(x) > 0
            except Exception:
                return False

        def neg(x):
            try:
                return float(x) < 0
            except Exception:
                return False

        work["is_new"] = work["status"].eq("NEW")
        work["is_sold_out"] = work["status"].eq("SOLD OUT")
        work["fare_up"] = work["min_fare_delta"].apply(pos) | work["max_fare_delta"].apply(pos)
        work["fare_down"] = work["min_fare_delta"].apply(neg) | work["max_fare_delta"].apply(neg)
        work["seat_up"] = work["seat_delta"].apply(pos)
        work["seat_down"] = work["seat_delta"].apply(neg)
        work["tax_up"] = work["tax_delta"].apply(pos)
        work["tax_down"] = work["tax_delta"].apply(neg)
        work["load_up"] = work["load_delta"].apply(pos)
        work["load_down"] = work["load_delta"].apply(neg)

        by_airline = (
            work.groupby("airline", as_index=False)
            .agg(
                rows=("airline", "count"),
                new=("is_new", "sum"),
                sold_out=("is_sold_out", "sum"),
                fare_up=("fare_up", "sum"),
                fare_down=("fare_down", "sum"),
                seat_up=("seat_up", "sum"),
                seat_down=("seat_down", "sum"),
                tax_up=("tax_up", "sum"),
                tax_down=("tax_down", "sum"),
                load_up=("load_up", "sum"),
                load_down=("load_down", "sum"),
            )
            .sort_values(["rows", "fare_up", "fare_down"], ascending=[False, False, False])
        )

        by_route = (
            work.groupby(["route", "airline"], as_index=False)
            .agg(
                rows=("airline", "count"),
                new=("is_new", "sum"),
                sold_out=("is_sold_out", "sum"),
                fare_up=("fare_up", "sum"),
                fare_down=("fare_down", "sum"),
                seat_down=("seat_down", "sum"),
                load_up=("load_up", "sum"),
            )
        )
        by_route["total_events"] = (
            by_route["new"] + by_route["sold_out"] + by_route["fare_up"] + by_route["fare_down"] + by_route["seat_down"] + by_route["load_up"]
        )
        by_route = by_route.sort_values(["total_events", "rows"], ascending=[False, False]).head(30)

        sheet.write(0, 0, "What Changed Since Last Run", fmt_title)
        sheet.write(1, 0, "Compact event view of movement by airline and route", fmt_note)
        self._write_methodology_note(sheet, 0, 12, workbook)

        row = 3
        sheet.write(row, 0, "A) Change Counts by Airline", fmt_section)
        row += 1
        cols_air = ["airline", "rows", "new", "sold_out", "fare_up", "fare_down", "seat_up", "seat_down", "tax_up", "tax_down", "load_up", "load_down"]
        labels_air = ["Airline", "Rows", "NEW", "SOLD OUT", "Fare Up", "Fare Down", "Seat Up", "Seat Down", "Tax Up", "Tax Down", "Press Up", "Press Down"]
        for c_idx, label in enumerate(labels_air):
            sheet.write(row, c_idx, label, fmt_header)
        row += 1
        for _, rec in by_airline.iterrows():
            for c_idx, col in enumerate(cols_air):
                val = rec[col]
                if c_idx == 0:
                    sheet.write(row, c_idx, val, fmt_cell_left)
                else:
                    sheet.write(row, c_idx, int(val) if pd.notna(val) else 0, fmt_cell)
            row += 1

        row += 2
        sheet.write(row, 0, "B) Top Routes by Change Events", fmt_section)
        row += 1
        cols_route = ["route", "airline", "rows", "new", "sold_out", "fare_up", "fare_down", "seat_down", "load_up", "total_events"]
        labels_route = ["Route", "Airline", "Rows", "NEW", "SOLD OUT", "Fare Up", "Fare Down", "Seat Down", "Press Up", "Total Events"]
        for c_idx, label in enumerate(labels_route):
            sheet.write(row, c_idx, label, fmt_header)
        row += 1
        for _, rec in by_route.iterrows():
            for c_idx, col in enumerate(cols_route):
                val = rec[col]
                if c_idx in (0, 1):
                    sheet.write(row, c_idx, val, fmt_cell_left)
                else:
                    sheet.write(row, c_idx, int(val) if pd.notna(val) else 0, fmt_cell)
            row += 1

        sheet.set_column(0, 0, 14)
        sheet.set_column(1, 1, 10)
        sheet.set_column(2, 11, 11)
        sheet.freeze_panes(3, 0)

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

    def _write_airline_ops_compare(self, workbook, df: pd.DataFrame):
        sheet = workbook.add_worksheet("Airline Ops Compare")
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
        fmt_legend_key = workbook.add_format({"bold": True, "font_name": "Segoe UI", "font_size": cfg["header"], "border": 1, "bg_color": "#F2F2F2", "align": "center"})

        airline_theme = {
            # BG (Biman Bangladesh): bold crimson + white + subtle gray
            "BG": {
                "header_bg": "#C8102E",
                "subheader_bg": "#FFFFFF",
                "cell_bg": "#F7F7F8",
                "header_font": "#FFFFFF",
                "text_font": "#2F2F2F",
            },
            # VQ (NOVOAIR): logo-led deep blue + vibrant orange
            "VQ": {
                "header_bg": "#003A70",
                "subheader_bg": "#F58220",
                "cell_bg": "#FFF3E8",
                "header_font": "#FFFFFF",
                "text_font": "#123A6E",
            },
            "DEFAULT": {
                "header_bg": "#EAEAEA",
                "subheader_bg": "#F5F5F5",
                "cell_bg": "#FFFFFF",
                "header_font": "#333333",
                "text_font": "#333333",
            },
        }

        required = ["airline", "route", "flight_key", "flight_number", "departure_time", "aircraft", "flight_date"]
        if any(c not in df.columns for c in required):
            sheet.write(0, 0, "Airline Ops Compare", fmt_title)
            sheet.write(2, 0, "Insufficient columns to build comparison sheet.", fmt_cell)
            return

        working = df.copy()
        working["airline"] = working["airline"].astype(str).str.upper()
        working["route"] = working["route"].astype(str)
        working["departure_time"] = working["departure_time"].astype(str)
        working["flight_date"] = pd.to_datetime(working["flight_date"], errors="coerce")
        working["aircraft"] = working["aircraft"].fillna("Aircraft NA").astype(str)
        if "departure" in working.columns:
            working["departure"] = pd.to_datetime(working["departure"], errors="coerce")
        if "arrival" in working.columns:
            working["arrival"] = pd.to_datetime(working["arrival"], errors="coerce")

        airlines = sorted([a for a in working["airline"].dropna().unique() if str(a).strip()])
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
        legend_note = "LB = peak concurrent flights from observed search results (lower bound, not exact fleet). Fleet Coverage = LB / Website Fleet Count."
        sheet.merge_range(3, 0, 3, max(6, legend_col + 2), legend_note, fmt_note)
        self._write_methodology_note(sheet, 0, 10, workbook)

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
            "Observed Daily Flights (avg)",
            "Observed Weekly Flights (est)",
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
                first_dep = dep_times[0] if dep_times else "—"
                last_dep = dep_times[-1] if dep_times else "—"
                route_n = int(sub["route"].nunique())
                day_n = int(sub["flight_date"].dropna().nunique())
                obs_rows = int(len(sub))
                daily_avg = round(obs_rows / day_n, 2) if day_n else 0.0
                weekly_est = round(daily_avg * 7.0, 2)

                known_rows = inventory_map.get(airline, [])
                known_types = self._join_limited([r.get("aircraft_type") for r in known_rows], limit=8) if known_rows else "—"
                known_count = sum(int(r.get("aircraft_count") or 0) for r in known_rows) if known_rows else None
                search_fleet_lb = self._peak_concurrent_flights_lower_bound(sub)
                fleet_coverage_pct = None
                if known_count and known_count > 0 and search_fleet_lb is not None:
                    fleet_coverage_pct = round((float(search_fleet_lb) / float(known_count)) * 100.0, 2)
                known_caps = self._join_limited(
                    sorted({str(r.get("seats_per_aircraft")) for r in known_rows if r.get("seats_per_aircraft")}),
                    limit=8,
                ) if known_rows else "—"

                value = "—"
                if metric == "Known Fleet Types":
                    value = known_types
                elif metric == "Website Fleet Count":
                    value = known_count if known_count is not None else "—"
                elif metric == "Search Fleet (LB)":
                    value = search_fleet_lb if search_fleet_lb is not None else "?"
                elif metric == "Fleet Coverage (LB/Website)":
                    value = f"{fleet_coverage_pct:.2f}%" if fleet_coverage_pct is not None else "?"
                elif metric == "Known Seat Capacities":
                    value = known_caps
                elif metric == "Observed Routes":
                    value = route_n
                elif metric == "Observed Daily Flights (avg)":
                    value = daily_avg
                elif metric == "Observed Weekly Flights (est)":
                    value = weekly_est
                elif metric == "First/Last Departure":
                    value = f"{first_dep} - {last_dep}"

                if metric in {"Website Fleet Count", "Search Fleet (LB)", "Fleet Coverage (LB/Website)", "Observed Routes", "Observed Daily Flights (avg)", "Observed Weekly Flights (est)"}:
                    fmt = fmt_airline_center.get(airline, fmt_center)
                else:
                    fmt = fmt_airline_cell.get(airline, fmt_cell)
                sheet.write(row, 1 + idx, value, fmt)
            row += 1

        fleet_end_row = row - 1
        for idx in range(len(airlines)):
            col = 1 + idx
            # Highlight comparative magnitude on frequency rows
            daily_row = fleet_metric_row_index.get("Observed Daily Flights (avg)")
            weekly_row = fleet_metric_row_index.get("Observed Weekly Flights (est)")
            if daily_row is not None and weekly_row is not None:
                sheet.conditional_format(daily_row, col, weekly_row, col, {"type": "3_color_scale"})

        # -------------------------
        # Section B: Route comparison side-by-side
        # -------------------------
        row += 2
        sheet.write(row, 0, "B) Route Operations (Side-by-Side)", fmt_section)
        row += 1

        grp = (
            working.groupby(["airline", "route"], as_index=False)
            .agg(
                operating_days=("flight_date", lambda s: int(s.dropna().nunique())),
                observed_flights=("flight_key", "count"),
                timings=("departure_time", lambda s: sorted({str(v) for v in s if str(v).strip()})),
                aircraft_types=("aircraft", lambda s: sorted({str(v) for v in s if str(v).strip()})),
            )
        )
        route_rows = sorted(grp["route"].dropna().unique().tolist())

        # two header rows: airline group + metric
        sheet.write(row, 0, "Route", fmt_header)
        col = 1
        for airline in airlines:
            sheet.merge_range(row, col, row, col + 3, airline, fmt_airline_header.get(airline, fmt_header))
            col += 4
        row += 1
        sheet.write(row, 0, "Route", fmt_header)
        col = 1
        metric_labels = ["Daily", "Weekly", "Days", "Timings"]
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
                    daily_avg = ""
                    weekly_est = ""
                    op_days = ""
                    timings = ""
                else:
                    rec = m.iloc[0]
                    op_days = int(rec["operating_days"]) if pd.notna(rec["operating_days"]) else 0
                    observed = int(rec["observed_flights"]) if pd.notna(rec["observed_flights"]) else 0
                    daily_avg = round(observed / op_days, 2) if op_days else 0.0
                    weekly_est = round(daily_avg * 7.0, 2)
                    timings = self._join_limited(rec["timings"], limit=8)
                sheet.write(row, col, daily_avg, fmt_airline_center.get(airline, fmt_center))
                sheet.write(row, col + 1, weekly_est, fmt_airline_center.get(airline, fmt_center))
                sheet.write(row, col + 2, op_days, fmt_airline_center.get(airline, fmt_center))
                sheet.write(row, col + 3, timings, fmt_airline_cell.get(airline, fmt_cell))
                col += 4
            row += 1
        routes_end_row = row - 1

        # color-scale per airline daily/weekly columns
        for idx in range(len(airlines)):
            base = 1 + idx * 4
            if routes_end_row >= routes_start_row:
                sheet.conditional_format(routes_start_row, base, routes_end_row, base, {"type": "3_color_scale"})
                sheet.conditional_format(routes_start_row, base + 1, routes_end_row, base + 1, {"type": "3_color_scale"})

        # Column sizing and freeze
        sheet.set_column(0, 0, 16)
        col = 1
        for _airline in airlines:
            sheet.set_column(col, col, 10)       # Daily
            sheet.set_column(col + 1, col + 1, 10)  # Weekly
            sheet.set_column(col + 2, col + 2, 8)   # Days
            sheet.set_column(col + 3, col + 3, 28)  # Timings
            col += 4
        sheet.freeze_panes(3, 1)

    def write_route_flight_fare_monitor(self, writer, df: pd.DataFrame):
        # ==============================
        # REQUIRED COLUMNS CHECK
        # ==============================
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
        sheet = workbook.add_worksheet("Route Flight Fare Monitor")
        cfg = self._style_cfg()
        sheet.set_zoom(cfg["zoom"])
        sheet.set_default_row(cfg["default_row"])
        block_border = 2

        airline_theme = {
            # BG (Biman Bangladesh): bold crimson + white + subtle gray
            "BG": {
                "header_bg": "#C8102E",
                "subheader_bg": "#FFFFFF",
                "cell_bg": "#F7F7F8",
                "header_font": "#FFFFFF",
                "text_font": "#2F2F2F",
            },
            # VQ (NOVOAIR): logo-led deep blue + vibrant orange
            "VQ": {
                "header_bg": "#003A70",
                "subheader_bg": "#F58220",
                "cell_bg": "#FFF3E8",
                "header_font": "#FFFFFF",
                "text_font": "#123A6E",
            },
            "DEFAULT": {
                "header_bg": "#EAEAEA",
                "subheader_bg": "#F5F5F5",
                "cell_bg": "#FFFFFF",
                "header_font": "#333333",
                "text_font": "#333333",
            },
        }

        fmt_arrow_up = workbook.add_format({"font_name": "Segoe UI", "font_color": "green", "bold": True, "font_size": cfg["sub"] + 2})
        fmt_arrow_down = workbook.add_format({"font_name": "Segoe UI", "font_color": "red", "bold": True, "font_size": cfg["sub"] + 2})

        fmt_sub = workbook.add_format({"font_name": "Segoe UI", "font_script": 2, "font_size": cfg["sub"], "bold": True})
        fmt_sub_soldout = workbook.add_format({"font_name": "Segoe UI", "font_script": 2, "font_size": cfg["sub"], "bold": True, "italic": True, "font_color": "#777777"})
        fmt_sub_new = workbook.add_format({"font_name": "Segoe UI", "font_script": 2, "font_size": cfg["sub"], "bold": True, "italic": True, "font_color": "#1F4BD8"})

        fmt_route = workbook.add_format({"font_name": "Segoe UI", "bold": True, "font_size": cfg["title"]})
        fmt_route_leader_default = workbook.add_format(
            {"font_name": "Segoe UI", "font_size": cfg["body"], "bold": True, "align": "left", "valign": "vcenter", "text_wrap": True, "bg_color": "#F2F2F2", "border": 1}
        )
        fmt_header = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["header"], "bold": True, "border": 1, "align": "center", "valign": "vcenter"})
        fmt_cell = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "align": "center"})
        fmt_gray = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "align": "center", "font_color": "#777777"})
        fmt_date_row = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "align": "center", "bg_color": "#FAFAFA"})
        fmt_legend_key = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["header"], "bold": True, "border": 1, "bg_color": "#F2F2F2", "align": "center"})
        fmt_tag_new = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "bold": True, "italic": True, "font_color": "#1F4BD8", "border": 1, "align": "center"})
        fmt_tag_soldout = workbook.add_format({"font_name": "Segoe UI", "font_size": cfg["body"], "bold": True, "italic": True, "font_color": "#777777", "border": 1, "align": "center"})
        fmt_date_row_bottom = workbook.add_format(
            {
                "font_name": "Segoe UI",
                "font_size": cfg["body"],
                "border": 1,
                "align": "center",
                "bg_color": "#FAFAFA",
                "bottom": block_border,
            }
        )

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
            fmt_header_airline[code] = workbook.add_format(
                {
                    "font_name": "Segoe UI",
                    "font_size": cfg["header"],
                    "bold": True,
                    "border": 1,
                    "left": block_border,
                    "right": block_border,
                    "top": block_border,
                    "align": "center",
                    "bg_color": t["header_bg"],
                    "font_color": t["header_font"],
                }
            )
            fmt_metric_airline[code] = workbook.add_format(
                {"font_name": "Segoe UI", "font_size": cfg["header"], "bold": True, "border": 1, "align": "center", "bg_color": t["subheader_bg"], "font_color": t["text_font"]}
            )
            fmt_metric_airline_left[code] = workbook.add_format(
                {"font_name": "Segoe UI", "font_size": cfg["header"], "bold": True, "border": 1, "left": block_border, "align": "center", "bg_color": t["subheader_bg"], "font_color": t["text_font"]}
            )
            fmt_metric_airline_right[code] = workbook.add_format(
                {"font_name": "Segoe UI", "font_size": cfg["header"], "bold": True, "border": 1, "right": block_border, "align": "center", "bg_color": t["subheader_bg"], "font_color": t["text_font"]}
            )
            fmt_cell_airline[code] = workbook.add_format(
                {"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "align": "center", "bg_color": t["cell_bg"], "font_color": t["text_font"]}
            )
            fmt_cell_airline_left[code] = workbook.add_format(
                {"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "left": block_border, "align": "center", "bg_color": t["cell_bg"], "font_color": t["text_font"]}
            )
            fmt_cell_airline_right[code] = workbook.add_format(
                {"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "right": block_border, "align": "center", "bg_color": t["cell_bg"], "font_color": t["text_font"]}
            )
            fmt_gray_airline[code] = workbook.add_format(
                {"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "align": "center", "bg_color": t["cell_bg"], "font_color": "#777777"}
            )
            fmt_gray_airline_left[code] = workbook.add_format(
                {"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "left": block_border, "align": "center", "bg_color": t["cell_bg"], "font_color": "#777777"}
            )
            fmt_gray_airline_right[code] = workbook.add_format(
                {"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "right": block_border, "align": "center", "bg_color": t["cell_bg"], "font_color": "#777777"}
            )
            fmt_cell_airline_bottom[code] = workbook.add_format(
                {"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "bottom": block_border, "align": "center", "bg_color": t["cell_bg"], "font_color": t["text_font"]}
            )
            fmt_cell_airline_left_bottom[code] = workbook.add_format(
                {"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "left": block_border, "bottom": block_border, "align": "center", "bg_color": t["cell_bg"], "font_color": t["text_font"]}
            )
            fmt_cell_airline_right_bottom[code] = workbook.add_format(
                {"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "right": block_border, "bottom": block_border, "align": "center", "bg_color": t["cell_bg"], "font_color": t["text_font"]}
            )
            fmt_gray_airline_bottom[code] = workbook.add_format(
                {"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "bottom": block_border, "align": "center", "bg_color": t["cell_bg"], "font_color": "#777777"}
            )
            fmt_gray_airline_left_bottom[code] = workbook.add_format(
                {"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "left": block_border, "bottom": block_border, "align": "center", "bg_color": t["cell_bg"], "font_color": "#777777"}
            )
            fmt_gray_airline_right_bottom[code] = workbook.add_format(
                {"font_name": "Segoe UI", "font_size": cfg["body"], "border": 1, "right": block_border, "bottom": block_border, "align": "center", "bg_color": t["cell_bg"], "font_color": "#777777"}
            )
            fmt_route_leader_airline[code] = workbook.add_format(
                {"font_name": "Segoe UI", "font_size": cfg["body"], "bold": True, "align": "left", "valign": "vcenter", "text_wrap": True, "bg_color": t["subheader_bg"], "font_color": t["text_font"], "border": 1}
            )

        df = df.sort_values(["route", "flight_date", "departure_time"])
        if "day_name" not in df.columns:
            df["day_name"] = pd.to_datetime(df["flight_date"]).dt.day_name()

        sheet.set_column(0, 0, 12)
        sheet.set_column(1, 1, 10)

        sheet.write(0, 0, "Route Flight Fare Monitor", fmt_route)
        sheet.write(1, 0, "Legend", fmt_legend_key)
        sheet.write(1, 1, "BG", fmt_header_airline["BG"])
        sheet.write(1, 2, "VQ", fmt_header_airline["VQ"])
        sheet.write(1, 3, "↑ Increase", fmt_cell)
        sheet.write(1, 4, "↓ Decrease", fmt_cell)
        sheet.write(1, 5, "NEW", fmt_tag_new)
        sheet.write(1, 6, "SOLD OUT", fmt_tag_soldout)
        sheet.write(1, 7, "— Unknown", fmt_gray)
        sheet.set_row(1, cfg["legend_row"])
        max_flights_any_route = int(df.groupby("route")["flight_key"].nunique().max()) if not df.empty else 1
        max_flight_cols = max(1, max_flights_any_route * 5)
        note_start_col = max(9, 2 + max_flight_cols + 2)
        self._write_methodology_note(sheet, 0, note_start_col, workbook)

        row = 3
        route_sep = "\u2013"
        leader_sep = "\u2014"

        for route, route_df in df.groupby("route", sort=False):
            route_display = str(route).replace("-", route_sep)
            sheet.write(row, 0, route_display, fmt_route)

            leader_df = route_df[route_df["leader"] & route_df["min_fare"].notna()]
            flights = (
                route_df.groupby("flight_key", as_index=False)
                .first()[["flight_key", "airline", "flight_number", "aircraft", "departure_time"]]
                .sort_values("departure_time")
            )

            total_flight_cols = max(1, len(flights) * 5)
            # Keep leader merge width aligned with actual displayed flight columns.
            leader_end_col = 1 + total_flight_cols
            if leader_df.empty:
                leader_txt = "Route Price Leader (Lowest Fare): —"
                leader_fmt = fmt_route_leader_default
            else:
                lr = leader_df.sort_values("min_fare").iloc[0]
                code = self._flight_code_label(lr.airline, lr.flight_number)
                leader_txt = f"Route Price Leader (Lowest Fare): {code} {leader_sep} {int(lr.min_fare):,}"
                leader_airline = str(getattr(lr, "airline", "") or "").upper()
                leader_fmt = fmt_route_leader_airline.get(leader_airline, fmt_route_leader_default)
            sheet.merge_range(row, 1, row, leader_end_col, leader_txt, leader_fmt)
            sheet.set_row(row, cfg["route_title_row"])
            row += 1

            sheet.merge_range(row, 0, row + 2, 0, "Date", fmt_header)
            sheet.merge_range(row, 1, row + 2, 1, "Day", fmt_header)
            col_map = {}
            col_airline = {}
            col = 2
            for _, f in flights.iterrows():
                aircraft = f.aircraft if pd.notna(f.aircraft) else "Aircraft NA"
                code = self._flight_code_label(f.airline, f.flight_number)
                header = f"{code} | {aircraft}"
                airline_code = str(f.airline or "").upper()
                theme = airline_code if airline_code in fmt_header_airline else "DEFAULT"
                sheet.merge_range(row, col, row, col + 4, header, fmt_header_airline[theme])
                col_map[f.flight_key] = col
                col_airline[f.flight_key] = airline_code
                for wcol in range(col, col + 5):
                    sheet.set_column(wcol, wcol, 13)
                col += 5
            row += 1

            for _, f in flights.iterrows():
                start_col = col_map[f.flight_key]
                dep_txt = str(f.departure_time) if pd.notna(f.departure_time) else ""
                airline_code = str(f.airline or "").upper()
                theme = airline_code if airline_code in fmt_header_airline else "DEFAULT"
                sheet.merge_range(row, start_col, row, start_col + 4, dep_txt, fmt_header_airline[theme])
            row += 1

            metrics = ["Min Fare", "Max Fare", "Tax Amount", "Open/Cap", "Inv Press"]
            for fk, start_col in col_map.items():
                airline_code = col_airline.get(fk, "")
                theme = airline_code if airline_code in fmt_metric_airline else "DEFAULT"
                for i, m in enumerate(metrics):
                    if i == 0:
                        metric_fmt = fmt_metric_airline_left[theme]
                    elif i == 4:
                        metric_fmt = fmt_metric_airline_right[theme]
                    else:
                        metric_fmt = fmt_metric_airline[theme]
                    sheet.write(row, start_col + i, m, metric_fmt)
            row += 1

            day_groups = list(route_df.groupby(["flight_date", "day_name"], sort=True))
            for day_idx, ((date, day), day_df) in enumerate(day_groups):
                is_last_day = day_idx == (len(day_groups) - 1)
                date_fmt = fmt_date_row_bottom if is_last_day else fmt_date_row
                sheet.write(row, 0, str(date), date_fmt)
                sheet.write(row, 1, day, date_fmt)

                # Ensure visible block borders for every flight group even when row has missing values.
                for fk, start_col in col_map.items():
                    airline_code = str(col_airline.get(fk, "") or "").upper()
                    theme = airline_code if airline_code in fmt_cell_airline else "DEFAULT"
                    n_o_left = fmt_gray_airline_left_bottom[theme] if is_last_day else fmt_gray_airline_left[theme]
                    n_o_mid = fmt_gray_airline_bottom[theme] if is_last_day else fmt_gray_airline[theme]
                    n_o_right = fmt_gray_airline_right_bottom[theme] if is_last_day else fmt_gray_airline_right[theme]
                    sheet.write(row, start_col + 0, "N/O", n_o_left)
                    sheet.write(row, start_col + 1, "—", n_o_mid)
                    sheet.write(row, start_col + 2, "—", n_o_mid)
                    sheet.write(row, start_col + 3, "— / —", n_o_mid)
                    sheet.write(row, start_col + 4, "—", n_o_right)

                for _, r in day_df.iterrows():
                    base = col_map.get(r.flight_key)
                    if base is None:
                        continue
                    airline_code = str(r.airline or col_airline.get(r.flight_key, "")).upper()
                    theme = airline_code if airline_code in fmt_cell_airline else "DEFAULT"
                    cell_fmt_left = fmt_cell_airline_left_bottom[theme] if is_last_day else fmt_cell_airline_left[theme]
                    cell_fmt_mid = fmt_cell_airline_bottom[theme] if is_last_day else fmt_cell_airline[theme]
                    cell_fmt_right = fmt_cell_airline_right_bottom[theme] if is_last_day else fmt_cell_airline_right[theme]
                    gray_fmt_left = fmt_gray_airline_left_bottom[theme] if is_last_day else fmt_gray_airline_left[theme]
                    gray_fmt_mid = fmt_gray_airline_bottom[theme] if is_last_day else fmt_gray_airline[theme]
                    gray_fmt_right = fmt_gray_airline_right_bottom[theme] if is_last_day else fmt_gray_airline_right[theme]

                    min_fare_int = self._to_int(r.min_fare)
                    min_rbd = str(r.min_rbd)[:1] if pd.notna(r.min_rbd) else ""
                    min_rbd_seats = self._to_int(r.min_rbd_seats)
                    sub = f"{min_rbd}-{min_rbd_seats}" if min_rbd and min_rbd_seats is not None else min_rbd
                    base_price = f"{min_fare_int:,}" if min_fare_int is not None else "—"

                    min_sign = self._delta_sign(r.min_fare_delta)
                    min_arrow = "\u2191" if min_sign > 0 else ("\u2193" if min_sign < 0 else "")
                    min_arrow_fmt = fmt_arrow_up if min_sign > 0 else (fmt_arrow_down if min_sign < 0 else cell_fmt_left)

                    status_txt = ""
                    status_fmt = fmt_sub
                    if r.status == "SOLD OUT":
                        status_txt = " SOLD OUT"
                        status_fmt = fmt_sub_soldout
                    elif r.status == "NEW":
                        status_txt = " NEW"
                        status_fmt = fmt_sub_new

                    rich_parts = [cell_fmt_left, base_price]
                    if sub:
                        rich_parts += [fmt_sub, f"({sub})"]
                    if status_txt:
                        rich_parts += [status_fmt, status_txt]
                    if min_arrow:
                        rich_parts += [min_arrow_fmt, f" {min_arrow}"]
                    rich_parts += [cell_fmt_left]
                    if len(rich_parts) <= 3:
                        sheet.write(row, base, base_price, cell_fmt_left)
                    else:
                        sheet.write_rich_string(row, base, *rich_parts)

                    max_fare_int = self._to_int(r.max_fare)
                    if max_fare_int is not None:
                        max_rbd = str(r.max_rbd)[:1] if pd.notna(r.max_rbd) else ""
                        max_rbd_seats = self._to_int(r.max_rbd_seats)
                        max_sub = f"{max_rbd}-{max_rbd_seats}" if max_rbd and max_rbd_seats is not None else max_rbd
                        max_sign = self._delta_sign(r.max_fare_delta)
                        max_arrow = "\u2191" if max_sign > 0 else ("\u2193" if max_sign < 0 else "")
                        max_arrow_fmt = fmt_arrow_up if max_sign > 0 else (fmt_arrow_down if max_sign < 0 else gray_fmt_mid)
                        parts = [gray_fmt_mid, f"{max_fare_int:,}"]
                        if max_sub:
                            parts += [fmt_sub, f"({max_sub})"]
                        if max_arrow:
                            parts += [max_arrow_fmt, f" {max_arrow}"]
                        parts += [gray_fmt_mid]
                        if len(parts) <= 3:
                            sheet.write(row, base + 1, f"{max_fare_int:,}", gray_fmt_mid)
                        else:
                            sheet.write_rich_string(row, base + 1, *parts)
                    else:
                        sheet.write(row, base + 1, "—", gray_fmt_mid)

                    tax_int = self._to_int(r.current_tax)
                    tax_sign = self._delta_sign(r.tax_delta)
                    tax_arrow = "\u2191" if tax_sign > 0 else ("\u2193" if tax_sign < 0 else "")
                    tax_arrow_fmt = fmt_arrow_up if tax_sign > 0 else (fmt_arrow_down if tax_sign < 0 else gray_fmt_mid)
                    if tax_int is None:
                        sheet.write(row, base + 2, "—", gray_fmt_mid)
                    elif tax_arrow:
                        sheet.write_rich_string(row, base + 2, gray_fmt_mid, f"{tax_int:,}", tax_arrow_fmt, f" {tax_arrow}", gray_fmt_mid)
                    else:
                        sheet.write(row, base + 2, f"{tax_int:,}", gray_fmt_mid)

                    min_seat_int = self._to_int(r.min_seats)
                    max_seat_int = self._to_int(r.max_seats)
                    seat_sign = self._delta_sign(r.seat_delta)
                    seat_arrow = "\u2191" if seat_sign > 0 else ("\u2193" if seat_sign < 0 else "")
                    seat_arrow_fmt = fmt_arrow_up if seat_sign > 0 else (fmt_arrow_down if seat_sign < 0 else cell_fmt_mid)
                    if min_seat_int is None and max_seat_int is None:
                        sheet.write(row, base + 3, "— / —", gray_fmt_mid)
                    elif min_seat_int is None:
                        sheet.write(row, base + 3, f"— / {max_seat_int}", gray_fmt_mid)
                    elif max_seat_int is None:
                        sheet.write(row, base + 3, f"{min_seat_int} / —", gray_fmt_mid)
                    elif seat_arrow:
                        sheet.write_rich_string(
                            row,
                            base + 3,
                            cell_fmt_mid,
                            f"{min_seat_int} / {max_seat_int}",
                            seat_arrow_fmt,
                            f" {seat_arrow}",
                            cell_fmt_mid,
                        )
                    else:
                        sheet.write(row, base + 3, f"{min_seat_int} / {max_seat_int}", cell_fmt_mid)

                    load_int = self._to_int(r.load_pct)
                    load_sign = self._delta_sign(r.load_delta)
                    load_arrow = "\u2191" if load_sign > 0 else ("\u2193" if load_sign < 0 else "")
                    load_arrow_fmt = fmt_arrow_up if load_sign > 0 else (fmt_arrow_down if load_sign < 0 else gray_fmt_right)
                    if load_int is None:
                        sheet.write(row, base + 4, "—", gray_fmt_right)
                    elif load_arrow:
                        sheet.write_rich_string(row, base + 4, gray_fmt_right, f"{load_int}%", load_arrow_fmt, f" {load_arrow}", gray_fmt_right)
                    else:
                        sheet.write(row, base + 4, f"{load_int}%", gray_fmt_right)

                row += 1

        sheet.freeze_panes(3, 2)
        self._write_airline_ops_compare(workbook, df)
        self._write_changes_summary(workbook, df)
        self._write_fare_trend_sparklines(workbook, df)
