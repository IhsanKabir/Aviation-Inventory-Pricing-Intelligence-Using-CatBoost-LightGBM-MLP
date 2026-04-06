# engines/scrape_context.py
from sqlalchemy import text
from typing import List, Dict, Iterable
from datetime import datetime

class ScrapeContext:
    def __init__(self, engine):
        self.engine = engine

    @staticmethod
    def _normalize_airline_codes(airline_codes: Iterable[str] | None) -> list[str]:
        if not airline_codes:
            return []
        out = []
        seen = set()
        for code in airline_codes:
            c = str(code or "").strip().upper()
            if not c or c in seen:
                continue
            seen.add(c)
            out.append(c)
        return out

    def get_latest_two_scrapes(self, airline_codes: Iterable[str] | None = None):
        airline_codes = self._normalize_airline_codes(airline_codes)
        where_sql = ""
        params = {}
        if airline_codes:
            where_sql = "WHERE airline = ANY(:airline_codes)"
            params["airline_codes"] = airline_codes

        sql = text(f"""
            SELECT scrape_id
            FROM flight_offers
            {where_sql}
            GROUP BY scrape_id
            ORDER BY MAX(scraped_at) DESC
        """)

        with self.engine.connect() as conn:
            rows = conn.execute(sql, params).fetchall()

        if len(rows) < 2:
            raise RuntimeError("Need at least two scrapes")

        return rows[0][0], rows[1][0]

    def get_latest_two_full_scrapes(
        self,
        lookback: int = 40,
        min_rows_floor: int = 100,
        min_full_ratio: float = 0.30,
        airline_codes: Iterable[str] | None = None,
    ):
        """
        Prefer the latest two "full" scrapes and skip tiny test/incomplete scrapes.

        A scrape is treated as full when:
          for each target airline:
              airline_row_count >= clamp(
                  max(min_rows_floor, int(max_airline_row_count_in_lookback * min_full_ratio)),
                  to max_airline_row_count_in_lookback
              )

        Target airlines:
          - explicit ``airline_codes`` when provided
          - otherwise all airlines observed in the lookback window

        The method may expand the lookback window internally (up to at least 200)
        before falling back, so recent probe/test scrapes do not crowd out the
        latest full scrape pair for a target airline.

        Falls back to latest two scrapes if fewer than two satisfy the threshold.
        """
        airline_codes = self._normalize_airline_codes(airline_codes)
        summary_sql = text(
            """
            SELECT
                scrape_id,
                MAX(scraped_at) AS max_scraped_at,
                COUNT(*) AS row_count
            FROM flight_offers
            GROUP BY scrape_id
            ORDER BY MAX(scraped_at) DESC
            LIMIT :lookback
            """
        )

        def _load_recent_summary_rows(limit: int):
            with self.engine.connect() as conn:
                return conn.execute(summary_sql, {"lookback": int(limit)}).fetchall()

        def _per_airline_full_ids(summary_rows):
            if len(summary_rows) < 2:
                return []

            recent_scrape_ids = [r[0] for r in summary_rows]
            airline_where = ""
            params = {"scrape_ids": recent_scrape_ids}
            if airline_codes:
                airline_where = " AND airline = ANY(:airline_codes)"
                params["airline_codes"] = airline_codes

            per_airline_sql = text(
                f"""
                SELECT
                    scrape_id,
                    airline,
                    COUNT(*) AS row_count
                FROM flight_offers
                WHERE scrape_id = ANY(:scrape_ids)
                  {airline_where}
                GROUP BY scrape_id, airline
                """
            )

            with self.engine.connect() as conn:
                airline_rows = conn.execute(per_airline_sql, params).fetchall()

            counts_by_scrape: Dict[object, Dict[str, int]] = {sid: {} for sid in recent_scrape_ids}
            max_rows_by_airline: Dict[str, int] = {}
            for scrape_id, airline, row_count in airline_rows:
                a = str(airline or "").upper()
                c = int(row_count or 0)
                if not a:
                    continue
                counts_by_scrape.setdefault(scrape_id, {})[a] = c
                if c > max_rows_by_airline.get(a, 0):
                    max_rows_by_airline[a] = c

            target_airlines = airline_codes or sorted(max_rows_by_airline.keys())
            if not target_airlines:
                return []

            thresholds: Dict[str, int] = {}
            floor = int(min_rows_floor)
            ratio = float(min_full_ratio)
            for a in target_airlines:
                max_rows = int(max_rows_by_airline.get(a, 0))
                if max_rows <= 0:
                    thresholds[a] = 1
                    continue
                adaptive_min = int(max_rows * ratio)
                raw_threshold = max(floor, adaptive_min)
                thresholds[a] = min(max_rows, raw_threshold)

            full_ids = []
            for scrape_id, _max_scraped_at, _row_count in summary_rows:
                per_airline_counts = counts_by_scrape.get(scrape_id, {})
                if all(int(per_airline_counts.get(a, 0)) >= thresholds[a] for a in target_airlines):
                    full_ids.append(scrape_id)
            return full_ids

        base_lookback = max(2, int(lookback or 0))
        effective_lookback = max(base_lookback, 200)

        rows = _load_recent_summary_rows(effective_lookback)
        if len(rows) >= 2:
            full_ids = _per_airline_full_ids(rows)
            if len(full_ids) >= 2:
                return full_ids[0], full_ids[1]

        if len(rows) < 2:
            raise RuntimeError("Need at least two scrapes")

        # Legacy/global fallback when per-airline thresholds do not yield a pair.
        max_rows = max(int(r[2] or 0) for r in rows) if rows else 0
        adaptive_min = int(max_rows * float(min_full_ratio))
        threshold = max(int(min_rows_floor), adaptive_min)
        full_ids = [r[0] for r in rows if int(r[2] or 0) >= threshold]
        if len(full_ids) >= 2:
            return full_ids[0], full_ids[1]

        # Fallback: keep legacy behavior
        return rows[0][0], rows[1][0]

    def get_last_n_scrapes(self, n: int) -> list[int]:
        sql = text("""
            SELECT scrape_id
            FROM flight_offers
            GROUP BY scrape_id
            ORDER BY scrape_id DESC
            LIMIT :n
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(sql, {"n": n}).fetchall()
        return [r[0] for r in rows][::-1]

    def get_scrape_time_map(self, scrape_ids: List[int]) -> Dict[int, datetime]:
        """
        Returns {scrape_id: scraped_at_utc}
        Used for presentation (date/day columns).
        """
        if not scrape_ids:
            return {}

        normalized_ids = [str(scrape_id) for scrape_id in scrape_ids if scrape_id]
        if not normalized_ids:
            return {}

        sql = text("""
            SELECT scrape_id::text AS scrape_id, MAX(scraped_at) AS scraped_at
            FROM flight_offers
            WHERE scrape_id::text = ANY(:scrape_ids)
            GROUP BY scrape_id
        """)

        with self.engine.connect() as conn:
            rows = conn.execute(sql, {"scrape_ids": normalized_ids}).fetchall()

        return {r[0]: r[1] for r in rows}

