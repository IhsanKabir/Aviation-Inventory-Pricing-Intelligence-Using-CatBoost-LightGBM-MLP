# engines/scrape_context.py
from sqlalchemy import text
from typing import List, Dict
from datetime import datetime

class ScrapeContext:
    def __init__(self, engine):
        self.engine = engine

    def get_latest_two_scrapes(self):
        sql = text("""
            SELECT scrape_id
            FROM flight_offers
            GROUP BY scrape_id
            ORDER BY MAX(scraped_at) DESC
        """)

        with self.engine.connect() as conn:
            rows = conn.execute(sql).fetchall()

        if len(rows) < 2:
            raise RuntimeError("Need at least two scrapes")

        return rows[0][0], rows[1][0]

    def get_latest_two_full_scrapes(
        self,
        lookback: int = 40,
        min_rows_floor: int = 100,
        min_full_ratio: float = 0.30,
    ):
        """
        Prefer the latest two "full" scrapes and skip tiny test/incomplete scrapes.

        A scrape is treated as full when:
          row_count >= max(min_rows_floor, int(max_row_count_in_lookback * min_full_ratio))

        Falls back to latest two scrapes if fewer than two satisfy the threshold.
        """
        sql = text(
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

        with self.engine.connect() as conn:
            rows = conn.execute(sql, {"lookback": lookback}).fetchall()

        if len(rows) < 2:
            raise RuntimeError("Need at least two scrapes")

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

        sql = text("""
            SELECT scrape_id, MAX(scraped_at) AS scraped_at
            FROM flight_offers
            WHERE scrape_id = ANY(:scrape_ids)
            GROUP BY scrape_id
        """)

        with self.engine.connect() as conn:
            rows = conn.execute(sql, {"scrape_ids": scrape_ids}).fetchall()

        return {r[0]: r[1] for r in rows}

