# engines/time_series_engine.py

import pandas as pd
from sqlalchemy import text

class TimeSeriesEngine:
    """
    Attaches last-N scrape history per identity.
    Scrape-count based, not time-based.
    """

    def __init__(self, engine, identity_cols):
        self.engine = engine
        self.identity_cols = identity_cols

    def load_history(self, scrape_ids: list[str]) -> pd.DataFrame:
        """
        Load rows for the given scrape_ids only.
        """

        sql = text("""
            SELECT
                scrape_id,
                scraped_at,
                airline,
                origin,
                destination,
                flight_number,
                departure,
                cabin,
                brand,
                price_total_bdt,
                fare_basis,
                seat_available,
                seat_capacity
            FROM flight_offers
            WHERE scrape_id = ANY(:scrape_ids)
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(
                sql,
                conn,
                params={"scrape_ids": scrape_ids}
            )

        return df

    def attach_scrape_rank(self, df: pd.DataFrame, scrape_ids: list[str]) -> pd.DataFrame:
        """
        Adds scrape_rank where 1 = most recent scrape
        """

        rank_map = {
            scrape_id: rank + 1
            for rank, scrape_id in enumerate(scrape_ids[::-1])
        }

        df["scrape_rank"] = df["scrape_id"].map(rank_map)

        return df
