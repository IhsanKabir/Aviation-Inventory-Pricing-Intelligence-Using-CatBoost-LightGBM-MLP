import pandas as pd


class TrendEngine:
    """
    Scrape-count based historical behavior engine.
    NO comparison logic.
    NO state logic.
    """

    def __init__(self, n_scrapes: int = 5):
        self.n_scrapes = n_scrapes

    def compute(self, history_df: pd.DataFrame) -> pd.DataFrame:
        required_cols = {
            "scrape_id",
            "price_total_bdt",
            "seat_available",
        }
        missing = required_cols - set(history_df.columns)
        if missing:
            raise ValueError(f"TrendEngine missing required columns: {missing}")

        group_cols = [
            "airline",
            "origin",
            "destination",
            "flight_number",
            "departure",
            "cabin",
            "brand",
        ]

        agg = (
            history_df
            .sort_values("scrape_id")
            .groupby(group_cols)
            .agg(
                price_trend=("price_total_bdt", lambda x: x.iloc[-1] - x.iloc[0]),
                seat_trend=("seat_available", lambda x: x.iloc[-1] - x.iloc[0]),
                price_volatility=("price_total_bdt", "std"),
                seat_volatility=("seat_available", "std"),
            )
            .reset_index()
        )

        return agg
