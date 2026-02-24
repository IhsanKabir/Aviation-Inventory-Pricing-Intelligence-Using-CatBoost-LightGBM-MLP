import pandas as pd


class RouteIntelligence:
    """
    Route-level synthesis engine.
    NO raw price access.
    NO re-computation of state.
    """

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        required_cols = {
            "origin",
            "destination",
            "flight_number",
            "price_trend",
            "price_volatility",
            "seat_pressure_index",
            "is_price_leader",
        }
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"RouteIntelligence missing columns: {missing}")

        route_df = (
            df.groupby(["origin", "destination"], as_index=False)
              .agg(
                  flight_count=("flight_number", "nunique"),
                  leader_count=("is_price_leader", "sum"),
                  avg_price_trend=("price_trend", "mean"),
                  avg_price_volatility=("price_volatility", "mean"),
                  avg_seat_pressure=("seat_pressure_index", "mean"),
              )
        )

        # Leadership concentration (dominance proxy)
        route_df["leader_concentration"] = (
            route_df["leader_count"] / route_df["flight_count"]
        )

        # Composite pressure score (v4-safe)
        route_df["route_pressure_score"] = (
            route_df["avg_seat_pressure"].abs()
            * (1 + route_df["avg_price_volatility"])
        )

        return route_df


def detect_route_regime(route_row: pd.Series) -> str:
    """
    Route-level regime classifier.
    Deterministic, interpretable, publishable.
    """

    if (
        route_row["leader_concentration"] > 0.8
        and route_row["avg_seat_pressure"] > 0.7
    ):
        return "DOMINANT_SUPPLY_CONTROL"

    if (
        route_row["avg_price_trend"] < 0
        and route_row["avg_price_volatility"] > 0.4
    ):
        return "PRICE_WAR"

    if (
        route_row["avg_seat_pressure"] < 0.4
        and route_row["avg_price_trend"] < 0
    ):
        return "CAPACITY_EXPANSION"

    return "NORMAL"
