import pandas as pd

def adapt_comparison_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    # Adapter MUST NOT change logic
    # Only light reshaping / safety
    df["day_name"] = pd.to_datetime(df["flight_date"]).dt.day_name()

    required = [
        "route", "flight_key", "flight_date", "departure_time",
        "airline", "flight_number", "aircraft",
        "min_fare", "max_fare", "current_tax",
        "min_seats", "max_seats", "load_pct",
        "status", "leader", "row_visible",
        "seat_arrow", "tax_arrow", "rbd"
    ]

    for c in required:
        if c not in df.columns:
            df[c] = pd.NA

    return df
