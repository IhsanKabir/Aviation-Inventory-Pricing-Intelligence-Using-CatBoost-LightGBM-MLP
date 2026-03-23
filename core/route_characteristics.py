"""
Route characteristics feature engineering for aviation inventory prediction.

Captures route-specific attributes like distance, type, hub-spoke configuration,
and competition level to improve prediction accuracy.
"""

import json
from pathlib import Path
import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt


# Airport coordinates (major airports relevant to Bangladesh aviation)
AIRPORT_COORDINATES = {
    "DAC": (23.8433, 90.3978),  # Dhaka
    "CXB": (21.4522, 91.9639),  # Cox's Bazar
    "JSR": (23.1838, 89.1608),  # Jessore
    "DXB": (25.2528, 55.3644),  # Dubai
    "DOH": (25.2731, 51.6080),  # Doha
    "AUH": (24.4330, 54.6511),  # Abu Dhabi
    "RUH": (24.9578, 46.6988),  # Riyadh
    "JED": (21.6796, 39.1566),  # Jeddah
    "MCT": (23.5933, 58.2844),  # Muscat
    "KWI": (29.2267, 47.9689),  # Kuwait
    "BAH": (26.2708, 50.6336),  # Bahrain
    "BKK": (13.6900, 100.7501), # Bangkok
    "SIN": (1.3644, 103.9915),  # Singapore
    "KUL": (2.7456, 101.7099),  # Kuala Lumpur
    "DEL": (28.5562, 77.1000),  # Delhi
    "BOM": (19.0896, 72.8656),  # Mumbai
    "CCU": (22.6520, 88.4463),  # Kolkata
    "MAA": (12.9941, 80.1709),  # Chennai
    "CMB": (7.1808, 79.8841),   # Colombo
    "HKG": (22.3080, 113.9185), # Hong Kong
    "ICN": (37.4602, 126.4407), # Seoul Incheon
    "NRT": (35.7720, 140.3929), # Tokyo Narita
    "IST": (40.9769, 28.8146),  # Istanbul
}


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate great circle distance between two points in kilometers.

    Args:
        lat1, lon1: Latitude and longitude of first point
        lat2, lon2: Latitude and longitude of second point

    Returns:
        float: Distance in kilometers
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers
    return c * r


def load_route_config():
    """Load route characteristics configuration from JSON file."""
    config_path = Path(__file__).parent.parent / "config" / "route_characteristics.json"
    if config_path.exists():
        with open(config_path) as f:
            return json.load(f)
    return {"hubs": {}, "route_types": {}}


def calculate_route_distance(origin: str, destination: str):
    """
    Calculate distance between two airports.

    Args:
        origin: IATA code of origin airport
        destination: IATA code of destination airport

    Returns:
        float: Distance in kilometers, or None if airports not found
    """
    if origin not in AIRPORT_COORDINATES or destination not in AIRPORT_COORDINATES:
        return None

    lat1, lon1 = AIRPORT_COORDINATES[origin]
    lat2, lon2 = AIRPORT_COORDINATES[destination]

    return haversine_distance(lat1, lon1, lat2, lon2)


def classify_route_type(distance_km: float):
    """
    Classify route based on distance.

    Args:
        distance_km: Route distance in kilometers

    Returns:
        str: Route type (domestic/regional/international_short/long_haul)
    """
    if distance_km is None or pd.isna(distance_km):
        return "unknown"

    if distance_km < 500:
        return "domestic"
    elif distance_km < 2500:
        return "regional"
    elif distance_km < 5000:
        return "international_short"
    else:
        return "long_haul"


def is_hub_airport(airport: str, hub_type: str = "global_major"):
    """
    Check if airport is a hub.

    Args:
        airport: IATA code
        hub_type: Type of hub (global_major/middle_east/south_asia/southeast_asia)

    Returns:
        bool: True if airport is a hub of specified type
    """
    config = load_route_config()
    hubs = config.get("hubs", {}).get(hub_type, [])
    return airport in hubs


def add_route_characteristics(df: pd.DataFrame):
    """
    Add route characteristic features to dataframe.

    Args:
        df: DataFrame with 'origin' and 'destination' columns

    Returns:
        DataFrame with added features:
        - route_distance_km: Distance in kilometers
        - route_type: Categorical (domestic/regional/international_short/long_haul)
        - route_type_code: Numeric encoding (0=unknown, 1=domestic, 2=regional, 3=int_short, 4=long_haul)
        - origin_is_hub: Binary indicator
        - destination_is_hub: Binary indicator
        - is_hub_spoke: Binary indicator (either endpoint is hub)
        - is_hub_to_hub: Binary indicator (both endpoints are hubs)
        - origin_is_middle_east_hub: Binary for Middle East hubs
        - destination_is_middle_east_hub: Binary for Middle East hubs
        - is_bangladesh_domestic: Binary for Bangladesh domestic routes
        - log_route_distance: Log-transformed distance
    """
    df = df.copy()

    # Calculate route distance
    df["route_distance_km"] = df.apply(
        lambda row: calculate_route_distance(row["origin"], row["destination"]),
        axis=1
    )

    # Classify route type
    df["route_type"] = df["route_distance_km"].apply(classify_route_type)

    # Encode route type as numeric
    route_type_encoding = {
        "unknown": 0,
        "domestic": 1,
        "regional": 2,
        "international_short": 3,
        "long_haul": 4
    }
    df["route_type_code"] = df["route_type"].map(route_type_encoding).fillna(0).astype(int)

    # Hub indicators
    df["origin_is_hub"] = df["origin"].apply(lambda x: is_hub_airport(x, "global_major")).astype(int)
    df["destination_is_hub"] = df["destination"].apply(lambda x: is_hub_airport(x, "global_major")).astype(int)
    df["is_hub_spoke"] = ((df["origin_is_hub"] == 1) | (df["destination_is_hub"] == 1)).astype(int)
    df["is_hub_to_hub"] = ((df["origin_is_hub"] == 1) & (df["destination_is_hub"] == 1)).astype(int)

    # Middle East hub indicators (important for Bangladesh aviation market)
    df["origin_is_middle_east_hub"] = df["origin"].apply(lambda x: is_hub_airport(x, "middle_east")).astype(int)
    df["destination_is_middle_east_hub"] = df["destination"].apply(lambda x: is_hub_airport(x, "middle_east")).astype(int)

    # Bangladesh domestic routes
    config = load_route_config()
    bd_airports = config.get("hubs", {}).get("bangladesh", ["DAC", "CXB"])
    df["is_bangladesh_domestic"] = (
        (df["origin"].isin(bd_airports)) & (df["destination"].isin(bd_airports))
    ).astype(int)

    # Log-transformed distance (handles long tail better)
    df["log_route_distance"] = np.log1p(df["route_distance_km"].fillna(0))

    return df


def estimate_competition_level(df: pd.DataFrame, group_cols: list = ["origin", "destination"]):
    """
    Estimate competition level based on number of airlines serving the route.

    Args:
        df: DataFrame with airline, origin, destination columns
        group_cols: Columns to group by for competition calculation

    Returns:
        DataFrame with added features:
        - route_airline_count: Number of airlines serving this route
        - competition_level: Categorical (monopoly/duopoly/competitive/high_competition)
        - competition_level_code: Numeric encoding (0=monopoly, 1=duopoly, 2=competitive, 3=high)
    """
    df = df.copy()

    # Count unique airlines per route
    airline_counts = df.groupby(group_cols)["airline"].nunique().reset_index()
    airline_counts.columns = group_cols + ["route_airline_count"]

    # Merge back
    df = df.merge(airline_counts, on=group_cols, how="left")

    # Classify competition level
    def classify_competition(count):
        if pd.isna(count) or count == 1:
            return "monopoly"
        elif count == 2:
            return "duopoly"
        elif count <= 4:
            return "competitive"
        else:
            return "high_competition"

    df["competition_level"] = df["route_airline_count"].apply(classify_competition)

    # Encode as numeric
    competition_encoding = {
        "monopoly": 0,
        "duopoly": 1,
        "competitive": 2,
        "high_competition": 3
    }
    df["competition_level_code"] = df["competition_level"].map(competition_encoding).fillna(0).astype(int)

    return df


def get_route_characteristics_columns():
    """
    Get list of all route characteristic feature column names.

    Returns:
        list: Column names for route characteristics features
    """
    return [
        "route_distance_km",
        "route_type_code",
        "origin_is_hub",
        "destination_is_hub",
        "is_hub_spoke",
        "is_hub_to_hub",
        "origin_is_middle_east_hub",
        "destination_is_middle_east_hub",
        "is_bangladesh_domestic",
        "log_route_distance",
        "route_airline_count",
        "competition_level_code",
    ]
