
REQUIRED_COLUMNS = {
    "airline": str,
    "origin": str,
    "destination": str,
    "flight_number": str,
    "departure": "datetime64[ns]",
    "cabin": str,
    "brand": str,
    "price_total_bdt": float,
    "seat_available": int,
}

OPTIONAL_COLUMNS = {
    "fare_basis": str,
    "aircraft": str,
}