import json

def build_payload(origin, destination, date, adults=1):
    payload = {
        "operationName": "bookingAirSearch",
        "variables": {
            "airSearchInput": {
                "cabinClass": "Economy",
                "awardBooking": False,
                "promoCodes": [],
                "searchType": "BRANDED",
                "itineraryParts": [
                    {
                        "from": {"useNearbyLocations": False, "code": origin},
                        "to": {"useNearbyLocations": False, "code": destination},
                        "when": {"date": date}
                    }
                ],
                "passengers": {"ADT": adults}
            }
        },
        "extensions": {},
        "query": "query bookingAirSearch($airSearchInput: CustomAirSearchInput) {\n  bookingAirSearch(airSearchInput: $airSearchInput) {\n    originalResponse\n    __typename\n  }\n}"
    }

    with open("payload.json", "w") as f:
        json.dump(payload, f, indent=2)

    print("Generated payload.json for:", origin, destination, date)


if __name__ == "__main__":
    o = input("Origin (e.g., DAC): ").strip() or "DAC"
    d = input("Destination (e.g., CXB): ").strip() or "JED"
    date = input("Date (YYYY-MM-DD): ").strip() or "2026-01-10"

    build_payload(o.upper(), d.upper(), date)

