import json
import sys
import requests
import pandas as pd

# --------------------------
# Aircraft and Baggage Maps
# --------------------------
AIRCRAFT_MAP = {
    "738": "Boeing 737-800",
    "788": "Boeing 787-8 Dreamliner",
    "789": "Boeing 787-9 Dreamliner",
    "773": "Boeing 777-300ER",
    "AT7": "ATR 72-600",
    "DH8": "Dash-8 Q400"
}

BAGGAGE_RULES = {
    "ZB": "0 kg (Cabin only)",
    "EV": "30–35 kg (Economy Saver)",
    "EF": "30–35 kg (Economy Flexi)",
    "SV": "20 kg (Super Saver)",
    "PE": "35 kg (Premium Economy)",
    "BS": "40–45 kg (Business Saver)",
    "BF": "40–45 kg (Business Flexi)"
}

# --------------------------
# Step 1: Generate Payload
# --------------------------
def build_payload(origin, destination, date):
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
                "passengers": {"ADT": 1}
            }
        },
        "query": "query bookingAirSearch($airSearchInput: CustomAirSearchInput) {\n  bookingAirSearch(airSearchInput: $airSearchInput) {\n    originalResponse\n    __typename\n  }\n}"
    }

    with open("payload.json", "w") as f:
        json.dump(payload, f, indent=2)

    print(f"✔ Payload generated for {origin} → {destination} on {date}")


# --------------------------
# Step 2: Perform API Search
# --------------------------
def search_flights():
    with open("payload.json", "r") as f:
        payload = json.load(f)

    with open("cookies.json", "r") as f:
        cookies_list = json.load(f)

    cookies = {c["name"]: c["value"] for c in cookies_list}

    url = "https://booking.biman-airlines.com/api/graphql"

    headers = {
        "content-type": "application/json",
        "x-sabre-storefront": "BGDX",
        "referer": "https://booking.biman-airlines.com/dx/BGDX/",
        "user-agent": "Mozilla/5.0"
    }

    print("🌐 Fetching live fares...")

    response = requests.post(url, json=payload, headers=headers, cookies=cookies)

    with open("response.json", "w") as f:
        f.write(response.text)

    print(f"✔ API Status: {response.status_code}")
    return response.status_code


# --------------------------
# Step 3: Parse response.json
# --------------------------
def parse_response():
    with open("response.json", "r") as f:
        data = json.load(f)

    try:
        offers = data["data"]["bookingAirSearch"]["originalResponse"]["unbundledOffers"][0]
    except Exception:
        print("❌ No flight data found!")
        return None

    rows = []
    for offer in offers:
        part = offer["itineraryPart"][0]
        seg = part["segments"][0]

        equip = seg.get("equipment")
        aircraft_name = AIRCRAFT_MAP.get(equip, equip)

        rows.append({
            "Carrier": seg["flight"]["airlineCode"],
            "FlightNumber": seg["flight"]["flightNumber"],
            "Aircraft": aircraft_name,
            "Origin": seg["origin"],
            "Destination": seg["destination"],
            "Departure": seg["departure"],
            "Arrival": seg["arrival"],
            "DepTerminal": seg["flight"].get("departureTerminal"),
            "ArrTerminal": seg["flight"].get("arrivalTerminal"),
            "Brand": offer["brandId"],
            "CabinClass": seg.get("cabinClass"),
            "Stops": part.get("stops", 0),
            "DurationMin": seg.get("duration"),
            "BaseFare": offer["fare"]["alternatives"][0][0]["amount"],
            "Tax": offer["taxes"]["alternatives"][0][0]["amount"],
            "TotalFare": offer["total"]["alternatives"][0][0]["amount"],
            "Baggage": BAGGAGE_RULES.get(offer["brandId"], "Unknown")
        })

    df = pd.DataFrame(rows)
    df.to_excel("flights.xlsx", index=False)
    print("✔ flights.xlsx generated!")
    print(df)
    return df


# --------------------------
# Integrated CLI Entry
# --------------------------
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python flight_cli.py ORIGIN DESTINATION DATE")
        print("Example: python flight_cli.py DAC JED 2026-01-12")
        sys.exit(1)

    origin = sys.argv[1]
    dest = sys.argv[2]
    date = sys.argv[3]

    print("=== Biman Flight CLI ===")

    build_payload(origin, dest, date)
    status = search_flights()

    if status == 200:
        parse_response()
    else:
        print("❌ API Returned non-200 status. Check cookies.json.")
