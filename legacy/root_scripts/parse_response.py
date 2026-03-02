import json

with open("response.json", "r", encoding="utf-8") as f:
    data = json.load(f)

try:
    groups = data["data"]["bookingAirSearch"]["originalResponse"]["brandedResults"]["itineraryPartBrands"]
except KeyError:
    print("❌ No brandedResults → itineraryPartBrands found.")
    exit()

flights = []

for group in groups:
    for entry in group:
        dep = entry.get("departure")
        arr = entry.get("arrival")
        duration = entry.get("duration")

        # Extract flight info (flight number, aircraft)
        flight_segments = entry.get("flight", [])

        if flight_segments:
            segment = flight_segments[0]
            flight_no = segment.get("flightNumber")
            aircraft = segment.get("equipment", {}).get("type")
        else:
            flight_no = None
            aircraft = None

        # Extract brand offers
        brand_offers = entry.get("brandOffers", [])

        for offer in brand_offers:
            brand = offer.get("brandId")

            # price
            total_price = (
                offer.get("total", {})
                     .get("alternatives", [[{}]])[0][0]
                     .get("amount")
            )

            # baggage extraction
            # Some brands contain baggage rules
            baggage = None
            for svc in offer.get("services", []):
                if svc.get("code") == "BG":   # BG = baggage
                    allowance = svc.get("allowance", {})
                    baggage = allowance.get("value")  # usually in KG

            flights.append({
                "flight_no": flight_no,
                "aircraft": aircraft,
                "departure": dep,
                "arrival": arr,
                "duration": duration,
                "brand": brand,
                "price": total_price,
                "baggage": baggage
            })

# Print results
print("\n============================")
print("        FLIGHT LIST")
print("============================\n")

if not flights:
    print("❌ No flights found.")
else:
    for f in flights:
        print(
            f"{f['flight_no']} | {f['aircraft']} | "
            f"{f['departure']} → {f['arrival']} | "
            f"{f['duration']} | Brand: {f['brand']} | "
            f"Price: {f['price']} BDT | "
            f"Baggage: {f['baggage']} KG"
        )
