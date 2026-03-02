import json

def list_flights(data):
    flights = []

    # Navigate Sabre response
    try:
        itineraries = (
            data["data"]["bookingAirSearch"]["originalResponse"]
                ["groupedItineraryResponse"]["itineraryGroups"][0]["groupedItinerary"])
    except:
        print("Could not find itinerary data.")
        return flights

    for item in itineraries:
        itin = item["itinerary"]

        # Extract first segment (DAC → CXB)
        seg = itin["segments"][0]["flightSegment"]

        dep = seg["departureDateTime"]
        arr = seg["arrivalDateTime"]
        flight_number = seg["flightNumber"]
        carrier = seg["operatingAirline"]["code"]
        origin = seg["departureAirport"]["code"]
        destination = seg["arrivalAirport"]["code"]

        # Prices
        price_info = item["pricingInformation"][0]["fare"][0]["passengerInfoList"][0]
        total_price = price_info["passengerTotalFare"]["totalFare"]["amount"]
        currency = price_info["passengerTotalFare"]["totalFare"]["currencyCode"]

        flights.append({
            "flight": f"{carrier}{flight_number}",
            "from": origin,
            "to": destination,
            "departure": dep,
            "arrival": arr,
            "price": f"{total_price} {currency}"
        })

    return flights


# Load response from search_flights.py output
with open("response.json", "r") as f:
    data = json.load(f)

flights = list_flights(data)

print("\n=== FLIGHT RESULTS ===")
for f in flights:
    print(f"{f['flight']} | {f['departure']} → {f['arrival']} | {f['price']}")
