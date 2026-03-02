import json

with open("response.json", "r") as f:
    data = json.load(f)

offers = data["data"]["bookingAirSearch"]["originalResponse"]["unbundledOffers"]

flights = []

for group in offers:
    for offer in group:
        seg = offer["itineraryPart"][0]["segments"][0]

        flight_info = {
            "flight_number": f"{seg['flight']['airlineCode']} {seg['flight']['flightNumber']}",
            "origin": seg["origin"],
            "destination": seg["destination"],
            "departure": seg["departure"],
            "arrival": seg["arrival"],
            "brand": offer["brandId"],
            "booking_class": seg["bookingClass"],
            "price_bdt": offer["total"]["alternatives"][0][0]["amount"]
        }

        flights.append(flight_info)

# Print nicely
for f in flights:
    print(f)
