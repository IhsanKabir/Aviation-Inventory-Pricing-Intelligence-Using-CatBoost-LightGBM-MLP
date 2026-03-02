import json
import requests
import time

# ---- Load cookies from state.json ----
with open("state.json", "r", encoding="utf8") as f:
    data = json.load(f)

raw_cookies = data["cookies"]
cookies = {c["name"]: c["value"] for c in raw_cookies}

# ---- Load payload ----
with open("payload.json", "r", encoding="utf8") as f:
    payload = json.load(f)

url = "https://booking.biman-airlines.com/api/graphql"

headers = {
    "content-type": "application/json",
    "x-sabre-storefront": "BGDX",
    "referer": "https://booking.biman-airlines.com/dx/BGDX/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "sec-ch-ua": '"Chromium";v="141", "Not A(Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

# ---- Retry Logic ----
for attempt in range(3):
    response = requests.post(url, json=payload, headers=headers, cookies=cookies)

    print(f"Attempt {attempt+1}: STATUS {response.status_code}")

    if response.status_code == 200:
        break

    time.sleep(2)

# ---- Save response ----
with open("response.json", "w", encoding="utf-8") as f:
    f.write(response.text)

print("Saved response.json")
