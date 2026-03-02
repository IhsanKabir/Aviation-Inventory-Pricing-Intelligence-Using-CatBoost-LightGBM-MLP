import json
import requests
import os
from datetime import datetime, timedelta
import shutil
import pandas as pd

from parse_response_full import parse_response

# Load config.json
with open("config.json", "r") as f:
    CONFIG = json.load(f)

ROUTES = CONFIG["routes"]
DATE_OFFSETS = CONFIG["dates"]
OUTPUT_DIR = CONFIG["output"]["directory"]
ARCHIVE_DIR = "archive"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)


def build_payload(origin, destination, date):
    return {
        "operationName": "bookingAirSearch",
        "variables": {
            "search": {
                "tripType": "ONE_WAY",
                "legs": [{
                    "origin": origin,
                    "destination": destination,
                    "departureDate": date
                }],
                "cabinClass": "ECONOMY",
                "adt": 1,
                "chd": 0,
                "inf": 0,
                "pos": "BD"
            }
        },
        "query": "query bookingAirSearch($search: AirSearchRequestInput!) { bookingAirSearch(search: $search) { originalResponse } }"
    }


def load_cookies():
    with open("cookies.json", "r") as f:
        return json.load(f)


def fetch_from_biman(payload, cookies):
    url = "https://booking.biman-airlines.com/api/graphql"

    headers = {
        "content-type": "application/json",
        "x-sabre-storefront": "BGDX",
        "referer": "https://booking.biman-airlines.com/dx/BGDX/",
        "user-agent": "Mozilla/5.0"
    }

    resp = requests.post(url, json=payload, headers=headers, cookies=cookies)
    return resp.status_code, resp.text, resp.json() if resp.status_code == 200 else None


# Archive previous master file
def archive_old_file(path):
    if os.path.exists(path):
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        base = os.path.basename(path)
        new_name = base.replace(".", f"_{ts}.")
        shutil.move(path, os.path.join(ARCHIVE_DIR, new_name))
        print(f"📦 Archived old file → {new_name}")


def save_master_files(rows):
    df = pd.DataFrame(rows)

    csv_path = f"{OUTPUT_DIR}/Biman_All_Routes.csv"
    xlsx_path = f"{OUTPUT_DIR}/Biman_All_Routes.xlsx"

    archive_old_file(csv_path)
    archive_old_file(xlsx_path)

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False)

    print(f"\n✔ Master CSV saved: {csv_path}")
    print(f"✔ Master Excel saved: {xlsx_path}\n")


def run_biman_full():
    cookies = load_cookies()
    today = datetime.now().date()

    master_rows = []  # <-- all rows collected here

    print("\n========== Running BIMAN full automation ==========\n")

    for route in ROUTES:
        origin, dest = route[0], route[1]

        for offset in DATE_OFFSETS:
            search_date = (today + timedelta(days=offset)).strftime("%Y-%m-%d")

            print(f"[BG] Searching {origin}->{dest} on {search_date}")

            payload = build_payload(origin, dest, search_date)
            status, text, json_data = fetch_from_biman(payload, cookies)

            if status != 200:
                print(f"❌ Request failed: {status}")
                continue

            # save raw
            raw_path = f"{OUTPUT_DIR}/BG_{origin}_{dest}_{search_date}_raw.json"
            with open(raw_path, "w", encoding="utf-8") as f:
                f.write(text)

            rows = parse_response(json_data)
            if not rows:
                print(f"⚠ No data for {origin}-{dest} on {search_date}")
                continue

            # Add metadata: origin, destination, date
            for r in rows:
                r["origin"] = origin
                r["destination"] = dest
                r["search_date"] = search_date

            master_rows.extend(rows)
            print(f"✔ Added {len(rows)} rows\n")

    # Save consolidated CSV + XLSX + Archive
    if master_rows:
        save_master_files(master_rows)
    else:
        print("⚠ No data found for any route.")

    print("\n========== COMPLETE ==========\n")


if __name__ == "__main__":
    run_biman_full()
