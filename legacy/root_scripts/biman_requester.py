# biman_requester.py
import json
import random
import time
import requests

# ---------------------------
# Load cookies once
# ---------------------------
with open("cookies.json", "r") as f:
    RAW_COOKIES = json.load(f)

COOKIES = RAW_COOKIES

# ---------------------------
# Headers copied from Playwright session
# ---------------------------
HEADERS = {
    "content-type": "application/json",
    "x-sabre-storefront": "BGDX",
    "origin": "https://booking.biman-airlines.com",
    "referer": "https://booking.biman-airlines.com/dx/BGDX/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

GRAPHQL_URL = "https://booking.biman-airlines.com/api/graphql"


# ---------------------------------------------------------
# SAFE REQUEST FUNCTION — with retry, jitter, and safeguards
# ---------------------------------------------------------
def fetch_biman(payload):
    for attempt in range(1, 6):  # up to 5 retries
        try:
            resp = requests.post(
                GRAPHQL_URL,
                json=payload,
                headers=HEADERS,
                cookies=COOKIES,
                timeout=20
            )

            if resp.status_code == 200:
                return {"ok": True, "json": resp.json()}

            # Cloudflare / Incapsula block
            if resp.status_code == 403:
                print(f"⚠️ 403 on attempt {attempt} — Cloudflare blocked us.")
            else:
                print(f"⚠️ HTTP {resp.status_code} on attempt {attempt}")

        except Exception as e:
            print(f"❌ ERROR on attempt {attempt}: {e}")

        # Wait with 4–9 seconds + jitter
        wait = random.uniform(4.0, 9.0)
        time.sleep(wait)

    # All attempts failed
    return {"ok": False, "error": "Blocked or failed after retries."}


