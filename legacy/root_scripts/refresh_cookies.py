from playwright.sync_api import sync_playwright
import json
import time

def save_cookies():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        print("\nOpening Biman…")
        page.goto("https://booking.biman-airlines.com/dx/BGDX/", timeout=120000)

        print("""
=====================================================
⚠️  DO NOT PRESS ENTER UNTIL ALL STEPS ARE DONE

1️⃣ Pass Cloudflare
2️⃣ Make sure the search box loads
3️⃣ Perform this search manually:
        DAC → CXB → next available date
4️⃣ WAIT UNTIL flights appear

Only then come back here.
=====================================================
""")

        input("Press ENTER when flights appear → ")

        cookies = context.cookies()

        with open("state.json", "w") as f:
            json.dump({"cookies": cookies}, f, indent=2)

        print("✅ Saved fresh state.json")

        browser.close()


save_cookies()