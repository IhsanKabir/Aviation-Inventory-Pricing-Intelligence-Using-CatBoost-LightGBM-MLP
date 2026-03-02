import json
import time
import uuid
from playwright.sync_api import sync_playwright

def main():
    temp_profile = f"biman_profile_{uuid.uuid4().hex}"
    print(f"Using temporary profile: {temp_profile}")

    with sync_playwright() as p:
        browser = p.chromium.launch_persistent_context(
            user_data_dir=temp_profile,
            headless=False,
            args=[
                "--start-maximized"
            ],
            viewport={"width": 1600, "height": 900}
        )

        page = browser.new_page()
        print("Opening Biman…")
        page.goto("https://biman-airlines.com", wait_until="domcontentloaded")

        print("""
======================================================
⚠️  IMPORTANT — DO NOT PRESS ENTER YET

Complete these steps inside the browser window:

1️⃣ Solve Cloudflare (captcha if shown)
2️⃣ Make sure the SEARCH BOX works
3️⃣ Perform a REAL search:
      DAC → CXB → valid date → Search
4️⃣ FLIGHTS MUST LOAD properly
5️⃣ No 'Service Not Available' errors

Only then come back here.
======================================================
""")

        input("Press ENTER ONLY AFTER flights load successfully: ")

        # Extract cookies
        cookies = browser.cookies()

        cleaned_cookies = []
        for c in cookies:
            if "expiry" in c and isinstance(c["expiry"], (float, int)):
                c["expires"] = int(c["expiry"])
                del c["expiry"]
            cleaned_cookies.append(c)

        with open("state.json", "w") as f:
            json.dump(cleaned_cookies, f, indent=2)

        print(f"✅ Saved new state.json with {len(cleaned_cookies)} cookies")

        browser.close()

if __name__ == "__main__":
    main()
