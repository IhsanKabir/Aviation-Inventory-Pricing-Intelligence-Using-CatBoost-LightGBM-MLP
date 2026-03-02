import json
import time
import os
from pathlib import Path
from playwright.sync_api import sync_playwright

def main():
    chrome_path = r"C:\Program Files\Google\Chrome\Application\chrome.exe"  # <-- update path if needed
    profile_dir = Path("real_chrome_profile")

    print(f"Using REAL Chrome with profile: {profile_dir}")

    with sync_playwright() as p:

        browser = p.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=False,
            executable_path=chrome_path,   # <<< USE REAL CHROME
            args=[
                "--start-maximized",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--ignore-certificate-errors",
                "--no-proxy-server"
            ],
            viewport={"width": 1600, "height": 900}
        )

        page = browser.new_page()
        print("Opening Biman home page...")
        page.goto("https://biman-airlines.com", wait_until="domcontentloaded")

        print("""
======================================================
⚠️ IMPORTANT — DO NOT PRESS ENTER NOW

Do these inside Chrome manually:

1️⃣ Solve Cloudflare (it should complete!)
2️⃣ Click “Book a flight”
3️⃣ Perform a real search:
      DAC → CXB → valid date → Search
4️⃣ You MUST see normal flight results
   (not 'Service Not Available')

Only AFTER flights load → press ENTER.
======================================================
""")

        input("Press ENTER only after flights appear: ")

        cookies = browser.cookies()
        with open("state.json", "w") as f:
            json.dump(cookies, f, indent=2)

        print(f"✅ Saved {len(cookies)} cookies into state.json")
        browser.close()

if __name__ == "__main__":
    main()
