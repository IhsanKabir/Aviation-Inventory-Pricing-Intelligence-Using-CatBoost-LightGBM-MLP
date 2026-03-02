import json
import time
from pathlib import Path
from playwright.sync_api import sync_playwright


def main():

    print("""
======================================================
 NEW SESSION INITIALIZING — WINDOWS 11 MODE
======================================================
    """)

    profile_dir = "biman_verified_profile_W11"

    # Ensure folder exists
    Path(profile_dir).mkdir(exist_ok=True)

    with sync_playwright() as p:

        browser = p.chromium.launch_persistent_context(
            user_data_dir=profile_dir,
            headless=False,
            channel="chrome",       # ⚠️ Windows 11 requires real Chrome
            args=[
                "--start-maximized",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-popup-blocking",
                "--disable-features=IsolateOrigins,site-per-process",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-site-isolation-trials",
                "--disable-web-security",
                "--disable-background-mode",
                "--force-device-scale-factor=1",
                "--disable-features=RendererCodeIntegrity",
            ]
        )

        page = browser.pages[0]
        page.goto("https://booking.biman-airlines.com/dx/BGDX/", timeout=120000)

        print("""
======================================================
⚠️  DO NOT PRESS ENTER UNTIL FLIGHTS LOAD

Inside the browser:

 1. Solve Cloudflare (if shown)
 2. Wait until search bar becomes active
 3. Perform a REAL search (DAC → CXB → some date)
 4. CONFIRM flights appear (no errors)

Then come back here and press ENTER.
======================================================
        """)

        input("Press ENTER only after flights appear normally: ")

        # Save cookies
        cookies = browser.cookies()

        with open("state.json", "w") as f:
            json.dump({"cookies": cookies}, f, indent=2)

        print("\n\n✅ state.json updated successfully!")
        print("You may now run the API scripts without 403 errors.")

        browser.close()


if __name__ == "__main__":
    main()
