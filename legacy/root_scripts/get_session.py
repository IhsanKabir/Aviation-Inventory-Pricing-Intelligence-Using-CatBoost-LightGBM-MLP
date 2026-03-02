from playwright.sync_api import sync_playwright
import json

def get_biman_session():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        print("Opening Biman...")
        page.goto("https://booking.biman-airlines.com/dx/BGDX/", timeout=120000)

        # Wait for iframe to load
        page.wait_for_timeout(8000)

        # -------- FIND THE BOOKING IFRAME --------
        iframe = None
        for frame in page.frames:
            if "BGDX" in frame.url or "spark" in frame.url or "booking" in frame.url:
                iframe = frame
                break

        if not iframe:
            print("❌ Could not find booking iframe. Printing all frames:")
            for f in page.frames:
                print("FRAME:", f.url)
            return

        print("✅ Booking iframe found:", iframe.url)

        # -------- INTERACT INSIDE IFRAME --------

        # ORIGIN
        try:
            iframe.click("#origin")
            iframe.keyboard.type("DAC", delay=150)
            iframe.wait_for_timeout(1500)
            iframe.keyboard.press("Enter")
        except Exception as e:
            print("Origin error:", e)

        iframe.wait_for_timeout(2000)

        # DESTINATION
        try:
            iframe.click("#destination")
            iframe.keyboard.type("CXB", delay=150)
            iframe.wait_for_timeout(1500)
            iframe.keyboard.press("Enter")
        except Exception as e:
            print("Destination error:", e)

        iframe.wait_for_timeout(2000)

        # DATE PICKER
        try:
            iframe.click("button[class*='datepicker']")
            iframe.wait_for_timeout(1500)
            iframe.click("td.is-available")
        except Exception as e:
            print("Date error:", e)

        iframe.wait_for_timeout(2000)

        # SEARCH
        try:
            iframe.click("button:has-text('Search')")
        except Exception as e:
            print("Search error:", e)

        # Wait for search results to trigger API → cookies created
        iframe.wait_for_timeout(8000)

        cookies = context.cookies()
        print("Total cookies:", len(cookies))

        browser.close()
        return cookies

if __name__ == "__main__":
    cookies = get_biman_session()
    print(json.dumps(cookies, indent=2))
