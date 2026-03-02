from playwright.sync_api import sync_playwright
import json

def main():
    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir="biman_profile",
            headless=False,
            args=[
                "--start-maximized",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
            ],
            ignore_default_args=[
                "--enable-automation",
                "--disable-blink-features=AutomationControlled",
            ]
        )

        pages = context.pages
        page = pages[0] if pages else context.new_page()

        print("Opening Biman booking page…")
        page.goto("https://booking.biman-airlines.com/dx/BGDX/", wait_until="load")

        print("""
=========================================
⚠️  IMPORTANT — READ THIS
-----------------------------------------
You must now do ALL of the following:

 1️⃣ Wait for Cloudflare to finish  
 2️⃣ If captcha → solve it  
 3️⃣ Search DAC → CXB (any date)  
 4️⃣ Flights MUST appear normally
-----------------------------------------
Only after you SEE real flights,
return here and press ENTER.
=========================================
        """)

        input("Press ENTER ONLY AFTER flights load successfully...")

        cookies = context.cookies()
        with open("state.json", "w") as f:
            json.dump({"cookies": cookies}, f, indent=2)

        print("✅ GOOD state.json saved with", len(cookies), "cookies")

        context.close()

if __name__ == "__main__":
    main()
