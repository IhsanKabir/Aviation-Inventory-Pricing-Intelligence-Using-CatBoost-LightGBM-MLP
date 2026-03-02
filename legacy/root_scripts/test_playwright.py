from playwright.sync_api import sync_playwright

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--disable-dev-shm-usage",
                "--no-sandbox"
            ]
        )

        # Try loading state.json, otherwise create it
        try:
            context = browser.new_context(storage_state="state.json")
        except FileNotFoundError:
            context = browser.new_context()
            context.storage_state(path="state.json")

        page = context.new_page()

        print("Loading Biman site…")
        page.goto("https://booking.biman-airlines.com/dx/BGDX/", timeout=90000)

        print("Browser ready – try searching now.")
        page.wait_for_timeout(999999999)

if __name__ == "__main__":
    main()
