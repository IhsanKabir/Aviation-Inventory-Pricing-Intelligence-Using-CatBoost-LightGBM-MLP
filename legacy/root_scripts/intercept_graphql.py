import json
from playwright.sync_api import sync_playwright

STATE_FILE = "state.json"

def load_state():
    """Load cookies from state.json."""
    try:
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
        print(f"Loaded {len(state.get('cookies', []))} cookies from {STATE_FILE}")
        return state
    except Exception as e:
        print("ERROR loading state.json:", e)
        return {"cookies": []}


def main():
    state = load_state()
    cookies = state.get("cookies", [])

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)

        # Create context and attach cookies
        context = browser.new_context()
        if cookies:
            context.add_cookies(cookies)
            print("Cookies added to browser context.")
        else:
            print("WARNING: No cookies loaded!")

        page = context.new_page()

        # Intercept all GraphQL API traffic
        def on_response(response):
            url = response.url
            if "api/graphql" in url:
                print("\n--- Captured GraphQL RESPONSE ---")
                print("URL:", url)
                print("STATUS:", response.status)
                print("RESPONSE HEADERS:")
                try:
                    for k, v in response.headers.items():
                        print(f"  {k}: {v}")
                except:
                    print("  <could not read headers>")
                print("--- end response ---\n")

        def on_request(request):
            url = request.url
            if "api/graphql" in url:
                print("\n========== GRAPHQL REQUEST ==========")
                print("URL:", url)
                print("HEADERS:", json.dumps(request.headers, indent=2))

                # Print cookie header if present
                cookie_header = request.headers.get("cookie", "(no cookie header)")
                print("COOKIES:", cookie_header)

                # Print request payload
                try:
                    payload = request.post_data
                    print("PAYLOAD:", payload)
                except:
                    print("PAYLOAD: <none>")
                print("=====================================\n")

        # Attach listeners
        context.on("request", on_request)
        context.on("response", on_response)

        print("Opening Biman…")
        page.goto("https://booking.biman-airlines.com/dx/BGDX/", wait_until="domcontentloaded")

        print("\n➡️ Now perform a flight search IN THIS BROWSER.")
        print("➡️ I will capture the GraphQL payload and responses automatically.\n")

        page.wait_for_timeout(1000 * 60 * 5)  # keep browser open for 5 minutes


if __name__ == "__main__":
    main()
