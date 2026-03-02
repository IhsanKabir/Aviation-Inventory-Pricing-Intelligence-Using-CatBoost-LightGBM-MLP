# capture_cookies_and_response.py
from playwright.sync_api import sync_playwright
import json
import time

TARGET_PATH = "/api/graphql"

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        # Hold the last response we care about
        last_graphql_resp = {"url": None, "status": None, "headers": None, "body_preview": None}

        def on_response(response):
            try:
                if TARGET_PATH in response.url and response.request.method == "POST":
                    last_graphql_resp["url"] = response.url
                    last_graphql_resp["status"] = response.status
                    # response.headers() is a dict-like
                    last_graphql_resp["headers"] = response.headers
                    # try to get short body preview safely
                    try:
                        txt = response.text()
                        last_graphql_resp["body_preview"] = txt[:4000]
                    except Exception:
                        last_graphql_resp["body_preview"] = "(could not read body)"
                    print("\n--- Captured GraphQL RESPONSE ---")
                    print("URL:", last_graphql_resp["url"])
                    print("STATUS:", last_graphql_resp["status"])
                    print("RESPONSE HEADERS:")
                    for k, v in last_graphql_resp["headers"].items():
                        print(f"  {k}: {v}")
                    print("--- end response ---\n")
            except Exception as e:
                print("on_response error:", e)

        page.on("response", on_response)

        print("Opening Biman booking page...")
        page.goto("https://booking.biman-airlines.com/dx/BGDX/", timeout=120000)
        print("➡️ When the browser window appears, perform a manual search (DAC → CXB → date → Search).")
        print("Waiting for you to do the search... (script will continue after you trigger the request)\n")

        # Wait a long time but allow you to do the manual search
        try:
            page.wait_for_timeout(99999999)
        except KeyboardInterrupt:
            # If you press CTRL+C in terminal, the script will continue to the cookie dump
            pass
        finally:
            # After you stop the script (Ctrl+C), or if it times out, we still try to fetch cookies
            pass

        # But to make it easier, we can also poll for the GraphQL response having been captured
        # (if you don't want to use Ctrl+C). Poll small number of times for a response.
        poll_tries = 12
        for i in range(poll_tries):
            if last_graphql_resp["url"]:
                break
            print("Waiting for GraphQL response to be captured... attempt", i+1)
            time.sleep(1)

        # Wait a bit for cookies to be actually set in the browser context
        time.sleep(2)
        cookies = context.cookies()
        print("\n==== CONTEXT.COOKIES ====")
        print(json.dumps(cookies, indent=2))
        print("==== END COOKIES ====\n")

        # If response headers included set-cookie, they were already printed above.
        # Close browser
        browser.close()

if __name__ == "__main__":
    main()
