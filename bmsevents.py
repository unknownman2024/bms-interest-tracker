from playwright.sync_api import sync_playwright
import json, cloudscraper

def get_session_headers():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://in.bookmyshow.com/explore/movies-hyderabad")
        cookies = page.context.cookies()
        user_agent = page.evaluate("() => navigator.userAgent")
        browser.close()

        # Build headers with session cookies
        cookie_header = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
        return {
            "user-agent": user_agent,
            "cookie": cookie_header,
            "accept": "application/json, text/plain, */*",
            "referer": "https://in.bookmyshow.com/"
        }

scraper = cloudscraper.create_scraper()
headers = get_session_headers()
resp = scraper.get("https://in.bookmyshow.com/api/mobile/movies?regionCode=HYD", headers=headers)
print(resp.status_code)
print(resp.text[:500])
