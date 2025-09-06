from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.goto("https://in.bookmyshow.com/explore/movies-hyderabad")

    # Page content
    print(page.content()[:500])

    # Get cookies for API requests
    cookies = page.context.cookies()
    cookie_header = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
    print("Cookie header:", cookie_header)

    browser.close()
