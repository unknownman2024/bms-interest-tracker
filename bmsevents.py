from playwright.sync_api import sync_playwright
import requests
import json

def fetch_bms_json(region_code="HYD"):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(f"https://in.bookmyshow.com/explore/movies-{region_code.lower()}")
        
        # Extract cookies
        cookies = page.context.cookies()
        cookie_header = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
        
        # Extract user-agent
        user_agent = page.evaluate("() => navigator.userAgent")
        browser.close()
    
    headers = {
        "User-Agent": user_agent,
        "Cookie": cookie_header,
        "Accept": "application/json, text/plain, */*",
        "Referer": f"https://in.bookmyshow.com/explore/movies-{region_code.lower()}"
    }
    
    # BMS API URL
    url = (
        f"https://in.bookmyshow.com/api/mobile/movies?"
        f"regionCode={region_code}&bmsId=1.0.{region_code}&isSuperStar=N&channel=mobile"
        f"&appCode=WEBV2&platform=mobile&enableSA=Y&enablePE=Y"
        f"&token=1F201EC3D23C41E8B2E3&appVersion=7.3.6&deviceToken=1F201EC3D23C41E8B2E3"
    )
    
    response = requests.get(url, headers=headers)
    print(response.status_code)
    print(response.text[:500])
    
    if response.status_code == 200:
        return response.json()
    return {}

# Example usage
data = fetch_bms_json("HYD")
with open("moviesdata.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)
