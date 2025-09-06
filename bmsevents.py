import time
import json
import random
import requests
from playwright.sync_api import sync_playwright

# Region codes
REGION_CODES = ["HYD", "NCR", "BANG", "KOCH", "MUMBAI", "AHD", "CHEN", "KOLK", "PUNE", "CHD"]

# API constants
TOKEN = "1F201EC3D23C41E8B2E3"
APP_VERSION = "7.3.6"

def get_headers_from_playwright(region_code):
    """Launch Playwright to get cookies + user-agent for a region."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        url = f"https://in.bookmyshow.com/explore/movies-{region_code.lower()}"
        page.goto(url, timeout=60000)  # wait up to 60s
        time.sleep(random.uniform(2, 4))  # allow JS to run

        cookies = page.context.cookies()
        cookie_header = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
        user_agent = page.evaluate("() => navigator.userAgent")
        browser.close()

    headers = {
        "User-Agent": user_agent,
        "Cookie": cookie_header,
        "Accept": "application/json, text/plain, */*",
        "Referer": url,
    }
    return headers

def fetch_region_data(region_code):
    """Fetch movie JSON for a region using requests + Playwright session."""
    headers = get_headers_from_playwright(region_code)
    bms_id = f"1.{random.randint(0, 100000000)}.{int(time.time() * 1000)}"

    url = (
        f"https://in.bookmyshow.com/api/mobile/movies?"
        f"regionCode={region_code}&bmsId={bms_id}&isSuperStar=N&channel=mobile"
        f"&appCode=WEBV2&platform=mobile&enableSA=Y&enablePE=Y"
        f"&token={TOKEN}&appVersion={APP_VERSION}&deviceToken={TOKEN}"
    )

    response = requests.get(url, headers=headers)
    print(f"📡 [{region_code}] Status: {response.status_code}")
    if response.status_code == 200:
        return response.json()
    else:
        print(f"⚠️ Failed to fetch region {region_code}, response preview:")
        print(response.text[:500])
        return {}

def main():
    all_data = []
    for code in REGION_CODES:
        time.sleep(random.uniform(2, 5))  # avoid triggering bot detection
        data = fetch_region_data(code)
        if data:
            all_data.append(data)

    # Aggregate movie info
    movie_map = {}
    for region_data in all_data:
        events = region_data.get("nowShowing", {}).get("arrEvents", [])
        for event in events:
            group_key = event.get("EventTitle", "").rsplit("(", 1)[0].strip()
            if not group_key or not event.get("ChildEvents"):
                continue
            base = event["ChildEvents"][0]

            if group_key not in movie_map:
                movie_map[group_key] = {
                    "title": group_key,
                    "releaseDate": base.get("EventDate"),
                    "genres": base.get("EventGenre", "").split("|"),
                    "censor": base.get("EventCensor"),
                    "duration": base.get("Duration"),
                    "isNewEvent": base.get("isNewEvent"),
                    "synopsis": base.get("EventSynopsis"),
                    "rating": event.get("ratings", {}).get("userRating", 0),
                    "avgRating": event.get("ratings", {}).get("avgRating", 0),
                    "totalVotes": event.get("ratings", {}).get("totalVotes", 0),
                    "bmsInterests": event.get("ratings", {}).get("wtsCount", 0),
                    "imageURL": f"https://in.bmscdn.com/events/moviecard/{base.get('EventImageCode')}.jpg",
                    "languages": {}
                }

            movie_entry = movie_map[group_key]
            for version in event.get("ChildEvents", []):
                lang = version.get("EventLanguage")
                if lang and lang not in movie_entry["languages"]:
                    movie_entry["languages"][lang] = {
                        "eventCode": version.get("EventCode"),
                        "trailer": version.get("EventTrailerURL"),
                        "eventURL": f"https://in.bookmyshow.com/movies/{version.get('EventURL')}"
                    }

    with open("moviesdata.json", "w", encoding="utf-8") as f:
        json.dump(list(movie_map.values()), f, indent=2, ensure_ascii=False)
    print(f"✅ Saved {len(movie_map)} movies to moviesdata.json")

if __name__ == "__main__":
    main()
