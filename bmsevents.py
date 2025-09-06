import random
import uuid
import time
import json
import geohash2
import cloudscraper

# Constants
REGION_CODES = ["HYD", "NCR", "BANG", "KOCH", "MUMBAI", "AHD", "CHEN", "KOLK","PUNE","CHD"]
TOKEN = "1F201EC3D23C41E8B2E3"
APP_VERSION = "7.3.6"

# Random values
bms_id = f"1.{random.randint(0, 100000000)}.{int(time.time() * 1000)}"
advertiser_id = str(uuid.uuid4())

user_agents = [
    {
        "ua": "Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36",
        "make": "Google-Pixel 5"
    },
    {
        "ua": "Mozilla/5.0 (Linux; Android 10; Redmi Note 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
        "make": "Xiaomi-Redmi Note 9"
    },
    {
        "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile Safari/604.1",
        "make": "Apple-iPhone"
    }
]
device = random.choice(user_agents)

networks = ["Android | WIFI", "Android | 4G", "Android | LTE"]
network = random.choice(networks)

screens = [
    {"w": "1080", "h": "2340", "d": "2.5"},
    {"w": "720", "h": "1600", "d": "2.0"},
    {"w": "1440", "h": "2392", "d": "3.5"}
]
screen = random.choice(screens)

# Jittered geo
jitter = lambda: (random.random() - 0.5) * 0.01
lat = 17.385044 + jitter()
lon = 78.486671 + jitter()
geo_hash = geohash2.encode(lat, lon, precision=5)

# Headers
headers = {
    "accept": "application/json",
    "user-agent": device["ua"],
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "accept-encoding": "gzip, deflate, br",
    "x-bms-id": bms_id,
    "x-advertiser-id": advertiser_id,
    "x-device-make": device["make"],
    "x-network": network,
    "x-latitude": f"{lat:.6f}",
    "x-longitude": f"{lon:.6f}",
    "x-geohash": geo_hash,
    "x-screen-width": screen["w"],
    "x-screen-height": screen["h"],
    "x-screen-density": screen["d"],
    "referer": "https://in.bookmyshow.com/"
}

# Create CloudScraper session
scraper = cloudscraper.create_scraper()

def fetch_region_data(region_code):
    url = (
        f"https://in.bookmyshow.com/api/mobile/movies?"
        f"regionCode={region_code}&bmsId={bms_id}&isSuperStar=N&channel=mobile"
        f"&appCode=WEBV2&platform=mobile&enableSA=Y&enablePE=Y"
        f"&token={TOKEN}&appVersion={APP_VERSION}&deviceToken={TOKEN}"
    )
    response = scraper.get(url, headers=headers)

    print(f"📡 [{region_code}] Status: {response.status_code}")
    print(f"🧾 [{region_code}] First 500 chars:\n{response.text[:500]}")

    return response.json()


def main():
    try:
        all_data = [fetch_region_data(code) for code in REGION_CODES]
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

        # Print to console
        # ✅ Save to file (properly indented inside try)
        with open("moviesdata.json", "w", encoding="utf-8") as f:
            json.dump(list(movie_map.values()), f, indent=2, ensure_ascii=False)
        print(json.dumps(list(movie_map.values()), indent=2))

    except Exception as e:
        print("❌ Error fetching data:", str(e))

if __name__ == "__main__":
    main()
