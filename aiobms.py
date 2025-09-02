import cloudscraper, json, os, random, time, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from datetime import datetime, timedelta, timezone
import pandas as pd
from collections import defaultdict


# constants
DATE_CODE = "20250905"   # 👈 apna dateCode daalna
VENUES_FILE = "venues.json"
OUTPUT_FILE = "scraped_output.json"
MAX_ERRORS = 20
NUM_WORKERS = 5

IST = timezone(timedelta(hours=5, minutes=30))
now = datetime.now(IST)
scraper = cloudscraper.create_scraper()
lock = threading.Lock()
error_count = 0

# --- headers generator ---

IST = timezone(timedelta(hours=5, minutes=30))
now = datetime.now(IST)

scraper = cloudscraper.create_scraper()
lock = threading.Lock()
error_count = 0

# Example User-Agent pool
USER_AGENTS = [
    # Chrome on Windows
    "Mozilla/5.1 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36",
    # Firefox on Windows
    "Mozilla/5.1 (Windows NT 11.0; Win64; x64; rv:{version}) Gecko/20100101 Firefox/{version}",
    # Chrome on Mac
    "Mozilla/5.1 (Macintosh; Intel Mac OS X 10_{minor}_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.38",
    # Safari on Mac
    "Mozilla/5.1 (Macintosh; Intel Mac OS X 10_{minor}_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/{safari_ver} Safari/605.1.16",
]


def get_random_user_agent():
    template = random.choice(USER_AGENTS)
    return template.format(
        version=f"{random.randint(70,120)}.0.{random.randint(1000,5000)}.{random.randint(0,150)}",
        minor=random.randint(12, 15),
        safari_ver=f"{random.randint(13,17)}.0.{random.randint(1,3)}",
    )


def get_random_ip():
    return ".".join(str(random.randint(1, 255)) for _ in range(4))


def get_headers():
    random_ip = get_random_ip()
    return {
        "User-Agent": get_random_user_agent(),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://in.bookmyshow.com",
        "Referer": "https://in.bookmyshow.com/",
        "X-Forwarded-For": random_ip,
        "Client-IP": random_ip,
    }


headers = get_headers()

# --- fetch with retry ---
def fetch_data(venue_code):
    url = f"https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue?venueCode={venue_code}&dateCode={DATE_CODE}"
    for attempt in range(3):
        try:
            res = scraper.get(url, headers=get_headers(), timeout=15)
            if res.status_code == 200:
                return res.json()
            elif res.status_code == 429:
                print(f"⏳ 429 for {venue_code}, retrying... (attempt {attempt+1})", flush=True)
                time.sleep(random.uniform(1, 3))
                continue
            else:
                print(f"⚠️ {venue_code} got status {res.status_code}", flush=True)
                return None
        except Exception as e:
            print(f"⚠️ Failed {venue_code}: {e}", flush=True)
            return None
    return None

# --- process venue safely ---
def fetch_venue_safe(venue_code):
    global error_count
    data = fetch_data(venue_code)
    if not data:
        error_count += 1
        if error_count > MAX_ERRORS:
            print("🔄 Too many errors, restarting...", flush=True)
            time.sleep(0.5)
            os.execv(sys.executable, ['python'] + sys.argv)
        return None
    return {"venue": venue_code, "data": data}

# --- main runner ---
def run():
    global error_count
    error_count = 0

    if not os.path.exists(VENUES_FILE):
        print(f"❌ {VENUES_FILE} not found")
        return

    with open(VENUES_FILE, "r", encoding="utf-8") as f:
        venues = json.load(f)

    results = []
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {executor.submit(fetch_venue_safe, v): v for v in venues}
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                results.append(res)
                print(f"✅ {res['venue']} done", flush=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\n🎉 Finished {len(results)} venues", flush=True)

if __name__ == "__main__":
    run()
