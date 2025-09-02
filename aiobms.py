import cloudscraper, json, os, random, time, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from datetime import datetime, timedelta, timezone

# constants
DATE_CODE = "20250905"   # 👈 apna dateCode daalna
VENUES_FILE = "venues.json"
OUTPUT_FILE = "scraped_output.json"
PROGRESS_FILE = "progress.json"
MAX_ERRORS = 20
MAX_CONSECUTIVE_ERRORS = 10
NUM_WORKERS = 5

# globals
error_count = 0
consecutive_errors = 0
lock = threading.Lock()

IST = timezone(timedelta(hours=5, minutes=30))
now = datetime.now(IST)

scraper = cloudscraper.create_scraper()

# Global state
all_data = {}
fetched_venues = set()

# Example User-Agent pool
USER_AGENTS = [
    "Mozilla/5.1 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36",
    "Mozilla/5.1 (Windows NT 11.0; Win64; x64; rv:{version}) Gecko/20100101 Firefox/{version}",
    "Mozilla/5.1 (Macintosh; Intel Mac OS X 10_{minor}_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.38",
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

# --- PROGRESS HELPERS ---
def dump_progress(all_data, fetched_venues):
    progress = {
        "all_data": all_data,
        "fetched_venues": list(fetched_venues),
    }
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)
    print("💾 Progress saved.", flush=True)

def load_progress():
    global all_data, fetched_venues
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                all_data = data.get("all_data", {})
                fetched_venues = set(data.get("fetched_venues", []))
            print(f"🔄 Loaded progress: {len(fetched_venues)} venues already fetched.")
        except Exception as e:
            print(f"⚠️ Failed to load progress: {e}")

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
            print(f"⚠️ Failed {venue_code} attempt {attempt+1}: {e}", flush=True)
            time.sleep(1)
            continue
    return None

# --- Restart Helper ---
def restart_script():
    dump_progress(all_data, fetched_venues)
    print("🔄 Restarting script...", flush=True)
    time.sleep(0.5)
    os.execv(sys.executable, ["python"] + sys.argv)

# --- FETCH SAFE ---
def fetch_venue_safe(venue_code):
    global error_count, consecutive_errors

    with lock:
        if venue_code in fetched_venues:
            return

    data = fetch_data(venue_code)

    if data is None:  # error
        with lock:
            error_count += 1
            consecutive_errors += 1

            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                print("🛑 10 consecutive errors. Restarting...")
                restart_script()

            if error_count >= MAX_ERRORS:
                print("🛑 20 total errors. Restarting...")
                restart_script()

    else:  # success
        with lock:
            consecutive_errors = 0  # reset streak

            if venue_code not in all_data:
                all_data[venue_code] = {}

            if data:
                for movie, shows in data.items():
                    all_data[venue_code][movie] = shows

            fetched_venues.add(venue_code)
            print(f"✅ Successfully fetched venue: {venue_code} ({len(fetched_venues)} fetched so far)")
            dump_progress(all_data, fetched_venues)

# --- main runner ---
def run():
    global error_count, consecutive_errors
    error_count = 0
    consecutive_errors = 0

    if not os.path.exists(VENUES_FILE):
        print(f"❌ {VENUES_FILE} not found")
        return

    with open(VENUES_FILE, "r", encoding="utf-8") as f:
        venues = json.load(f)

    load_progress()
    to_fetch = [v for v in venues if v not in fetched_venues]

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {executor.submit(fetch_venue_safe, v): v for v in to_fetch}
        for fut in as_completed(futures):
            fut.result()  # result handled inside fetch_venue_safe

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    print(f"\n🎉 Finished {len(fetched_venues)} venues", flush=True)

if __name__ == "__main__":
    run()
