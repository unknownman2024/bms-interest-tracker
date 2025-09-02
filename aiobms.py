import cloudscraper, json, os, random, time, sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# constants
DATE_CODE = "20250905"   # 👈 apna dateCode daalna
VENUES_FILE = "venues.json"
OUTPUT_FILE = "scraped_output.json"
MAX_ERRORS = 20
NUM_WORKERS = 2  # zyada mat rakho warna 429 chances badhenge

scraper = cloudscraper.create_scraper()

# --- headers generator ---
def get_headers():
    return {
        "User-Agent": random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:116.0) Gecko/20100101 Firefox/116.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
        ]),
        "Accept": "application/json",
        "X-Forwarded-For": ".".join(str(random.randint(0, 255)) for _ in range(4)),
    }

# --- fetch with hard restart on 429 ---
def fetch_data(venue_code):
    url = f"https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue?venueCode={venue_code}&dateCode={DATE_CODE}"
    try:
        res = scraper.get(url, headers=get_headers(), timeout=15)
        if res.status_code == 200:
            return res.json()
        elif res.status_code == 403:
            print(f"⚠️ {venue_code} got status 403 (skipping)", flush=True)
            return None
        elif res.status_code == 429:
            print(f"⏳ 429 for {venue_code}, restarting script...", flush=True)
            time.sleep(0.5)
            os.execv(sys.executable, [sys.executable] + sys.argv)
        else:
            print(f"⚠️ {venue_code} got status {res.status_code}", flush=True)
            return None
    except Exception as e:
        print(f"⚠️ Failed {venue_code}: {e}", flush=True)
        return None

# --- process venue safely ---
def fetch_venue_safe(venue_code):
    global error_count
    data = fetch_data(venue_code)
    if not data:
        error_count += 1
        if error_count > MAX_ERRORS:
            print("🔄 Too many errors, restarting script...", flush=True)
            time.sleep(0.5)
            os.execv(sys.executable, [sys.executable] + sys.argv)
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
            time.sleep(0.5)  # pause between tasks

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\n🎉 Finished {len(results)} venues", flush=True)

if __name__ == "__main__":
    run()
