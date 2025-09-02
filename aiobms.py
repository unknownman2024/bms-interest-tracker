import cloudscraper, json, os, random, time, sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# constants
DATE_CODE = "20250905"
VENUES_FILE = "venues.json"
OUTPUT_FILE = "scraped_output.json"
MAX_RETRIES = 5
NUM_WORKERS = 5
BATCH_SIZE = 300   # 3189 venues ~ 11 batches

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

# --- fetch with retry ---
def fetch_data(venue_code):
    url = f"https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue?venueCode={venue_code}&dateCode={DATE_CODE}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = scraper.get(url, headers=get_headers(), timeout=15)

            if res.status_code == 200:
                return res.json()

            elif res.status_code == 403:
                print(f"⚠️ {venue_code} → 403 Forbidden (skipped)", flush=True)
                return None

            elif res.status_code == 429:
                wait_time = attempt * 5
                print(f"⏳ 429 Too Many Requests on {venue_code}, retrying in {wait_time}s...", flush=True)
                time.sleep(wait_time)
                continue

            else:
                print(f"⚠️ {venue_code} → {res.status_code}", flush=True)
                return None

        except Exception as e:
            print(f"⚠️ {venue_code} failed: {e}", flush=True)
            time.sleep(2)

    print(f"❌ {venue_code} failed after {MAX_RETRIES} retries", flush=True)
    return None

# --- process venue ---
def fetch_venue_safe(venue_code):
    data = fetch_data(venue_code)
    if not data:
        return None
    return {"venue": venue_code, "data": data}

# --- run in batches ---
def run():
    if not os.path.exists(VENUES_FILE):
        print(f"❌ {VENUES_FILE} not found")
        return

    with open(VENUES_FILE, "r", encoding="utf-8") as f:
        venues = json.load(f)

    results = []

    # split into batches
    for i in range(0, len(venues), BATCH_SIZE):
        batch = venues[i:i + BATCH_SIZE]
        print(f"\n🚀 Processing batch {i//BATCH_SIZE+1} with {len(batch)} venues", flush=True)

        with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
            futures = {executor.submit(fetch_venue_safe, v): v for v in batch}
            for fut in as_completed(futures):
                res = fut.result()
                if res:
                    results.append(res)
                    shows_count = len(res["data"].get("shows", [])) if "data" in res else "?"
                    print(f"✅ {res['venue']} → {shows_count} shows", flush=True)

        # save after each batch
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        print(f"💾 Saved progress after batch {i//BATCH_SIZE+1}", flush=True)

    print(f"\n🎉 Finished {len(results)} venues total", flush=True)

if __name__ == "__main__":
    run()
