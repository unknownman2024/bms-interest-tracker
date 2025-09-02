# aiobms.py
import cloudscraper, json, os, random, time, sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# constants
DATE_CODE = "20250905"
VENUES_FILE = "venues.json"
PROGRESS_FILE = "progress.json"
RESULT_FILE = "partial_results.json"
MAX_ERRORS = 20
NUM_WORKERS = 3

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

# --- save progress ---
def save_progress(done, remaining, results):
    json.dump({"done": done, "remaining": remaining}, open(PROGRESS_FILE, "w"), indent=2)
    json.dump(results, open(RESULT_FILE, "w"), indent=2, ensure_ascii=False)

# --- graceful restart (dump + trigger workflow) ---
def graceful_restart(done, remaining, results, reason="429"):
    print(f"🔄 Restarting workflow because: {reason}", flush=True)
    save_progress(done, remaining, results)
    # Trigger new workflow (GitHub Actions)
    os.system("gh workflow run fetch-bms.yml")
    sys.exit(1)

# --- fetch ---
def fetch_data(venue_code):
    url = f"https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue?venueCode={venue_code}&dateCode={DATE_CODE}"
    try:
        res = scraper.get(url, headers=get_headers(), timeout=15)
        if res.status_code == 200:
            return res.json()
        elif res.status_code == 403:
            print(f"⚠️ {venue_code} got 403", flush=True)
            return None
        elif res.status_code == 429:
            graceful_restart(done, remaining, results, "429 rate limited")
        else:
            print(f"⚠️ {venue_code} got status {res.status_code}", flush=True)
            return None
    except Exception as e:
        print(f"⚠️ Failed {venue_code}: {e}", flush=True)
        return None

# --- safe wrapper ---
def fetch_venue_safe(venue_code):
    global error_count
    data = fetch_data(venue_code)
    if not data:
        error_count += 1
        if error_count > MAX_ERRORS:
            graceful_restart(done, remaining, results, "too many errors")
        return None
    return {"venue": venue_code, "data": data}

# --- main ---
def run():
    global error_count, done, remaining, results
    error_count = 0

    # Resume if progress exists
    if os.path.exists(PROGRESS_FILE):
        prog = json.load(open(PROGRESS_FILE))
        done = prog.get("done", [])
        remaining = prog.get("remaining", [])
        results = json.load(open(RESULT_FILE)) if os.path.exists(RESULT_FILE) else []
        print(f"▶️ Resuming: {len(done)} done, {len(remaining)} left")
    else:
        if not os.path.exists(VENUES_FILE):
            print(f"❌ {VENUES_FILE} not found")
            return
        venues = json.load(open(VENUES_FILE))
        done, results = [], []
        remaining = venues
        print(f"🚀 Starting fresh with {len(venues)} venues")

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {executor.submit(fetch_venue_safe, v): v for v in remaining}
        for fut in as_completed(futures):
            res = fut.result()
            v = futures[fut]
            if res:
                results.append(res)
                done.append(v)
                remaining.remove(v)
                print(f"✅ {v} done", flush=True)
                save_progress(done, remaining, results)  # dump after every success
            time.sleep(0.5)

    save_progress(done, remaining, results)
    print(f"\n🎉 Finished all {len(results)} venues", flush=True)

if __name__ == "__main__":
    run()
