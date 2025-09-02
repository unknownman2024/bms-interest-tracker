import json, os, sys, time, random, subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import cloudscraper

DATE_CODE = "20250905"
VENUES_FILE = "venues.json"
OUTPUT_FILE = "scraped_output.json"
PROGRESS_FILE = "progress.json"
MAX_ERRORS = 20
NUM_WORKERS = 5

scraper = cloudscraper.create_scraper()
error_count = 0

# --- headers ---
def get_headers():
    return {
        "User-Agent": random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:116.0) Gecko/20100101 Firefox/116.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1",
        ]),
        "Accept": "application/json",
        "X-Forwarded-For": ".".join(str(random.randint(0, 255)) for _ in range(4)),
    }

# --- workflow restart ---
def restart_workflow(reason: str):
    print(f"🔄 Restarting workflow because: {reason}", flush=True)

    # dump state before exit
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "reason": reason,
            "time": time.time()
        }, f)

    # trigger new workflow
    subprocess.run([
        "gh", "workflow", "run", "aiobms.yml",
        "--ref", os.getenv("GITHUB_REF", "main")
    ], check=False)

    sys.exit(1)

# --- fetch one venue ---
def fetch_data(venue_code):
    url = f"https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue?venueCode={venue_code}&dateCode={DATE_CODE}"
    try:
        res = scraper.get(url, headers=get_headers(), timeout=15)
        if res.status_code == 200:
            return res.json()
        elif res.status_code == 403:
            print(f"⚠️ {venue_code} → 403 forbidden", flush=True)
            return None
        elif res.status_code == 429:
            restart_workflow("429 rate limited")
        else:
            print(f"⚠️ {venue_code} → {res.status_code}", flush=True)
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
            restart_workflow("too many errors")
        return None
    return {"venue": venue_code, "data": data}

# --- main runner ---
def run():
    global error_count
    error_count = 0

    # load venues
    with open(VENUES_FILE, "r", encoding="utf-8") as f:
        all_venues = json.load(f)

    # load already fetched progress
    already_done = set()
    results = []

    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            results = json.load(f)
            already_done = {r["venue"] for r in results}

    remaining = [v for v in all_venues if v not in already_done]

    print(f"🚀 Fetching {len(remaining)} remaining venues with {NUM_WORKERS} workers")

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {executor.submit(fetch_venue_safe, v): v for v in remaining}
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                results.append(res)
                already_done.add(res["venue"])
                print(f"✅ {res['venue']} → {len(res['data'].get('shows', []))} shows", flush=True)

                # dump progress after every success
                with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                    json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\n🎉 Finished {len(results)} total venues", flush=True)

if __name__ == "__main__":
    run()
