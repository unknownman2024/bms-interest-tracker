import requests
import json
import os
import time
import asyncio
import aiohttp
from aiohttp_retry import RetryClient, ExponentialRetry
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import ssl

# CONFIG
DATE = "2025-09-24"
TARGET_MOVIE_ID = 241979
# Change for different movie 
#241979 for OG
#241378 for coolie
#240770 for war2
#CODE BY BFILMY - DONT REMOVE

MAX_WORKERS = 4  # For showtime fetching multiprocessing
CONCURRENCY = 10  # For async seat fetching concurrency
ZIP_FILE = "zipcodes.txt"
OUTPUT_FILE = "newSeatData.json"
ERROR_FILE = "errored_seats.json"
AUTHORIZATION_TOKEN = "<your-auth-token>"  # Replace here
SESSION_ID = "<your-session-id>"           # Replace here

KNOWN_LANGUAGES = [
    "English", "Hindi", "Tamil", "Telugu", "Kannada",
    "Malayalam", "Punjabi", "Gujarati", "Marathi", "Bengali"
]

FORMAT_KEYWORDS = [
    "RPX", "D-Box", "IMAX", "EMX", "Sony Digital Cinema",
    "4DX", "ScreenX", "Dolby Cinema"
]

# === Helper functions for language and format extraction ===

def extract_language(amenities):
    lang_priority = []
    for item in amenities:
        lowered = item.lower()
        for lang in KNOWN_LANGUAGES:
            if f"{lang.lower()} language" in lowered:
                return lang
            if lang.lower() in lowered:
                lang_priority.append((lang, lowered.find(lang.lower())))
    if lang_priority:
        lang_priority.sort(key=lambda x: x[1])
        return lang_priority[0][0]
    return "Unknown"

def extract_format(amenities, default_format):
    for keyword in FORMAT_KEYWORDS:
        if any(keyword.lower() in a.lower() for a in amenities):
            return keyword
    return default_format

def prepare_showtimes(movie):
    out = []
    for variant in movie.get('variants', []):
        fmt = variant.get('formatName', 'Standard')
        for ag in variant.get('amenityGroups', []):
            amenities = [a.get('name', '') for a in ag.get('amenities', [])]
            lang = extract_language(amenities)
            fmt_final = extract_format(amenities, fmt)
            for show in ag.get('showtimes', []):
                out.append({
                    'showtime_id': show.get('id'),
                    'date': show.get('ticketingDate', 'N/A'),
                    'format': fmt_final,
                    'language': lang
                })
    return out

def get_theaters(zip_code, date, page=1, limit=40):
    url = "https://www.fandango.com/napi/theaterswithshowtimes"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": f"https://www.fandango.com/{zip_code}_movietimes?date={date}",
    }
    params = {
        "zipCode": zip_code,
        "date": date,
        "page": page,
        "limit": limit,
        "filter": "open-theaters",
        "filterEnabled": "true",
    }
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"❌ Error fetching theaters for ZIP {zip_code}: {e}")
    return {}

def process_zip(args):
    zip_code, date, page, limit, movie_id = args
    data = get_theaters(zip_code, date, page, limit)
    theaters = []
    if 'theaters' in data:
        for theater in data['theaters']:
            for movie in theater.get('movies', []):
                if movie.get('id') == movie_id:
                    theaters.append({
                        'theater_name': theater.get('name'),
                        'state': theater.get('state'),
                        'zip': theater.get('zip'),
                        'chainCode': theater.get('chainCode'),
                        'chainName': theater.get('chainName'),
                        'city': theater.get('city'),
                        'showtimes': prepare_showtimes(movie)
                    })
    return theaters

def scrape_showtimes(zip_list, date, movie_id):
    args = [(z, date, 1, 40, movie_id) for z in zip_list]
    all_theaters = []
    with ProcessPoolExecutor(MAX_WORKERS) as executor:
        futures = {executor.submit(process_zip, a): a[0] for a in args}
        for f in as_completed(futures):
            zip_code = futures[f]
            try:
                result = f.result()
                if result:
                    all_theaters.extend(result)
                    print(f"✅ ZIP {zip_code} processed, found {len(result)} theaters")
                else:
                    print(f"⚪ ZIP {zip_code} processed, no theaters found")
            except Exception as e:
                print(f"❌ ZIP {zip_code} failed: {e}")
    return all_theaters

# === Async seat map fetching ===

def seatmap_url(showtime_id):
    return f"https://tickets.fandango.com/checkoutapi/showtimes/v2/{showtime_id}/seat-map/"

HEADERS = {
    "authority": "tickets.fandango.com",
    "accept": "application/json",
    "Authorization": AUTHORIZATION_TOKEN,
    "X-Fd-Sessionid": SESSION_ID,
    "Referer": "https://tickets.fandango.com/mobileexpress/seatselection",
    "User-Agent": "Mozilla/5.0"
}

async def fetch_seat(session, show):
    sid = str(show['showtime_id'])
    url = seatmap_url(sid)
    try:
        async with session.get(url, headers=HEADERS, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                d = data.get("data", {})
                area = d.get("areas", [{}])[0]
                available = d.get("totalAvailableSeatCount", 0)
                total = d.get("totalSeatCount", 0)
                sold = total - available
                show.update({
                    'totalSeatSold': sold,
                    'occupancy': round((sold/total)*100, 2) if total else 0.0,
                    'totalAvailableSeatCount': available,
                    'totalSeatCount': total,
                    'grossRevenueUSD': 0.0,
                    'adultTicketPrice': 0.0
                })
                ticket_info = area.get("ticketInfo", [])
                for t in ticket_info:
                    if "adult" in t.get("desc", "").lower():
                        try:
                            price = float(t.get("price", "0.0"))
                            show['adultTicketPrice'] = price
                            show['grossRevenueUSD'] = round(price * sold, 2)
                            break
                        except Exception:
                            pass
                if show['adultTicketPrice'] == 0.0 and ticket_info:
                    try:
                        price = float(ticket_info[0].get("price", "0.0"))
                        show['adultTicketPrice'] = price
                        show['grossRevenueUSD'] = round(price * sold, 2)
                    except Exception:
                        pass
            else:
                show['error'] = {'status': resp.status}
    except Exception as e:
        show['error'] = {'exception': str(e)}

async def run_all(shows, concurrency=CONCURRENCY):
    connector = aiohttp.TCPConnector(ssl=ssl.create_default_context())
    retry = ExponentialRetry(attempts=3)
    async with RetryClient(connector=connector, retry_options=retry) as session:
        sem = asyncio.Semaphore(concurrency)
        async def bound(s):
            async with sem:
                await fetch_seat(session, s)
        tasks = [bound(s) for s in shows]
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Fetching seat maps"):
            await f

# === Main ===

if __name__ == "__main__":
    print("📥 Reading zipcodes...")
    if not os.path.exists(ZIP_FILE):
        print(f"❌ Missing {ZIP_FILE}")
        exit(1)

    zipcodes = open(ZIP_FILE).read().splitlines()
    print(f"✅ {len(zipcodes)} ZIPs loaded.")

    print("🎬 Scraping showtimes...")
    theaters = scrape_showtimes(zipcodes, DATE, TARGET_MOVIE_ID)

    # Deduplicate theaters by theater_name
    unique_theaters = {}
    for t in theaters:
        key = t['theater_name']
        if key not in unique_theaters:
            unique_theaters[key] = t

    print(f"🧹 Deduplicated to {len(unique_theaters)} unique theaters.")

    # Flatten showtimes for async fetching
    flat_showtimes = []
    for theater in unique_theaters.values():
        for s in theater['showtimes']:
            flat_showtimes.append({
                'state': theater['state'],
                'city': theater['city'],
                'zip': theater['zip'],
                'theater_name': theater['theater_name'],
                'chainName': theater['chainName'],
                'chainCode': theater['chainCode'],
                **s
            })

    print(f"🎟️ Total unique showtimes: {len(flat_showtimes)}")

    print("💺 Fetching seat maps...")
    asyncio.run(run_all(flat_showtimes, CONCURRENCY))

    # Load previous data if exists
    existing = {}
    if os.path.exists(OUTPUT_FILE):
        try:
            old = json.load(open(OUTPUT_FILE))
            existing = {str(d['showtime_id']): d for d in old}
        except:
            pass

    updated, added, skipped = 0, 0, 0
    for show in flat_showtimes:
        sid = str(show['showtime_id'])
        if 'error' in show:
            if sid not in existing:
                existing[sid] = show
            else:
                # ✅ Check for 500 + not full occupancy in old data
                if show['error'].get('status') == 500:
                    old_occ = existing[sid].get('occupancy', 0)
                    if old_occ < 100:
                        print(f"⚠️ Please check: {existing[sid]['theater_name']} "
                              f"| {existing[sid]['city']} | {existing[sid]['language']} "
                              f"| Showtime {sid} might be housefull "
                              f"(previous occupancy {old_occ}%)")
            skipped += 1
        else:
            if sid in existing:
                updated += 1
            else:
                added += 1
            existing[sid] = show

    # Save all entries including errors in one file
    final_all = []
    for s in existing.values():
        final_all.append(s)

    out_dir = "USA Data"
    os.makedirs(out_dir, exist_ok=True)

    main_file = os.path.join(out_dir, f"{TARGET_MOVIE_ID}_{DATE}.json")
    error_file = os.path.join(out_dir, f"{TARGET_MOVIE_ID}_{DATE}_errors.json")

    with open(main_file, "w") as f:
        json.dump(final_all, f, indent=2)

    errors = [s for s in existing.values() if 'error' in s]
    with open(error_file, "w") as f:
        json.dump(errors, f, indent=2)

    print("\n✅ Done.")
    print(f"🔁 Updated: {updated} | ➕ Added: {added} | ⏭️ Skipped (errors or unchanged): {skipped}")
    print(f"💾 Saved: {len(final_all)} to {main_file}")
    print(f"⚠️ Error entries saved to {error_file}")
