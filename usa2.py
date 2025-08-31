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
import random
import datetime
from zoneinfo import ZoneInfo
from collections import defaultdict

# CONFIG
DATE = "2025-09-24"
MAX_WORKERS = 30
CONCURRENCY = 100
ZIP_FILE = "zipcodes.txt"
AUTHORIZATION_TOKEN = "<your-auth-token>"
SESSION_ID = "<your-session-id>"

# 🎯 If empty → fetch all movies
TARGET_MOVIES = []

KNOWN_LANGUAGES = [
    "English","Hindi","Tamil","Telugu","Kannada",
    "Malayalam","Punjabi","Gujarati","Marathi","Bengali",
]

FORMAT_KEYWORDS = [
    "RPX","D-Box","IMAX","EMX","Sony Digital Cinema",
    "4DX","ScreenX","Cinemark XD","Dolby Cinema",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:{version}) Gecko/20100101 Firefox/{version}",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_{minor}_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_{minor}_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/{safari_ver} Safari/605.1.15",
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

def get_seatmap_headers():
    random_ip = get_random_ip()
    return {
        "User-Agent": get_random_user_agent(),
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://fandango.com",
        "Referer": "https://tickets.fandango.com/mobileexpress/seatselection",
        "X-Forwarded-For": random_ip,
        "Client-IP": random_ip,
        "Connection": "keep-alive",
        "Authorization": AUTHORIZATION_TOKEN,
        "X-Fd-Sessionid": SESSION_ID,
        "authority": "tickets.fandango.com",
        "accept": "application/json",
    }

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
    for variant in movie.get("variants", []):
        fmt = variant.get("formatName", "Standard")
        for ag in variant.get("amenityGroups", []):
            amenities = [a.get("name", "") for a in ag.get("amenities", [])]
            lang = extract_language(amenities)
            fmt_final = extract_format(amenities, fmt)
            for show in ag.get("showtimes", []):
                out.append(
                    {
                        "showtime_id": show.get("id"),
                        "date": show.get("ticketingDate", "N/A"),
                        "format": fmt_final,
                        "language": lang,
                    }
                )
    return out

def get_headers2(zip_code, date):
    random_ip = get_random_ip()
    return {
        "User-Agent": get_random_user_agent(),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://www.fandango.com",
        "Referer": f"https://www.fandango.com/{zip_code}_movietimes?date={date}",
        "X-Forwarded-For": random_ip,
        "Client-IP": random_ip,
        "Connection": "keep-alive",
    }

def get_theaters(zip_code, date, page=1, limit=40):
    url = "https://www.fandango.com/napi/theaterswithshowtimes"
    params = {
        "zipCode": zip_code,
        "date": date,
        "page": page,
        "limit": limit,
        "filter": "open-theaters",
        "filterEnabled": "true",
    }
    try:
        r = requests.get(url, headers=get_headers2(zip_code, date), params=params, timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"❌ Error fetching theaters for ZIP {zip_code}: {e}")
    return {}

def process_zip(args):
    zip_code, date, page, limit = args
    data = get_theaters(zip_code, date, page, limit)
    theaters = []
    if "theaters" in data:
        for theater in data["theaters"]:
            for movie in theater.get("movies", []):
                if TARGET_MOVIES and str(movie.get("id")) not in TARGET_MOVIES:
                    continue
                theaters.append(
                    {
                        "movie_id": movie.get("id"),
                        "theater_name": theater.get("name"),
                        "state": theater.get("state"),
                        "zip": theater.get("zip"),
                        "chainCode": theater.get("chainCode"),
                        "chainName": theater.get("chainName"),
                        "city": theater.get("city"),
                        "showtimes": prepare_showtimes(movie),
                    }
                )
    return theaters

def scrape_showtimes(zip_list, date):
    args = [(z, date, 1, 40) for z in zip_list]
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

def seatmap_url(showtime_id):
    return f"https://tickets.fandango.com/checkoutapi/showtimes/v2/{showtime_id}/seat-map/"

async def fetch_seat(session, show):
    sid = str(show["showtime_id"])
    url = seatmap_url(sid)
    try:
        async with session.get(url, headers=get_seatmap_headers(), timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                d = data.get("data", {})
                area = d.get("areas", [{}])[0]
                available = d.get("totalAvailableSeatCount", 0)
                total = d.get("totalSeatCount", 0)
                sold = total - available
                show.update(
                    {
                        "totalSeatSold": sold,
                        "occupancy": round((sold / total) * 100, 2) if total else 0.0,
                        "totalAvailableSeatCount": available,
                        "totalSeatCount": total,
                        "grossRevenueUSD": 0.0,
                        "adultTicketPrice": 0.0,
                    }
                )
                ticket_info = area.get("ticketInfo", [])
                for t in ticket_info:
                    if "adult" in t.get("desc", "").lower():
                        try:
                            price = float(t.get("price", "0.0"))
                            show["adultTicketPrice"] = price
                            show["grossRevenueUSD"] = round(price * sold, 2)
                            break
                        except Exception:
                            pass
                if show["adultTicketPrice"] == 0.0 and ticket_info:
                    try:
                        price = float(ticket_info[0].get("price", "0.0"))
                        show["adultTicketPrice"] = price
                        show["grossRevenueUSD"] = round(price * sold, 2)
                    except Exception:
                        pass
            else:
                show["error"] = {"status": resp.status}
    except Exception as e:
        show["error"] = {"exception": str(e)}

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

if __name__ == "__main__":
    print("📥 Reading zipcodes...")
    if not os.path.exists(ZIP_FILE):
        print(f"❌ Missing {ZIP_FILE}")
        exit(1)
    zipcodes = open(ZIP_FILE).read().splitlines()
    print(f"✅ {len(zipcodes)} ZIPs loaded.")

    print("🎬 Scraping showtimes...")
    theaters = scrape_showtimes(zipcodes, DATE)

    # Deduplicate shows
    seen = set()
    movies_data = defaultdict(list)
    for t in theaters:
        for s in t["showtimes"]:
            key = (t["movie_id"], s["showtime_id"])
            if key in seen:
                continue
            seen.add(key)
            entry = {
                "state": t["state"],
                "city": t["city"],
                "zip": t["zip"],
                "theater_name": t["theater_name"],
                "chainName": t["chainName"],
                "chainCode": t["chainCode"],
                **s,
            }
            movies_data[t["movie_id"]].append(entry)

    flat_showtimes = []
    for mid, shows in movies_data.items():
        for s in shows:
            s["movie_id"] = mid
            flat_showtimes.append(s)

    print(f"🎟️ Total unique showtimes: {len(flat_showtimes)}")
    print("💺 Fetching seat maps...")
    asyncio.run(run_all(flat_showtimes, CONCURRENCY))

    out_dir = "USA Data"
    os.makedirs(out_dir, exist_ok=True)

    main_file = os.path.join(out_dir, f"ALLMOVIES_{DATE}.json")
    final_json = [{"id": mid, "data": shows} for mid, shows in movies_data.items()]
    with open(main_file, "w") as f:
        json.dump(final_json, f, indent=2, ensure_ascii=False)
    print(f"💾 Saved master data → {main_file}")

    now_ist = datetime.datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %I:%M:%S %p")
    combined_logs_file = os.path.join(out_dir, f"ALLMOVIES_{DATE}_logs.json")
    combined_logs_entry = {"time": now_ist, "movies": []}

    for mid, shows in movies_data.items():
        total_gross, total_shows, total_sold, total_capacity = 0.0, 0, 0, 0
        venues = set()
        for s in shows:
            if "error" not in s:
                total_gross += s.get("grossRevenueUSD", 0.0)
                total_shows += 1
                total_sold += s.get("totalSeatSold", 0)
                total_capacity += s.get("totalSeatCount", 0)
                venues.add(s.get("theater_name"))
        avg_occupancy = round((total_sold / total_capacity) * 100, 2) if total_capacity else 0.0
        combined_logs_entry["movies"].append({
            "id": mid,
            "total_gross_usd": round(total_gross, 2),
            "total_shows": total_shows,
            "avg_occupancy": avg_occupancy,
            "tickets_sold": total_sold,
            "unique_venues": len(venues),
        })

    existing_combined = []
    if os.path.exists(combined_logs_file):
        try:
            existing_combined = json.load(open(combined_logs_file))
            if not isinstance(existing_combined, list):
                existing_combined = []
        except Exception:
            existing_combined = []

    existing_combined.append(combined_logs_entry)
    with open(combined_logs_file, "w") as f:
        json.dump(existing_combined, f, indent=2, ensure_ascii=False)

    print(f"📝 Combined logs saved → {combined_logs_file}")
    print("\n✅ Done.")
