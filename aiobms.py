import json
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import cloudscraper
import random
import pandas as pd
from collections import defaultdict

# ---------------- CONFIG ----------------
DATE_CODE = 20250905
NUM_WORKERS = 20        # increased from 5
MAX_ERRORS = 10
DUMP_INTERVAL = 50      # dump progress every 50 venues

IST = timezone(timedelta(hours=5, minutes=30))
now = datetime.now(IST)

scraper = cloudscraper.create_scraper()
lock = threading.Lock()
error_count = 0

# force print to flush immediately
print = lambda *a, **k: __builtins__.print(*a, **{**k, "flush": True})

# ---------------- HEADERS ----------------
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

headers = get_headers()   # keep global, as you wanted

# ---------------- VENUES LOADER ----------------
def load_all_venues(path="venues.json"):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def format_rgross(value):
    if value >= 1e7:
        return f"{round(value/1e7, 2)} Cr"
    elif value >= 1e5:
        return f"{round(value/1e5, 2)} L"
    elif value >= 1e3:
        return f"{round(value/1e3, 2)} K"
    else:
        return str(round(value, 2))

# ---------------- FETCH DATA ----------------
def fetch_data(venue_code):
    url = f"https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue?venueCode={venue_code}&dateCode={DATE_CODE}"
    try:
        res = scraper.get(url, headers=headers, timeout=15)
        res.raise_for_status()
        data = res.json()
    except Exception as e:
        print(f"⚠️ Failed {venue_code}: {e}")
        return None

    show_details = data.get("ShowDetails", [])
    if not show_details:
        return {}

    api_date = show_details[0].get("Date")
    if str(api_date) != str(DATE_CODE):
        print(f"⏩ Skipping summary for {venue_code} (date mismatch: {api_date} vs {DATE_CODE})")
        return {}

    venue_info = show_details[0].get("Venues", {})
    if not venue_info:
        return {}

    venue_name = venue_info.get("VenueName", "")
    venue_add = venue_info.get("VenueAdd", "")
    shows_by_movie = defaultdict(list)

    for event in data.get("ShowDetails", [{}])[0].get("Event", []):
        parent_title = event.get("EventTitle", "Unknown")
        parent_event_code = event.get("EventGroup") or event.get("EventCode")

        for child in event.get("ChildEvents", []):
            dimension = child.get("EventDimension", "").strip()
            language = child.get("EventLanguage", "").strip()
            child_event_code = child.get("EventCode")

            parts = []
            if dimension:
                parts.append(dimension)
            if language:
                parts.append(language)
            extra_info = " | ".join(parts)

            if extra_info:
                movie_title = f"{parent_title} [{extra_info}]"
            else:
                movie_title = parent_title

            for show in child.get("ShowTimes", []):
                total = sold = available = gross = 0

                for cat in show.get("Categories", []):
                    seats = int(cat.get("MaxSeats", 0))
                    avail = int(cat.get("SeatsAvail", 0))
                    price = float(cat.get("CurPrice", 0))
                    total += seats
                    available += avail
                    sold += seats - avail
                    gross += (seats - avail) * price

                shows_by_movie[movie_title].append(
                    {
                        "venue_code": venue_code,
                        "venue": venue_name,
                        "address": venue_add,
                        "chain": venue_info.get("VenueCompName", "Unknown"),
                        "movie": movie_title,
                        "parent_event_code": parent_event_code,
                        "child_event_code": child_event_code,
                        "dimension": dimension,
                        "language": language,
                        "time": show.get("ShowTime"),
                        "session_id": show.get("SessionId"),
                        "audi": show.get("Attributes", ""),
                        "total": total,
                        "sold": sold,
                        "available": available,
                        "occupancy": round((sold / total * 100), 2) if total else 0,
                        "gross": gross,
                    }
                )
    return shows_by_movie

# ---------------- FETCH SAFE ----------------
def fetch_venue_safe(venue_code):
    global error_count
    with lock:
        if venue_code in fetched_venues:
            return

    data = fetch_data(venue_code)
    if data is None:  # real error
        with lock:
            error_count += 1
            if error_count >= MAX_ERRORS:
                print("🛑 Too many errors. Pausing 30s...")
                time.sleep(30)
                error_count = 0
    else:
        with lock:
            if venue_code not in all_data:
                all_data[venue_code] = {}
            if data:
                for movie, shows in data.items():
                    all_data[venue_code][movie] = shows
            fetched_venues.add(venue_code)
            print(f"✅ Successfully fetched venue: {venue_code} ({len(fetched_venues)} fetched so far)")

            # dump every 50 venues
            if len(fetched_venues) % DUMP_INTERVAL == 0:
                dump_progress(all_data, fetched_venues)

# ---------------- MAIN ----------------
if __name__ == "__main__":
    with open("venues.json", "r", encoding="utf-8") as f:
        venues = json.load(f)

    if os.path.exists("fetchedvenues.json"):
        with open("fetchedvenues.json", "r", encoding="utf-8") as f:
            fetched_venues = set(json.load(f))
    else:
        fetched_venues = set()

    if os.path.exists("venues_data.json"):
        with open("venues_data.json", "r", encoding="utf-8") as f:
            try:
                all_data = json.load(f)
            except:
                all_data = {}
    else:
        all_data = {}

    print(f"🚀 Starting fetch with {NUM_WORKERS} workers. Already fetched: {len(fetched_venues)} venues")

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [executor.submit(fetch_venue_safe, vcode) for vcode in venues.keys()]
        for _ in as_completed(futures):
            pass

    dump_progress(all_data, fetched_venues)
    print("✅ Final progress saved.")
