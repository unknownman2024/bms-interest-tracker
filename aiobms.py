import json, os, sys, time, threading, random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import cloudscraper
import pandas as pd

DATE_CODE = 20250905
NUM_WORKERS = 5
MAX_ERRORS = 20

IST = timezone(timedelta(hours=5, minutes=30))
scraper = cloudscraper.create_scraper()
lock = threading.Lock()
error_count = 0

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:{version}) Gecko/20100101 Firefox/{version}",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_{minor}_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.38",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_{minor}_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/{safari_ver} Safari/605.1.16",
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
    ip = get_random_ip()
    return {
        "User-Agent": get_random_user_agent(),
        "Accept": "application/json",
        "X-Forwarded-For": ip,
        "Client-IP": ip,
        "Origin": "https://in.bookmyshow.com",
        "Referer": "https://in.bookmyshow.com/",
    }

def fetch_data(venue_code):
    url = f"https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue?venueCode={venue_code}&dateCode={DATE_CODE}"
    try:
        res = scraper.get(url, headers=get_headers(), timeout=15)
        if res.status_code == 200:
            return res.json()
        elif res.status_code == 403:
            print(f"⚠️  {venue_code} → 403 Forbidden", flush=True)
            return None
        elif res.status_code == 429:
            print(f"⏳ 429 Too Many Requests on {venue_code}, restarting...", flush=True)
            time.sleep(1)
            os.execv(sys.executable, [sys.executable] + sys.argv)
        else:
            print(f"⚠️  {venue_code} → HTTP {res.status_code}", flush=True)
            return None
    except Exception as e:
        print(f"⚠️  {venue_code} failed: {e}", flush=True)
        return None

def parse_shows(data, venue_code):
    out = []
    show_details = data.get("ShowDetails", [])
    if not show_details:
        return out
    venue_info = show_details[0].get("Venues", {})
    venue_name = venue_info.get("VenueName", "")
    for event in show_details[0].get("Event", []):
        for child in event.get("ChildEvents", []):
            movie_title = child.get("EventTitle", event.get("EventTitle", "Unknown"))
            for show in child.get("ShowTimes", []):
                total = sold = avail = gross = 0
                for cat in show.get("Categories", []):
                    seats = int(cat.get("MaxSeats", 0))
                    a = int(cat.get("SeatsAvail", 0))
                    p = float(cat.get("CurPrice", 0))
                    total += seats
                    avail += a
                    sold += seats - a
                    gross += (seats - a) * p
                occ = round((sold/total*100),2) if total else 0
                out.append({
                    "venue_code": venue_code,
                    "venue": venue_name,
                    "movie": movie_title,
                    "time": show.get("ShowTime"),
                    "session_id": show.get("SessionId"),
                    "total": total,
                    "sold": sold,
                    "available": avail,
                    "occupancy": occ,
                    "gross": gross,
                })
    return out

def fetch_venue_safe(venue_code):
    global error_count
    data = fetch_data(venue_code)
    if not data:
        with lock:
            error_count += 1
            if error_count >= MAX_ERRORS:
                print("🛑 Too many errors. Restarting...", flush=True)
                os.execv(sys.executable, [sys.executable] + sys.argv)
        return []
    shows = parse_shows(data, venue_code)
    print(f"✅ {venue_code} → {len(shows)} shows", flush=True)
    return shows

# Heartbeat thread (to prove script is alive)
def heartbeat():
    while True:
        print(f"💓 Heartbeat @ {datetime.now(IST).strftime('%H:%M:%S')}", flush=True)
        time.sleep(10)

if __name__ == "__main__":
    with open("venues.json","r",encoding="utf-8") as f:
        venues = list(json.load(f).keys())

    threading.Thread(target=heartbeat, daemon=True).start()

    all_shows = []
    print(f"🚀 Fetching {len(venues)} venues with {NUM_WORKERS} workers", flush=True)

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as ex:
        futures = {ex.submit(fetch_venue_safe,v):v for v in venues}
        for fut in as_completed(futures):
            all_shows.extend(fut.result() or [])

    # Save summary
    movie_summary = {}
    for s in all_shows:
        m = s["movie"]
        if m not in movie_summary:
            movie_summary[m] = {"shows":0,"gross":0.0,"sold":0,"totalSeats":0}
        movie_summary[m]["shows"] += 1
        movie_summary[m]["gross"] += s["gross"]
        movie_summary[m]["sold"] += s["sold"]
        movie_summary[m]["totalSeats"] += s["total"]

    with open("movie_summary.json","w",encoding="utf-8") as f:
        json.dump(movie_summary,f,ensure_ascii=False,indent=2)

    df = pd.DataFrame([{"Movie":k,**v} for k,v in movie_summary.items()])
    df.to_csv("movie_summary.csv",index=False)

    print(f"🎉 Done. {len(all_shows)} shows across {len(movie_summary)} movies", flush=True)
