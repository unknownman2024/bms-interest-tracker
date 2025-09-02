# aiobms.py
import asyncio, aiohttp, json, os, time, subprocess
from collections import defaultdict

DATE_CODE = os.getenv("DATE_CODE", "20250902")
PROGRESS_FILE = "progress.json"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

# ---------------- LOAD/SAVE HELPERS ----------------
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {"remaining": [], "fetched": {}}
    return {"remaining": [], "fetched": {}}

def save_progress(state):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

# ---------------- FETCH DATA ----------------
def parse_show_data(data, venue_code):
    show_details = data.get("ShowDetails", [])
    if not show_details:
        return {}

    api_date = show_details[0].get("Date")
    if str(api_date) != str(DATE_CODE):
        return {}

    venue_info = show_details[0].get("Venues", {})
    if not venue_info:
        return {}

    venue_name = venue_info.get("VenueName", "")
    venue_add = venue_info.get("VenueAdd", "")
    shows_by_movie = defaultdict(list)

    for event in show_details[0].get("Event", []):
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
            movie_title = f"{parent_title} [{extra_info}]" if extra_info else parent_title

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


async def fetch_data(session, venue_code):
    url = f"https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue?venueCode={venue_code}&dateCode={DATE_CODE}"
    try:
        async with session.get(url, headers=HEADERS, timeout=20) as res:
            if res.status == 429:
                raise Exception("429 rate limited")
            res.raise_for_status()
            data = await res.json()
    except Exception as e:
        print(f"⚠️ Failed {venue_code}: {e}")
        return None

    return parse_show_data(data, venue_code)


# ---------------- SUMMARY ----------------
def compile_summary(all_data, venues_info):
    movie_stats = {}

    for venue_code, movies in all_data.items():
        venue_meta = venues_info.get(venue_code, {})
        city = venue_meta.get("City", "Unknown")
        state = venue_meta.get("State", "Unknown")

        for movie, shows in movies.items():
            if movie not in movie_stats:
                movie_stats[movie] = {
                    "shows": 0,
                    "gross": 0.0,
                    "sold": 0,
                    "fastfilling": 0,
                    "housefull": 0,
                    "totalSeats": 0,
                    "venues": 0,
                    "details": [],
                }

            city_obj = next(
                (c for c in movie_stats[movie]["details"] if c["city"] == city and c["state"] == state),
                None,
            )
            if not city_obj:
                city_obj = {
                    "city": city,
                    "state": state,
                    "shows": 0,
                    "gross": 0.0,
                    "sold": 0,
                    "totalSeats": 0,
                    "fastfilling": 0,
                    "housefull": 0,
                }
                movie_stats[movie]["details"].append(city_obj)

            movie_stats[movie]["venues"] += 1

            for show in shows:
                occ = show["occupancy"]
                movie_stats[movie]["shows"] += 1
                movie_stats[movie]["gross"] += show["gross"]
                movie_stats[movie]["sold"] += show["sold"]
                movie_stats[movie]["totalSeats"] += show["total"]

                if occ >= 98:
                    movie_stats[movie]["housefull"] += 1
                elif occ >= 50:
                    movie_stats[movie]["fastfilling"] += 1

                city_obj["shows"] += 1
                city_obj["gross"] += show["gross"]
                city_obj["sold"] += show["sold"]
                city_obj["totalSeats"] += show["total"]

                if occ >= 98:
                    city_obj["housefull"] += 1
                elif occ >= 50:
                    city_obj["fastfilling"] += 1

    return movie_stats


# ---------------- MAIN RUNNER ----------------
async def worker(name, queue, session, state):
    while True:
        venue = await queue.get()
        if venue is None:
            break

        data = await fetch_data(session, venue)
        if data is None:
            # put back to queue for retry
            await queue.put(venue)
        else:
            state["fetched"][venue] = data
            print(f"✅ {venue} → {sum(len(v) for v in data.values())} shows")
        queue.task_done()


async def main():
    state = load_progress()
    venues = state["remaining"] or ["KRGD", "SLJJ", "SPTJ", "SNVG", "SWJG", "NWCS", "CXMH"]  # demo
    state["remaining"] = venues
    save_progress(state)

    queue = asyncio.Queue()
    for v in venues:
        await queue.put(v)

    async with aiohttp.ClientSession() as session:
        workers = [asyncio.create_task(worker(f"W{i}", queue, session, state)) for i in range(5)]

        hb = time.time()
        while not queue.empty():
            await asyncio.sleep(1)
            if time.time() - hb > 10:
                print(f"💓 Heartbeat @ {time.strftime('%H:%M:%S')}")
                hb = time.time()
            save_progress(state)

        for _ in workers:
            await queue.put(None)
        await asyncio.gather(*workers)

    save_progress(state)
    print("🎉 Done!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"🔄 Restarting workflow because: {e}")
        subprocess.run([
            "gh", "workflow", "run", "aiobms.yml"
        ], check=False)
