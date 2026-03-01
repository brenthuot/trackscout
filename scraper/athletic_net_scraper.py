"""
Run Stats — Athletic.net Backfill Scraper
Searches athletic.net for each TFRRS athlete (by name + hs_grad_year),
extracts hometown, high school, and HS performances,
then upserts back into Supabase.

Usage:
    python athletic_net_scraper.py               # backfill all missing
    python athletic_net_scraper.py --limit 50    # test with 50 athletes
    python athletic_net_scraper.py --group 1     # same conference groups as TFRRS scraper
"""

import os, time, re, logging, argparse, json
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── SUPABASE ──────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── CONSTANTS ─────────────────────────────────────────────────────────────────
RATE_LIMIT = 2.0   # seconds between requests — be polite
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.athletic.net/",
}

# Conference groups matching TFRRS scraper
CONFERENCE_GROUPS = {
    "1": ["SEC", "Big Ten"],
    "2": ["ACC", "Big 12"],
    "3": ["Pac-12", "Ivy League", "Big East"],
    "4": ["Mountain West", "Big Sky"],
    "5": ["American", "Atlantic 10"],
    "6": ["West Coast"],
}

# Event name mapping: athletic.net → our standard
ANET_EVENT_MAP = {
    "100 Meters": "100m", "100m": "100m",
    "200 Meters": "200m", "200m": "200m",
    "400 Meters": "400m", "400m": "400m",
    "800 Meters": "800m", "800m": "800m",
    "1,500 Meters": "1500m", "1500m": "1500m", "1500 Meters": "1500m",
    "1 Mile Run": "Mile", "Mile": "Mile",
    "3,000 Meters": "3000m", "3000 Meters": "3000m",
    "3,000 Meter Steeplechase": "3000SC",
    "5,000 Meters": "5000m", "5000 Meters": "5000m",
    "10,000 Meters": "10000m",
    "110 Meter Hurdles": "110mH", "100 Meter Hurdles": "100mH",
    "400 Meter Hurdles": "400mH",
    "High Jump": "HJ", "Long Jump": "LJ", "Triple Jump": "TJ",
    "Pole Vault": "PV", "Shot Put": "SP", "Discus Throw": "DT",
    "Hammer Throw": "HT", "Javelin Throw": "JT", "Weight Throw": "WT",
    "Pentathlon": "Pent", "Heptathlon": "Hept", "Decathlon": "Dec",
}

RELAY_EVENTS = {"4x100", "4x400", "4x100 Meter Relay", "4x400 Meter Relay",
                "4 x 100", "4 x 400", "Sprint Medley", "Distance Medley"}


# ── HTTP HELPERS ──────────────────────────────────────────────────────────────
def get_json(url: str, params=None, retries=3):
    for attempt in range(retries):
        try:
            time.sleep(RATE_LIMIT)
            r = requests.get(url, headers=HEADERS, params=params, timeout=20)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                log.warning("Rate limited — sleeping 60s")
                time.sleep(60)
            else:
                log.warning(f"HTTP {r.status_code}: {url}")
        except Exception as e:
            log.error(f"Attempt {attempt+1}: {e}")
            time.sleep(5)
    return None


def get_page(url: str, retries=3):
    for attempt in range(retries):
        try:
            time.sleep(RATE_LIMIT)
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 200:
                return BeautifulSoup(r.text, "lxml")
            elif r.status_code == 429:
                log.warning("Rate limited — sleeping 60s")
                time.sleep(60)
            elif r.status_code == 404:
                return None
        except Exception as e:
            log.error(f"Attempt {attempt+1}: {e}")
            time.sleep(5)
    return None


# ── MARK PARSER ───────────────────────────────────────────────────────────────
def parse_mark(s: str) -> float | None:
    if not s:
        return None
    s = s.strip().lstrip("*").strip()
    # Remove wind readings like "(+1.2)"
    s = re.sub(r'\([^)]+\)', '', s).strip()
    try:
        if ":" in s:
            parts = s.split(":")
            if len(parts) == 2:
                return round(float(parts[0]) * 60 + float(parts[1]), 3)
        # Field events: "6.52m" -> 6.52, "21-05.75" (feet-inches) -> convert to meters
        m = re.match(r'^(\d+)-(\d+(?:\.\d+)?)$', s)
        if m:
            feet, inches = float(m.group(1)), float(m.group(2))
            return round((feet * 12 + inches) * 0.0254, 3)
        return float(re.sub(r'[^0-9.]', '', s))
    except ValueError:
        return None


# ── ATHLETIC.NET SEARCH ───────────────────────────────────────────────────────
def search_athlete(name: str, hs_grad_year: int | None) -> list[dict]:
    """
    Use athletic.net's search API to find candidate athlete profiles.
    Returns list of {id, name, school, grad_year, url} dicts.
    """
    # athletic.net search API
    url = "https://www.athletic.net/api/v1/AthleticAPI/Search"
    params = {"q": name, "type": "athlete"}
    data = get_json(url, params)

    candidates = []
    if not data:
        return candidates

    # Results may be under different keys depending on API version
    results = data.get("athletes") or data.get("results") or data.get("Athletes") or []
    if isinstance(results, dict):
        results = results.get("hits") or results.get("items") or []

    for r in results:
        aid = r.get("AthleteID") or r.get("id") or r.get("ID")
        aname = r.get("Name") or r.get("name") or ""
        school = r.get("SchoolName") or r.get("school") or ""
        grad = r.get("GraduationYear") or r.get("gradYear") or r.get("GradYear")
        if not aid:
            continue
        # Filter by grad year if we have it
        if hs_grad_year and grad and abs(int(grad) - int(hs_grad_year)) > 1:
            continue
        candidates.append({
            "id": str(aid),
            "name": aname,
            "school": school,
            "grad_year": grad,
            "url": f"https://www.athletic.net/TrackAndField/Athlete.aspx?AID={aid}",
        })
    return candidates


def search_athlete_html(name: str, hs_grad_year: int | None) -> list[dict]:
    """
    Fallback: scrape athletic.net search results page directly.
    """
    url = "https://www.athletic.net/Search.aspx"
    params = {"q": name, "itype": "athlete"}
    try:
        time.sleep(RATE_LIMIT)
        r = requests.get(url, headers=HEADERS, params=params, timeout=20)
        if r.status_code != 200:
            return []
    except Exception:
        return []

    soup = BeautifulSoup(r.text, "lxml")
    candidates = []

    for link in soup.find_all("a", href=re.compile(r"/TrackAndField/Athlete\.aspx\?AID=\d+")):
        href = link.get("href", "")
        aid_match = re.search(r"AID=(\d+)", href)
        if not aid_match:
            continue
        aid = aid_match.group(1)
        aname = link.get_text(strip=True)
        if not aname or len(aname) < 2:
            continue

        # Look for grad year nearby
        row = link.find_parent(["tr","li","div"])
        row_text = row.get_text(" ", strip=True) if row else ""
        grad_match = re.search(r"\b(20[12]\d)\b", row_text)
        grad = int(grad_match.group(1)) if grad_match else None

        if hs_grad_year and grad and abs(grad - hs_grad_year) > 1:
            continue

        candidates.append({
            "id": aid,
            "name": aname,
            "school": "",
            "grad_year": grad,
            "url": f"https://www.athletic.net/TrackAndField/Athlete.aspx?AID={aid}",
        })

    return candidates[:5]  # Return top 5 candidates max


# ── ATHLETE PROFILE SCRAPER ───────────────────────────────────────────────────
def scrape_athlete_profile(candidate: dict, expected_name: str, hs_grad_year: int | None) -> dict | None:
    """
    Scrape an athletic.net athlete profile page.
    Returns {hometown, high_school, performances: [...]} or None if not a match.
    """
    soup = get_page(candidate["url"])
    if not soup:
        return None

    # ── Verify name match ────────────────────────────────────────────────────
    page_name = ""
    for tag in soup.find_all(["h1", "h2"]):
        t = tag.get_text(strip=True)
        if len(t) > 2 and len(t) < 80:
            page_name = t
            break

    # Fuzzy name match — require first + last name both present
    expected_parts = expected_name.lower().split()
    page_lower = page_name.lower()
    if not all(p in page_lower for p in expected_parts[:2]):
        log.debug(f"    Name mismatch: expected '{expected_name}', got '{page_name}'")
        return None

    # ── Extract hometown and high school ─────────────────────────────────────
    US_STATES = {"AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
                 "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
                 "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
                 "VA","WA","WV","WI","WY","DC"}

    hometown = ""
    hometown_state = ""
    high_school = ""

    page_text = soup.get_text(" ", strip=True)

    # Look for "City, ST" patterns
    for text in soup.stripped_strings:
        t = text.strip()
        m = re.match(r'^([\w\s\.\'\-]{2,40}),\s+([A-Z]{2})$', t)
        if m and m.group(2) in US_STATES:
            hometown = t
            hometown_state = m.group(2)
            break

    # Look for high school name — athletic.net shows school on profile
    # Usually in a link or header area
    for tag in soup.find_all(["h3","h4","a","span","td","li"]):
        t = tag.get_text(strip=True)
        if ("High School" in t or "HS" in t or re.search(r'\b(Academy|Prep|School)\b', t)) and len(t) < 80:
            if t not in ("High School", "HS"):
                high_school = t
                break

    # Alternative: look for school name in profile metadata
    if not high_school:
        m = re.search(r'(?:School|Team)[\s:]+([A-Z][A-Za-z\s\.\-\']+(?:High|HS|Academy|Prep|School)[A-Za-z\s\.\-\']*)', page_text)
        if m:
            high_school = m.group(1).strip()[:80]

    # ── Extract HS performances ──────────────────────────────────────────────
    performances = []

    # Try the athletic.net results API
    aid = candidate["id"]
    results_url = f"https://www.athletic.net/api/v1/AthleticAPI/AthleteResults?athleteId={aid}&sport=tf"
    results_data = get_json(results_url)

    if results_data:
        results = results_data.get("results") or results_data.get("Results") or []
        for res in results:
            event_raw = res.get("EventName") or res.get("event") or ""
            if any(relay in event_raw for relay in ["Relay", "4x", "Medley"]):
                continue
            event_norm = ANET_EVENT_MAP.get(event_raw)
            if not event_norm:
                continue
            mark_raw = str(res.get("Result") or res.get("mark") or "")
            mark_num = parse_mark(mark_raw)
            if not mark_num or mark_num <= 0:
                continue
            year = res.get("Year") or res.get("year")
            meet = res.get("MeetName") or res.get("meet") or ""
            performances.append({
                "event": event_norm,
                "mark": mark_num,
                "mark_display": mark_raw,
                "year": int(year) if year else None,
                "season": "outdoor",  # athletic.net is primarily outdoor
                "level": "hs",
                "meet_name": str(meet)[:100],
            })
    else:
        # Fallback: parse results table from HTML page
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = [td.get_text(strip=True) for td in row.find_all(["td","th"])]
                if len(cells) < 3:
                    continue
                for i, cell in enumerate(cells):
                    event_norm = ANET_EVENT_MAP.get(cell)
                    if event_norm and i + 1 < len(cells):
                        mark_raw = cells[i+1]
                        mark_num = parse_mark(mark_raw)
                        if mark_num and 0 < mark_num < 100000:
                            yr_match = re.search(r'\b(20[12]\d)\b', " ".join(cells))
                            performances.append({
                                "event": event_norm,
                                "mark": mark_num,
                                "mark_display": mark_raw,
                                "year": int(yr_match.group(1)) if yr_match else None,
                                "season": "outdoor",
                                "level": "hs",
                                "meet_name": "",
                            })

    return {
        "hometown": hometown,
        "hometown_state": hometown_state,
        "high_school": high_school,
        "performances": performances,
        "anet_url": candidate["url"],
    }


# ── BEST CANDIDATE SELECTOR ───────────────────────────────────────────────────
def find_best_match(name: str, hs_grad_year: int | None) -> dict | None:
    """
    Search athletic.net and return the best matching profile, or None.
    """
    # Try JSON API first
    candidates = search_athlete(name, hs_grad_year)

    # Fallback to HTML search
    if not candidates:
        candidates = search_athlete_html(name, hs_grad_year)

    if not candidates:
        log.debug(f"  No candidates for '{name}'")
        return None

    log.debug(f"  {len(candidates)} candidates for '{name}'")

    # Try candidates in order until we get a name match
    for cand in candidates[:3]:
        profile = scrape_athlete_profile(cand, name, hs_grad_year)
        if profile:
            return profile

    return None


# ── SUPABASE BACKFILL ─────────────────────────────────────────────────────────
def backfill_athlete(athlete_row: dict) -> bool:
    """
    Find athlete on athletic.net and update Supabase with hometown/HS data.
    """
    athlete_id = athlete_row["id"]
    name = athlete_row["name"]
    hs_grad_year = athlete_row.get("hs_grad_year")

    log.info(f"  Searching: {name} (HS {hs_grad_year or '?'})")

    profile = find_best_match(name, hs_grad_year)
    if not profile:
        log.info(f"    No match found for {name}")
        return False

    log.info(f"    ✓ Found: hometown={profile['hometown']!r}, HS={profile['high_school']!r}, {len(profile['performances'])} HS perfs")

    # Update athlete record
    update_data = {
        "updated_at": datetime.utcnow().isoformat(),
    }
    if profile["hometown"]:
        update_data["hometown"] = profile["hometown"]
    if profile["hometown_state"]:
        update_data["hometown_state"] = profile["hometown_state"]
    if profile["high_school"]:
        update_data["high_school"] = profile["high_school"]

    try:
        supabase.table("athletes").update(update_data).eq("id", athlete_id).execute()

        # Insert HS performances
        if profile["performances"]:
            hs_rows = [
                {**p, "athlete_id": athlete_id, "source": "athletic_net"}
                for p in profile["performances"]
                if p.get("mark") and p.get("event")
            ]
            if hs_rows:
                # Delete existing HS perfs from athletic.net first
                supabase.table("performances").delete().eq("athlete_id", athlete_id).eq("source", "athletic_net").execute()
                # Insert in batches
                for i in range(0, len(hs_rows), 50):
                    supabase.table("performances").insert(hs_rows[i:i+50]).execute()

        return True
    except Exception as e:
        log.error(f"    Supabase error for {name}: {e}")
        return False


# ── MAIN ──────────────────────────────────────────────────────────────────────
def run(group: str = "all", limit: int = 99999):
    log.info("=" * 60)
    log.info(f"Athletic.net Backfill Scraper — Group: {group}, Limit: {limit}")
    log.info("=" * 60)

    # Fetch athletes without hometown from Supabase
    try:
        query = supabase.table("athletes").select(
            "id, name, college, conference, hs_grad_year, college_year, hometown"
        ).eq("source", "tfrrs")

        # Filter by conference group if specified
        if group != "all" and group in CONFERENCE_GROUPS:
            target_confs = CONFERENCE_GROUPS[group]
            log.info(f"Group {group} conferences: {target_confs}")
            # Supabase doesn't have an "in" for multiple values easily in python client,
            # so we fetch all and filter
        result = query.execute()
        all_athletes = result.data or []
    except Exception as e:
        log.error(f"Failed to fetch athletes: {e}")
        return

    # Filter by group and missing hometown
    if group != "all" and group in CONFERENCE_GROUPS:
        target_confs = set(CONFERENCE_GROUPS[group])
        athletes = [a for a in all_athletes if a.get("conference") in target_confs]
    else:
        athletes = all_athletes

    # Only process those missing hometown
    missing = [a for a in athletes if not a.get("hometown")]
    log.info(f"Athletes without hometown: {len(missing)} / {len(athletes)}")

    saved = errors = skipped = 0
    for i, athlete in enumerate(missing[:limit]):
        if i > 0 and i % 50 == 0:
            log.info(f"Progress: {i}/{min(len(missing),limit)} — {saved} saved, {errors} errors")
        success = backfill_athlete(athlete)
        if success:
            saved += 1
        else:
            errors += 1

    log.info("=" * 60)
    log.info(f"Done: {saved} updated, {errors} not found/errors")
    log.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Athletic.net backfill scraper")
    parser.add_argument("--group", default="all", help="Conference group: 1-6 or 'all'")
    parser.add_argument("--limit", type=int, default=99999, help="Max athletes to process")
    args = parser.parse_args()
    run(group=args.group, limit=args.limit)
