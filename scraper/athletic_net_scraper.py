"""
Run Stats — Athletic.net Backfill Scraper v3
Uses Google search (site:athletic.net "Name") to find athlete profiles,
then scrapes hometown, high school, and HS performances directly.

Usage:
    python athletic_net_scraper.py               # backfill all missing hometown
    python athletic_net_scraper.py --limit 5     # test with 5 athletes
    python athletic_net_scraper.py --group 1     # conference group 1-6
    python athletic_net_scraper.py --all         # re-process all athletes
"""

import os, time, re, logging, argparse
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
RATE_LIMIT = 2.5   # seconds between requests

GOOGLE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.bing.com/",
}

ANET_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.athletic.net/",
}

# Matches TFRRS scraper conference groups exactly
CONFERENCE_GROUPS = {
    "1": ["SEC", "West Coast"],
    "2": ["Big Ten", "Big Sky"],
    "3": ["ACC", "Ivy League"],
    "4": ["Big 12", "Atlantic 10"],
    "5": ["Pac-12", "American"],
    "6": ["Mountain West", "Big East"],
}

ANET_EVENT_MAP = {
    "100 Meters": "100m", "100m": "100m", "100 Meter Dash": "100m",
    "200 Meters": "200m", "200m": "200m", "200 Meter Dash": "200m",
    "400 Meters": "400m", "400m": "400m", "400 Meter Dash": "400m",
    "800 Meters": "800m", "800m": "800m", "800 Meter Run": "800m",
    "1,500 Meters": "1500m", "1500 Meters": "1500m", "1500m": "1500m",
    "1 Mile Run": "Mile", "Mile": "Mile", "Mile Run": "Mile",
    "3,000 Meters": "3000m", "3000 Meters": "3000m",
    "3,000 Meter Steeplechase": "3000SC", "3000 Steeplechase": "3000SC",
    "5,000 Meters": "5000m", "5000 Meters": "5000m",
    "10,000 Meters": "10000m",
    "110 Meter Hurdles": "110mH", "110m Hurdles": "110mH",
    "100 Meter Hurdles": "100mH", "100m Hurdles": "100mH",
    "400 Meter Hurdles": "400mH", "400m Hurdles": "400mH",
    "High Jump": "HJ", "Long Jump": "LJ", "Triple Jump": "TJ",
    "Pole Vault": "PV", "Shot Put": "SP", "Discus Throw": "DT",
    "Hammer Throw": "HT", "Javelin Throw": "JT", "Weight Throw": "WT",
    "Pentathlon": "Pent", "Heptathlon": "Hept", "Decathlon": "Dec",
}

RELAY_KEYWORDS = {"relay", "4x", "medley", "4 x", "sprint medley", "distance medley"}

US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC"
}

# Track Google blocks so we can back off
_google_blocked = False


# ── MARK PARSER ───────────────────────────────────────────────────────────────
def parse_mark(s: str) -> float | None:
    if not s:
        return None
    s = str(s).strip().lstrip("*").strip()
    s = re.sub(r'\([^)]+\)', '', s).strip()
    if re.match(r'^(DNF|DNS|DQ|NM|NH|ND|SCR|NT)$', s, re.I):
        return None
    try:
        if ":" in s:
            parts = s.split(":")
            if len(parts) == 2:
                return round(float(parts[0]) * 60 + float(parts[1]), 3)
        m = re.match(r'^(\d+)-(\d+(?:\.\d+)?)$', s)
        if m:
            feet, inches = float(m.group(1)), float(m.group(2))
            return round((feet * 12 + inches) * 0.0254, 3)
        cleaned = re.sub(r'[^0-9.]', '', s)
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


# ── GOOGLE SEARCH ─────────────────────────────────────────────────────────────
def google_find_anet_url(name: str, hs_grad_year: int | None) -> list[str]:
    """
    Search DuckDuckGo Lite for site:athletic.net/athlete "Name".
    DDG Lite serves real HTML without JavaScript rendering.
    """
    global _google_blocked
    if _google_blocked:
        return []

    query = f'site:athletic.net/athlete "{name}"'
    if hs_grad_year:
        query += f' {hs_grad_year}'

    url = "https://lite.duckduckgo.com/lite/"
    params = {"q": query}

    try:
        time.sleep(RATE_LIMIT)
        r = requests.get(url, headers=GOOGLE_HEADERS, params=params, timeout=20)
        log.info(f"  [DDG] Status: {r.status_code}, query: {query}")

        if r.status_code == 429:
            log.warning("  [DDG] Rate limited — backing off 60s")
            _google_blocked = True
            time.sleep(60)
            return []

        if r.status_code != 200:
            log.warning(f"  [DDG] HTTP {r.status_code}")
            return []

        soup = BeautifulSoup(r.text, "lxml")

        # DDG Lite puts result URLs in <a class="result-link"> tags
        all_hrefs = [a.get("href", "") for a in soup.find_all("a", href=True)]
        anet_hrefs = [h for h in all_hrefs if "athletic.net" in h]
        log.info(f"  [DDG] Total hrefs: {len(all_hrefs)}, anet hrefs: {anet_hrefs[:5]}")

        # Also log a snippet if no results
        if not anet_hrefs:
            log.info(f"  [DDG] Body snippet: {r.text[200:600]}")

        urls = []
        for href in all_hrefs:
            # DDG Lite links directly, no redirect wrapping
            if re.search(r'athletic\.net/athlete/\d+', href):
                urls.append(re.sub(r'\?.*$', '', href))  # strip query params
            elif re.search(r'athletic\.net/TrackAndField/Athlete\.aspx\?AID=\d+', href):
                aid = re.search(r'AID=(\d+)', href).group(1)
                urls.append(f"https://www.athletic.net/athlete/{aid}/track-and-field/")

        # Deduplicate
        seen = set()
        unique = [u for u in urls if not (u in seen or seen.add(u))]

        log.info(f"  [DDG] Found {len(unique)} profile URLs: {unique[:3]}")
        return unique

    except Exception as e:
        log.error(f"  [DDG] Error: {e}")
        return []


# ── ATHLETIC.NET PROFILE SCRAPER ──────────────────────────────────────────────
def scrape_profile(url: str, expected_name: str, hs_grad_year: int | None) -> dict | None:
    """
    Scrape an athletic.net athlete profile page.
    Returns {hometown, hometown_state, high_school, performances, anet_url} or None.
    """
    try:
        time.sleep(RATE_LIMIT)
        r = requests.get(url, headers=ANET_HEADERS, timeout=20)
        log.info(f"  [Profile] {r.status_code} {url[:70]}")
        if r.status_code != 200:
            return None
    except Exception as e:
        log.error(f"  [Profile] Error fetching {url}: {e}")
        return None

    soup = BeautifulSoup(r.text, "lxml")
    page_text = soup.get_text(" ", strip=True)

    # ── Verify name match ────────────────────────────────────────────────────
    page_name = ""
    for tag in soup.find_all(["h1", "h2"]):
        t = tag.get_text(strip=True)
        if 2 < len(t) < 80:
            page_name = t
            break

    expected_parts = expected_name.lower().split()
    if len(expected_parts) >= 2:
        if not all(p in page_name.lower() for p in expected_parts[:2]):
            log.debug(f"    Name mismatch: expected '{expected_name}', got '{page_name}'")
            return None

    # ── Extract hometown ─────────────────────────────────────────────────────
    hometown = ""
    hometown_state = ""
    for text in soup.stripped_strings:
        t = text.strip()
        m = re.match(r'^([\w\s\.\'\-]{2,40}),\s+([A-Z]{2})$', t)
        if m and m.group(2) in US_STATES:
            hometown = t
            hometown_state = m.group(2)
            break
    if not hometown:
        m = re.search(r'\b([\w\s\.\'\-]{2,30}),\s+([A-Z]{2})\b', page_text)
        if m and m.group(2) in US_STATES:
            hometown = m.group(0).strip()
            hometown_state = m.group(2)

    # ── Extract high school ──────────────────────────────────────────────────
    high_school = ""
    for tag in soup.find_all(["h3", "h4", "a", "span", "td", "li", "p"]):
        t = tag.get_text(strip=True)
        if 5 <= len(t) <= 80 and re.search(r'\b(High School|High|Academy|Prep|School)\b', t, re.I):
            cleaned = re.sub(r'\s+', ' ', t).strip()
            if cleaned not in ("High School", "HS", "School", "Academy", "Prep"):
                high_school = cleaned[:80]
                break
    if not high_school:
        m = re.search(
            r'(?:School|Team)[\s:–]+([A-Z][A-Za-z\s\.\-\']{2,50}(?:High|HS|Academy|Prep|School)[A-Za-z\s\.\-\']*)',
            page_text
        )
        if m:
            high_school = m.group(1).strip()[:80]

    # ── Extract HS performances from results tables ──────────────────────────
    performances = []
    current_year = None

    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells:
                continue
            # Track year from any cell
            for cell in cells:
                yr = re.search(r'\b(20[01][0-9]|202[0-6])\b', cell)
                if yr:
                    current_year = int(yr.group(1))
            if len(cells) < 2:
                continue
            for i, cell in enumerate(cells):
                if any(kw in cell.lower() for kw in RELAY_KEYWORDS):
                    continue
                event_norm = ANET_EVENT_MAP.get(cell.strip())
                if event_norm and i + 1 < len(cells):
                    mark_num = parse_mark(cells[i + 1])
                    if mark_num and 0 < mark_num < 100000:
                        performances.append({
                            "event": event_norm,
                            "mark": mark_num,
                            "mark_display": cells[i + 1],
                            "year": current_year,
                            "season": "outdoor",
                            "level": "hs",
                            "meet_name": "",
                        })

    log.info(f"    hometown={hometown!r}, HS={high_school!r}, {len(performances)} perfs")
    return {
        "hometown": hometown,
        "hometown_state": hometown_state,
        "high_school": high_school,
        "performances": performances,
        "anet_url": url,
    }


# ── FIND BEST MATCH ───────────────────────────────────────────────────────────
def find_best_match(name: str, hs_grad_year: int | None) -> dict | None:
    """Google for the athlete's athletic.net URL, then scrape the profile."""
    urls = google_find_anet_url(name, hs_grad_year)
    if not urls:
        return None
    for url in urls[:3]:
        profile = scrape_profile(url, name, hs_grad_year)
        if profile:
            return profile
    return None


# ── SUPABASE SAVE ─────────────────────────────────────────────────────────────
def backfill_athlete(athlete_row: dict) -> bool:
    athlete_id = athlete_row["id"]
    name = athlete_row["name"]
    hs_grad_year = athlete_row.get("hs_grad_year")

    log.info(f"  Searching: {name} (HS {hs_grad_year or '?'})")

    profile = find_best_match(name, hs_grad_year)
    if not profile:
        log.info(f"    ✗ No match: {name}")
        return False

    log.info(f"    ✓ Matched: {name}")

    update_data = {"updated_at": datetime.utcnow().isoformat()}
    if profile["hometown"]:
        update_data["hometown"] = profile["hometown"]
    if profile["hometown_state"]:
        update_data["hometown_state"] = profile["hometown_state"]
    if profile["high_school"]:
        update_data["high_school"] = profile["high_school"]

    try:
        supabase.table("athletes").update(update_data).eq("id", athlete_id).execute()

        if profile["performances"]:
            hs_rows = [
                {**p, "athlete_id": athlete_id, "source": "athletic_net"}
                for p in profile["performances"]
                if p.get("mark") and p.get("event")
            ]
            if hs_rows:
                supabase.table("performances")\
                    .delete()\
                    .eq("athlete_id", athlete_id)\
                    .eq("source", "athletic_net")\
                    .execute()
                for i in range(0, len(hs_rows), 50):
                    supabase.table("performances").insert(hs_rows[i:i+50]).execute()

        return True
    except Exception as e:
        log.error(f"    Supabase error for {name}: {e}")
        return False


# ── MAIN ──────────────────────────────────────────────────────────────────────
def run(group: str = "all", limit: int = 99999, process_all: bool = False):
    log.info("=" * 60)
    log.info(f"Athletic.net Backfill v3 — Group: {group}, Limit: {limit}, All: {process_all}")
    log.info("=" * 60)

    try:
        result = supabase.table("athletes").select(
            "id, name, college, conference, hs_grad_year, college_year, hometown"
        ).eq("source", "tfrrs").execute()
        all_athletes = result.data or []
    except Exception as e:
        log.error(f"Failed to fetch athletes: {e}")
        return

    # Filter by conference group
    if group != "all" and group in CONFERENCE_GROUPS:
        target_confs = set(CONFERENCE_GROUPS[group])
        log.info(f"Group {group} conferences: {target_confs}")
        athletes = [a for a in all_athletes if a.get("conference") in target_confs]
    else:
        athletes = all_athletes

    # Only missing hometown unless --all
    if not process_all:
        to_process = [a for a in athletes if not a.get("hometown")]
    else:
        to_process = athletes

    log.info(f"Athletes to process: {len(to_process)} / {len(athletes)}")

    saved = errors = 0
    for i, athlete in enumerate(to_process[:limit]):
        if i > 0 and i % 50 == 0:
            log.info(f"Progress: {i}/{min(len(to_process), limit)} — {saved} saved, {errors} errors")
        if backfill_athlete(athlete):
            saved += 1
        else:
            errors += 1

    log.info("=" * 60)
    log.info(f"Done: {saved} updated, {errors} not found/errors")
    log.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Athletic.net backfill scraper v3")
    parser.add_argument("--group", default="all", help="Conference group 1-6 or 'all'")
    parser.add_argument("--limit", type=int, default=99999, help="Max athletes to process")
    parser.add_argument("--all", action="store_true", dest="process_all",
                        help="Re-process all athletes, not just those missing hometown")
    parser.add_argument("--name", default=None, help="Test with a specific athlete name")
    args = parser.parse_args()

    if args.name:
        # Quick test mode
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
        result = google_find_anet_url(args.name, None)
        print(f"Result for '{args.name}': {result}")
    else:
        run(group=args.group, limit=args.limit, process_all=args.process_all)
