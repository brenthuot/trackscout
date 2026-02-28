"""
Run Stats — TFRRS Scraper
Scrapes college track & field performance data from TFRRS.org
Writes athlete profiles and PRs to Supabase
"""

import os
import time
import re
import logging
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── SUPABASE ──────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── CONSTANTS ─────────────────────────────────────────────────────────────────
BASE_URL = "https://www.tfrrs.org"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}
RATE_LIMIT_SECONDS = 2.0   # be respectful — 1 request every 2 seconds
MAX_ATHLETES_PER_RUN = 300 # keep each run under GitHub's 6-hour limit
BACKFILL_YEARS = [2019, 2020, 2021, 2022, 2023, 2024, 2025]

# ── CONFERENCES AND TEAMS ─────────────────────────────────────────────────────
# TFRRS team keys — these are the school identifiers used in TFRRS URLs
# Format: https://www.tfrrs.org/teams/college/{year}/{key}.html
CONFERENCE_TEAMS = {
    "SEC": [
        "Alabama", "Arkansas", "Auburn", "Florida", "Georgia",
        "Kentucky", "LSU", "Mississippi_State", "Missouri",
        "Ole_Miss", "South_Carolina", "Tennessee", "Texas_AM", "Vanderbilt"
    ],
    "Big_Ten": [
        "Illinois", "Indiana", "Iowa", "Maryland", "Michigan",
        "Michigan_State", "Minnesota", "Nebraska", "Northwestern",
        "Ohio_State", "Penn_State", "Purdue", "Rutgers", "Wisconsin"
    ],
    "ACC": [
        "Boston_College", "Clemson", "Duke", "Florida_State",
        "Georgia_Tech", "Louisville", "Miami_FL", "NC_State",
        "North_Carolina", "Notre_Dame", "Pittsburgh", "Syracuse",
        "Virginia", "Virginia_Tech", "Wake_Forest"
    ],
    "Big_12": [
        "Baylor", "BYU", "Iowa_State", "Kansas", "Kansas_State",
        "Oklahoma_State", "TCU", "Texas", "Texas_Tech", "West_Virginia"
    ],
}

# Map TFRRS event names to normalized names
EVENT_MAP = {
    "60": "60m", "60m": "60m",
    "60 Hurdles": "60mH", "60m Hurdles": "60mH", "60H": "60mH",
    "100": "100m", "100m": "100m",
    "200": "200m", "200m": "200m",
    "400": "400m", "400m": "400m",
    "800": "800m", "800m": "800m",
    "1500": "1500m", "1500m": "1500m",
    "Mile": "Mile", "1 Mile": "Mile",
    "3000": "3000m", "3000m": "3000m",
    "3000 Steeplechase": "3000mSC", "3000m Steeplechase": "3000mSC",
    "5000": "5000m", "5000m": "5000m",
    "10,000": "10000m", "10000": "10000m", "10000m": "10000m",
    "110 Hurdles": "110mH", "110m Hurdles": "110mH", "110H": "110mH",
    "100 Hurdles": "100mH", "100m Hurdles": "100mH",
    "400 Hurdles": "400mH", "400m Hurdles": "400mH", "400H": "400mH",
    "High Jump": "HJ", "Long Jump": "LJ",
    "Triple Jump": "TJ", "Pole Vault": "PV",
    "Shot Put": "SP", "Discus": "DT", "Discus Throw": "DT",
    "Hammer": "HT", "Hammer Throw": "HT",
    "Javelin": "JT", "Javelin Throw": "JT",
    "Weight Throw": "WT",
    "Heptathlon": "Hept", "Decathlon": "Dec", "Pentathlon": "Pent",
    "4x100": "4x100m", "4 x 100": "4x100m",
    "4x400": "4x400m", "4 x 400": "4x400m",
}


# ── HELPERS ───────────────────────────────────────────────────────────────────
def get_page(url: str, retries: int = 3) -> BeautifulSoup | None:
    """Fetch a page with retries and rate limiting."""
    for attempt in range(retries):
        try:
            time.sleep(RATE_LIMIT_SECONDS)
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                return BeautifulSoup(resp.text, "lxml")
            elif resp.status_code == 429:
                log.warning(f"Rate limited on {url}, waiting 30s...")
                time.sleep(30)
            elif resp.status_code == 404:
                log.warning(f"404 not found: {url}")
                return None
            else:
                log.warning(f"HTTP {resp.status_code} for {url}")
        except Exception as e:
            log.error(f"Request error (attempt {attempt+1}): {e}")
            time.sleep(5)
    return None


def normalize_event(raw: str) -> str:
    """Normalize a raw TFRRS event name to our standard format."""
    raw = raw.strip()
    return EVENT_MAP.get(raw, raw)


def parse_mark_to_seconds(mark_str: str) -> float | None:
    """
    Convert a time string to seconds for storage.
    '4:12.34' → 252.34
    '10.45' → 10.45
    '6.72' → 6.72
    Field events stored as-is (meters).
    """
    if not mark_str:
        return None
    mark_str = mark_str.strip().replace(",", "")
    try:
        # MM:SS.ss format
        if ":" in mark_str:
            parts = mark_str.split(":")
            if len(parts) == 2:
                return float(parts[0]) * 60 + float(parts[1])
            elif len(parts) == 3:
                return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
        # Plain seconds or meters
        return float(mark_str)
    except ValueError:
        return None


def extract_year_from_text(text: str) -> int | None:
    """Extract a 4-digit year from a string."""
    match = re.search(r"\b(20\d{2})\b", text)
    return int(match.group(1)) if match else None


def determine_season(event: str) -> str:
    """Determine if an event is indoor or outdoor."""
    indoor = {"60m", "60mH", "3000m", "WT", "Pent", "Mile"}
    if event in indoor:
        return "indoor"
    return "outdoor"


# ── TEAM PAGE SCRAPER ─────────────────────────────────────────────────────────
def get_athlete_urls_from_team(conference: str, team: str, year: int) -> list[dict]:
    """
    Scrape a TFRRS team page and return a list of athlete URLs.
    Returns: [{"url": "...", "name": "...", "college": "...", "conference": "..."}]
    """
    url = f"{BASE_URL}/teams/college/{year}/{team}.html"
    log.info(f"Fetching team page: {url}")

    soup = get_page(url)
    if not soup:
        # Try alternate URL formats TFRRS uses
        url = f"{BASE_URL}/teams/{team}_{year}.html"
        soup = get_page(url)
    if not soup:
        log.warning(f"Could not fetch team page for {team} {year}")
        return []

    athletes = []
    # TFRRS roster tables contain links to /athletes/ID/Name.html
    for link in soup.find_all("a", href=re.compile(r"/athletes/\d+")):
        href = link.get("href", "")
        name = link.get_text(strip=True)
        if name and len(name) > 2:
            full_url = href if href.startswith("http") else BASE_URL + href
            athletes.append({
                "url": full_url,
                "name": name,
                "college": team.replace("_", " "),
                "conference": conference,
            })

    # Deduplicate by URL
    seen = set()
    unique = []
    for a in athletes:
        if a["url"] not in seen:
            seen.add(a["url"])
            unique.append(a)

    log.info(f"  Found {len(unique)} athletes for {team} {year}")
    return unique


# ── ATHLETE PAGE SCRAPER ───────────────────────────────────────────────────────
def scrape_athlete(info: dict) -> dict | None:
    """
    Scrape a single TFRRS athlete page.
    Returns structured athlete data with performances.
    """
    url = info["url"]
    log.info(f"  Scraping athlete: {info['name']} — {url}")

    soup = get_page(url)
    if not soup:
        return None

    # Extract TFRRS athlete ID from URL
    id_match = re.search(r"/athletes/(\d+)", url)
    if not id_match:
        return None
    tfrrs_id = id_match.group(1)

    # ── Athlete name ──
    name_tag = (
        soup.find("h3", class_=re.compile("athlete-name")) or
        soup.find("h1") or
        soup.find("h2")
    )
    name = name_tag.get_text(strip=True) if name_tag else info["name"]

    # ── School / College ──
    school_tag = soup.find("a", href=re.compile(r"/teams/"))
    college = school_tag.get_text(strip=True) if school_tag else info.get("college", "")

    # ── Hometown (if listed) ──
    hometown = ""
    hometown_state = ""
    for tag in soup.find_all(["span", "div", "td"], string=re.compile(r"Hometown|From", re.I)):
        sibling = tag.find_next_sibling()
        if sibling:
            hometown = sibling.get_text(strip=True)
            # Extract state from "City, ST" format
            parts = hometown.split(",")
            if len(parts) >= 2:
                hometown_state = parts[-1].strip()[:2].upper()
            break

    # ── HS / Grad year ──
    hs_year = None
    hs_name = ""
    for tag in soup.find_all(text=re.compile(r"Class of|Grad|20\d{2}")):
        yr = extract_year_from_text(str(tag))
        if yr and 2015 <= yr <= 2026:
            hs_year = yr
            break

    # ── Parse performance tables ──
    performances = []
    events_set = set()

    # TFRRS uses tables with class "table" — each table may be a season
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue

        current_year = None
        current_season = None

        for row in rows:
            # Year/season header rows
            row_text = row.get_text(" ", strip=True)
            yr = extract_year_from_text(row_text)
            if yr:
                current_year = yr
                current_season = "indoor" if "indoor" in row_text.lower() else "outdoor"

            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            cell_texts = [c.get_text(strip=True) for c in cells]

            # Look for event + mark pattern
            # TFRRS format varies but generally: Event | Mark | Meet | Date
            for i, text in enumerate(cell_texts):
                norm_event = normalize_event(text)
                if norm_event in EVENT_MAP.values() and i + 1 < len(cell_texts):
                    mark_raw = cell_texts[i + 1]
                    mark_num = parse_mark_to_seconds(mark_raw)
                    if mark_num and mark_num > 0:
                        meet = cell_texts[i + 2] if i + 2 < len(cell_texts) else ""
                        performances.append({
                            "event": norm_event,
                            "mark": mark_num,
                            "mark_display": mark_raw,
                            "year": current_year,
                            "season": current_season or determine_season(norm_event),
                            "level": "college",
                            "meet_name": meet,
                        })
                        events_set.add(norm_event)

    athlete_data = {
        "id": f"tfrrs_{tfrrs_id}",
        "name": name,
        "source": "tfrrs",
        "source_id": tfrrs_id,
        "college": college or info.get("college", ""),
        "conference": info.get("conference", ""),
        "hometown": hometown,
        "hometown_state": hometown_state,
        "hs_grad_year": hs_year,
        "events": list(events_set),
        "tfrrs_url": url,
        "updated_at": datetime.utcnow().isoformat(),
        "performances": performances,
    }

    log.info(f"    → {len(performances)} performances across {len(events_set)} events")
    return athlete_data


# ── SUPABASE WRITER ────────────────────────────────────────────────────────────
def save_athlete(data: dict) -> bool:
    """Upsert athlete + performances to Supabase."""
    performances = data.pop("performances", [])

    try:
        # Upsert athlete (insert or update if already exists)
        supabase.table("athletes").upsert(data, on_conflict="id").execute()

        if performances:
            # Delete existing performances for this athlete to avoid duplicates
            supabase.table("performances") \
                .delete() \
                .eq("athlete_id", data["id"]) \
                .execute()

            # Insert fresh batch
            perf_rows = [
                {**p, "athlete_id": data["id"], "source": "tfrrs"}
                for p in performances
                if p.get("mark") and p.get("event")
            ]
            if perf_rows:
                # Insert in batches of 50
                for i in range(0, len(perf_rows), 50):
                    supabase.table("performances") \
                        .insert(perf_rows[i:i+50]) \
                        .execute()

        log.info(f"    ✓ Saved {data['name']} ({len(performances)} perfs)")
        return True

    except Exception as e:
        log.error(f"    ✗ Supabase error for {data.get('name')}: {e}")
        return False


# ── BACKFILL RUNNER ────────────────────────────────────────────────────────────
def get_already_scraped_ids() -> set:
    """Get IDs already in the database to avoid re-scraping."""
    try:
        result = supabase.table("athletes") \
            .select("source_id") \
            .eq("source", "tfrrs") \
            .execute()
        return {row["source_id"] for row in result.data}
    except Exception as e:
        log.error(f"Could not fetch existing IDs: {e}")
        return set()


def run_scraper():
    """Main entry point — scrapes all conferences for all backfill years."""
    log.info("=" * 60)
    log.info("Run Stats TFRRS Scraper starting")
    log.info(f"Backfill years: {BACKFILL_YEARS}")
    log.info(f"Conferences: {list(CONFERENCE_TEAMS.keys())}")
    log.info("=" * 60)

    already_done = get_already_scraped_ids()
    log.info(f"Already in DB: {len(already_done)} athletes — will skip these")

    total_saved = 0
    total_skipped = 0
    total_errors = 0

    for year in BACKFILL_YEARS:
        if total_saved >= MAX_ATHLETES_PER_RUN:
            log.info(f"Reached max athletes per run ({MAX_ATHLETES_PER_RUN}), stopping")
            break

        for conference, teams in CONFERENCE_TEAMS.items():
            for team in teams:
                if total_saved >= MAX_ATHLETES_PER_RUN:
                    break

                athlete_urls = get_athlete_urls_from_team(conference, team, year)

                for athlete_info in athlete_urls:
                    if total_saved >= MAX_ATHLETES_PER_RUN:
                        break

                    # Extract ID from URL to check if already scraped
                    id_match = re.search(r"/athletes/(\d+)", athlete_info["url"])
                    if id_match and id_match.group(1) in already_done:
                        total_skipped += 1
                        continue

                    data = scrape_athlete(athlete_info)
                    if data:
                        success = save_athlete(data)
                        if success:
                            total_saved += 1
                            already_done.add(data["source_id"])
                        else:
                            total_errors += 1
                    else:
                        total_errors += 1

    log.info("=" * 60)
    log.info(f"Run complete: {total_saved} saved, {total_skipped} skipped, {total_errors} errors")
    log.info("=" * 60)


if __name__ == "__main__":
    run_scraper()
