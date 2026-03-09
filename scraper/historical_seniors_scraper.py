"""
historical_seniors_scraper.py
─────────────────────────────
Discovers athletes who were college seniors (4th year) in spring 2023,
spring 2024, or spring 2025 and adds them to the Supabase athletes table.

These are recently graduated athletes not captured by current roster scrapers.

Strategy:
  1. Pull all distinct schools + one athlete URL per school from DB
  2. From each athlete page, find the school's TFRRS team page link
  3. Scrape the team page to get all athletes (TFRRS lists historical athletes too)
  4. For each athlete not already in DB, fetch their TFRRS profile
  5. Parse their season history to determine graduation year
  6. If they graduated spring 2023, 2024, or 2025 → INSERT athlete + performances

Target graduation springs:
  - Spring 2023: last TFRRS season heading is "2022-23" or "2023 Outdoor"
  - Spring 2024: last TFRRS season heading is "2023-24" or "2024 Outdoor"
  - Spring 2025: last TFRRS season heading is "2024-25" or "2025 Outdoor"

Usage:
  python scraper/historical_seniors_scraper.py --dry-run
  python scraper/historical_seniors_scraper.py --limit 20       # 20 schools
  python scraper/historical_seniors_scraper.py                   # all schools
  python scraper/historical_seniors_scraper.py --years 2024 2025
"""

import os
import re
import sys
import time
import argparse
import logging
from typing import Optional

import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

REQUEST_DELAY = 1.2   # seconds between TFRRS requests
TARGET_GRAD_SPRINGS = {2023, 2024, 2025}

TFRRS_BASE = "https://www.tfrrs.org"


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def fetch_page(url: str) -> Optional[BeautifulSoup]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except requests.RequestException as e:
        log.warning(f"  Fetch failed ({url}): {e}")
        return None


# ── Season / year parsing ─────────────────────────────────────────────────────

def parse_spring_year_from_heading(text: str) -> Optional[int]:
    """
    Extract the 'spring year' from a TFRRS season panel heading.

    Examples:
      "2022-23 — University of Oregon"  → 2023
      "2024 Outdoor — Arizona"           → 2024
      "2023 Indoor — Illinois"           → 2023  (belongs to 2022-23 academic year,
                                                    but we treat it as spring 2023 season)
      "2021 Cross Country — Baylor"      → 2021
    """
    text = text.strip()

    # Academic year format: "2022-23" → spring = 2023
    m = re.match(r"(\d{4})-(\d{2})", text)
    if m:
        start = int(m.group(1))
        return start + 1  # 2022-23 → 2023

    # "YYYY Outdoor / Indoor / Cross Country"
    m = re.match(r"(\d{4})\s+(outdoor|indoor|cross|xc|track|field)", text, re.I)
    if m:
        return int(m.group(1))

    # Bare year at start
    m = re.match(r"(\d{4})", text)
    if m:
        return int(m.group(1))

    return None


def get_athlete_season_info(soup: BeautifulSoup) -> dict:
    """
    Parse TFRRS athlete profile soup.

    Returns:
      {
        spring_years: list[int],   sorted spring years of all seasons
        last_spring:  int,         most recent spring year
        first_spring: int,
        num_seasons:  int,
        gender:       "M" | "F" | None
      }
    """
    spring_years = set()

    # Season panel headings
    for el in soup.select(
        "h3.panel-title, h4.panel-title, "
        ".panel-heading h3, .panel-heading h4, "
        ".list-group-item-heading"
    ):
        text = el.get_text(strip=True)
        # Must contain a 4-digit year and a dash/school separator
        if re.search(r"\d{4}", text) and ("—" in text or "-" in text or "–" in text):
            yr = parse_spring_year_from_heading(text)
            if yr and 2015 <= yr <= 2026:
                spring_years.add(yr)

    # Fallback: any heading with a year + school-like pattern
    if not spring_years:
        for el in soup.select("[class*='season'], [class*='year'], h3, h4"):
            text = el.get_text(strip=True)
            yr = parse_spring_year_from_heading(text)
            if yr and 2015 <= yr <= 2026:
                spring_years.add(yr)

    # Gender: infer from page text or header
    gender = None
    page_text = soup.get_text()
    if "women" in page_text.lower()[:500] or "female" in page_text.lower()[:500]:
        gender = "F"
    elif "men" in page_text.lower()[:500] or "male" in page_text.lower()[:500]:
        gender = "M"

    # Try gender from the athlete profile header section
    for el in soup.select(".athlete-header, .profile-header, h1, h2"):
        t = el.get_text(strip=True).lower()
        if "women" in t or "(f)" in t:
            gender = "F"
            break
        if "men" in t or "(m)" in t:
            gender = "M"
            break

    spring_list = sorted(spring_years)
    return {
        "spring_years": spring_list,
        "last_spring": spring_list[-1] if spring_list else None,
        "first_spring": spring_list[0] if spring_list else None,
        "num_seasons": len(spring_list),
        "gender": gender,
    }


# ── Performance parsing ───────────────────────────────────────────────────────

# Mappings for common TFRRS event names → normalized event name
EVENT_NORMALIZE = {
    "100 meters": "100m",
    "200 meters": "200m",
    "400 meters": "400m",
    "800 meters": "800m",
    "1,500 meters": "1500m",
    "1500 meters": "1500m",
    "mile": "Mile",
    "1 mile": "Mile",
    "3,000 meters": "3000m",
    "3000 meters": "3000m",
    "5,000 meters": "5000m",
    "5000 meters": "5000m",
    "10,000 meters": "10000m",
    "10000 meters": "10000m",
    "110 meter hurdles": "110mH",
    "100 meter hurdles": "100mH",
    "400 meter hurdles": "400mH",
    "3,000 meter steeplechase": "3000mSC",
    "3000 meter steeplechase": "3000mSC",
    "4x100 meter relay": "4x100m",
    "4x400 meter relay": "4x400m",
    "high jump": "HJ",
    "pole vault": "PV",
    "long jump": "LJ",
    "triple jump": "TJ",
    "shot put": "SP",
    "discus throw": "DT",
    "hammer throw": "HT",
    "javelin throw": "JT",
    "heptathlon": "Hept",
    "decathlon": "Dec",
    "pentathlon": "Pent",
}

def normalize_event(raw: str) -> str:
    key = raw.strip().lower()
    for k, v in EVENT_NORMALIZE.items():
        if k in key:
            return v
    # Clean up remaining text
    return raw.strip().title()


def mark_to_float(mark_str: str) -> Optional[float]:
    """
    Convert a mark string to a sortable float (lower = better for track, higher = better for field).
    Track: "3:58.45" → 238.45 seconds; "10.23" → 10.23
    Field: "7.85m" → 7.85; "25-8.5" → 7.84 (approx feet to meters)
    Returns None if unparseable.
    """
    s = mark_str.strip().upper().replace(",", "")

    # Time format: M:SS.ss or H:MM:SS.ss
    m = re.match(r"^(\d+):(\d{2})\.(\d+)$", s)
    if m:
        mins = int(m.group(1))
        secs = int(m.group(2))
        frac = float("0." + m.group(3))
        return mins * 60 + secs + frac

    # Pure seconds: "10.23"
    m = re.match(r"^(\d{1,2})\.(\d{2,3})$", s)
    if m:
        return float(s)

    # Metric field: "7.85m" or "7.85 M" or "7.85"
    m = re.match(r"^(\d+\.\d+)\s*M?$", s)
    if m:
        return float(m.group(1))

    # Imperial field: "25-8.5" (feet-inches)
    m = re.match(r"^(\d+)-(\d+\.?\d*)$", s)
    if m:
        feet = int(m.group(1))
        inches = float(m.group(2))
        return round((feet * 12 + inches) * 0.0254, 3)

    return None


def scrape_performances(soup: BeautifulSoup, athlete_id: int) -> list[dict]:
    """
    Parse TFRRS athlete page for their performances.
    Returns list of performance dicts ready for Supabase insert.
    """
    performances = []
    seen = set()  # (event, mark_display) dedup

    # TFRRS performance tables: each season section has a table
    # with columns like: Event | Mark | Wind | Venue | Date | Meet
    for table in soup.select("table"):
        # Try to find season context (year + indoor/outdoor) from nearest heading
        season_heading = None
        for prev in table.find_all_previous(["h3", "h4"]):
            text = prev.get_text(strip=True)
            if re.search(r"\d{4}", text):
                season_heading = text
                break

        season_year = parse_spring_year_from_heading(season_heading or "") if season_heading else None

        # Determine season type
        season_type = "outdoor"
        if season_heading:
            h_lower = season_heading.lower()
            if "indoor" in h_lower:
                season_type = "indoor"
            elif "cross" in h_lower or "xc" in h_lower:
                season_type = "xc"

        headers = [th.get_text(strip=True).lower() for th in table.select("th")]
        if not headers:
            continue

        # Find column indices
        try:
            event_col = next(i for i, h in enumerate(headers) if "event" in h)
        except StopIteration:
            continue

        mark_col = next((i for i, h in enumerate(headers) if "mark" in h or "result" in h), None)
        meet_col = next((i for i, h in enumerate(headers) if "meet" in h), None)

        if mark_col is None:
            continue

        for row in table.select("tbody tr, tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) <= max(event_col, mark_col):
                continue
            if cells[0].name == "th":
                continue  # skip header rows

            event_raw = cells[event_col].get_text(strip=True)
            mark_raw = cells[mark_col].get_text(strip=True)

            if not event_raw or not mark_raw or mark_raw in ("—", "-", "DNF", "DNS", "DQ", "NH", "NM"):
                continue

            event = normalize_event(event_raw)
            mark_display = mark_raw.replace("*", "").strip()  # strip wind-legal markers
            mark_val = mark_to_float(mark_display)

            key = (event, mark_display)
            if key in seen:
                continue
            seen.add(key)

            meet_name = ""
            if meet_col is not None and meet_col < len(cells):
                meet_name = cells[meet_col].get_text(strip=True)

            perf = {
                "athlete_id": athlete_id,
                "event": event,
                "mark": mark_val,
                "mark_display": mark_display,
                "year": season_year,
                "season": season_type,
                "level": "college",
                "meet_name": meet_name[:200] if meet_name else None,
            }
            performances.append(perf)

    return performances


# ── Team page scraping ────────────────────────────────────────────────────────

def find_team_url_from_athlete_page(soup: BeautifulSoup) -> Optional[str]:
    """
    From an athlete's TFRRS page, extract a link to their school's team page.
    TFRRS athlete pages have a breadcrumb or team link.
    """
    # Try breadcrumb links: e.g. "Oregon" links to /teams/tf/OR/Oregon.html
    for link in soup.select("ol.breadcrumb a, nav a, .breadcrumb a"):
        href = link.get("href", "")
        if "/teams/" in href:
            url = href if href.startswith("http") else TFRRS_BASE + href
            return url

    # Try direct team links
    for link in soup.select("a[href*='/teams/']"):
        href = link.get("href", "")
        text = link.get_text(strip=True)
        # Should be a school name, not a generic link
        if href and len(text) > 3 and "roster" not in text.lower():
            url = href if href.startswith("http") else TFRRS_BASE + href
            # Filter out cross-country vs track (prefer tf)
            if "/teams/tf/" in url or "/teams/xc/" in url:
                return url

    return None


def scrape_team_athletes(team_url: str) -> list[dict]:
    """
    Scrape a TFRRS team roster page.
    Returns list of {name, tfrrs_url, tfrrs_id, year_label}
    """
    soup = fetch_page(team_url)
    if not soup:
        return []

    athletes = []
    seen_ids = set()

    for link in soup.select("a[href*='/athletes/']"):
        href = link.get("href", "")
        name = link.get_text(strip=True)

        # Filter junk
        if not name or len(name) < 3 or len(name) > 60:
            continue
        if any(x in name.lower() for x in ["indoor", "outdoor", "cross", "relay", "team"]):
            continue

        m = re.search(r"/athletes/(\d+)/", href)
        if not m:
            continue

        tfrrs_id = m.group(1)
        if tfrrs_id in seen_ids:
            continue
        seen_ids.add(tfrrs_id)

        full_url = href if href.startswith("http") else TFRRS_BASE + href

        # Try to grab year label (SR, JR, SO, FR) from sibling text
        parent = link.find_parent("tr") or link.find_parent("li") or link.parent
        year_label = ""
        if parent:
            text = parent.get_text(separator=" ", strip=True)
            m2 = re.search(r"\b(SR|JR|SO|FR|GR|5TH)\b", text, re.I)
            if m2:
                year_label = m2.group(1).upper()

        athletes.append({
            "name": name,
            "tfrrs_url": full_url,
            "tfrrs_id": tfrrs_id,
            "year_label": year_label,
        })

    log.info(f"  Team page: found {len(athletes)} athlete links")
    return athletes


# ── School helper ─────────────────────────────────────────────────────────────

def extract_school_name_from_tfrrs_url(url: str) -> str:
    """
    "https://www.tfrrs.org/athletes/8230372/Boston_College/Colin_Peattie.html"
    → "Boston College"
    """
    m = re.search(r"/athletes/\d+/([^/]+)/", url)
    if m:
        return m.group(1).replace("_", " ")
    return ""


# ── Supabase helpers ──────────────────────────────────────────────────────────

def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def get_schools_from_db(supabase: Client) -> list[dict]:
    """
    Return distinct schools with a sample tfrrs_url for each.
    """
    result = (
        supabase.table("athletes")
        .select("college, conference, tfrrs_url")
        .eq("source", "tfrrs")
        .not_.is_("tfrrs_url", "null")
        .neq("tfrrs_url", "")
        .execute()
    )

    seen = {}
    for row in (result.data or []):
        college = row.get("college") or ""
        if college and college not in seen:
            seen[college] = {
                "college": college,
                "conference": row.get("conference") or "",
                "sample_url": row["tfrrs_url"],
            }

    schools = list(seen.values())
    log.info(f"Found {len(schools)} distinct schools in DB")
    return schools


def get_existing_source_ids(supabase: Client) -> set:
    """
    Return all existing source_ids to avoid duplicates.
    """
    result = (
        supabase.table("athletes")
        .select("source_id")
        .eq("source", "tfrrs")
        .execute()
    )
    return {row["source_id"] for row in (result.data or []) if row.get("source_id")}


def insert_athlete(supabase: Client, payload: dict, dry_run: bool) -> Optional[int]:
    """
    Insert an athlete row. Returns the new athlete id (or None on dry run / error).
    """
    if dry_run:
        log.info(
            f"  [DRY RUN] Would INSERT: {payload['name']} | "
            f"{payload['college']} | grad {payload.get('hs_grad_year')} | "
            f"source_id={payload['source_id']}"
        )
        return None

    try:
        res = (
            supabase.table("athletes")
            .insert(payload)
            .execute()
        )
        if res.data:
            return res.data[0]["id"]
    except Exception as e:
        log.warning(f"  Insert failed for {payload.get('name')}: {e}")
    return None


def insert_performances(supabase: Client, perfs: list[dict], dry_run: bool):
    """
    Bulk insert performance rows.
    """
    if not perfs:
        return
    if dry_run:
        log.info(f"  [DRY RUN] Would INSERT {len(perfs)} performances")
        return

    try:
        # Insert in batches of 50
        for i in range(0, len(perfs), 50):
            batch = perfs[i:i+50]
            supabase.table("performances").insert(batch).execute()
    except Exception as e:
        log.warning(f"  Performance insert failed: {e}")


# ── Main processing ───────────────────────────────────────────────────────────

def process_athlete(
    athlete_info: dict,
    school: dict,
    existing_ids: set,
    supabase: Client,
    dry_run: bool,
    target_years: set,
) -> Optional[str]:
    """
    Process one athlete candidate. Returns "added", "skipped", or "not_senior".
    """
    tfrrs_id = athlete_info["tfrrs_id"]
    source_id = f"tfrrs_{tfrrs_id}"

    # Already in DB
    if source_id in existing_ids:
        return "skipped"

    url = athlete_info["tfrrs_url"]
    soup = fetch_page(url)
    time.sleep(REQUEST_DELAY)

    if not soup:
        return "skipped"

    season_info = get_athlete_season_info(soup)
    last_spring = season_info.get("last_spring")

    if last_spring not in target_years:
        return "not_senior"

    # Optionally filter: only consider athletes with at least 2 seasons
    # (weeds out athletes who competed briefly / walk-ons)
    if season_info.get("num_seasons", 0) < 2:
        return "not_senior"

    # Infer hs_grad_year: if they were a 4-year senior, hs_grad = last_spring - 4
    # COVID athletes may have 5 seasons; we still infer from last_spring
    hs_grad_year = last_spring - 4

    # Infer gender from URL or page
    gender = season_info.get("gender")
    if not gender:
        url_lower = url.lower()
        if "women" in url_lower or "_w_" in url_lower:
            gender = "F"
        elif "men" in url_lower or "_m_" in url_lower:
            gender = "M"

    # Extract name from TFRRS page title or h1
    name = athlete_info["name"]
    for el in soup.select("h1, h2.athlete-name, .athlete-name"):
        t = el.get_text(strip=True)
        if len(t) > 3 and not any(x in t.lower() for x in ["tfrrs", "roster"]):
            name = t
            break

    # Scrape events from performance tables
    dummy_id = 0  # will replace after insert
    raw_perfs = scrape_performances(soup, dummy_id)
    events = list({p["event"] for p in raw_perfs})

    payload = {
        "name": name,
        "source": "tfrrs",
        "source_id": source_id,
        "tfrrs_url": url,
        "college": school["college"],
        "conference": school["conference"],
        "college_year": "Senior",
        "hs_grad_year": hs_grad_year,
        "gender": gender,
        "events": events or None,
        "is_transfer": None,   # let transfer scraper handle this
    }

    log.info(
        f"  ✓ Senior grad {last_spring}: {name} | {school['college']} | "
        f"hs_grad={hs_grad_year} | {len(raw_perfs)} performances"
    )

    new_id = insert_athlete(supabase, payload, dry_run)

    # Insert performances if we got a real ID
    if new_id and raw_perfs:
        for p in raw_perfs:
            p["athlete_id"] = new_id
        insert_performances(supabase, raw_perfs, dry_run)

    existing_ids.add(source_id)
    return "added"


def main():
    parser = argparse.ArgumentParser(description="TrackScout historical seniors scraper")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to Supabase")
    parser.add_argument("--limit", type=int, default=None, help="Max schools to process")
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=[2023, 2024, 2025],
        metavar="YEAR",
        help="Target graduation spring years (default: 2023 2024 2025)",
    )
    args = parser.parse_args()

    target_years = set(args.years)
    log.info(f"Targeting seniors who graduated spring: {sorted(target_years)}")

    supabase = get_supabase()

    # Get all schools from DB
    schools = get_schools_from_db(supabase)
    if args.limit:
        schools = schools[:args.limit]

    # Load existing source_ids to avoid duplicates
    log.info("Loading existing athlete IDs from DB...")
    existing_ids = get_existing_source_ids(supabase)
    log.info(f"  {len(existing_ids)} existing athletes in DB")

    total_added = 0
    total_skipped = 0
    total_not_senior = 0
    errors = 0

    for school_idx, school in enumerate(schools, 1):
        college = school["college"]
        sample_url = school["sample_url"]
        log.info(f"\n[{school_idx}/{len(schools)}] Processing school: {college}")

        # Step 1: Get team page URL from a sample athlete page
        soup = fetch_page(sample_url)
        time.sleep(REQUEST_DELAY)
        if not soup:
            log.warning(f"  Could not fetch sample athlete page for {college}")
            errors += 1
            continue

        team_url = find_team_url_from_athlete_page(soup)
        if not team_url:
            log.warning(f"  No team page URL found for {college}")
            # Try constructing from URL pattern
            m = re.search(r"/athletes/\d+/([^/]+)/", sample_url)
            if m:
                school_slug = m.group(1)
                # Try common TFRRS team URL patterns
                for sport in ["tf", "xc"]:
                    candidate = f"{TFRRS_BASE}/teams/{sport}/US/{school_slug}.html"
                    test = fetch_page(candidate)
                    time.sleep(REQUEST_DELAY)
                    if test and test.select("a[href*='/athletes/']"):
                        team_url = candidate
                        log.info(f"  Constructed team URL: {team_url}")
                        break
            if not team_url:
                errors += 1
                continue

        log.info(f"  Team page: {team_url}")

        # Step 2: Scrape team roster for all athletes
        team_athletes = scrape_team_athletes(team_url)
        time.sleep(REQUEST_DELAY)

        if not team_athletes:
            log.warning(f"  No athletes found on team page for {college}")
            continue

        # Step 3: Process each athlete not already in DB
        new_for_school = 0
        for ath in team_athletes:
            result = process_athlete(
                ath, school, existing_ids, supabase, args.dry_run, target_years
            )
            if result == "added":
                total_added += 1
                new_for_school += 1
            elif result == "skipped":
                total_skipped += 1
            elif result == "not_senior":
                total_not_senior += 1

        log.info(f"  → Added {new_for_school} new seniors from {college}")

    log.info(f"\n{'='*60}")
    log.info(f"Done.")
    log.info(f"  Schools processed : {len(schools)}")
    log.info(f"  Athletes added    : {total_added}")
    log.info(f"  Already in DB     : {total_skipped}")
    log.info(f"  Not senior/target : {total_not_senior}")
    log.info(f"  Errors            : {errors}")


if __name__ == "__main__":
    main()
