"""
hometown_enrichment.py
──────────────────────────────────────────────────────────────────────────────
Two-step pipeline to fill in missing athlete hometowns.

Step 1 — TFRRS Profile Pass
  Revisits TFRRS athlete profile pages for every athlete where hometown IS
  NULL. Tries four extraction strategies in order:
    1. Labelled dl/dt/dd fields  ("Hometown" label + adjacent dd value)
    2. Key-value table rows       (<tr> with "Hometown" th/td)
    3. Plain-text regex           "Hometown: City, ST" anywhere in body text
    4. Unstructured bio scan      first recognisable "City, ST" in page header
  Also extracts high_school when missing.
  If high_school contains a parenthetical city like
  "Jefferson HS (Springfield, IL)" that is used as a hometown fallback.

Step 2 — Roster Page Pass
  Finds all schools that still have athletes with null hometowns, then scrapes
  their official Sidearm roster pages using requests + BeautifulSoup.
  Builds an in-memory normalised-name lookup at startup — no per-athlete DB
  queries during the scrape. Updates only — never inserts new athletes.

After both steps, geocode_backfill.py runs automatically on non-dry runs so
all newly written hometowns get coordinates immediately.

Usage:
  python scraper/hometown_enrichment.py                    # both steps
  python scraper/hometown_enrichment.py --dry-run          # preview, no writes
  python scraper/hometown_enrichment.py --step1-only       # TFRRS pass only
  python scraper/hometown_enrichment.py --step2-only       # roster pass only
  python scraper/hometown_enrichment.py --limit 200        # cap athletes/step
  python scraper/hometown_enrichment.py --skip-geocode     # skip backfill
"""

import argparse
import logging
import os
import re
import subprocess
import sys
import time
import unicodedata
from typing import Optional

import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ["SUPABASE_KEY"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

REQUEST_DELAY = 1.2   # seconds between TFRRS requests
ROSTER_DELAY  = 1.5   # seconds between roster page fetches

# ── State tables ──────────────────────────────────────────────────────────────

US_STATE_ABBRS = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC",
}

FULL_STATE_TO_ABBR = {
    "Alabama":"AL","Alaska":"AK","Arizona":"AZ","Arkansas":"AR","California":"CA",
    "Colorado":"CO","Connecticut":"CT","Delaware":"DE","Florida":"FL","Georgia":"GA",
    "Hawaii":"HI","Idaho":"ID","Illinois":"IL","Indiana":"IN","Iowa":"IA",
    "Kansas":"KS","Kentucky":"KY","Louisiana":"LA","Maine":"ME","Maryland":"MD",
    "Massachusetts":"MA","Michigan":"MI","Minnesota":"MN","Mississippi":"MS",
    "Missouri":"MO","Montana":"MT","Nebraska":"NE","Nevada":"NV",
    "New Hampshire":"NH","New Jersey":"NJ","New Mexico":"NM","New York":"NY",
    "North Carolina":"NC","North Dakota":"ND","Ohio":"OH","Oklahoma":"OK",
    "Oregon":"OR","Pennsylvania":"PA","Rhode Island":"RI","South Carolina":"SC",
    "South Dakota":"SD","Tennessee":"TN","Texas":"TX","Utah":"UT","Vermont":"VT",
    "Virginia":"VA","Washington":"WA","West Virginia":"WV","Wisconsin":"WI",
    "Wyoming":"WY","District of Columbia":"DC",
}

DOTTED_ABBR = {
    "Ala":"AL","Ariz":"AZ","Ark":"AR","Calif":"CA","Colo":"CO","Conn":"CT",
    "Del":"DE","Fla":"FL","Ga":"GA","Ill":"IL","Ind":"IN","Kan":"KS","Ky":"KY",
    "La":"LA","Mass":"MA","Md":"MD","Mich":"MI","Minn":"MN","Miss":"MS",
    "Mo":"MO","Mont":"MT","Neb":"NE","Nev":"NV","Okla":"OK","Ore":"OR",
    "Pa":"PA","Tenn":"TN","Tex":"TX","Va":"VA","Vt":"VT","Wash":"WA",
    "Wis":"WI","Wyo":"WY","N.H":"NH","N.J":"NJ","N.M":"NM","N.Y":"NY",
    "N.C":"NC","N.D":"ND","R.I":"RI","S.C":"SC","S.D":"SD","W.Va":"WV","D.C":"DC",
}

# ── State helpers ─────────────────────────────────────────────────────────────

def _normalise_state(raw: str) -> Optional[str]:
    """Return a 2-letter postal code for any state representation, or None."""
    s = raw.strip().rstrip(".")
    if s in US_STATE_ABBRS:
        return s
    if s in FULL_STATE_TO_ABBR:
        return FULL_STATE_TO_ABBR[s]
    titled = s.title()
    if titled in FULL_STATE_TO_ABBR:
        return FULL_STATE_TO_ABBR[titled]
    for dot, code in DOTTED_ABBR.items():
        if s.lower() == dot.lower():
            return code
    return None


def parse_city_state(raw: str) -> Optional[str]:
    """
    Normalise any 'City, State' string into canonical 'City, ST' form.
    Returns None for international addresses or unrecognisable input.
    """
    if not raw:
        return None
    raw = raw.strip()
    raw = re.sub(r",?\s*U\.?S\.?A\.?\s*$", "", raw, flags=re.I).strip()
    raw = raw.strip("()")
    # Split on last comma
    m = re.match(r"^(.+?),\s*(.+)$", raw)
    if not m:
        return None
    city  = m.group(1).strip().rstrip(".")
    state = _normalise_state(m.group(2))
    if not state or not city or len(city) < 2:
        return None
    return f"{city}, {state}"


# ── Name normalisation ────────────────────────────────────────────────────────

def normalize(name: str) -> str:
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = name.lower().strip()
    name = re.sub(r"\b(jr\.?|sr\.?|ii|iii|iv)\b", "", name)
    name = re.sub(r"[^a-z\s\-]", "", name)
    return re.sub(r"\s+", " ", name).strip()


# ── HTTP helper ───────────────────────────────────────────────────────────────

def fetch_soup(url: str) -> Optional[BeautifulSoup]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except requests.RequestException as e:
        log.warning(f"  GET failed ({url}): {e}")
        return None


# ── Supabase helpers ──────────────────────────────────────────────────────────

def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def _paginate(supabase: Client, query_fn) -> list[dict]:
    """Run a Supabase query with 1000-row pagination. query_fn receives (offset)."""
    rows, page_size, offset = [], 1000, 0
    while True:
        batch = query_fn(offset, page_size)
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


def fetch_athletes_missing_hometown(supabase: Client, limit: int = 0) -> list[dict]:
    """Athletes where hometown IS NULL and tfrrs_url IS NOT NULL."""
    def query(offset, size):
        return (
            supabase.table("athletes")
            .select("id, name, college, tfrrs_url, hometown, high_school")
            .eq("source", "tfrrs")
            .is_("hometown", "null")
            .not_.is_("tfrrs_url", "null")
            .neq("tfrrs_url", "")
            .range(offset, offset + size - 1)
            .execute()
            .data or []
        )
    rows = _paginate(supabase, query)
    return rows[:limit] if limit else rows


def fetch_schools_with_missing_hometowns(supabase: Client) -> list[str]:
    """Distinct college names that still have athletes missing hometowns."""
    result = (
        supabase.table("athletes")
        .select("college")
        .eq("source", "tfrrs")
        .is_("hometown", "null")
        .execute()
    )
    seen = set()
    for row in (result.data or []):
        college = (row.get("college") or "").strip()
        if college:
            seen.add(college)
    return sorted(seen)


def fetch_all_athletes_for_schools(supabase: Client, colleges: list[str]) -> list[dict]:
    """
    Return ALL athletes from the given colleges regardless of hometown status.
    Batched to stay within Supabase filter URL limits.
    """
    rows = []
    batch_size = 50
    for i in range(0, len(colleges), batch_size):
        chunk = colleges[i:i + batch_size]
        result = (
            supabase.table("athletes")
            .select("id, name, college, hometown")
            .in_("college", chunk)
            .execute()
        )
        rows.extend(result.data or [])
    return rows


def update_athlete(
    supabase: Client,
    athlete_id: int,
    payload: dict,
    dry_run: bool,
) -> bool:
    if dry_run:
        log.info(f"  [DRY RUN] Would update id={athlete_id}: {payload}")
        return True
    try:
        supabase.table("athletes").update(payload).eq("id", athlete_id).execute()
        return True
    except Exception as e:
        log.error(f"  DB update failed (id={athlete_id}): {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — TFRRS Profile Pass
# ══════════════════════════════════════════════════════════════════════════════

def _hs_parenthetical_hometown(high_school: str) -> Optional[str]:
    """
    'Jefferson HS (Springfield, IL)' → 'Springfield, IL'
    Used as a hometown of last resort when TFRRS has no explicit hometown field.
    """
    if not high_school:
        return None
    m = re.search(r"\(([^)]+)\)\s*$", high_school)
    return parse_city_state(m.group(1)) if m else None


def _strategy_dl(soup: BeautifulSoup) -> Optional[str]:
    """Strategy 1: <dt>Hometown</dt><dd>City, ST</dd>"""
    for dt in soup.find_all("dt"):
        if "hometown" in dt.get_text(strip=True).lower():
            dd = dt.find_next_sibling("dd")
            if dd:
                return parse_city_state(dd.get_text(strip=True))
    return None


def _strategy_table_rows(soup: BeautifulSoup) -> Optional[str]:
    """Strategy 2: <tr> with 'Hometown' header cell."""
    for row in soup.find_all("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) >= 2 and "hometown" in cells[0].get_text(strip=True).lower():
            return parse_city_state(cells[1].get_text(strip=True))
    return None


def _strategy_text_regex(soup: BeautifulSoup) -> Optional[str]:
    """Strategy 3: 'Hometown: City, ST' inline or 'Hometown\\nCity, ST' two-line."""
    text = soup.get_text(separator="\n")

    # Inline: "Hometown: Seattle, WA" or "Hometown  Seattle, WA"
    m = re.search(
        r"Hometown[:\s]+([A-Za-z][A-Za-z\s\.'\-]{1,40}),\s*([A-Z][a-zA-Z\s\.]{1,25})",
        text,
    )
    if m:
        result = parse_city_state(f"{m.group(1).strip()}, {m.group(2).strip()}")
        if result:
            return result

    # Two-line: standalone "Hometown" then value on next non-blank line
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    for i, line in enumerate(lines):
        if line.lower() == "hometown" and i + 1 < len(lines):
            return parse_city_state(lines[i + 1])

    return None


def _strategy_bio_scan(soup: BeautifulSoup) -> Optional[str]:
    """
    Strategy 4: Scan the header/bio section (content before the first
    performance table) for a 'City, ST' pattern with a hard 2-letter state
    code.  Conservative to reduce false positives.
    """
    first_table = soup.find("table")
    if first_table and soup.body:
        bio_parts = []
        for el in soup.body.children:
            if el == first_table:
                break
            if hasattr(el, "get_text"):
                bio_parts.append(el.get_text(separator=" "))
        bio_text = " ".join(bio_parts)[:2000]
    else:
        bio_text = soup.get_text()[:2000]

    for city_raw, state in re.findall(
        r"\b([A-Z][A-Za-z\s\.'\-]{1,30}),\s+([A-Z]{2})\b", bio_text
    ):
        city_raw = city_raw.strip()
        if state not in US_STATE_ABBRS:
            continue
        if re.search(r"\b(university|college|indoor|outdoor|cross|country)\b", city_raw, re.I):
            continue
        result = parse_city_state(f"{city_raw}, {state}")
        if result:
            return result

    return None


def _extract_high_school(soup: BeautifulSoup) -> Optional[str]:
    """Best-effort high school extraction from a TFRRS profile."""
    # dl/dt/dd
    for dt in soup.find_all("dt"):
        if "high school" in dt.get_text(strip=True).lower():
            dd = dt.find_next_sibling("dd")
            if dd:
                return dd.get_text(strip=True)[:120]
    # table rows
    for row in soup.find_all("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) >= 2 and "high school" in cells[0].get_text(strip=True).lower():
            text = cells[1].get_text(strip=True)
            if len(text) > 2:
                return text[:120]
    # plain text
    text = soup.get_text(separator="\n")
    m = re.search(r"High School[:\s]+(.+)", text, re.I)
    if m:
        hs = m.group(1).strip().splitlines()[0].strip()
        if 2 < len(hs) < 120:
            return hs
    return None


def scrape_tfrrs_for_hometown(url: str) -> dict:
    """
    Fetch a TFRRS athlete page and extract hometown + high_school.
    Returns: {"hometown": str|None, "high_school": str|None}
    """
    result = {"hometown": None, "high_school": None}
    soup = fetch_soup(url)
    if not soup:
        return result

    for strategy in (_strategy_dl, _strategy_table_rows, _strategy_text_regex, _strategy_bio_scan):
        ht = strategy(soup)
        if ht:
            result["hometown"] = ht
            break

    result["high_school"] = _extract_high_school(soup)

    # Last-resort: parenthetical in high school name
    if not result["hometown"] and result["high_school"]:
        result["hometown"] = _hs_parenthetical_hometown(result["high_school"])

    return result


def run_step1(supabase: Client, dry_run: bool, limit: int) -> int:
    """
    Visit each athlete's TFRRS page and write hometown / high_school.
    Returns number of athletes updated.
    """
    log.info("=" * 60)
    log.info("STEP 1 — TFRRS Profile Pass")
    log.info("=" * 60)

    athletes = fetch_athletes_missing_hometown(supabase, limit=limit)
    log.info(f"  {len(athletes)} athletes with missing hometowns and TFRRS URLs")

    updated = found = skipped = errors = 0

    for i, athlete in enumerate(athletes, 1):
        name    = athlete["name"]
        college = athlete.get("college") or ""
        url     = athlete["tfrrs_url"]
        log.info(f"[{i}/{len(athletes)}] {name} ({college})")

        scraped = scrape_tfrrs_for_hometown(url)
        time.sleep(REQUEST_DELAY)

        payload: dict = {}

        if scraped["hometown"]:
            payload["hometown"] = scraped["hometown"]
            payload["hometown_state"] = scraped["hometown"].rsplit(", ", 1)[-1]
            log.info(f"  ✓ hometown: {scraped['hometown']}")
            found += 1

        # Only write high_school when currently missing
        existing_hs = (athlete.get("high_school") or "").strip()
        if scraped["high_school"] and not existing_hs:
            payload["high_school"] = scraped["high_school"]
            log.info(f"  ✓ high_school: {scraped['high_school']}")

        if not payload:
            log.debug("  — nothing found on TFRRS page")
            skipped += 1
            continue

        if update_athlete(supabase, athlete["id"], payload, dry_run):
            updated += 1
        else:
            errors += 1

    log.info("─" * 60)
    log.info(
        f"Step 1 summary: {found} hometowns found, {updated} athletes updated, "
        f"{skipped} no data, {errors} errors"
    )
    return updated


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — Roster Page Pass
# ══════════════════════════════════════════════════════════════════════════════

# Official roster URLs (requests-only, no Playwright needed for static pages).
# Sourced from roster_scraper.py — the full Playwright version is authoritative
# for JS-heavy sites; this pass covers what static HTML can provide cheaply.
ROSTER_URLS: list[dict] = [
    # ACC
    {"school": "Boston College",    "url": "https://bceagles.com/sports/mens-track-and-field/roster"},
    {"school": "Boston College",    "url": "https://bceagles.com/sports/womens-track-and-field/roster"},
    {"school": "Clemson",           "url": "https://clemsontigers.com/sports/track-field/roster/"},
    {"school": "Duke",              "url": "https://goduke.com/sports/track-and-field/roster"},
    {"school": "Florida State",     "url": "https://seminoles.com/sports/mens-track-and-field/roster"},
    {"school": "Florida State",     "url": "https://seminoles.com/sports/womens-track-and-field/roster"},
    {"school": "Louisville",        "url": "https://gocards.com/sports/track-and-field/roster"},
    {"school": "Miami",             "url": "https://miamihurricanes.com/sports/track/roster/"},
    {"school": "NC State",          "url": "https://gopack.com/sports/track-and-field/roster"},
    {"school": "North Carolina",    "url": "https://goheels.com/sports/track-and-field/roster"},
    {"school": "Notre Dame",        "url": "https://und.com/sports/track/roster/"},
    {"school": "Pittsburgh",        "url": "https://pittsburghpanthers.com/sports/track-and-field/roster"},
    {"school": "Stanford",          "url": "https://gostanford.com/sports/track-field/roster"},
    {"school": "Syracuse",          "url": "https://cuse.com/sports/mens-track-and-field/roster"},
    {"school": "Syracuse",          "url": "https://cuse.com/sports/womens-track-and-field/roster"},
    {"school": "Virginia",          "url": "https://virginiasports.com/sports/xctrack/roster"},
    {"school": "Virginia Tech",     "url": "https://hokiesports.com/sports/track-field/roster"},
    {"school": "Wake Forest",       "url": "https://godeacs.com/sports/track-and-field/roster"},
    # Big Ten
    {"school": "Illinois",          "url": "https://fightingillini.com/sports/mens-track-and-field/roster"},
    {"school": "Illinois",          "url": "https://fightingillini.com/sports/womens-track-and-field/roster"},
    {"school": "Indiana",           "url": "https://iuhoosiers.com/sports/track-and-field/roster"},
    {"school": "Iowa",              "url": "https://hawkeyesports.com/sports/mtrack/roster"},
    {"school": "Iowa",              "url": "https://hawkeyesports.com/sports/wtrack/roster"},
    {"school": "Maryland",          "url": "https://umterps.com/sports/track-and-field/roster"},
    {"school": "Michigan",          "url": "https://mgoblue.com/sports/mens-track-and-field/roster"},
    {"school": "Michigan",          "url": "https://mgoblue.com/sports/womens-track-and-field/roster"},
    {"school": "Michigan State",    "url": "https://msuspartans.com/sports/track-and-field/roster"},
    {"school": "Minnesota",         "url": "https://gophersports.com/sports/mens-track-and-field/roster"},
    {"school": "Minnesota",         "url": "https://gophersports.com/sports/womens-track-and-field/roster"},
    {"school": "Nebraska",          "url": "https://huskers.com/sports/track-and-field/roster/2025-26"},
    {"school": "Ohio State",        "url": "https://ohiostatebuckeyes.com/sports/mens-track-field/roster"},
    {"school": "Ohio State",        "url": "https://ohiostatebuckeyes.com/sports/womens-track-field/roster"},
    {"school": "Oregon",            "url": "https://goducks.com/sports/track-and-field/roster"},
    {"school": "Penn State",        "url": "https://gopsusports.com/sports/track-field/roster"},
    {"school": "Purdue",            "url": "https://purduesports.com/sports/track-field/roster"},
    {"school": "Rutgers",           "url": "https://scarletknights.com/sports/track-and-field/roster"},
    {"school": "UCLA",              "url": "https://uclabruins.com/sports/track-and-field/roster"},
    {"school": "USC",               "url": "https://usctrojans.com/sports/track-and-field/roster"},
    {"school": "Washington",        "url": "https://gohuskies.com/sports/track-and-field/roster"},
    {"school": "Wisconsin",         "url": "https://uwbadgers.com/sports/mens-track-and-field/roster"},
    {"school": "Wisconsin",         "url": "https://uwbadgers.com/sports/womens-track-and-field/roster"},
    # SEC
    {"school": "Alabama",           "url": "https://rolltide.com/sports/xctrack/roster"},
    {"school": "Arkansas",          "url": "https://arkansasrazorbacks.com/sport/m-track/roster/"},
    {"school": "Arkansas",          "url": "https://arkansasrazorbacks.com/sport/w-track/roster/"},
    {"school": "Auburn",            "url": "https://auburntigers.com/sports/xctrack/roster"},
    {"school": "Florida",           "url": "https://floridagators.com/sports/track-and-field/roster"},
    {"school": "Georgia",           "url": "https://georgiadogs.com/sports/track-and-field/roster"},
    {"school": "Kentucky",          "url": "https://ukathletics.com/sports/track/roster"},
    {"school": "LSU",               "url": "https://lsusports.net/sports/tf/roster/"},
    {"school": "Ole Miss",          "url": "https://olemisssports.com/sports/track-and-field/roster"},
    {"school": "Mississippi State", "url": "https://hailstate.com/sports/track-and-field/roster"},
    {"school": "Missouri",          "url": "https://mutigers.com/sports/track-and-field/roster"},
    {"school": "South Carolina",    "url": "https://gamecocksonline.com/sports/track/roster/"},
    {"school": "Tennessee",         "url": "https://utsports.com/sports/track-and-field/roster"},
    {"school": "Texas",             "url": "https://texassports.com/sports/track-and-field/roster"},
    {"school": "Texas A&M",         "url": "https://12thman.com/sports/track-and-field/roster"},
    # Big 12
    {"school": "Arizona",           "url": "https://arizonawildcats.com/sports/track-and-field/roster"},
    {"school": "Arizona State",     "url": "https://thesundevils.com/sports/track-field/roster"},
    {"school": "Baylor",            "url": "https://baylorbears.com/sports/track-and-field/roster"},
    {"school": "BYU",               "url": "https://byucougars.com/sports/mens-track-and-field/roster"},
    {"school": "BYU",               "url": "https://byucougars.com/sports/womens-track-and-field/roster"},
    {"school": "Colorado",          "url": "https://cubuffs.com/sports/track-and-field/roster"},
    {"school": "Iowa State",        "url": "https://cyclones.com/sports/track-and-field/roster"},
    {"school": "Kansas",            "url": "https://kuathletics.com/sports/track-and-field/roster"},
    {"school": "Kansas State",      "url": "https://kstatesports.com/sports/track-and-field/roster"},
    {"school": "Oklahoma State",    "url": "https://okstate.com/sports/mxct/roster"},
    {"school": "Oklahoma State",    "url": "https://okstate.com/sports/womens-cross-country-track/roster"},
    {"school": "TCU",               "url": "https://gofrogs.com/sports/mens-track-and-field/roster"},
    {"school": "TCU",               "url": "https://gofrogs.com/sports/womens-track-and-field/roster"},
    {"school": "Texas Tech",        "url": "https://texastech.com/sports/track-and-field/roster"},
    {"school": "Utah",              "url": "https://utahutes.com/sports/track-and-field/roster"},
    {"school": "West Virginia",     "url": "https://wvusports.com/sports/womens-track-and-field/roster"},
    # Ivy
    {"school": "Brown",             "url": "https://brownbears.com/sports/mens-track-and-field/roster"},
    {"school": "Brown",             "url": "https://brownbears.com/sports/womens-track-and-field/roster"},
    {"school": "Columbia",          "url": "https://gocolumbialions.com/sports/track-and-field/roster"},
    {"school": "Cornell",           "url": "https://cornellbigred.com/sports/mens-track-and-field/roster"},
    {"school": "Cornell",           "url": "https://cornellbigred.com/sports/womens-track-and-field/roster"},
    {"school": "Dartmouth",         "url": "https://dartmouthsports.com/sports/mens-track-and-field/roster"},
    {"school": "Dartmouth",         "url": "https://dartmouthsports.com/sports/womens-track-and-field/roster"},
    {"school": "Harvard",           "url": "https://gocrimson.com/sports/mens-track-and-field/roster"},
    {"school": "Princeton",         "url": "https://goprincetontigers.com/sports/mens-track-and-field/roster"},
    {"school": "Princeton",         "url": "https://goprincetontigers.com/sports/womens-track-and-field/roster"},
    {"school": "Yale",              "url": "https://yalebulldogs.com/sports/mens-track-and-field/roster"},
    {"school": "Yale",              "url": "https://yalebulldogs.com/sports/womens-track-and-field/roster"},
    # Big East
    {"school": "Butler",            "url": "https://butlersports.com/sports/mens-track-and-field/roster"},
    {"school": "Butler",            "url": "https://butlersports.com/sports/womens-track-and-field/roster"},
    {"school": "Georgetown",        "url": "https://guhoyas.com/sports/mens-track-and-field-xc/roster"},
    {"school": "Georgetown",        "url": "https://guhoyas.com/sports/womens-track-and-field/roster"},
    {"school": "Marquette",         "url": "https://gomarquette.com/sports/track-and-field/roster"},
    {"school": "Providence",        "url": "https://friars.com/sports/mens-track-and-field/roster"},
    {"school": "Providence",        "url": "https://friars.com/sports/womens-track-and-field/roster"},
    {"school": "Connecticut",       "url": "https://uconnhuskies.com/sports/mens-track-and-field/roster"},
    {"school": "Connecticut",       "url": "https://uconnhuskies.com/sports/womens-track-and-field/roster"},
    {"school": "Villanova",         "url": "https://villanova.com/sports/mens-track-and-field/roster"},
    {"school": "Villanova",         "url": "https://villanova.com/sports/womens-track-and-field/roster"},
    # Mountain West
    {"school": "Air Force",         "url": "https://goairforcefalcons.com/sports/track-and-field/roster"},
    {"school": "Boise State",       "url": "https://broncosports.com/sports/track-and-field/roster"},
    {"school": "Colorado State",    "url": "https://csurams.com/sports/track-and-field/roster"},
    {"school": "Fresno State",      "url": "https://gobulldogs.com/sports/track-and-field/roster"},
    {"school": "New Mexico",        "url": "https://golobos.com/sports/track/roster"},
    {"school": "San Diego State",   "url": "https://goaztecs.com/sports/track-and-field/roster"},
    {"school": "San Jose State",    "url": "https://sjsuspartans.com/sports/track-and-field/roster"},
    {"school": "Utah State",        "url": "https://utahstateaggies.com/sports/track-and-field/roster"},
    {"school": "Wyoming",           "url": "https://gowyo.com/sports/track-and-field/roster"},
    # Big Sky
    {"school": "Eastern Washington","url": "https://goeags.com/sports/track-and-field/roster"},
    {"school": "Idaho",             "url": "https://govandals.com/sports/tfxc/roster"},
    {"school": "Montana",           "url": "https://gogriz.com/sports/mens-track-and-field/roster"},
    {"school": "Montana",           "url": "https://gogriz.com/sports/womens-track-and-field/roster"},
    {"school": "Montana State",     "url": "https://msubobcats.com/sports/mens-track-and-field/roster"},
    {"school": "Montana State",     "url": "https://msubobcats.com/sports/womens-track-and-field/roster"},
    {"school": "Northern Arizona",  "url": "https://nauathletics.com/sports/track-and-field/roster"},
    {"school": "Northern Colorado", "url": "https://uncbears.com/sports/track-and-field/roster"},
    {"school": "Sacramento State",  "url": "https://hornetsports.com/sports/track/roster"},
    {"school": "Weber State",       "url": "https://weberstatesports.com/sports/mens-track-and-field/roster"},
    {"school": "Weber State",       "url": "https://weberstatesports.com/sports/track-and-field/roster"},
    # Atlantic 10
    {"school": "Dayton",            "url": "https://daytonflyers.com/sports/womens-track-and-field/roster"},
    {"school": "Fordham",           "url": "https://fordhamsports.com/sports/mens-track-and-field/roster"},
    {"school": "Fordham",           "url": "https://fordhamsports.com/sports/womens-track-and-field/roster"},
    {"school": "George Mason",      "url": "https://gomason.com/sports/mens-track-and-field/roster"},
    {"school": "George Mason",      "url": "https://gomason.com/sports/womens-track-and-field/roster"},
    {"school": "Rhode Island",      "url": "https://gorhody.com/sports/mens-track-and-field/roster"},
    {"school": "Rhode Island",      "url": "https://gorhody.com/sports/womens-track-and-field/roster"},
    {"school": "Richmond",          "url": "https://richmondspiders.com/sports/womens-track-and-field/roster"},
    {"school": "Saint Louis",       "url": "https://slubillikens.com/sports/track-and-field/roster"},
    {"school": "VCU",               "url": "https://vcuathletics.com/sports/mens-track-and-field/roster"},
    {"school": "VCU",               "url": "https://vcuathletics.com/sports/womens-track-and-field/roster"},
]


def _parse_roster_html(html: str, school: str) -> list[dict]:
    """
    Extract (name, hometown) pairs from a static Sidearm roster page.

    Pattern A: 'Hometown' label → adjacent sibling contains the value.
               Works for dl/dt/dd, span+span, and similar label-value pairs.
    Pattern B: <table> with a column header containing 'hometown'.
    """
    soup = BeautifulSoup(html, "html.parser")
    results: list[dict] = []

    # ── Pattern A ─────────────────────────────────────────────────────────────
    for el in soup.find_all(string=re.compile(r"^Hometown$", re.I)):
        parent = el.parent
        sibling = parent.find_next_sibling()
        if sibling:
            ht = parse_city_state(sibling.get_text(strip=True))
            if ht:
                name = _find_nearby_name(parent)
                if name:
                    results.append({"name": name, "hometown": ht})

    if results:
        return _dedup(results)

    # ── Pattern B ─────────────────────────────────────────────────────────────
    for table in soup.find_all("table"):
        ths = table.find_all("th")
        if not ths:
            continue
        header_texts = [h.get_text(strip=True).lower() for h in ths]
        ht_col   = next((i for i, h in enumerate(header_texts) if "hometown" in h), None)
        name_col = next((i for i, h in enumerate(header_texts) if "name" in h), 0)
        if ht_col is None:
            continue
        for row in table.find_all("tr")[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) <= ht_col:
                continue
            name_raw = cells[name_col].get_text(strip=True) if len(cells) > name_col else ""
            ht       = parse_city_state(cells[ht_col].get_text(strip=True))
            if ht and name_raw and len(name_raw) >= 4:
                results.append({"name": name_raw, "hometown": ht})

    return _dedup(results)


def _find_nearby_name(el) -> Optional[str]:
    """Walk backwards through the DOM to find the nearest athlete name element."""
    SKIP_LOWER = {
        "hometown", "high school", "hometown / high school",
        "event", "year", "class", "height", "weight",
    }
    for _ in range(25):
        el = el.find_previous()
        if not el or not hasattr(el, "get_text"):
            break
        text = el.get_text(strip=True)
        if not text or text.lower() in SKIP_LOWER:
            continue
        words = text.split()
        if (
            2 <= len(words) <= 5
            and re.match(r"[A-Z][a-z]", text)
            and not re.search(r"\d", text)
        ):
            return text
    return None


def _dedup(items: list[dict]) -> list[dict]:
    seen, out = set(), []
    for r in items:
        k = normalize(r["name"])
        if k not in seen:
            seen.add(k)
            out.append(r)
    return out


def build_name_index(db_rows: list[dict]) -> dict:
    """
    Build a normalised-name lookup from DB rows.
    Key: (normalised_name, normalised_college) → list[athlete_dict]
    """
    idx: dict[tuple, list] = {}
    for r in db_rows:
        key = (normalize(r["name"]), normalize(r.get("college") or ""))
        idx.setdefault(key, []).append(r)
    return idx


def lookup_athlete(idx: dict, name: str, college: str) -> Optional[dict]:
    """
    Three-tier name match against the pre-built index.
    Tier 1: exact normalised full name + college.
    Tier 2: first+last only (drops middle name) + college.
    Returns the single matching row, or None if not found / ambiguous.
    """
    nname    = normalize(name)
    ncollege = normalize(college)

    # Tier 1
    matches = idx.get((nname, ncollege), [])
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        return None  # ambiguous

    # Tier 2: first + last only
    parts = nname.split()
    if len(parts) >= 3:
        short   = f"{parts[0]} {parts[-1]}"
        matches = idx.get((short, ncollege), [])
        if len(matches) == 1:
            return matches[0]

    return None


def scrape_roster_page(url: str, school: str) -> list[dict]:
    """Fetch one static roster page and parse it."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return _parse_roster_html(r.text, school)
    except requests.RequestException as e:
        log.warning(f"  Roster fetch failed ({url}): {e}")
        return []


def run_step2(supabase: Client, dry_run: bool, limit: int) -> int:
    """
    Scrape official roster pages for schools that still have athletes missing
    hometowns.  Updates only athletes with hometown IS NULL — never overwrites.
    Returns number of athletes updated.
    """
    log.info("=" * 60)
    log.info("STEP 2 — Roster Page Pass")
    log.info("=" * 60)

    schools_needing_work = set(fetch_schools_with_missing_hometowns(supabase))
    log.info(f"  {len(schools_needing_work)} schools still have athletes with missing hometowns")

    # Only process schools we have roster URLs for
    relevant = [r for r in ROSTER_URLS if r["school"] in schools_needing_work]
    relevant_schools = sorted({r["school"] for r in relevant})
    log.info(f"  {len(relevant_schools)} of those have roster URLs in this script")

    if not relevant_schools:
        log.info("  Nothing to do — all schools with missing hometowns lack roster URLs.")
        return 0

    # Build in-memory name index once, upfront
    log.info("  Building athlete name index from DB...")
    db_rows = fetch_all_athletes_for_schools(supabase, relevant_schools)
    idx = build_name_index(db_rows)
    log.info(f"  Index built: {len(db_rows)} athletes across {len(relevant_schools)} schools")

    # Set of athlete IDs that actually need updating (hometown IS NULL)
    needs_update: set[int] = {
        r["id"] for r in db_rows
        if not (r.get("hometown") or "").strip()
    }
    log.info(f"  {len(needs_update)} of those athletes still need hometowns")

    pages_scraped = updated = no_match = already_has = update_count = 0

    for entry in relevant:
        school = entry["school"]
        url    = entry["url"]
        log.info(f"\n── {school}: {url}")

        parsed = scrape_roster_page(url, school)
        pages_scraped += 1
        log.info(f"  Parsed {len(parsed)} athletes from page")
        time.sleep(ROSTER_DELAY)

        for a in parsed:
            if not a.get("hometown"):
                continue

            row = lookup_athlete(idx, a["name"], school)
            if not row:
                log.debug(f"  No DB match: {a['name']} @ {school}")
                no_match += 1
                continue

            if row["id"] not in needs_update:
                already_has += 1
                continue

            state   = a["hometown"].rsplit(", ", 1)[-1] if ", " in a["hometown"] else None
            payload = {"hometown": a["hometown"]}
            if state:
                payload["hometown_state"] = state

            log.info(f"  ✓ {a['name']} → {a['hometown']}")
            if update_athlete(supabase, row["id"], payload, dry_run):
                updated += 1
                needs_update.discard(row["id"])
                update_count += 1
                if limit and update_count >= limit:
                    log.info(f"  Reached --limit {limit} — stopping step 2")
                    break

        if limit and update_count >= limit:
            break

    log.info("─" * 60)
    log.info(
        f"Step 2 summary: {pages_scraped} pages scraped, {updated} athletes updated, "
        f"{no_match} no DB match, {already_has} already had hometowns"
    )
    return updated


# ══════════════════════════════════════════════════════════════════════════════
# POST-RUN: GEOCODE BACKFILL
# ══════════════════════════════════════════════════════════════════════════════

def run_geocode_backfill(dry_run: bool) -> None:
    """Invoke geocode_backfill.py so newly written hometowns get coordinates."""
    log.info("=" * 60)
    log.info("Running geocode_backfill.py...")
    log.info("=" * 60)
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "geocode_backfill.py")
    cmd = [sys.executable, script]
    if dry_run:
        cmd.append("--dry-run")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        log.error(f"Geocode backfill exited with error: {e}")
    except FileNotFoundError:
        log.error(f"geocode_backfill.py not found at {script}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="TrackScout hometown enrichment — fills null hometowns via TFRRS + rosters"
    )
    parser.add_argument("--dry-run",      action="store_true",
                        help="Log changes but make no DB writes")
    parser.add_argument("--step1-only",   action="store_true",
                        help="Run TFRRS profile pass only (skip roster pass)")
    parser.add_argument("--step2-only",   action="store_true",
                        help="Run roster page pass only (skip TFRRS pass)")
    parser.add_argument("--limit",        type=int, default=0,
                        help="Cap athletes processed per step (0 = all)")
    parser.add_argument("--skip-geocode", action="store_true",
                        help="Do not run geocode backfill after enrichment")
    args = parser.parse_args()

    if args.dry_run:
        log.info("DRY RUN — no writes will be made to Supabase")

    supabase      = get_supabase()
    total_updated = 0

    if not args.step2_only:
        total_updated += run_step1(supabase, dry_run=args.dry_run, limit=args.limit)

    if not args.step1_only:
        total_updated += run_step2(supabase, dry_run=args.dry_run, limit=args.limit)

    log.info("=" * 60)
    log.info(f"TOTAL athletes updated across both steps: {total_updated}")
    if args.dry_run:
        log.info("DRY RUN — no changes written to Supabase")
    log.info("=" * 60)

    # Kick off geocode backfill immediately so new hometowns get coordinates
    if not args.skip_geocode and total_updated > 0 and not args.dry_run:
        run_geocode_backfill(dry_run=False)
    elif args.dry_run and not args.skip_geocode:
        log.info("Skipping geocode backfill (dry run)")


if __name__ == "__main__":
    main()
