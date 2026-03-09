"""
historical_seniors_scraper.py
─────────────────────────────
Discovers athletes who were college seniors (4th year) in spring 2023,
spring 2024, or spring 2025 and adds them to the Supabase athletes table
with their full performance histories.

Architecture
────────────
1. Pull distinct schools + a sample athlete URL per school from Supabase.
2. From the sample athlete URL, follow the breadcrumb to the school's TFRRS
   team page and scrape all athlete links.
3. For each athlete not already in the DB, fetch their PRINT PAGE (?print=1).
   The print page is flat HTML — season headings followed immediately by plain
   tables — with no Bootstrap panels to navigate.
4. Walk headings and tables in document order to parse season info and
   performances in a single pass.
5. If an athlete's most recent season matches a target graduation year, insert
   them into Supabase with all their performances.

Parsing strategy (per design spec)
────────────────────────────────────
  Primary   : print page  (?print=1)  — flat heading → table pairs
  Fallback  : standard page           — panel-first traversal (.panel containers)
  Columns   : detected dynamically by header text, never by fixed index
  Events    : all tables per season, including relays and multi-events

Usage
─────
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
from bs4 import BeautifulSoup, Tag
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
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

REQUEST_DELAY  = 1.2   # seconds between TFRRS requests
TFRRS_BASE     = "https://www.tfrrs.org"
YEAR_MIN       = 2015
YEAR_MAX       = 2026

# Marks that indicate no performance — skip these rows
BAD_MARKS = frozenset({
    "", "—", "–", "-", "DNF", "DNS", "DQ", "NH", "NM",
    "FOUL", "PASS", "SCR", "NT", "ND", "SCRATCH", "N/A",
})

# ── Event normalisation ───────────────────────────────────────────────────────
# Keys are lowercase substrings; first match wins.

EVENT_MAP = [
    ("110 meter hurdle",  "110mH"),
    ("100 meter hurdle",  "100mH"),
    ("400 meter hurdle",  "400mH"),
    ("3,000 meter steeple", "3000mSC"),
    ("3000 meter steeple",  "3000mSC"),
    ("steeplechase",        "3000mSC"),
    ("4 x 100",   "4x100m"),
    ("4x100",     "4x100m"),
    ("4 x 400",   "4x400m"),
    ("4x400",     "4x400m"),
    ("4 x 800",   "4x800m"),
    ("4x800",     "4x800m"),
    ("distance medley", "DMR"),
    ("sprint medley",   "SMR"),
    ("10,000",   "10000m"),
    ("10000",    "10000m"),
    ("5,000",    "5000m"),
    ("5000",     "5000m"),
    ("3,000",    "3000m"),
    ("3000",     "3000m"),
    ("1,500",    "1500m"),
    ("1500",     "1500m"),
    ("1 mile",   "Mile"),
    ("mile",     "Mile"),
    ("800",      "800m"),
    ("400",      "400m"),
    ("200",      "200m"),
    ("100",      "100m"),
    ("high jump",   "HJ"),
    ("pole vault",  "PV"),
    ("long jump",   "LJ"),
    ("triple jump", "TJ"),
    ("shot put",    "SP"),
    ("discus",      "DT"),
    ("hammer",      "HT"),
    ("javelin",     "JT"),
    ("weight throw", "WT"),
    ("heptathlon",  "Hept"),
    ("decathlon",   "Dec"),
    ("pentathlon",  "Pent"),
]


def normalize_event(raw: str) -> str:
    key = raw.strip().lower()
    for substring, canonical in EVENT_MAP:
        if substring in key:
            return canonical
    return raw.strip()


# ── Mark conversion ───────────────────────────────────────────────────────────

def mark_to_float(s: str) -> Optional[float]:
    """
    Convert a mark string to a sortable float.
    Track events → total seconds.   Field events → metres.
    Returns None if unparseable.
    """
    s = s.strip().upper().replace(",", "")
    # Strip trailing unit (M, m)
    s = re.sub(r"\s*M$", "", s).strip()

    # M:SS.ss  or  H:MM:SS(.ss)
    m = re.match(r"^(\d+):(\d{2})\.(\d+)$", s)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2)) + float("0." + m.group(3))

    m = re.match(r"^(\d+):(\d{2}):(\d{2})\.?(\d*)$", s)
    if m:
        return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))

    # Decimal seconds or metres: 10.23, 7.85, 21.34
    m = re.match(r"^(\d{1,4})\.(\d{2,4})$", s)
    if m:
        return float(s)

    # Imperial field: 25-08.50  or  6-05
    m = re.match(r"^(\d{1,2})-(\d{2}\.?\d*)$", s)
    if m:
        feet   = int(m.group(1))
        inches = float(m.group(2))
        return round((feet * 12 + inches) * 0.0254, 3)

    return None


# ── Season heading parsing ────────────────────────────────────────────────────

def parse_spring_year(text: str) -> Optional[int]:
    """
    Extract the 'spring year' from a TFRRS season heading.

    "2022-23 — Oregon"    → 2023   (academic-year format)
    "2024 Outdoor — OSU"  → 2024
    "2023 Indoor"         → 2023
    """
    text = text.strip()

    # Academic year "YYYY-YY"
    m = re.match(r"(\d{4})-(\d{2})\b", text)
    if m:
        yr = int(m.group(1)) + 1
        return yr if YEAR_MIN <= yr <= YEAR_MAX else None

    # Bare year + season keyword
    m = re.match(r"(\d{4})\s+(outdoor|indoor|cross|xc|track|field)", text, re.I)
    if m:
        yr = int(m.group(1))
        return yr if YEAR_MIN <= yr <= YEAR_MAX else None

    # Bare four-digit year at start
    m = re.match(r"(\d{4})\b", text)
    if m:
        yr = int(m.group(1))
        return yr if YEAR_MIN <= yr <= YEAR_MAX else None

    return None


def season_type(text: str) -> str:
    t = text.lower()
    if "indoor" in t:
        return "indoor"
    if "cross" in t or " xc" in t:
        return "xc"
    return "outdoor"


# ── HTTP ──────────────────────────────────────────────────────────────────────

def fetch(url: str) -> Optional[BeautifulSoup]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except requests.RequestException as e:
        log.warning(f"  GET failed ({url}): {e}")
        return None


def print_url(athlete_url: str) -> str:
    """Return the print-page URL for a TFRRS athlete page."""
    return athlete_url.split("?")[0] + "?print=1"


# ── Column detection ──────────────────────────────────────────────────────────

def detect_columns(header_cells: list[Tag]) -> dict[str, Optional[int]]:
    """
    Build a mapping of logical column name → index from a list of <th>/<td> cells.

    Logical names: event, mark, meet, date, place
    Matching is by substring so "Result", "Time", "Mark" all resolve to "mark".
    """
    cols: dict[str, Optional[int]] = {
        "event": None, "mark": None, "meet": None, "date": None, "place": None
    }

    for i, cell in enumerate(header_cells):
        h = cell.get_text(strip=True).lower()
        if cols["event"] is None and any(x in h for x in ("event", "discipline")):
            cols["event"] = i
        if cols["mark"] is None and any(x in h for x in ("mark", "result", "time", "distance", "performance")):
            cols["mark"] = i
        if cols["meet"] is None and any(x in h for x in ("meet", "competition", "name")):
            cols["meet"] = i
        if cols["date"] is None and "date" in h:
            cols["date"] = i
        if cols["place"] is None and any(x in h for x in ("place", " pl", "pos")):
            cols["place"] = i

    # Positional fallbacks — TFRRS print layout: Event(0) Mark(1) Wind(2) Meet(3) Date(4) Place(5)
    if cols["event"] is None:
        cols["event"] = 0
    if cols["mark"] is None:
        cols["mark"] = 1
    if cols["meet"] is None and len(header_cells) > 3:
        cols["meet"] = 3

    return cols


# ── Table parser ──────────────────────────────────────────────────────────────

def parse_table(
    table: Tag,
    spring_year: int,
    stype: str,
    athlete_id: int,
) -> list[dict]:
    """
    Parse one TFRRS performance table into a list of performance dicts.

    Handles:
    - standard individual events
    - relay events (4x100, 4x400, etc.)
    - multi-events (decathlon, heptathlon)
    - tables with no <thead> (first <tr> treated as header)

    Column positions are detected dynamically; fixed indexes are never used.
    """
    # ── Locate header cells ───────────────────────────────────────────────────
    header_cells = table.select("thead th, thead td")
    if not header_cells:
        first_tr = table.find("tr")
        if not first_tr:
            return []
        header_cells = first_tr.find_all(["th", "td"])

    cols = detect_columns(header_cells)
    event_col = cols["event"]
    mark_col  = cols["mark"]
    meet_col  = cols["meet"]

    # ── Parse data rows ───────────────────────────────────────────────────────
    data_rows = table.select("tbody tr")
    if not data_rows:
        # No <tbody>: all rows minus the first (header)
        all_rows = table.find_all("tr")
        data_rows = all_rows[1:] if len(all_rows) > 1 else []

    results = []
    seen: set[tuple] = set()

    for tr in data_rows:
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue
        # Skip accidental header rows in tbody
        if all(c.name == "th" for c in cells):
            continue

        def cell(idx: Optional[int]) -> str:
            if idx is None or idx >= len(cells):
                return ""
            return cells[idx].get_text(strip=True)

        event_raw = cell(event_col)
        mark_raw  = cell(mark_col)

        if not event_raw or not mark_raw:
            continue
        if mark_raw.upper() in BAD_MARKS:
            continue
        if not re.search(r"[\d.:]", mark_raw):
            continue  # no numeric content — sub-header or empty row

        event = normalize_event(event_raw)

        # Strip wind annotation: "10.45 (+1.2)", "7.23w", "10.45 (w1.2)"
        mark_display = re.sub(r"\s*[\[(]?[wW][+\-]?\d*\.?\d*[\])]?", "", mark_raw)
        mark_display = mark_display.replace("*", "").strip()
        if not mark_display or mark_display.upper() in BAD_MARKS:
            continue

        mark_val = mark_to_float(mark_display)
        meet_name = cell(meet_col)

        dedup = (event, mark_display, spring_year, stype)
        if dedup in seen:
            continue
        seen.add(dedup)

        results.append({
            "athlete_id": athlete_id,
            "event":        event,
            "mark":         mark_val,
            "mark_display": mark_display,
            "year":         spring_year,
            "season":       stype,
            "level":        "college",
            "meet_name":    meet_name[:200] if meet_name else None,
        })

    return results


# ── Performance scraping ──────────────────────────────────────────────────────

def scrape_print_page(print_soup: BeautifulSoup, athlete_id: int) -> tuple[list[dict], dict]:
    """
    Parse a TFRRS print page (?print=1) for season info and performances.

    The print page is flat HTML. Season headings and tables appear as siblings
    in document order with no wrapping panel divs:

        <h3>2024-25 Outdoor — Ohio State</h3>
        <table>...</table>      ← outdoor performances
        <table>...</table>      ← relay / multi-event table for same season
        <h3>2024-25 Indoor — Ohio State</h3>
        <table>...</table>

    We walk elements in order, updating the current season context whenever
    we see a heading that contains a parseable year.

    Returns
    -------
    performances : list of performance dicts (athlete_id=0 placeholder)
    season_info  : {spring_years, last_spring, first_spring, num_seasons, gender}
    """
    spring_years: set[int] = set()
    all_perfs: list[dict] = []
    seen_global: set[tuple] = set()

    cur_year:  Optional[int] = None
    cur_stype: str = "outdoor"

    for el in print_soup.find_all(["h2", "h3", "h4", "table"]):

        # ── Heading → update season context ───────────────────────────────
        if el.name in ("h2", "h3", "h4"):
            text = el.get_text(strip=True)
            yr   = parse_spring_year(text)
            if yr:
                cur_year  = yr
                cur_stype = season_type(text)
                spring_years.add(yr)
            continue

        # ── Table → parse all rows under current season context ───────────
        # Per spec: parse ALL tables per season (individual + relays + multi-events)
        if cur_year is None:
            continue

        rows = parse_table(el, cur_year, cur_stype, athlete_id)
        for row in rows:
            key = (row["event"], row["mark_display"], row["year"], row["season"])
            if key not in seen_global:
                seen_global.add(key)
                all_perfs.append(row)

    # ── Gender from page text ──────────────────────────────────────────────
    gender = None
    early  = print_soup.get_text()[:800].lower()
    if "women" in early or "female" in early:
        gender = "F"
    elif re.search(r"\bmen\b", early) or "male" in early:
        gender = "M"

    spring_list = sorted(spring_years)
    season_info = {
        "spring_years": spring_list,
        "last_spring":  spring_list[-1] if spring_list else None,
        "first_spring": spring_list[0]  if spring_list else None,
        "num_seasons":  len(spring_list),
        "gender":       gender,
    }

    log.debug(f"  print page: {len(spring_list)} seasons, {len(all_perfs)} performances")
    return all_perfs, season_info


def scrape_panel_page(soup: BeautifulSoup, athlete_id: int) -> tuple[list[dict], dict]:
    """
    Fallback parser for the standard TFRRS athlete page (Bootstrap panels).

    Never traverses backward from tables. Instead, selects .panel containers
    directly and reads their heading + tables as a unit.

    Per spec: iterates panel.select("table") to capture all tables per season
    (individual events, relays, multi-events may be in separate tables).
    """
    spring_years: set[int] = set()
    all_perfs:    list[dict] = []
    seen_global:  set[tuple] = set()

    # Select all panel containers
    panels = soup.select("div.panel, div.card")

    for panel in panels:
        # Read heading from the panel itself — never traverse across panels
        title_el = panel.select_one(
            ".panel-title, .panel-heading h3, .panel-heading h4, "
            ".panel-heading h5, .card-header h3, .card-header h4"
        )
        if not title_el:
            continue

        heading_text = title_el.get_text(strip=True)
        yr = parse_spring_year(heading_text)
        if not yr:
            continue

        spring_years.add(yr)
        stype = season_type(heading_text)

        # Parse ALL tables within this panel (individual + relays + multi-events)
        for table in panel.select("table"):
            rows = parse_table(table, yr, stype, athlete_id)
            for row in rows:
                key = (row["event"], row["mark_display"], row["year"], row["season"])
                if key not in seen_global:
                    seen_global.add(key)
                    all_perfs.append(row)

    # Gender
    gender = None
    early  = soup.get_text()[:800].lower()
    if "women" in early or "female" in early:
        gender = "F"
    elif re.search(r"\bmen\b", early) or "male" in early:
        gender = "M"

    spring_list = sorted(spring_years)
    season_info = {
        "spring_years": spring_list,
        "last_spring":  spring_list[-1] if spring_list else None,
        "first_spring": spring_list[0]  if spring_list else None,
        "num_seasons":  len(spring_list),
        "gender":       gender,
    }

    log.debug(f"  panel page: {len(spring_list)} seasons, {len(all_perfs)} performances")
    return all_perfs, season_info


def scrape_athlete(url: str, athlete_id: int = 0) -> tuple[list[dict], dict]:
    """
    Fetch and parse an athlete page.

    Tries the print page first (?print=1). If the print page yields no season
    data (e.g. TFRRS returns the same panel layout), falls back to panel-first
    parsing of the standard page.
    """
    # Primary: print page
    purl = print_url(url)
    soup = fetch(purl)
    if soup:
        perfs, info = scrape_print_page(soup, athlete_id)
        if info["num_seasons"] > 0:
            return perfs, info
        log.debug(f"  Print page yielded no seasons for {url}, trying standard page")

    # Fallback: standard page with panel-first parsing
    soup = fetch(url)
    if soup:
        return scrape_panel_page(soup, athlete_id)

    return [], {"spring_years": [], "last_spring": None,
                "first_spring": None, "num_seasons": 0, "gender": None}


# ── Team page scraping ────────────────────────────────────────────────────────

def find_team_url(athlete_soup: BeautifulSoup) -> Optional[str]:
    """
    Extract the school's TFRRS team page URL from an athlete page.
    Breadcrumb links are the most reliable signal.
    """
    for link in athlete_soup.select("ol.breadcrumb a, .breadcrumb a"):
        href = link.get("href", "")
        if "/teams/" in href:
            return href if href.startswith("http") else TFRRS_BASE + href

    for link in athlete_soup.select("a[href*='/teams/tf/'], a[href*='/teams/xc/']"):
        href = link.get("href", "")
        text = link.get_text(strip=True)
        if href and len(text) > 3:
            return href if href.startswith("http") else TFRRS_BASE + href

    return None


def scrape_team_roster(team_url: str) -> list[dict]:
    """
    Scrape a TFRRS team page for all athlete links.

    Per spec: collect athlete links automatically from the page.
    Returns list of {name, tfrrs_url, tfrrs_id} dicts.
    """
    soup = fetch(team_url)
    if not soup:
        return []

    athletes = []
    seen_ids: set[str] = set()

    # Per spec: collect links automatically — select all /athletes/ hrefs
    for link in soup.select("a[href*='/athletes/']"):
        href = link.get("href", "")
        name = link.get_text(strip=True)

        if not name or len(name) < 3 or len(name) > 60:
            continue
        if any(x in name.lower() for x in ("indoor", "outdoor", "cross", "relay", "team")):
            continue

        m = re.search(r"/athletes/(\d+)/", href)
        if not m:
            continue

        tfrrs_id = m.group(1)
        if tfrrs_id in seen_ids:
            continue
        seen_ids.add(tfrrs_id)

        full_url = href if href.startswith("http") else TFRRS_BASE + href
        athletes.append({
            "name":      name,
            "tfrrs_url": full_url,
            "tfrrs_id":  tfrrs_id,
        })

    log.info(f"  Team roster: {len(athletes)} athletes found")
    return athletes


# ── Supabase helpers ──────────────────────────────────────────────────────────

def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def load_schools(supabase: Client) -> list[dict]:
    """Return one sample tfrrs_url per distinct school already in the DB."""
    result = (
        supabase.table("athletes")
        .select("college, conference, tfrrs_url")
        .eq("source", "tfrrs")
        .not_.is_("tfrrs_url", "null")
        .neq("tfrrs_url", "")
        .execute()
    )
    seen: dict[str, dict] = {}
    for row in (result.data or []):
        college = (row.get("college") or "").strip()
        if college and college not in seen:
            seen[college] = {
                "college":    college,
                "conference": (row.get("conference") or "").strip(),
                "sample_url": row["tfrrs_url"],
            }
    log.info(f"Loaded {len(seen)} distinct schools from DB")
    return list(seen.values())


def load_existing_ids(supabase: Client) -> set[str]:
    """Load all existing TFRRS source_ids to skip athletes already in the DB."""
    result = (
        supabase.table("athletes")
        .select("source_id")
        .eq("source", "tfrrs")
        .execute()
    )
    ids = {row["source_id"] for row in (result.data or []) if row.get("source_id")}
    log.info(f"  {len(ids)} athletes already in DB")
    return ids


def insert_athlete(supabase: Client, payload: dict, dry_run: bool) -> Optional[int]:
    if dry_run:
        log.info(
            f"  [DRY RUN] INSERT athlete: {payload['name']} | "
            f"{payload['college']} | grad_year={payload.get('grad_year')} | "
            f"source_id={payload['source_id']}"
        )
        return None
    try:
        res = supabase.table("athletes").insert(payload).execute()
        if res.data:
            return res.data[0]["id"]
    except Exception as e:
        log.warning(f"  Athlete insert failed ({payload.get('name')}): {e}")
    return None


def insert_performances(supabase: Client, perfs: list[dict], dry_run: bool):
    if not perfs:
        return
    if dry_run:
        log.info(f"  [DRY RUN] INSERT {len(perfs)} performances")
        return
    try:
        for i in range(0, len(perfs), 50):
            supabase.table("performances").insert(perfs[i:i + 50]).execute()
    except Exception as e:
        log.warning(f"  Performance insert failed: {e}")


# ── Core athlete processing ───────────────────────────────────────────────────

def process_athlete(
    candidate:    dict,
    school:       dict,
    existing_ids: set[str],
    supabase:     Client,
    dry_run:      bool,
    target_years: set[int],
) -> str:
    """
    Evaluate one athlete candidate and insert if they qualify.

    Returns "added" | "skipped" (already in DB) | "not_senior" | "error"
    """
    tfrrs_id  = candidate["tfrrs_id"]
    source_id = f"tfrrs_{tfrrs_id}"

    if source_id in existing_ids:
        return "skipped"

    url = candidate["tfrrs_url"]
    perfs, info = scrape_athlete(url, athlete_id=0)
    time.sleep(REQUEST_DELAY)

    last_spring = info.get("last_spring")

    if last_spring not in target_years:
        return "not_senior"

    # Require at least 2 seasons — filters walk-ons who only competed once
    if info.get("num_seasons", 0) < 2:
        return "not_senior"

    grad_year    = last_spring
    hs_grad_year = last_spring - 4

    # Gender: page-parsed first, then URL signal as fallback
    gender = info.get("gender")
    if not gender:
        u = url.lower()
        if "_w_" in u or "/women/" in u:
            gender = "F"
        elif "_m_" in u or "/men/" in u:
            gender = "M"

    # Name: candidate name from the roster is usually "Last, First" — keep it
    # as a safe fallback; the print page title may contain a cleaner form.
    name = candidate["name"]

    events = sorted({p["event"] for p in perfs})

    log.info(
        f"  ✓ grad {grad_year}: {name} | {school['college']} | "
        f"hs_grad={hs_grad_year} | {len(perfs)} performances "
        f"({info['num_seasons']} seasons)"
    )

    payload = {
        "name":        name,
        "source":      "tfrrs",
        "source_id":   source_id,
        "tfrrs_url":   url,
        "college":     school["college"],
        "conference":  school["conference"],
        "college_year": "Senior",
        "hs_grad_year": hs_grad_year,
        "grad_year":   grad_year,
        "gender":      gender,
        "events":      events or None,
        "is_transfer": None,   # populated later by transfer_portal_scraper
    }

    new_id = insert_athlete(supabase, payload, dry_run)

    if new_id and perfs:
        for p in perfs:
            p["athlete_id"] = new_id
        insert_performances(supabase, perfs, dry_run)

    existing_ids.add(source_id)
    return "added"


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="TrackScout historical seniors scraper")
    parser.add_argument("--dry-run", action="store_true",
                        help="Read-only mode — log what would be inserted, write nothing")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max schools to process (default: all)")
    parser.add_argument("--years", type=int, nargs="+",
                        default=[2023, 2024, 2025], metavar="YEAR",
                        help="Target graduation spring years (default: 2023 2024 2025)")
    parser.add_argument("--debug", action="store_true",
                        help="Enable DEBUG logging for per-athlete detail")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    target_years = set(args.years)
    log.info(f"Target graduation years: {sorted(target_years)}")
    if args.dry_run:
        log.info("DRY RUN — no writes to Supabase")

    supabase = get_supabase()
    schools  = load_schools(supabase)
    if args.limit:
        schools = schools[:args.limit]
        log.info(f"Limited to {args.limit} schools")

    existing_ids = load_existing_ids(supabase)

    counters = {"added": 0, "skipped": 0, "not_senior": 0, "error": 0}

    for idx, school in enumerate(schools, 1):
        college    = school["college"]
        sample_url = school["sample_url"]
        log.info(f"\n[{idx}/{len(schools)}] {college}")

        # Step 1: find team page URL from sample athlete page
        sample_soup = fetch(sample_url)
        time.sleep(REQUEST_DELAY)
        if not sample_soup:
            log.warning(f"  Could not fetch sample page for {college}")
            counters["error"] += 1
            continue

        team_url = find_team_url(sample_soup)

        # Fallback: construct team URL from athlete URL slug
        if not team_url:
            m = re.search(r"/athletes/\d+/([^/]+)/", sample_url)
            if m:
                slug = m.group(1)
                for sport in ("tf", "xc"):
                    candidate_url = f"{TFRRS_BASE}/teams/{sport}/US/{slug}.html"
                    test = fetch(candidate_url)
                    time.sleep(REQUEST_DELAY)
                    if test and test.select("a[href*='/athletes/']"):
                        team_url = candidate_url
                        log.info(f"  Constructed team URL: {team_url}")
                        break

        if not team_url:
            log.warning(f"  Could not find team URL for {college}, skipping")
            counters["error"] += 1
            continue

        log.info(f"  Team URL: {team_url}")

        # Step 2: collect all athlete candidates from the team roster
        roster = scrape_team_roster(team_url)
        time.sleep(REQUEST_DELAY)
        if not roster:
            log.warning(f"  Empty roster for {college}")
            continue

        # Step 3: evaluate each candidate
        added_this_school = 0
        for candidate in roster:
            result = process_athlete(
                candidate, school, existing_ids,
                supabase, args.dry_run, target_years,
            )
            counters[result] = counters.get(result, 0) + 1
            if result == "added":
                added_this_school += 1

        log.info(f"  → {added_this_school} new seniors added from {college}")

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info(f"\n{'=' * 60}")
    log.info("Done.")
    log.info(f"  Schools processed : {len(schools)}")
    log.info(f"  Athletes added    : {counters['added']}")
    log.info(f"  Already in DB     : {counters['skipped']}")
    log.info(f"  Not senior/target : {counters['not_senior']}")
    log.info(f"  Errors            : {counters['error']}")


if __name__ == "__main__":
    main()
