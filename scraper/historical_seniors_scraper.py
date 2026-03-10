"""
historical_seniors_scraper.py
─────────────────────────────
Discovers athletes who were college seniors (4th year) in spring 2023,
spring 2024, or spring 2025 and adds them to the Supabase athletes table
with their full performance histories.

Architecture
────────────
1. Pull distinct schools + a sample athlete URL per school from Supabase.
2. From the sample athlete URL, resolve the school's TFRRS team BASE URL.
3. For each target year, scrape the year-specific roster page:
     {base_url}/{year}.html  →  every athlete on the team that year
4. For each athlete on that year's roster, fetch their PRINT PAGE (?print=1)
   and confirm their last active spring == target_year (i.e. they were a
   senior / final-year athlete that spring).
5. Insert new athletes with full performance history; update grad_year on
   athletes already in the DB from the main TFRRS scraper.

TFRRS historical roster URL pattern
─────────────────────────────────────
  Current roster:  .../teams/tf/MT_college_f_Montana_State.html
  2025 roster:     .../teams/tf/MT_college_f_Montana_State/2025.html
  2024 roster:     .../teams/tf/MT_college_f_Montana_State/2024.html
  2023 roster:     .../teams/tf/MT_college_f_Montana_State/2023.html

Usage
─────
  python scraper/historical_seniors_scraper.py --dry-run
  python scraper/historical_seniors_scraper.py --limit 5
  python scraper/historical_seniors_scraper.py
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
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ["SUPABASE_KEY"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

REQUEST_DELAY = 1.2
TFRRS_BASE    = "https://www.tfrrs.org"
YEAR_MIN      = 2015
YEAR_MAX      = 2026

BAD_MARKS = frozenset({
    "", "—", "–", "-", "DNF", "DNS", "DQ", "NH", "NM",
    "FOUL", "PASS", "SCR", "NT", "ND", "SCRATCH", "N/A",
})

# ── Event normalisation ───────────────────────────────────────────────────────

EVENT_MAP = [
    ("110 meter hurdle",    "110mH"),
    ("100 meter hurdle",    "100mH"),
    ("400 meter hurdle",    "400mH"),
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
    ("high jump",    "HJ"),
    ("pole vault",   "PV"),
    ("long jump",    "LJ"),
    ("triple jump",  "TJ"),
    ("shot put",     "SP"),
    ("discus",       "DT"),
    ("hammer",       "HT"),
    ("javelin",      "JT"),
    ("weight throw", "WT"),
    ("heptathlon",   "Hept"),
    ("decathlon",    "Dec"),
    ("pentathlon",   "Pent"),
]


def normalize_event(raw: str) -> str:
    key = raw.strip().lower()
    for substring, canonical in EVENT_MAP:
        if substring in key:
            return canonical
    return raw.strip()


# ── Mark conversion ───────────────────────────────────────────────────────────

def mark_to_float(s: str) -> Optional[float]:
    s = s.strip().upper().replace(",", "")
    s = re.sub(r"\s*M$", "", s).strip()
    m = re.match(r"^(\d+):(\d{2})\.(\d+)$", s)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2)) + float("0." + m.group(3))
    m = re.match(r"^(\d+):(\d{2}):(\d{2})\.?(\d*)$", s)
    if m:
        return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))
    m = re.match(r"^(\d{1,4})\.(\d{2,4})$", s)
    if m:
        return float(s)
    m = re.match(r"^(\d{1,2})-(\d{2}\.?\d*)$", s)
    if m:
        return round((int(m.group(1)) * 12 + float(m.group(2))) * 0.0254, 3)
    return None


# ── Season heading parsing ────────────────────────────────────────────────────

def parse_spring_year(text: str) -> Optional[int]:
    text = text.strip()
    m = re.match(r"(\d{4})-(\d{2})\b", text)
    if m:
        yr = int(m.group(1)) + 1
        return yr if YEAR_MIN <= yr <= YEAR_MAX else None
    m = re.match(r"(\d{4})\s+(outdoor|indoor|cross|xc|track|field)", text, re.I)
    if m:
        yr = int(m.group(1))
        return yr if YEAR_MIN <= yr <= YEAR_MAX else None
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
    return athlete_url.split("?")[0] + "?print=1"


# ── Column detection ──────────────────────────────────────────────────────────

def detect_columns(header_cells: list) -> dict:
    cols: dict = {"event": None, "mark": None, "meet": None, "date": None, "place": None}
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
    if cols["event"] is None:
        cols["event"] = 0
    if cols["mark"] is None:
        cols["mark"] = 1
    if cols["meet"] is None and len(header_cells) > 3:
        cols["meet"] = 3
    return cols


# ── Table parser ──────────────────────────────────────────────────────────────

def parse_table(table: Tag, spring_year: int, stype: str, athlete_id: int) -> list:
    header_cells = table.select("thead th, thead td")
    if not header_cells:
        first_tr = table.find("tr")
        if not first_tr:
            return []
        header_cells = first_tr.find_all(["th", "td"])

    cols      = detect_columns(header_cells)
    event_col = cols["event"]
    mark_col  = cols["mark"]
    meet_col  = cols["meet"]

    data_rows = table.select("tbody tr")
    if not data_rows:
        all_rows  = table.find_all("tr")
        data_rows = all_rows[1:] if len(all_rows) > 1 else []

    results = []
    seen: set = set()

    for tr in data_rows:
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue
        if all(c.name == "th" for c in cells):
            continue

        def cell(idx) -> str:
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
            continue

        event        = normalize_event(event_raw)
        mark_display = re.sub(r"\s*[\[(]?[wW][+\-]?\d*\.?\d*[\])]?", "", mark_raw)
        mark_display = mark_display.replace("*", "").strip()
        if not mark_display or mark_display.upper() in BAD_MARKS:
            continue

        mark_val  = mark_to_float(mark_display)
        meet_name = cell(meet_col)

        dedup = (event, mark_display, spring_year, stype)
        if dedup in seen:
            continue
        seen.add(dedup)

        results.append({
            "athlete_id":   athlete_id,
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

def scrape_print_page(print_soup: BeautifulSoup, athlete_id: int) -> tuple:
    spring_years: set = set()
    all_perfs:    list = []
    seen_global:  set = set()
    cur_year:  Optional[int] = None
    cur_stype: str = "outdoor"

    for el in print_soup.find_all(["h2", "h3", "h4", "table"]):
        if el.name in ("h2", "h3", "h4"):
            yr = parse_spring_year(el.get_text(strip=True))
            if yr:
                cur_year  = yr
                cur_stype = season_type(el.get_text(strip=True))
                spring_years.add(yr)
            continue
        if cur_year is None:
            continue
        for row in parse_table(el, cur_year, cur_stype, athlete_id):
            key = (row["event"], row["mark_display"], row["year"], row["season"])
            if key not in seen_global:
                seen_global.add(key)
                all_perfs.append(row)

    gender = None
    early  = print_soup.get_text()[:800].lower()
    if "women" in early or "female" in early:
        gender = "F"
    elif re.search(r"\bmen\b", early) or "male" in early:
        gender = "M"

    spring_list = sorted(spring_years)
    return all_perfs, {
        "spring_years": spring_list,
        "last_spring":  spring_list[-1] if spring_list else None,
        "first_spring": spring_list[0]  if spring_list else None,
        "num_seasons":  len(spring_list),
        "gender":       gender,
    }


def scrape_panel_page(soup: BeautifulSoup, athlete_id: int) -> tuple:
    spring_years: set = set()
    all_perfs:    list = []
    seen_global:  set = set()

    for panel in soup.select("div.panel, div.card"):
        title_el = panel.select_one(
            ".panel-title, .panel-heading h3, .panel-heading h4, "
            ".panel-heading h5, .card-header h3, .card-header h4"
        )
        if not title_el:
            continue
        yr = parse_spring_year(title_el.get_text(strip=True))
        if not yr:
            continue
        spring_years.add(yr)
        stype = season_type(title_el.get_text(strip=True))
        for table in panel.select("table"):
            for row in parse_table(table, yr, stype, athlete_id):
                key = (row["event"], row["mark_display"], row["year"], row["season"])
                if key not in seen_global:
                    seen_global.add(key)
                    all_perfs.append(row)

    gender = None
    early  = soup.get_text()[:800].lower()
    if "women" in early or "female" in early:
        gender = "F"
    elif re.search(r"\bmen\b", early) or "male" in early:
        gender = "M"

    spring_list = sorted(spring_years)
    return all_perfs, {
        "spring_years": spring_list,
        "last_spring":  spring_list[-1] if spring_list else None,
        "first_spring": spring_list[0]  if spring_list else None,
        "num_seasons":  len(spring_list),
        "gender":       gender,
    }


def scrape_athlete(url: str, athlete_id: int = 0) -> tuple:
    soup = fetch(print_url(url))
    if soup:
        perfs, info = scrape_print_page(soup, athlete_id)
        if info["num_seasons"] > 0:
            return perfs, info
    soup = fetch(url)
    if soup:
        return scrape_panel_page(soup, athlete_id)
    return [], {"spring_years": [], "last_spring": None,
                "first_spring": None, "num_seasons": 0, "gender": None}


# ── Team URL helpers ──────────────────────────────────────────────────────────

def find_team_url(athlete_soup: BeautifulSoup) -> Optional[str]:
    for link in athlete_soup.select("ol.breadcrumb a, .breadcrumb a"):
        href = link.get("href", "")
        if "/teams/" in href:
            return href if href.startswith("http") else TFRRS_BASE + href
    for link in athlete_soup.select("a[href*='/teams/tf/'], a[href*='/teams/xc/']"):
        href = link.get("href", "")
        if href and len(link.get_text(strip=True)) > 3:
            return href if href.startswith("http") else TFRRS_BASE + href
    return None


def team_base_url(team_url: str) -> str:
    """Strip .html to get the base for year-specific pages."""
    return re.sub(r"\.html$", "", team_url.rstrip("/"))


def year_roster_url(base: str, year: int) -> str:
    """Build a year-specific TFRRS roster URL: {base}/{year}.html"""
    return f"{base}/{year}.html"


def scrape_team_roster(roster_url: str) -> list:
    soup = fetch(roster_url)
    if not soup:
        return []

    athletes:  list = []
    seen_ids:  set  = set()

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
        athletes.append({"name": name, "tfrrs_url": full_url, "tfrrs_id": tfrrs_id})

    log.info(f"    Roster: {len(athletes)} athletes")
    return athletes


# ── Supabase helpers ──────────────────────────────────────────────────────────

def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def load_schools(supabase: Client) -> list:
    result = (
        supabase.table("athletes")
        .select("college, conference, tfrrs_url")
        .eq("source", "tfrrs")
        .not_.is_("tfrrs_url", "null")
        .neq("tfrrs_url", "")
        .execute()
    )
    seen: dict = {}
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


def load_existing_ids(supabase: Client) -> set:
    """Paginate through all TFRRS athletes to build the full skip-set."""
    ids:        set = set()
    batch_size: int = 1000
    offset:     int = 0
    while True:
        result = (
            supabase.table("athletes")
            .select("id, source_id")
            .eq("source", "tfrrs")
            .range(offset, offset + batch_size - 1)
            .execute()
        )
        rows = result.data or []
        for row in rows:
            if row.get("id"):
                ids.add(str(row["id"]))
            if row.get("source_id"):
                ids.add(str(row["source_id"]))
        if len(rows) < batch_size:
            break
        offset += batch_size
    log.info(f"  {len(ids)} athletes already in DB")
    return ids


def insert_athlete(supabase: Client, payload: dict, dry_run: bool) -> Optional[str]:
    if dry_run:
        log.info(
            f"    [DRY RUN] INSERT: {payload['name']} | "
            f"{payload['college']} | grad_year={payload.get('grad_year')}"
        )
        return None
    try:
        res = supabase.table("athletes").upsert(payload, ignore_duplicates=True).execute()
        if res.data:
            return res.data[0]["id"]
    except Exception as e:
        log.warning(f"    Insert failed ({payload.get('name')}): {e}")
    return None


def update_athlete_grad_year(
    supabase: Client, athlete_id: str, grad_year: int,
    hs_grad_year: int, events: list, dry_run: bool,
) -> bool:
    if dry_run:
        log.info(f"    [DRY RUN] UPDATE grad_year={grad_year} on id={athlete_id}")
        return True
    try:
        payload: dict = {"grad_year": grad_year, "hs_grad_year": hs_grad_year}
        if events:
            payload["events"] = events
        supabase.table("athletes").update(payload).eq("id", athlete_id).execute()
        return True
    except Exception as e:
        log.warning(f"    Update failed for {athlete_id}: {e}")
        return False


def insert_performances(supabase: Client, perfs: list, dry_run: bool):
    if not perfs:
        return
    if dry_run:
        log.info(f"    [DRY RUN] INSERT {len(perfs)} performances")
        return
    try:
        for i in range(0, len(perfs), 50):
            supabase.table("performances").insert(perfs[i:i + 50]).execute()
    except Exception as e:
        log.warning(f"    Performance insert failed: {e}")


# ── Core athlete processing ───────────────────────────────────────────────────

def process_athlete(
    candidate:    dict,
    school:       dict,
    target_year:  int,
    existing_ids: set,
    supabase:     Client,
    dry_run:      bool,
) -> str:
    """
    Evaluate one athlete from a year-specific roster.

    Checks that last_spring == target_year (they were a senior that year).
    Returns: "added" | "updated" | "not_senior" | "error"
    """
    tfrrs_id       = candidate["tfrrs_id"]
    source_id      = f"tfrrs_{tfrrs_id}"
    already_exists = source_id in existing_ids

    url = candidate["tfrrs_url"]
    perfs, info = scrape_athlete(url, athlete_id=0)
    time.sleep(REQUEST_DELAY)

    last_spring = info.get("last_spring")

    # Must have finished their career in exactly target_year
    if last_spring != target_year:
        return "not_senior"

    # Require at least 2 seasons — filters walk-ons / redshirt transfers
    if info.get("num_seasons", 0) < 2:
        return "not_senior"

    grad_year    = last_spring
    hs_grad_year = last_spring - 4

    gender = info.get("gender")
    if not gender:
        u = url.lower()
        if "_w_" in u or "/women/" in u:
            gender = "F"
        elif "_m_" in u or "/men/" in u:
            gender = "M"

    name   = candidate["name"]
    events = sorted({p["event"] for p in perfs})

    log.info(
        f"    ✓ {name} | grad={grad_year} | hs_grad={hs_grad_year} | "
        f"{len(perfs)} perfs ({info['num_seasons']} seasons)"
    )

    if already_exists:
        update_athlete_grad_year(
            supabase, source_id, grad_year, hs_grad_year, events or None, dry_run
        )
        return "updated"

    payload = {
        "id":           source_id,
        "name":         name,
        "source":       "tfrrs",
        "source_id":    source_id,
        "tfrrs_url":    url,
        "college":      school["college"],
        "conference":   school["conference"],
        "hs_grad_year": hs_grad_year,
        "grad_year":    grad_year,
        "gender":       gender,
        "events":       events or None,
        "is_transfer":  None,
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
                        help="Read-only mode — log what would happen, write nothing")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max schools to process (default: all)")
    parser.add_argument("--years", type=int, nargs="+",
                        default=[2023, 2024, 2025], metavar="YEAR",
                        help="Target graduation spring years (default: 2023 2024 2025)")
    parser.add_argument("--debug", action="store_true",
                        help="Enable DEBUG logging")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    target_years = sorted(args.years)
    log.info(f"Target graduation years: {target_years}")
    if args.dry_run:
        log.info("DRY RUN — no writes to Supabase")

    supabase = get_supabase()
    schools  = load_schools(supabase)
    if args.limit:
        schools = schools[:args.limit]
        log.info(f"Limited to {args.limit} schools")

    existing_ids = load_existing_ids(supabase)
    counters = {"added": 0, "updated": 0, "not_senior": 0, "error": 0}
    total_schools = len(schools)

    for idx, school in enumerate(schools, 1):
        college    = school["college"]
        sample_url = school["sample_url"]
        log.info(f"\n[{idx}/{total_schools}] {college}")

        # ── Resolve team base URL ──────────────────────────────────────────────
        sample_soup = fetch(sample_url)
        time.sleep(REQUEST_DELAY)
        if not sample_soup:
            log.warning(f"  Could not fetch sample page for {college}")
            counters["error"] += 1
            continue

        team_url = find_team_url(sample_soup)

        # Fallback: construct from athlete URL slug
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

        base = team_base_url(team_url)
        log.info(f"  Base: {base}")

        # ── For each target year, scrape the year-specific roster ──────────────
        for year in target_years:
            roster_url = year_roster_url(base, year)
            log.info(f"  [{year}] {roster_url}")

            roster = scrape_team_roster(roster_url)
            time.sleep(REQUEST_DELAY)

            if not roster:
                log.info(f"  [{year}] No athletes (page may not exist)")
                continue

            added_yr   = 0
            updated_yr = 0

            for candidate in roster:
                result = process_athlete(
                    candidate, school, year,
                    existing_ids, supabase, args.dry_run,
                )
                counters[result] = counters.get(result, 0) + 1
                if result == "added":
                    added_yr += 1
                elif result == "updated":
                    updated_yr += 1

            log.info(f"  [{year}] → {added_yr} added, {updated_yr} updated")

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info(f"\n{'=' * 60}")
    log.info("Done.")
    log.info(f"  Schools processed : {total_schools}")
    log.info(f"  Athletes added    : {counters['added']}")
    log.info(f"  Athletes updated  : {counters['updated']}")
    log.info(f"  Not senior/target : {counters['not_senior']}")
    log.info(f"  Errors            : {counters['error']}")


if __name__ == "__main__":
    main()
