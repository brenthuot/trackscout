"""
historical_seniors_scraper.py
─────────────────────────────
Discovers athletes who were college seniors (4th year) in spring 2023,
spring 2024, or spring 2025 and adds them to the Supabase athletes table
with their full performance histories.

Architecture
────────────
1. Pull distinct schools + a sample athlete URL per school from Supabase.
   Falls back to TFRRS_TEAM_URLS for schools with no DB athletes yet.
2. From the sample athlete URL (or direct team URL), resolve the school's
   TFRRS team BASE URL.
3. For each target year, scrape the year-specific roster page:
     {base_url}?config_hnd={hnd}  →  every athlete on the team that year
4. For each athlete on that year's roster, fetch their PRINT PAGE (?print=1)
   and confirm their last active spring == target_year (i.e. they were a
   senior / final-year athlete that spring).
5. Insert new athletes with full performance history; update grad_year on
   athletes already in the DB from the main TFRRS scraper.

TFRRS historical roster URL pattern
─────────────────────────────────────
  Current roster:  .../teams/tf/MT_college_f_Montana_State.html
  2025 roster:     .../teams/tf/MT_college_f_Montana_State.html?config_hnd=380
  2024 roster:     .../teams/tf/MT_college_f_Montana_State.html?config_hnd=333
  2023 roster:     .../teams/tf/MT_college_f_Montana_State.html?config_hnd=290

Usage
─────
  python scraper/historical_seniors_scraper.py --dry-run
  python scraper/historical_seniors_scraper.py --limit 5
  python scraper/historical_seniors_scraper.py
  python scraper/historical_seniors_scraper.py --years 2024 2025
  python scraper/historical_seniors_scraper.py --school Syracuse
  python scraper/historical_seniors_scraper.py --school Syracuse --years 2023 2024 2025
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


# ── Season config_hnd values (global TFRRS season identifiers) ────────────────
# We use the OUTDOOR season (final season of each year) to find seniors.
SEASON_HND: dict[int, int] = {
    2023: 290,   # 2023 outdoor
    2024: 333,   # 2024 outdoor
    2025: 380,   # 2025 outdoor
}

# ── Fallback TFRRS team URLs ──────────────────────────────────────────────────
# Covers schools that may have zero athletes in the DB yet, so load_schools()
# would otherwise miss them entirely.
TFRRS_TEAM_URLS: list[dict] = [
    # ── ACC ───────────────────────────────────────────────────────────────────
    {"college": "Boston College",    "conference": "ACC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/MA_college_m_Boston_College.html"},
    {"college": "Boston College",    "conference": "ACC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/MA_college_f_Boston_College.html"},
    {"college": "California",        "conference": "ACC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/CA_college_m_California.html"},
    {"college": "California",        "conference": "ACC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/CA_college_f_California.html"},
    {"college": "Clemson",           "conference": "ACC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/SC_college_m_Clemson.html"},
    {"college": "Clemson",           "conference": "ACC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/SC_college_f_Clemson.html"},
    {"college": "Duke",              "conference": "ACC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/NC_college_m_Duke.html"},
    {"college": "Duke",              "conference": "ACC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/NC_college_f_Duke.html"},
    {"college": "Florida State",     "conference": "ACC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/FL_college_m_Florida_State.html"},
    {"college": "Florida State",     "conference": "ACC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/FL_college_f_Florida_State.html"},
    {"college": "Georgia Tech",      "conference": "ACC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/GA_college_m_Georgia_Tech.html"},
    {"college": "Georgia Tech",      "conference": "ACC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/GA_college_f_Georgia_Tech.html"},
    {"college": "Louisville",        "conference": "ACC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/KY_college_m_Louisville.html"},
    {"college": "Louisville",        "conference": "ACC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/KY_college_f_Louisville.html"},
    {"college": "Miami",             "conference": "ACC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/FL_college_m_Miami__FL_.html"},
    {"college": "Miami",             "conference": "ACC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/FL_college_f_Miami__FL_.html"},
    {"college": "NC State",          "conference": "ACC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/NC_college_m_NC_State.html"},
    {"college": "NC State",          "conference": "ACC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/NC_college_f_NC_State.html"},
    {"college": "North Carolina",    "conference": "ACC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/NC_college_m_North_Carolina.html"},
    {"college": "North Carolina",    "conference": "ACC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/NC_college_f_North_Carolina.html"},
    {"college": "Notre Dame",        "conference": "ACC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/IN_college_m_Notre_Dame.html"},
    {"college": "Notre Dame",        "conference": "ACC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/IN_college_f_Notre_Dame.html"},
    {"college": "Pittsburgh",        "conference": "ACC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/PA_college_m_Pittsburgh.html"},
    {"college": "Pittsburgh",        "conference": "ACC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/PA_college_f_Pittsburgh.html"},
    {"college": "SMU",               "conference": "ACC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/TX_college_m_SMU.html"},
    {"college": "SMU",               "conference": "ACC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/TX_college_f_SMU.html"},
    {"college": "Stanford",          "conference": "ACC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/CA_college_m_Stanford.html"},
    {"college": "Stanford",          "conference": "ACC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/CA_college_f_Stanford.html"},
    {"college": "Syracuse",          "conference": "ACC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/NY_college_m_Syracuse.html"},
    {"college": "Syracuse",          "conference": "ACC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/NY_college_f_Syracuse.html"},
    {"college": "Virginia",          "conference": "ACC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/VA_college_m_Virginia.html"},
    {"college": "Virginia",          "conference": "ACC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/VA_college_f_Virginia.html"},
    {"college": "Virginia Tech",     "conference": "ACC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/VA_college_m_Virginia_Tech.html"},
    {"college": "Virginia Tech",     "conference": "ACC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/VA_college_f_Virginia_Tech.html"},
    {"college": "Wake Forest",       "conference": "ACC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/NC_college_m_Wake_Forest.html"},
    {"college": "Wake Forest",       "conference": "ACC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/NC_college_f_Wake_Forest.html"},
    # ── Big Ten ───────────────────────────────────────────────────────────────
    {"college": "Illinois",          "conference": "Big Ten",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/IL_college_m_Illinois.html"},
    {"college": "Illinois",          "conference": "Big Ten",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/IL_college_f_Illinois.html"},
    {"college": "Indiana",           "conference": "Big Ten",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/IN_college_m_Indiana.html"},
    {"college": "Indiana",           "conference": "Big Ten",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/IN_college_f_Indiana.html"},
    {"college": "Iowa",              "conference": "Big Ten",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/IA_college_m_Iowa.html"},
    {"college": "Iowa",              "conference": "Big Ten",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/IA_college_f_Iowa.html"},
    {"college": "Maryland",          "conference": "Big Ten",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/MD_college_m_Maryland.html"},
    {"college": "Maryland",          "conference": "Big Ten",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/MD_college_f_Maryland.html"},
    {"college": "Michigan",          "conference": "Big Ten",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/MI_college_m_Michigan.html"},
    {"college": "Michigan",          "conference": "Big Ten",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/MI_college_f_Michigan.html"},
    {"college": "Michigan State",    "conference": "Big Ten",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/MI_college_m_Michigan_State.html"},
    {"college": "Michigan State",    "conference": "Big Ten",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/MI_college_f_Michigan_State.html"},
    {"college": "Minnesota",         "conference": "Big Ten",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/MN_college_m_Minnesota.html"},
    {"college": "Minnesota",         "conference": "Big Ten",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/MN_college_f_Minnesota.html"},
    {"college": "Nebraska",          "conference": "Big Ten",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/NE_college_m_Nebraska.html"},
    {"college": "Nebraska",          "conference": "Big Ten",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/NE_college_f_Nebraska.html"},
    {"college": "Northwestern",      "conference": "Big Ten",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/IL_college_m_Northwestern.html"},
    {"college": "Northwestern",      "conference": "Big Ten",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/IL_college_f_Northwestern.html"},
    {"college": "Ohio State",        "conference": "Big Ten",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/OH_college_m_Ohio_State.html"},
    {"college": "Ohio State",        "conference": "Big Ten",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/OH_college_f_Ohio_State.html"},
    {"college": "Oregon",            "conference": "Big Ten",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/OR_college_m_Oregon.html"},
    {"college": "Oregon",            "conference": "Big Ten",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/OR_college_f_Oregon.html"},
    {"college": "Penn State",        "conference": "Big Ten",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/PA_college_m_Penn_State.html"},
    {"college": "Penn State",        "conference": "Big Ten",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/PA_college_f_Penn_State.html"},
    {"college": "Purdue",            "conference": "Big Ten",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/IN_college_m_Purdue.html"},
    {"college": "Purdue",            "conference": "Big Ten",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/IN_college_f_Purdue.html"},
    {"college": "Rutgers",           "conference": "Big Ten",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/NJ_college_m_Rutgers.html"},
    {"college": "Rutgers",           "conference": "Big Ten",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/NJ_college_f_Rutgers.html"},
    {"college": "UCLA",              "conference": "Big Ten",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/CA_college_m_UCLA.html"},
    {"college": "UCLA",              "conference": "Big Ten",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/CA_college_f_UCLA.html"},
    {"college": "USC",               "conference": "Big Ten",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/CA_college_m_USC.html"},
    {"college": "USC",               "conference": "Big Ten",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/CA_college_f_USC.html"},
    {"college": "Washington",        "conference": "Big Ten",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/WA_college_m_Washington.html"},
    {"college": "Washington",        "conference": "Big Ten",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/WA_college_f_Washington.html"},
    {"college": "Wisconsin",         "conference": "Big Ten",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/WI_college_m_Wisconsin.html"},
    {"college": "Wisconsin",         "conference": "Big Ten",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/WI_college_f_Wisconsin.html"},
    # ── SEC ───────────────────────────────────────────────────────────────────
    {"college": "Alabama",           "conference": "SEC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/AL_college_m_Alabama.html"},
    {"college": "Alabama",           "conference": "SEC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/AL_college_f_Alabama.html"},
    {"college": "Arkansas",          "conference": "SEC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/AR_college_m_Arkansas.html"},
    {"college": "Arkansas",          "conference": "SEC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/AR_college_f_Arkansas.html"},
    {"college": "Auburn",            "conference": "SEC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/AL_college_m_Auburn.html"},
    {"college": "Auburn",            "conference": "SEC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/AL_college_f_Auburn.html"},
    {"college": "Florida",           "conference": "SEC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/FL_college_m_Florida.html"},
    {"college": "Florida",           "conference": "SEC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/FL_college_f_Florida.html"},
    {"college": "Georgia",           "conference": "SEC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/GA_college_m_Georgia.html"},
    {"college": "Georgia",           "conference": "SEC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/GA_college_f_Georgia.html"},
    {"college": "Kentucky",          "conference": "SEC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/KY_college_m_Kentucky.html"},
    {"college": "Kentucky",          "conference": "SEC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/KY_college_f_Kentucky.html"},
    {"college": "LSU",               "conference": "SEC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/LA_college_m_LSU.html"},
    {"college": "LSU",               "conference": "SEC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/LA_college_f_LSU.html"},
    {"college": "Mississippi State", "conference": "SEC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/MS_college_m_Mississippi_State.html"},
    {"college": "Mississippi State", "conference": "SEC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/MS_college_f_Mississippi_State.html"},
    {"college": "Missouri",          "conference": "SEC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/MO_college_m_Missouri.html"},
    {"college": "Missouri",          "conference": "SEC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/MO_college_f_Missouri.html"},
    {"college": "Oklahoma",          "conference": "SEC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/OK_college_m_Oklahoma.html"},
    {"college": "Oklahoma",          "conference": "SEC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/OK_college_f_Oklahoma.html"},
    {"college": "Ole Miss",          "conference": "SEC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/MS_college_m_Ole_Miss.html"},
    {"college": "Ole Miss",          "conference": "SEC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/MS_college_f_Ole_Miss.html"},
    {"college": "South Carolina",    "conference": "SEC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/SC_college_m_South_Carolina.html"},
    {"college": "South Carolina",    "conference": "SEC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/SC_college_f_South_Carolina.html"},
    {"college": "Tennessee",         "conference": "SEC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/TN_college_m_Tennessee.html"},
    {"college": "Tennessee",         "conference": "SEC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/TN_college_f_Tennessee.html"},
    {"college": "Texas",             "conference": "SEC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/TX_college_m_Texas.html"},
    {"college": "Texas",             "conference": "SEC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/TX_college_f_Texas.html"},
    {"college": "Texas A&M",         "conference": "SEC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/TX_college_m_Texas_A_M.html"},
    {"college": "Texas A&M",         "conference": "SEC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/TX_college_f_Texas_A_M.html"},
    {"college": "Vanderbilt",        "conference": "SEC",         "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/TN_college_m_Vanderbilt.html"},
    {"college": "Vanderbilt",        "conference": "SEC",         "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/TN_college_f_Vanderbilt.html"},
    # ── Big 12 ────────────────────────────────────────────────────────────────
    {"college": "Arizona",           "conference": "Big 12",      "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/AZ_college_m_Arizona.html"},
    {"college": "Arizona",           "conference": "Big 12",      "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/AZ_college_f_Arizona.html"},
    {"college": "Arizona State",     "conference": "Big 12",      "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/AZ_college_m_Arizona_State.html"},
    {"college": "Arizona State",     "conference": "Big 12",      "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/AZ_college_f_Arizona_State.html"},
    {"college": "Baylor",            "conference": "Big 12",      "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/TX_college_m_Baylor.html"},
    {"college": "Baylor",            "conference": "Big 12",      "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/TX_college_f_Baylor.html"},
    {"college": "BYU",               "conference": "Big 12",      "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/UT_college_m_BYU.html"},
    {"college": "BYU",               "conference": "Big 12",      "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/UT_college_f_BYU.html"},
    {"college": "Cincinnati",        "conference": "Big 12",      "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/OH_college_m_Cincinnati.html"},
    {"college": "Cincinnati",        "conference": "Big 12",      "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/OH_college_f_Cincinnati.html"},
    {"college": "Colorado",          "conference": "Big 12",      "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/CO_college_m_Colorado.html"},
    {"college": "Colorado",          "conference": "Big 12",      "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/CO_college_f_Colorado.html"},
    {"college": "Houston",           "conference": "Big 12",      "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/TX_college_m_Houston.html"},
    {"college": "Houston",           "conference": "Big 12",      "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/TX_college_f_Houston.html"},
    {"college": "Iowa State",        "conference": "Big 12",      "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/IA_college_m_Iowa_State.html"},
    {"college": "Iowa State",        "conference": "Big 12",      "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/IA_college_f_Iowa_State.html"},
    {"college": "Kansas",            "conference": "Big 12",      "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/KS_college_m_Kansas.html"},
    {"college": "Kansas",            "conference": "Big 12",      "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/KS_college_f_Kansas.html"},
    {"college": "Kansas State",      "conference": "Big 12",      "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/KS_college_m_Kansas_State.html"},
    {"college": "Kansas State",      "conference": "Big 12",      "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/KS_college_f_Kansas_State.html"},
    {"college": "Oklahoma State",    "conference": "Big 12",      "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/OK_college_m_Oklahoma_State.html"},
    {"college": "Oklahoma State",    "conference": "Big 12",      "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/OK_college_f_Oklahoma_State.html"},
    {"college": "TCU",               "conference": "Big 12",      "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/TX_college_m_TCU.html"},
    {"college": "TCU",               "conference": "Big 12",      "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/TX_college_f_TCU.html"},
    {"college": "Texas Tech",        "conference": "Big 12",      "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/TX_college_m_Texas_Tech.html"},
    {"college": "Texas Tech",        "conference": "Big 12",      "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/TX_college_f_Texas_Tech.html"},
    {"college": "Utah",              "conference": "Big 12",      "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/UT_college_m_Utah.html"},
    {"college": "Utah",              "conference": "Big 12",      "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/UT_college_f_Utah.html"},
    {"college": "West Virginia",     "conference": "Big 12",      "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/WV_college_m_West_Virginia.html"},
    {"college": "West Virginia",     "conference": "Big 12",      "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/WV_college_f_West_Virginia.html"},
    # ── Ivy League ────────────────────────────────────────────────────────────
    {"college": "Brown",             "conference": "Ivy League",  "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/RI_college_m_Brown.html"},
    {"college": "Brown",             "conference": "Ivy League",  "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/RI_college_f_Brown.html"},
    {"college": "Columbia",          "conference": "Ivy League",  "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/NY_college_m_Columbia.html"},
    {"college": "Columbia",          "conference": "Ivy League",  "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/NY_college_f_Columbia.html"},
    {"college": "Cornell",           "conference": "Ivy League",  "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/NY_college_m_Cornell.html"},
    {"college": "Cornell",           "conference": "Ivy League",  "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/NY_college_f_Cornell.html"},
    {"college": "Dartmouth",         "conference": "Ivy League",  "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/NH_college_m_Dartmouth.html"},
    {"college": "Dartmouth",         "conference": "Ivy League",  "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/NH_college_f_Dartmouth.html"},
    {"college": "Harvard",           "conference": "Ivy League",  "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/MA_college_m_Harvard.html"},
    {"college": "Harvard",           "conference": "Ivy League",  "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/MA_college_f_Harvard.html"},
    {"college": "Pennsylvania",      "conference": "Ivy League",  "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/PA_college_m_Pennsylvania.html"},
    {"college": "Pennsylvania",      "conference": "Ivy League",  "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/PA_college_f_Pennsylvania.html"},
    {"college": "Princeton",         "conference": "Ivy League",  "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/NJ_college_m_Princeton.html"},
    {"college": "Princeton",         "conference": "Ivy League",  "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/NJ_college_f_Princeton.html"},
    {"college": "Yale",              "conference": "Ivy League",  "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/CT_college_m_Yale.html"},
    {"college": "Yale",              "conference": "Ivy League",  "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/CT_college_f_Yale.html"},
    # ── Big East ──────────────────────────────────────────────────────────────
    {"college": "Butler",            "conference": "Big East",    "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/IN_college_m_Butler.html"},
    {"college": "Butler",            "conference": "Big East",    "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/IN_college_f_Butler.html"},
    {"college": "Connecticut",       "conference": "Big East",    "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/CT_college_m_Connecticut.html"},
    {"college": "Connecticut",       "conference": "Big East",    "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/CT_college_f_Connecticut.html"},
    {"college": "Georgetown",        "conference": "Big East",    "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/DC_college_m_Georgetown.html"},
    {"college": "Georgetown",        "conference": "Big East",    "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/DC_college_f_Georgetown.html"},
    {"college": "Villanova",         "conference": "Big East",    "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/PA_college_m_Villanova.html"},
    {"college": "Villanova",         "conference": "Big East",    "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/PA_college_f_Villanova.html"},
    {"college": "Xavier",            "conference": "Big East",    "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/OH_college_m_Xavier.html"},
    {"college": "Xavier",            "conference": "Big East",    "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/OH_college_f_Xavier.html"},
    # ── Mountain West ─────────────────────────────────────────────────────────
    {"college": "Air Force",         "conference": "Mountain West","gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/CO_college_m_Air_Force.html"},
    {"college": "Air Force",         "conference": "Mountain West","gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/CO_college_f_Air_Force.html"},
    {"college": "Boise State",       "conference": "Mountain West","gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/ID_college_m_Boise_State.html"},
    {"college": "Boise State",       "conference": "Mountain West","gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/ID_college_f_Boise_State.html"},
    {"college": "Colorado State",    "conference": "Mountain West","gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/CO_college_m_Colorado_State.html"},
    {"college": "Colorado State",    "conference": "Mountain West","gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/CO_college_f_Colorado_State.html"},
    {"college": "Fresno State",      "conference": "Mountain West","gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/CA_college_m_Fresno_State.html"},
    {"college": "Fresno State",      "conference": "Mountain West","gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/CA_college_f_Fresno_State.html"},
    {"college": "Hawaii",            "conference": "Mountain West","gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/HI_college_m_Hawaii.html"},
    {"college": "Hawaii",            "conference": "Mountain West","gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/HI_college_f_Hawaii.html"},
    {"college": "Nevada",            "conference": "Mountain West","gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/NV_college_m_Nevada.html"},
    {"college": "Nevada",            "conference": "Mountain West","gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/NV_college_f_Nevada.html"},
    {"college": "New Mexico",        "conference": "Mountain West","gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/NM_college_m_New_Mexico.html"},
    {"college": "New Mexico",        "conference": "Mountain West","gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/NM_college_f_New_Mexico.html"},
    {"college": "San Diego State",   "conference": "Mountain West","gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/CA_college_m_San_Diego_State.html"},
    {"college": "San Diego State",   "conference": "Mountain West","gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/CA_college_f_San_Diego_State.html"},
    {"college": "San Jose State",    "conference": "Mountain West","gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/CA_college_m_San_Jose_State.html"},
    {"college": "San Jose State",    "conference": "Mountain West","gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/CA_college_f_San_Jose_State.html"},
    {"college": "UNLV",              "conference": "Mountain West","gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/NV_college_m_UNLV.html"},
    {"college": "UNLV",              "conference": "Mountain West","gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/NV_college_f_UNLV.html"},
    {"college": "Utah State",        "conference": "Mountain West","gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/UT_college_m_Utah_State.html"},
    {"college": "Utah State",        "conference": "Mountain West","gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/UT_college_f_Utah_State.html"},
    {"college": "Wyoming",           "conference": "Mountain West","gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/WY_college_m_Wyoming.html"},
    {"college": "Wyoming",           "conference": "Mountain West","gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/WY_college_f_Wyoming.html"},
    # ── Atlantic 10 ───────────────────────────────────────────────────────────
    {"college": "Dayton",            "conference": "Atlantic 10", "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/OH_college_m_Dayton.html"},
    {"college": "Dayton",            "conference": "Atlantic 10", "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/OH_college_f_Dayton.html"},
    {"college": "Fordham",           "conference": "Atlantic 10", "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/NY_college_m_Fordham.html"},
    {"college": "Fordham",           "conference": "Atlantic 10", "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/NY_college_f_Fordham.html"},
    {"college": "George Mason",      "conference": "Atlantic 10", "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/VA_college_m_George_Mason.html"},
    {"college": "George Mason",      "conference": "Atlantic 10", "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/VA_college_f_George_Mason.html"},
    {"college": "Rhode Island",      "conference": "Atlantic 10", "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/RI_college_m_Rhode_Island.html"},
    {"college": "Rhode Island",      "conference": "Atlantic 10", "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/RI_college_f_Rhode_Island.html"},
    {"college": "Richmond",          "conference": "Atlantic 10", "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/VA_college_m_Richmond.html"},
    {"college": "Richmond",          "conference": "Atlantic 10", "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/VA_college_f_Richmond.html"},
    {"college": "Saint Louis",       "conference": "Atlantic 10", "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/MO_college_m_Saint_Louis.html"},
    {"college": "Saint Louis",       "conference": "Atlantic 10", "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/MO_college_f_Saint_Louis.html"},
    {"college": "VCU",               "conference": "Atlantic 10", "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/VA_college_m_VCU.html"},
    {"college": "VCU",               "conference": "Atlantic 10", "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/VA_college_f_VCU.html"},
    # ── Big Sky ───────────────────────────────────────────────────────────────
    {"college": "Montana",           "conference": "Big Sky",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/MT_college_m_Montana.html"},
    {"college": "Montana",           "conference": "Big Sky",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/MT_college_f_Montana.html"},
    {"college": "Montana State",     "conference": "Big Sky",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/MT_college_m_Montana_State.html"},
    {"college": "Montana State",     "conference": "Big Sky",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/MT_college_f_Montana_State.html"},
    {"college": "Northern Arizona",  "conference": "Big Sky",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/AZ_college_m_Northern_Arizona.html"},
    {"college": "Northern Arizona",  "conference": "Big Sky",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/AZ_college_f_Northern_Arizona.html"},
    {"college": "Northern Colorado", "conference": "Big Sky",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/CO_college_m_Northern_Colorado.html"},
    {"college": "Northern Colorado", "conference": "Big Sky",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/CO_college_f_Northern_Colorado.html"},
    {"college": "Sacramento State",  "conference": "Big Sky",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/CA_college_m_Sacramento_State.html"},
    {"college": "Sacramento State",  "conference": "Big Sky",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/CA_college_f_Sacramento_State.html"},
    {"college": "Weber State",       "conference": "Big Sky",     "gender": "M", "team_url": "https://www.tfrrs.org/teams/tf/UT_college_m_Weber_State.html"},
    {"college": "Weber State",       "conference": "Big Sky",     "gender": "F", "team_url": "https://www.tfrrs.org/teams/tf/UT_college_f_Weber_State.html"},
]


def year_roster_url(team_url: str, year: int) -> str:
    """
    Build a year-specific TFRRS roster URL using the global config_hnd.

    team_url = https://www.tfrrs.org/teams/tf/NY_college_m_Syracuse.html
    year     = 2023
    →          https://www.tfrrs.org/teams/tf/NY_college_m_Syracuse.html?config_hnd=290
    """
    base = team_url.split("?")[0]   # strip any existing query string
    hnd  = SEASON_HND.get(year)
    if not hnd:
        raise ValueError(f"No config_hnd known for year {year}")
    return f"{base}?config_hnd={hnd}"


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
    """
    Return one entry per (college, gender) combination so both men's and
    women's teams are scraped for each school.

    Primary source: Supabase athletes table (gives a real sample_url so
    find_team_url() can resolve the TFRRS team page).

    Fallback: TFRRS_TEAM_URLS, which stores the team URL directly.
    Schools already in the DB are NOT duplicated — the fallback only adds
    schools with zero athletes in the DB yet.
    """
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
        url     = (row.get("tfrrs_url") or "").strip()
        if not college or not url:
            continue

        if "_college_m_" in url or "_m_" in url:
            gender = "M"
        elif "_college_f_" in url or "_f_" in url:
            gender = "F"
        else:
            gender = "U"

        key = (college, gender)
        if key not in seen:
            seen[key] = {
                "college":    college,
                "conference": (row.get("conference") or "").strip(),
                "sample_url": url,
                "team_url":   None,   # resolved dynamically via find_team_url()
                "gender":     gender,
            }

    # ── Merge fallback entries ────────────────────────────────────────────────
    fallback_added = 0
    for entry in TFRRS_TEAM_URLS:
        key = (entry["college"], entry["gender"])
        if key not in seen:
            seen[key] = {
                "college":    entry["college"],
                "conference": entry["conference"],
                "sample_url": None,           # not needed — team_url is pre-set
                "team_url":   entry["team_url"],
                "gender":     entry["gender"],
            }
            fallback_added += 1

    teams = list(seen.values())
    log.info(
        f"Loaded {len(teams)} distinct school/gender teams "
        f"({len(teams) - fallback_added} from DB, {fallback_added} from fallback list)"
    )
    return teams


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
    parser.add_argument("--school", type=str, default=None,
                        help="Process only this school by name, e.g. --school Syracuse")
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

    # ── Optional single-school filter ─────────────────────────────────────────
    if args.school:
        schools = [s for s in schools if s["college"].lower() == args.school.lower()]
        if not schools:
            log.error(f"No school found matching '{args.school}' — check spelling")
            sys.exit(1)
        log.info(f"Filtered to school: {args.school} ({len(schools)} team(s))")

    if args.limit:
        schools = schools[:args.limit]
        log.info(f"Limited to {args.limit} schools")

    existing_ids  = load_existing_ids(supabase)
    counters      = {"added": 0, "updated": 0, "not_senior": 0, "error": 0}
    total_schools = len(schools)

    for idx, school in enumerate(schools, 1):
        college  = school["college"]
        log.info(f"\n[{idx}/{total_schools}] {college} ({school['gender']})")

        # ── Resolve team base URL ──────────────────────────────────────────────
        # Fallback entries from TFRRS_TEAM_URLS already have team_url set, so
        # we skip the sample-page fetch for those.
        team_url = school.get("team_url")

        if not team_url:
            sample_url  = school["sample_url"]
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

        log.info(f"  Team URL: {team_url}")

        # ── For each target year, scrape the year-specific roster ──────────────
        for year in target_years:
            if year not in SEASON_HND:
                log.warning(f"  [{year}] No config_hnd known, skipping")
                continue
            roster_url = year_roster_url(team_url, year)
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
