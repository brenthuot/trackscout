"""
Run Stats — TFRRS Scraper v3
Uses correct TFRRS URL format: /teams/tf/{STATE}_college_{m/f}_{Slug}.html
Scrapes both Men's and Women's rosters.
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
    "Referer": "https://www.tfrrs.org/",
}
RATE_LIMIT_SECONDS = 2.5
MAX_ATHLETES_PER_RUN = 300

# ── TEAM DEFINITIONS ──────────────────────────────────────────────────────────
# Format: "Display Name": (state_code, slug, conference)
# URL built as: /teams/tf/{state_code}_college_{m/f}_{slug}.html
# Verified pattern from: NY_college_m_Syracuse, CA_college_m_UCLA

TEAMS = {
    # ── SEC ───────────────────────────────────────────────────────────────────
    "Alabama":            ("AL", "Alabama",            "SEC"),
    "Arkansas":           ("AR", "Arkansas",           "SEC"),
    "Auburn":             ("AL", "Auburn",             "SEC"),
    "Florida":            ("FL", "Florida",            "SEC"),
    "Georgia":            ("GA", "Georgia",            "SEC"),
    "Kentucky":           ("KY", "Kentucky",           "SEC"),
    "LSU":                ("LA", "LSU",                "SEC"),
    "Mississippi State":  ("MS", "Mississippi_State",  "SEC"),
    "Missouri":           ("MO", "Missouri",           "SEC"),
    "Ole Miss":           ("MS", "Ole_Miss",           "SEC"),
    "South Carolina":     ("SC", "South_Carolina",     "SEC"),
    "Tennessee":          ("TN", "Tennessee",          "SEC"),
    "Texas A&M":          ("TX", "Texas_AM",           "SEC"),
    "Vanderbilt":         ("TN", "Vanderbilt",         "SEC"),

    # ── Big Ten ───────────────────────────────────────────────────────────────
    "Illinois":           ("IL", "Illinois",           "Big Ten"),
    "Indiana":            ("IN", "Indiana",            "Big Ten"),
    "Iowa":               ("IA", "Iowa",               "Big Ten"),
    "Maryland":           ("MD", "Maryland",           "Big Ten"),
    "Michigan":           ("MI", "Michigan",           "Big Ten"),
    "Michigan State":     ("MI", "Michigan_State",     "Big Ten"),
    "Minnesota":          ("MN", "Minnesota",          "Big Ten"),
    "Nebraska":           ("NE", "Nebraska",           "Big Ten"),
    "Northwestern":       ("IL", "Northwestern",       "Big Ten"),
    "Ohio State":         ("OH", "Ohio_State",         "Big Ten"),
    "Penn State":         ("PA", "Penn_State",         "Big Ten"),
    "Purdue":             ("IN", "Purdue",             "Big Ten"),
    "Rutgers":            ("NJ", "Rutgers",            "Big Ten"),
    "Wisconsin":          ("WI", "Wisconsin",          "Big Ten"),

    # ── ACC ───────────────────────────────────────────────────────────────────
    "Boston College":     ("MA", "Boston_College",     "ACC"),
    "Clemson":            ("SC", "Clemson",            "ACC"),
    "Duke":               ("NC", "Duke",               "ACC"),
    "Florida State":      ("FL", "Florida_State",      "ACC"),
    "Georgia Tech":       ("GA", "Georgia_Tech",       "ACC"),
    "Louisville":         ("KY", "Louisville",         "ACC"),
    "Miami (FL)":         ("FL", "Miami",              "ACC"),
    "NC State":           ("NC", "NC_State",           "ACC"),
    "North Carolina":     ("NC", "North_Carolina",     "ACC"),
    "Notre Dame":         ("IN", "Notre_Dame",         "ACC"),
    "Pittsburgh":         ("PA", "Pittsburgh",         "ACC"),
    "Syracuse":           ("NY", "Syracuse",           "ACC"),
    "Virginia":           ("VA", "Virginia",           "ACC"),
    "Virginia Tech":      ("VA", "Virginia_Tech",      "ACC"),
    "Wake Forest":        ("NC", "Wake_Forest",        "ACC"),

    # ── Big 12 ────────────────────────────────────────────────────────────────
    "Baylor":             ("TX", "Baylor",             "Big 12"),
    "BYU":                ("UT", "BYU",                "Big 12"),
    "Iowa State":         ("IA", "Iowa_State",         "Big 12"),
    "Kansas":             ("KS", "Kansas",             "Big 12"),
    "Kansas State":       ("KS", "Kansas_State",       "Big 12"),
    "Oklahoma State":     ("OK", "Oklahoma_State",     "Big 12"),
    "TCU":                ("TX", "TCU",                "Big 12"),
    "Texas":              ("TX", "Texas",              "Big 12"),
    "Texas Tech":         ("TX", "Texas_Tech",         "Big 12"),
    "West Virginia":      ("WV", "West_Virginia",      "Big 12"),

    # ── Pac-12 ────────────────────────────────────────────────────────────────
    "Arizona":            ("AZ", "Arizona",            "Pac-12"),
    "Arizona State":      ("AZ", "Arizona_State",      "Pac-12"),
    "Cal":                ("CA", "California",         "Pac-12"),
    "Colorado":           ("CO", "Colorado",           "Pac-12"),
    "Oregon":             ("OR", "Oregon",             "Pac-12"),
    "Oregon State":       ("OR", "Oregon_State",       "Pac-12"),
    "Stanford":           ("CA", "Stanford",           "Pac-12"),
    "UCLA":               ("CA", "UCLA",               "Pac-12"),
    "USC":                ("CA", "USC",                "Pac-12"),
    "Utah":               ("UT", "Utah",               "Pac-12"),
    "Washington":         ("WA", "Washington",         "Pac-12"),
    "Washington State":   ("WA", "Washington_State",   "Pac-12"),

    # ── Ivy League ────────────────────────────────────────────────────────────
    "Brown":              ("RI", "Brown",              "Ivy League"),
    "Columbia":           ("NY", "Columbia",           "Ivy League"),
    "Cornell":            ("NY", "Cornell",            "Ivy League"),
    "Dartmouth":          ("NH", "Dartmouth",          "Ivy League"),
    "Harvard":            ("MA", "Harvard",            "Ivy League"),
    "Penn":               ("PA", "Penn",               "Ivy League"),
    "Princeton":          ("NJ", "Princeton",          "Ivy League"),
    "Yale":               ("CT", "Yale",               "Ivy League"),

    # ── Big East ──────────────────────────────────────────────────────────────
    "Butler":             ("IN", "Butler",             "Big East"),
    "Creighton":          ("NE", "Creighton",          "Big East"),
    "DePaul":             ("IL", "DePaul",             "Big East"),
    "Georgetown":         ("DC", "Georgetown",         "Big East"),
    "Marquette":          ("WI", "Marquette",          "Big East"),
    "Providence":         ("RI", "Providence",         "Big East"),
    "Seton Hall":         ("NJ", "Seton_Hall",         "Big East"),
    "St. John's":         ("NY", "St_Johns",           "Big East"),
    "Villanova":          ("PA", "Villanova",          "Big East"),
    "Xavier":             ("OH", "Xavier",             "Big East"),
    "UConn":              ("CT", "Connecticut",        "Big East"),

    # ── Mountain West ─────────────────────────────────────────────────────────
    "Air Force":          ("CO", "Air_Force",          "Mountain West"),
    "Boise State":        ("ID", "Boise_State",        "Mountain West"),
    "Colorado State":     ("CO", "Colorado_State",     "Mountain West"),
    "Fresno State":       ("CA", "Fresno_State",       "Mountain West"),
    "Hawaii":             ("HI", "Hawaii",             "Mountain West"),
    "Nevada":             ("NV", "Nevada",             "Mountain West"),
    "New Mexico":         ("NM", "New_Mexico",         "Mountain West"),
    "San Diego State":    ("CA", "San_Diego_State",    "Mountain West"),
    "San Jose State":     ("CA", "San_Jose_State",     "Mountain West"),
    "UNLV":               ("NV", "UNLV",               "Mountain West"),
    "Utah State":         ("UT", "Utah_State",         "Mountain West"),
    "Wyoming":            ("WY", "Wyoming",            "Mountain West"),

    # ── Big Sky ───────────────────────────────────────────────────────────────
    "Eastern Washington": ("WA", "Eastern_Washington", "Big Sky"),
    "Idaho":              ("ID", "Idaho",              "Big Sky"),
    "Idaho State":        ("ID", "Idaho_State",        "Big Sky"),
    "Montana":            ("MT", "Montana",            "Big Sky"),
    "Montana State":      ("MT", "Montana_State",      "Big Sky"),
    "Northern Arizona":   ("AZ", "Northern_Arizona",   "Big Sky"),
    "Northern Colorado":  ("CO", "Northern_Colorado",  "Big Sky"),
    "Portland State":     ("OR", "Portland_State",     "Big Sky"),
    "Sacramento State":   ("CA", "Sacramento_State",   "Big Sky"),
    "Southern Utah":      ("UT", "Southern_Utah",      "Big Sky"),
    "Weber State":        ("UT", "Weber_State",        "Big Sky"),

    # ── American Athletic ─────────────────────────────────────────────────────
    "East Carolina":      ("NC", "East_Carolina",      "American"),
    "Florida Atlantic":   ("FL", "Florida_Atlantic",   "American"),
    "Memphis":            ("TN", "Memphis",            "American"),
    "North Texas":        ("TX", "North_Texas",        "American"),
    "Rice":               ("TX", "Rice",               "American"),
    "South Florida":      ("FL", "South_Florida",      "American"),
    "Temple":             ("PA", "Temple",             "American"),
    "Tulane":             ("LA", "Tulane",             "American"),
    "Tulsa":              ("OK", "Tulsa",              "American"),
    "UAB":                ("AL", "UAB",                "American"),
    "UTSA":               ("TX", "UTSA",               "American"),
    "Wichita State":      ("KS", "Wichita_State",      "American"),

    # ── Atlantic 10 ───────────────────────────────────────────────────────────
    "Davidson":           ("NC", "Davidson",           "Atlantic 10"),
    "Dayton":             ("OH", "Dayton",             "Atlantic 10"),
    "Duquesne":           ("PA", "Duquesne",           "Atlantic 10"),
    "Fordham":            ("NY", "Fordham",            "Atlantic 10"),
    "George Mason":       ("VA", "George_Mason",       "Atlantic 10"),
    "George Washington":  ("DC", "George_Washington",  "Atlantic 10"),
    "UMass":              ("MA", "Massachusetts",      "Atlantic 10"),
    "Rhode Island":       ("RI", "Rhode_Island",       "Atlantic 10"),
    "Richmond":           ("VA", "Richmond",           "Atlantic 10"),
    "Saint Louis":        ("MO", "Saint_Louis",        "Atlantic 10"),
    "St. Bonaventure":    ("NY", "St_Bonaventure",     "Atlantic 10"),
    "VCU":                ("VA", "VCU",                "Atlantic 10"),

    # ── West Coast ────────────────────────────────────────────────────────────
    "Gonzaga":            ("WA", "Gonzaga",            "West Coast"),
    "Loyola Marymount":   ("CA", "Loyola_Marymount",   "West Coast"),
    "Pepperdine":         ("CA", "Pepperdine",         "West Coast"),
    "Portland":           ("OR", "Portland",           "West Coast"),
    "Saint Mary's":       ("CA", "Saint_Marys",        "West Coast"),
    "San Diego":          ("CA", "San_Diego",          "West Coast"),
    "San Francisco":      ("CA", "San_Francisco",      "West Coast"),
    "Santa Clara":        ("CA", "Santa_Clara",        "West Coast"),
}

# ── EVENT MAP ─────────────────────────────────────────────────────────────────
EVENT_MAP = {
    "60": "60m", "60m": "60m", "60 Meters": "60m",
    "60 Hurdles": "60mH", "60m Hurdles": "60mH",
    "100": "100m", "100m": "100m", "100 Meters": "100m",
    "200": "200m", "200m": "200m", "200 Meters": "200m",
    "400": "400m", "400m": "400m", "400 Meters": "400m",
    "800": "800m", "800m": "800m", "800 Meters": "800m",
    "1500": "1500m", "1500m": "1500m", "1,500": "1500m",
    "Mile": "Mile", "1 Mile": "Mile",
    "3000": "3000m", "3000m": "3000m", "3,000": "3000m",
    "3000 Steeplechase": "3000SC", "3000m Steeplechase": "3000SC",
    "5000": "5000m", "5000m": "5000m", "5,000": "5000m",
    "10,000": "10000m", "10000": "10000m", "10000m": "10000m",
    "110 Hurdles": "110mH", "110m Hurdles": "110mH",
    "100 Hurdles": "100mH", "100m Hurdles": "100mH",
    "400 Hurdles": "400mH", "400m Hurdles": "400mH",
    "High Jump": "HJ", "Long Jump": "LJ", "Triple Jump": "TJ",
    "Pole Vault": "PV", "Shot Put": "SP", "Discus": "DT",
    "Hammer": "HT", "Javelin": "JT", "Weight Throw": "WT",
    "Heptathlon": "Hept", "Decathlon": "Dec",
    "4x100": "4x100", "4 x 100": "4x100",
    "4x400": "4x400", "4 x 400": "4x400",
}

GENDERS = [("m", "Men"), ("f", "Women")]


# ── HTTP HELPER ───────────────────────────────────────────────────────────────
def get_page(url: str, retries: int = 3) -> BeautifulSoup | None:
    for attempt in range(retries):
        try:
            time.sleep(RATE_LIMIT_SECONDS)
            resp = requests.get(url, headers=HEADERS, timeout=20)
            if resp.status_code == 200:
                return BeautifulSoup(resp.text, "lxml")
            elif resp.status_code == 429:
                log.warning("Rate limited — waiting 60s")
                time.sleep(60)
            elif resp.status_code == 404:
                return None
            else:
                log.warning(f"HTTP {resp.status_code}: {url}")
        except Exception as e:
            log.error(f"Attempt {attempt+1} error: {e}")
            time.sleep(5)
    return None


# ── MARK PARSER ───────────────────────────────────────────────────────────────
def parse_mark(s: str) -> float | None:
    if not s:
        return None
    s = s.strip().lstrip("*").strip()
    try:
        if ":" in s:
            parts = s.split(":")
            if len(parts) == 2:
                return round(float(parts[0]) * 60 + float(parts[1]), 3)
        return float(s.replace(",", ""))
    except ValueError:
        return None


# ── ATHLETE SCRAPER ───────────────────────────────────────────────────────────
def scrape_athlete(info: dict) -> dict | None:
    url = info["url"]
    id_match = re.search(r"/athletes/(\d+)", url)
    if not id_match:
        return None
    tfrrs_id = id_match.group(1)

    soup = get_page(url)
    if not soup:
        return None

    # Name
    name = info["name"]
    for tag in soup.find_all(["h1", "h2", "h3"]):
        t = tag.get_text(strip=True)
        if len(t) > 3 and len(t) < 60 and not any(x in t.lower() for x in ["tfrrs", "track", "field"]):
            name = t.split("|")[0].strip()
            break

    # College & hometown
    college = info.get("college", "")
    hometown = ""
    hometown_state = ""

    # Try to find hometown in page metadata
    for text in soup.stripped_strings:
        m = re.match(r"^([A-Za-z\s\-]+),\s+([A-Z]{2})$", text.strip())
        if m and len(m.group(1)) > 2:
            hometown = text.strip()
            hometown_state = m.group(2)
            break

    # HS grad year
    hs_year = None
    for text in soup.find_all(string=re.compile(r"\b20(1[5-9]|2[0-6])\b")):
        m = re.search(r"\b(20(?:1[5-9]|2[0-6]))\b", str(text))
        if m:
            hs_year = int(m.group(1))
            break

    # Performances
    performances = []
    events_set = set()
    current_year = None
    current_season = "outdoor"

    for table in soup.find_all("table"):
        # Detect season from surrounding header
        prev = table.find_previous(["h3", "h4", "h5", "span", "div"])
        if prev:
            hdr = prev.get_text(" ", strip=True).lower()
            if "indoor" in hdr:
                current_season = "indoor"
            elif "cross country" in hdr or " xc" in hdr:
                current_season = "xc"
            else:
                current_season = "outdoor"

        for row in table.find_all("tr"):
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells:
                continue

            # Look for year in any cell
            for cell in cells:
                yr = re.search(r"\b(20(?:1[5-9]|2[0-6]))\b", cell)
                if yr:
                    current_year = int(yr.group(1))

            if len(cells) < 2:
                continue

            # Find event + mark pairs
            for i, cell in enumerate(cells):
                event_norm = EVENT_MAP.get(cell)
                if event_norm and i + 1 < len(cells):
                    mark_raw = cells[i + 1]
                    mark_num = parse_mark(mark_raw)
                    if mark_num and 0 < mark_num < 100000:
                        meet = cells[i + 2] if i + 2 < len(cells) else ""
                        performances.append({
                            "event": event_norm,
                            "mark": mark_num,
                            "mark_display": mark_raw,
                            "year": current_year,
                            "season": current_season,
                            "level": "college",
                            "meet_name": meet[:100],
                        })
                        events_set.add(event_norm)

    log.info(f"    {name} ({info.get('gender','')}) — {len(performances)} perfs")

    return {
        "id": f"tfrrs_{tfrrs_id}",
        "name": name,
        "source": "tfrrs",
        "source_id": tfrrs_id,
        "college": college,
        "conference": info.get("conference", ""),
        "hometown": hometown,
        "hometown_state": hometown_state,
        "hs_grad_year": hs_year,
        "gender": info.get("gender", "M"),
        "events": list(events_set),
        "tfrrs_url": url,
        "updated_at": datetime.utcnow().isoformat(),
        "performances": performances,
    }


# ── SUPABASE WRITER ───────────────────────────────────────────────────────────
def save_athlete(data: dict) -> bool:
    performances = data.pop("performances", [])
    try:
        supabase.table("athletes").upsert(data, on_conflict="id").execute()
        if performances:
            supabase.table("performances").delete().eq("athlete_id", data["id"]).execute()
            rows = [
                {**p, "athlete_id": data["id"], "source": "tfrrs"}
                for p in performances
                if p.get("mark") and p.get("event")
            ]
            for i in range(0, len(rows), 50):
                supabase.table("performances").insert(rows[i:i+50]).execute()
        log.info(f"    ✓ Saved: {data['name']}")
        return True
    except Exception as e:
        log.error(f"    ✗ Supabase error for {data.get('name')}: {e}")
        return False


def get_scraped_ids() -> set:
    try:
        result = supabase.table("athletes").select("source_id").eq("source", "tfrrs").execute()
        return {r["source_id"] for r in result.data}
    except Exception as e:
        log.error(f"Could not load existing IDs: {e}")
        return set()


# ── MAIN ──────────────────────────────────────────────────────────────────────
def run_scraper():
    log.info("=" * 60)
    log.info("Run Stats TFRRS Scraper v3")
    log.info("URL pattern: /teams/tf/{STATE}_college_{m/f}_{Slug}.html")
    log.info("=" * 60)

    already_done = get_scraped_ids()
    log.info(f"Already in DB: {len(already_done)} athletes — skipping these")

    saved = skipped = errors = found_teams = missing_teams = 0

    for school, (state, slug, conference) in TEAMS.items():
        if saved >= MAX_ATHLETES_PER_RUN:
            log.info(f"Reached MAX_ATHLETES_PER_RUN ({MAX_ATHLETES_PER_RUN}) — stopping")
            break

        for gender_code, gender_label in GENDERS:
            url = f"{BASE_URL}/teams/tf/{state}_college_{gender_code}_{slug}.html"
            log.info(f"Fetching: {school} {gender_label} → {url}")

            time.sleep(RATE_LIMIT_SECONDS)
            try:
                resp = requests.get(url, headers=HEADERS, timeout=20)
            except Exception as e:
                log.error(f"  Request failed: {e}")
                missing_teams += 1
                continue

            if resp.status_code == 404:
                log.warning(f"  404 — slug may differ for {school} {gender_label}")
                missing_teams += 1
                continue
            elif resp.status_code != 200:
                log.warning(f"  HTTP {resp.status_code} for {school} {gender_label}")
                missing_teams += 1
                continue

            found_teams += 1
            soup = BeautifulSoup(resp.text, "lxml")

            # Collect athlete links from roster
            athlete_links = []
            seen_urls = set()
            for link in soup.find_all("a", href=re.compile(r"/athletes/\d+")):
                href = link.get("href", "")
                name = link.get_text(strip=True)
                if not name or len(name) < 3:
                    continue
                full_url = href if href.startswith("http") else BASE_URL + href
                if full_url not in seen_urls:
                    seen_urls.add(full_url)
                    athlete_links.append({
                        "url": full_url,
                        "name": name,
                        "college": school,
                        "conference": conference,
                        "gender": "M" if gender_code == "m" else "F",
                    })

            log.info(f"  Found {len(athlete_links)} {gender_label} athletes at {school}")

            for info in athlete_links:
                if saved >= MAX_ATHLETES_PER_RUN:
                    break

                id_match = re.search(r"/athletes/(\d+)", info["url"])
                if id_match and id_match.group(1) in already_done:
                    skipped += 1
                    continue

                data = scrape_athlete(info)
                if data:
                    if save_athlete(data):
                        saved += 1
                        already_done.add(data["source_id"])
                    else:
                        errors += 1
                else:
                    errors += 1

    log.info("=" * 60)
    log.info(f"Teams found: {found_teams} | Teams 404'd: {missing_teams}")
    log.info(f"Athletes: {saved} saved | {skipped} skipped | {errors} errors")
    log.info("=" * 60)


if __name__ == "__main__":
    run_scraper()
