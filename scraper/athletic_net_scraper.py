"""
Run Stats — Athletic.net Backfill Scraper v2
Searches athletic.net for each TFRRS athlete (by name + hs_grad_year),
extracts hometown, high school, and HS performances,
then upserts back into Supabase.

Usage:
    python athletic_net_scraper.py               # backfill all missing hometown
    python athletic_net_scraper.py --limit 5     # test with 5 athletes
    python athletic_net_scraper.py --group 1     # conference group 1-6
    python athletic_net_scraper.py --all         # re-process all, not just missing
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
RATE_LIMIT = 2.5

HEADERS = {
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

ALGOLIA_APP_ID = None
ALGOLIA_API_KEY = None

def discover_algolia_keys():
    """Fetch athletic.net scripts and extract Algolia app ID + search key."""
    global ALGOLIA_APP_ID, ALGOLIA_API_KEY
    try:
        time.sleep(RATE_LIMIT)
        r = requests.get("https://www.athletic.net", headers=HEADERS, timeout=20)
        log.info(f"  [Algolia] Homepage status: {r.status_code}")
        if r.status_code != 200:
            return

        soup = BeautifulSoup(r.text, "lxml")

        # Collect all script src URLs
        script_urls = []
        for script in soup.find_all("script", src=True):
            src = script["src"]
            url = src if src.startswith("http") else f"https://www.athletic.net{src}"
            script_urls.append(url)

        log.info(f"  [Algolia] Found {len(script_urls)} script tags: {script_urls[:5]}")

        # Also search inline scripts
        inline_js = " ".join(s.get_text() for s in soup.find_all("script", src=False))
        for js in [inline_js] + [None] * len(script_urls):  # inline first
            if js is None:
                if not script_urls:
                    break
                url = script_urls.pop(0)
                try:
                    time.sleep(1)
                    jr = requests.get(url, headers=HEADERS, timeout=20)
                    js = jr.text if jr.status_code == 200 else ""
                    log.info(f"  [Algolia] Fetched {url[:70]}: {jr.status_code}, {len(js)} chars")
                except Exception as e:
                    log.warning(f"  [Algolia] Failed to fetch {url}: {e}")
                    continue

            # Look for Algolia app ID (10 uppercase alphanumeric) and API key (32 hex chars)
            aid = re.search(r'appId["\s:=]+["\']([A-Z0-9]{10})["\']', js)
            akey = re.search(r'(?:apiKey|searchKey|SEARCH_KEY)["\s:=]+["\']([a-f0-9]{32})["\']', js, re.I)
            if aid and akey:
                ALGOLIA_APP_ID = aid.group(1)
                ALGOLIA_API_KEY = akey.group(1)
                log.info(f"  [Algolia] ✓ Found keys! appId={ALGOLIA_APP_ID}")
                return

        log.info("  [Algolia] Keys not found in any script. Will rely on HTML fallback.")
    except Exception as e:
        log.error(f"  [Algolia] Discovery failed: {e}")



US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC"
}


# ── HTTP HELPERS ──────────────────────────────────────────────────────────────
def get_page(url: str, params=None, retries=3, as_json=False):
    for attempt in range(retries):
        try:
            time.sleep(RATE_LIMIT)
            r = requests.get(url, headers=HEADERS, params=params, timeout=20)
            log.info(f"  [HTTP] {r.status_code} {url[:80]}")
            if r.status_code == 200:
                if as_json:
                    try:
                        return r.json()
                    except Exception:
                        log.info(f"  [HTTP] 200 but not JSON. Body[:200]: {r.text[:200]}")
                        return None
                return BeautifulSoup(r.text, "lxml")
            elif r.status_code == 429:
                log.warning("Rate limited — sleeping 60s")
                time.sleep(60)
            elif r.status_code == 404:
                return None
            else:
                log.warning(f"  [HTTP] {r.status_code} body[:200]: {r.text[:200]}")
        except Exception as e:
            log.error(f"Attempt {attempt+1}: {e}")
            time.sleep(5)
    return None


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


# ── ATHLETIC.NET SEARCH ───────────────────────────────────────────────────────
def search_athlete(name: str, hs_grad_year: int | None) -> list[dict]:
    """
    Search athletic.net for an athlete by name + grad year.
    Tries JSON API first, falls back to HTML search page.
    """
    candidates = []

    # Try Algolia if we have keys
    if ALGOLIA_APP_ID and ALGOLIA_API_KEY:
        algolia_url = f"https://{ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/athletes/query"
        algolia_headers = {**HEADERS,
            "X-Algolia-Application-Id": ALGOLIA_APP_ID,
            "X-Algolia-API-Key": ALGOLIA_API_KEY,
        }
        try:
            time.sleep(RATE_LIMIT)
            r = requests.post(algolia_url, json={"query": name, "hitsPerPage": 10}, headers=algolia_headers, timeout=20)
            log.info(f"  [Algolia] Query status: {r.status_code}, body[:200]: {r.text[:200]}")
            if r.status_code == 200:
                hits = r.json().get("hits", [])
                for h in hits:
                    aid = str(h.get("AthleteID") or h.get("objectID") or "")
                    if not aid:
                        continue
                    grad = h.get("GraduationYear") or h.get("gradYear")
                    if hs_grad_year and grad:
                        try:
                            if abs(int(grad) - int(hs_grad_year)) > 1:
                                continue
                        except (ValueError, TypeError):
                            pass
                    candidates.append({
                        "id": aid,
                        "name": h.get("Name") or h.get("name") or "",
                        "school": h.get("SchoolName") or "",
                        "grad_year": grad,
                        "url": f"https://www.athletic.net/athlete/{aid}/track-and-field/",
                        "url_old": f"https://www.athletic.net/TrackAndField/Athlete.aspx?AID={aid}",
                    })
                if candidates:
                    return candidates[:5]
        except Exception as e:
            log.error(f"  [Algolia] Search failed: {e}")

    # Try JSON search endpoints
    for endpoint in [
        "https://www.athletic.net/api/v1/AthleticAPI/Search",
        "https://www.athletic.net/api/v1/Search",
    ]:
        data = get_page(endpoint, params={"q": name, "type": "athlete"}, as_json=True)
        log.info(f"  [DEBUG] {endpoint} → {str(data)[:300] if data else 'None/empty'}")
        if not data:
            continue
        results = (
            data.get("athletes") or data.get("Athletes") or
            data.get("results") or data.get("Results") or []
        )
        if isinstance(results, dict):
            results = results.get("hits") or results.get("items") or []
        for r in results:
            aid = str(
                r.get("AthleteID") or r.get("athleteId") or
                r.get("id") or r.get("ID") or ""
            )
            if not aid:
                continue
            grad = r.get("GraduationYear") or r.get("gradYear") or r.get("GradYear")
            if hs_grad_year and grad:
                try:
                    if abs(int(grad) - int(hs_grad_year)) > 1:
                        continue
                except (ValueError, TypeError):
                    pass
            candidates.append({
                "id": aid,
                "name": r.get("Name") or r.get("name") or "",
                "school": r.get("SchoolName") or r.get("school") or "",
                "grad_year": grad,
                "url": f"https://www.athletic.net/athlete/{aid}/track-and-field/",
                "url_old": f"https://www.athletic.net/TrackAndField/Athlete.aspx?AID={aid}",
            })
        if candidates:
            log.debug(f"  JSON API: {len(candidates)} candidates")
            return candidates[:5]

    # HTML search fallback
    soup = get_page("https://www.athletic.net/Search.aspx", params={"q": name, "itype": "athlete"})
    if not soup:
        log.info("  [DEBUG] HTML search returned nothing")
        return []
    log.info(f"  [DEBUG] HTML search page title: {soup.title.string if soup.title else 'no title'}")
    all_links = soup.find_all("a", href=re.compile(r"(?:AID=|/athlete/)\d+"))
    log.info(f"  [DEBUG] Found {len(all_links)} athlete links in HTML")

    for link in soup.find_all("a", href=re.compile(r"(?:AID=|/athlete/)\d+")):
        href = link.get("href", "")
        aid_match = re.search(r"(?:AID=|/athlete/)(\d+)", href)
        if not aid_match:
            continue
        aid = aid_match.group(1)
        aname = link.get_text(strip=True)
        if not aname or len(aname) < 2:
            continue
        row = link.find_parent(["tr", "li", "div"])
        row_text = row.get_text(" ", strip=True) if row else ""
        grad_match = re.search(r"\b(20[12]\d)\b", row_text)
        grad = int(grad_match.group(1)) if grad_match else None
        if hs_grad_year and grad:
            try:
                if abs(grad - hs_grad_year) > 1:
                    continue
            except (ValueError, TypeError):
                pass
        candidates.append({
            "id": aid,
            "name": aname,
            "school": "",
            "grad_year": grad,
            "url": f"https://www.athletic.net/athlete/{aid}/track-and-field/",
            "url_old": f"https://www.athletic.net/TrackAndField/Athlete.aspx?AID={aid}",
        })

    return candidates[:5]


# ── ATHLETE PROFILE SCRAPER ───────────────────────────────────────────────────
def scrape_profile(candidate: dict, expected_name: str, hs_grad_year: int | None) -> dict | None:
    """
    Scrape athletic.net athlete profile. Tries new URL format then old.
    Returns profile dict or None if name doesn't match.
    """
    soup = None
    used_url = None

    for url in [candidate["url"], candidate.get("url_old", "")]:
        if not url:
            continue
        soup = get_page(url)
        if soup:
            used_url = url
            break

    if not soup:
        return None

    page_text = soup.get_text(" ", strip=True)

    # Verify name match
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

    # Extract hometown
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

    # Extract high school
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

    # Extract HS performances — try JSON API first
    performances = []
    aid = candidate["id"]
    results_data = None
    for jurl in [
        f"https://www.athletic.net/api/v1/AthleticAPI/AthleteResults?athleteId={aid}&sport=tf",
        f"https://www.athletic.net/api/v1/athlete/{aid}/results?sport=tf",
    ]:
        results_data = get_page(jurl, as_json=True)
        if results_data:
            break

    if results_data:
        results = results_data.get("results") or results_data.get("Results") or []
        for res in results:
            event_raw = str(res.get("EventName") or res.get("event") or "")
            if any(kw in event_raw.lower() for kw in RELAY_KEYWORDS):
                continue
            event_norm = ANET_EVENT_MAP.get(event_raw)
            if not event_norm:
                continue
            mark_raw = str(res.get("Result") or res.get("mark") or "")
            mark_num = parse_mark(mark_raw)
            if not mark_num or mark_num <= 0:
                continue
            year = res.get("Year") or res.get("year")
            meet = str(res.get("MeetName") or res.get("meet") or "")
            performances.append({
                "event": event_norm,
                "mark": mark_num,
                "mark_display": mark_raw,
                "year": int(year) if year else None,
                "season": "outdoor",
                "level": "hs",
                "meet_name": meet[:100],
            })
    else:
        # HTML table fallback
        current_year = None
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if not cells:
                    continue
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

    return {
        "hometown": hometown,
        "hometown_state": hometown_state,
        "high_school": high_school,
        "performances": performances,
        "anet_url": used_url,
    }


# ── BEST CANDIDATE SELECTOR ───────────────────────────────────────────────────
def find_best_match(name: str, hs_grad_year: int | None) -> dict | None:
    candidates = search_athlete(name, hs_grad_year)
    if not candidates:
        log.debug(f"  No candidates for '{name}'")
        return None
    for cand in candidates[:3]:
        profile = scrape_profile(cand, name, hs_grad_year)
        if profile:
            return profile
    return None


# ── SUPABASE BACKFILL ─────────────────────────────────────────────────────────
def backfill_athlete(athlete_row: dict) -> bool:
    athlete_id = athlete_row["id"]
    name = athlete_row["name"]
    hs_grad_year = athlete_row.get("hs_grad_year")

    log.info(f"  Searching: {name} (HS {hs_grad_year or '?'})")

    profile = find_best_match(name, hs_grad_year)
    if not profile:
        log.info(f"    ✗ No match: {name}")
        return False

    log.info(
        f"    ✓ Found: hometown={profile['hometown']!r}, "
        f"HS={profile['high_school']!r}, "
        f"{len(profile['performances'])} HS perfs"
    )

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
    log.info(f"Athletic.net Backfill v2 — Group: {group}, Limit: {limit}, All: {process_all}")
    log.info("=" * 60)

    # Discover Algolia credentials
    log.info("Discovering Algolia search keys...")
    discover_algolia_keys()

    # Load athletes from Supabase
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

    # Only process missing hometown unless --all flag
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
    parser = argparse.ArgumentParser(description="Athletic.net backfill scraper v2")
    parser.add_argument("--group", default="all", help="Conference group 1-6 or 'all'")
    parser.add_argument("--limit", type=int, default=99999, help="Max athletes to process")
    parser.add_argument("--all", action="store_true", dest="process_all",
                        help="Re-process all athletes, not just those missing hometown")
    args = parser.parse_args()
    run(group=args.group, limit=args.limit, process_all=args.process_all)
