"""
Run Stats — Athletic.net Backfill Scraper v4 (Playwright)
Uses a real headless Chromium browser to search athletic.net and extract
fully JS-rendered profile data: hometown, high school, HS performances.

Usage:
    python athletic_net_scraper.py               # backfill all missing hometown
    python athletic_net_scraper.py --limit 5     # test with 5 athletes
    python athletic_net_scraper.py --group 1     # conference group 1-6
    python athletic_net_scraper.py --all         # re-process all athletes
"""

import os, re, logging, argparse, json, time
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from supabase import create_client, Client

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── SUPABASE ──────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── CONSTANTS ─────────────────────────────────────────────────────────────────
RATE_LIMIT = 3.0   # seconds between page navigations

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
            return round((float(m.group(1)) * 12 + float(m.group(2))) * 0.0254, 3)
        cleaned = re.sub(r'[^0-9.]', '', s)
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


# ── PLAYWRIGHT SEARCH ─────────────────────────────────────────────────────────
def search_athlete(page, name: str, hs_grad_year: int | None) -> list[str]:
    """Search athletic.net for athlete, return list of profile URLs."""
    from urllib.parse import quote
    # Properly encode the full query including quotes
    query = f'"{name}"'
    if hs_grad_year:
        query += f' {hs_grad_year}'
    encoded_query = quote(query)  # encodes quotes as %22, spaces as %20
    search_url = f"https://www.athletic.net/search#?q={encoded_query}&sport=tf"

    try:
        log.info(f"  [Search] {name} → {search_url}")
        page.goto(search_url, wait_until="domcontentloaded", timeout=30000)

        # Wait for Angular to process the hash and render athlete links
        try:
            page.wait_for_selector("a[href*='/athlete/']", timeout=12000)
        except PlaywrightTimeout:
            # Take a screenshot to see what's on the page
            try:
                page.screenshot(path=f"/tmp/search_debug_{name.replace(' ','_')}.png")
                log.info(f"  [Search] Screenshot saved. Page title: {page.title()!r}")
                log.info(f"  [Search] Page URL: {page.url!r}")
                # Log first 500 chars of visible text
                body = page.inner_text("body")[:500]
                log.info(f"  [Search] Body text: {body!r}")
            except Exception:
                pass
            log.info(f"  [Search] No athlete links appeared after 12s — no results")
            return []

        time.sleep(1)

        links = page.eval_on_selector_all(
            "a[href*='/athlete/']",
            "els => els.map(e => e.href)"
        )

        urls = []
        seen_ids = set()
        for href in links:
            m = re.search(r'athletic\.net/athlete/(\d+)', href)
            if m and m.group(1) not in seen_ids:
                seen_ids.add(m.group(1))
                urls.append(f"https://www.athletic.net/athlete/{m.group(1)}/track-and-field/")

        log.info(f"  [Search] Found {len(urls)} profile URLs: {urls[:3]}")
        return urls

    except PlaywrightTimeout:
        log.warning(f"  [Search] Timeout for {name}")
        return []
    except Exception as e:
        log.error(f"  [Search] Error for {name}: {e}")
        return []


# ── PLAYWRIGHT PROFILE SCRAPER ────────────────────────────────────────────────
def scrape_profile(page, url: str, expected_name: str) -> dict | None:
    """Load a fully rendered athletic.net profile page and extract all data."""
    try:
        log.info(f"  [Profile] Loading {url}")
        page.goto(url, wait_until="domcontentloaded", timeout=30000)

        # Wait for Angular to render the athlete name/bio
        try:
            page.wait_for_selector("h1, h2, title", timeout=10000)
        except PlaywrightTimeout:
            pass
        time.sleep(2)  # extra buffer for full render

        # ── Verify name ──────────────────────────────────────────────────────
        title = page.title()
        log.info(f"    Title: {title!r}")
        expected_parts = expected_name.lower().split()
        if len(expected_parts) >= 2:
            if not all(p in title.lower() for p in expected_parts[:2]):
                log.info(f"    Name mismatch: '{expected_name}' not in title '{title}'")
                return None

        # ── Extract hometown & high school from rendered DOM ─────────────────
        hometown = ""
        hometown_state = ""
        high_school = ""

        # State from title: "Name - ST Track and Field Bio"
        title_state = re.search(r'-\s+([A-Z]{2})\s+Track and Field Bio', title)
        if title_state and title_state.group(1) in US_STATES:
            hometown_state = title_state.group(1)

        # Try to get full rendered text and look for structured data
        try:
            # athletic.net shows hometown as "City, ST" in the bio section
            body_text = page.inner_text("body")

            # Look for "City, ST" pattern in rendered text
            m = re.search(
                r'\b([\w\s\.\'\-]{2,30}),\s+(' + '|'.join(US_STATES) + r')\b',
                body_text
            )
            if m:
                candidate = m.group(0).strip()
                # Filter out noise like "Track and Field, AL"
                if not any(kw in candidate.lower() for kw in ["track", "field", "cross", "sport", "event"]):
                    hometown = candidate
                    hometown_state = hometown_state or m.group(2)

            # Look for high school name
            hs_patterns = [
                r'(?:High School|HS)[:\s]+([A-Z][A-Za-z\s\.\-\']{3,50})',
                r'([A-Z][A-Za-z\s\.\-\']{3,40}(?:High School|High|Academy|Prep))',
                r'School[:\s]+([A-Z][A-Za-z\s\.\-\']{3,50})',
            ]
            for pat in hs_patterns:
                m2 = re.search(pat, body_text)
                if m2:
                    candidate_hs = m2.group(1).strip()[:80]
                    if len(candidate_hs) > 3:
                        high_school = candidate_hs
                        break

        except Exception as e:
            log.debug(f"    Body text extraction error: {e}")

        # Fallback: check page JSON state (Angular sometimes embeds data)
        if not hometown:
            try:
                json_data = page.evaluate("""() => {
                    // Try common Angular/Next.js state patterns
                    const sources = [
                        window.__INITIAL_STATE__,
                        window.__STATE__,
                        window.__PRELOADED_STATE__,
                    ];
                    for (const s of sources) {
                        if (s) return JSON.stringify(s);
                    }
                    return null;
                }""")
                if json_data:
                    data = json.loads(json_data)
                    data_str = json.dumps(data)
                    for key in ["hometown", "city", "location"]:
                        m = re.search(f'"{key}"\\s*:\\s*"([^"]+)"', data_str)
                        if m:
                            hometown = m.group(1)
                            break
                    for key in ["highSchool", "school", "team"]:
                        m = re.search(f'"{key}"\\s*:\\s*"([^"]+)"', data_str)
                        if m and not high_school:
                            high_school = m.group(1)[:80]
                            break
            except Exception:
                pass

        # ── Extract HS performances from rendered results table ───────────────
        performances = []
        try:
            rows = page.query_selector_all("tr")
            current_year = None
            for row in rows:
                cells = [td.inner_text().strip() for td in row.query_selector_all("td, th")]
                if not cells:
                    continue
                for cell in cells:
                    yr = re.search(r'\b(20[01]\d|202[0-6])\b', cell)
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
        except Exception as e:
            log.debug(f"    Performance extraction error: {e}")

        log.info(f"    hometown={hometown!r}, state={hometown_state!r}, HS={high_school!r}, {len(performances)} perfs")
        return {
            "hometown": hometown,
            "hometown_state": hometown_state,
            "high_school": high_school,
            "performances": performances,
            "anet_url": url,
        }

    except PlaywrightTimeout:
        log.warning(f"  [Profile] Timeout: {url}")
        return None
    except Exception as e:
        log.error(f"  [Profile] Error {url}: {e}")
        return None


# ── FIND BEST MATCH ───────────────────────────────────────────────────────────
def find_best_match(page, name: str, hs_grad_year: int | None) -> dict | None:
    urls = search_athlete(page, name, hs_grad_year)
    if not urls:
        return None
    for url in urls[:3]:
        time.sleep(RATE_LIMIT)
        profile = scrape_profile(page, url, name)
        if profile:
            return profile
    return None


# ── SUPABASE SAVE ─────────────────────────────────────────────────────────────
def backfill_athlete(page, athlete_row: dict) -> bool:
    athlete_id = athlete_row["id"]
    name = athlete_row["name"]
    hs_grad_year = athlete_row.get("hs_grad_year")

    log.info(f"  Searching: {name} (HS {hs_grad_year or '?'})")

    time.sleep(RATE_LIMIT)
    profile = find_best_match(page, name, hs_grad_year)
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
    log.info(f"Athletic.net Backfill v4 (Playwright) — Group: {group}, Limit: {limit}")
    log.info("=" * 60)

    try:
        result = supabase.table("athletes").select(
            "id, name, college, conference, hs_grad_year, college_year, hometown"
        ).eq("source", "tfrrs").execute()
        all_athletes = result.data or []
    except Exception as e:
        log.error(f"Failed to fetch athletes: {e}")
        return

    if group != "all" and group in CONFERENCE_GROUPS:
        target_confs = set(CONFERENCE_GROUPS[group])
        log.info(f"Group {group} conferences: {target_confs}")
        athletes = [a for a in all_athletes if a.get("conference") in target_confs]
    else:
        athletes = all_athletes

    to_process = athletes if process_all else [a for a in athletes if not a.get("hometown")]
    log.info(f"Athletes to process: {len(to_process)} / {len(athletes)}")

    saved = errors = 0

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ]
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            locale="en-US",
        )
        # Hide webdriver flag
        context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        page = context.new_page()

        # Warm up: visit homepage first so we have cookies/session
        log.info("  Warming up browser on athletic.net...")
        try:
            page.goto("https://www.athletic.net", wait_until="domcontentloaded", timeout=20000)
            time.sleep(2)
            page.screenshot(path="/tmp/search_debug_warmup.png")
            log.info(f"  Warmup page title: {page.title()!r}")
        except Exception as e:
            log.warning(f"  Warmup failed: {e}")

        for i, athlete in enumerate(to_process[:limit]):
            if i > 0 and i % 25 == 0:
                log.info(f"Progress: {i}/{min(len(to_process), limit)} — {saved} saved, {errors} errors")
            if backfill_athlete(page, athlete):
                saved += 1
            else:
                errors += 1

        browser.close()

    log.info("=" * 60)
    log.info(f"Done: {saved} updated, {errors} not found/errors")
    log.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Athletic.net backfill scraper v4 (Playwright)")
    parser.add_argument("--group", default="all", help="Conference group 1-6 or 'all'")
    parser.add_argument("--limit", type=int, default=99999, help="Max athletes to process")
    parser.add_argument("--all", action="store_true", dest="process_all",
                        help="Re-process all athletes, not just those missing hometown")
    args = parser.parse_args()
    run(group=args.group, limit=args.limit, process_all=args.process_all)
