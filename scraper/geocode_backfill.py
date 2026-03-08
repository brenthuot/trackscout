"""
geocode_backfill.py
────────────────────────────────────────────────────────────────────────────────
Fetches every unique hometown that still has null coords, geocodes via the free
OpenStreetMap Nominatim API, and writes hometown_lat / hometown_lng back to
Supabase.  Supabase itself acts as the resume cache — already-resolved cities
are skipped automatically, so the workflow is safe to re-run at any time.
"""

import argparse
import logging
import os
import re
import time

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

NOMINATIM_DELAY   = 1.1
NOMINATIM_URL     = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {"User-Agent": "TrackScout/1.0 (educational track-stats project)"}

STATE_NAMES = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California",
    "CO":"Colorado","CT":"Connecticut","DE":"Delaware","FL":"Florida","GA":"Georgia",
    "HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa",
    "KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland",
    "MA":"Massachusetts","MI":"Michigan","MN":"Minnesota","MS":"Mississippi",
    "MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New Hampshire",
    "NJ":"New Jersey","NM":"New Mexico","NY":"New York","NC":"North Carolina",
    "ND":"North Dakota","OH":"Ohio","OK":"Oklahoma","OR":"Oregon","PA":"Pennsylvania",
    "RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota","TN":"Tennessee",
    "TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington",
    "WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming","DC":"Washington DC",
}

# ── Hometown cleaning ──────────────────────────────────────────────────────────

# Known typos: raw value → corrected city string
_TYPO_CORRECTIONS = {
    "Ankney, IA":               "Ankeny, IA",
    "Burlingtown Township, NJ": "Burlington Township, NJ",
    "Charoltte, NC":            "Charlotte, NC",
    "Claredon Hills, IL":       "Clarendon Hills, IL",
    "Couer D'Alene, ID":        "Coeur d'Alene, ID",
    "Couer d'Alene, ID":        "Coeur d'Alene, ID",
    "Cuningham, NC":            "Cunningham, NC",
    "East Manchester, CT":      "Manchester, CT",
    "Exter, NH":                "Exeter, NH",
    "Fort Lauderale, FL":       "Fort Lauderdale, FL",
    "Frankort, IL":             "Frankfort, IL",
    "Golden Brdge, NY":         "Golden Bridge, NY",
    "Harleyville, PA":          "Harleysville, PA",
    "Hillboro, OR":             "Hillsboro, OR",
    "Kalamzoo, MI":             "Kalamazoo, MI",
    "Laguna Nigel, CA":         "Laguna Niguel, CA",
    "Lake Owsego, OR":          "Lake Oswego, OR",
    "Lambertsville, NJ":        "Lambertville, NJ",
    "Milwakee, WI":             "Milwaukee, WI",
    "Nine Miles Falls, WA":     "Nine Mile Falls, WA",
    "Riatlo, CA":               "Rialto, CA",
    "South Winsor, CT":         "South Windsor, CT",
    "Spencerport, PA":          "Spencerport, NY",
    "Timnath, OR":              "Timnath, CO",
    "Warenton, VA":             "Warrenton, VA",
    "Warner Robbins, GA":       "Warner Robins, GA",
}

# Academic / program / class-year words that are NOT part of a US city name.
# Used to detect and strip leading garbage from athlete hometown strings.
_ACADEMIC_TERMS = {
    "administration", "bioengineering", "baylor", "collegiate",
    "developmental", "development", "disorders", "ecosystem", "education",
    "elementary", "engineering", "english", "entrepreneurship", "exercise",
    "experience", "first-year", "fresh", "freshman", "health", "healthcare",
    "hpe", "human", "interdisciplinary", "management", "marketing", "mba",
    "mechanical", "medicine", "politics", "practice", "pre-nursing",
    "psychology", "relations", "science", "secondary", "social",
    "studies", "studies-public", "undeclared", "work",
}


def clean_hometown(raw: str) -> str:
    """
    Normalise a raw hometown string before passing it to Nominatim.

    Handles two classes of bad data found in athlete records:

    1. Typos — direct lookup in _TYPO_CORRECTIONS.
       e.g. "Charoltte, NC"  →  "Charlotte, NC"

    2. Academic / major / class-year prefix prepended to the real city.
       e.g. "Exercise Science Arlington, WA"  →  "Arlington, WA"
            "Bioengineering Ellicott City, MD" →  "Ellicott City, MD"
            "First-Year Chicago, IL"           →  "Chicago, IL"
            "Baylor\\nBearsCollegiate\\nWaco, TX" →  "Waco, TX"

    Returns the original string unchanged if neither pattern is detected,
    so well-formed values like "Seattle, WA" pass through without modification.
    """
    if not raw or not isinstance(raw, str):
        return raw

    value = raw.strip()

    # Step 1: direct typo fix
    if value in _TYPO_CORRECTIONS:
        return _TYPO_CORRECTIONS[value]

    # Step 2: flatten embedded newlines
    # e.g. "Baylor\nBearsCollegiate\nWaco, TX" → "Baylor BearsCollegiate Waco, TX"
    flat = " ".join(value.splitlines()).strip()

    # Step 3: confirm string ends with ", ST"
    m = re.search(r',\s*([A-Z]{2})\s*$', flat)
    if not m:
        return value  # no recognisable state code — leave as-is

    state = m.group(1)
    words = flat[:m.start()].strip().split()

    # Step 4: for 3+ words before the comma, scan for the last academic term
    # and strip everything up to and including it.
    # Substring match (not just exact) catches run-together tokens like
    # "BearsCollegiate" which contains "collegiate".
    if len(words) >= 3:
        last_bad = -1
        for i, word in enumerate(words):
            if any(term in word.lower() for term in _ACADEMIC_TERMS):
                last_bad = i
        if 0 <= last_bad < len(words) - 1:
            return f"{' '.join(words[last_bad + 1:])}, {state}"

    # Step 5: for 2-word strings, use exact match on the first word to
    # reduce false-positive risk on legitimate 2-word city names.
    if len(words) >= 2:
        first = words[0].lower()
        if first in _ACADEMIC_TERMS or any(term in first for term in _ACADEMIC_TERMS):
            return f"{' '.join(words[1:])}, {state}"

    return value


# ── Geocoding ──────────────────────────────────────────────────────────────────

def geocode(city: str, state_abbr: str) -> tuple[float, float] | None:
    state_full = STATE_NAMES.get(state_abbr, state_abbr)
    try:
        r = requests.get(
            NOMINATIM_URL,
            params={"q": f"{city}, {state_full}, USA", "format": "json",
                    "limit": 1, "countrycodes": "us"},
            headers=NOMINATIM_HEADERS,
            timeout=10,
        )
        r.raise_for_status()
        hits = r.json()
        if hits:
            return float(hits[0]["lat"]), float(hits[0]["lon"])
    except Exception as e:
        log.warning(f"  Nominatim error for {city}, {state_abbr}: {e}")
    return None


def fetch_all_athletes(supabase) -> list[dict]:
    rows, page_size, offset = [], 1000, 0
    while True:
        batch = (
            supabase.table("athletes")
            .select("id, hometown, hometown_lat")
            .not_.is_("hometown", "null")
            .range(offset, offset + page_size - 1)
            .execute()
            .data
        )
        if not batch:
            break
        rows.extend(batch)
        offset += page_size
        if len(batch) < page_size:
            break
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0,
                        help="Only geocode this many unique cities (0 = all)")
    args = parser.parse_args()

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    log.info("Fetching athletes from Supabase...")
    all_athletes = fetch_all_athletes(supabase)
    log.info(f"  {len(all_athletes)} athletes with hometowns")

    # Split into already-resolved vs still-needing-coords.
    # Use the *cleaned* hometown as the dedup key so that e.g.
    # "Exercise Science Arlington, WA" and "Arlington, WA" share a cache entry.
    resolved: set[str] = set()
    unresolved_athletes: list[dict] = []
    for a in all_athletes:
        ht = (a.get("hometown") or "").strip()
        if not ht:
            continue
        cleaned = clean_hometown(ht)
        if ", " not in cleaned:
            continue
        if a.get("hometown_lat") is not None:
            resolved.add(cleaned)
        else:
            unresolved_athletes.append(a)

    # Map raw → cleaned so the DB-update loop can key on the raw value.
    raw_to_clean: dict[str, str] = {
        a["hometown"].strip(): clean_hometown(a["hometown"].strip())
        for a in unresolved_athletes
    }

    # Unique cities that still need geocoding
    cities_to_geocode = sorted(
        {raw for raw, cleaned in raw_to_clean.items()
         if cleaned not in resolved}
    )
    if args.limit:
        cities_to_geocode = cities_to_geocode[: args.limit]

    log.info(f"  {len(resolved)} cities already resolved")
    log.info(f"  {len(cities_to_geocode)} cities to geocode now")

    # ── Geocode ────────────────────────────────────────────────────────────────
    coords_map: dict[str, tuple[float, float]] = {}   # keyed by RAW hometown
    failed: list[str] = []

    for i, raw_hometown in enumerate(cities_to_geocode, 1):
        cleaned = raw_to_clean[raw_hometown]
        if cleaned != raw_hometown:
            log.info(f"  [{i}/{len(cities_to_geocode)}] Cleaned: '{raw_hometown}' → '{cleaned}'")

        city, state = cleaned.rsplit(", ", 1)
        result = geocode(city, state)
        if result:
            coords_map[raw_hometown] = result
            log.info(f"  [{i}/{len(cities_to_geocode)}] {cleaned} → "
                     f"{result[0]:.4f}, {result[1]:.4f}")
        else:
            failed.append(raw_hometown)
            log.warning(f"  [{i}/{len(cities_to_geocode)}] {cleaned} → NOT FOUND")
        time.sleep(NOMINATIM_DELAY)

    # ── Write coords to DB ─────────────────────────────────────────────────────
    log.info("Writing coordinates to Supabase...")
    updated = skipped = 0

    for athlete in unresolved_athletes:
        ht = athlete["hometown"].strip()
        coords = coords_map.get(ht)
        if not coords:
            skipped += 1
            continue
        if args.dry_run:
            skipped += 1
            continue
        try:
            supabase.table("athletes").update({
                "hometown_lat": coords[0],
                "hometown_lng": coords[1],
            }).eq("id", athlete["id"]).execute()
            updated += 1
        except Exception as e:
            log.error(f"  DB error athlete {athlete['id']}: {e}")
        if updated % 200 == 0 and updated:
            log.info(f"  ... {updated} athletes updated")

    log.info("─" * 60)
    log.info("SUMMARY")
    log.info(f"  Athletes with hometowns      : {len(all_athletes)}")
    log.info(f"  Cities already resolved      : {len(resolved)}")
    log.info(f"  Cities geocoded this run     : {len(coords_map)}")
    log.info(f"  Cities not found (Nominatim) : {len(failed)}")
    log.info(f"  Athletes updated in DB       : {updated}")
    if args.dry_run:
        log.info("  DRY RUN — no DB writes made")
    if failed:
        log.warning(f"\nNot found ({len(failed)}):")
        for c in failed[:30]:
            log.warning(f"  {c}")


if __name__ == "__main__":
    main()
