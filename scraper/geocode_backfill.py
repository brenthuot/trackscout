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

    # Split into already-resolved vs still-needing-coords
    resolved: set[str] = set()
    unresolved_athletes: list[dict] = []
    for a in all_athletes:
        ht = (a.get("hometown") or "").strip()
        if not ht or ", " not in ht:
            continue
        if a.get("hometown_lat") is not None:
            resolved.add(ht)
        else:
            unresolved_athletes.append(a)

    # Unique cities that need geocoding (skip cities already resolved on other athletes)
    cities_to_geocode = sorted(
        {a["hometown"].strip() for a in unresolved_athletes} - resolved
    )
    if args.limit:
        cities_to_geocode = cities_to_geocode[: args.limit]

    log.info(f"  {len(resolved)} cities already resolved")
    log.info(f"  {len(cities_to_geocode)} cities to geocode now")

    # ── Geocode ────────────────────────────────────────────────────────────────
    coords_map: dict[str, tuple[float, float]] = {}
    failed: list[str] = []

    for i, hometown in enumerate(cities_to_geocode, 1):
        city, state = hometown.rsplit(", ", 1)
        result = geocode(city, state)
        if result:
            coords_map[hometown] = result
            log.info(f"  [{i}/{len(cities_to_geocode)}] {hometown} → "
                     f"{result[0]:.4f}, {result[1]:.4f}")
        else:
            failed.append(hometown)
            log.warning(f"  [{i}/{len(cities_to_geocode)}] {hometown} → NOT FOUND")
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
