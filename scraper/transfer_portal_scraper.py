"""
transfer_portal_scraper.py
──────────────────────────
Detects NCAA track & field transfers by inspecting each athlete's TFRRS page
for multi-school performance history, then updates Supabase.

Strategy:
  TFRRS athlete profile pages list performances with section headers that
  include the school name. If an athlete has performed under >1 school, they
  transferred. We extract:
    - is_transfer (bool)
    - transfer_from (text)  — the previous school name
    - transfer_year (int)   — year the transfer took effect

Also attempts to scrape The Stride Report transfer list for distance runners
(HTML table, no JS required) for a secondary signal.

Usage:
  python scraper/transfer_portal_scraper.py
  python scraper/transfer_portal_scraper.py --dry-run
  python scraper/transfer_portal_scraper.py --limit 500
"""

import os
import re
import sys
import time
import argparse
import logging
from datetime import datetime

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

REQUEST_DELAY = 1.2  # seconds between TFRRS requests — be polite


# ── TFRRS multi-school detection ──────────────────────────────────────────────

def fetch_tfrrs_page(url: str) -> BeautifulSoup | None:
    """Fetch a TFRRS athlete page and return parsed HTML."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except requests.RequestException as e:
        log.warning(f"  TFRRS fetch failed ({url}): {e}")
        return None


def extract_schools_from_tfrrs(soup: BeautifulSoup) -> list[str]:
    """
    Parse a TFRRS athlete profile for school names.

    TFRRS structures performance tables under headings like:
      <h3 class="panel-title">2023-24 — University of Oregon</h3>
    or inline school badges in the athlete header.

    Returns a deduplicated ordered list of schools (most recent first).
    """
    schools = []

    # Method 1: season panel headings contain "YEAR — School Name"
    for heading in soup.select("h3.panel-title, h4.panel-title, .panel-heading h3, .panel-heading h4"):
        text = heading.get_text(strip=True)
        # Pattern: "2023-24 — University of Oregon" or "2022 Outdoor — Oregon"
        m = re.search(r"\d{4}.*?[–—-]\s*(.+)$", text)
        if m:
            school = m.group(1).strip()
            # Filter out noise (season names, event names)
            if len(school) > 3 and not re.search(r"\b(indoor|outdoor|cross|country|championship)\b", school, re.I):
                if school not in schools:
                    schools.append(school)

    # Method 2: athlete profile header often has school affiliation
    for el in soup.select(".athlete-name + .school, .athlete-school, [class*='school']"):
        text = el.get_text(strip=True)
        if text and len(text) > 3 and text not in schools:
            schools.append(text)

    # Method 3: breadcrumb / team links
    for link in soup.select("a[href*='/teams/']"):
        text = link.get_text(strip=True)
        if text and len(text) > 3 and text not in schools:
            schools.append(text)

    return schools


def detect_transfer_from_tfrrs(url: str, current_college: str) -> dict | None:
    """
    Returns dict with transfer info if athlete transferred, else None.
    {
        "is_transfer": True,
        "transfer_from": "Previous School Name",
        "transfer_year": 2023  # optional
    }
    """
    soup = fetch_tfrrs_page(url)
    if not soup:
        return None

    schools = extract_schools_from_tfrrs(soup)
    log.debug(f"  Schools found on TFRRS: {schools}")

    if len(schools) < 2:
        return None

    # The current college may appear under various abbreviations
    # Use fuzzy matching: check if current_college words appear in any school name
    def matches_current(s: str) -> bool:
        s_lower = s.lower()
        cc_lower = current_college.lower()
        # Direct substring or word overlap
        if cc_lower in s_lower or s_lower in cc_lower:
            return True
        # Word overlap (≥2 words in common)
        sw = set(s_lower.split())
        cw = set(cc_lower.split())
        return len(sw & cw) >= 2

    non_current = [s for s in schools if not matches_current(s)]
    if not non_current:
        return None

    # Oldest non-current school = transfer_from
    transfer_from = non_current[-1]

    # Try to infer transfer year from when this school last appears
    transfer_year = None
    for heading in soup.select("h3.panel-title, h4.panel-title"):
        text = heading.get_text(strip=True)
        if transfer_from.lower().split()[0] in text.lower():
            m = re.search(r"(\d{4})", text)
            if m:
                transfer_year = int(m.group(1))

    return {
        "is_transfer": True,
        "transfer_from": transfer_from,
        "transfer_year": transfer_year,
    }


# ── The Stride Report scraper (distance runners) ──────────────────────────────

STRIDE_REPORT_URL = "https://www.thestridereport.com/transfers"

def scrape_stride_report() -> list[dict]:
    """
    Scrape The Stride Report transfer list.
    Returns list of {name, from_school, to_school, event} dicts.
    Note: This site may require JS. Falls back gracefully if unavailable.
    """
    try:
        r = requests.get(STRIDE_REPORT_URL, headers=HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        transfers = []

        # Try to find table rows
        for row in soup.select("table tr, .transfer-row, [class*='transfer']"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            texts = [c.get_text(strip=True) for c in cells]
            # Expect: Name | From | To | Event (order varies)
            if texts[0] and len(texts[0]) > 3:
                entry = {"name": texts[0]}
                if len(texts) > 1:
                    entry["from_school"] = texts[1]
                if len(texts) > 2:
                    entry["to_school"] = texts[2]
                if len(texts) > 3:
                    entry["event"] = texts[3]
                transfers.append(entry)

        log.info(f"Stride Report: found {len(transfers)} transfer entries")
        return transfers

    except Exception as e:
        log.warning(f"Stride Report scrape failed (JS-rendered or blocked): {e}")
        return []


# ── Supabase ──────────────────────────────────────────────────────────────────

def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_athletes_with_tfrrs_urls(supabase: Client, limit: int = None) -> list[dict]:
    """Fetch athletes that have TFRRS URLs and haven't been checked yet."""
    query = (
        supabase.table("athletes")
        .select("id, name, college, tfrrs_url, is_transfer")
        .eq("source", "tfrrs")
        .not_.is_("tfrrs_url", "null")
        .neq("tfrrs_url", "")
    )
    # Only re-check athletes not yet flagged (to avoid re-scraping known transfers)
    # Remove this filter to do a full re-check
    query = query.is_("is_transfer", "null")

    if limit:
        query = query.limit(limit)

    result = query.execute()
    return result.data or []


def update_athlete_transfer(supabase: Client, athlete_id: int, transfer_info: dict, dry_run: bool):
    """Upsert transfer fields on athlete row."""
    payload = {
        "is_transfer": transfer_info["is_transfer"],
        "transfer_from": transfer_info.get("transfer_from"),
        "transfer_year": transfer_info.get("transfer_year"),
    }

    if dry_run:
        log.info(f"  [DRY RUN] Would update athlete {athlete_id}: {payload}")
        return

    supabase.table("athletes").update(payload).eq("id", athlete_id).execute()


def mark_not_transfer(supabase: Client, athlete_id: int, dry_run: bool):
    """Mark athlete as confirmed non-transfer so we don't re-check."""
    if dry_run:
        return
    supabase.table("athletes").update({"is_transfer": False}).eq("id", athlete_id).execute()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="TrackScout transfer portal scraper")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to Supabase")
    parser.add_argument("--limit", type=int, default=None, help="Max athletes to process")
    parser.add_argument("--stride-only", action="store_true", help="Only scrape Stride Report")
    args = parser.parse_args()

    supabase = get_supabase()

    # ── Step 1: Stride Report (quick secondary signal) ────────────────────────
    stride_transfers = scrape_stride_report()
    stride_names = {t["name"].lower() for t in stride_transfers}

    # ── Step 2: TFRRS multi-school detection ──────────────────────────────────
    if args.stride_only:
        log.info("--stride-only: skipping TFRRS detection")
        return

    athletes = fetch_athletes_with_tfrrs_urls(supabase, limit=args.limit)
    log.info(f"Processing {len(athletes)} athletes with unchecked TFRRS pages...")

    found_transfers = 0
    errors = 0

    for i, athlete in enumerate(athletes, 1):
        name = athlete["name"]
        college = athlete["college"] or ""
        url = athlete["tfrrs_url"]

        log.info(f"[{i}/{len(athletes)}] {name} ({college})")

        # Quick check: is this name in Stride Report transfers?
        stride_match = name.lower() in stride_names

        transfer_info = detect_transfer_from_tfrrs(url, college)

        if transfer_info:
            log.info(f"  ✓ TRANSFER detected: {transfer_info['transfer_from']} → {college}")
            update_athlete_transfer(supabase, athlete["id"], transfer_info, args.dry_run)
            found_transfers += 1
        elif stride_match:
            # Stride Report says transfer but TFRRS didn't confirm — flag anyway
            stride_entry = next((t for t in stride_transfers if t["name"].lower() == name.lower()), {})
            transfer_info = {
                "is_transfer": True,
                "transfer_from": stride_entry.get("from_school"),
                "transfer_year": None,
            }
            log.info(f"  ✓ TRANSFER (Stride Report only): {transfer_info['transfer_from']} → {college}")
            update_athlete_transfer(supabase, athlete["id"], transfer_info, args.dry_run)
            found_transfers += 1
        else:
            mark_not_transfer(supabase, athlete["id"], args.dry_run)

        time.sleep(REQUEST_DELAY)

    log.info(f"\n{'='*50}")
    log.info(f"Done. {found_transfers} transfers found out of {len(athletes)} athletes checked.")
    log.info(f"Errors: {errors}")


if __name__ == "__main__":
    main()
