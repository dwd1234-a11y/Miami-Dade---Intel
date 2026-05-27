"""
Deals With Dignity — Miami-Dade County Motivated Seller Lead Scraper
Fetches distressed property records from the Miami-Dade Clerk portal
and enriches them with Property Appraiser data.

Run: python scraper/fetch.py
"""

import asyncio
import json
import csv
import io
import os
import re
import time
import logging
import traceback
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ─────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", 7))
CLERK_BASE = "https://www.miamidadeclerk.gov"
CLERK_RECORDS_URL = f"{CLERK_BASE}/clerk/records.page"
PA_SEARCH_URL = "https://apps.miamidadepa.gov/PropertySearch/api/Search"
PA_PROPERTY_URL = "https://apps.miamidadepa.gov/PropertySearch/api/Property"
OUTPUT_PATHS = [
    Path("dashboard/records.json"),
    Path("data/records.json"),
]
GHL_CSV_PATH = Path("data/ghl_export.csv")
MAX_RETRIES = 3
RETRY_DELAY = 3  # seconds
HEADLESS = os.environ.get("HEADLESS", "true").lower() != "false"

# Doc type → category mapping
DOC_TYPE_MAP = {
    "LP":        ("foreclosure",   "Lis Pendens"),
    "NOFC":      ("foreclosure",   "Notice of Foreclosure"),
    "TAXDEED":   ("tax",           "Tax Deed"),
    "JUD":       ("judgment",      "Judgment"),
    "CCJ":       ("judgment",      "Certified Judgment"),
    "DRJUD":     ("judgment",      "Domestic Relations Judgment"),
    "LNCORPTX":  ("lien",         "Corporate Tax Lien"),
    "LNIRS":     ("lien",         "IRS Lien"),
    "LNFED":     ("lien",         "Federal Lien"),
    "LN":        ("lien",         "Lien"),
    "LNMECH":    ("lien",         "Mechanic's Lien"),
    "LNHOA":     ("lien",         "HOA Lien"),
    "MEDLN":     ("lien",         "Medicaid Lien"),
    "PRO":       ("probate",      "Probate"),
    "NOC":       ("construction", "Notice of Commencement"),
    "RELLP":     ("release",      "Release of Lis Pendens"),
}

# All target doc types to search
TARGET_DOC_TYPES = list(DOC_TYPE_MAP.keys())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("dwd_scraper")


# ─────────────────────────────────────────────────────────
# SCORING ENGINE
# ─────────────────────────────────────────────────────────

def compute_flags(record: dict) -> list[str]:
    flags = []
    doc_type = record.get("doc_type", "")
    cat = record.get("cat", "")
    owner = (record.get("owner") or "").upper()
    amount = record.get("amount") or 0
    filed_str = record.get("filed") or ""

    if doc_type in ("LP",):
        flags.append("Lis pendens")
    if doc_type in ("LP", "NOFC"):
        flags.append("Pre-foreclosure")
    if cat == "judgment":
        flags.append("Judgment lien")
    if doc_type in ("LNIRS", "LNFED", "LNCORPTX", "TAXDEED"):
        flags.append("Tax lien")
    if doc_type == "LNMECH":
        flags.append("Mechanic lien")
    if cat == "probate":
        flags.append("Probate / estate")
    if any(kw in owner for kw in ("LLC", "CORP", "INC", "LTD", "TRUST")):
        flags.append("LLC / corp owner")

    # New this week
    if filed_str:
        try:
            filed_date = datetime.strptime(filed_str, "%Y-%m-%d").date()
            if (date.today() - filed_date).days <= 7:
                flags.append("New this week")
        except Exception:
            pass

    return list(dict.fromkeys(flags))  # deduplicate, preserve order


def compute_score(record: dict, flags: list[str]) -> int:
    score = 30  # base
    score += len(flags) * 10

    # LP + foreclosure combo bonus
    doc_type = record.get("doc_type", "")
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20

    amount = record.get("amount") or 0
    if amount > 100_000:
        score += 15
    elif amount > 50_000:
        score += 10

    if "New this week" in flags:
        score += 5

    has_address = bool(record.get("prop_address") or record.get("mail_address"))
    if has_address:
        score += 5

    return min(score, 100)


# ─────────────────────────────────────────────────────────
# PROPERTY APPRAISER ENRICHMENT
# ─────────────────────────────────────────────────────────

class PropertyAppraiser:
    """
    Queries the Miami-Dade PA public API to enrich records with
    property address and mailing address by owner name lookup.
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.miamidadepa.gov/pa/real-estate/property-search.page",
        })
        self._cache: dict[str, Optional[dict]] = {}

    def _name_variants(self, full_name: str) -> list[str]:
        """Generate multiple name format variants for fuzzy matching."""
        variants = [full_name]
        parts = full_name.split()
        if len(parts) >= 2:
            # LAST FIRST
            variants.append(f"{parts[-1]} {' '.join(parts[:-1])}")
            # LAST, FIRST
            variants.append(f"{parts[-1]}, {' '.join(parts[:-1])}")
        return variants

    def _search_by_name(self, name: str) -> Optional[dict]:
        """Hit the PA property search API with owner name."""
        if name in self._cache:
            return self._cache[name]

        for variant in self._name_variants(name):
            try:
                resp = self.session.get(
                    PA_SEARCH_URL,
                    params={"q": variant, "s": "ownername", "p": 1, "size": 5},
                    timeout=15,
                )
                if resp.status_code != 200:
                    continue
                data = resp.json()
                # PA search returns {"MinimumResults": [...]}
                results = (
                    data.get("MinimumResults")
                    or data.get("Results")
                    or data.get("items")
                    or []
                )
                if results:
                    hit = results[0]
                    enriched = self._parse_pa_result(hit)
                    self._cache[name] = enriched
                    return enriched
            except Exception as e:
                log.debug(f"PA name lookup failed for '{variant}': {e}")

        self._cache[name] = None
        return None

    def _get_property_detail(self, folio: str) -> Optional[dict]:
        """Fetch full detail for a folio number."""
        try:
            resp = self.session.get(
                PA_PROPERTY_URL,
                params={"f": folio},
                timeout=15,
            )
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            log.debug(f"PA folio detail failed for '{folio}': {e}")
        return None

    def _parse_pa_result(self, hit: dict) -> dict:
        """Normalize PA API response to a flat address dict."""
        # Try different field name conventions the PA API may return
        site_addr = (
            hit.get("SiteAddress") or hit.get("SITEADDR") or
            hit.get("SiteAddr") or hit.get("site_addr") or ""
        ).strip()
        site_city = (
            hit.get("SiteCity") or hit.get("SITE_CITY") or
            hit.get("City") or ""
        ).strip()
        site_zip = (
            hit.get("SiteZip") or hit.get("SITE_ZIP") or
            hit.get("Zip") or ""
        ).strip()
        mail_addr = (
            hit.get("MailingAddress1") or hit.get("MAILADR1") or
            hit.get("MailAddr1") or hit.get("mail_addr") or ""
        ).strip()
        mail_city = (
            hit.get("MailingCity") or hit.get("MAILCITY") or
            hit.get("MailCity") or ""
        ).strip()
        mail_state = (
            hit.get("MailingState") or hit.get("STATE") or
            hit.get("MailState") or "FL"
        ).strip()
        mail_zip = (
            hit.get("MailingZip") or hit.get("MAILZIP") or
            hit.get("MailZip") or ""
        ).strip()

        return {
            "prop_address": site_addr,
            "prop_city":    site_city,
            "prop_state":   "FL",
            "prop_zip":     site_zip,
            "mail_address": mail_addr,
            "mail_city":    mail_city,
            "mail_state":   mail_state,
            "mail_zip":     mail_zip,
        }

    def enrich(self, record: dict) -> dict:
        """Add address data to a record dict in-place. Never raises."""
        # Blank out address fields first
        addr_fields = [
            "prop_address", "prop_city", "prop_state", "prop_zip",
            "mail_address", "mail_city", "mail_state", "mail_zip",
        ]
        for f in addr_fields:
            record.setdefault(f, "")

        owner = (record.get("owner") or "").strip()
        if not owner:
            return record

        pa_data = None
        try:
            pa_data = self._search_by_name(owner)
        except Exception as e:
            log.debug(f"PA enrichment failed for '{owner}': {e}")

        if pa_data:
            for k, v in pa_data.items():
                if v:  # only overwrite if we got a value
                    record[k] = v

        return record


# ─────────────────────────────────────────────────────────
# CLERK PORTAL SCRAPER (Playwright)
# ─────────────────────────────────────────────────────────

class ClerkScraper:
    """
    Scrapes Miami-Dade Clerk's official records portal using Playwright.
    The portal uses ASP.NET WebForms with __doPostBack calls.
    """

    SEARCH_URL = CLERK_RECORDS_URL
    RESULTS_PER_PAGE = 25

    def __init__(self, page):
        self.page = page
        self.base_url = CLERK_BASE

    async def _safe_goto(self, url: str, **kwargs):
        for attempt in range(MAX_RETRIES):
            try:
                await self.page.goto(url, wait_until="networkidle", timeout=60_000, **kwargs)
                return
            except PlaywrightTimeout:
                log.warning(f"Timeout on goto {url}, attempt {attempt + 1}/{MAX_RETRIES}")
                if attempt == MAX_RETRIES - 1:
                    raise
                await asyncio.sleep(RETRY_DELAY)

    async def _wait_and_fill(self, selector: str, value: str):
        await self.page.wait_for_selector(selector, timeout=20_000)
        await self.page.fill(selector, value)

    async def _click_and_wait(self, selector: str):
        await self.page.click(selector)
        await self.page.wait_for_load_state("networkidle", timeout=45_000)

    async def search_doc_type(
        self,
        doc_type: str,
        date_from: str,
        date_to: str,
    ) -> list[dict]:
        """
        Search the clerk portal for a specific document type in the date range.
        Returns list of record dicts.
        """
        records = []
        log.info(f"Searching clerk for doc_type={doc_type} ({date_from} to {date_to})")

        try:
            await self._safe_goto(self.SEARCH_URL)

            # The portal has a records search form. We'll look for the
            # Document Type dropdown and date range fields.
            # The exact selectors depend on the portal's rendered HTML;
            # we try multiple strategies.

            # Strategy 1: Standard form fields approach
            try:
                await self._fill_search_form(doc_type, date_from, date_to)
                records = await self._extract_results(doc_type)
            except Exception as e:
                log.warning(f"Strategy 1 failed for {doc_type}: {e}")
                # Strategy 2: Try URL-based search pattern
                try:
                    records = await self._url_search(doc_type, date_from, date_to)
                except Exception as e2:
                    log.warning(f"Strategy 2 failed for {doc_type}: {e2}")

        except Exception as e:
            log.error(f"Clerk search error for {doc_type}: {e}")
            log.debug(traceback.format_exc())

        log.info(f"  Got {len(records)} records for {doc_type}")
        return records

    async def _fill_search_form(self, doc_type: str, date_from: str, date_to: str):
        """Fill the official records search form."""
        # Wait for the page to stabilize
        await self.page.wait_for_load_state("networkidle", timeout=30_000)

        # Try to find the document type select
        doc_type_selectors = [
            "select[name*='DocType']",
            "select[name*='docType']",
            "select[name*='document']",
            "#DocType",
            "#ddlDocType",
            "select[id*='DocType']",
        ]

        doc_type_selector = None
        for sel in doc_type_selectors:
            if await self.page.locator(sel).count() > 0:
                doc_type_selector = sel
                break

        if doc_type_selector:
            await self.page.select_option(doc_type_selector, value=doc_type)
        else:
            # Try typing doc type in a text field
            await self.page.fill("input[name*='DocType'], input[id*='DocType']", doc_type)

        # Fill date from
        date_from_selectors = [
            "input[name*='DateFrom']", "input[name*='StartDate']",
            "input[id*='DateFrom']", "input[id*='StartDate']",
            "#DateFrom", "#StartDate",
        ]
        for sel in date_from_selectors:
            if await self.page.locator(sel).count() > 0:
                await self.page.fill(sel, date_from)
                break

        # Fill date to
        date_to_selectors = [
            "input[name*='DateTo']", "input[name*='EndDate']",
            "input[id*='DateTo']", "input[id*='EndDate']",
            "#DateTo", "#EndDate",
        ]
        for sel in date_to_selectors:
            if await self.page.locator(sel).count() > 0:
                await self.page.fill(sel, date_to)
                break

        # Submit the search
        submit_selectors = [
            "input[type='submit']",
            "button[type='submit']",
            "input[value*='Search']",
            "button:has-text('Search')",
        ]
        for sel in submit_selectors:
            if await self.page.locator(sel).count() > 0:
                await self._click_and_wait(sel)
                break

    async def _url_search(self, doc_type: str, date_from: str, date_to: str) -> list[dict]:
        """
        Attempt direct URL parameter search (some clerk portals support this).
        """
        # Miami-Dade clerk uses a POST-based search but may have GET params
        search_url = (
            f"{self.SEARCH_URL}?"
            f"docType={doc_type}&dateFrom={date_from}&dateTo={date_to}"
        )
        await self._safe_goto(search_url)
        await self.page.wait_for_load_state("networkidle", timeout=30_000)
        return await self._extract_results(doc_type)

    async def _extract_results(self, doc_type: str) -> list[dict]:
        """Parse the results table from the current page, paginating through all pages."""
        all_records = []
        page_num = 1

        while True:
            html = await self.page.content()
            soup = BeautifulSoup(html, "lxml")

            page_records = self._parse_results_table(soup, doc_type)
            all_records.extend(page_records)

            log.debug(f"  Page {page_num}: {len(page_records)} records")

            # Check for next page
            next_page = await self._go_next_page(soup)
            if not next_page or not page_records:
                break

            page_num += 1
            await self.page.wait_for_load_state("networkidle", timeout=30_000)
            await asyncio.sleep(1)

        return all_records

    def _parse_results_table(self, soup: BeautifulSoup, doc_type: str) -> list[dict]:
        """Extract records from the HTML results table."""
        records = []

        # Look for results table
        tables = soup.find_all("table")
        result_table = None

        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if any(kw in " ".join(headers) for kw in
                   ["document", "grantor", "grantee", "filed", "book"]):
                result_table = table
                break

        if not result_table:
            # Try finding by common result div patterns
            rows_div = soup.find("div", class_=re.compile(r"result|record", re.I))
            if rows_div:
                result_table = rows_div.find("table")

        if not result_table:
            return records

        rows = result_table.find_all("tr")
        if not rows:
            return records

        # Parse header row to map column positions
        header_row = rows[0]
        headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])]

        col_map = {}
        for i, h in enumerate(headers):
            if "doc" in h and "num" in h:
                col_map["doc_num"] = i
            elif "type" in h:
                col_map["doc_type_col"] = i
            elif "filed" in h or "record" in h or "date" in h:
                col_map["filed"] = i
            elif "grantor" in h or "owner" in h:
                col_map["grantor"] = i
            elif "grantee" in h:
                col_map["grantee"] = i
            elif "legal" in h:
                col_map["legal"] = i
            elif "amount" in h or "consid" in h:
                col_map["amount"] = i

        for row in rows[1:]:
            try:
                cells = row.find_all(["td", "th"])
                if not cells:
                    continue

                def cell_text(key: str) -> str:
                    idx = col_map.get(key)
                    if idx is not None and idx < len(cells):
                        return cells[idx].get_text(strip=True)
                    return ""

                doc_num = cell_text("doc_num")
                if not doc_num:
                    # Try to find doc number from any anchor link
                    link = row.find("a")
                    if link:
                        doc_num = link.get_text(strip=True)

                # Build clerk direct URL
                link_tag = row.find("a", href=True)
                clerk_url = ""
                if link_tag:
                    href = link_tag["href"]
                    clerk_url = href if href.startswith("http") else f"{self.base_url}{href}"

                # Parse amount - strip non-numeric except dot
                amount_str = cell_text("amount").replace("$", "").replace(",", "").strip()
                try:
                    amount = float(amount_str) if amount_str else 0.0
                except ValueError:
                    amount = 0.0

                # Normalize filed date
                filed_raw = cell_text("filed").strip()
                filed = self._normalize_date(filed_raw)

                record = {
                    "doc_num":  doc_num.strip(),
                    "doc_type": doc_type,
                    "filed":    filed,
                    "owner":    cell_text("grantor").strip(),
                    "grantee":  cell_text("grantee").strip(),
                    "legal":    cell_text("legal").strip(),
                    "amount":   amount,
                    "clerk_url": clerk_url,
                    # Address fields filled later by PA enrichment
                    "prop_address": "",
                    "prop_city":    "",
                    "prop_state":   "FL",
                    "prop_zip":     "",
                    "mail_address": "",
                    "mail_city":    "",
                    "mail_state":   "FL",
                    "mail_zip":     "",
                }

                cat, cat_label = DOC_TYPE_MAP.get(doc_type, ("other", doc_type))
                record["cat"] = cat
                record["cat_label"] = cat_label

                if doc_num:  # skip empty rows
                    records.append(record)

            except Exception as e:
                log.debug(f"Row parse error: {e}")
                continue

        return records

    def _normalize_date(self, raw: str) -> str:
        """Try to parse various date formats to YYYY-MM-DD."""
        if not raw:
            return ""
        formats = [
            "%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d",
            "%m/%d/%y", "%B %d, %Y", "%b %d, %Y",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return raw  # Return as-is if all fail

    async def _go_next_page(self, soup: BeautifulSoup) -> bool:
        """Click next page if available, return True if navigated."""
        next_selectors = [
            "a:has-text('Next')",
            "a[title*='Next']",
            "input[value*='Next']",
            "[aria-label*='next']",
            "a.next",
        ]
        for sel in next_selectors:
            if await self.page.locator(sel).count() > 0:
                await self._click_and_wait(sel)
                return True

        # Try __doPostBack for ASP.NET paging
        page_links = soup.find_all("a", href=re.compile(r"__doPostBack"))
        for link in page_links:
            text = link.get_text(strip=True)
            if text == ">" or text.lower() == "next":
                event_target = re.search(r"__doPostBack\('([^']+)'", link["href"])
                if event_target:
                    await self.page.evaluate(
                        f"__doPostBack('{event_target.group(1)}', '')"
                    )
                    await self.page.wait_for_load_state("networkidle", timeout=30_000)
                    return True

        return False


# ─────────────────────────────────────────────────────────
# GHL CSV EXPORT
# ─────────────────────────────────────────────────────────

GHL_COLUMNS = [
    "First Name", "Last Name", "Mailing Address", "Mailing City",
    "Mailing State", "Mailing Zip", "Property Address", "Property City",
    "Property State", "Property Zip", "Lead Type", "Document Type",
    "Date Filed", "Document Number", "Amount/Debt Owed",
    "Seller Score", "Motivated Seller Flags", "Source", "Public Records URL",
]


def split_name(full_name: str) -> tuple[str, str]:
    """Split 'LAST, FIRST' or 'FIRST LAST' into (first, last)."""
    if not full_name:
        return "", ""
    if "," in full_name:
        parts = [p.strip() for p in full_name.split(",", 1)]
        return parts[1].title(), parts[0].title()
    parts = full_name.split()
    if len(parts) == 1:
        return "", parts[0].title()
    return " ".join(parts[:-1]).title(), parts[-1].title()


def records_to_ghl_csv(records: list[dict]) -> str:
    """Convert records to GHL-importable CSV string."""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=GHL_COLUMNS)
    writer.writeheader()

    for r in records:
        first, last = split_name(r.get("owner", ""))
        _, cat_label = DOC_TYPE_MAP.get(r.get("doc_type", ""), ("other", r.get("doc_type", "")))
        writer.writerow({
            "First Name":           first,
            "Last Name":            last,
            "Mailing Address":      r.get("mail_address", ""),
            "Mailing City":         r.get("mail_city", ""),
            "Mailing State":        r.get("mail_state", "FL"),
            "Mailing Zip":          r.get("mail_zip", ""),
            "Property Address":     r.get("prop_address", ""),
            "Property City":        r.get("prop_city", ""),
            "Property State":       r.get("prop_state", "FL"),
            "Property Zip":         r.get("prop_zip", ""),
            "Lead Type":            r.get("cat_label", ""),
            "Document Type":        r.get("doc_type", ""),
            "Date Filed":           r.get("filed", ""),
            "Document Number":      r.get("doc_num", ""),
            "Amount/Debt Owed":     r.get("amount", ""),
            "Seller Score":         r.get("score", 0),
            "Motivated Seller Flags": " | ".join(r.get("flags", [])),
            "Source":               "Miami-Dade Clerk Public Records",
            "Public Records URL":   r.get("clerk_url", ""),
        })

    return output.getvalue()


# ─────────────────────────────────────────────────────────
# OUTPUT / PERSISTENCE
# ─────────────────────────────────────────────────────────

def build_output(records: list[dict], date_from: str, date_to: str) -> dict:
    """Build the final JSON output structure."""
    with_address = sum(
        1 for r in records
        if r.get("prop_address") or r.get("mail_address")
    )
    return {
        "fetched_at":    datetime.utcnow().isoformat() + "Z",
        "source":        "Miami-Dade Clerk of Courts Public Records",
        "date_range":    {"from": date_from, "to": date_to},
        "total":         len(records),
        "with_address":  with_address,
        "records":       records,
    }


def save_outputs(output: dict, records: list[dict]):
    """Write records.json to all output paths and GHL CSV."""
    payload = json.dumps(output, indent=2, default=str)

    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload, encoding="utf-8")
        log.info(f"Saved {output['total']} records to {path}")

    # GHL CSV
    GHL_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    ghl_csv = records_to_ghl_csv(records)
    GHL_CSV_PATH.write_text(ghl_csv, encoding="utf-8")
    log.info(f"Saved GHL export to {GHL_CSV_PATH}")


# ─────────────────────────────────────────────────────────
# DEDUPLICATION
# ─────────────────────────────────────────────────────────

def deduplicate(records: list[dict]) -> list[dict]:
    """Remove duplicate doc_num entries, keeping highest scored."""
    seen: dict[str, dict] = {}
    for r in records:
        key = r.get("doc_num", "")
        if not key:
            continue
        if key not in seen or r.get("score", 0) > seen[key].get("score", 0):
            seen[key] = r
    return list(seen.values())


# ─────────────────────────────────────────────────────────
# MAIN ORCHESTRATOR
# ─────────────────────────────────────────────────────────

async def run():
    today = date.today()
    date_from_dt = today - timedelta(days=LOOKBACK_DAYS)
    date_from = date_from_dt.strftime("%m/%d/%Y")
    date_to   = today.strftime("%m/%d/%Y")
    date_from_iso = date_from_dt.strftime("%Y-%m-%d")
    date_to_iso   = today.strftime("%Y-%m-%d")

    log.info("=" * 60)
    log.info("DWD Motivated Seller Scraper — Miami-Dade County")
    log.info(f"Date range: {date_from} to {date_to}")
    log.info(f"Doc types: {', '.join(TARGET_DOC_TYPES)}")
    log.info("=" * 60)

    pa = PropertyAppraiser()
    all_records: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        page = await context.new_page()
        scraper = ClerkScraper(page)

        for doc_type in TARGET_DOC_TYPES:
            try:
                records = await scraper.search_doc_type(doc_type, date_from, date_to)
                for r in records:
                    all_records.append(r)
            except Exception as e:
                log.error(f"Failed to scrape {doc_type}: {e}")
                log.debug(traceback.format_exc())
            # Polite delay between searches
            await asyncio.sleep(2)

        await browser.close()

    log.info(f"Raw records collected: {len(all_records)}")

    # Enrich with PA data
    log.info("Enriching with Property Appraiser data...")
    for i, record in enumerate(all_records):
        try:
            pa.enrich(record)
        except Exception as e:
            log.debug(f"PA enrichment error on record {i}: {e}")
        if (i + 1) % 50 == 0:
            log.info(f"  Enriched {i + 1}/{len(all_records)}...")

    # Compute flags + scores
    for record in all_records:
        try:
            flags = compute_flags(record)
            score = compute_score(record, flags)
            record["flags"] = flags
            record["score"] = score
        except Exception as e:
            log.debug(f"Scoring error: {e}")
            record["flags"] = []
            record["score"] = 30

    # Deduplicate
    all_records = deduplicate(all_records)

    # Sort: highest score first, then by filed date desc
    all_records.sort(
        key=lambda r: (-(r.get("score") or 0), r.get("filed") or ""),
        reverse=False,
    )

    log.info(f"Final record count after dedup: {len(all_records)}")

    # Build and save output
    output = build_output(all_records, date_from_iso, date_to_iso)
    save_outputs(output, all_records)

    # Summary stats
    by_type = {}
    for r in all_records:
        t = r.get("doc_type", "?")
        by_type[t] = by_type.get(t, 0) + 1

    log.info("\n--- SUMMARY ---")
    for t, count in sorted(by_type.items(), key=lambda x: -x[1]):
        log.info(f"  {t}: {count}")
    log.info(f"  Total: {len(all_records)} records")
    log.info(f"  With address: {output['with_address']}")
    avg_score = (
        sum(r.get("score", 0) for r in all_records) / len(all_records)
        if all_records else 0
    )
    log.info(f"  Avg seller score: {avg_score:.1f}")
    log.info("Done.")


if __name__ == "__main__":
    asyncio.run(run())
