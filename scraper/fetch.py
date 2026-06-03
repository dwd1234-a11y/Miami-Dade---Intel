"""
Deals With Dignity — Miami-Dade County Motivated Seller Lead Scraper
Sources:
  1. RealForeclose.com  — public foreclosure auction calendar (no login)
  2. Miami-Dade Property Appraiser API — address enrichment (public)

Run: python scraper/fetch.py
"""

import asyncio
import json
import csv
import io
import os
import re
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
HEADLESS = os.environ.get("HEADLESS", "true").lower() != "false"

# RealForeclose — Miami-Dade public foreclosure auction portal
REALFORECLOSE_BASE = "https://www.miamidade.realforeclose.com"
REALFORECLOSE_CALENDAR = f"{REALFORECLOSE_BASE}/index.cfm?zaction=user&zmethod=calendar"
REALFORECLOSE_SEARCH = (
    f"{REALFORECLOSE_BASE}/index.cfm"
    "?zaction=AUCTION&Zmethod=PREVIEW&TYPEID=1&bypassPage=1"
)

# Miami-Dade Property Appraiser — public REST API
PA_SEARCH_URL = "https://www.miamidadepa.gov/PApublicServiceProxy/PaServicesProxy.ashx"

OUTPUT_PATHS = [
    Path("dashboard/records.json"),
    Path("data/records.json"),
]
GHL_CSV_PATH = Path("data/ghl_export.csv")

MAX_RETRIES = 3
RETRY_DELAY = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("dwd_miami")


# ─────────────────────────────────────────────────────────
# SCORING ENGINE
# ─────────────────────────────────────────────────────────

def compute_flags(record: dict) -> list[str]:
    flags = []
    cat = record.get("cat", "")
    owner = (record.get("owner") or "").upper()
    filed_str = record.get("filed") or ""
    sale_date_str = record.get("frcl_sale_date") or ""

    if cat == "foreclosure":
        flags.append("Pre-foreclosure")
        flags.append("Lis pendens")
    if cat == "tax":
        flags.append("Tax lien")
    if cat == "judgment":
        flags.append("Judgment lien")
    if any(kw in owner for kw in ("LLC", "CORP", "INC", "LTD", "TRUST")):
        flags.append("LLC / corp owner")

    # Upcoming sale within 30 days
    if sale_date_str:
        try:
            sale_date = datetime.strptime(sale_date_str, "%Y-%m-%d").date()
            days_until = (sale_date - date.today()).days
            if 0 <= days_until <= 30:
                flags.append("Sale in 30 days")
            elif 0 <= days_until <= 7:
                flags.append("Sale this week")
        except Exception:
            pass

    if filed_str:
        try:
            filed_date = datetime.strptime(filed_str, "%Y-%m-%d").date()
            if (date.today() - filed_date).days <= 7:
                flags.append("New this week")
        except Exception:
            pass

    return list(dict.fromkeys(flags))


def compute_score(record: dict, flags: list[str]) -> int:
    score = 30
    cat = record.get("cat", "")

    if cat == "foreclosure":
        score += 25
    elif cat == "tax":
        score += 20
    elif cat == "judgment":
        score += 15

    score += len(flags) * 10

    amount = record.get("amount") or 0
    if amount >= 100_000:
        score += 15
    elif amount >= 50_000:
        score += 10

    if record.get("prop_address"):
        score += 5

    if "Sale this week" in flags:
        score += 15
    elif "Sale in 30 days" in flags:
        score += 10

    return min(score, 100)


# ─────────────────────────────────────────────────────────
# PROPERTY APPRAISER ENRICHMENT
# ─────────────────────────────────────────────────────────

class PropertyAppraiser:
    """
    Queries Miami-Dade PA public proxy API.
    Endpoint: PaServicesProxy.ashx?Operation=GetPropertySearchByFolio
    Also supports address and owner name searches.
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
            ),
            "Referer": "https://www.miamidadepa.gov/",
            "Accept": "application/json, text/javascript, */*",
        })
        self._cache: dict[str, Optional[dict]] = {}

    def lookup_by_address(self, address: str) -> Optional[dict]:
        """Search PA by property address string."""
        if not address or address in self._cache:
            return self._cache.get(address)

        try:
            resp = self.session.get(
                PA_SEARCH_URL,
                params={
                    "Operation": "GetPropertySearchByAddress",
                    "Address": address,
                    "selFormatting": "2",
                },
                timeout=15,
            )
            if resp.status_code != 200:
                self._cache[address] = None
                return None

            data = resp.json()
            results = (
                data.get("MinimumPropertyInfos")
                or data.get("PropertyInfo")
                or []
            )
            if results:
                result = self._parse_pa_result(results[0])
                self._cache[address] = result
                return result
        except Exception as e:
            log.debug(f"PA address lookup failed for '{address}': {e}")

        self._cache[address] = None
        return None

    def lookup_by_folio(self, folio: str) -> Optional[dict]:
        """Search PA by folio number."""
        folio_clean = re.sub(r"[^0-9]", "", folio)
        if not folio_clean or folio_clean in self._cache:
            return self._cache.get(folio_clean)

        try:
            resp = self.session.get(
                PA_SEARCH_URL,
                params={
                    "Operation": "GetPropertySearchByFolio",
                    "clientAppName": "PropertySearch",
                    "folioNumber": folio_clean,
                },
                timeout=15,
            )
            if resp.status_code != 200:
                self._cache[folio_clean] = None
                return None

            data = resp.json()
            results = (
                data.get("MinimumPropertyInfos")
                or data.get("PropertyInfo")
                or []
            )
            if results:
                result = self._parse_pa_result(results[0])
                self._cache[folio_clean] = result
                return result
        except Exception as e:
            log.debug(f"PA folio lookup failed for '{folio}': {e}")

        self._cache[folio_clean] = None
        return None

    def _parse_pa_result(self, hit: dict) -> dict:
        site_addr = (
            hit.get("SiteAddress") or hit.get("siteAddress") or
            hit.get("SITEADDR") or ""
        ).strip()
        site_city = (
            hit.get("SiteCity") or hit.get("siteCity") or
            hit.get("City") or ""
        ).strip()
        site_zip = (
            hit.get("SiteZip") or hit.get("siteZip") or
            hit.get("Zip") or ""
        ).strip()
        mail_addr = (
            hit.get("MailAddress1") or hit.get("mailAddress1") or
            hit.get("MAILADR1") or ""
        ).strip()
        mail_city = (
            hit.get("MailCity") or hit.get("mailCity") or
            hit.get("MAILCITY") or ""
        ).strip()
        mail_state = (
            hit.get("MailState") or hit.get("mailState") or "FL"
        ).strip()
        mail_zip = (
            hit.get("MailZip") or hit.get("mailZip") or
            hit.get("MAILZIP") or ""
        ).strip()
        owner = (
            hit.get("OwnerName") or hit.get("ownerName") or
            hit.get("Owner1") or ""
        ).strip()
        folio = (
            hit.get("FolioNumber") or hit.get("folioNumber") or
            hit.get("Folio") or ""
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
            "owner":        owner,
            "folio":        folio,
            "pa_url": (
                f"https://www.miamidadepa.gov/PropertySearch/default.aspx"
                f"#!searchtype=folio&folio={folio}" if folio else ""
            ),
        }

    def enrich(self, record: dict) -> dict:
        """Enrich record with PA data. Tries folio first, then address."""
        folio = record.get("folio", "").strip()
        address = record.get("prop_address", "").strip()

        pa_data = None
        if folio:
            pa_data = self.lookup_by_folio(folio)
        if not pa_data and address:
            pa_data = self.lookup_by_address(address)

        if pa_data:
            for k, v in pa_data.items():
                if v and not record.get(k):
                    record[k] = v
            if pa_data.get("owner") and not record.get("owner"):
                record["owner"] = pa_data["owner"]

        return record


# ─────────────────────────────────────────────────────────
# REALFORECLOSE SCRAPER
# ─────────────────────────────────────────────────────────

class RealForecloseScraper:
    """
    Scrapes Miami-Dade foreclosure auction data from RealForeclose.com.
    This is the official, publicly accessible foreclosure auction platform
    used by Miami-Dade County — no login required.
    """

    COUNTY = "MIAMI-DADE"
    STATE = "FL"

    def __init__(self, page):
        self.page = page

    async def _goto(self, url: str):
        for attempt in range(MAX_RETRIES):
            try:
                await self.page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                await self.page.wait_for_timeout(2000)
                return
            except PlaywrightTimeout:
                log.warning(f"Timeout navigating to {url}, attempt {attempt + 1}")
                if attempt == MAX_RETRIES - 1:
                    raise
                await asyncio.sleep(RETRY_DELAY)

    async def scrape_auctions(self) -> list[dict]:
        """
        Fetch current and upcoming foreclosure auctions for Miami-Dade.
        Iterates the calendar and preview pages.
        """
        records = []
        log.info("Accessing RealForeclose.com — Miami-Dade foreclosure auctions")

        try:
            # Strategy 1: Direct preview page with county filter
            preview_records = await self._scrape_preview_page()
            records.extend(preview_records)
            log.info(f"Preview page: {len(preview_records)} records")

            # Strategy 2: Monthly calendar pages (next 3 months)
            for month_offset in range(0, 3):
                target_date = date.today().replace(day=1) + timedelta(days=32 * month_offset)
                target_date = target_date.replace(day=1)
                cal_records = await self._scrape_calendar_month(
                    target_date.year, target_date.month
                )
                records.extend(cal_records)
                log.info(
                    f"Calendar {target_date.strftime('%B %Y')}: "
                    f"{len(cal_records)} records"
                )
                await asyncio.sleep(1)

        except Exception as e:
            log.error(f"RealForeclose scrape error: {e}")
            log.debug(traceback.format_exc())

        return records

    async def _scrape_preview_page(self) -> list[dict]:
        """Scrape the PREVIEW/upcoming auctions page filtered to Miami-Dade."""
        records = []
        page_num = 1

        try:
            await self._goto(REALFORECLOSE_SEARCH)

            # Log page title to confirm we're on the right page
            title = await self.page.title()
            log.info(f"RealForeclose page title: {title}")

            # Debug: log visible text to understand page structure
            try:
                body_text = await self.page.locator("body").inner_text()
                log.info(f"Page body snippet (first 500 chars): {body_text[:500]}")
            except Exception:
                pass

            while True:
                html = await self.page.content()
                soup = BeautifulSoup(html, "lxml")
                page_records = self._parse_auction_table(soup, source="FRCL_PREVIEW")

                if not page_records:
                    # Log available tables for debugging
                    tables = soup.find_all("table")
                    log.info(f"Tables found on page: {len(tables)}")
                    for i, tbl in enumerate(tables[:3]):
                        headers = [th.get_text(strip=True) for th in tbl.find_all("th")]
                        log.info(f"  Table {i} headers: {headers[:6]}")
                    break

                records.extend(page_records)
                log.info(f"  Preview page {page_num}: {len(page_records)} auctions")

                # Paginate
                has_next = await self._click_next(soup)
                if not has_next:
                    break
                page_num += 1
                await asyncio.sleep(1.5)

        except Exception as e:
            log.warning(f"Preview page scrape failed: {e}")

        return records

    async def _scrape_calendar_month(self, year: int, month: int) -> list[dict]:
        """Scrape a specific month's auction calendar."""
        records = []
        try:
            cal_url = (
                f"{REALFORECLOSE_BASE}/index.cfm"
                f"?zaction=user&zmethod=calendar&year={year}&month={month:02d}"
            )
            await self._goto(cal_url)
            html = await self.page.content()
            soup = BeautifulSoup(html, "lxml")

            # Find auction day links in the calendar
            auction_links = soup.find_all("a", href=re.compile(r"AUCTIONDATE|auctiondate|auction", re.I))
            log.info(
                f"  Calendar {year}/{month:02d}: "
                f"{len(auction_links)} auction day links found"
            )

            seen_urls = set()
            for link in auction_links:
                href = link.get("href", "")
                if not href:
                    continue
                full_url = href if href.startswith("http") else f"{REALFORECLOSE_BASE}{href}"
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                try:
                    day_records = await self._scrape_auction_day(full_url)
                    records.extend(day_records)
                    await asyncio.sleep(1)
                except Exception as e:
                    log.debug(f"Auction day scrape failed {full_url}: {e}")

        except Exception as e:
            log.warning(f"Calendar month {year}/{month} failed: {e}")

        return records

    async def _scrape_auction_day(self, url: str) -> list[dict]:
        """Scrape a single auction day page."""
        await self._goto(url)
        html = await self.page.content()
        soup = BeautifulSoup(html, "lxml")
        records = self._parse_auction_table(soup, source="FRCL_CALENDAR")
        log.debug(f"  Auction day {url}: {len(records)} records")
        return records

    def _parse_auction_table(self, soup: BeautifulSoup, source: str) -> list[dict]:
        """
        Parse RealForeclose.com auction data.
        The site uses label:value pairs within table cells (NOT traditional columns).
        Scans all cell texts for known labels and groups them by case #.
        """
        records = []

        # Collect all non-empty cell texts from the entire page
        all_texts = []
        for cell in soup.find_all(["td", "th"]):
            t = cell.get_text(separator=" ", strip=True)
            if t:
                all_texts.append(t)

        log.info(f"[DEBUG] Total cell texts: {len(all_texts)}")

        SKIP_LABELS = {
            "auction type:", "final judgment amount:", "parcel id:",
            "property address:", "assessed value:", "plaintiff max bid:",
            "auction date/time:", "auction date:", "case #:",
        }

        def is_label(t: str) -> bool:
            tl = t.lower().strip()
            return any(tl.startswith(lbl) for lbl in SKIP_LABELS)

        # Scan through texts building blocks keyed by "case #:"
        raw_blocks: list[dict] = []
        current: dict = {}
        i = 0

        while i < len(all_texts):
            t = all_texts[i]
            tl = t.lower().strip()

            if "case #:" in tl:
                if current.get("case_num"):
                    raw_blocks.append(current)
                current = {}
                # Value may be inline ("Case #: 2022-123") or next cell
                if ":" in t:
                    after = t.split("case #:", 1)[-1].strip() if "case #:" in tl else ""
                    if after and not is_label(after):
                        current["case_num"] = after
                        i += 1
                        continue
                val = all_texts[i + 1].strip() if i + 1 < len(all_texts) else ""
                current["case_num"] = val
                i += 2

            elif "final judgment amount:" in tl:
                after = t.split("final judgment amount:", 1)[-1].strip()
                if after:
                    current["amount_str"] = after
                    i += 1
                else:
                    val = all_texts[i + 1].strip() if i + 1 < len(all_texts) else ""
                    current["amount_str"] = val if not is_label(val) else ""
                    i += 2

            elif "parcel id:" in tl:
                after = t.split("parcel id:", 1)[-1].strip()
                if after:
                    current["folio"] = after
                    i += 1
                else:
                    val = all_texts[i + 1].strip() if i + 1 < len(all_texts) else ""
                    current["folio"] = val if not is_label(val) else ""
                    i += 2

            elif "property address:" in tl:
                after = t.split("property address:", 1)[-1].strip()
                if after:
                    current["address1"] = after
                    i += 1
                else:
                    addr1 = all_texts[i + 1].strip() if i + 1 < len(all_texts) else ""
                    addr2 = ""
                    if i + 2 < len(all_texts):
                        nxt = all_texts[i + 2].strip()
                        if re.search(r",\s*fl[\s\-]", nxt, re.I) or (not is_label(nxt) and "fl" in nxt.lower()):
                            addr2 = nxt
                            i += 3
                        else:
                            i += 2
                    else:
                        i += 2
                    current["address1"] = addr1
                    current["address2"] = addr2

            elif "assessed value:" in tl:
                after = t.split("assessed value:", 1)[-1].strip()
                current["assessed"] = after if after else (
                    all_texts[i + 1].strip() if i + 1 < len(all_texts) else ""
                )
                i += 1 if after else 2

            elif "plaintiff max bid:" in tl:
                after = t.split("plaintiff max bid:", 1)[-1].strip()
                current["plaintiff"] = after if after else (
                    all_texts[i + 1].strip() if i + 1 < len(all_texts) else ""
                )
                i += 1 if after else 2

            elif "auction type:" in tl:
                after = t.split("auction type:", 1)[-1].strip()
                current["auction_type"] = after if after else (
                    all_texts[i + 1].strip() if i + 1 < len(all_texts) else ""
                )
                i += 1 if after else 2

            elif "auction date" in tl and ":" in tl:
                after = t.split(":", 1)[-1].strip()
                current["auction_date"] = after if after else (
                    all_texts[i + 1].strip() if i + 1 < len(all_texts) else ""
                )
                i += 1 if after else 2

            else:
                i += 1

        if current.get("case_num"):
            raw_blocks.append(current)

        log.info(f"[DEBUG] Auction blocks parsed: {len(raw_blocks)}")
        if raw_blocks:
            log.info(f"[DEBUG] First block: {raw_blocks[0]}")

        for block in raw_blocks:
            try:
                record = self._block_to_record(block, source)
                if record:
                    records.append(record)
            except Exception as e:
                log.debug(f"Block conversion error: {e}")

        return records

    def _block_to_record(self, block: dict, source: str) -> Optional[dict]:
        """Convert a parsed label:value block into a lead record."""
        case_num = block.get("case_num", "").strip()
        if not case_num:
            return None

        # Amount
        amount_raw = block.get("amount_str", "").replace("$", "").replace(",", "").strip()
        try:
            amount = float(amount_raw) if amount_raw else 0.0
        except ValueError:
            amount = 0.0

        # Address
        addr1 = block.get("address1", "").strip()
        addr2 = block.get("address2", "").strip()

        # Parse "miami, fl- 33177" → city, zip
        city, zip_code = "Miami", ""
        if addr2:
            m = re.match(r"^(.+?),\s*fl[\s\-]+(\d{5})", addr2, re.I)
            if m:
                city = m.group(1).strip().title()
                zip_code = m.group(2).strip()
            else:
                city = addr2.split(",")[0].strip().title()
        elif addr1:
            # Sometimes city is embedded in addr1: "12825 sw 188 st miami, fl- 33177"
            m = re.search(r"(.+?),\s*fl[\s\-]+(\d{5})", addr1, re.I)
            if m:
                city = m.group(1).rsplit(" ", 1)[-1].strip().title() if " " in m.group(1) else m.group(1).strip().title()
                zip_code = m.group(2).strip()
                addr1 = addr1[:m.start()].strip()

        folio = block.get("folio", "").strip()
        auction_date = self._parse_date(block.get("auction_date", ""))

        return {
            "doc_num":        case_num,
            "doc_type":       "FRCL",
            "cat":            "foreclosure",
            "cat_label":      "Foreclosure Auction",
            "filed":          auction_date or date.today().isoformat(),
            "frcl_sale_date": auction_date,
            "owner":          "",
            "contact":        "",
            "grantee":        block.get("plaintiff", ""),
            "amount":         amount,
            "legal":          "",
            "folio":          folio,
            "prop_address":   addr1,
            "prop_city":      city,
            "prop_state":     "FL",
            "prop_zip":       zip_code,
            "mail_address":   "",
            "mail_city":      "",
            "mail_state":     "FL",
            "mail_zip":       "",
            "clerk_url":      "",
            "sources":        [source],
            "match_confidence": "HIGH" if folio else "LOW",
        }

    def _parse_date(self, raw: str) -> str:
        if not raw:
            return ""
        # Remove time part if present: "06/15/2026 09:00 AM" → "06/15/2026"
        raw = raw.split()[0] if raw else raw
        for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%y"]:
            try:
                return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return ""

    async def _click_next(self, soup: BeautifulSoup) -> bool:
        """Click 'Next' pagination if available."""
        next_selectors = [
            "a:text-is('Next')",
            "a:text-is('>')",
            "a[title='Next']",
            "input[value='Next']",
        ]
        for sel in next_selectors:
            try:
                if await self.page.locator(sel).count() > 0:
                    await self.page.locator(sel).first.click()
                    await self.page.wait_for_load_state("domcontentloaded", timeout=20_000)
                    await self.page.wait_for_timeout(1500)
                    return True
            except Exception:
                pass

        # ASP.NET __doPostBack next page
        for link in soup.find_all("a", href=re.compile(r"__doPostBack")):
            text = link.get_text(strip=True)
            if text in (">", "Next", "next"):
                match = re.search(r"__doPostBack\('([^']+)'", link.get("href", ""))
                if match:
                    try:
                        await self.page.evaluate(f"__doPostBack('{match.group(1)}', '')")
                        await self.page.wait_for_load_state("domcontentloaded", timeout=20_000)
                        await self.page.wait_for_timeout(1500)
                        return True
                    except Exception:
                        pass

        return False


# ─────────────────────────────────────────────────────────
# GHL CSV EXPORT
# ─────────────────────────────────────────────────────────

GHL_COLUMNS = [
    "First Name", "Last Name", "Mailing Address", "Mailing City",
    "Mailing State", "Mailing Zip", "Property Address", "Property City",
    "Property State", "Property Zip", "Lead Type", "Document Type",
    "Date Filed", "Document Number", "Amount/Debt Owed",
    "Seller Score", "Motivated Seller Flags", "Sale Date", "Source", "Public Records URL",
]


def split_name(full_name: str) -> tuple[str, str]:
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
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=GHL_COLUMNS)
    writer.writeheader()
    for r in records:
        first, last = split_name(r.get("owner", ""))
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
            "Sale Date":            r.get("frcl_sale_date", ""),
            "Source":               "RealForeclose.com — Miami-Dade County",
            "Public Records URL":   r.get("clerk_url", ""),
        })
    return output.getvalue()


# ─────────────────────────────────────────────────────────
# OUTPUT
# ─────────────────────────────────────────────────────────

def build_output(records: list[dict]) -> dict:
    with_address = sum(1 for r in records if r.get("prop_address"))
    return {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "RealForeclose.com — Miami-Dade County Foreclosure Auctions",
        "date_range":   {
            "from": date.today().isoformat(),
            "to":   (date.today() + timedelta(days=90)).isoformat(),
        },
        "total":        len(records),
        "with_address": with_address,
        "records":      records,
    }


def save_outputs(output: dict, records: list[dict]):
    payload = json.dumps(output, indent=2, default=str)
    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload, encoding="utf-8")
        log.info(f"JSON guardado en: {path}")

    GHL_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    GHL_CSV_PATH.write_text(records_to_ghl_csv(records), encoding="utf-8")
    log.info(f"GHL CSV guardado en: {GHL_CSV_PATH}")


def deduplicate(records: list[dict]) -> list[dict]:
    seen: dict[str, dict] = {}
    for r in records:
        key = r.get("doc_num", "")
        if not key:
            continue
        if key not in seen or (r.get("score") or 0) > (seen[key].get("score") or 0):
            seen[key] = r
    return list(seen.values())


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────

async def run():
    log.info("=" * 60)
    log.info("DWD MIAMI-DADE — Extractor de Ejecuciones Hipotecarias Activas")
    log.info("Fuente: RealForeclose.com (portal público de subastas)")
    log.info("=" * 60)

    pa = PropertyAppraiser()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            locale="en-US",
            ignore_https_errors=True,
        )
        page = await context.new_page()
        scraper = RealForecloseScraper(page)

        log.info("Accediendo al calendario de subastas públicas de Miami-Dade RealForeclose...")
        all_records = await scraper.scrape_auctions()

        await browser.close()

    log.info(f"Leads de Foreclosures encontrados en subastas: {len(all_records)}")

    # Enrich with Property Appraiser data
    if all_records:
        log.info("Enriqueciendo con datos del Miami-Dade Property Appraiser...")
        for i, record in enumerate(all_records):
            try:
                pa.enrich(record)
            except Exception as e:
                log.debug(f"PA enrichment error on record {i}: {e}")
            if (i + 1) % 20 == 0:
                log.info(f"  Enriquecidos {i + 1}/{len(all_records)}...")

    # Score and flag
    for record in all_records:
        flags = compute_flags(record)
        record["flags"] = flags
        record["score"] = compute_score(record, flags)

    # Deduplicate
    all_records = deduplicate(all_records)

    # Sort by score desc
    all_records.sort(key=lambda r: -(r.get("score") or 0))

    log.info(f"Total de leads únicos: {len(all_records)}")

    output = build_output(all_records)
    save_outputs(output, all_records)

    with_addr = output["with_address"]
    log.info(f"Con dirección confirmada: {with_addr}/{len(all_records)}")
    log.info(
        f"Proceso finalizado. {len(all_records)} leads de alto valor "
        f"sincronizados en tu panel."
    )


if __name__ == "__main__":
    asyncio.run(run())
