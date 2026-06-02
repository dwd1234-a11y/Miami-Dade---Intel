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
# Cambiado a la URL del portal real moderno de registros de Miami-Dade
CLERK_BASE = "https://onlineservices.miamidadeclerk.gov"
CLERK_RECORDS_URL = f"{CLERK_BASE}/officialrecords"
PA_SEARCH_URL = "https://apps.miamidadepa.gov/PropertySearch/api/Search"
PA_PROPERTY_URL = "https://apps.miamidadepa.gov/PropertySearch/api/Property"
OUTPUT_PATHS = [
    Path("dashboard/records.json"),
    Path("data/records.json"),
]
GHL_CSV_PATH = Path("data/ghl_export.csv")
MAX_RETRIES = 3
RETRY_DELAY = 5  # Segundos
HEADLESS = os.environ.get("HEADLESS", "true").lower() != "false"

# Mapeo de Códigos Oficiales de Florida / Miami-Dade
DOC_TYPE_MAP = {
    "LP":       ("foreclosure",   "Lis Pendens"),
    "NOFC":     ("foreclosure",   "Notice of Foreclosure"),
    "TAXDEED":  ("tax",           "Tax Deed"),
    "JUD":      ("judgment",      "Judgment"),
    "CCJ":      ("judgment",      "Certified Judgment"),
    "DRJUD":    ("judgment",      "Domestic Relations Judgment"),
    "LNCORPTX": ("lien",          "Corporate Tax Lien"),
    "LNIRS":    ("lien",          "IRS Lien"),
    "LNFED":    ("lien",          "Federal Lien"),
    "LN":       ("lien",          "Lien"),
    "LNMECH":   ("lien",          "Mechanic's Lien"),
    "LNHOA":    ("lien",          "HOA Lien"),
    "MEDLN":    ("lien",          "Medicaid Lien"),
    "PRO":      ("probate",       "Probate"),
    "NOC":      ("construction",  "Notice of Commencement"),
    "RELLP":    ("release",       "Release of Lis Pendens"),
}

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

    if filed_str:
        try:
            filed_date = datetime.strptime(filed_str, "%Y-%m-%d").date()
            if (date.today() - filed_date).days <= 7:
                flags.append("New this week")
        except Exception:
            pass

    return list(dict.fromkeys(flags))


def compute_score(record: dict, flags: list[str]) -> int:
    score = 30  # Base
    score += len(flags) * 10

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
        variants = [full_name]
        parts = full_name.split()
        if len(parts) >= 2:
            variants.append(f"{parts[-1]} {' '.join(parts[:-1])}")
            variants.append(f"{parts[-1]}, {' '.join(parts[:-1])}")
        return variants

    def _search_by_name(self, name: str) -> Optional[dict]:
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

    def _parse_pa_result(self, hit: dict) -> dict:
        site_addr = (hit.get("SiteAddress") or hit.get("SITEADDR") or hit.get("SiteAddr") or hit.get("site_addr") or "").strip()
        site_city = (hit.get("SiteCity") or hit.get("SITE_CITY") or hit.get("City") or "").strip()
        site_zip = (hit.get("SiteZip") or hit.get("SITE_ZIP") or hit.get("Zip") or "").strip()
        mail_addr = (hit.get("MailingAddress1") or hit.get("MAILADR1") or hit.get("MailAddr1") or hit.get("mail_addr") or "").strip()
        mail_city = (hit.get("MailingCity") or hit.get("MAILCITY") or hit.get("MailCity") or "").strip()
        mail_state = (hit.get("MailingState") or hit.get("STATE") or hit.get("MailState") or "FL").strip()
        mail_zip = (hit.get("MailingZip") or hit.get("MAILZIP") or hit.get("MailZip") or "").strip()

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
                if v:
                    record[k] = v

        return record


# ─────────────────────────────────────────────────────────
# PORTAL CLERK SCRAPER (Playwright Automatizado)
# ─────────────────────────────────────────────────────────

class ClerkScraper:
    def __init__(self, page):
        self.page = page
        self.base_url = CLERK_BASE

    async def _safe_goto(self, url: str):
        for attempt in range(MAX_RETRIES):
            try:
                await self.page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                return
            except PlaywrightTimeout:
                log.warning(f"Timeout al ir a {url}, intento {attempt + 1}/{MAX_RETRIES}")
                if attempt == MAX_RETRIES - 1:
                    raise
                await asyncio.sleep(RETRY_DELAY)

    async def search_doc_type(self, doc_type: str, date_from: str, date_to: str) -> list[dict]:
        records = []
        log.info(f"Iniciando búsqueda en Clerk: {doc_type} ({date_from} hasta {date_to})")

        try:
            await self._safe_goto(CLERK_RECORDS_URL)
            await self.page.wait_for_timeout(3000)

            # ── 1. GESTIÓN DEL DISCLAIMER O AVISO LEGAL OBLIGATORIO ──
            # Se buscan variaciones de botones típicas en plataformas de Florida
            disclaimer_selectors = [
                "button:has-text('Accept')", 
                "button:has-text('I Agree')", 
                "#btnAccept", 
                "a:has-text('AcceptTerms')",
                "button:has-text('Acknowledge')"
            ]
            for selector in disclaimer_selectors:
                if await self.page.locator(selector).count() > 0:
                    log.info("Aviso legal detectado. Aceptando términos para ingresar al portal...")
                    await self.page.click(selector)
                    await self.page.wait_for_load_state("networkidle")
                    await self.page.wait_for_timeout(2000)
                    break

            # Asegurar la creación de directorios para las capturas de depuración
            Path("dashboard").mkdir(parents=True, exist_ok=True)

            # ── 2. VOLCADO DE COMPONENTES (Debug de la guía de replicación) ──
            elements = await self.page.eval_on_selector_all(
                "input, select, button",
                "els => els.map(e => ({tag: e.tagName, id: e.id, name: e.name, type: e.type}))"
            )
            log.info(f"Campos interactivos detectados en el DOM: {elements[:12]}")

            # ── 3. RELLENAR CAMPOS DEL FORMULARIO DE MIAMI-DADE ──
            # Reescrito con selectores adaptados al entorno actual de Miami-Dade
            doc_input = self.page.locator("input[name*='docType'], select[name*='docType'], #txtDocType, input[id*='DocType']").first
            if await doc_input.count() > 0:
                if await doc_input.evaluate("el => el.tagName") == "SELECT":
                    await doc_input.select_option(value=doc_type)
                else:
                    await doc_input.fill(doc_type)
            else:
                log.warning(f"No se localizó componente directo para DocType. Intentando continuar...")

            # Fechas en formato MM/DD/YYYY requerido nativamente por Miami-Dade
            date_from_input = self.page.locator("input[name*='From'], input[id*='From'], input[name*='start'], #txtDateFrom").first
            date_to_input = self.page.locator("input[name*='To'], input[id*='To'], input[name*='end'], #txtDateTo").first

            if await date_from_input.count() > 0:
                await date_from_input.fill(date_from)
            if await date_to_input.count() > 0:
                await date_to_input.fill(date_to)

            # ── 4. PRESIONAR BOTÓN DE BÚSQUEDA ──
            search_btn = self.page.locator("input[type='submit'], button[id*='Search'], button:has-text('Search'), #btnSearch").first
            if await search_btn.count() > 0:
                await search_btn.click()
                await self.page.wait_for_load_state("networkidle", timeout=30_000)
            else:
                # Intento de contingencia usando la tecla Enter
                await self.page.keyboard.press("Enter")
                await self.page.wait_for_load_state("networkidle", timeout=30_000)

            await self.page.wait_for_timeout(2000)
            
            # Guardar captura de pantalla en la carpeta pública del Dashboard para auditoría visual
            await self.page.screenshot(path="dashboard/debug_clerk_search.png")

            # ── 5. EXTRACCIÓN DE RESULTADOS DE LA TABLA ──
            records = await self._extract_results(doc_type)

        except Exception as e:
            log.error(f"Error crítico procesando {doc_type}: {e}")
            log.debug(traceback.format_exc())
            try:
                await self.page.screenshot(path="dashboard/error_clerk_screenshot.png")
            except Exception:
                pass

        log.info(f"   Finalizado: {len(records)} registros extraídos para {doc_type}")
        return records

    async def _extract_results(self, doc_type: str) -> list[dict]:
        all_records = []
        page_num = 1

        while True:
            html = await self.page.content()
            soup = BeautifulSoup(html, "lxml")

            page_records = self._parse_results_table(soup, doc_type)
            all_records.extend(page_records)

            log.info(f"   Página {page_num}: parseados {len(page_records)} registros")

            # Si la página actual no arrojó registros, cancelamos paginación inmediatamente
            if not page_records:
                break

            next_page = await self._go_next_page(soup)
            if not next_page:
                break

            page_num += 1
            await asyncio.sleep(2)

        return all_records

    def _parse_results_table(self, soup: BeautifulSoup, doc_type: str) -> list[dict]:
        records = []
        tables = soup.find_all("table")
        result_table = None

        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all(["th", "td"])]
            if any(kw in " ".join(headers) for kw in ["document", "grantor", "grantee", "filed", "book", "case"]):
                result_table = table
                break

        if not result_table:
            return records

        rows = result_table.find_all("tr")
        if len(rows) <= 1:
            return records

        # Mapeo posicional dinámico de columnas del encabezado
        header_row = rows[0]
        headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])]

        col_map = {}
        for i, h in enumerate(headers):
            if "doc" in h or "number" in h or "clerk" in h:
                col_map["doc_num"] = i
            elif "type" in h:
                col_map["doc_type_col"] = i
            elif "filed" in h or "date" in h or "record" in h:
                col_map["filed"] = i
            elif "grantor" in h or "owner" in h or "from" in h:
                col_map["grantor"] = i
            elif "grantee" in h or "to" in h:
                col_map["grantee"] = i
            elif "legal" in h or "desc" in h:
                col_map["legal"] = i
            elif "amount" in h or "value" in h:
                col_map["amount"] = i

        for row in rows[1:]:
            try:
                cells = row.find_all(["td", "th"])
                if not cells or len(cells) < 2:
                    continue

                def cell_text(key: str) -> str:
                    idx = col_map.get(key)
                    if idx is not None and idx < len(cells):
                        return cells[idx].get_text(strip=True)
                    return ""

                doc_num = cell_text("doc_num")
                if not doc_num:
                    link = row.find("a")
                    if link:
                        doc_num = link.get_text(strip=True)

                if not doc_num:
                    continue

                link_tag = row.find("a", href=True)
                clerk_url = ""
                if link_tag:
                    href = link_tag["href"]
                    clerk_url = href if href.startswith("http") else f"{self.base_url}{href}"

                amount_str = cell_text("amount").replace("$", "").replace(",", "").strip()
                try:
                    amount = float(amount_str) if amount_str else 0.0
                except ValueError:
                    amount = 0.0

                filed_raw = cell_text("filed").strip()
                filed = self._normalize_date(filed_raw)

                # Si es un Lis Pendens, invertimos contacto para capturar al Propietario (Grantee)
                owner_name = cell_text("grantor").strip()
                grantee_name = cell_text("grantee").strip()
                if doc_type == "LP" and grantee_name:
                    owner_name = grantee_name

                record = {
                    "doc_num":  doc_num.strip(),
                    "doc_type": doc_type,
                    "filed":    filed,
                    "owner":    owner_name,
                    "grantee":  grantee_name,
                    "legal":    cell_text("legal").strip(),
                    "amount":   amount,
                    "clerk_url": clerk_url,
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

                records.append(record)
            except Exception:
                continue

        return records

    def _normalize_date(self, raw: str) -> str:
        if not raw:
            return ""
        formats = ["%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%m/%d/%y"]
        for fmt in formats:
            try:
                return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return raw

    async def _go_next_page(self, soup: BeautifulSoup) -> bool:
        next_selectors = [
            "a:has-text('Next')",
            "a[title*='Next']",
            "button:has-text('Next')",
            "li.next a",
            "a:has-text('❯')"
        ]
        for sel in next_selectors:
            if await self.page.locator(sel).count() > 0:
                try:
                    await self.page.click(sel)
                    await self.page.wait_for_load_state("networkidle", timeout=15_000)
                    return True
                except Exception:
                    pass
        return False


# ─────────────────────────────────────────────────────────
# EXPORTACIÓN Y PERSISTENCIA (Sincronizado con el Dashboard)
# ─────────────────────────────────────────────────────────

GHL_COLUMNS = [
    "First Name", "Last Name", "Mailing Address", "Mailing City",
    "Mailing State", "Mailing Zip", "Property Address", "Property City",
    "Property State", "Property Zip", "Lead Type", "Document Type",
    "Date Filed", "Document Number", "Amount/Debt Owed",
    "Seller Score", "Motivated Seller Flags", "Source", "Public Records URL",
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
            "Source":               "Miami-Dade Clerk Public Records",
            "Public Records URL":   r.get("clerk_url", ""),
        })
    return output.getvalue()


def build_output(records: list[dict], date_from: str, date_to: str) -> dict:
    with_address = sum(1 for r in records if r.get("prop_address") or r.get("mail_address"))
    return {
        "fetched_at":    datetime.utcnow().isoformat() + "Z",
        "source":        "Miami-Dade Clerk of Courts Public Records",
        "date_range":    {"from": date_from, "to": date_to},
        "total":         len(records),
        "with_address":  with_address,
        "records":       records,
    }


def save_outputs(output: dict, records: list[dict]):
    payload = json.dumps(output, indent=2, default=str)
    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload, encoding="utf-8")
        log.info(f"Guardado exitoso en JSON: {path}")

    GHL_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    GHL_CSV_PATH.write_text(records_to_ghl_csv(records), encoding="utf-8")
    log.info(f"Guardado exitoso en GHL CSV: {GHL_CSV_PATH}")


def deduplicate(records: list[dict]) -> list[dict]:
    seen: dict[str, dict] = {}
    for r in records:
        key = r.get("doc_num", "")
        if not key:
            continue
        if key not in seen or r.get("score", 0) > seen[key].get("score", 0):
            seen[key] = r
    return list(seen.values())


# ─────────────────────────────────────────────────────────
# ORQUESTRADOR CENTRAL
# ─────────────────────────────────────────────────────────

async def run():
    today = date.today()
    date_from_dt = today - timedelta(days=LOOKBACK_DAYS)
    
    # Formato nativo MM/DD/YYYY para inputs del portal de Miami-Dade
    date_from = date_from_dt.strftime("%m/%d/%Y")
    date_to   = today.strftime("%m/%d/%Y")
    
    date_from_iso = date_from_dt.strftime("%Y-%m-%d")
    date_to_iso   = today.strftime("%Y-%m-%d")

    log.info("=" * 60)
    log.info("DWD Motivated Seller Scraper — Miami-Dade County (Florida)")
    log.info(f"Rango de Fechas: {date_from} hasta {date_to}")
    log.info("=" * 60)

    pa = PropertyAppraiser()
    all_records: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        page = await context.new_page()
        scraper = ClerkScraper(page)

        for doc_type in TARGET_DOC_TYPES:
            try:
                records = await scraper.search_doc_type(doc_type, date_from, date_to)
                all_records.extend(records)
            except Exception as e:
                log.error(f"Fallo en bloque secuencial para {doc_type}: {e}")
            await asyncio.sleep(3)

        await browser.close()

    log.info(f"Total registros crudos recolectados: {len(all_records)}")

    # Enriquecimiento cruzado automatizado mediante la API del Property Appraiser de Miami-Dade
    if all_records:
        log.info("Iniciando cruce de datos con la API del Property Appraiser...")
        for i, record in enumerate(all_records):
            try:
                pa.enrich(record)
            except Exception as e:
                log.debug(f"Error omitido en fila {i}: {e}")
            if (i + 1) % 50 == 0 or (i + 1) == len(all_records):
                log.info(f"   Progreso de enriquecimiento: {i + 1}/{len(all_records)}...")

    # Computar banderas de distress y scores de motivación (0-100)
    for record in all_records:
        try:
            flags = compute_flags(record)
            record["flags"] = flags
            record["score"] = compute_score(record, flags)
        except Exception:
            record["flags"] = []
            record["score"] = 30

    all_records = deduplicate(all_records)
    all_records.sort(key=lambda r: (-(r.get("score") or 0), r.get("filed") or ""), reverse=False)

    output = build_output(all_records, date_from_iso, date_to_iso)
    save_outputs(output, all_records)

    log.info("\n--- RESUMEN FINAL DE LA EJECUCIÓN ---")
    log.info(f"   Total leads únicos procesados: {len(all_records)}")
    log.info(f"   Leads con dirección mapeada con éxito: {output['with_address']}")
    log.info("Proceso completado exitosamente.")


if __name__ == "__main__":
    asyncio.run(run())
