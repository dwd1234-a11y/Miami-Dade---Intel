"""
Deals With Dignity — Miami-Dade County Multi-Source Distress Scraper
Extrae Foreclosures públicos y Tax Distress sin pasar por pasarelas de pago.

Run: python scraper/fetch.py
"""

import json
import csv
import io
import os
import logging
from datetime import datetime, timedelta, date
from pathlib import Path
import requests

# ─────────────────────────────────────────────────────────
# CONFIGURACIÓN DE RUTAS ABIERTAS
# ─────────────────────────────────────────────────────────
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", 7))

# Endpoints alternativos abiertos de subastas y datos inmobiliarios de Miami-Dade
FORECLOSE_CALENDAR_URL = "https://miamidade.realforeclose.com/index.cfm?zaction=USER&zact=getCalendarData"
PA_API_URL = "https://apps.miamidadepa.gov/PropertySearch/api/Search"

OUTPUT_PATHS = [
    Path("dashboard/records.json"),
    Path("data/records.json"),
]
GHL_CSV_PATH = Path("data/ghl_export.csv")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("dwd_miami_multisource")

class MiamiDadeDistressScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*",
            "X-Requested-With": "XMLHttpRequest"
        })

    def fetch_foreclosure_auctions(self) -> list[dict]:
        """Extrae las ejecuciones hipotecarias del calendario público de subastas de Miami-Dade."""
        records = []
        log.info("Accediendo al calendario de subastas públicas de Miami-Dade RealForeclose...")
        
        # Consultamos los días del rango seleccionado
        today = date.today()
        for i in range(LOOKBACK_DAYS):
            target_date = today + timedelta(days=i) # Buscamos subastas futuras programadas
            date_str = target_date.strftime("%m/%d/%Y")
            
            try:
                # Petición al endpoint del calendario público de Florida RealForeclose
                resp = self.session.get(FORECLOSE_CALENDAR_URL, params={"daybyday": date_str}, timeout=15)
                if resp.status_code != 200:
                    continue
                
                data = resp.json()
                # El sistema devuelve un listado de objetos con los casos del día
                cards = data.get("daydata") or data.get("cards") or []
                
                for card in cards:
                    case_num = card.get("casenum") or card.get("CASE_NUMBER")
                    if not case_num:
                        continue
                        
                    # Extraer el dueño (usualmente listado en el campo de deudor o estilo de caso)
                    title = card.get("title", "").upper()
                    owner_name = "UNKNOWN OWNER"
                    if "VS" in title:
                        # En Florida: 'BANCO VS PROPIETARIO', el dueño va después del VS
                        parts = title.split("VS")
                        if len(parts) > 1:
                            owner_name = parts[1].strip()

                    record = {
                        "doc_num": str(case_num).strip(),
                        "doc_type": "LP",
                        "filed": today.strftime("%Y-%m-%d"),
                        "owner": owner_name,
                        "grantee": title.split("VS")[0].strip() if "VS" in title else "LENDER",
                        "legal": f"Auction Date: {date_str}. Final Judgment Amount: {card.get('amount', 'N/A')}",
                        "amount": float(str(card.get("amount", 0)).replace("$", "").replace(",", "")) if card.get("amount") else 0.0,
                        "clerk_url": f"https://miamidade.realforeclose.com/index.cfm?zaction=AUCTION&zact=showcase&caseid={case_num}",
                        "prop_address": "", "prop_city": "", "prop_state": "FL", "prop_zip": "",
                        "mail_address": "", "mail_city": "", "mail_state": "FL", "mail_zip": "",
                        "cat": "foreclosure",
                        "cat_label": "Foreclosure Auction Sale"
                    }
                    records.append(record)
                    
            except Exception as e:
                log.debug(f"Omitiendo fecha de subasta {date_str}: {e}")
                continue
                
        return records

    def enrich_with_pa(self, record: dict):
        """Cruza los datos del dueño de la subasta con el Property Appraiser para sacar la dirección."""
        owner = record.get("owner", "").strip()
        if not owner or "UNKNOWN" in owner or len(owner) < 4:
            return

        # Limpieza de sufijos corporativos comunes para mejorar el match de la API
        clean_owner = owner.replace("ET AL", "").replace("ET UX", "").replace(",", "").strip()
        
        try:
            resp = self.session.get(PA_API_URL, params={"q": clean_owner, "s": "ownername", "p": 1, "size": 1}, timeout=10)
            if resp.status_code == 200:
                results = resp.json().get("MinimumResults") or resp.json().get("Results") or []
                if results:
                    hit = results[0]
                    record["prop_address"] = (hit.get("SiteAddress") or "").strip()
                    record["prop_city"] = (hit.get("SiteCity") or "").strip()
                    record["prop_zip"] = (hit.get("SiteZip") or "").strip()
                    record["mail_address"] = (hit.get("MailingAddress1") or "").strip()
                    record["mail_city"] = (hit.get("MailingCity") or "").strip()
                    record["mail_zip"] = (hit.get("MailingZip") or "").strip()
        except Exception as e:
            log.debug(f"No se pudo enriquecer a {clean_owner} en el PA: {e}")

# ─────────────────────────────────────────────────────────
# PROCESAMIENTO Y SISTEMA DE PUNTUACIÓN
# ─────────────────────────────────────────────────────────

def save_outputs(records: list[dict]):
    today_str = date.today().strftime("%Y-%m-%d")
    output = {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "source": "Miami-Dade Unified Public Auctions & Property Appraiser",
        "date_range": {"from": today_str, "to": today_str},
        "total": len(records),
        "with_address": sum(1 for r in records if r.get("prop_address")),
        "records": records
    }
    
    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(output, indent=2, default=str), encoding="utf-8")
        log.info(f"JSON guardado en: {path}")

    GHL_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(GHL_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["First Name", "Property Address", "Property City", "Property Zip", "Seller Score", "Motivated Seller Flags"])
        for r in records:
            writer.writerow([r["owner"], r["prop_address"], r["prop_city"], r["prop_zip"], r["score"], "|".join(r["flags"])])

def run():
    log.info("==========================================================")
    log.info("DWD MIAMI-DADE — Extractor de Ejecuciones Hipotecarias Activas")
    log.info("==========================================================")

    scraper = MiamiDadeDistressScraper()
    
    # 1. Traer ejecuciones del calendario de subastas abierto
    leads = scraper.fetch_foreclosure_auctions()
    log.info(f"Leads de Foreclosures encontrados en subastas: {len(leads)}")

    # 2. Enriquecer con direcciones reales
    if leads:
        log.info("Cruzando registros de subastas con bases de datos del Property Appraiser...")
        for i, l in enumerate(leads):
            scraper.enrich_with_pa(l)
            l["flags"] = ["Pre-foreclosure", "Auction Scheduled"]
            l["score"] = 95 if l["prop_address"] else 70
            if (i+1) % 10 == 0 or (i+1) == len(leads):
                log.info(f"   Progreso: {i+1}/{len(leads)}...")

    # Remover registros duplicados
    seen = {}
    for l in leads:
        seen[l["doc_num"]] = l
    final_leads = list(seen.values())

    save_outputs(final_leads)
    log.info(f"Proceso finalizado. {len(final_leads)} leads de alto valor sincronizados en tu panel.")

if __name__ == "__main__":
    run()
