"""
Deals With Dignity — Miami-Dade County Motivated Seller Lead Scraper
PORTAL DE CORTES PÚBLICAS (No requiere Login / No requiere Pago)

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
# CONFIG / ENDPOINTS ABIERTOS
# ─────────────────────────────────────────────────────────
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", 7))

# Usamos el endpoint público del sistema de casos civiles (Foreclosures civiles)
MIAMI_COURT_API = "https://www2.miamidadeclerk.gov/CivilWeb/CaseSearch/CaseSearchByDateFiled"
PA_SEARCH_URL = "https://apps.miamidadepa.gov/PropertySearch/api/Search"

OUTPUT_PATHS = [
    Path("dashboard/records.json"),
    Path("data/records.json"),
]
GHL_CSV_PATH = Path("data/ghl_export.csv")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("dwd_court_rescue")

class MiamiDadeCourtScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest"
        })

    def fetch_foreclosures(self, date_str: str) -> list[dict]:
        """Trae los casos de Foreclosure presentados en una fecha específica (Abierto)."""
        records = []
        log.info(f"Buscando ejecuciones hipotecarias archivadas el: {date_str}...")
        
        # Parámetros para el portal civil público de Miami-Dade
        # Tipo de Código de Caso 22 = 'CH FORECLOSURE' / 'PROPERTY'
        params = {
            "dateFiled": date_str,  # Formato MM/DD/YYYY
            "codeCode": "22",       # Código de corte para ejecuciones de propiedades
            "page": 1,
            "rows": 100
        }

        try:
            # Petición al visor de casos del circuito civil que alimenta el portal sin clave
            response = self.session.get(MIAMI_COURT_API, params=params, timeout=20)
            if response.status_code != 200:
                return records

            data = response.json()
            # Estructura de filas devuelta por la tabla de la corte
            rows = data.get("rows") or data.get("Results") or []
            
            for item in rows:
                case_id = item.get("CaseNumber") or item.get("caseNumber")
                if not case_id:
                    continue

                # En las demandas de la corte:
                # Defendant = El dueño demandado (A quien queremos comprarle)
                # Plaintiff = El banco o la HOA demandante (Quien inicia el Lis Pendens)
                defendant = item.get("DefendantName") or item.get("defName") or "UNKNOWN"
                plaintiff = item.get("PlaintiffName") or item.get("plName") or ""

                record = {
                    "doc_num": str(case_id).strip(),
                    "doc_type": "LP",
                    "filed": datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d"),
                    "owner": str(defendant).strip().upper(),
                    "grantee": str(plaintiff).strip().upper(),
                    "legal": f"Case Style: {item.get('CaseStyle', '')}",
                    "amount": 0.0,
                    "clerk_url": f"https://www2.miamidadeclerk.gov/CivilWeb/CaseSearch/CaseSummary?CaseNumber={case_id}",
                    "prop_address": "", "prop_city": "", "prop_state": "FL", "prop_zip": "",
                    "mail_address": "", "mail_city": "", "mail_state": "FL", "mail_zip": "",
                    "cat": "lp",
                    "cat_label": "Lis Pendens"
                }
                records.append(record)

        except Exception as e:
            log.error(f"Error conectando a la base de datos de la corte: {e}")
        
        return records

    def enrich_with_pa(self, record: dict):
        """Cruza el nombre del demandado con el Property Appraiser."""
        owner = record.get("owner", "").strip()
        if not owner or "UNKNOWN" in owner or len(owner) < 4:
            return

        # Limpieza básica del formato de la corte 'APELLIDO NOMBRE'
        clean_owner = owner.replace(",", "")
        
        try:
            resp = self.session.get(PA_SEARCH_URL, params={"q": clean_owner, "s": "ownername", "p": 1, "size": 1}, timeout=10)
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
            log.debug(f"Error en Property Appraiser para {clean_owner}: {e}")

# ─────────────────────────────────────────────────────────
# GUARDADO DE DATOS (Contrato records.json)
# ─────────────────────────────────────────────────────────

def save_outputs(records: list[dict], str_from: str, str_to: str):
    with_address = sum(1 for r in records if r.get("prop_address"))
    output = {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "source": "Miami-Dade Public Court Records (Free Feed)",
        "date_range": {"from": str_from, "to": str_to},
        "total": len(records),
        "with_address": with_address,
        "records": records
    }
    
    payload = json.dumps(output, indent=2, default=str)
    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload, encoding="utf-8")

    # CSV para GoHighLevel
    GHL_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(GHL_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["First Name", "Property Address", "Property City", "Property Zip", "Score", "Flags"])
        for r in records:
            writer.writerow([r["owner"], r["prop_address"], r["prop_city"], r["prop_zip"], 85, "Lis Pendens | Pre-foreclosure"])

# ─────────────────────────────────────────────────────────
# ORQUESTRADOR PRINCIPAL
# ─────────────────────────────────────────────────────────

def run():
    today = date.today()
    scraper = MiamiDadeCourtScraper()
    all_leads = []

    log.info("================================────────────────==========")
    log.info("DWD RESCUE — Extrayendo Lis Pendens desde Registros de Corte")
    log.info("================================────────────────==========")

    # Iterar sobre los últimos días buscando registros abiertos paso a paso
    for d in range(LOOKBACK_DAYS):
        target_date = today - timedelta(days=d)
        date_str = target_date.strftime("%m/%d/%Y")
        leads = scraper.fetch_foreclosures(date_str)
        all_leads.extend(leads)

    log.info(f"Casos de Foreclosure crudos encontrados en la corte: {len(all_leads)}")

    if all_leads:
        log.info("Enriqueciendo demandados con el Property Appraiser de Miami-Dade...")
        for i, lead in enumerate(all_leads):
            scraper.enrich_with_pa(lead)
            lead["flags"] = ["Lis pendens", "Pre-foreclosure"]
            lead["score"] = 85 if lead["prop_address"] else 60
            if (i+1) % 10 == 0:
                log.info(f"   Progreso: {i+1}/{len(all_leads)}...")

    # Eliminar duplicados por número de caso de corte
    seen = {}
    for l in all_leads:
        seen[l["doc_num"]] = l
    final_leads = list(seen.values())

    save_outputs(final_leads, (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))
    log.info(f"¡Proceso Terminado! {len(final_leads)} leads subidos de forma gratuita al Dashboard.")

if __name__ == "__main__":
    run()
