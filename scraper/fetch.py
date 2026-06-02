"""
Deals With Dignity — Miami-Dade County Motivated Seller Lead Scraper
API-BASED RESCUE VERSION (No Playwright / No Cloudflare Blocks)
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
# CONFIG DIRECTA A APIS
# ─────────────────────────────────────────────────────────
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", 7))

# URLs de los Endpoints de API reales que alimentan la App de Miami-Dade
CLERK_API_URL = "https://onlineservices.miamidadeclerk.gov/officialrecords/api/Search/StandardSearch"
PA_SEARCH_URL = "https://apps.miamidadepa.gov/PropertySearch/api/Search"

OUTPUT_PATHS = [
    Path("dashboard/records.json"),
    Path("data/records.json"),
]
GHL_CSV_PATH = Path("data/ghl_export.csv")

DOC_TYPE_MAP = {
    "LP":       ("foreclosure",   "Lis Pendens"),
    "NOFC":     ("foreclosure",   "Notice of Foreclosure"),
    "TAXDEED":  ("tax",           "Tax Deed"),
    "JUD":      ("judgment",      "Judgment"),
    "CCJ":      ("judgment",      "Certified Judgment"),
    "LNIRS":    ("lien",          "IRS Lien"),
    "LNFED":    ("lien",          "Federal Lien"),
    "LN":       ("lien",          "Lien"),
    "LNMECH":   ("lien",          "Mechanic's Lien"),
    "LNHOA":    ("lien",          "HOA Lien"),
    "PRO":      ("probate",       "Probate"),
}

TARGET_DOC_TYPES = list(DOC_TYPE_MAP.keys())

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("dwd_api_rescue")


# ─────────────────────────────────────────────────────────
# ENGINE DE EXTRACCIÓN MEDIANTE API (Rápido y Seguro)
# ─────────────────────────────────────────────────────────

class MiamiDadeAPIClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=UTF-8",
            "Origin": "https://onlineservices.miamidadeclerk.gov",
            "Referer": "https://onlineservices.miamidadeclerk.gov/officialrecords"
        })

    def fetch_clerk_records(self, doc_type: str, date_from: str, date_to: str) -> list[dict]:
        """Consulta el API del Clerk simulando una búsqueda avanzada del portal."""
        records = []
        log.info(f"Pidiendo API Clerk para {doc_type} del {date_from} al {date_to}...")
        
        # Payload estructurado de la base de datos de Miami-Dade
        payload = {
            "SearchType": "STANDARD",
            "DocType": doc_type,
            "DateFrom": date_from, # MM/DD/YYYY
            "DateTo": date_to,     # MM/DD/YYYY
            "PageNumber": 1,
            "PageSize": 100
        }

        try:
            response = self.session.post(CLERK_API_URL, json=payload, timeout=20)
            if response.status_code != 200:
                log.error(f"Error API Clerk ({response.status_code}) para {doc_type}")
                return records

            data = response.json()
            # Estructura típica nativa: { "Results": [...], "TotalRows": X }
            results = data.get("Results") or data.get("Items") or []
            
            for item in results:
                doc_num = item.get("ClerkFileNumber") or item.get("DocNum") or item.get("ID")
                if not doc_num:
                    continue

                # Capturar nombres nativos de Florida
                grantor = item.get("GrantorName") or item.get("From") or ""
                grantee = item.get("GranteeName") or item.get("To") or ""
                
                # Regla Crítica: Si es Lis Pendens, el demandado/dueño es el Grantee
                owner_name = grantee if doc_type == "LP" else grantor

                filed_raw = item.get("RecordingDate") or item.get("FiledDate") or ""
                
                # Limpieza de montos monetarios
                try:
                    amount = float(str(item.get("Amount") or 0).replace("$", "").replace(",", ""))
                except:
                    amount = 0.0

                record = {
                    "doc_num": str(doc_num).strip(),
                    "doc_type": doc_type,
                    "filed": self._parse_date(filed_raw),
                    "owner": str(owner_name).strip().upper(),
                    "grantee": str(grantee).strip().upper(),
                    "legal": item.get("LegalDescription") or "",
                    "amount": amount,
                    "clerk_url": f"https://onlineservices.miamidadeclerk.gov/officialrecords/BookPageSummary?comDocId={doc_num}",
                    "prop_address": "", "prop_city": "", "prop_state": "FL", "prop_zip": "",
                    "mail_address": "", "mail_city": "", "mail_state": "FL", "mail_zip": ""
                }
                cat, cat_label = DOC_TYPE_MAP.get(doc_type, ("other", doc_type))
                record["cat"] = cat
                record["cat_label"] = cat_label
                records.append(record)

        except Exception as e:
            log.error(f"Fallo de conexión con API Clerk para {doc_type}: {e}")
        
        return records

    def enrich_with_pa(self, record: dict):
        """Consulta la API del Property Appraiser usando el nombre extraído."""
        owner = record.get("owner", "").strip()
        if not owner or len(owner) < 3:
            return

        try:
            # Endpoint público sin bloqueo perimetral de Cloudflare para consultas rápidas
            resp = self.session.get(PA_SEARCH_URL, params={"q": owner, "s": "ownername", "p": 1, "size": 1}, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                results = data.get("MinimumResults") or data.get("Results") or []
                if results:
                    hit = results[0]
                    record["prop_address"] = (hit.get("SiteAddress") or "").strip()
                    record["prop_city"] = (hit.get("SiteCity") or "").strip()
                    record["prop_zip"] = (hit.get("SiteZip") or "").strip()
                    record["mail_address"] = (hit.get("MailingAddress1") or "").strip()
                    record["mail_city"] = (hit.get("MailingCity") or "").strip()
                    record["mail_zip"] = (hit.get("MailingZip") or "").strip()
        except Exception as e:
            log.debug(f"API PA Error para {owner}: {e}")

    def _parse_date(self, raw: str) -> str:
        if not raw: return ""
        for fmt in ["%Y-%m-%dT%H:%M:%S", "%m/%d/%Y", "%Y-%m-%d"]:
            try: return datetime.strptime(raw.split(".")[0], fmt).strftime("%Y-%m-%d")
            except: continue
        return raw

# ─────────────────────────────────────────────────────────
# LOGICA DE FORMATOS (Sincronizado con GHL y Dashboards)
# ─────────────────────────────────────────────────────────

def compute_flags(record: dict) -> list[str]:
    flags = []
    owner = record.get("owner", "")
    if record.get("doc_type") == "LP": flags.append("Lis pendens")
    if any(kw in owner for kw in ["LLC", "CORP", "INC", "TRUST"]): flags.append("LLC / corp owner")
    return flags

def compute_score(record: dict, flags: list[str]) -> int:
    score = 40
    score += len(flags) * 15
    if record.get("amount", 0) > 50000: score += 15
    if record.get("prop_address"): score += 10
    return min(score, 100)

def save_outputs(records: list[dict], date_from: str, date_to: str):
    with_address = sum(1 for r in records if r.get("prop_address"))
    output = {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "source": "Miami-Dade API Rescued Portal",
        "date_range": {"from": date_from, "to": date_to},
        "total": len(records),
        "with_address": with_address,
        "records": records
    }
    
    payload = json.dumps(output, indent=2, default=str)
    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload, encoding="utf-8")

    # Guardar CSV listo para GoHighLevel
    GHL_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(GHL_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["First Name", "Last Name", "Property Address", "Property City", "Property Zip", "Score", "Flags"])
        for r in records:
            writer.writerow([r["owner"], "", r["prop_address"], r["prop_city"], r["prop_zip"], r["score"], "|".join(r["flags"])])

# ─────────────────────────────────────────────────────────
# RUN
# ─────────────────────────────────────────────────────────

def run():
    today = date.today()
    date_from_dt = today - timedelta(days=LOOKBACK_DAYS)
    
    # Formato de API pura estadounidense: MM/DD/YYYY
    str_from = date_from_dt.strftime("%m/%d/%Y")
    str_to = today.strftime("%m/%d/%Y")

    log.info(f"Iniciando Rescate de API para Miami-Dade: {str_from} -> {str_to}")
    
    client = MiamiDadeAPIClient()
    raw_collected = []

    for doc_type in TARGET_DOC_TYPES:
        records = client.fetch_clerk_records(doc_type, str_from, str_to)
        raw_collected.extend(records)
    
    log.info(f"Registros encontrados en Clerk: {len(raw_collected)}")

    # Cruzar con Property Appraiser de inmediato sin bloqueos de navegador
    if raw_collected:
        log.info("Enriqueciendo datos mediante API directa del Property Appraiser...")
        for i, r in enumerate(raw_collected):
            client.enrich_with_pa(r)
            flags = compute_flags(r)
            r["flags"] = flags
            r["score"] = compute_score(r, flags)
            if (i+1) % 20 == 0: log.info(f"   Procesados {i+1}/{len(raw_collected)}...")

    # Remover duplicados
    seen = {}
    for r in raw_collected:
        seen[r["doc_num"]] = r
    final_records = list(seen.values())

    save_outputs(final_records, date_from_dt.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))
    log.info(f"¡Éxito total! Mapeados {len(final_records)} leads listos en tu panel público.")

if __name__ == "__main__":
    run()
