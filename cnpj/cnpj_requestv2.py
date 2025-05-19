import re
import csv
import time
import requests
import logging
from pathlib import Path
from typing import List, Dict, Optional, Any
from requests.exceptions import RequestException
from googlesearch import search  

INPUT_CNPJ_FILE        = Path('cnpj.csv')
OUTPUT_DETAILED_FILE   = Path('cnpj_detalhado.csv')
RECEITAWS_BASE_URL     = 'https://www.receitaws.com.br/v1/cnpj/'
MAX_API_RETRIES        = 5
API_REQUEST_TIMEOUT    = 10  # seconds
MIN_INTERVAL_BETWEEN_REQUESTS = 20  # seconds between requests (3 req/min)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_and_clean_cnpjs(file_path: Path) -> List[str]:
    """
    Load CNPJs from a file, remove non-digit characters,
    and pad with leading zeros to 14 digits.
    """
    formatted_cnpjs: List[str] = []
    try:
        with file_path.open('r', encoding='utf-8') as f:
            for line in f:
                num = re.sub(r'\D', '', line.strip())
                if num:
                    formatted_cnpjs.append(num.zfill(14))
        logger.info(f"{len(formatted_cnpjs)} CNPJs loaded.")
    except Exception as e:
        logger.error(f"Error reading '{file_path}': {e}")
    return formatted_cnpjs

def query_receitaws_api(session: requests.Session, cnpj: str) -> Optional[Dict[str, Any]]:
    """
    Query the ReceitaWS API for a given CNPJ with exponential backoff retries.
    """
    url = f"{RECEITAWS_BASE_URL}{cnpj}"
    backoff = 1
    for attempt in range(1, MAX_API_RETRIES + 1):
        try:
            resp = session.get(url, timeout=API_REQUEST_TIMEOUT)
            if resp.status_code == 429:
                wait = backoff * 5
                logger.warning(f"[{cnpj}] HTTP 429 – waiting {wait}s (attempt {attempt})")
                time.sleep(wait)
                backoff *= 2
                continue
            resp.raise_for_status()
            return resp.json()
        except RequestException as e:
            logger.error(f"[{cnpj}] Request error (attempt {attempt}): {e}")
            if attempt < MAX_API_RETRIES:
                time.sleep(backoff * 2)
                backoff *= 2
            else:
                logger.error(f"[{cnpj}] Exceeded max retries.")
    return None

def extract_company_details(api: Dict[str, Any]) -> Dict[str, str]:
    """
    Extract basic company details: name, status, city, state, and Receita phone.
    """
    return {
        'nome_empresa':     api.get('nome', 'N/A'),
        'situacao':         api.get('situacao', 'N/A'),
        'cidade':           api.get('municipio', 'N/A'),
        'estado':           api.get('uf', 'N/A'),
        'telefone_receita': api.get('telefone', 'N/A'),
    }

def get_phone_from_website(url: str) -> Optional[str]:
    """
    Fetch the given URL and attempt to extract a landline phone number via regex.
    """
    try:
        resp = requests.get(url, timeout=API_REQUEST_TIMEOUT)
        resp.raise_for_status()
        # Matches patterns like (xx) xxxx-xxxx or (xx) xxxxx-xxxx
        found = re.findall(r'\(?\d{2}\)?\s?\d{4,5}-\d{4}', resp.text)
        return found[0] if found else None
    except Exception:
        return None

def get_telefone_site_empresa(company_name: str) -> str:
    """
    Search Google for the company's official site and extract a landline number.
    """
    query = f"site official {company_name}"
    try:
        for url in search(query, stop=1, lang='pt'):
            tel = get_phone_from_website(url)
            if tel:
                return tel
    except Exception as e:
        logger.warning(f"Google search error for '{company_name}': {e}")
    return 'N/A'

def process_api_response(cnpj: str, api_data: Optional[Dict[str, Any]]) -> List[Dict[str, str]]:
    """
    Process the API response to extract the list of partners (QSA).
    """
    socios_list: List[Dict[str, str]] = []
    if not api_data or api_data.get('status') == 'ERROR':
        return [{'cnpj': cnpj, 'nome_qsa': 'N/A'}]
    for socio in api_data.get('qsa', []):
        nome = socio.get('nome', '').strip() or 'N/A'
        socios_list.append({'cnpj': cnpj, 'nome_qsa': nome})
    if not socios_list:
        socios_list.append({'cnpj': cnpj, 'nome_qsa': 'N/A'})
    return socios_list

def main():
    """
    Main workflow:
    1. Load and clean CNPJs.
    2. Query ReceitaWS and extract company details.
    3. Build 'owner' field by joining partners with '/'.
    4. Lookup site phone via Google search as fallback.
    5. Write results to detailed CSV.
    """
    cnpjs = load_and_clean_cnpjs(INPUT_CNPJ_FILE)
    if not cnpjs:
        logger.info("No CNPJs to process. Exiting.")
        return

    detailed_results: List[Dict[str, str]] = []
    last_ts = 0.0

    with requests.Session() as sess:
        for idx, cnpj in enumerate(cnpjs, 1):
            # Respect API rate limit: 3 requests per minute
            elapsed = time.monotonic() - last_ts
            if elapsed < MIN_INTERVAL_BETWEEN_REQUESTS:
                time.sleep(MIN_INTERVAL_BETWEEN_REQUESTS - elapsed)

            logger.info(f"[{idx}/{len(cnpjs)}] Processing CNPJ {cnpj}")
            api_data = query_receitaws_api(sess, cnpj)
            last_ts = time.monotonic()

            # Extract basic company details
            detalhes = extract_company_details(api_data or {})

            # Build 'owner' field from QSA partners
            socios = process_api_response(cnpj, api_data)
            nomes_socios = [s['nome_qsa'] for s in socios]
            dono = "/".join(nomes_socios)

            # Google-based phone lookup as fallback
            telefone_site = get_telefone_site_empresa(detalhes['nome_empresa'])

            detailed_results.append({
                **detalhes,
                'cnpj': cnpj,
                'dono': dono,
                'telefone_site': telefone_site,
            })

    # Write detailed CSV
    fieldnames = [
        'nome_empresa',
        'situacao',
        'cidade',
        'estado',
        'cnpj',
        'dono',
        'telefone_site',
        'telefone_receita'
    ]
    try:
        with OUTPUT_DETAILED_FILE.open('w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(detailed_results)
        logger.info(f"Detailed CSV saved to '{OUTPUT_DETAILED_FILE}'.")
    except Exception as e:
        logger.error(f"Error saving '{OUTPUT_DETAILED_FILE}': {e}")

if __name__ == '__main__':
    main()
