import re
import csv
import time
import requests
import logging
from pathlib import Path
from typing import List, Dict, Optional, Any
from requests.exceptions import RequestException


INPUT_CNPJ_FILE = Path('cnpj.csv')
OUTPUT_QSA_FILE = Path('qsa_resultados.csv')
RECEITAWS_BASE_URL = 'https://www.receitaws.com.br/v1/cnpj/'
MAX_API_RETRIES = 5
API_REQUEST_TIMEOUT = 10 # seconds

# The ReceitaWS free API allows up to 3 requests per minute.
# This means 1 request every 20 seconds.

MIN_INTERVAL_BETWEEN_REQUESTS = 20 # seconds


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_and_clean_cnpjs(file_path: Path) -> List[str]:
    """
    Loads CNPJs from a file, removes non-numeric characters,
    and pads with leading zeros to complete 14 digits.

    """
    formatted_cnpjs: List[str] = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_content in f:
                clean_cnpj_line = line_content.strip()
                if clean_cnpj_line: 
                    numeric_cnpj = re.sub(r'\D', '', clean_cnpj_line)
                    formatted_cnpjs.append(numeric_cnpj.zfill(14))
        logger.info(f"{len(formatted_cnpjs)} CNPJs carregados de '{file_path}'.")
    except FileNotFoundError:
        logger.error(f"Arquivo de CNPJs '{file_path}' não encontrado.")
    except Exception as e:
        logger.error(f"Erro ao carregar CNPJs de '{file_path}': {e}")
    return formatted_cnpjs

def query_receitaws_api(session: requests.Session, cnpj: str) -> Optional[Dict[str, Any]]:
    """
    Queries a CNPJ on the ReceitaWS API with retries and exponential backoff.

    """
    url = f'{RECEITAWS_BASE_URL}{cnpj}'
    backoff_factor = 1
    for attempt_number in range(1, MAX_API_RETRIES + 1):
        try:
            response = session.get(url, timeout=API_REQUEST_TIMEOUT)

            if response.status_code == 429: # Too Many Requests
                sleep_duration = backoff_factor * 5
                logger.warning(
                    f"[{cnpj}] HTTP 429 (Too Many Requests). "
                    f"Esperando {sleep_duration}s (tentativa {attempt_number}/{MAX_API_RETRIES})."
                )
                time.sleep(sleep_duration)
                backoff_factor *= 2
                continue 

            response.raise_for_status() # Raises HTTPError for 4xx/5xx codes not handled above
            return response.json()

        except RequestException as e:
            logger.error(f"[{cnpj}] Erro na requisição (tentativa {attempt_number}/{MAX_API_RETRIES}): {e}")
            if attempt_number < MAX_API_RETRIES:
                sleep_duration = backoff_factor * 2
                logger.info(f"[{cnpj}] Esperando {sleep_duration}s antes da próxima tentativa.")
                time.sleep(sleep_duration)
                backoff_factor *= 2
            else:
                logger.error(f"[{cnpj}] Máximo de tentativas atingido. Desistindo.")
                return None
    return None

def process_api_response(cnpj: str, api_data: Optional[Dict[str, Any]]) -> List[Dict[str, str]]:
    """
    Processes the data returned by the API to extract QSA information.

    """
    qsa_results: List[Dict[str, str]] = []

    if not api_data or api_data.get('status') == 'ERROR':
        error_message = api_data.get('message', 'Status ERROR') if api_data else 'Sem dados'
        logger.warning(f"[{cnpj}] Consulta retornou erro ou sem dados válidos: {error_message}")
        qsa_results.append({'cnpj': cnpj, 'nome_qsa': 'N/A', 'qualificacao_qsa': 'N/A'})
        return qsa_results

    qsa_list_from_api = api_data.get('qsa', [])
    if qsa_list_from_api:
        for partner_info in qsa_list_from_api:
            partner_name = partner_info.get('nome', '').strip()
            partner_qualification = partner_info.get('qual', '').strip()
            qsa_results.append({
                'cnpj': cnpj,
                'nome_qsa': partner_name if partner_name else 'N/A',
                'qualificacao_qsa': partner_qualification if partner_qualification else 'N/A'
            })
    else:
        logger.info(f"[{cnpj}] Não foram encontradas informações de QSA.")
        qsa_results.append({'cnpj': cnpj, 'nome_qsa': 'N/A', 'qualificacao_qsa': 'N/A'})

    return qsa_results


def main():
    """
     Main function to load CNPJs, query ReceitaWS, and save QSA results.
    
    """
    cnpjs_to_process = load_and_clean_cnpjs(INPUT_CNPJ_FILE)
    if not cnpjs_to_process:
        logger.info("Nenhum CNPJ para processar. Encerrando.")
        return

    all_qsa_results: List[Dict[str, str]] = []
    last_request_timestamp = 0.0

    with requests.Session() as http_session:
        for index, cnpj_value in enumerate(cnpjs_to_process):
            logger.info(f"Processando CNPJ {index+1}/{len(cnpjs_to_process)}: {cnpj_value}")
            current_timestamp = time.monotonic()
            time_since_last_request = current_timestamp - last_request_timestamp
            if time_since_last_request < MIN_INTERVAL_BETWEEN_REQUESTS:
                wait_duration = MIN_INTERVAL_BETWEEN_REQUESTS - time_since_last_request
                logger.info(f"Aguardando {wait_duration:.2f}s para respeitar o limite da API.")
                time.sleep(wait_duration)

            last_request_timestamp = time.monotonic() 
            api_response_data = query_receitaws_api(http_session, cnpj_value)

            current_cnpj_qsa_results = process_api_response(cnpj_value, api_response_data)
            all_qsa_results.extend(current_cnpj_qsa_results)


    if not all_qsa_results:
        logger.info("Nenhum resultado de QSA foi obtido.")
        return

    try:
        with open(OUTPUT_QSA_FILE, 'w', newline='', encoding='utf-8') as output_csv_file:
            # Adjust field names based on what 'process_api_response' returns
            csv_field_names = ['cnpj', 'nome_qsa', 'qualificacao_qsa']
            csv_dict_writer = csv.DictWriter(output_csv_file, fieldnames=csv_field_names)
            csv_dict_writer.writeheader()
            csv_dict_writer.writerows(all_qsa_results)
        logger.info(f"Resultados do QSA salvos em '{OUTPUT_QSA_FILE}'.")
    except IOError as e:
        logger.error(f"Erro ao salvar o arquivo CSV '{OUTPUT_QSA_FILE}': {e}")

    logger.info("Processamento concluído.")

if __name__ == '__main__':
    main()