# Data Processing for TETO

This directory will host a collection of scripts and processes related to the NGO **TETO** and its operations. These scripts are designed to support various data extraction, transformation, and analysis tasks pertinent to **TETO**'s work.

## Current Processes:

### 1. CNPJ QSA (Quadro de Sócios e Administradores) Extraction

* **Script:** `cnpj_request.py` (or the name you saved the Python script as)
* **Purpose:** This script is the first process in this collection. It takes a list of Brazilian company registration numbers (CNPJs) from an input CSV file (`cnpj.csv`), queries an external API (ReceitaWS) to fetch company details, and specifically extracts information about the partners and administrators (QSA - Quadro de Sócios e Administradores). This can be useful for due diligence, partnership analysis, or other activities relevant to **TETO**.
* **Input:** A CSV file named `cnpj.csv` located in the same directory, with one CNPJ per line.
* **Output:** A CSV file named `qsa_resultados.csv` containing the CNPJ, the name of each partner/administrator (`nome_qsa`), and their qualification (`qualificacao_qsa`). If no QSA information is found or an error occurs for a CNPJ, "N/A" is used for the partner details.
* **Dependencies:**
    * Python 3.x
    * `requests` library (`pip install requests`)
* **Usage:**
    1.  Ensure Python 3 and the `requests` library are installed.
    2.  Create a `cnpj.csv` file in this directory with the CNPJs you want to query.
    3.  Run the script from your terminal: `python cnpj_request.py`
* **Important Notes:**
    * The script respects the rate limits of the free tier of the ReceitaWS API (approximately 3 requests per minute) by including a `MIN_INTERVAL_BETWEEN_REQUESTS`.
    * Logging (in Portuguese, as per the script's design) provides information about the script's progress and any errors encountered.

---

As more processes related to **TETO** are developed, they will be added to this directory with corresponding explanations.
