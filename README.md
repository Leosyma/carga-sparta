# ⚡ SPARTA and PERSAS Data Loading and Automation
## 📌 Detailed Description

### 🔍 Objective
This project aims to consolidate, transform, and insert data from multiple regulatory data sources (e.g., SPARTA, PERSAS) into an Oracle database, enabling structured analysis and regulatory monitoring for electricity distribution companies.

---

### 📂 Structure

- **Web Scraping**  
  - `webscraping_SPARTA_20240131.py`  
    - Automates data retrieval from ANEEL's tariff system (SPARTA).
  - `webscraping_PERSAS_20231005.py`  
    - Extracts PERSAS-related documents by simulating user interaction on the ANEEL portal.

- **Data Extraction (SPARTA)**
  - Scripts prefixed with `Extrator_` load Excel files and extract information for each regulatory component:
    - `Extrator_CVA`, `Extrator_DRA`, `Extrator_DRP`, `Extrator_Energia`, `Extrator_FatorX`, `Extrator_Financeiro`, `Extrator_Indices`, `Extrator_Mercado`, `Extrator_NUC`, `Extrator_Receita`, `Extrator_Subsidios`, `Extrator_VPB`, `Extrator_Capa`

- **Data Extraction (PERSAS)**
  - Updated versions of SPARTA scripts for permissionárias:
    - `Extrator_Financeiros_20231010.py`, `Extrator_Indices_20231010.py`, `Extrator_Mercado_20231010.py`, etc.
  - `Extrator_Suprimento_20231010.py` handles multi-supplier data for each distributor and energy source.

- **SQL Scripts**
  - `Tabelas SPARTA.sql`: Oracle table creation for SPARTA data.
  - `Tabelas PERSAS.sql`: Oracle table creation for PERSAS data.

---

### 🛠️ Functionalities

- **Automatic Download** of regulatory data from ANEEL via web scraping.
- **Flexible Excel Parsing** for handling multiple layouts and structures.
- **Oracle Integration** using `cx_Oracle` for efficient data ingestion.
- **Data Consolidation** from various regulatory instruments into a single structured environment.
- **Error Handling and Logging** for robust execution in production environments.

---

### 🗃️ Input Data

- Downloaded `.xlsx` files for SPARTA and PERSAS from ANEEL’s regulatory portal.
- Raw files stored locally and processed in batch.

---

### 💾 Output

- Structured tables in Oracle containing:
  - Energy tariffs, subsidies, financials, CVA/DRA/DRP costs, Fator X, and more.

---

### 🧰 Libraries Used

- `pandas`, `numpy`, `os`, `glob`
- `selenium`, `BeautifulSoup`, `urllib`, `requests`
- `cx_Oracle`, `keyring`


