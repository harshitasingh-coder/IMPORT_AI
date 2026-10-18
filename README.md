# AVYUKT – Importer Data Pipeline

AVYUKT helps Indian exporters find overseas companies that could buy their product.
The first pilot product is **groundnuts from India**.

## Current status

Module 1 (country-level trade statistics from UN Comtrade) is **scaffolded only**.
The API client is not connected yet; `src/main.py` just checks that the project is set up.

## Project layout

```
data/
  product_profile.json   # product, exporter, product forms, candidate HS codes
  raw/                   # original API responses and downloads (not committed)
  processed/             # cleaned market data, e.g. market_data.csv (not committed)
  logs/                  # run logs (not committed)
src/
  config.py              # loads .env and prepares data folders
  main.py                # startup check
tests/
```

## Setup (Windows, PowerShell)

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env      # then add UN_COMTRADE_API_KEY when you have it
python -m src.main
```

If PowerShell blocks activation, use Command Prompt and run `.venv\Scripts\activate.bat`.

Expected output:

```
AVYUKT setup is ready.
Product: Groundnuts
Exporter: India
Broad HS code: 1202
UN Comtrade API: not connected yet.
```

## Roadmap

1. `src/comtrade_client.py` – fetch annual groundnut import statistics, save raw responses, write `data/processed/market_data.csv`
2. Buyer-company collection (APEDA leads, shipment sources)
3. Website enrichment, cleaning and deduplication
4. AI classification and ranking
