# AVYUKT – Importer Data Pipeline

AVYUKT helps Indian exporters find overseas companies that could buy their product.
The first pilot product is **groundnuts from India**.

## Current status

Module 1 (country-level trade statistics from UN Comtrade) is working.

```powershell
python -m src.comtrade_client                 # years from product_profile.json
python -m src.comtrade_client --years 2023 2024
```

For each year it collects (a) every country's groundnut imports from the World and
(b) India's groundnut exports to each partner, for the HS codes in `product_profile.json`.
Raw responses go to `data/raw/`, and the combined table to `data/processed/market_data.csv`.
The most recent year is usually incomplete because countries report with a delay.

## Project layout

```
data/
  product_profile.json   # product, exporter, product forms, candidate HS codes
  raw/                   # original API responses and downloads (not committed)
  processed/             # cleaned market data, e.g. market_data.csv (not committed)
  logs/                  # run logs (not committed)
src/
  config.py              # loads .env and prepares data folders
  comtrade_client.py     # UN Comtrade collector -> market_data.csv
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

1. ~~UN Comtrade market data~~ (done)
2. Buyer-company collection (APEDA leads, shipment sources)
3. Website enrichment, cleaning and deduplication
4. AI classification and ranking
