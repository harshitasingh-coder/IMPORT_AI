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

## Target markets (first buyer search)

Selected on 2026-10-18 from UN Comtrade data for 2021–2024 (`python -m src.market_shortlist`):

| Market | 2024 imports | India's share | Why |
|---|---|---|---|
| Indonesia | $419M | ~71% | Biggest buyer of Indian groundnuts, growing |
| Viet Nam | $224M (2023) | ~96% (2023) | Almost all imports come from India; India's exports fell in 2024 |
| Netherlands | $624M | ~2% | 2nd-largest importer worldwide, EU hub, India barely present |

The reasons are also stored in `data/product_profile.json` under `target_markets`.

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
  market_shortlist.py    # ranks importing countries -> market_shortlist.csv
  schemas.py             # columns for the company-level buyer datasets
  init_buyer_dataset.py  # creates empty buyer dataset CSVs with headers
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
2. ~~Market shortlist~~ (done: Indonesia, Viet Nam, Netherlands)
3. Buyer-company collection (APEDA leads, shipment sources)
4. Website enrichment, cleaning and deduplication
5. AI classification and ranking
