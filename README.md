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

## Adding buyer leads

APEDA buy leads require an exporter login and APEDA restricts commercial reuse,
so leads are collected by hand and then ingested:

1. Copy `data/templates/buyer_leads_template.csv` into `data/inbox/` (e.g. `apeda_2026-10-18.csv`).
2. Fill one row per lead. `company_name_raw`, `country_raw` and `source_url` are required.
   Copy text exactly as the source shows it.
3. Check it, then add it:

```powershell
python -m src.buyer_discovery ingest data/inbox/apeda_2026-10-18.csv --source APEDA --source-type buyer_lead --collected-by Shubhi --dry-run
python -m src.buyer_discovery ingest data/inbox/apeda_2026-10-18.csv --source APEDA --source-type buyer_lead --collected-by Shubhi
python -m src.buyer_discovery summary
```

Rows outside the target markets or without a groundnut/peanut mention are kept but reported.
Re-ingesting the same file adds nothing. The original file is copied to `data/raw/manual/`.

## End-to-end pipeline

```powershell
python -m src.pipeline run                      # all stages
python -m src.pipeline run --stage discovery    # one stage (profile|market|discovery|extraction|resolution|claims|qualification|scoring|export)
python -m src.pipeline run --from-stage claims  # that stage and everything after
python -m src.pipeline run --offline            # no network: cached/ingested data only
python -m src.pipeline audit                    # integrity checks on the outputs
python -m unittest discover -s tests -t .       # tests
```

### Web app (frontend)

```powershell
python -m src.web            # then open http://127.0.0.1:8000
```

Enter a product profile (or use the saved pilot profile), click Run, and browse the ranked buyers, evidence and
score breakdowns. It binds to localhost only, refuses cross-origin requests, and runs one pipeline at a time.
**No search API key?** Use the "Add companies to check" box in the web app: paste `Company | https://website | Country`
lines (found by hand on Google, directories or trade fairs). They go to `data/inbox/discovery_seeds.csv` and the next run
reads their public pages and builds evidence. (On a host without a persistent disk, the list is lost on restart.)

### Putting the web app online

The app has a login for online use. To accept outside connections you must set `AVYUKT_PASSWORD`
(user name `avyukt`); without it the server refuses to listen on a public address.

```powershell
$env:AVYUKT_PASSWORD="a-long-random-password"; python -m src.web --host 0.0.0.0
```

Hosting platforms: a `Dockerfile` and a Render blueprint (`render.yaml`) are included. Set `AVYUKT_PASSWORD`,
`UN_COMTRADE_API_KEY` and `BRAVE_SEARCH_API_KEY` as secrets in the platform dashboard (never commit them), keep
`/data` on a persistent disk, and serve it over HTTPS (the platform does this). The platform's `PORT` variable is
picked up automatically. The Dockerfile has not been built or tested yet (Docker is not available here).

A static copy of the dashboard is also written to `data/exports/dashboard.html` by every run. Design, rules and limitations: `docs/ARCHITECTURE.md`.
Optional keys in `.env`: `UN_COMTRADE_API_KEY`, `BRAVE_SEARCH_API_KEY`. Extra discovery pages can be listed in
`data/inbox/discovery_seeds.csv` (raw_company_name, source_url, source_type, country, source_name).

## Project layout

```
data/
  product_profile.json   # product, exporter, product forms, candidate HS codes
  templates/             # buyer_leads_template.csv
  inbox/                 # your filled lead files (not committed)
  raw/                   # original API responses and downloads (not committed)
  processed/             # cleaned market data, e.g. market_data.csv (not committed)
  logs/                  # run logs (not committed)
src/
  config.py              # loads .env and prepares data folders
  comtrade_client.py     # UN Comtrade collector -> market_data.csv
  market_shortlist.py    # ranks importing countries -> market_shortlist.csv
  schemas.py             # columns for the company-level buyer datasets
  init_buyer_dataset.py  # creates empty buyer dataset CSVs with headers
  buyer_discovery.py     # ingests hand-collected leads -> raw_importer_records.csv
  main.py                # startup check
  pipeline/              # the 9-stage pipeline (see docs/ARCHITECTURE.md)
  web/                   # local web app: python -m src.web
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
