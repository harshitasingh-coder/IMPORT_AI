# AVYUKT pipeline: repository audit and architecture (2026-11-03)

## 1. Audit of the repository as found
| Area | Found | Decision |
|---|---|---|
| UN Comtrade (`src/comtrade_client.py`) | Working collector -> `market_data.csv` | **Reused** (`collect_year`, `to_rows`, `CSV_FIELDS`); unchanged |
| Market shortlist (`src/market_shortlist.py`) | Country ranking | Left unchanged; superseded by stage 2 outputs (old file is archived, not lost) |
| Lead ingest (`src/buyer_discovery.py`, `src/schemas.py`) | Hand-collected lead ingest -> `raw_importer_records.csv` | **Reused** as discovery route 1; unchanged |
| Config (`src/config.py`) | paths, key | Modified: + `BRAVE_SEARCH_API_KEY` |
| Product profile | `data/product_profile.json` | Kept untouched; stage 1 writes normalized copy to `data/processed/` |
| Website enrichment, contacts, company resolution, evidence/claims, classification, scoring, dashboard | **Did not exist** | Built (stages 3-9) |
| Tests | none (`tests/.gitkeep`) | 71 tests added |
| Research data / manual decisions | **Not in this checkout**: `data/raw`, `data/processed`, `data/inbox` are gitignored and empty here | Pipeline is built to preserve them (see below); nothing could be migrated or verified against real data |

## 2. Pipeline (`python -m src.pipeline run`)
`profile -> market -> discovery -> extraction -> resolution -> claims -> qualification -> scoring -> export`
Each stage: reads files written by the previous stage, validates, logs to `data/logs/pipeline_audit.jsonl`,
returns a status (`completed | completed_with_warnings | failed | skipped`). A crashing stage is recorded and
later stages still run; only a failed profile stage skips everything. Re-running one stage uses saved files.

Outputs (`data/processed/`): product_profile.json, market_analysis.csv, market_shortlist.csv,
company_candidates_raw.csv, discovery_rejects.csv, company_profiles.json, companies.csv,
duplicate_suggestions.csv, record_company_map.csv, evidence_log.csv, company_claims.csv,
buyer_qualification.csv, classification_changes.csv, ranked_buyers.csv, pipeline_run_summary.json.
Exports (`data/exports/`): ranked_buyers.csv, company_profiles.csv, evidence_log.csv, market_analysis.csv,
pipeline_run_summary.json, dashboard.html (self-contained), dashboard_data.json.

## 3. Preservation and decision files
* Derived outputs are never overwritten silently: a differing previous version is copied to `data/processed/archive/`.
* Inputs (`product_profile.json`, `raw_importer_records.csv`, `market_data.csv`, `raw/manual/`) are never rewritten
  (market_data.csv is only merged-by-year when the API succeeds, with the old version archived).
* Human decisions live in `data/manual/` and are read-only for code:
  `company_merge_decisions.csv` (company_id_a, company_id_b, decision merge|keep_separate, decided_by, decided_at, note) and
  `qualification_overrides.csv` (company_id, classification, decided_by, decided_at, note).
  An override wins in `final_classification`; the automatic result stays in `auto_classification`.
* Any classification change between runs is logged to `classification_changes.csv` and surfaced as a warning.

## 4. Key rules
Claim statuses supported / contradicted / not_supported / unknown; `not_supported` means "examined, nothing found"
and is never negative evidence. Source tier (1-6) and confidence (high/medium/low) are separate columns.
Snippets only support product relevance and importer role, at low confidence. Only a dated (<=180 days), open
buyer lead supports `current_buying_requirement`. Scores: see `src/pipeline/scoring.py` docstring.

## 5. Known limitations / risks
* Extraction and claim assessment are rule-based (keyword/sentence patterns, EN/ID/NL/VI vocab); no LLM is used.
  Misses are likely on image-only or JS-rendered sites; negation handling is simple.
* Discovery needs `BRAVE_SEARCH_API_KEY`; the Brave adapter is implemented but was only tested against a fake.
* Trade data needs `UN_COMTRADE_API_KEY`; otherwise stage 2 reports "unavailable".
* Domain normalization uses a heuristic (no public-suffix list); a shared-hosting list prevents merging free-site subdomains.
* Source tiers for hand-collected records (buyer_lead 4, shipment 5, manual company_website 6) are assumptions.
* No registry/legal verification; HS codes stay `needs_verification`.
* The previous repo had no frontend; the dashboard is a new static page, not a redesign.
