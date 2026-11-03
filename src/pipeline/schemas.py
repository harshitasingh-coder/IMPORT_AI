"""Column definitions for every pipeline output (single source of truth)."""

MARKET_ANALYSIS_FIELDS = [
    "market_rank", "country", "iso3", "is_target_market", "exporting_country", "hs_codes",
    "trade_flow", "years_reported", "first_year", "latest_year", "latest_import_value_usd",
    "latest_import_quantity_kg", "avg_import_value_usd", "growth_pct_first_to_latest",
    "trend", "import_history_usd", "india_export_latest_usd", "india_share_latest_pct",
    "india_presence", "market_relevance_score", "relevance_explanation",
    "data_source", "retrieved_at", "data_age_days", "freshness_status",
]

CANDIDATE_FIELDS = [
    "candidate_id", "run_id", "raw_company_name", "source_url", "source_type", "source_domain",
    "source_name", "country_raw", "country_hint", "country_basis", "target_market",
    "discovery_method", "discovery_query", "discovered_at", "page_title", "snippet",
    "initial_relevance_reason", "evidence_level", "source_record_id", "hs_code",
    "shipment_date", "origin_country", "quantity_raw", "buying_requirement_raw",
    "lead_posted_date", "lead_status", "website_raw", "product_description_raw",
]

REJECT_FIELDS = ["run_id", "raw_company_name", "source_url", "source_name", "reason", "rejected_at"]

COMPANY_FIELDS = [
    "company_id", "id_basis", "company_name", "legal_suffix", "normalized_name", "domain",
    "country_iso3", "country_source", "website", "aliases", "candidate_ids", "source_record_ids",
    "dedup_status", "dedup_notes", "manual_merge_applied",
]

DUPLICATE_FIELDS = ["company_id_a", "company_id_b", "reason", "similarity", "status", "suggested_at"]

EVIDENCE_FIELDS = [
    "evidence_id", "company_id", "source_url", "source_type", "source_authority_tier",
    "source_authority_label", "page_title", "exact_text", "text_kind", "retrieved_at",
    "publication_date", "snapshot_path", "extraction_method", "candidate_id",
]

CLAIM_FIELDS = [
    "claim_id", "company_id", "claim_type", "claim_status", "source_url", "source_type",
    "page_title", "exact_text", "retrieved_at", "publication_date", "confidence_level",
    "review_status", "limitations", "evidence_ids", "source_count", "corroboration",
    "source_authority_tier", "assessed_by",
]

QUALIFICATION_FIELDS = [
    "company_id", "company_name", "country_iso3", "target_market", "business_type",
    "auto_classification", "final_classification", "override_applied", "override_note",
    "classification_rule", "classification_reason", "product_relevance", "buyer_role_evidence",
    "historical_import_activity", "india_origin_evidence", "current_buying_intent",
    "legal_identity_status", "contact_availability", "supporting_claim_ids",
    "supporting_evidence_ids", "unresolved_issues", "recommended_by", "qualification_status",
    "classified_at",
]

CHANGE_FIELDS = ["changed_at", "run_id", "company_id", "previous_classification", "new_classification", "reason"]

SCORE_COMPONENTS = [
    ("product_relevance", 25), ("buyer_role", 25), ("evidence_quality", 20),
    ("market_fit", 10), ("india_origin", 10), ("contactability", 10),
]

RANKED_FIELDS = (
    ["rank", "company_id", "company_name", "country_iso3", "target_market", "business_type",
     "final_classification", "total_score", "priority_band"]
    + [f"score_{n}" for n, _ in SCORE_COMPONENTS]
    + [f"why_{n}" for n, _ in SCORE_COMPONENTS]
    + ["website", "public_email", "public_phone", "public_address", "classification_reason",
       "evidence_ids", "oldest_evidence_retrieved_at", "newest_evidence_retrieved_at",
       "evidence_age_days", "unresolved_issues", "score_version", "scored_at"]
)

COMPANY_PROFILE_EXPORT_FIELDS = [
    "company_id", "company_name", "legal_suffix", "domain", "website", "country_iso3",
    "business_type", "business_description", "product_categories", "groundnut_relevance",
    "role_terms", "public_email", "public_phone", "public_address", "contact_page",
    "about_page", "product_page", "extraction_status", "missing_fields", "dedup_status",
    "aliases", "source_url", "page_title", "retrieved_at", "publication_date",
]
