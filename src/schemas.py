"""Column definitions for the company-level buyer datasets.

raw_importer_records.csv   every record exactly as a source returned it
record_company_map.csv     links each raw record to one AVYUKT company
verified_importers.csv     one clean, checked row per company
"""

RAW_IMPORTER_FIELDS = [
    "source_record_id",         # ID from the source, or our own if none (e.g. apeda-0001)
    "source_name",              # APEDA, Volza, ImportYeti, trade fair, embassy list, ...
    "source_type",              # buyer_lead | shipment_record | directory_listing | company_website
    "target_market",            # ISO3 of the market this record was collected for
    "company_name_raw",
    "country_raw",
    "product_description_raw",
    "hs_code",
    "shipment_date",
    "origin_country",
    "quantity_raw",
    "buying_requirement_raw",
    "lead_posted_date",
    "lead_status",
    "website_raw",
    "source_url",
    "collected_at",
    "collected_by",             # person or script that collected it
]

RECORD_COMPANY_MAP_FIELDS = [
    "source_record_id",
    "company_id",
    "match_method",             # same_domain | same_registration_id | manual_review
]

VERIFIED_IMPORTER_FIELDS = [
    "company_id",
    "company_name",
    "country_iso3",
    "website",
    "products",
    "buyer_type",
    "shipment_evidence",
    "active_buying_requirement",    # yes | no | unknown
    "verification_status",          # verified | partially_verified | unverified
    "evidence_urls",
    "evidence_snippets",
    "label",                        # one of BUYER_LABELS, set during manual review
    "last_verified_at",
]

BUYER_LABELS = [
    "active_buyer_lead",
    "historical_importer",
    "potential_buyer",
    "supplier_or_competitor",
    "irrelevant",
]

DATASETS = {
    "raw_importer_records.csv": RAW_IMPORTER_FIELDS,
    "record_company_map.csv": RECORD_COMPANY_MAP_FIELDS,
    "verified_importers.csv": VERIFIED_IMPORTER_FIELDS,
}
