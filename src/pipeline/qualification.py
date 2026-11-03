"""STAGE 7 - Buyer qualification (transparent rules, evidence-linked).

Classes: potential_buyer | relevant_business | weak_lead | excluded | undetermined
Rules (rules_v1), first match wins:
  excluded          company located in India (supplier/competitor base, not an overseas buyer)
  potential_buyer   product evidence (non-snippet) AND buyer evidence (imports raw groundnuts, historical
                    import record, or a dated open buyer lead) AND no contradicted import claim
  relevant_business product evidence without buyer evidence (seller/processor/distributor, or conflicting)
  weak_lead         only snippet-level product/role evidence, role evidence without product evidence,
                    or prepared/roasted-only peanut products
  undetermined      no usable evidence
Dimensions are stored separately and never merged into one flag. Nobody is promoted to
'verified buyer': qualification_status is always 'unreviewed'. A human override in
data/manual/qualification_overrides.csv wins and is stored next to the automatic result.
"""

from __future__ import annotations

from collections import defaultdict

from src.pipeline.common import (Context, StageResult, STATUS_COMPLETED, STATUS_WARNINGS, iso,
                                 read_csv, read_json, write_csv)
from src.pipeline.schemas import CHANGE_FIELDS, QUALIFICATION_FIELDS

CLASSES = ["potential_buyer", "relevant_business", "weak_lead", "excluded", "undetermined"]


def _usable(claim):
    """A claim only counts when supported AND linked to evidence."""
    return bool(claim and claim["claim_status"] == "supported" and claim["evidence_ids"])


def _tier(claim):
    try:
        return int(claim["source_authority_tier"])
    except (TypeError, ValueError):
        return 6


def classify(company, claims, prof):
    """-> dict with classification, rule, reason, dimensions, issues, supporting ids."""
    c = {t: claims.get(t) for t in (
        "product_relevance", "sells_groundnuts", "processes_groundnuts", "imports_raw_groundnuts",
        "imports_from_india", "historical_import_activity", "current_buying_requirement",
        "importer_or_distributor_role", "company_identity", "public_contact_information")}
    prod_types = ("product_relevance", "sells_groundnuts", "processes_groundnuts", "imports_raw_groundnuts")
    prod_all = [t for t in prod_types if _usable(c[t])]
    prod_strong = [t for t in prod_all if _tier(c[t]) <= 5]
    buyer_types = [t for t in ("imports_raw_groundnuts", "historical_import_activity", "current_buying_requirement")
                   if _usable(c[t]) and _tier(c[t]) <= 5]
    conflict = bool(c["imports_raw_groundnuts"] and c["imports_raw_groundnuts"]["claim_status"] == "contradicted")
    role = _usable(c["importer_or_distributor_role"])
    prepared_only = bool(c["product_relevance"] and c["product_relevance"]["claim_status"] == "not_supported"
                         and "prepared/roasted" in c["product_relevance"]["limitations"])
    iso3 = company["country_iso3"]

    dims = {
        "product_relevance": ("supported" if prod_strong else "lead_only" if prod_all
                              else "prepared_only" if prepared_only else "unknown"),
        "buyer_role_evidence": ("imports_raw_groundnuts" if _usable(c["imports_raw_groundnuts"]) else
                                "importer_or_distributor_role" if role else
                                "seller_or_processor_only" if (_usable(c["sells_groundnuts"]) or _usable(c["processes_groundnuts"]))
                                else "none"),
        "historical_import_activity": "supported" if _usable(c["historical_import_activity"]) else "unknown",
        "india_origin_evidence": "explicit" if _usable(c["imports_from_india"]) else "unknown",
        "current_buying_intent": "supported_lead" if _usable(c["current_buying_requirement"]) else "unknown",
        "legal_identity_status": "registry_not_verified" if _usable(c["company_identity"]) else "unknown",
    }
    channels = []
    if _usable(c["public_contact_information"]) and prof:
        if prof.get("public_emails"): channels.append("email")
        if prof.get("public_phones"): channels.append("phone")
        if prof.get("public_address"): channels.append("address")
    dims["contact_availability"] = ";".join(channels) or "none"

    if iso3 == "IND":
        cls, rule = "excluded", "R0_located_in_india"
        reason = "Company is located in India: part of the supplier/competitor base, not an overseas buyer."
    elif prod_strong and buyer_types and not conflict:
        cls, rule = "potential_buyer", "R1_product_and_buyer_evidence"
        bits = []
        if "imports_raw_groundnuts" in buyer_types: bits.append("imports raw groundnuts")
        if "historical_import_activity" in buyer_types: bits.append("historical import record (not proof of current need)")
        if "current_buying_requirement" in buyer_types: bits.append("dated open buyer lead")
        reason = f"Product evidence ({', '.join(prod_strong)}) plus buyer evidence: {'; '.join(bits)}."
    elif prod_strong:
        cls, rule = "relevant_business", "R2_product_evidence_without_buyer_evidence"
        reason = (f"Product evidence ({', '.join(prod_strong)}) but "
                  + ("a contradicted import claim blocks buyer status." if conflict else
                     "no evidence that the company imports/buys raw groundnuts."))
    elif prod_all or role or prepared_only:
        cls, rule = "weak_lead", "R3_weak_or_indirect_evidence"
        parts = []
        if prod_all: parts.append("product mention only from a search snippet/lead")
        if role: parts.append("importer/distributor role stated without product evidence")
        if prepared_only: parts.append("only prepared/roasted peanut products mentioned (outside raw scope)")
        reason = "; ".join(parts).capitalize() + "."
    else:
        cls, rule = "undetermined", "R4_no_usable_evidence"
        reason = "No usable supporting evidence; pages missing, inaccessible, or without relevant text."

    issues = ["legal identity not verified against a registry"]
    if company["dedup_status"] != "unique":
        issues.append(f"identity/dedup: {company['dedup_status']}")
    if conflict: issues.append("conflicting import evidence (contradicted claim)")
    if cls in ("potential_buyer", "relevant_business", "weak_lead"):
        if dims["current_buying_intent"] == "unknown": issues.append("no evidence of current buying intent")
        if dims["india_origin_evidence"] == "unknown": issues.append("no explicit India-origin evidence")
        if dims["contact_availability"] == "none": issues.append("no public contact channel extracted")
    if prof and prof.get("extraction_status") not in (None, "complete"):
        issues.append(f"website extraction {prof['extraction_status']}")
    if not company["country_iso3"]:
        issues.append("country unknown")
    elif company["country_source"] == "query_context":
        issues.append("country inferred only from search query")

    support_claims = [c[t] for t in c if c[t] and (_usable(c[t]) or c[t]["claim_status"] == "contradicted")]
    return {
        "classification": cls, "rule": rule, "reason": reason, "dims": dims, "issues": issues,
        "claim_ids": [x["claim_id"] for x in support_claims],
        "evidence_ids": sorted({e for x in support_claims for e in x["evidence_ids"].split(";") if e}),
    }


def run(ctx: Context) -> StageResult:
    res = StageResult("qualification")
    companies = read_csv(ctx.paths.companies)
    claims_rows = read_csv(ctx.paths.company_claims)
    profiles = (read_json(ctx.paths.company_profiles, {}) or {}).get("profiles", [])
    prof_by_cand = {cid: p for p in profiles for cid in p.get("candidate_ids", [])}
    by_company = defaultdict(dict)
    for r in claims_rows:
        by_company[r["company_id"]][r["claim_type"]] = r
    profile = read_json(ctx.paths.product_profile) or {"target_markets": []}
    targets = {m["iso3"] for m in profile["target_markets"]}
    overrides = {r["company_id"]: r for r in read_csv(ctx.paths.manual_qualification)
                 if r.get("classification") in CLASSES}
    previous = {r["company_id"]: r for r in read_csv(ctx.paths.buyer_qualification)}

    rows, changes = [], []
    for comp in companies:
        prof = next((prof_by_cand[x] for x in comp["candidate_ids"].split(";") if x in prof_by_cand), None)
        r = classify(comp, by_company.get(comp["company_id"], {}), prof)
        ov = overrides.get(comp["company_id"])
        final = ov["classification"] if ov else r["classification"]
        prev = previous.get(comp["company_id"])
        if prev and prev["final_classification"] != final:
            changes.append({"changed_at": iso(ctx.now), "run_id": ctx.run_id, "company_id": comp["company_id"],
                            "previous_classification": prev["final_classification"], "new_classification": final,
                            "reason": "evidence changed (rules_v1 re-run)" if not ov else "manual override applied"})
        rows.append({
            "company_id": comp["company_id"], "company_name": comp["company_name"],
            "country_iso3": comp["country_iso3"],
            "target_market": comp["country_iso3"] if comp["country_iso3"] in targets else "",
            "business_type": (prof or {}).get("business_type", ""), "auto_classification": r["classification"],
            "final_classification": final, "override_applied": "yes" if ov else "no",
            "override_note": (f"{ov.get('decided_by','')} {ov.get('decided_at','')}: {ov.get('note','')}".strip() if ov else ""),
            "classification_rule": r["rule"], "classification_reason": r["reason"],
            "product_relevance": r["dims"]["product_relevance"], "buyer_role_evidence": r["dims"]["buyer_role_evidence"],
            "historical_import_activity": r["dims"]["historical_import_activity"],
            "india_origin_evidence": r["dims"]["india_origin_evidence"],
            "current_buying_intent": r["dims"]["current_buying_intent"],
            "legal_identity_status": r["dims"]["legal_identity_status"],
            "contact_availability": r["dims"]["contact_availability"],
            "supporting_claim_ids": ";".join(r["claim_ids"]), "supporting_evidence_ids": ";".join(r["evidence_ids"]),
            "unresolved_issues": " | ".join(r["issues"]), "recommended_by": "rules_v1",
            "qualification_status": "unreviewed", "classified_at": iso(ctx.now)})
    if companies or ctx.paths.buyer_qualification.exists():
        res.outputs.append(str(write_csv(ctx.paths.buyer_qualification, rows, QUALIFICATION_FIELDS, ctx.paths, ctx.now)))
    if changes:
        old = read_csv(ctx.paths.classification_changes)
        res.outputs.append(str(write_csv(ctx.paths.classification_changes, old + changes, CHANGE_FIELDS, ctx.paths, ctx.now)))
        res.warn(f"{len(changes)} company classification(s) changed vs the previous run (logged in classification_changes.csv).")
    counts = defaultdict(int)
    for r in rows:
        counts[r["final_classification"]] += 1
    res.counts = {"companies": len(rows), "overrides_applied": sum(r["override_applied"] == "yes" for r in rows),
                  **{f"class_{k}": counts.get(k, 0) for k in CLASSES}}
    if not companies:
        res.warn("No companies to qualify.")
    res.status = STATUS_WARNINGS if res.warnings else STATUS_COMPLETED
    return res
