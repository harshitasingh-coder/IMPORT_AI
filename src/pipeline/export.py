"""STAGE 9 - Exports and dashboard.

data/exports/: ranked_buyers.csv, company_profiles.csv, evidence_log.csv, market_analysis.csv,
dashboard.html (self-contained; open it in a browser) and dashboard_data.json.
pipeline_run_summary.json is written by the orchestrator after this stage.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

from src.pipeline.common import (Context, StageResult, STATUS_COMPLETED, STATUS_WARNINGS, iso, read_csv,
                                 read_json, write_csv, write_output)
from src.pipeline.schemas import (COMPANY_PROFILE_EXPORT_FIELDS, EVIDENCE_FIELDS, MARKET_ANALYSIS_FIELDS,
                                  RANKED_FIELDS)

TEMPLATE = Path(__file__).with_name("dashboard_template.html")


def company_profile_rows(ctx):
    companies = read_csv(ctx.paths.companies)
    profiles = (read_json(ctx.paths.company_profiles, {}) or {}).get("profiles", [])
    by_cand = {cid: p for p in profiles for cid in p.get("candidate_ids", [])}
    rows = []
    for c in companies:
        p = next((by_cand[x] for x in c["candidate_ids"].split(";") if x in by_cand), None) or {}
        rows.append({
            "company_id": c["company_id"], "company_name": c["company_name"], "legal_suffix": c["legal_suffix"],
            "domain": c["domain"], "website": c["website"] or p.get("website", ""), "country_iso3": c["country_iso3"],
            "business_type": p.get("business_type", ""), "business_description": p.get("business_description", ""),
            "product_categories": "; ".join(p.get("product_categories", [])),
            "groundnut_relevance": p.get("groundnut_relevance", "unknown"),
            "role_terms": "; ".join(r["role"] for r in p.get("role_terms", [])),
            "public_email": "; ".join(p.get("public_emails", [])), "public_phone": "; ".join(p.get("public_phones", [])),
            "public_address": p.get("public_address", ""), "contact_page": p.get("contact_page", ""),
            "about_page": p.get("about_page", ""), "product_page": p.get("product_page", ""),
            "extraction_status": p.get("extraction_status", "not_attempted"),
            "missing_fields": "; ".join(p.get("missing_fields", [])), "dedup_status": c["dedup_status"],
            "aliases": c["aliases"], "source_url": p.get("source_url", ""), "page_title": p.get("page_title", ""),
            "retrieved_at": p.get("retrieved_at", ""), "publication_date": p.get("publication_date", "")})
    return rows


def build_dashboard_payload(ctx, run_info=None):
    profile = read_json(ctx.paths.product_profile) or {}
    ranked = read_csv(ctx.paths.ranked_buyers)
    quals = {q["company_id"]: q for q in read_csv(ctx.paths.buyer_qualification)}
    evidence = {e["evidence_id"]: e for e in read_csv(ctx.paths.evidence_log)}
    claims = defaultdict(list)
    for c in read_csv(ctx.paths.company_claims):
        claims[c["company_id"]].append(c)
    profiles = {r["company_id"]: r for r in company_profile_rows(ctx)}
    market = read_csv(ctx.paths.market_analysis)
    counts = defaultdict(int)
    for r in ranked:
        counts[r["final_classification"]] += 1
    issues = []
    if profile.get("hs_verification_summary") != "all_verified":
        issues.append("HS codes are unverified: confirm against each importing country's tariff schedule.")
    if not market:
        issues.append("Trade (market) data unavailable for this run; no market statistics are shown.")
    issues.append("Legal identity / registry verification is a separate follow-up; no company is registry-verified.")
    issues.append("All claims are single-source or hand-collected and unreviewed; none is independently cross-verified.")
    comps = []
    for r in ranked:
        q = quals.get(r["company_id"], {})
        ev = [evidence[e] for e in r["evidence_ids"].split(";") if e in evidence]
        comps.append({"ranking": r, "qualification": q, "profile": profiles.get(r["company_id"], {}),
                      "claims": claims.get(r["company_id"], []), "evidence": ev})
    return {
        "generated_at": iso(ctx.now), "run": run_info or {}, "product_profile": profile,
        "markets": [m for m in market if m["is_target_market"] == "yes"], "market_available": bool(market),
        "counts": {"discovered": len(ranked), **{k: counts.get(k, 0) for k in
                  ("potential_buyer", "relevant_business", "weak_lead", "undetermined", "excluded")}},
        "companies": comps, "global_issues": issues}


def render_dashboard(payload):
    data = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/").replace("<!--", "<\\!--")
    return TEMPLATE.read_text(encoding="utf-8").replace("/*__DATA__*/null", data)


def run(ctx: Context) -> StageResult:
    res = StageResult("export")
    ex, n = ctx.paths.exports, ctx.now
    ex.mkdir(parents=True, exist_ok=True)
    ranked = read_csv(ctx.paths.ranked_buyers)
    res.outputs.append(str(write_csv(ex / "ranked_buyers.csv", ranked, RANKED_FIELDS, ctx.paths, n)))
    res.outputs.append(str(write_csv(ex / "company_profiles.csv", company_profile_rows(ctx),
                                     COMPANY_PROFILE_EXPORT_FIELDS, ctx.paths, n)))
    res.outputs.append(str(write_csv(ex / "evidence_log.csv", read_csv(ctx.paths.evidence_log), EVIDENCE_FIELDS, ctx.paths, n)))
    market = read_csv(ctx.paths.market_analysis)
    if market:
        res.outputs.append(str(write_csv(ex / "market_analysis.csv", market, MARKET_ANALYSIS_FIELDS, ctx.paths, n)))
    else:
        res.warn("market_analysis.csv not exported: trade data unavailable.")
    payload = build_dashboard_payload(ctx, ctx.options.get("run_info"))
    write_output(ex / "dashboard_data.json", json.dumps(payload, indent=1, ensure_ascii=False) + "\n", ctx.paths, n)
    res.outputs.append(str(write_output(ex / "dashboard.html", render_dashboard(payload), ctx.paths, n)))
    res.counts = {"ranked_rows": len(ranked), "exports_written": len(res.outputs)}
    if not ranked:
        res.warn("Export contains no companies (empty ranked list).")
    res.status = STATUS_WARNINGS if res.warnings else STATUS_COMPLETED
    return res
