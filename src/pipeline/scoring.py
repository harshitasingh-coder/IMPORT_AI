"""STAGE 8 - Explainable 100-point lead-prioritization score (NOT a purchase probability).

Components (max): product relevance 25, buyer/purchasing role 25, evidence quality 20,
target-market fit 10, India-origin relevance 10, contactability 10.

Rules enforced in code:
* only claims with status 'supported' AND evidence IDs can earn points (unsupported assertions = 0)
* unknown information earns nothing
* search-snippet (tier 6) evidence is capped (product 8, role 6, India 3, evidence quality 3)
* historical/trade-record import evidence is capped at 15 of the 25 role points and never earns the
  current-intent bonus (+5, only from a dated open buyer lead)
* India points need the word 'India' in the evidence text itself
* contactability needs an extracted public email/phone/address backed by a supported contact claim
Bands: 75-100 High priority | 50-74 Medium | 25-49 Low | 0-24 Insufficient evidence.
"""

from __future__ import annotations

from collections import defaultdict

from src.pipeline import countries
from src.pipeline.common import (Context, StageResult, STATUS_COMPLETED, STATUS_WARNINGS, age_days, iso,
                                 parse_dt, read_csv, read_json, write_csv)
from src.pipeline.qualification import _tier, _usable
from src.pipeline.schemas import RANKED_FIELDS, SCORE_COMPONENTS

SCORE_VERSION = "score_v1"
CF = {"high": 1.0, "medium": 0.7, "low": 0.4}
AUTH_POINTS = {1: 12, 2: 12, 3: 10, 4: 8, 5: 6, 6: 3}
MARKET_SOURCE_FACTOR = {"page_address": 1.0, "source_record": 1.0, "tld": 0.6, "query_context": 0.0}
CLASS_ORDER = {"potential_buyer": 0, "relevant_business": 1, "weak_lead": 2, "undetermined": 3}
CORE = ["product_relevance", "sells_groundnuts", "processes_groundnuts", "imports_raw_groundnuts",
        "imports_from_india", "historical_import_activity", "current_buying_requirement",
        "importer_or_distributor_role"]


def band(total):
    return ("High priority" if total >= 75 else "Medium priority" if total >= 50
            else "Low priority" if total >= 25 else "Insufficient evidence")


def _pts(maxp, claim):
    return maxp * CF.get(claim["confidence_level"], 0.0)


def score_company(claims, company, prof, targets, now):
    u = {t: c for t, c in claims.items() if _usable(c)}
    comp, why = {}, {}

    # product relevance (25)
    best, bt = 0.0, None
    for t, mx in (("imports_raw_groundnuts", 25), ("sells_groundnuts", 25), ("processes_groundnuts", 25),
                  ("product_relevance", 20)):
        if t in u:
            p = _pts(mx, u[t])
            if _tier(u[t]) >= 6:
                p = min(p, 8)
            if p > best:
                best, bt = p, t
    comp["product_relevance"] = round(best, 1)
    why["product_relevance"] = (f"{bt} supported ({u[bt]['confidence_level']} confidence, source tier {_tier(u[bt])})"
                                if bt else "no supported product evidence")

    # buyer / purchasing role (25)
    best, bt = 0.0, None
    cands = []
    if "imports_raw_groundnuts" in u:
        p = _pts(25, u["imports_raw_groundnuts"])
        if u["imports_raw_groundnuts"]["source_type"] in ("shipment_record", "trade_data"):
            p = min(p, 15)
        cands.append((p, "imports_raw_groundnuts"))
    if "historical_import_activity" in u:
        cands.append((min(_pts(15, u["historical_import_activity"]), 15), "historical_import_activity"))
    if "importer_or_distributor_role" in u:
        p = _pts(10, u["importer_or_distributor_role"])
        cands.append((min(p, 6) if _tier(u["importer_or_distributor_role"]) >= 6 else p, "importer_or_distributor_role"))
    if cands:
        best, bt = max(cands)
    bonus = 5.0 if "current_buying_requirement" in u else 0.0
    comp["buyer_role"] = round(min(25.0, best + bonus), 1)
    why["buyer_role"] = ((f"{bt} ({u[bt]['confidence_level']}, tier {_tier(u[bt])})" if bt else "no purchasing-role evidence")
                         + ("; +5 dated open buyer lead (current intent)" if bonus else
                            "; no current-buying-intent evidence"))

    # evidence quality (20)
    core = {t: c for t, c in u.items() if t in CORE}
    if core:
        tiers = [_tier(c) for c in core.values()]
        auth = AUTH_POINTS[min(tiers)]
        high = sum(c["confidence_level"] == "high" for c in core.values()) / len(core)
        dated = any((age_days(c["publication_date"], now) is not None and 0 <= age_days(c["publication_date"], now) <= 730)
                    for c in core.values())
        comp["evidence_quality"] = round(auth + 5 * high + (3 if dated else 0), 1)
        why["evidence_quality"] = (f"best source tier {min(tiers)} -> {auth}/12; explicitness {5*high:.1f}/5; "
                                   f"dated within 2y {'3' if dated else '0 (no usable date)'}/3")
        if min(tiers) >= 6:
            why["evidence_quality"] += "; snippet-only evidence"
    else:
        comp["evidence_quality"], why["evidence_quality"] = 0.0, "no supported buyer-relevant evidence"

    # market fit (10)
    iso3, src = company["country_iso3"], company["country_source"]
    if iso3 in targets:
        f = MARKET_SOURCE_FACTOR.get(src, 0.0)
        comp["market_fit"] = round(10 * f, 1)
        why["market_fit"] = (f"{countries.country_name(iso3) or iso3} is a target market ({targets[iso3] or 'type n/a'}); "
                             f"country evidence: {src or 'none'} x{f}")
    else:
        comp["market_fit"] = 0.0
        why["market_fit"] = "country unknown" if not iso3 else f"{iso3} is not a target market"

    # India origin (10)
    c = u.get("imports_from_india")
    if c and "india" in (c["exact_text"] or "").lower():
        p = _pts(10, c)
        comp["india_origin"] = round(min(p, 3) if _tier(c) >= 6 else p, 1)
        why["india_origin"] = f"explicit India mention ({c['confidence_level']}, tier {_tier(c)})"
    else:
        comp["india_origin"], why["india_origin"] = 0.0, "no explicit India-origin evidence"

    # contactability (10)
    pts, ch = 0, []
    if "public_contact_information" in u and prof:
        if prof.get("public_emails"): pts, ch = pts + 5, ch + ["email"]
        if prof.get("public_phones"): pts, ch = pts + 3, ch + ["phone"]
        if prof.get("public_address"): pts, ch = pts + 2, ch + ["address"]
    comp["contactability"] = float(pts)
    why["contactability"] = ("public " + ", ".join(ch)) if ch else "no extracted public contact channel"

    total = round(sum(comp.values()), 1)
    return comp, why, total


def run(ctx: Context) -> StageResult:
    res = StageResult("scoring")
    companies = {r["company_id"]: r for r in read_csv(ctx.paths.companies)}
    quals = read_csv(ctx.paths.buyer_qualification)
    claims_rows = read_csv(ctx.paths.company_claims)
    evidence = {r["evidence_id"]: r for r in read_csv(ctx.paths.evidence_log)}
    profile = read_json(ctx.paths.product_profile) or {"target_markets": []}
    targets = {m["iso3"]: m.get("market_type", "") for m in profile["target_markets"]}
    profiles = (read_json(ctx.paths.company_profiles, {}) or {}).get("profiles", [])
    prof_by_cand = {cid: p for p in profiles for cid in p.get("candidate_ids", [])}
    by_company = defaultdict(dict)
    for r in claims_rows:
        by_company[r["company_id"]][r["claim_type"]] = r

    # guard: supported claims without evidence earn nothing; report them
    orphan = [r["claim_id"] for r in claims_rows if r["claim_status"] == "supported" and not r["evidence_ids"]]
    if orphan:
        res.warn(f"{len(orphan)} supported claim(s) had no evidence IDs and earned zero points.")

    rows = []
    for q in quals:
        comp_row = companies[q["company_id"]]
        prof = next((prof_by_cand[x] for x in comp_row["candidate_ids"].split(";") if x in prof_by_cand), None)
        comp, why, total = score_company(by_company.get(q["company_id"], {}), comp_row, prof, targets, ctx.now)
        eids = [e for e in q["supporting_evidence_ids"].split(";") if e]
        dates = sorted(evidence[e]["retrieved_at"] for e in eids if e in evidence and evidence[e]["retrieved_at"])
        row = {
            "company_id": q["company_id"], "company_name": q["company_name"], "country_iso3": q["country_iso3"],
            "target_market": q["target_market"], "business_type": q["business_type"],
            "final_classification": q["final_classification"], "total_score": total,
            "priority_band": ("Excluded (not ranked)" if q["final_classification"] == "excluded" else band(total)),
            **{f"score_{k}": comp[k] for k, _ in SCORE_COMPONENTS},
            **{f"why_{k}": why[k] for k, _ in SCORE_COMPONENTS},
            "website": comp_row["website"] or (prof or {}).get("website", ""),
            "public_email": ((prof or {}).get("public_emails") or [""])[0],
            "public_phone": ((prof or {}).get("public_phones") or [""])[0],
            "public_address": (prof or {}).get("public_address", ""),
            "classification_reason": q["classification_reason"], "evidence_ids": q["supporting_evidence_ids"],
            "oldest_evidence_retrieved_at": dates[0] if dates else "",
            "newest_evidence_retrieved_at": dates[-1] if dates else "",
            "evidence_age_days": age_days(dates[0], ctx.now) if dates else "",
            "unresolved_issues": q["unresolved_issues"], "score_version": SCORE_VERSION, "scored_at": iso(ctx.now)}
        rows.append(row)
    ranked = sorted([r for r in rows if r["final_classification"] != "excluded"],
                    key=lambda r: (-r["total_score"], CLASS_ORDER.get(r["final_classification"], 9), r["company_name"].lower()))
    for i, r in enumerate(ranked, 1):
        r["rank"] = i
    for r in rows:
        r.setdefault("rank", "")
    out_rows = ranked + [r for r in rows if r["final_classification"] == "excluded"]
    if rows or ctx.paths.ranked_buyers.exists():
        res.outputs.append(str(write_csv(ctx.paths.ranked_buyers, out_rows, RANKED_FIELDS, ctx.paths, ctx.now)))
    bands = defaultdict(int)
    for r in out_rows:
        bands[r["priority_band"]] += 1
    res.counts = {"companies_scored": len(rows), **{f"band_{k.split()[0].lower()}": v for k, v in bands.items()}}
    if not rows:
        res.warn("No qualified companies to score.")
    res.status = STATUS_WARNINGS if res.warnings else STATUS_COMPLETED
    return res
