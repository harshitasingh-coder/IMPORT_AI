"""`python -m src.pipeline audit` - integrity checks over the pipeline outputs.

Levels: ERROR (breaks a rule; non-zero exit) | WARN (needs attention) | INFO.
"""

from __future__ import annotations

from collections import Counter

from src.pipeline import schemas as S
from src.pipeline.common import Context, age_days, read_csv, read_csv_header, read_json

HEADERS = {
    "market_analysis.csv": S.MARKET_ANALYSIS_FIELDS, "market_shortlist.csv": S.MARKET_ANALYSIS_FIELDS,
    "company_candidates_raw.csv": S.CANDIDATE_FIELDS, "companies.csv": S.COMPANY_FIELDS,
    "duplicate_suggestions.csv": S.DUPLICATE_FIELDS, "evidence_log.csv": S.EVIDENCE_FIELDS,
    "company_claims.csv": S.CLAIM_FIELDS, "buyer_qualification.csv": S.QUALIFICATION_FIELDS,
    "ranked_buyers.csv": S.RANKED_FIELDS,
}


def audit(ctx: Context):
    P, out = ctx.paths, []
    add = lambda lvl, msg: out.append((lvl, msg))
    prof = read_json(P.product_profile)
    if not prof:
        add("ERROR", "product_profile.json missing (run stage 'profile')")
    else:
        add("INFO", f"profile {prof['profile_id']}: HS verification = {prof['hs_verification_summary']}")
    for name, fields in HEADERS.items():
        path = P.processed / name
        if path.exists():
            head = read_csv_header(path)
            if head != fields:
                add("ERROR", f"{name}: header differs from schema (missing {sorted(set(fields)-set(head))}, "
                              f"extra {sorted(set(head)-set(fields))})")
    companies = {r["company_id"]: r for r in read_csv(P.companies)}
    claims, evidence = read_csv(P.company_claims), {r["evidence_id"]: r for r in read_csv(P.evidence_log)}
    quals, ranked = read_csv(P.buyer_qualification), read_csv(P.ranked_buyers)
    cands = read_csv(P.candidates)
    add("INFO", f"{len(cands)} candidates, {len(companies)} companies, {len(claims)} claims, "
                f"{len(evidence)} evidence rows, {len(ranked)} ranked rows")
    for c in claims:
        if c["company_id"] not in companies:
            add("ERROR", f"claim {c['claim_id']} references unknown company")
        ids = [e for e in c["evidence_ids"].split(";") if e]
        if c["claim_status"] in ("supported", "contradicted"):
            if not ids:
                add("ERROR", f"claim {c['claim_id']} ({c['claim_type']}) is {c['claim_status']} without evidence")
            if not c["retrieved_at"] or not c["source_url"]:
                add("ERROR", f"claim {c['claim_id']} lacks retrieval date or source URL")
        for e in ids:
            if e not in evidence:
                add("ERROR", f"claim {c['claim_id']} cites missing evidence {e}")
        if "verified" in (c["corroboration"] or "") and "not" not in c["corroboration"]:
            add("ERROR", f"claim {c['claim_id']} corroboration claims verification")
    for e in evidence.values():
        if e["company_id"] not in companies:
            add("ERROR", f"evidence {e['evidence_id']} references unknown company")
        if not e["retrieved_at"] or not e["source_url"] or not e["exact_text"]:
            add("ERROR", f"evidence {e['evidence_id']} lacks retrieval date, URL or text")
        if e["snapshot_path"] and not (P.data / e["snapshot_path"]).exists():
            add("WARN", f"evidence {e['evidence_id']}: snapshot file missing ({e['snapshot_path']})")
    for q in quals:
        if q["company_id"] not in companies:
            add("ERROR", f"qualification for unknown company {q['company_id']}")
        if q["final_classification"] == "potential_buyer" and q["override_applied"] != "yes" and not q["supporting_evidence_ids"]:
            add("ERROR", f"{q['company_id']} is potential_buyer without evidence IDs")
        if q["qualification_status"] != "unreviewed" :
            add("WARN", f"{q['company_id']} qualification_status={q['qualification_status']}")
    for r in ranked:
        parts = sum(float(r[f"score_{n}"]) for n, _ in S.SCORE_COMPONENTS)
        if abs(parts - float(r["total_score"])) > 0.11:
            add("ERROR", f"{r['company_id']}: components sum {parts} != total {r['total_score']}")
        for n, mx in S.SCORE_COMPONENTS:
            if float(r[f"score_{n}"]) > mx:
                add("ERROR", f"{r['company_id']}: {n} exceeds max {mx}")
    stale = [r for r in read_csv(P.market_analysis) if r["is_target_market"] == "yes" and r["freshness_status"] != "fresh"]
    if stale:
        add("WARN", f"trade data for {len(stale)} target market(s) is stale (original dates preserved)")
    if not read_csv(P.market_analysis):
        add("WARN", "no market analysis available (trade data unavailable)")
    dup = [x for x in read_csv(P.duplicate_suggestions)]
    if dup:
        add("WARN", f"{len(dup)} possible duplicate pair(s) await manual decision")
    dd = Counter(c["dedup_status"] for c in companies.values())
    if dd.get("manual_review_required"):
        add("WARN", f"{dd['manual_review_required']} company record(s) need manual identity review")
    if P.archive.exists():
        add("INFO", f"{len(list(P.archive.iterdir()))} archived previous output version(s) in {P.archive}")
    return out


def print_report(out):
    for lvl, msg in out:
        print(f"[{lvl}] {msg}")
    errs = sum(1 for l, _ in out if l == "ERROR")
    warns = sum(1 for l, _ in out if l == "WARN")
    print(f"\nAudit: {errs} error(s), {warns} warning(s)")
    return 1 if errs else 0
