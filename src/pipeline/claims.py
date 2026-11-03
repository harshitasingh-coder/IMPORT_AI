"""STAGE 6 - Evidence log and claim assessment.

Every claim is built from stored evidence: an exact text span taken from a saved page
snapshot, a hand-collected source record, or a search snippet. Rules (rules_v1):

* status supported     = explicit evidence text found (evidence IDs attached)
* status contradicted  = explicit negating text (e.g. "we do not import peanuts")
* status not_supported = sources WERE examined but contain no supporting text. This is NOT evidence
                         of absence and is never treated as a negative downstream.
* status unknown       = nothing relevant was examined (or the claim cannot come from this source type)
* source authority (tier 1-6) and claim confidence (high/medium/low) are stored separately.
* One source never produces cross-verification: corroboration is 'single_source' or
  'multiple_sources' (distinct domains; independence is NOT verified).
* Search snippets only ever support product_relevance and importer_or_distributor_role, at low confidence.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field

from src.pipeline import terms
from src.pipeline.common import (Context, StageResult, STATUS_COMPLETED, STATUS_WARNINGS, age_days,
                                 fold, iso, parse_dt, read_csv, read_json, stable_id, write_csv)
from src.pipeline.discovery import domain_of
from src.pipeline.schemas import CLAIM_FIELDS, EVIDENCE_FIELDS

CLAIM_TYPES = [
    "product_relevance", "sells_groundnuts", "processes_groundnuts", "imports_raw_groundnuts",
    "imports_from_india", "historical_import_activity", "current_buying_requirement",
    "importer_or_distributor_role", "company_identity", "public_contact_information",
]
TIER_LABEL = {1: "official_company_website_or_document", 2: "government_registry_or_filing",
              3: "industry_association", 4: "business_directory_or_portal", 5: "trade_data_aggregator",
              6: "search_snippet_or_unverified_third_party"}
RECORD_TIER = {"buyer_lead": 4, "directory_listing": 4, "shipment_record": 5, "trade_data": 5,
               "company_website": 6, "social_profile": 6, "search_result": 6}
CURRENT_LEAD_MAX_AGE_DAYS = 180
CLOSED_LEAD = re.compile(r"closed|expired|fulfil|cancel|inactive|completed", re.I)
SELL_STRONG = re.compile(r"\b(?:sell|sells|selling|supply|supplies|supplier|suppliers|supplying|offer|offers|"
                         r"provide|provides|distribut\w+|wholesal\w+|trader|trading)\b", re.I)
PAST_IMPORT = re.compile(r"\b(?:has|have|had)\s+(?:been\s+)?import(?:ed|ing)\b|\bimported\b|"
                         r"import(?:ing)?\s+since\b|\bsince\s+\d{4}\b.{0,60}\bimport", re.I)
MAX_TEXT = 400
BASE_LIMITS = {
    "product_relevance": "Product mention only; says nothing about buying.",
    "sells_groundnuts": "Selling raw groundnuts is not evidence of purchasing/importing them.",
    "processes_groundnuts": "A processor is not automatically an importer.",
    "imports_raw_groundnuts": "Self-reported or record-based; no customs data cross-check.",
    "imports_from_india": "Requires explicit mention of India; origin of future purchases is not implied.",
    "historical_import_activity": "Past activity only; does not show a current buying need.",
    "current_buying_requirement": "Only dated, open buyer leads count; website contact pages do not.",
    "importer_or_distributor_role": "Role statement; does not show interest in groundnuts or Indian origin.",
    "company_identity": "Identity not verified against a government registry.",
    "public_contact_information": "Contact details shown publicly; they do not prove buying intent.",
}


@dataclass
class Source:
    kind: str                 # page | record | snippet
    tier: int
    source_type: str
    url: str
    title: str
    retrieved_at: str
    pub_date: str
    items: list
    snapshot: str = ""
    candidate_id: str = ""
    text_kind: str = "page_text"
    method: str = "sentence_match"
    record: dict = field(default_factory=dict)

    @property
    def domain(self):
        return domain_of(self.url) or self.url


@dataclass
class Hit:
    source: Source
    text: str
    strength: str            # explicit | window
    text_kind: str = ""


def _clip(sentence, pattern=None, limit=MAX_TEXT):
    if len(sentence) <= limit:
        return sentence
    m = pattern.search(sentence) if pattern else None
    start = max(0, (m.start() if m else 0) - limit // 2)
    return sentence[start:start + limit]


# ---------------------------------------------------------------- sources

def build_sources(ctx, company, cands_by_id, prof_by_cand):
    sources, seen_prof = [], set()
    for cid in [x for x in company["candidate_ids"].split(";") if x]:
        c = cands_by_id.get(cid)
        if not c:
            continue
        prof = prof_by_cand.get(cid)
        if prof and prof["profile_key"] not in seen_prof:
            seen_prof.add(prof["profile_key"])
            for pg in prof.get("pages", []):
                if pg.get("snapshot_text") and not pg.get("error"):
                    tp = ctx.paths.data / pg["snapshot_text"]
                    if tp.exists():
                        sources.append(Source(
                            "page", 1, "official_company_website", pg["url"], pg.get("title", ""),
                            pg["retrieved_at"], prof.get("publication_date", ""),
                            terms.sentences(tp.read_text(encoding="utf-8")), snapshot=pg["snapshot_text"],
                            candidate_id=cid))
        if c["evidence_level"] == "lead_only_snippet":
            items = [x for x in (c["snippet"], c["page_title"]) if x]
            sources.append(Source("snippet", 6, "search_snippet", c["source_url"], c["page_title"],
                                  c["discovered_at"], "", items, candidate_id=cid, text_kind="search_snippet",
                                  method="snippet_match"))
        elif c["evidence_level"] in ("manual_record", "seed_url"):
            tier = RECORD_TIER.get(c["source_type"], 6)
            fields_ = [("product_description_raw", c["product_description_raw"] or ""),
                       ("buying_requirement_raw", c["buying_requirement_raw"]),
                       ("snippet", c["snippet"] if c["evidence_level"] == "seed_url" else "")]
            sources.append(Source("record", tier, c["source_type"], c["source_url"], c["page_title"],
                                  c["discovered_at"], c["lead_posted_date"] or c["shipment_date"],
                                  [v for _, v in fields_ if v], candidate_id=cid,
                                  text_kind="record_fields", method="record_field", record=c))
    return sources


# ---------------------------------------------------------------- matching helpers

def _conf(tier, strength):
    if strength == "explicit":
        return "high" if tier <= 4 else "medium" if tier == 5 else "low"
    return "medium" if tier <= 4 else "low"


def _scan(sources, main_re, require_product=True, allow_prepared=False, kinds=("page", "snippet", "record")):
    """Return (supporting, contradicting) Hit lists from sentence-level pattern matching."""
    sup, con = [], []
    for src in sources:
        if src.kind not in kinds:
            continue
        items = src.items
        for i, s in enumerate(items):
            if len(s) > 600:
                continue
            prev = items[i - 1] if i else ""
            nxt = items[i + 1] if i + 1 < len(items) else ""
            main_here = main_re.search(s)
            prod_here = terms.PRODUCT_RE.search(s)
            strength = None
            if main_here and (prod_here or not require_product):
                strength = "explicit"
            elif src.kind == "page" and require_product:
                if main_here and (terms.PRODUCT_RE.search(prev) or terms.PRODUCT_RE.search(nxt)):
                    strength = "window"
                elif prod_here and (main_re.search(prev) or main_re.search(nxt)):
                    strength = "window"
            if not strength:
                continue
            if require_product and not allow_prepared and terms.is_prepared_only(s) and prod_here:
                continue
            anchor = main_here or prod_here
            h = Hit(src, _clip(s, anchor.re if anchor else None), strength, src.text_kind)
            if terms.NEGATION_RE.search(s) and strength == "explicit":
                con.append(h)
            else:
                sup.append(h)
    return sup, con


def _rank(hits):
    return sorted(hits, key=lambda h: (h.source.tier, 0 if h.strength == "explicit" else 1))


# ---------------------------------------------------------------- claim assessment

def _record_hits(sources, claim, now):
    """Claims derived from hand-collected records (structured fields, exact values)."""
    hits = []
    for s in sources:
        if s.kind != "record":
            continue
        r = s.record
        prod_txt = " ".join([r["product_description_raw"] or "", r["buying_requirement_raw"] or ""])
        has_prod = bool(terms.PRODUCT_RE.search(prod_txt)) or r["hs_code"].startswith("1202")
        prepared = terms.is_prepared_only(prod_txt) and not r["hs_code"].startswith("1202")
        if claim in ("product_relevance",) and has_prod and not prepared:
            txt = (r["product_description_raw"] or r["buying_requirement_raw"]
                   or f"hs_code={r['hs_code']}")
            hits.append(Hit(s, _clip(txt), "explicit", "record_field"))
        if r["source_type"] == "shipment_record":
            if claim in ("historical_import_activity", "imports_raw_groundnuts") and has_prod and not prepared \
                    and (r["shipment_date"] or claim == "imports_raw_groundnuts"):
                txt = "; ".join(f"{k}={r[k]}" for k in ("product_description_raw", "hs_code", "shipment_date",
                                                         "origin_country", "quantity_raw") if r[k])
                hits.append(Hit(s, txt, "explicit" if r["hs_code"] or r["shipment_date"] else "window", "record_fields"))
            if claim == "imports_from_india" and has_prod and fold(r["origin_country"]) in ("india", "ind", "in"):
                hits.append(Hit(s, f"origin_country={r['origin_country']}", "explicit", "record_fields"))
            if claim == "importer_or_distributor_role":
                hits.append(Hit(s, f"listed as consignee/importer in {r['source_name'] or 'trade record'}: "
                                   f"{r['product_description_raw']}"[:MAX_TEXT], "explicit", "record_fields"))
        if r["source_type"] == "buyer_lead" and claim == "current_buying_requirement" and has_prod:
            lead_age = age_days(r["lead_posted_date"], now)
            if lead_age is not None and 0 <= lead_age <= CURRENT_LEAD_MAX_AGE_DAYS \
                    and not CLOSED_LEAD.search(r["lead_status"] or ""):
                hits.append(Hit(s, _clip(r["buying_requirement_raw"] or r["product_description_raw"]),
                                "explicit", "record_field"))
    return hits


def _contact_hits(ctx, prof):
    """(hits for contact claim) from extracted emails/phones/address, with the exact page line."""
    hits = []
    if not prof:
        return hits
    for pg in prof.get("pages", []):
        if not pg.get("snapshot_text") or pg.get("error"):
            continue
        tp = ctx.paths.data / pg["snapshot_text"]
        if not tp.exists():
            continue
        lines = tp.read_text(encoding="utf-8").split("\n")
        src = Source("page", 1, "official_company_website", pg["url"], pg.get("title", ""),
                     pg["retrieved_at"], prof.get("publication_date", ""), lines, snapshot=pg["snapshot_text"])
        wanted = [*(prof.get("public_emails") or []), *(prof.get("public_phones") or [])]
        if prof.get("public_address"):
            wanted.append(prof["public_address"])
        for w in wanted:
            for line in lines:
                if w.lower() in line.lower() and len(line) <= 300:
                    hits.append(Hit(src, line, "explicit", "page_text"))
                    break
    return hits


def assess_company(ctx, company, sources, prof, now):
    claims, evidence = [], {}
    examined_pages = any(s.kind == "page" for s in sources)
    examined_any = bool(sources)

    def ev_row(h):
        s = h.source
        eid = stable_id("evd", company["company_id"], s.url, h.text)
        evidence.setdefault(eid, {
            "evidence_id": eid, "company_id": company["company_id"], "source_url": s.url,
            "source_type": s.source_type, "source_authority_tier": s.tier,
            "source_authority_label": TIER_LABEL[s.tier], "page_title": s.title, "exact_text": h.text,
            "text_kind": h.text_kind or s.text_kind, "retrieved_at": s.retrieved_at,
            "publication_date": s.pub_date if s.kind == "page" else "", "snapshot_path": s.snapshot,
            "extraction_method": s.method, "candidate_id": s.candidate_id})
        return eid

    def emit(ctype, status, sup=(), con=(), limit_extra=""):
        sup, con = _rank(list(sup)), _rank(list(con))
        shown = (con if status == "contradicted" else sup)[:3]
        ids = [ev_row(h) for h in shown]
        primary = shown[0] if shown else None
        domains = {h.source.domain for h in sup} if status == "supported" else set()
        conf = _conf(primary.source.tier, primary.strength) if primary and status in ("supported", "contradicted") else ""
        if primary and primary.source.kind == "record":
            conf = "medium" if conf == "high" else conf
        lim = [BASE_LIMITS[ctype]]
        if limit_extra:
            lim.append(limit_extra)
        if status == "supported":
            if primary.source.tier >= 6:
                lim.append("Search snippet: a discovery lead, not proof.")
            if primary.source.kind == "page":
                lim.append("Self-reported text on the company's own website.")
            if primary.source.kind == "record":
                lim.append("Hand-collected record; source page not re-fetched by the pipeline.")
            if len(domains) <= 1:
                lim.append("Single source: not independently cross-verified.")
        if status == "not_supported":
            lim.append("Examined sources contain no supporting text; this is NOT evidence of absence.")
        if status == "unknown":
            lim.append("No relevant source was examined for this claim.")
        claims.append({
            "claim_id": stable_id("clm", company["company_id"], ctype), "company_id": company["company_id"],
            "claim_type": ctype, "claim_status": status,
            "source_url": primary.source.url if primary else "", "source_type": primary.source.source_type if primary else "",
            "page_title": primary.source.title if primary else "", "exact_text": primary.text if primary else "",
            "retrieved_at": primary.source.retrieved_at if primary else "",
            "publication_date": (primary.source.pub_date if primary else ""), "confidence_level": conf,
            "review_status": "auto_unreviewed", "limitations": " ".join(lim), "evidence_ids": ";".join(ids),
            "source_count": len(domains) if status == "supported" else (1 if shown else 0),
            "corroboration": ("none" if not shown else "single_source" if len(domains) <= 1 else "multiple_sources"),
            "source_authority_tier": primary.source.tier if primary else "", "assessed_by": "rules_v1"})

    # 1 product relevance (non-prepared mention)
    sup, con = _scan(sources, terms.PRODUCT_RE, require_product=False) if examined_any else ([], [])
    sup = [h for h in sup if not terms.is_prepared_only(h.text)] + _record_hits(sources, "product_relevance", now)
    if sup:
        emit("product_relevance", "supported", sup)
    elif not examined_any:
        emit("product_relevance", "unknown")
    else:
        prep, _ = _scan(sources, terms.PRODUCT_RE, require_product=False, allow_prepared=True)
        emit("product_relevance", "not_supported" if examined_pages or prep else "unknown", sup=prep,
             limit_extra=("Only prepared/roasted peanut products mentioned (outside raw scope)." if prep else ""))
        if prep:  # keep the informational evidence rows
            claims[-1]["claim_status"] = "not_supported"
    # 2-5 pattern claims (pages and records; snippets never support these)
    page_rec = ("page", "record")
    def pc(ctype, rx, **kw):
        kw.setdefault("kinds", page_rec)
        sup, con = _scan(sources, rx, **kw) if examined_any else ([], [])
        sup += _record_hits(sources, ctype, now)
        if not examined_any:
            return emit(ctype, "unknown")
        if sup:
            return emit(ctype, "supported", sup, con)
        if con:
            return emit(ctype, "contradicted", [], con)
        return emit(ctype, "not_supported" if examined_pages else "unknown")
    pc("sells_groundnuts", SELL_STRONG, kinds=("page",))
    pc("processes_groundnuts", terms.PROCESS_RE, kinds=("page",))
    pc("imports_raw_groundnuts", terms.IMPORT_RE)
    # 5 imports from India: explicit India mention + import/sourcing wording
    ind_re = re.compile(r"(?=.*\bIndia(?:n)?\b)(?=.*(?:\bimport\w*|\bsourc\w*|\bprocure\w*|\bpurchas\w*|\borigin\b|supplied\s+by))", re.I | re.S)
    sup, con = _scan(sources, ind_re, require_product=False, kinds=page_rec) if examined_any else ([], [])
    sup += _record_hits(sources, "imports_from_india", now)
    for h in sup:   # product nearby => stronger; otherwise window-level
        if not terms.PRODUCT_RE.search(h.text) and h.source.kind == "page":
            h.strength = "window"
    if not examined_any:
        emit("imports_from_india", "unknown")
    elif sup:
        emit("imports_from_india", "supported", sup, con)
    else:
        emit("imports_from_india", "not_supported" if examined_pages else "unknown")
    # 6 historical import activity
    sup, _ = _scan(sources, PAST_IMPORT, kinds=("page",)) if examined_any else ([], [])
    sup += _record_hits(sources, "historical_import_activity", now)
    emit("historical_import_activity", "supported" if sup else ("not_supported" if examined_pages else "unknown"), sup)
    # 7 current buying requirement: only dated open buyer leads
    cur = _record_hits(sources, "current_buying_requirement", now)
    emit("current_buying_requirement", "supported" if cur else "unknown", cur,
         limit_extra="" if cur else "No dated, open buyer lead found; website text cannot establish current intent.")
    # 8 importer / distributor role
    role_re = re.compile("|".join(terms.ROLE_PATTERNS[k].pattern for k in ("importer", "distributor", "wholesaler", "trader")), re.I)
    sup, con = _scan(sources, role_re, require_product=False, kinds=("page", "snippet")) if examined_any else ([], [])
    sup = [h for h in sup if not terms.NEGATION_RE.search(h.text)]
    sup += _record_hits(sources, "importer_or_distributor_role", now)
    if not examined_any:
        emit("importer_or_distributor_role", "unknown")
    elif sup:
        emit("importer_or_distributor_role", "supported", sup, con)
    elif con:
        emit("importer_or_distributor_role", "contradicted", [], con)
    else:
        emit("importer_or_distributor_role", "not_supported" if examined_pages else "unknown")
    # 9 company identity
    ident = []
    if prof and prof.get("extraction_status") in ("complete", "partial") and prof.get("name_source") not in ("", "discovery_name"):
        pg = next((p for p in prof["pages"] if not p.get("error") and p.get("title")), None)
        if pg:
            src = Source("page", 1, "official_company_website", pg["url"], pg["title"], pg["retrieved_at"],
                         prof.get("publication_date", ""), [pg["title"]], snapshot=pg["snapshot_html"],
                         text_kind="page_title", method="html_title")
            ident.append(Hit(src, pg["title"], "explicit", "page_title"))
    for s in sources:
        # only a hand-collected third-party record counts; our own seed input would be circular
        if s.kind == "record" and s.record["evidence_level"] == "manual_record" and s.record["raw_company_name"] \
                and s.record["country_raw"]:
            ident.append(Hit(s, f"{s.record['raw_company_name']} ({s.record['country_raw']})", "explicit", "record_fields"))
    emit("company_identity", "supported" if ident else ("not_supported" if examined_pages else "unknown"), ident,
         limit_extra="" if ident else "")
    # 10 public contact information
    contact = _contact_hits(ctx, prof)
    emit("public_contact_information",
         "supported" if contact else ("not_supported" if examined_pages else "unknown"), contact)
    return claims, list(evidence.values())


def run(ctx: Context) -> StageResult:
    res = StageResult("claims")
    companies = read_csv(ctx.paths.companies)
    cands = {c["candidate_id"]: c for c in read_csv(ctx.paths.candidates)}
    profiles = (read_json(ctx.paths.company_profiles, {}) or {}).get("profiles", [])
    prof_by_cand = {cid: p for p in profiles for cid in p.get("candidate_ids", [])}
    claims, evidence, errors = [], {}, 0
    for comp in companies:
        try:
            sources = build_sources(ctx, comp, cands, prof_by_cand)
            prof = next((prof_by_cand[c] for c in comp["candidate_ids"].split(";") if c in prof_by_cand), None)
            cl, ev = assess_company(ctx, comp, sources, prof, ctx.now)
            claims += cl
            for e in ev:
                evidence[e["evidence_id"]] = e
        except Exception as exc:
            errors += 1
            res.warn(f"Claim assessment failed for {comp['company_id']}: {exc}")
            ctx.log("claims", "company_error", company_id=comp["company_id"], error=str(exc))
    ev_rows = sorted(evidence.values(), key=lambda e: e["evidence_id"])
    if companies or ctx.paths.company_claims.exists():
        res.outputs += [str(write_csv(ctx.paths.evidence_log, ev_rows, EVIDENCE_FIELDS, ctx.paths, ctx.now)),
                        str(write_csv(ctx.paths.company_claims, claims, CLAIM_FIELDS, ctx.paths, ctx.now))]
    by = defaultdict(int)
    for c in claims:
        by[c["claim_status"]] += 1
    res.counts = {"companies": len(companies), "claims": len(claims), "evidence_rows": len(ev_rows),
                  **{f"claims_{k}": v for k, v in sorted(by.items())}, "company_errors": errors}
    if not companies:
        res.warn("No companies to assess.")
    res.status = STATUS_WARNINGS if res.warnings else STATUS_COMPLETED
    return res
