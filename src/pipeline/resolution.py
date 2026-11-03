"""STAGE 5 - Company normalization and de-duplication (MVP-level, deliberately conservative).

* Exact duplicates are merged ONLY on the same canonical company domain, or the same
  normalized name (legal suffix included) in the same country.
* Similar names are never merged: they produce suggestions (duplicate_suggestions.csv)
  and the status 'possible_duplicate'.
* Human decisions live in data/manual/company_merge_decisions.csv (read-only here):
  columns company_id_a, company_id_b, decision (merge|keep_separate), decided_by, decided_at, note.
* IDs are stable: avy-<ISO3>-<sha1[:10] of domain, else name+country>.
Statuses: unique | possible_duplicate | manual_review_required
"""

from __future__ import annotations

import re
from collections import defaultdict
from difflib import SequenceMatcher

from src.pipeline import countries
from src.pipeline.common import (Context, StageResult, STATUS_COMPLETED, STATUS_WARNINGS, fold, iso,
                                 read_csv, read_json, stable_id, write_csv)
from src.pipeline.discovery import classify_domain, domain_of
from src.pipeline.schemas import COMPANY_FIELDS, DUPLICATE_FIELDS

SIMILARITY_THRESHOLD = 0.88

# longest first; matched as whole-token sequences at the end (or start for prefix forms)
_SUFFIXES = ["sdn bhd", "pte ltd", "pvt ltd", "co ltd", "joint stock company", "pt tbk", "tbk", "gmbh",
             "limited", "ltd", "llc", "inc", "corp", "plc", "jsc", "tnhh", "bv", "nv", "vof", "srl",
             "sa", "cv", "ud", "pt", "co"]
_PREFIXES = ["cong ty tnhh", "cong ty co phan", "cong ty", "pt", "cv", "ud"]


def normalize_name(name: str):
    """-> (normalized_name_with_suffix, legal_suffix, name_core). Legal suffix is preserved."""
    text = fold(name).replace("&", " and ")
    text = text.replace("đ", "d")
    text = re.sub(r"(?<=\b[a-z])\.(?=[a-z]\b)", "", text)   # b.v. -> bv
    text = re.sub(r"[^\w\s]", " ", text)
    tokens = text.split()
    normalized = " ".join(tokens)
    suffix = ""
    for suf in _SUFFIXES:
        st = suf.split()
        if len(tokens) > len(st) and tokens[-len(st):] == st:
            suffix = suf
            break
    if not suffix:
        for pre in _PREFIXES:
            pt = pre.split()
            if len(tokens) > len(pt) and tokens[:len(pt)] == pt:
                suffix = pre
                break
    core = normalized
    if suffix:
        sp = suffix.split()
        core = " ".join(tokens[:-len(sp)] if tokens[-len(sp):] == sp else tokens[len(sp):])
    return normalized, suffix, core


def normalize_domain(value: str) -> str:
    return domain_of(value) if value else ""


def normalize_country(value) -> str:
    return countries.to_iso3(value)


def _stable_company_id(domain, norm_name, iso3):
    # the country must not change an ID that is derived from the domain
    if domain:
        return "avy-" + stable_id("h", domain)[2:], "domain"
    if iso3:
        return "avy-" + stable_id("h", norm_name, iso3)[2:], "name_country"
    return "avy-" + stable_id("h", norm_name)[2:], "name_only"


def _resolve_candidate(c, prof):
    """Domain/name/country for one candidate, with the source of each."""
    dom = ""
    name = c["raw_company_name"].strip()
    if prof and prof.get("domain"):
        dom = prof["domain"]
    if not dom:
        web = c.get("website_raw", "")
        if web and classify_domain(domain_of(web))[0] == "company_website":
            dom = domain_of(web)
    if not dom and c["source_type"] == "company_website" and classify_domain(c["source_domain"])[0] == "company_website":
        dom = c["source_domain"]
    names = [name]
    if prof and prof.get("company_name") and prof.get("name_source") not in ("", "discovery_name"):
        names.append(prof["company_name"])
    iso3, src = "", ""
    if prof and prof.get("country_iso3"):
        iso3, src = prof["country_iso3"], prof.get("country_source", "")
    if not iso3 and countries.to_iso3(c.get("country_raw", "")):
        iso3, src = countries.to_iso3(c["country_raw"]), "source_record"
    if not iso3 and c.get("country_hint"):
        iso3, src = c["country_hint"], "query_context"
    return dom, names, iso3, src


def resolve(candidates, profiles, merge_decisions, now_iso):
    """Pure function: -> (companies, suggestions, record_map)."""
    by_cand = {}
    for p in profiles:
        for cid in p.get("candidate_ids", []):
            by_cand[cid] = p
    groups = {}
    for c in candidates:
        prof = by_cand.get(c["candidate_id"])
        dom, names, iso3, csrc = _resolve_candidate(c, prof)
        canon = names[-1]  # extracted name preferred, discovery name otherwise
        norm, suffix, core = normalize_name(canon)
        cid, basis = _stable_company_id(dom, norm, iso3)
        g = groups.setdefault(cid, {
            "company_id": cid, "id_basis": basis, "domain": dom, "names": [], "country_iso3": iso3,
            "country_source": csrc, "candidates": [], "records": [], "website": (prof or {}).get("website", ""),
            "norm": norm, "suffix": suffix, "core": core, "canon": canon})
        g["names"] += names
        g["candidates"].append(c["candidate_id"])
        g["records"].append((c.get("source_record_id") or c["candidate_id"], basis))
        if not g["country_iso3"] and iso3:
            g["country_iso3"], g["country_source"] = iso3, csrc

    # manual merges (human decisions only)
    keep_sep, parent = set(), {}
    def find(x):
        while parent.get(x, x) != x:
            x = parent[x]
        return x
    for d in merge_decisions:
        a, b, dec = d.get("company_id_a", ""), d.get("company_id_b", ""), (d.get("decision") or "").lower()
        if a not in groups or b not in groups:
            continue
        if dec == "merge":
            parent[find(b)] = find(a)
        elif dec == "keep_separate":
            keep_sep.add(frozenset((a, b)))
    merged_flag = set()
    for cid in list(groups):
        root = find(cid)
        if root != cid:
            g, r = groups.pop(cid), groups[root]
            r["names"] += g["names"]; r["candidates"] += g["candidates"]; r["records"] += g["records"]
            r["website"] = r["website"] or g["website"]
            merged_flag.update((cid, root))

    suggestions, flagged = [], defaultdict(set)
    ids = sorted(groups)
    for i, a in enumerate(ids):
        for b in ids[i + 1:]:
            ga, gb = groups[a], groups[b]
            if frozenset((a, b)) in keep_sep:
                continue
            if ga["country_iso3"] and gb["country_iso3"] and ga["country_iso3"] != gb["country_iso3"]:
                continue
            reason, sim = "", 0.0
            if ga["norm"] == gb["norm"]:
                reason, sim = "same normalized name, different domain/country evidence", 1.0
            elif ga["core"] and ga["core"] == gb["core"]:
                reason, sim = "same name core, different legal suffix", 0.95
            else:
                sim = SequenceMatcher(None, ga["core"], gb["core"]).ratio()
                if sim >= SIMILARITY_THRESHOLD:
                    reason = "similar names"
            if reason:
                suggestions.append({"company_id_a": a, "company_id_b": b, "reason": reason,
                                    "similarity": round(sim, 3), "status": "suggested_not_merged",
                                    "suggested_at": now_iso})
                flagged[a].add(b); flagged[b].add(a)

    companies, rmap = [], []
    for cid in ids:
        g = groups[cid]
        names = list(dict.fromkeys(n for n in g["names"] if n))
        canon = g["canon"]
        aliases = [n for n in names if n != canon]
        notes, status = [], "unique"
        n_cand = len(set(g["candidates"]))
        if n_cand > 1:
            notes.append(f"{n_cand} source records merged on exact {g['id_basis'].replace('_', '+')}")
        if g["id_basis"] == "name_only":
            status = "manual_review_required"
            notes.append("name only: no domain and no country evidence")
        elif g["country_source"] == "query_context" and g["id_basis"] != "domain":
            status = "manual_review_required"
            notes.append("country only inferred from the search query")
        elif cid in flagged:
            status = "possible_duplicate"
            notes.append("similar to " + ", ".join(sorted(flagged[cid])))
        companies.append({
            "company_id": cid, "id_basis": g["id_basis"], "company_name": canon, "legal_suffix": g["suffix"],
            "normalized_name": g["norm"], "domain": g["domain"], "country_iso3": g["country_iso3"],
            "country_source": g["country_source"], "website": g["website"], "aliases": " | ".join(aliases),
            "candidate_ids": ";".join(sorted(set(g["candidates"]))),
            "source_record_ids": ";".join(sorted({r for r, _ in g["records"]})),
            "dedup_status": status, "dedup_notes": "; ".join(notes),
            "manual_merge_applied": "yes" if cid in merged_flag else "no"})
        method = {"domain": "same_domain", "name_country": "exact_name_country", "name_only": "manual_review"}[g["id_basis"]]
        for rec, _ in g["records"]:
            rmap.append({"source_record_id": rec, "company_id": cid,
                         "match_method": "manual_merge" if cid in merged_flag else method})
    return companies, suggestions, rmap


def run(ctx: Context) -> StageResult:
    res = StageResult("resolution")
    cands = read_csv(ctx.paths.candidates)
    profiles = (read_json(ctx.paths.company_profiles, {}) or {}).get("profiles", [])
    decisions = read_csv(ctx.paths.manual_merges)
    companies, sugg, rmap = resolve(cands, profiles, decisions, iso(ctx.now))
    for path, rows, fields in ((ctx.paths.companies, companies, COMPANY_FIELDS),
                               (ctx.paths.duplicate_suggestions, sugg, DUPLICATE_FIELDS),
                               (ctx.paths.record_company_map, rmap, ["source_record_id", "company_id", "match_method"])):
        if rows or path.exists():
            res.outputs.append(str(write_csv(path, rows, fields, ctx.paths, ctx.now)))
    counts = defaultdict(int)
    for c in companies:
        counts[c["dedup_status"]] += 1
    res.counts = {"candidates": len(cands), "companies": len(companies), "duplicate_suggestions": len(sugg),
                  "manual_merge_decisions_read": len(decisions), **{f"status_{k}": v for k, v in counts.items()}}
    if counts.get("manual_review_required"):
        res.warn(f"{counts['manual_review_required']} company record(s) need manual identity review.")
    if sugg:
        res.warn(f"{len(sugg)} possible duplicate pair(s) suggested; none were merged automatically.")
    res.status = STATUS_WARNINGS if res.warnings else STATUS_COMPLETED
    return res
