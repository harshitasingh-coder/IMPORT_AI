"""STAGE 3 - Buyer discovery (leads only).

Routes (all permitted/public or user-supplied):
  1. hand-collected leads already ingested by src.buyer_discovery (raw_importer_records.csv)
  2. optional data/inbox/discovery_seeds.csv  (directory / exhibitor / association pages you picked)
  3. search-engine results through an official API (Brave Search; needs BRAVE_SEARCH_API_KEY)

A search snippet is a discovery lead, NOT proof of importing or buying. Output is cumulative:
existing candidate rows are never removed or rewritten. Invalid rows go to discovery_rejects.csv.
Product-specific queries are generated from the profile, de-duplicated, capped, and cached.
"""

from __future__ import annotations

import hashlib
import json
import re
from urllib.parse import urlparse

import requests

from src import config
from src.pipeline import countries
from src.pipeline.common import (Context, StageResult, STATUS_COMPLETED, STATUS_WARNINGS, age_days,
                                 fold, iso, read_csv, read_json, stable_id, write_csv)
from src.pipeline.profile import load as load_profile
from src.pipeline.schemas import CANDIDATE_FIELDS, REJECT_FIELDS

QUERY_TEMPLATES = [
    "{term} importer {country}",
    "raw {term} distributor {country}",
    "shelled {term} wholesaler {country}",
    "{term} food ingredient supplier {country}",
]
QUERY_TERMS = ["peanut", "groundnut"]
QUERY_COUNTRY_NAME = {"VNM": "Vietnam"}  # the spelling people actually search with
SEARCH_CACHE_TTL_DAYS = 30
MAX_QUERIES_DEFAULT = 30
RESULTS_PER_QUERY = 10

# Domain classes -> (source_type, note). Everything else is an unclassified site.
TRADE_DATA_DOMAINS = {"volza.com", "importyeti.com", "zauba.com", "tridge.com", "panjiva.com",
                      "seair.co.in", "eximpulse.com"}
DIRECTORY_DOMAINS = {"alibaba.com", "made-in-china.com", "tradeindia.com", "indiamart.com",
                     "exportersindia.com", "tradekey.com", "ec21.com", "go4worldbusiness.com",
                     "globalsources.com", "europages.com", "kompass.com", "dnb.com",
                     "yellowpages.com", "ecplaza.net", "foodmarks.com", "tradewheel.com"}
SOCIAL_DOMAINS = {"linkedin.com", "facebook.com", "instagram.com", "twitter.com", "x.com",
                  "youtube.com", "tiktok.com", "pinterest.com"}
NON_COMPANY_DOMAINS = {"wikipedia.org", "reddit.com", "quora.com", "medium.com", "wikihow.com",
                       "britannica.com", "bbc.com", "reuters.com", "nytimes.com", "forbes.com"}
SOURCE_TYPES = {"buyer_lead", "shipment_record", "directory_listing", "company_website",
                "search_result", "trade_data", "social_profile"}

_SECOND_LEVEL = {"co", "com", "net", "org", "gov", "ac", "or", "go", "web", "edu", "ltd"}


# Free-hosting platforms: every subdomain is a different owner, so the full host is the identity.
SHARED_HOSTS = {"wixsite.com", "blogspot.com", "weebly.com", "wordpress.com", "myshopify.com", "github.io",
                "business.site", "squarespace.com", "webflow.io", "netlify.app", "vercel.app", "godaddysites.com",
                "site123.me", "jimdofree.com", "000webhostapp.com", "carrd.co", "ueniweb.com", "mystrikingly.com"}


def registrable_domain(host: str) -> str:
    host = (host or "").lower().strip().strip(".")
    host = re.sub(r"^www\d?\.", "", host)
    parts = host.split(".")
    if len(parts) <= 2 or re.fullmatch(r"[\d.]+|\[?[0-9a-f:]+\]?", host):
        return host  # short names and IP addresses are their own identity
    if len(parts[-1]) == 2 and parts[-2] in _SECOND_LEVEL:
        base = ".".join(parts[-3:])
    else:
        base = ".".join(parts[-2:])
    return host if base in SHARED_HOSTS else base


def normalize_url(url: str) -> str:
    """http(s) URL or '' when invalid. Adds https:// to bare domains."""
    url = (url or "").strip()
    if not url:
        return ""
    if not re.match(r"^[a-z][a-z0-9+.-]*://", url, re.I):
        url = "https://" + url
    p = urlparse(url)
    if p.scheme.lower() not in ("http", "https") or not p.netloc or "." not in p.netloc:
        return ""
    if re.search(r"\s", p.netloc):
        return ""
    return url


def domain_of(url: str) -> str:
    u = normalize_url(url)
    return registrable_domain(urlparse(u).hostname or "") if u else ""


def classify_domain(domain: str):
    """-> (source_type, note)"""
    if domain in TRADE_DATA_DOMAINS:
        return "trade_data", "trade-data aggregator"
    if domain in DIRECTORY_DOMAINS:
        return "directory_listing", "business directory / marketplace"
    if domain in SOCIAL_DOMAINS:
        return "social_profile", "social network page"
    if domain in NON_COMPANY_DOMAINS:
        return "non_company", "encyclopedia/news/forum page"
    if re.search(r"(^|\.)gov(\.[a-z]{2})?$|\.go\.id$|\.gov\.vn$|\.overheid\.nl$", domain):
        return "company_website", "government site (not a company)"  # handled in extraction
    return "company_website", "unclassified site (assumed company; extraction must confirm)"


# ---------------------------------------------------------------- search

class BraveSearch:
    URL = "https://api.search.brave.com/res/v1/web/search"

    def __init__(self, api_key, timeout=30):
        self.api_key, self.timeout = api_key, timeout

    def search(self, query, count=RESULTS_PER_QUERY):
        r = requests.get(self.URL, params={"q": query, "count": count},
                         headers={"X-Subscription-Token": self.api_key, "Accept": "application/json"},
                         timeout=self.timeout)
        if r.status_code != 200:
            raise RuntimeError(f"Brave Search HTTP {r.status_code}: {r.text[:200]}")
        return [{"title": x.get("title", ""), "url": x.get("url", ""), "snippet": x.get("description", "")}
                for x in (r.json().get("web", {}).get("results") or [])]


def build_queries(profile, max_queries=MAX_QUERIES_DEFAULT):
    """Deterministic, de-duplicated product-specific queries: [(query, iso3)]."""
    terms = [t for t in QUERY_TERMS if t in profile.get("strong_terms", QUERY_TERMS)] or QUERY_TERMS
    out, seen = [], set()
    for tpl in QUERY_TEMPLATES:
        for m in profile["target_markets"]:
            for term in terms:
                q = tpl.format(term=term, country=QUERY_COUNTRY_NAME.get(m["iso3"], m["country"]))
                if fold(q) not in seen:
                    seen.add(fold(q))
                    out.append((q, m["iso3"]))
    return out[:max_queries]


def _cached_search(ctx, provider, query):
    path = ctx.paths.discovery_cache / f"search_{hashlib.sha1(fold(query).encode()).hexdigest()[:12]}.json"
    cached = read_json(path)
    if cached and (age_days(cached.get("retrieved_at"), ctx.now) or 0) <= SEARCH_CACHE_TTL_DAYS:
        return cached["results"], cached["retrieved_at"], True
    if provider is None or ctx.offline:
        return None, None, False
    results = provider.search(query)
    ctx.paths.discovery_cache.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"query": query, "retrieved_at": iso(ctx.now), "results": results},
                               indent=1, ensure_ascii=False), encoding="utf-8")
    return results, iso(ctx.now), False


def _title_to_name(title):
    name = re.split(r"\s[-|–—:·»]\s", title or "")[0].strip()
    return name[:120]


# ---------------------------------------------------------------- candidate building

def make_candidate(ctx, *, name, url, source_type, method, query="", country_raw="", country_hint="",
                   country_basis="", title="", snippet="", reason="", evidence_level="",
                   source_name="", discovered_at="", extra=None):
    nurl = normalize_url(url)
    extra = extra or {}
    c = {f: "" for f in CANDIDATE_FIELDS}
    c.update({
        "run_id": ctx.run_id, "raw_company_name": (name or "").strip(), "source_url": nurl or (url or "").strip(),
        "source_type": source_type, "source_domain": domain_of(url), "source_name": source_name,
        "country_raw": country_raw, "country_hint": country_hint, "country_basis": country_basis,
        "discovery_method": method, "discovery_query": query, "discovered_at": discovered_at or iso(ctx.now),
        "page_title": title, "snippet": snippet, "initial_relevance_reason": reason,
        "evidence_level": evidence_level, **extra,
    })
    iso3 = countries.to_iso3(country_raw) or country_hint
    c["target_market"] = iso3 if iso3 in ctx.options["_target_isos"] else ""
    c["candidate_id"] = stable_id("cand", nurl or url, name, extra.get("source_record_id", ""))
    return c


def validate_candidate(c):
    """List of problems; empty list means valid."""
    problems = []
    if not c["raw_company_name"]:
        problems.append("missing company name")
    if not normalize_url(c["source_url"]):
        problems.append("invalid or missing source URL")
    if c["source_type"] == "non_company":
        problems.append("page is not a company/directory page (news, encyclopedia, forum)")
    if not c["discovery_method"]:
        problems.append("missing discovery method")
    return problems


def _mentions_product(text, profile):
    low = fold(text)
    return any(re.search(rf"\b{re.escape(t)}\b", low) for t in profile.get("strong_terms", []))


def from_manual_records(ctx, profile):
    """Route 1: hand-collected leads (schema: src.schemas.RAW_IMPORTER_FIELDS)."""
    out = []
    for r in read_csv(ctx.paths.raw_importer_records):
        text = " ".join([r.get("product_description_raw", ""), r.get("buying_requirement_raw", "")])
        has_prod = _mentions_product(text, profile)
        out.append(make_candidate(
            ctx, name=r.get("company_name_raw", ""), url=r.get("source_url", ""),
            source_type=r.get("source_type") or "buyer_lead", method=f"manual_ingest:{r.get('source_name','')}",
            country_raw=r.get("country_raw", ""), country_basis="source_record",
            title="", snippet=(r.get("product_description_raw") or r.get("buying_requirement_raw") or ""),
            reason=("record text mentions the product" if has_prod
                    else "record kept but no groundnut/peanut mention in its text"),
            evidence_level="manual_record", source_name=r.get("source_name", ""),
            discovered_at=r.get("collected_at", "") or iso(ctx.now),
            extra={"source_record_id": r.get("source_record_id", ""), "hs_code": r.get("hs_code", ""),
                   "shipment_date": r.get("shipment_date", ""), "origin_country": r.get("origin_country", ""),
                   "quantity_raw": r.get("quantity_raw", ""),
                   "buying_requirement_raw": r.get("buying_requirement_raw", ""),
                   "lead_posted_date": r.get("lead_posted_date", ""), "lead_status": r.get("lead_status", ""),
                   "website_raw": r.get("website_raw", ""),
                   "product_description_raw": r.get("product_description_raw", "")}))
    return out


def from_seeds(ctx, profile):
    """Route 2: seed pages picked by a person. Columns: raw_company_name, source_url, source_type,
    country, source_name, page_title, snippet."""
    out = []
    for r in read_csv(ctx.paths.discovery_seeds):
        stype = r.get("source_type") or classify_domain(domain_of(r.get("source_url", "")))[0]
        out.append(make_candidate(
            ctx, name=r.get("raw_company_name", ""), url=r.get("source_url", ""), source_type=stype,
            method=f"seed_file:{r.get('source_name') or 'inbox'}", country_raw=r.get("country", ""),
            country_basis="source_record" if r.get("country") else "", title=r.get("page_title", ""),
            snippet=r.get("snippet", ""), source_name=r.get("source_name", ""),
            reason="supplied seed page (directory/exhibitor/association/company)", evidence_level="seed_url"))
    return out


def from_search(ctx, profile, res):
    """Route 3: search API. Returns candidates; records errors without stopping."""
    provider = ctx.search_provider
    if provider is None and config.BRAVE_SEARCH_API_KEY and not ctx.offline:
        provider = BraveSearch(config.BRAVE_SEARCH_API_KEY)
    queries = build_queries(profile, ctx.options.get("max_queries", MAX_QUERIES_DEFAULT))
    out, ran, cached_n, failed = [], 0, 0, 0
    if provider is None and not any(
            read_json(ctx.paths.discovery_cache / f"search_{hashlib.sha1(fold(q).encode()).hexdigest()[:12]}.json")
            for q, _ in queries):
        res.warn("Search discovery skipped: no search provider (set BRAVE_SEARCH_API_KEY) and no cached results. "
                 "Only manual leads and seed pages were used.")
        return out
    for query, iso3 in queries:
        try:
            results, retrieved_at, was_cached = _cached_search(ctx, provider, query)
        except Exception as exc:
            failed += 1
            res.warn(f"Search failed for '{query}': {exc}")
            ctx.log("discovery", "search_error", query=query, error=str(exc))
            continue
        if results is None:
            continue
        ran += 0 if was_cached else 1
        cached_n += 1 if was_cached else 0
        for item in results:
            dom = domain_of(item.get("url", ""))
            stype, note = classify_domain(dom)
            title = item.get("title", "")
            has_prod = _mentions_product(f"{title} {item.get('snippet','')}", profile)
            out.append(make_candidate(
                ctx, name=_title_to_name(title) or dom, url=item.get("url", ""),
                source_type=stype if stype != "company_website" else "company_website",
                method="search_api", query=query, country_hint=iso3, country_basis="query_context",
                title=title, snippet=item.get("snippet", ""),
                reason=("snippet/title mentions the product; " if has_prod else "no product term in snippet; ")
                       + f"lead from query '{query}' ({note}); not proof of importing",
                evidence_level="lead_only_snippet", discovered_at=retrieved_at or iso(ctx.now)))
    res.counts.update({"queries_planned": len(queries), "queries_run": ran, "queries_from_cache": cached_n,
                       "queries_failed": failed})
    return out


def run(ctx: Context) -> StageResult:
    res = StageResult("discovery")
    profile = load_profile(ctx)
    ctx.options["_target_isos"] = {m["iso3"] for m in profile["target_markets"]}
    found = []
    for name, fn in (("manual_records", from_manual_records), ("seed_file", from_seeds)):
        try:
            batch = fn(ctx, profile)
            res.counts[name] = len(batch)
            found += batch
        except Exception as exc:
            res.errors.append(f"{name} route failed: {exc}")
            res.warn(f"{name} route failed: {exc}")
    try:
        found += from_search(ctx, profile, res)
    except Exception as exc:
        res.errors.append(f"search route failed: {exc}")
        res.warn(f"search route failed: {exc}")

    existing = {r["candidate_id"]: r for r in read_csv(ctx.paths.candidates)}
    merged, rejects, new = dict(existing), [], 0
    for c in found:
        problems = validate_candidate(c)
        if problems:
            rejects.append({"run_id": ctx.run_id, "raw_company_name": c["raw_company_name"],
                            "source_url": c["source_url"], "source_name": c["source_name"],
                            "reason": "; ".join(problems), "rejected_at": iso(ctx.now)})
            continue
        if c["candidate_id"] not in merged:
            merged[c["candidate_id"]] = c
            new += 1
    rows = sorted(merged.values(), key=lambda r: r["candidate_id"])
    if rows or existing:
        res.outputs.append(str(write_csv(ctx.paths.candidates, rows, CANDIDATE_FIELDS, ctx.paths, ctx.now)))
    if rejects:
        old = read_csv(ctx.paths.discovery_rejects)
        res.outputs.append(str(write_csv(ctx.paths.discovery_rejects, old + rejects, REJECT_FIELDS,
                                         ctx.paths, ctx.now)))
        res.warn(f"{len(rejects)} discovery rows rejected (see discovery_rejects.csv).")
    res.counts.update({"candidates_total": len(rows), "candidates_new": new, "rejected": len(rejects)})
    if not rows:
        res.warn("No company candidates available. Ingest leads (src.buyer_discovery), add "
                 "data/inbox/discovery_seeds.csv, or configure BRAVE_SEARCH_API_KEY.")
    res.status = STATUS_WARNINGS if res.warnings else STATUS_COMPLETED
    return res
