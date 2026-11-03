"""STAGE 4 - Company website extraction.

Fetches permitted public pages (robots.txt respected, no CAPTCHA/login/paywall bypass),
saves raw snapshots under data/raw/enrichment/<domain>/ and structured profiles to
data/processed/company_profiles.json. Extraction is rule-based and deterministic: every
field is copied from page text; missing fields stay empty and are listed in missing_fields.
Cached snapshots newer than the TTL are reused instead of fetching again.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
import urllib.robotparser
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from src.pipeline import countries, terms
from src.pipeline.common import (Context, StageResult, STATUS_COMPLETED, STATUS_WARNINGS, age_days,
                                 fold, iso, read_csv, read_json, slug, utcnow, write_json)
from src.pipeline.discovery import classify_domain, domain_of, normalize_url, registrable_domain
from src.pipeline.profile import load as load_profile

USER_AGENT = "AVYUKT-ResearchBot/0.1 (public-page research; respects robots.txt)"
SNAPSHOT_TTL_DAYS = 14
FAILURE_TTL_DAYS = 1
MAX_PAGES_PER_SITE = 4
MAX_BYTES = 2_000_000
MIN_INTERVAL = 1.5
DEFAULT_MAX_SITES = 60

PAGE_HINTS = {
    "about": re.compile(r"about|company|profile|who-we-are|tentang|gioi-?thieu|over-ons|over-ons|perusahaan", re.I),
    "contact": re.compile(r"contact|kontak|lien-?he|hubungi|neem-contact|get-in-touch", re.I),
    "product": re.compile(r"product|produk|san-?pham|producten|peanut|groundnut|nuts|commodit", re.I),
}
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}")
BAD_EMAIL = re.compile(r"\.(png|jpe?g|gif|svg|webp|css|js)$|^(?:name|user|you|email|example)@|"
                       r"@(?:example\.|sentry|wixpress|domain\.)", re.I)
PHONE_LABEL = re.compile(r"\b(?:tel|telp|phone|mobile|hotline|call|whatsapp|wa|hp|fax)\b|điện\s+thoại|sđt|telefoon", re.I)
PHONE_RE = re.compile(r"(?<![\w/])(\+?\d[\d\s().-]{6,}\d)(?![\w/])")
ADDRESS_LABEL = re.compile(r"^(?:address|alamat|địa\s+chỉ|adres|head\s+office|office)\s*[:\-]\s*(.{12,200})$", re.I)
CAPTCHA_RE = re.compile(r"captcha|are you a robot|verify you are human|access denied", re.I)


@dataclass
class FetchResult:
    url: str
    final_url: str = ""
    status_code: int = 0
    content_type: str = ""
    body: str = ""
    error: str = ""
    retrieved_at: str = ""
    blocked: bool = False
    from_cache: bool = False


class HttpFetcher:
    """Polite network layer: robots.txt, per-host delay, timeouts, size cap, no retries on blocks."""

    def __init__(self, min_interval=MIN_INTERVAL, timeout=20):
        self.min_interval, self.timeout = min_interval, timeout
        self._robots, self._last = {}, {}
        self.session = requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT

    def _allowed(self, url):
        p = urlparse(url)
        origin = f"{p.scheme}://{p.netloc}"
        if origin not in self._robots:
            rp = urllib.robotparser.RobotFileParser()
            try:
                r = self.session.get(origin + "/robots.txt", timeout=10)
                if r.status_code in (404, 410):
                    rp.parse([])
                elif r.status_code == 200:
                    rp.parse(r.text.splitlines())
                else:
                    rp = None  # robots unavailable for another reason: be conservative
            except requests.RequestException:
                rp = None
            self._robots[origin] = rp
        rp = self._robots[origin]
        return bool(rp and rp.can_fetch(USER_AGENT, url))

    def __call__(self, url):
        res = FetchResult(url=url, retrieved_at=iso(utcnow()))
        if not self._allowed(url):
            res.blocked, res.error = True, "blocked_by_robots_or_robots_unavailable"
            return res
        host = urlparse(url).netloc
        wait = self.min_interval - (time.time() - self._last.get(host, 0))
        if wait > 0:
            time.sleep(wait)
        try:
            r = self.session.get(url, timeout=self.timeout, stream=True, allow_redirects=True)
            self._last[host] = time.time()
            res.status_code, res.final_url = r.status_code, r.url
            res.content_type = r.headers.get("Content-Type", "")
            if r.status_code in (401, 403, 429):
                res.blocked, res.error = True, f"http_{r.status_code}_access_restricted"
                return res
            if r.status_code != 200:
                res.error = f"http_{r.status_code}"
                return res
            if "html" not in res.content_type.lower():
                res.error = f"not_html:{res.content_type[:40]}"
                return res
            raw = r.raw.read(MAX_BYTES, decode_content=True)
            res.body = raw.decode(r.encoding or "utf-8", errors="replace")
            if CAPTCHA_RE.search(res.body[:5000]) and len(res.body) < 20000:
                res.blocked, res.error, res.body = True, "captcha_or_access_challenge", ""
        except requests.RequestException as exc:
            res.error = f"{type(exc).__name__}: {str(exc)[:120]}"
        return res


# ---------------------------------------------------------------- parsing

def _visible_text(soup):
    for t in soup(["script", "style", "noscript", "svg", "template", "iframe"]):
        t.decompose()
    for br in soup.find_all("br"):
        br.replace_with("\n")
    text = soup.get_text("\n")
    lines = [" ".join(l.split()) for l in text.split("\n")]
    return "\n".join(l for l in lines if l)


def _jsonld(soup):
    out = []
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
        except (ValueError, TypeError):
            continue
        stack = data if isinstance(data, list) else [data]
        while stack:
            d = stack.pop()
            if isinstance(d, dict):
                out.append(d)
                for v in d.values():
                    if isinstance(v, (dict, list)):
                        stack.extend(v if isinstance(v, list) else [v])
            elif isinstance(d, list):
                stack.extend(d)
    return out


def _addr_from_ld(d):
    a = d.get("address")
    if isinstance(a, dict):
        parts = [a.get("streetAddress"), a.get("addressLocality"), a.get("addressRegion"),
                 a.get("postalCode")]
        c = a.get("addressCountry")
        c = c.get("name") if isinstance(c, dict) else c
        parts.append(c)
        return ", ".join(str(p) for p in parts if p), (c or "")
    if isinstance(a, str):
        return a, ""
    return "", ""


def parse_page(html: str, url: str) -> dict:
    """Structured fields from one HTML page. Pure function."""
    soup = BeautifulSoup(html or "", "html.parser")
    title = " ".join((soup.title.string or "").split()) if soup.title and soup.title.string else ""
    meta = lambda **kw: (soup.find("meta", attrs=kw) or {}).get("content", "").strip()
    description = meta(name="description") or meta(property="og:description")
    canonical = ""
    link = soup.find("link", rel="canonical")
    if link and link.get("href"):
        canonical = urljoin(url, link["href"])
    ld = _jsonld(soup)
    org = next((d for d in ld if str(d.get("@type", "")).lower().endswith(
        ("organization", "localbusiness", "corporation", "foodestablishment", "store"))), None)
    links = []
    for a in soup.find_all("a", href=True):
        href = urljoin(url, a["href"])
        if href.startswith("http"):
            links.append((href.split("#")[0], " ".join(a.get_text().split())[:80]))
    mailtos = [a["href"][7:].split("?")[0] for a in soup.find_all("a", href=True)
               if a["href"].lower().startswith("mailto:")]
    tels = [a["href"][4:] for a in soup.find_all("a", href=True) if a["href"].lower().startswith("tel:")]
    pub = (meta(property="article:published_time") or meta(name="date") or meta(itemprop="datePublished")
           or next((str(d["datePublished"]) for d in ld if d.get("datePublished")), ""))
    addr_node = soup.find("address")
    addr_text = " ".join(addr_node.get_text(" ").split()) if addr_node else ""
    og_site = meta(property="og:site_name")
    text = _visible_text(soup)

    emails = []
    for e in mailtos + EMAIL_RE.findall(text):
        e = e.strip().lower()
        if e and not BAD_EMAIL.search(e) and e not in emails:
            emails.append(e)
    phones = []
    for t in tels:
        t = " ".join(t.split())
        if 8 <= len(re.sub(r"\D", "", t)) <= 15 and t not in phones:
            phones.append(t)
    for line in text.split("\n"):
        if len(line) > 200:
            continue
        for m in PHONE_RE.finditer(line):
            num = " ".join(m.group(1).split())
            digits = len(re.sub(r"\D", "", num))
            labelled = PHONE_LABEL.search(line) and not re.search(r"\bfax\b", line, re.I)
            if 8 <= digits <= 15 and (num.startswith("+") or labelled) and num not in phones:
                phones.append(num)

    address, country_txt = "", ""
    if org:
        address, country_txt = _addr_from_ld(org)
    if not address and addr_text:
        address = addr_text
    if not address:
        for line in text.split("\n"):
            m = ADDRESS_LABEL.match(line.strip())
            if m:
                address = m.group(1).strip()
                break
    return {
        "url": url, "title": title, "meta_description": description, "canonical_url": canonical,
        "org_name": (org or {}).get("name", "") if org else "", "og_site_name": og_site,
        "text": text, "emails": emails, "phones": phones, "address": address,
        "country_text": country_txt, "publication_date": pub, "links": links,
    }


def select_links(links, root_domain, base_url):
    """First same-site link per page type (about/contact/product)."""
    chosen = {}
    for href, label in links:
        if domain_of(href) != root_domain:
            continue
        path = urlparse(href).path or "/"
        for ptype, rx in PAGE_HINTS.items():
            if ptype not in chosen and (rx.search(path) or rx.search(label)) and href.rstrip("/") != base_url.rstrip("/"):
                chosen[ptype] = href
    return chosen


# ---------------------------------------------------------------- snapshots / cache

def snapshot_paths(ctx, url):
    d = ctx.paths.enrichment / (slug(domain_of(url) or urlparse(url).netloc) or "unknown")
    h = hashlib.sha1(url.encode()).hexdigest()[:12]
    return d, d / f"{h}.json", d / f"{h}.html", d / f"{h}.txt"


def get_page(ctx: Context, url: str) -> FetchResult:
    """Cache-first page retrieval. Writes html/txt/meta snapshots. Never raises."""
    d, meta_p, html_p, txt_p = snapshot_paths(ctx, url)
    meta = read_json(meta_p)
    if meta:
        a = age_days(meta["retrieved_at"], ctx.now) or 0
        ttl = FAILURE_TTL_DAYS if meta.get("error") else SNAPSHOT_TTL_DAYS
        if ctx.offline or a <= ttl:
            body = html_p.read_text(encoding="utf-8") if html_p.exists() else ""
            return FetchResult(url=url, final_url=meta.get("final_url", ""), status_code=meta["status_code"],
                               content_type=meta.get("content_type", ""), body=body, error=meta.get("error", ""),
                               retrieved_at=meta["retrieved_at"], blocked=meta.get("blocked", False),
                               from_cache=True)
    if ctx.offline or ctx.fetcher is None:
        return FetchResult(url=url, error="not_fetched_offline", retrieved_at=iso(ctx.now))
    try:
        res = ctx.fetcher(url)
    except Exception as exc:  # a fetcher bug must not stop the pipeline
        res = FetchResult(url=url, error=f"fetcher_exception: {exc}", retrieved_at=iso(ctx.now))
    d.mkdir(parents=True, exist_ok=True)
    if res.body:
        html_p.write_text(res.body, encoding="utf-8")
        txt_p.write_text(parse_page(res.body, url)["text"], encoding="utf-8")
    meta_p.write_text(json.dumps({
        "url": url, "final_url": res.final_url, "status_code": res.status_code,
        "content_type": res.content_type, "retrieved_at": res.retrieved_at or iso(ctx.now),
        "error": res.error, "blocked": res.blocked,
        "html_snapshot": html_p.name if res.body else "", "text_snapshot": txt_p.name if res.body else "",
    }, indent=1), encoding="utf-8")
    return res


# ---------------------------------------------------------------- site targets

def site_target(c):
    """(entry_url, root_url, reason_if_none) for a candidate."""
    dom = domain_of(c["source_url"])
    stype, note = classify_domain(dom)
    web = normalize_url(c.get("website_raw", ""))
    if c["source_type"] == "company_website" and stype == "company_website" and "government" not in note:
        entry = normalize_url(c["source_url"])
        p = urlparse(entry)
        return entry, f"{p.scheme}://{p.netloc}/", ""
    if web:
        wd = domain_of(web)
        if classify_domain(wd)[0] == "company_website":
            p = urlparse(web)
            return web, f"{p.scheme}://{p.netloc}/", ""
    if "government" in note:
        return None, None, "skipped_government_site"
    if stype in ("directory_listing", "trade_data", "social_profile") or c["source_type"] in (
            "directory_listing", "trade_data", "social_profile", "shipment_record", "buyer_lead"):
        return None, None, "skipped_third_party_no_company_site"
    return None, None, "skipped_no_site"


def _pick_name(parsed_pages, fallback):
    for p in parsed_pages:
        if p["org_name"]:
            return p["org_name"].strip(), "json_ld_organization"
    for p in parsed_pages:
        if p["og_site_name"]:
            return p["og_site_name"].strip(), "og_site_name"
    for p in parsed_pages:
        first = re.split(r"\s[-|–—:·»]\s", p["title"] or "")
        for part in first:
            if part and fold(part) not in {"home", "homepage", "welcome", "index", "beranda", "trang chu"}:
                return part.strip(), "page_title"
    return fallback, "discovery_name"


def extract_site(ctx: Context, key, cands, entry, root) -> dict:
    """Fetch + parse one site. Always returns a profile dict, never raises."""
    domain = domain_of(root)
    prof = {
        "profile_key": key, "domain": domain, "website": root, "canonical_domain": domain,
        "candidate_ids": [c["candidate_id"] for c in cands], "company_name": "", "name_source": "",
        "country_iso3": "", "country_source": "", "business_description": "", "business_type": "",
        "business_types": [], "product_categories": [], "groundnut_relevance": "unknown",
        "product_mention_count": 0, "role_terms": [], "public_emails": [], "public_phones": [],
        "public_address": "", "contact_page": "", "about_page": "", "product_page": "",
        "source_url": entry, "page_title": "", "retrieved_at": "", "publication_date": "",
        "extraction_status": "failed", "missing_fields": [], "pages": [], "errors": [],
        "site_class": "company_website_unconfirmed",
    }
    ok_pages = []

    def fetch_one(url, ptype):
        fr = get_page(ctx, url)
        page = {"url": url, "page_type": ptype, "http_status": fr.status_code, "error": fr.error,
                "retrieved_at": fr.retrieved_at, "from_cache": fr.from_cache, "snapshot_html": "",
                "snapshot_text": "", "title": ""}
        if fr.body and not fr.error:
            _, _, hp, tp = snapshot_paths(ctx, url)
            page["snapshot_html"] = str(hp.relative_to(ctx.paths.data))
            page["snapshot_text"] = str(tp.relative_to(ctx.paths.data))
            parsed = parse_page(fr.body, fr.final_url or url)
            parsed["page_type"], parsed["retrieved_at"] = ptype, fr.retrieved_at
            page["title"] = parsed["title"]
            ok_pages.append(parsed)
        elif fr.error:
            prof["errors"].append(f"{url}: {fr.error}")
        prof["pages"].append(page)
        return fr

    try:
        entry_is_root = entry.rstrip("/") == root.rstrip("/")
        order = [(entry, "source_page")] if not entry_is_root else []
        order.append((root, "home"))
        links = []
        for url, ptype in order:
            if len(prof["pages"]) >= MAX_PAGES_PER_SITE:
                break
            fetch_one(url, ptype)
            if ok_pages and ptype == "home":
                links = ok_pages[-1]["links"]
        home = next((p for p in ok_pages if p["page_type"] in ("home", "source_page")), None)
        if home:
            links = links or home["links"]
            for ptype, href in select_links(links, domain, root).items():
                if len(prof["pages"]) >= MAX_PAGES_PER_SITE:
                    break
                if any(pg["url"] == href for pg in prof["pages"]):
                    continue
                fetch_one(href, ptype)
                prof[f"{ptype}_page"] = href
    except Exception as exc:  # isolate per-site failures
        prof["errors"].append(f"unexpected extraction error: {exc}")
        ctx.log("extraction", "site_error", domain=domain, error=str(exc))

    if not ok_pages:
        errs = " ".join(prof["errors"])
        prof["extraction_status"] = ("blocked" if "robots" in errs or "access_restricted" in errs
                                     or "captcha" in errs else
                                     "not_fetched_offline" if "not_fetched_offline" in errs else "failed")
        prof["missing_fields"] = ["all"]
        return prof

    texts = [p["text"] for p in ok_pages]
    prof["company_name"], prof["name_source"] = _pick_name(ok_pages, cands[0]["raw_company_name"])
    home = next((p for p in ok_pages if p["page_type"] in ("home", "source_page")), ok_pages[0])
    prof["page_title"] = home["title"]
    prof["retrieved_at"] = home["retrieved_at"]
    prof["publication_date"] = next((p["publication_date"] for p in ok_pages if p["publication_date"]), "")
    desc = next((p["meta_description"] for p in ok_pages if p["meta_description"]), "")
    if not desc:
        desc = next((l for p in ok_pages if p["page_type"] in ("about", "home", "source_page")
                     for l in p["text"].split("\n") if len(l) >= 80), "")
    prof["business_description"] = desc[:500]
    prof["public_emails"] = list(dict.fromkeys(e for p in ok_pages for e in p["emails"]))[:5]
    prof["public_phones"] = list(dict.fromkeys(t for p in ok_pages for t in p["phones"]))[:5]
    prof["public_address"] = next((p["address"] for p in ok_pages if p["address"]), "")
    # country: structured page data first, then candidate source record, then ccTLD
    ctext = next((p["country_text"] for p in ok_pages if p["country_text"]), "")
    iso3 = countries.to_iso3(ctext) or countries.find_country_in_text(prof["public_address"])
    if iso3:
        prof["country_iso3"], prof["country_source"] = iso3, "page_address"
    else:
        for c in cands:
            iso3 = countries.to_iso3(c.get("country_raw", ""))
            if iso3:
                prof["country_iso3"], prof["country_source"] = iso3, "source_record"
                break
        else:
            tld = countries.CCTLD_TO_ISO3.get(domain.rsplit(".", 1)[-1])
            if tld:
                prof["country_iso3"], prof["country_source"] = tld, "tld"
    # role terms, categories, product relevance
    alltext = "\n".join(texts)
    for role, rx in terms.ROLE_PATTERNS.items():
        hit = next((s for s in terms.sentences(alltext) if rx.search(s) and len(s) <= 300), None)
        if hit:
            prof["role_terms"].append({"role": role, "example": hit})
    prof["business_types"] = [r["role"] for r in prof["role_terms"]]
    prof["business_type"] = next((r for r in terms.ROLE_PRIORITY if r in prof["business_types"]), "unknown")
    prof["product_categories"] = [k for k, rx in terms.CATEGORY_PATTERNS.items() if rx.search(alltext)]
    prod_sents = [s for s in terms.sentences(alltext) if terms.PRODUCT_RE.search(s)]
    prof["product_mention_count"] = len(prod_sents)
    if not prod_sents:
        prof["groundnut_relevance"] = "none_found_in_fetched_pages"
    elif all(terms.is_prepared_only(s) for s in prod_sents):
        prof["groundnut_relevance"] = "prepared_only"
    else:
        prof["groundnut_relevance"] = "direct"
    cp = next((p for p in ok_pages if p["page_type"] == "contact"), None)
    prof["contact_page"] = cp["url"] if cp else prof["contact_page"]
    missing = [k for k, v in {"company_name": prof["company_name"], "country": prof["country_iso3"],
                              "business_description": prof["business_description"],
                              "public_contact": prof["public_emails"] or prof["public_phones"] or prof["public_address"],
                              "product_mention": prod_sents, "publication_date": prof["publication_date"],
                              "contact_page": prof["contact_page"], "about_page": prof["about_page"],
                              "product_page": prof["product_page"]}.items() if not v]
    prof["missing_fields"] = missing
    failed_pages = [p for p in prof["pages"] if p["error"]]
    prof["extraction_status"] = "complete" if len(ok_pages) >= 2 and not failed_pages else "partial"
    return prof


def run(ctx: Context) -> StageResult:
    res = StageResult("extraction")
    load_profile(ctx)
    cands = read_csv(ctx.paths.candidates)
    if ctx.fetcher is None and not ctx.offline:
        ctx.fetcher = HttpFetcher()
    groups, skipped = {}, []
    for c in cands:
        entry, root, why = site_target(c)
        if not entry:
            skipped.append({"profile_key": c["candidate_id"], "candidate_ids": [c["candidate_id"]],
                            "company_name": c["raw_company_name"], "source_url": c["source_url"],
                            "website": "", "domain": "", "extraction_status": why,
                            "missing_fields": ["all"], "pages": [], "errors": [], "country_iso3": "",
                            "country_source": "", "business_type": "", "public_emails": [], "public_phones": [],
                            "public_address": "", "groundnut_relevance": "unknown", "role_terms": [],
                            "product_categories": [], "business_description": "", "retrieved_at": "",
                            "page_title": c.get("page_title", ""), "publication_date": "", "name_source": "discovery_name",
                            "site_class": why})
            continue
        key = domain_of(root)
        g = groups.setdefault(key, {"entry": entry, "root": root, "cands": []})
        g["cands"].append(c)
    limit = ctx.options.get("max_sites", DEFAULT_MAX_SITES)
    profiles = []
    for i, (key, g) in enumerate(sorted(groups.items())):
        if i >= limit:
            res.warn(f"max_sites={limit} reached; {len(groups) - limit} sites not processed this run.")
            break
        profiles.append(extract_site(ctx, key, g["cands"], g["entry"], g["root"]))
    profiles += skipped
    statuses = {}
    for p in profiles:
        statuses[p["extraction_status"]] = statuses.get(p["extraction_status"], 0) + 1
    out = {"schema_version": "1.0", "run_id": ctx.run_id, "generated_at": iso(ctx.now), "profiles": profiles}
    if profiles or ctx.paths.company_profiles.exists():
        res.outputs.append(str(write_json(ctx.paths.company_profiles, out, ctx.paths, ctx.now)))
    res.counts = {"candidates": len(cands), "sites_targeted": len(groups), "profiles": len(profiles),
                  **{f"status_{k}": v for k, v in sorted(statuses.items())}}
    bad = sum(v for k, v in statuses.items() if k in ("failed", "blocked", "not_fetched_offline", "partial"))
    if bad:
        res.warn(f"{bad} site(s) failed, were blocked, offline-unfetched or only partially extracted.")
    if not cands:
        res.warn("No candidates to extract (company_candidates_raw.csv empty).")
    res.status = STATUS_WARNINGS if res.warnings else STATUS_COMPLETED
    return res
