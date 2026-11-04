import csv
import json
import unittest

from src import comtrade_client
from src.pipeline import (claims, discovery, export, extraction, market, profile, qualification,
                          resolution, runner, scoring)
from src.pipeline.common import read_csv, read_json
from tests.helpers import (FakeFetcher, FakeSearch, NOW, PipelineTestCase, RETRIEVED, SITES, record,
                           write_csv_rows, write_raw_records)


def search_result(url, title, snippet):
    return {"title": title, "url": url, "snippet": snippet}


class Stage1Profile(PipelineTestCase):
    def test_normalizes_existing_pilot_profile_and_keeps_hs_unverified(self):
        r = profile.run(self.ctx())
        self.assertIn(r.status, ("completed", "completed_with_warnings"))
        p = read_json(self.paths.product_profile)
        self.assertEqual(p["hs_verification_summary"], "unverified")
        self.assertEqual({f["hs_code"] for f in p["product_forms"]}, {"120241", "120242"})
        self.assertTrue(all(f["verification_status"] == "needs_verification" for f in p["product_forms"]))
        self.assertEqual([m["iso3"] for m in p["target_markets"]], ["IDN", "VNM", "NLD"])
        self.assertEqual(p["excluded_forms"][0]["hs_code"], "200811")
        self.assertNotIn("bold", p["strong_terms"])
        self.assertIn("bold", p["weak_terms"])

    def test_cannot_self_verify_hs_code(self):
        raw = {"product_name": "Groundnuts", "origin": "India", "target_countries": ["Indonesia"],
               "product_forms": [{"form": "Shelled", "hs_code": "120242", "verification_status": "verified"}]}
        p = profile.normalize_profile(raw, NOW)
        self.assertEqual(p["product_forms"][0]["verification_status"], "needs_verification")
        self.assertTrue(any("verification_source" in w for w in p["warnings"]))
        raw["product_forms"][0]["verification_source"] = "ID customs tariff 2026"
        p = profile.normalize_profile(raw, NOW)
        self.assertEqual(p["product_forms"][0]["verification_status"], "verified")

    def test_minimal_input_normalizes_and_invalid_input_fails(self):
        p = profile.normalize_profile({"product_name": " Peanuts ", "origin": "india", "hs_code": "1202.42",
                                       "target_countries": ["Vietnam", "viet nam", "NL"]}, NOW)
        self.assertEqual(p["origin_iso3"], "IND")
        self.assertEqual([m["iso3"] for m in p["target_markets"]], ["VNM", "NLD"])  # de-duplicated
        self.assertEqual(p["product_forms"][0]["hs_code"], "120242")
        with self.assertRaises(profile.ProfileError):
            profile.normalize_profile({"product_name": "X", "origin": "India", "target_countries": ["Atlantis"]}, NOW)
        with self.assertRaises(profile.ProfileError):
            profile.normalize_profile({"product_name": "", "origin": "India", "target_countries": ["Indonesia"]}, NOW)

    def test_profile_id_is_stable(self):
        raw = json.loads((self.data / "product_profile.json").read_text())
        self.assertEqual(profile.normalize_profile(raw, NOW)["profile_id"], profile.normalize_profile(raw, NOW)["profile_id"])

    def test_stage_does_not_modify_existing_input_profile(self):
        before = (self.data / "product_profile.json").read_text()
        profile.run(self.ctx())
        self.assertEqual(before, (self.data / "product_profile.json").read_text())


class Stage2Market(PipelineTestCase):
    def rows(self):
        def r(flow, period, rep, iso, partner, piso, val, w=None):
            return {"product_name": "Groundnuts", "hs_code": "120242", "hs_description": "x", "trade_flow": flow,
                    "period": str(period), "reporter_country": rep, "reporter_iso": iso, "partner_country": partner,
                    "partner_iso": piso, "trade_value_usd": str(val), "net_weight_kg": str(w or ""), "quantity": "",
                    "quantity_unit": "", "is_reported": "True", "source_url_or_id": "UN Comtrade API v1 | test",
                    "retrieved_at": "2026-10-18T10:00:00+00:00"}
        out = []
        for yr, idn, vnm, nld in ((2022, 100, 80, 200), (2023, 150, 90, 220), (2024, 200, 85, 240)):
            out += [r("Import", yr, "Indonesia", "IDN", "World", "W00", idn * 1e6, idn * 1e6 / 1.5),
                    r("Import", yr, "Viet Nam", "VNM", "World", "W00", vnm * 1e6),
                    r("Import", yr, "Netherlands", "NLD", "World", "W00", nld * 1e6),
                    r("Export", yr, "India", "IND", "Indonesia", "IDN", idn * 0.7e6),
                    r("Export", yr, "India", "IND", "Netherlands", "NLD", nld * 0.02e6)]
        return out

    def seed_market_data(self):
        write_csv_rows(self.paths.market_data, comtrade_client.CSV_FIELDS, self.rows())

    def test_comtrade_parsing(self):
        body = {"data": [{"cmdCode": "120242", "cmdDesc": "Groundnuts", "flowDesc": "Import", "period": "2023",
                          "reporterDesc": "Testland", "reporterISO": "IDN", "partnerDesc": "World", "partnerISO": "W00",
                          "primaryValue": 1234.5, "netWgt": 10, "qty": 0, "qtyUnitAbbr": "kg", "isReported": True}]}
        rows = comtrade_client.to_rows(body, "Groundnuts", "src", RETRIEVED)
        self.assertEqual(rows[0]["trade_value_usd"], 1234.5)
        self.assertIsNone(rows[0]["quantity"])
        typed = market.parse_trade_rows([{k: str(v) for k, v in rows[0].items()}, {"period": "bad", "reporter_iso": "X"}])
        self.assertEqual(len(typed), 1)
        self.assertEqual(typed[0]["value"], 1234.5)

    def test_analysis_metrics(self):
        profile.run(self.ctx())
        p = read_json(self.paths.product_profile)
        a = {r["iso3"]: r for r in market.analyze(market.parse_trade_rows(self.rows()), p, NOW)}
        self.assertEqual(a["IDN"]["trend"], "growing")
        self.assertEqual(a["IDN"]["growth_pct_first_to_latest"], 100.0)
        self.assertEqual(a["IDN"]["india_presence"], "present")
        self.assertAlmostEqual(a["IDN"]["india_share_latest_pct"], 70.0)
        self.assertEqual(a["VNM"]["india_presence"], "unknown")  # no India export rows => unknown, not "absent"
        self.assertEqual(a["NLD"]["is_target_market"], "yes")
        self.assertEqual(sorted(r["market_rank"] for r in a.values()), [1, 2, 3])

    def test_stage_writes_outputs_and_preserves_old_data_when_api_unavailable(self):
        self.seed_market_data()
        profile.run(self.ctx())
        r = market.run(self.ctx(offline=True))
        self.assertTrue(self.paths.market_analysis.exists() and self.paths.market_shortlist.exists())
        analysis = read_csv(self.paths.market_analysis)
        self.assertEqual(analysis[0]["retrieved_at"], "2026-10-18T10:00:00+00:00")  # original date kept
        self.assertTrue(r.counts["data_available"])
        self.assertFalse(r.counts["refreshed_this_run"])

    def test_api_failure_is_recorded_and_data_not_fabricated(self):
        profile.run(self.ctx())
        r = market.run(self.ctx(offline=True))
        self.assertTrue(any("unavailable" in w for w in r.warnings))
        self.assertFalse(self.paths.market_analysis.exists())
        self.assertFalse(r.counts["data_available"])

    def test_api_exception_keeps_existing_rows(self):
        self.seed_market_data()
        profile.run(self.ctx())
        import src.pipeline.market as m
        orig_key, orig_collect = m.config.has_comtrade_key, comtrade_client.collect_year
        m.config.has_comtrade_key = lambda: True
        comtrade_client.collect_year = lambda *a, **k: (_ for _ in ()).throw(comtrade_client.ComtradeError("boom"))
        try:
            r = market.run(self.ctx(refresh_market=True))
        finally:
            m.config.has_comtrade_key, comtrade_client.collect_year = orig_key, orig_collect
        self.assertTrue(any("failed" in w for w in r.warnings))
        self.assertEqual(len(read_csv(self.paths.market_data)), len(self.rows()))


class Stage3Discovery(PipelineTestCase):
    def setUp(self):
        super().setUp()
        profile.run(self.ctx())

    def test_queries_are_product_specific_deduplicated_and_capped(self):
        p = read_json(self.paths.product_profile)
        q = discovery.build_queries(p, 100)
        texts = [x for x, _ in q]
        self.assertEqual(len(texts), len(set(t.lower() for t in texts)))
        self.assertIn("peanut importer Indonesia", texts)
        self.assertIn("raw groundnut distributor Vietnam", texts)
        self.assertIn("shelled peanut wholesaler Netherlands", texts)
        self.assertIn("groundnut food ingredient supplier Indonesia", texts)
        self.assertEqual(len(discovery.build_queries(p, 5)), 5)

    def test_search_results_become_lead_only_candidates_and_are_cached(self):
        fs = FakeSearch({"peanut importer Indonesia": [
            search_result("https://nusantara-kacang.test/", "PT Nusantara Kacang Impor - Home", "Importer of peanuts in Jakarta"),
            search_result("https://www.alibaba.com/product-detail/peanuts", "Peanuts kernels - Alibaba", "supplier"),
            search_result("https://en.wikipedia.org/wiki/Peanut", "Peanut - Wikipedia", "legume"),
            search_result("not a url", "Broken", "x")]})
        r = discovery.run(self.ctx(search=fs, max_queries=3))
        rows = read_csv(self.paths.candidates)
        by_dom = {x["source_domain"]: x for x in rows}
        self.assertIn("nusantara-kacang.test", by_dom)
        c = by_dom["nusantara-kacang.test"]
        self.assertEqual(c["evidence_level"], "lead_only_snippet")
        self.assertEqual(c["discovery_query"], "peanut importer Indonesia")
        self.assertEqual(c["country_basis"], "query_context")
        self.assertEqual(c["country_raw"], "")
        self.assertEqual(c["country_hint"], "IDN")
        self.assertTrue(c["discovered_at"] and c["page_title"] and c["snippet"] and c["initial_relevance_reason"])
        self.assertEqual(by_dom["alibaba.com"]["source_type"], "directory_listing")
        rej = read_csv(self.paths.discovery_rejects)
        self.assertEqual(len(rej), 2)  # wikipedia (non-company) + invalid URL
        # cached: a second run makes no new search calls
        n = len(fs.queries)
        discovery.run(self.ctx(search=fs, max_queries=3))
        self.assertEqual(len(fs.queries), n)

    def test_search_failure_is_partial_not_fatal(self):
        r = discovery.run(self.ctx(search=FakeSearch(fail=True), max_queries=2))
        self.assertTrue(any("Search failed" in w for w in r.warnings))
        self.assertIn(r.status, ("completed_with_warnings",))

    def test_manual_records_ingested_preserved_and_idempotent(self):
        write_raw_records(self.paths, [
            record(source_record_id="apeda-1", source_name="APEDA", source_type="buyer_lead", target_market="IDN",
                   company_name_raw="Fictional Importer One", country_raw="Indonesia",
                   product_description_raw="Groundnut kernels", buying_requirement_raw="Need 100 MT peanuts",
                   source_url="https://apeda.test/lead/1", lead_posted_date="2026-10-20", lead_status="open"),
            record(source_record_id="apeda-2", source_name="APEDA", source_type="buyer_lead",
                   company_name_raw="", country_raw="Indonesia", source_url="https://apeda.test/lead/2")])
        discovery.run(self.ctx())
        first = read_csv(self.paths.candidates)
        self.assertEqual(len(first), 1)
        self.assertEqual(first[0]["source_record_id"], "apeda-1")
        self.assertEqual(first[0]["evidence_level"], "manual_record")
        self.assertEqual(len(read_csv(self.paths.discovery_rejects)), 1)
        r = discovery.run(self.ctx())
        self.assertEqual(read_csv(self.paths.candidates), first)
        self.assertEqual(r.counts["candidates_new"], 0)

    def test_existing_candidates_are_never_removed(self):
        write_raw_records(self.paths, [record(source_record_id="r1", source_type="buyer_lead", company_name_raw="Fictional A",
                                              country_raw="Indonesia", source_url="https://a.test/x",
                                              product_description_raw="peanuts")])
        discovery.run(self.ctx())
        write_raw_records(self.paths, [])  # source emptied
        discovery.run(self.ctx())
        self.assertEqual(len(read_csv(self.paths.candidates)), 1)

    def test_url_helpers(self):
        self.assertEqual(discovery.normalize_url("example.test"), "https://example.test")
        self.assertEqual(discovery.normalize_url("ftp://x.test"), "")
        self.assertEqual(discovery.normalize_url("javascript:alert(1)"), "")
        self.assertEqual(discovery.registrable_domain("www.pt-abc.co.id"), "pt-abc.co.id")
        # free-hosting subdomains belong to different owners and must not collapse into one domain
        self.assertEqual(discovery.registrable_domain("shop-a.wixsite.com"), "shop-a.wixsite.com")
        self.assertNotEqual(discovery.registrable_domain("shop-a.wixsite.com"), discovery.registrable_domain("shop-b.wixsite.com"))


def seed_company_site_candidates(test, urls_names):
    """Create candidates for sites via a seed file."""
    rows = [{"raw_company_name": n, "source_url": u, "source_type": "company_website", "country": c,
             "source_name": "seed"} for u, n, c in urls_names]
    write_csv_rows(test.paths.discovery_seeds, ["raw_company_name", "source_url", "source_type", "country", "source_name"], rows)


class Stage4Extraction(PipelineTestCase):
    def setUp(self):
        super().setUp()
        profile.run(self.ctx())

    def run_extraction(self, fetcher, sites, offline=False):
        seed_company_site_candidates(self, sites)
        discovery.run(self.ctx())
        return extraction.run(self.ctx(fetcher=fetcher, offline=offline))

    def test_structured_extraction_and_snapshots(self):
        f = FakeFetcher()
        r = self.run_extraction(f, [("https://nusantara-kacang.test/", "Nusantara", "Indonesia")])
        p = read_json(self.paths.company_profiles)["profiles"][0]
        self.assertEqual(p["extraction_status"], "complete")
        self.assertEqual(p["company_name"], "PT Nusantara Kacang Impor")
        self.assertEqual(p["country_iso3"], "IDN")
        self.assertEqual(p["country_source"], "page_address")
        self.assertIn("info@nusantara-kacang.test", p["public_emails"])
        self.assertTrue(any("5550 1234" in t for t in p["public_phones"]))
        self.assertIn("Jl. Sudirman 1", p["public_address"])
        self.assertEqual(p["business_type"], "importer")
        self.assertEqual(p["groundnut_relevance"], "direct")
        self.assertTrue(p["contact_page"].endswith("/contact"))
        self.assertTrue(p["about_page"].endswith("/about") and p["product_page"].endswith("/products"))
        for pg in p["pages"]:
            self.assertTrue((self.paths.data / pg["snapshot_html"]).exists())
            self.assertTrue((self.paths.data / pg["snapshot_text"]).exists())
            self.assertTrue(pg["retrieved_at"])

    def test_cached_snapshots_prevent_repeat_requests(self):
        f = FakeFetcher()
        self.run_extraction(f, [("https://nusantara-kacang.test/", "N", "Indonesia")])
        n = len(f.calls)
        extraction.run(self.ctx(fetcher=f))
        self.assertEqual(len(f.calls), n)

    def test_http_errors_timeouts_and_partial_failures_do_not_stop_the_run(self):
        f = FakeFetcher(errors={"https://broken.test/": 500})
        r = self.run_extraction(f, [("https://broken.test/", "Broken Co", "Indonesia"),
                                    ("https://hanoi-nuts.test/", "Hanoi Nut", "Viet Nam")])
        profs = {p["domain"]: p for p in read_json(self.paths.company_profiles)["profiles"]}
        self.assertEqual(profs["broken.test"]["extraction_status"], "failed")
        self.assertIn("http_500", " ".join(profs["broken.test"]["errors"]))
        self.assertIn(profs["hanoi-nuts.test"]["extraction_status"], ("partial", "complete"))
        self.assertTrue(any("failed" in w for w in r.warnings))

    def test_fetcher_exception_is_contained(self):
        def boom(url):
            raise TimeoutError("timed out")
        self.run_extraction(boom, [("https://hanoi-nuts.test/", "Hanoi Nut", "Viet Nam")])
        p = read_json(self.paths.company_profiles)["profiles"][0]
        self.assertEqual(p["extraction_status"], "failed")

    def test_missing_fields_reported_not_invented(self):
        self.run_extraction(FakeFetcher(), [("https://hanoi-nuts.test/", "Hanoi Nut", "Viet Nam")])
        p = read_json(self.paths.company_profiles)["profiles"][0]
        self.assertEqual(p["public_emails"], [])
        self.assertEqual(p["public_address"], "")
        self.assertIn("public_contact", p["missing_fields"])
        self.assertIn("contact_page", p["missing_fields"])

    def test_third_party_and_invalid_pages_are_not_treated_as_company_sites(self):
        write_csv_rows(self.paths.discovery_seeds, ["raw_company_name", "source_url", "source_type", "source_name"],
                       [{"raw_company_name": "Some Dir Listing", "source_url": "https://www.alibaba.com/x",
                         "source_type": "directory_listing", "source_name": "seed"}])
        discovery.run(self.ctx())
        f = FakeFetcher()
        extraction.run(self.ctx(fetcher=f))
        p = read_json(self.paths.company_profiles)["profiles"][0]
        self.assertTrue(p["extraction_status"].startswith("skipped"))
        self.assertEqual(f.calls, [])

    def test_offline_without_cache_reports_not_fetched(self):
        self.run_extraction(None, [("https://hanoi-nuts.test/", "Hanoi", "Viet Nam")], offline=True)
        p = read_json(self.paths.company_profiles)["profiles"][0]
        self.assertEqual(p["extraction_status"], "not_fetched_offline")

    def test_parse_page_edge_cases(self):
        self.assertEqual(extraction.parse_page("", "https://x.test/")["text"], "")
        d = extraction.parse_page("<html><body><p>Mail: a@b.co, logo@2x.png</p><script>var x='z@z.com'</script></body></html>", "https://x.test/")
        self.assertEqual(d["emails"], ["a@b.co"])
        d = extraction.parse_page("<p>Order no 20261103 12345678</p>", "https://x.test/")
        self.assertEqual(d["phones"], [])  # unlabelled digits are not phone numbers


class Stage5Resolution(PipelineTestCase):
    def cand(self, cid, name, url="", country_raw="", website="", stype="buyer_lead", basis="source_record", **kw):
        from src.pipeline.schemas import CANDIDATE_FIELDS
        c = {f: "" for f in CANDIDATE_FIELDS}
        c.update(candidate_id=cid, raw_company_name=name, source_url=url or f"https://src.test/{cid}",
                 source_domain=discovery.domain_of(url or "https://src.test/x"), source_type=stype,
                 country_raw=country_raw, website_raw=website, country_basis=basis, evidence_level="manual_record")
        c.update(kw)
        return c

    def test_name_normalization_preserves_legal_suffix(self):
        self.assertEqual(resolution.normalize_name("PT Abc Nusantara"), ("pt abc nusantara", "pt", "abc nusantara"))
        self.assertEqual(resolution.normalize_name("Abc Nusantara, B.V."), ("abc nusantara bv", "bv", "abc nusantara"))
        self.assertEqual(resolution.normalize_name("Công ty TNHH Hạt Việt")[1], "cong ty tnhh")
        self.assertEqual(resolution.normalize_domain("https://www.Abc.co.id/path"), "abc.co.id")
        self.assertEqual(resolution.normalize_country("The Netherlands"), "NLD")
        self.assertEqual(resolution.normalize_country("Narnia"), "")

    def test_exact_domain_duplicates_merge_and_ids_are_stable(self):
        a = self.cand("c1", "Fictional Foods Ltd", country_raw="Indonesia", website="https://www.fictional.test")
        b = self.cand("c2", "FICTIONAL FOODS LIMITED", country_raw="Indonesia", website="http://fictional.test/about")
        companies, sugg, rmap = resolution.resolve([a, b], [], [], "t")
        self.assertEqual(len(companies), 1)
        self.assertEqual(companies[0]["dedup_status"], "unique")
        self.assertEqual({r["company_id"] for r in rmap}, {companies[0]["company_id"]})
        c2, _, _ = resolution.resolve([b, a], [], [], "t")
        self.assertEqual(c2[0]["company_id"], companies[0]["company_id"])  # order independent
        self.assertIn("|", companies[0]["aliases"] + "|")

    def test_similar_names_are_not_merged(self):
        a = self.cand("c1", "Sinar Kacang Indonesia", country_raw="Indonesia", website="https://sinar1.test")
        b = self.cand("c2", "Sinar Kacang Indonesia PT", country_raw="Indonesia", website="https://sinar2.test")
        c = self.cand("c3", "Sinar Kacang Indonesa", country_raw="Indonesia", website="https://sinar3.test")
        companies, sugg, _ = resolution.resolve([a, b, c], [], [], "t")
        self.assertEqual(len(companies), 3)
        self.assertTrue(sugg)
        self.assertTrue(all(s["status"] == "suggested_not_merged" for s in sugg))
        self.assertTrue(all(c["dedup_status"] == "possible_duplicate" for c in companies))

    def test_name_only_requires_manual_review(self):
        companies, _, _ = resolution.resolve([self.cand("c1", "Mystery Traders")], [], [], "t")
        self.assertEqual(companies[0]["dedup_status"], "manual_review_required")

    def test_different_countries_are_not_suggested(self):
        a = self.cand("c1", "Global Nuts", country_raw="Indonesia", website="https://g1.test")
        b = self.cand("c2", "Global Nuts", country_raw="Netherlands", website="https://g2.test")
        _, sugg, _ = resolution.resolve([a, b], [], [], "t")
        self.assertEqual(sugg, [])

    def test_manual_merge_and_keep_separate_decisions_are_honoured(self):
        a = self.cand("c1", "Sinar Kacang", country_raw="Indonesia", website="https://s1.test")
        b = self.cand("c2", "Sinar Kacang PT", country_raw="Indonesia", website="https://s2.test")
        base, _, _ = resolution.resolve([a, b], [], [], "t")
        ia, ib = sorted(c["company_id"] for c in base)
        merged, _, rmap = resolution.resolve([a, b], [], [{"company_id_a": ia, "company_id_b": ib, "decision": "merge"}], "t")
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["manual_merge_applied"], "yes")
        sep, sugg, _ = resolution.resolve([a, b], [], [{"company_id_a": ia, "company_id_b": ib, "decision": "keep_separate"}], "t")
        self.assertEqual((len(sep), sugg), (2, []))


class ClaimsBase(PipelineTestCase):
    """Build the pipeline up to a chosen stage using the synthetic fixtures."""

    def build(self, fetcher=None, records=None, seeds=None, through="claims", search=None):
        profile.run(self.ctx())
        if records:
            write_raw_records(self.paths, records)
        if seeds:
            seed_company_site_candidates(self, seeds)
        ctx = self.ctx(fetcher=fetcher or FakeFetcher(), search=search)
        for mod in (discovery, extraction, resolution, claims, qualification, scoring):
            mod.run(ctx)
            if mod.__name__.endswith(through):
                break
        return ctx

    def claims_for(self, name):
        comp = next(c for c in read_csv(self.paths.companies) if name.lower() in c["company_name"].lower())
        return comp, {c["claim_type"]: c for c in read_csv(self.paths.company_claims) if c["company_id"] == comp["company_id"]}


class Stage6Claims(ClaimsBase):
    def test_importer_site_claims_with_provenance(self):
        self.build(seeds=[("https://nusantara-kacang.test/", "Nusantara", "Indonesia")])
        comp, c = self.claims_for("Nusantara")
        self.assertEqual(c["imports_raw_groundnuts"]["claim_status"], "supported")
        self.assertEqual(c["imports_from_india"]["claim_status"], "supported")
        self.assertIn("India", c["imports_from_india"]["exact_text"])
        self.assertEqual(c["historical_import_activity"]["claim_status"], "supported")
        self.assertEqual(c["importer_or_distributor_role"]["claim_status"], "supported")
        self.assertEqual(c["public_contact_information"]["claim_status"], "supported")
        self.assertEqual(c["company_identity"]["claim_status"], "supported")
        self.assertEqual(c["current_buying_requirement"]["claim_status"], "unknown")  # website cannot show current intent
        self.assertEqual(set(c), set(claims.CLAIM_TYPES))
        for cl in c.values():
            self.assertTrue(cl["claim_id"] and cl["company_id"] and cl["review_status"] and cl["limitations"])

    def test_every_supported_claim_has_evidence_and_verbatim_provenance(self):
        self.build(seeds=[("https://nusantara-kacang.test/", "Nusantara", "Indonesia")])
        ev = {e["evidence_id"]: e for e in read_csv(self.paths.evidence_log)}
        self.assertTrue(ev)
        for cl in read_csv(self.paths.company_claims):
            if cl["claim_status"] in ("supported", "contradicted"):
                ids = [i for i in cl["evidence_ids"].split(";") if i]
                self.assertTrue(ids, cl["claim_type"])
                for i in ids:
                    e = ev[i]
                    self.assertTrue(e["retrieved_at"] and e["source_url"] and e["exact_text"])
                    self.assertTrue(e["snapshot_path"], "page evidence must point at a snapshot")
                    snap = self.paths.data / e["snapshot_path"]
                    haystack = snap.read_text(encoding="utf-8")
                    self.assertIn(e["exact_text"], haystack, f"{cl['claim_type']} text not found verbatim in {snap.name}")

    def test_single_source_is_never_called_cross_verified(self):
        self.build(seeds=[("https://nusantara-kacang.test/", "Nusantara", "Indonesia")])
        for cl in read_csv(self.paths.company_claims):
            self.assertNotIn("verified", cl["corroboration"])
            if cl["claim_status"] == "supported":
                self.assertEqual(cl["corroboration"], "single_source")
                self.assertIn("not independently cross-verified", cl["limitations"])

    def test_seller_processor_is_not_an_importer(self):
        self.build(seeds=[("https://hanoi-nuts.test/", "Hanoi Nut", "Viet Nam")])
        _, c = self.claims_for("Hanoi")
        self.assertEqual(c["processes_groundnuts"]["claim_status"], "supported")
        self.assertEqual(c["sells_groundnuts"]["claim_status"], "supported")
        self.assertNotEqual(c["imports_raw_groundnuts"]["claim_status"], "supported")
        self.assertEqual(c["imports_raw_groundnuts"]["claim_status"], "not_supported")
        self.assertIn("NOT evidence of absence", c["imports_raw_groundnuts"]["limitations"])

    def test_explicit_negation_is_contradicted_not_supported(self):
        self.build(seeds=[("https://plain-foods.test/", "Plain Foods", "Indonesia")])
        _, c = self.claims_for("Plain Foods")
        self.assertEqual(c["imports_raw_groundnuts"]["claim_status"], "contradicted")

    def test_prepared_only_peanuts_do_not_support_raw_relevance(self):
        self.build(seeds=[("https://dutch-delights.test/", "Dutch Delights", "Netherlands")])
        _, c = self.claims_for("Dutch")
        self.assertEqual(c["product_relevance"]["claim_status"], "not_supported")
        self.assertIn("prepared", c["product_relevance"]["limitations"])

    def test_unfetched_company_claims_are_unknown_not_negative(self):
        self.build(fetcher=FakeFetcher(errors={"https://hanoi-nuts.test/": 500}),
                   seeds=[("https://hanoi-nuts.test/", "Hanoi Nut", "Viet Nam")])
        _, c = self.claims_for("Hanoi")
        for t in ("product_relevance", "imports_raw_groundnuts", "public_contact_information"):
            self.assertEqual(c[t]["claim_status"], "unknown", t)

    def test_search_snippet_is_low_confidence_lead_only(self):
        fs = FakeSearch({"peanut importer Indonesia": [search_result(
            "https://snippet-only.test/", "PT Snippet Only - Home", "We are an importer of peanuts and groundnuts in Jakarta, importing from India")]})
        self.build(fetcher=FakeFetcher(), search=fs, through="claims")
        _, c = self.claims_for("Snippet")
        # the page itself was fetched (404 in fixtures) so only the snippet exists
        self.assertEqual(c["product_relevance"]["confidence_level"], "low")
        self.assertEqual(c["product_relevance"]["source_authority_tier"], "6")
        self.assertIn("discovery lead", c["product_relevance"]["limitations"])
        self.assertNotEqual(c["imports_raw_groundnuts"]["claim_status"], "supported")
        self.assertNotEqual(c["imports_from_india"]["claim_status"], "supported")

    def test_shipment_record_is_historical_not_current(self):
        rec = record(source_record_id="ship-1", source_name="TradeDB", source_type="shipment_record",
                     company_name_raw="Fictional Importer Two", country_raw="Indonesia",
                     product_description_raw="Groundnut kernels", hs_code="120242", shipment_date="2024-05-10",
                     origin_country="India", quantity_raw="25 MT", source_url="https://tradedb.test/s/1")
        self.build(records=[rec])
        _, c = self.claims_for("Fictional Importer Two")
        self.assertEqual(c["historical_import_activity"]["claim_status"], "supported")
        self.assertEqual(c["imports_from_india"]["claim_status"], "supported")
        self.assertEqual(c["current_buying_requirement"]["claim_status"], "unknown")
        self.assertEqual(c["historical_import_activity"]["source_authority_tier"], "5")

    def test_buyer_lead_current_only_when_dated_and_open(self):
        base = dict(source_name="APEDA", source_type="buyer_lead", country_raw="Indonesia",
                    product_description_raw="Groundnut kernels", buying_requirement_raw="Need 50 MT",
                    source_url="https://apeda.test/l")
        recs = [record(source_record_id="l1", company_name_raw="Fresh Lead Co", lead_posted_date="2026-10-20", lead_status="open", **base),
                record(source_record_id="l2", company_name_raw="Old Lead Co", lead_posted_date="2025-01-10", lead_status="open", **base),
                record(source_record_id="l3", company_name_raw="Undated Lead Co", **base),
                record(source_record_id="l4", company_name_raw="Closed Lead Co", lead_posted_date="2026-10-25", lead_status="Closed", **base)]
        self.build(records=recs)
        status = {n: self.claims_for(n)[1]["current_buying_requirement"]["claim_status"]
                  for n in ("Fresh Lead", "Old Lead", "Undated Lead", "Closed Lead")}
        self.assertEqual(status, {"Fresh Lead": "supported", "Old Lead": "unknown", "Undated Lead": "unknown", "Closed Lead": "unknown"})


class Stage78QualificationScoring(ClaimsBase):
    def full(self):
        recs = [
            record(source_record_id="ship-1", source_name="TradeDB", source_type="shipment_record",
                   company_name_raw="Fictional Importer Two", country_raw="Indonesia", product_description_raw="Groundnut kernels",
                   hs_code="120242", shipment_date="2024-05-10", origin_country="India", source_url="https://tradedb.test/s/1"),
            record(source_record_id="in-1", source_name="Directory", source_type="directory_listing",
                   company_name_raw="Indian Exporter Pvt Ltd", country_raw="India", product_description_raw="Groundnut exporter",
                   source_url="https://dir.test/in1")]
        seeds = [("https://nusantara-kacang.test/", "Nusantara", "Indonesia"),
                 ("https://hanoi-nuts.test/", "Hanoi Nut", "Viet Nam"),
                 ("https://dutch-delights.test/", "Dutch Delights", "Netherlands"),
                 ("https://plain-foods.test/", "Plain Foods", "Indonesia"),
                 ("https://broken.test/", "Broken Co", "Indonesia")]
        return self.build(records=recs, seeds=seeds, through="scoring")

    def q(self):
        return {r["company_name"]: r for r in read_csv(self.paths.buyer_qualification)}

    def test_classifications_and_reasons(self):
        self.full()
        q = self.q()
        self.assertEqual(q["PT Nusantara Kacang Impor"]["final_classification"], "potential_buyer")
        self.assertEqual(q["Hanoi Nut Processing JSC"]["final_classification"], "relevant_business")
        self.assertEqual(q["Dutch Delights BV"]["final_classification"], "weak_lead")
        self.assertEqual(q["Fictional Importer Two"]["final_classification"], "potential_buyer")
        self.assertEqual(q["Indian Exporter Pvt Ltd"]["final_classification"], "excluded")
        broken = next(v for k, v in q.items() if "Broken" in k)
        self.assertEqual(broken["final_classification"], "undetermined")
        for row in q.values():
            self.assertTrue(row["classification_reason"] and row["classification_rule"])
            self.assertEqual(row["qualification_status"], "unreviewed")  # never auto-verified
        self.assertNotIn("verified_buyer", {r["final_classification"] for r in q.values()})

    def test_buyer_classification_always_links_evidence(self):
        self.full()
        ev = {e["evidence_id"] for e in read_csv(self.paths.evidence_log)}
        for row in self.q().values():
            if row["final_classification"] in ("potential_buyer", "relevant_business"):
                ids = [i for i in row["supporting_evidence_ids"].split(";") if i]
                self.assertTrue(ids and set(ids) <= ev)

    def test_dimensions_are_kept_separate(self):
        self.full()
        q = self.q()
        hist = q["Fictional Importer Two"]
        self.assertEqual(hist["historical_import_activity"], "supported")
        self.assertEqual(hist["current_buying_intent"], "unknown")  # historical import != current intent
        self.assertIn("no evidence of current buying intent", hist["unresolved_issues"])
        n = q["PT Nusantara Kacang Impor"]
        self.assertEqual(n["india_origin_evidence"], "explicit")
        self.assertEqual(n["legal_identity_status"], "registry_not_verified")
        self.assertIn("email", n["contact_availability"])
        self.assertEqual(q["Hanoi Nut Processing JSC"]["buyer_role_evidence"], "seller_or_processor_only")

    def test_contradicted_import_blocks_potential_buyer(self):
        self.full()
        self.assertNotEqual(self.q().get("Plain Foods Ltd", {"final_classification": ""})["final_classification"], "potential_buyer")

    def test_scores_components_sum_and_bands(self):
        self.full()
        rows = {r["company_name"]: r for r in read_csv(self.paths.ranked_buyers)}
        mx = dict(zip(("product_relevance", "buyer_role", "evidence_quality", "market_fit", "india_origin", "contactability"),
                      (25, 25, 20, 10, 10, 10)))
        for r in rows.values():
            parts = sum(float(r[f"score_{k}"]) for k in mx)
            self.assertAlmostEqual(parts, float(r["total_score"]), places=1)
            for k, m in mx.items():
                self.assertLessEqual(float(r[f"score_{k}"]), m)
                self.assertTrue(r[f"why_{k}"])
        n = rows["PT Nusantara Kacang Impor"]
        self.assertEqual(scoring.band(float(n["total_score"])), n["priority_band"])
        self.assertGreater(float(n["total_score"]), float(rows["Hanoi Nut Processing JSC"]["total_score"]))
        self.assertGreater(float(rows["Hanoi Nut Processing JSC"]["total_score"]), float(rows["Dutch Delights BV"]["total_score"]))
        self.assertEqual(float(n["score_india_origin"]), 10.0)
        self.assertEqual(float(n["score_contactability"]), 10.0)
        self.assertEqual(float(n["score_market_fit"]), 10.0)
        self.assertEqual(rows["Indian Exporter Pvt Ltd"]["priority_band"], "Excluded (not ranked)")
        self.assertEqual(rows["Indian Exporter Pvt Ltd"]["rank"], "")

    def test_historical_import_does_not_get_full_role_points_or_intent_bonus(self):
        self.full()
        rows = {r["company_name"]: r for r in read_csv(self.paths.ranked_buyers)}
        h = rows["Fictional Importer Two"]
        self.assertLessEqual(float(h["score_buyer_role"]), 15.0)
        self.assertIn("no current-buying-intent evidence", h["why_buyer_role"])
        self.assertEqual(float(h["score_contactability"]), 0.0)  # records carry no public contact channel

    def test_unknown_gets_no_points(self):
        self.full()
        rows = {r["company_name"]: r for r in read_csv(self.paths.ranked_buyers)}
        b = next(v for k, v in rows.items() if "Broken" in k)
        self.assertEqual(float(b["total_score"]), 0.0 + float(b["score_market_fit"]))  # only the declared country counts
        for k in ("product_relevance", "buyer_role", "evidence_quality", "india_origin", "contactability"):
            self.assertEqual(float(b[f"score_{k}"]), 0.0)

    def test_snippet_only_cannot_earn_strong_evidence_points(self):
        fs = FakeSearch({"peanut importer Indonesia": [search_result(
            "https://snippet-only.test/", "PT Snippet Only - Home",
            "Importer of peanuts in Jakarta importing from India")]})
        self.build(fetcher=FakeFetcher(), search=fs, through="scoring")
        r = next(r for r in read_csv(self.paths.ranked_buyers) if "Snippet" in r["company_name"])
        self.assertLessEqual(float(r["score_product_relevance"]), 8.0)
        self.assertLessEqual(float(r["score_buyer_role"]), 6.0)
        self.assertLessEqual(float(r["score_evidence_quality"]), 5.0)
        self.assertEqual(float(r["score_india_origin"]), 0.0)
        self.assertEqual(float(r["score_market_fit"]), 0.0)  # country only from the query
        self.assertEqual(r["final_classification"], "weak_lead")

    def test_unsupported_claim_without_evidence_earns_zero(self):
        claims_row = {t: {"claim_type": t, "claim_status": "supported", "evidence_ids": "", "confidence_level": "high",
                          "source_authority_tier": "1", "source_type": "x", "exact_text": "India", "publication_date": ""}
                      for t in ("product_relevance", "imports_raw_groundnuts", "imports_from_india")}
        comp = {"country_iso3": "IDN", "country_source": "page_address"}
        c, _, total = scoring.score_company(claims_row, comp, None, {"IDN": ""}, NOW)
        self.assertEqual(total, 10.0)  # market fit only
        self.assertEqual((c["product_relevance"], c["buyer_role"], c["india_origin"]), (0.0, 0.0, 0.0))

    def test_india_points_require_india_in_evidence_text(self):
        cl = {"imports_from_india": {"claim_type": "imports_from_india", "claim_status": "supported", "evidence_ids": "e1",
                                     "confidence_level": "high", "source_authority_tier": "1", "source_type": "official_company_website",
                                     "exact_text": "We import peanuts from Brazil.", "publication_date": ""}}
        c, _, _ = scoring.score_company(cl, {"country_iso3": "", "country_source": ""}, None, {}, NOW)
        self.assertEqual(c["india_origin"], 0.0)

    def test_manual_override_wins_and_is_recorded_beside_automatic_result(self):
        self.full()
        n = next(r for r in read_csv(self.paths.buyer_qualification) if "Nusantara" in r["company_name"])
        write_csv_rows(self.paths.manual_qualification, ["company_id", "classification", "decided_by", "decided_at", "note"],
                       [{"company_id": n["company_id"], "classification": "relevant_business", "decided_by": "Shubhi",
                         "decided_at": "2026-11-03", "note": "reviewed call"}])
        qualification.run(self.ctx())
        n2 = next(r for r in read_csv(self.paths.buyer_qualification) if "Nusantara" in r["company_name"])
        self.assertEqual((n2["final_classification"], n2["auto_classification"], n2["override_applied"]),
                         ("relevant_business", "potential_buyer", "yes"))
        self.assertIn("reviewed call", n2["override_note"])
        self.assertTrue(read_csv(self.paths.classification_changes))  # the change was logged, not silent

    def test_classification_changes_are_logged_not_silent(self):
        self.full()
        before = {r["company_id"]: r["final_classification"] for r in read_csv(self.paths.buyer_qualification)}
        # evidence disappears: the site now fails; cache is stale/offline so re-extraction fails
        import shutil
        shutil.rmtree(self.paths.enrichment)
        ctx = self.ctx(fetcher=FakeFetcher(sites={}))
        for mod in (extraction, resolution, claims, qualification):
            mod.run(ctx)
        after = {r["company_id"]: r["final_classification"] for r in read_csv(self.paths.buyer_qualification)}
        changed = {k for k in before if before[k] != after.get(k)}
        logged = {r["company_id"] for r in read_csv(self.paths.classification_changes)}
        self.assertTrue(changed)
        self.assertEqual(changed, logged)


class EndToEnd(ClaimsBase):
    def run_all(self, **kw):
        ctx = self.ctx(**kw)
        results = runner.run_stages(ctx)
        return ctx, results, runner.build_summary(ctx, results, NOW)

    def test_full_pipeline_runs_without_manual_editing_and_exports(self):
        write_raw_records(self.paths, [record(source_record_id="ship-1", source_name="TradeDB", source_type="shipment_record",
                                              company_name_raw="Fictional Importer Two", country_raw="Indonesia",
                                              product_description_raw="Groundnut kernels", hs_code="120242",
                                              shipment_date="2024-05-10", origin_country="India",
                                              source_url="https://tradedb.test/s/1")])
        seed_company_site_candidates(self, [("https://nusantara-kacang.test/", "Nusantara", "Indonesia"),
                                            ("https://broken.test/", "Broken Co", "Indonesia")])
        ctx, results, summary = self.run_all(fetcher=FakeFetcher(errors={"https://broken.test/": 500}))
        self.assertEqual(summary["stages_failed"], [])
        self.assertEqual([r.stage for r in results], runner.STAGE_NAMES)
        self.assertEqual(summary["candidate_count"], 3)
        self.assertEqual(summary["qualified_company_count"], 2)
        for name in ("ranked_buyers.csv", "company_profiles.csv", "evidence_log.csv", "dashboard.html", "dashboard_data.json"):
            self.assertTrue((self.paths.exports / name).exists(), name)
        # exports equal processed outputs
        self.assertEqual(read_csv(self.paths.exports / "ranked_buyers.csv"), read_csv(self.paths.ranked_buyers))
        profiles = read_csv(self.paths.exports / "company_profiles.csv")
        self.assertEqual(len(profiles), 3)
        self.assertEqual({p["extraction_status"] for p in profiles}, {"complete", "failed", "skipped_third_party_no_company_site"})

    def test_a_crashing_stage_is_recorded_and_the_rest_continue(self):
        ctx = self.ctx(fetcher=FakeFetcher())
        orig = discovery.run
        discovery.run = lambda c: (_ for _ in ()).throw(RuntimeError("discovery exploded"))
        try:
            import src.pipeline.runner as rn
            rn.STAGES = [(n, discovery if n == "discovery" else m) for n, m in rn.STAGES]
            results = rn.run_stages(ctx)
        finally:
            discovery.run = orig
        by = {r.stage: r for r in results}
        self.assertEqual(by["discovery"].status, "failed")
        self.assertIn("discovery exploded", by["discovery"].errors[0])
        self.assertEqual(by["export"].status in ("completed", "completed_with_warnings"), True)
        self.assertEqual(len(results), 9)

    def test_profile_failure_skips_everything_else(self):
        ctx = self.ctx(profile_input={"product_name": "X", "origin": "India", "target_countries": ["Atlantis"]})
        results = runner.run_stages(ctx)
        self.assertEqual(results[0].status, "failed")
        self.assertTrue(all(r.status == "skipped" for r in results[1:]))

    def test_run_summary_fields(self):
        _, _, s = self.run_all(fetcher=FakeFetcher(), offline=True)
        for k in ("run_id", "started_at", "completed_at", "product_profile_id", "stages_completed", "stages_failed",
                  "candidate_count", "qualified_company_count", "output_locations", "warnings", "source_coverage"):
            self.assertIn(k, s)
        self.assertTrue(s["product_profile_id"].startswith("prof-"))
        self.assertEqual(s["hs_verification"], "unverified")

    def test_pipeline_is_reproducible(self):
        seed_company_site_candidates(self, [("https://nusantara-kacang.test/", "Nusantara", "Indonesia")])
        self.run_all(fetcher=FakeFetcher())
        first = (self.paths.ranked_buyers.read_text(), self.paths.company_claims.read_text(), self.paths.evidence_log.read_text())
        f = FakeFetcher()
        self.run_all(fetcher=f)
        self.assertEqual(f.calls, [])  # cached snapshots reused
        self.assertEqual(first, (self.paths.ranked_buyers.read_text(), self.paths.company_claims.read_text(), self.paths.evidence_log.read_text()))

    def test_audit_passes_on_clean_run_and_catches_violations(self):
        from src.pipeline import audit
        seed_company_site_candidates(self, [("https://nusantara-kacang.test/", "Nusantara", "Indonesia")])
        ctx, _, _ = self.run_all(fetcher=FakeFetcher())
        self.assertEqual([m for lvl, m in audit.audit(ctx) if lvl == "ERROR"], [])
        rows = read_csv(self.paths.company_claims)
        rows[0]["claim_status"], rows[0]["evidence_ids"] = "supported", ""
        from src.pipeline.schemas import CLAIM_FIELDS
        write_csv_rows(self.paths.company_claims, CLAIM_FIELDS, rows)
        self.assertTrue([m for lvl, m in audit.audit(ctx) if lvl == "ERROR"])

    def test_dashboard_embeds_data_safely(self):
        rec = record(source_record_id="x1", source_name="Dir", source_type="directory_listing",
                     company_name_raw="</script><img src=x onerror=alert(1)> Peanut Co", country_raw="Indonesia",
                     product_description_raw="Groundnut kernels </script>", source_url="https://dir.test/x1")
        write_raw_records(self.paths, [rec])
        ctx, _, _ = self.run_all(fetcher=FakeFetcher())
        html = (self.paths.exports / "dashboard.html").read_text(encoding="utf-8")
        self.assertEqual(html.count("</script>"), 2)  # data block + app script only
        payload = json.loads(html.split('id="data">')[1].split("</script>")[0].replace("<\\/", "</"))
        self.assertEqual(payload["counts"]["discovered"], 1)
        for key in ("product_profile", "markets", "counts", "companies", "global_issues"):
            self.assertIn(key, payload)
        c = payload["companies"][0]
        for key in ("ranking", "qualification", "profile", "claims", "evidence"):
            self.assertIn(key, c)

    def test_dashboard_reports_unavailable_trade_data(self):
        ctx, _, _ = self.run_all(fetcher=FakeFetcher(), offline=True)
        payload = json.loads((self.paths.exports / "dashboard_data.json").read_text())
        self.assertFalse(payload["market_available"])
        self.assertTrue(any("unavailable" in i for i in payload["global_issues"]))


class HttpFetcherLocal(PipelineTestCase):
    """The real network layer against a local server: robots.txt, blocks, captcha, errors."""

    def setUp(self):
        super().setUp()
        import threading
        from http.server import BaseHTTPRequestHandler, HTTPServer
        hits = self.hits = []

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a): pass
            def do_GET(self):
                hits.append(self.path)
                routes = {
                    "/robots.txt": (200, "text/plain", "User-agent: *\nDisallow: /private\n"),
                    "/ok": (200, "text/html", "<html><title>Hi</title><body>peanuts importer</body></html>"),
                    "/private": (200, "text/html", "<html>secret</html>"),
                    "/forbidden": (403, "text/html", "no"), "/missing": (404, "text/html", "no"),
                    "/captcha": (200, "text/html", "<html><body>Please complete the CAPTCHA to continue</body></html>"),
                    "/pdf": (200, "application/pdf", "%PDF"),
                }
                code, ctype, body = routes.get(self.path, (404, "text/html", ""))
                self.send_response(code); self.send_header("Content-Type", ctype); self.end_headers()
                self.wfile.write(body.encode())
        self.srv = HTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.addCleanup(self.srv.server_close)
        self.addCleanup(self.srv.shutdown)
        self.base = f"http://127.0.0.1:{self.srv.server_port}"
        self.f = extraction.HttpFetcher(min_interval=0, timeout=5)

    def test_ok_page(self):
        r = self.f(self.base + "/ok")
        self.assertEqual((r.status_code, r.error), (200, ""))
        self.assertIn("peanuts", r.body)

    def test_robots_disallow_is_respected_and_page_never_requested(self):
        r = self.f(self.base + "/private")
        self.assertTrue(r.blocked)
        self.assertNotIn("/private", self.hits)

    def test_403_and_captcha_are_blocked_not_bypassed(self):
        for path, why in (("/forbidden", "access_restricted"), ("/captcha", "captcha")):
            r = self.f(self.base + path)
            self.assertTrue(r.blocked, path)
            self.assertIn(why, r.error)
            self.assertEqual(r.body, "")
        self.assertEqual(self.hits.count("/forbidden"), 1)  # no retries

    def test_404_non_html_and_connection_errors(self):
        self.assertEqual(self.f(self.base + "/missing").error, "http_404")
        self.assertTrue(self.f(self.base + "/pdf").error.startswith("not_html"))
        dead = extraction.HttpFetcher(min_interval=0, timeout=1)("http://127.0.0.1:1/x")
        self.assertTrue(dead.error)

    def test_end_to_end_extraction_through_real_fetcher_and_snapshot_cache(self):
        profile.run(self.ctx())
        write_csv_rows(self.paths.discovery_seeds, ["raw_company_name", "source_url", "source_type", "country", "source_name"],
                       [{"raw_company_name": "Local Co", "source_url": self.base + "/ok", "source_type": "company_website",
                         "country": "Indonesia", "source_name": "seed"}])
        discovery.run(self.ctx())
        extraction.run(self.ctx(fetcher=self.f))
        n = len(self.hits)
        p = read_json(self.paths.company_profiles)["profiles"][0]
        self.assertIn(p["extraction_status"], ("partial", "complete"))
        extraction.run(self.ctx(fetcher=self.f))
        self.assertEqual(len(self.hits), n)  # served from snapshot cache


class Regression(ClaimsBase):
    def test_existing_research_and_manual_files_are_never_overwritten(self):
        write_raw_records(self.paths, [record(source_record_id="keep-1", source_type="buyer_lead", company_name_raw="Fictional Keep Co",
                                              country_raw="Indonesia", source_url="https://k.test/1", product_description_raw="peanuts")])
        write_csv_rows(self.paths.manual_merges, ["company_id_a", "company_id_b", "decision", "decided_by", "decided_at", "note"], [])
        write_csv_rows(self.paths.manual_qualification, ["company_id", "classification", "decided_by", "decided_at", "note"], [])
        write_csv_rows(self.paths.market_data, comtrade_client.CSV_FIELDS, [])
        snapshots = {p: p.read_text() for p in (self.paths.raw_importer_records, self.paths.manual_merges,
                                                self.paths.manual_qualification, self.paths.market_data,
                                                self.data / "product_profile.json")}
        runner.run_stages(self.ctx(fetcher=FakeFetcher()))
        for p, text in snapshots.items():
            self.assertEqual(p.read_text(), text, f"{p.name} was modified")

    def test_rewritten_outputs_are_archived_not_lost(self):
        seed_company_site_candidates(self, [("https://nusantara-kacang.test/", "Nusantara", "Indonesia")])
        runner.run_stages(self.ctx(fetcher=FakeFetcher()))
        old = self.paths.ranked_buyers.read_text(encoding="utf-8-sig")
        from src.pipeline.common import write_csv
        write_csv(self.paths.ranked_buyers, [], ["rank"], self.paths, NOW)
        archived = list(self.paths.archive.glob("ranked_buyers.*.csv"))
        self.assertEqual(len(archived), 1)
        self.assertEqual(archived[0].read_text(encoding="utf-8-sig"), old)

    def test_pilot_company_ids_and_classifications_stable_across_reruns(self):
        seed_company_site_candidates(self, [("https://nusantara-kacang.test/", "Nusantara", "Indonesia"),
                                            ("https://hanoi-nuts.test/", "Hanoi", "Viet Nam")])
        runner.run_stages(self.ctx(fetcher=FakeFetcher()))
        a = {r["company_id"]: r["final_classification"] for r in read_csv(self.paths.buyer_qualification)}
        runner.run_stages(self.ctx(fetcher=FakeFetcher()))
        b = {r["company_id"]: r["final_classification"] for r in read_csv(self.paths.buyer_qualification)}
        self.assertEqual(a, b)
        self.assertEqual(read_csv(self.paths.classification_changes), [])

    def test_no_automatic_merge_from_similarity_end_to_end(self):
        recs = [record(source_record_id=f"r{i}", source_type="buyer_lead", company_name_raw=n, country_raw="Indonesia",
                       source_url=f"https://src.test/{i}", website_raw=w, product_description_raw="peanuts")
                for i, (n, w) in enumerate([("Sinar Kacang Indonesia", "https://s1.test"),
                                            ("Sinar Kacang Indonesia PT", "https://s2.test")])]
        self.build(records=recs, fetcher=FakeFetcher(), through="resolution")
        self.assertEqual(len(read_csv(self.paths.companies)), 2)
        self.assertTrue(read_csv(self.paths.duplicate_suggestions))

    def test_no_unsupported_claim_is_promoted(self):
        self.build(seeds=[("https://hanoi-nuts.test/", "Hanoi", "Viet Nam"),
                          ("https://dutch-delights.test/", "Dutch", "Netherlands"),
                          ("https://plain-foods.test/", "Plain", "Indonesia"),
                          ("https://broken.test/", "Broken", "Indonesia")], through="scoring")
        for c in read_csv(self.paths.company_claims):
            if c["claim_status"] in ("unknown", "not_supported"):
                self.assertEqual((c["confidence_level"], c["evidence_ids"]) if c["claim_status"] == "unknown" else c["confidence_level"],
                                 ("", "") if c["claim_status"] == "unknown" else "", c["claim_type"])
            if c["claim_status"] == "supported":
                self.assertTrue(c["confidence_level"] and c["evidence_ids"])
        statuses = {q["company_name"]: q["final_classification"] for q in read_csv(self.paths.buyer_qualification)}
        self.assertNotIn("potential_buyer", statuses.values())  # none of these has buyer evidence
        evidence_ids = {e["evidence_id"] for e in read_csv(self.paths.evidence_log)}
        claims_by_id = {c["claim_id"]: c for c in read_csv(self.paths.company_claims)}
        for q in read_csv(self.paths.buyer_qualification):
            for cid in filter(None, q["supporting_claim_ids"].split(";")):
                self.assertIn(claims_by_id[cid]["claim_status"], ("supported", "contradicted"))
            self.assertTrue(set(filter(None, q["supporting_evidence_ids"].split(";"))) <= evidence_ids)

    def test_no_historical_import_treated_as_current_intent_anywhere(self):
        rec = record(source_record_id="s", source_name="T", source_type="shipment_record", company_name_raw="Fictional Hist Co",
                     country_raw="Indonesia", product_description_raw="Peanuts", hs_code="120242", shipment_date="2026-10-30",
                     origin_country="India", source_url="https://t.test/s")
        self.build(records=[rec], through="scoring")
        c = {x["claim_type"]: x for x in read_csv(self.paths.company_claims)}
        self.assertEqual(c["current_buying_requirement"]["claim_status"], "unknown")
        q = read_csv(self.paths.buyer_qualification)[0]
        self.assertEqual(q["current_buying_intent"], "unknown")
        r = read_csv(self.paths.ranked_buyers)[0]
        self.assertLessEqual(float(r["score_buyer_role"]), 15.0)


if __name__ == "__main__":
    unittest.main()


class ConfigKeys(unittest.TestCase):
    def test_placeholder_keys_count_as_unset(self):
        import importlib, os
        from src import config
        old = {k: os.environ.get(k) for k in ("BRAVE_SEARCH_API_KEY", "UN_COMTRADE_API_KEY")}
        try:
            os.environ["BRAVE_SEARCH_API_KEY"], os.environ["UN_COMTRADE_API_KEY"] = "none", " None "
            importlib.reload(config)
            self.assertEqual((config.BRAVE_SEARCH_API_KEY, config.UN_COMTRADE_API_KEY, config.has_comtrade_key()), ("", "", False))
            os.environ["BRAVE_SEARCH_API_KEY"] = "real-key-123"
            importlib.reload(config)
            self.assertEqual(config.BRAVE_SEARCH_API_KEY, "real-key-123")
        finally:
            for k, v in old.items():
                os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)
            importlib.reload(config)
