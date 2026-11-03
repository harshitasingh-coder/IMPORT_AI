"""Shared fixtures. All companies here are FICTIONAL (.test domains) and exist only in temp dirs."""

import csv
import json
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from src.pipeline.common import Context, Paths
from src.pipeline.extraction import FetchResult
from src.schemas import RAW_IMPORTER_FIELDS

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 11, 3, 12, 0, tzinfo=timezone.utc)
RETRIEVED = "2026-11-03T11:00:00+00:00"


def page(title, body, head="", ):
    return f"<html><head><title>{title}</title>{head}</head><body>{body}</body></html>"


NUSANTARA_HOME = page(
    "PT Nusantara Kacang Impor - Home",
    '<h1>Welcome</h1><p>We import raw groundnut kernels from India for the Indonesian food industry. '
    'We have been importing peanuts since 2009.</p>'
    '<p>We are a leading importer and distributor of nuts in Indonesia.</p>'
    '<a href="/about">About us</a><a href="/contact">Contact</a><a href="/products">Products</a>',
    head='<meta name="description" content="Importer of groundnuts and nuts in Jakarta.">'
         '<script type="application/ld+json">{"@type":"Organization","name":"PT Nusantara Kacang Impor",'
         '"address":{"streetAddress":"Jl. Sudirman 1","addressLocality":"Jakarta","addressCountry":"Indonesia"}}</script>')
NUSANTARA_CONTACT = page("Contact", '<p>Email: <a href="mailto:info@nusantara-kacang.test">info@nusantara-kacang.test</a></p>'
                         '<p>Tel: +62 21 5550 1234</p><p>Alamat: Jl. Sudirman 1, Jakarta, Indonesia</p>')
NUSANTARA_ABOUT = page("About", "<p>Founded in 2009, serving bakeries and snack makers.</p>")
NUSANTARA_PRODUCTS = page("Products", "<p>Raw groundnut kernels, shelled peanuts and cashew nuts.</p>")

HANOI_HOME = page("Hanoi Nut Processing JSC", "<p>We process and sell shelled peanuts to local factories.</p>"
                  '<p>Our blanching and sorting plant handles peanuts daily.</p>')
DUTCH_HOME = page("Dutch Delights BV", "<p>Pinda's roasted and salted snacks for supermarkets.</p>")
NOIMPORT_HOME = page("Plain Foods Ltd", "<p>We do not import peanuts. We sell packaged biscuits.</p>")

SITES = {
    "https://nusantara-kacang.test/": NUSANTARA_HOME,
    "https://nusantara-kacang.test/about": NUSANTARA_ABOUT,
    "https://nusantara-kacang.test/contact": NUSANTARA_CONTACT,
    "https://nusantara-kacang.test/products": NUSANTARA_PRODUCTS,
    "https://hanoi-nuts.test/": HANOI_HOME,
    "https://dutch-delights.test/": DUTCH_HOME,
    "https://plain-foods.test/": NOIMPORT_HOME,
}


class FakeFetcher:
    def __init__(self, sites=None, errors=None):
        self.sites, self.errors, self.calls = dict(SITES if sites is None else sites), errors or {}, []

    def __call__(self, url):
        self.calls.append(url)
        if url in self.errors:
            return FetchResult(url=url, status_code=self.errors[url], error=f"http_{self.errors[url]}", retrieved_at=RETRIEVED)
        if url in self.sites:
            return FetchResult(url=url, final_url=url, status_code=200, content_type="text/html",
                               body=self.sites[url], retrieved_at=RETRIEVED)
        return FetchResult(url=url, status_code=404, error="http_404", retrieved_at=RETRIEVED)


class FakeSearch:
    def __init__(self, results_by_term=None, fail=False):
        self.results, self.fail, self.queries = results_by_term or {}, fail, []

    def search(self, query):
        self.queries.append(query)
        if self.fail:
            raise RuntimeError("search backend down")
        return self.results.get(query, [])


def record(**kw):
    base = {f: "" for f in RAW_IMPORTER_FIELDS}
    base.update(kw)
    return base


def write_raw_records(paths, rows):
    paths.processed.mkdir(parents=True, exist_ok=True)
    with paths.raw_importer_records.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=RAW_IMPORTER_FIELDS)
        w.writeheader()
        w.writerows(rows)


def write_csv_rows(path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


class PipelineTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="avy_test_"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.data = self.tmp / "data"
        self.data.mkdir()
        shutil.copy(ROOT / "data" / "product_profile.json", self.data / "product_profile.json")
        self.paths = Paths(self.data)
        self.paths.ensure()

    def ctx(self, fetcher=None, search=None, offline=False, **options):
        c = Context(paths=self.paths, run_id="run-test", now=NOW, offline=offline,
                    fetcher=fetcher, search_provider=search, options=dict(options))
        return c

    def read(self, path):
        from src.pipeline.common import read_csv
        return read_csv(path)
