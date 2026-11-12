import json
import threading
import time
import unittest
import urllib.error
import urllib.request

from src.web import server
from tests.helpers import PipelineTestCase


class WebBase(PipelineTestCase):
    def setUp(self):
        super().setUp()
        self.srv = server.make_server(self.paths, 0)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.addCleanup(self.srv.server_close)
        self.addCleanup(self.srv.shutdown)
        self.base = f"http://127.0.0.1:{self.srv.server_port}"

    def call(self, path, body=None, headers=None, method=None):
        h = {"Content-Type": "application/json", **(headers or {})}
        req = urllib.request.Request(self.base + path, data=json.dumps(body).encode() if body is not None else None,
                                     headers=h, method=method or ("POST" if body is not None else "GET"))
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.status, json.loads(r.read() or b"{}") if "json" in r.headers["Content-Type"] else r.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read() or b"{}")

    def wait_done(self):
        for _ in range(100):
            s = self.call("/api/status")[1]
            if s["state"] != "running":
                return s
            time.sleep(0.1)
        self.fail("run did not finish")


class WebApp(WebBase):
    def test_serves_page_and_empty_data(self):
        code, html = self.call("/")
        self.assertEqual(code, 200)
        self.assertIn("AVYUKT buyer shortlist", html)
        code, data = self.call("/api/data")
        self.assertTrue(data["live"])
        self.assertEqual(data["counts"]["discovered"], 0)

    def test_run_profile_stage_from_form_input_updates_data(self):
        code, j = self.call("/api/run", {"product": "Peanuts", "origin": "India", "hs": "1202.42",
                                         "countries": "Indonesia, Vietnam", "stage": "profile", "offline": True})
        self.assertEqual(code, 202, j)
        s = self.wait_done()
        self.assertEqual(s["state"], "done")
        self.assertEqual(s["stages"][0]["status"], "completed_with_warnings")
        p = self.call("/api/data")[1]["product_profile"]
        self.assertEqual(p["product_name"], "Peanuts")
        self.assertEqual([m["iso3"] for m in p["target_markets"]], ["IDN", "VNM"])
        self.assertEqual(p["product_forms"][0]["verification_status"], "needs_verification")

    def test_full_offline_run_with_saved_profile(self):
        self.assertEqual(self.call("/api/run", {"offline": True})[0], 202)
        s = self.wait_done()
        self.assertEqual(s["state"], "done")
        self.assertEqual(s["summary"]["stages_failed"], [])
        self.assertTrue((self.paths.exports / "dashboard.html").exists())

    def test_invalid_input_rejected(self):
        for body in ({"product": "X", "countries": []}, {"product": "X", "countries": "Indonesia", "hs": "12"},
                     {"stage": "nope"}, {"product": "x" * 200, "countries": "Indonesia"}, []):
            self.assertEqual(self.call("/api/run", body)[0], 400, body)

    def test_bad_profile_surfaces_as_failed_stage_not_crash(self):
        self.call("/api/run", {"product": "X", "countries": "Atlantis", "stage": "profile", "offline": True})
        s = self.wait_done()
        self.assertEqual(s["stages"][0]["status"], "failed")
        self.assertIn("Atlantis", s["stages"][0]["errors"][0])

    def test_cross_origin_and_wrong_host_refused(self):
        self.assertEqual(self.call("/api/run", {}, headers={"Origin": "https://evil.example"})[0], 403)
        self.assertEqual(self.call("/api/data", headers={"Host": "evil.example"})[0], 403)
        req = urllib.request.Request(self.base + "/api/run", data=b"{}", headers={"Content-Type": "text/plain"}, method="POST")
        with self.assertRaises(urllib.error.HTTPError) as cm:
            urllib.request.urlopen(req)
        self.assertEqual(cm.exception.code, 415)

    def test_second_run_while_running_is_refused(self):
        st = server.AppState(self.paths)
        st.status["state"] = "running"
        ok, msg = server.start_run(st, {"offline": True}, None)
        self.assertFalse(ok)

    def test_unknown_path_404(self):
        self.assertEqual(self.call("/nope")[0], 404)


class WebSeeds(WebBase):
    def test_paste_companies_validates_dedupes_and_feeds_discovery(self):
        text = "\n".join([
            "PT Fiktif Kacang | https://fiktif-kacang.test | Indonesia",
            "Fiktif Trading BV, www.fiktif-trading.test, Netherlands",
            "No Address Co | Indonesia",
            "Bad Country Co | https://bad-country.test | Narnia",
            "https://only-address.test",
            "PT Fiktif Kacang | https://fiktif-kacang.test/ | Indonesia",
        ])
        code, j = self.call("/api/seeds", {"text": text})
        self.assertEqual(code, 200, j)
        self.assertEqual((j["added"], j["duplicates_skipped"], len(j["rejected"])), (2, 1, 3))
        reasons = " ".join(r["reason"] for r in j["rejected"])
        self.assertIn("no website address", reasons)
        self.assertIn("unknown country", reasons)
        self.assertIn("missing company name", reasons)
        self.assertEqual(self.call("/api/seeds")[1]["count"], 2)
        # a second paste of the same companies adds nothing
        self.assertEqual(self.call("/api/seeds", {"text": text})[1]["added"], 0)
        # and the pipeline discovery stage picks them up as candidates (no network needed)
        self.call("/api/run", {"stage": "profile", "offline": True}); self.wait_done()
        self.call("/api/run", {"stage": "discovery", "offline": True}); self.wait_done()
        from src.pipeline.common import read_csv
        names = {c["raw_company_name"] for c in read_csv(self.paths.candidates)}
        self.assertEqual(names, {"PT Fiktif Kacang", "Fiktif Trading BV"})

    def test_seed_input_limits_and_bad_body(self):
        self.assertEqual(self.call("/api/seeds", {"text": "\n".join(f"A{i} | https://a{i}.test" for i in range(201))})[0], 400)
        self.assertEqual(self.call("/api/seeds", [])[0], 400)
        self.assertEqual(self.call("/api/seeds", {"text": ""})[1]["added"], 0)

    def test_seeds_need_login_when_password_set(self):
        srv = server.make_server(self.paths, 0, "127.0.0.1", "pw-123456789")
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.addCleanup(srv.server_close); self.addCleanup(srv.shutdown)
        req = urllib.request.Request(f"http://127.0.0.1:{srv.server_port}/api/seeds", data=b'{"text":"x"}',
                                     headers={"Content-Type": "application/json"}, method="POST")
        with self.assertRaises(urllib.error.HTTPError) as cm:
            urllib.request.urlopen(req)
        self.assertEqual(cm.exception.code, 401)


class WebAuth(PipelineTestCase):
    def start(self, password):
        srv = server.make_server(self.paths, 0, "127.0.0.1", password)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        return f"http://127.0.0.1:{srv.server_port}"

    def get(self, url, auth=None, headers=None, data=None):
        import base64
        h = dict(headers or {})
        if auth:
            h["Authorization"] = "Basic " + base64.b64encode(auth.encode()).decode()
        req = urllib.request.Request(url, headers=h, data=data, method="POST" if data is not None else "GET")
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.status
        except urllib.error.HTTPError as e:
            return e.code

    def test_password_required_when_set(self):
        base = self.start("s3cret-long-value")
        self.assertEqual(self.get(base + "/api/data"), 401)
        self.assertEqual(self.get(base + "/"), 401)
        self.assertEqual(self.get(base + "/api/data", auth="avyukt:wrong"), 401)
        self.assertEqual(self.get(base + "/api/data", auth="other:s3cret-long-value"), 401)
        self.assertEqual(self.get(base + "/api/data", auth="avyukt:s3cret-long-value"), 200)
        self.assertEqual(self.get(base + "/healthz"), 200)  # health check reveals nothing

    def test_run_requires_login_and_same_origin(self):
        base = self.start("pw-123456789")
        body = b"{}"
        j = {"Content-Type": "application/json"}
        self.assertEqual(self.get(base + "/api/run", headers=j, data=body), 401)
        self.assertEqual(self.get(base + "/api/run", auth="avyukt:pw-123456789",
                                  headers={**j, "Origin": "https://evil.example"}, data=body), 403)

    def test_public_bind_without_password_starts_locked_and_exposes_nothing(self):
        srv = server.make_server(self.paths, 0, "127.0.0.1", "")
        srv.RequestHandlerClass  # local mode is open
        srv.server_close()
        srv = server.make_server(self.paths, 0, "0.0.0.0", "")   # public + no password => locked
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        base = f"http://127.0.0.1:{srv.server_port}"
        self.assertEqual(self.get(base + "/healthz"), 200)       # port is open, platform health check passes
        for path in ("/", "/api/data", "/api/status"):
            self.assertEqual(self.get(base + path), 503, path)   # but nothing is served
        self.assertEqual(self.get(base + "/api/run", headers={"Content-Type": "application/json"}, data=b"{}"), 503)
        self.assertEqual(self.get(base + "/api/data", auth="avyukt:anything"), 503)

    def test_no_password_means_local_mode_still_open(self):
        base = self.start("")
        self.assertEqual(self.get(base + "/api/data"), 200)


if __name__ == "__main__":
    unittest.main()
