"""AVYUKT local web app: enter a product profile, run the pipeline, browse results.

    python -m src.web [--port 8000] [--data-dir PATH]

Standard library only. By default binds to 127.0.0.1 with no login (local use).
To accept outside connections (--host 0.0.0.0, or the PORT env var on a host platform) you MUST set
AVYUKT_PASSWORD: every request then needs HTTP Basic auth (user: avyukt). Use HTTPS in front of it.
POST requests must come from the same origin. One pipeline run at a time.
"""

from __future__ import annotations

import argparse
import base64
import hmac
import json
import os
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from src.pipeline import export, runner
from src.pipeline.common import Context, Paths, new_run_id, read_json, utcnow

MAX_BODY = 20_000


AUTH_USER = "avyukt"
LOCAL_HOSTS = ("127.0.0.1", "localhost")


class AppState:
    def __init__(self, paths: Paths, password: str = "", locked: bool = False):
        self.paths = paths
        self.password = password
        self.locked = locked  # public address without a password: show a setup message only
        self.lock = threading.Lock()
        self.status = {"state": "idle", "run_id": "", "message": "", "stages": [], "summary": None}

    def snapshot(self):
        with self.lock:
            return json.loads(json.dumps(self.status))


def validate_run_request(body):
    """-> (options dict, selected stages or None). Raises ValueError with a user-facing message."""
    if not isinstance(body, dict):
        raise ValueError("Request body must be a JSON object.")
    stage = body.get("stage") or None
    if stage is not None and stage not in runner.STAGE_NAMES:
        raise ValueError(f"Unknown stage '{stage}'.")
    opts = {"offline": bool(body.get("offline"))}
    product = str(body.get("product") or "").strip()
    if product:
        if len(product) > 100:
            raise ValueError("Product name is too long (max 100 characters).")
        countries = body.get("countries") or []
        if isinstance(countries, str):
            countries = [c for c in re.split(r"[,;\n]", countries) if c.strip()]
        if not isinstance(countries, list) or not countries or len(countries) > 10:
            raise ValueError("Enter 1-10 target countries.")
        hs = re.sub(r"\D", "", str(body.get("hs") or ""))
        if body.get("hs") and len(hs) not in (4, 6, 8, 10):
            raise ValueError("HS code must have 4, 6, 8 or 10 digits.")
        opts["profile_input"] = {
            "product_name": product, "product_form": str(body.get("form") or "")[:100],
            "origin": str(body.get("origin") or "India")[:60], "hs_code": hs,
            "target_countries": [str(c).strip()[:60] for c in countries]}
    return opts, ({stage} if stage else None)


SEED_FIELDS = ["raw_company_name", "source_url", "source_type", "country", "source_name"]
MAX_SEED_LINES = 200
_URL_RE = re.compile(r"(https?://[^\s|,;]+|www\.[^\s|,;]+)", re.I)


def parse_seed_text(text, paths):
    """Parse pasted lines like 'Company name | https://site.com | Indonesia' (country optional).
    Returns (rows to add, rejected [(line, reason)], duplicates skipped)."""
    from src.pipeline import countries
    from src.pipeline.common import read_csv
    from src.pipeline.discovery import classify_domain, domain_of, normalize_url
    existing = {normalize_url(r["source_url"]).rstrip("/").lower() for r in read_csv(paths.discovery_seeds)}
    rows, rejected, dups = [], [], 0
    lines = [l.strip() for l in str(text or "").splitlines() if l.strip()]
    if len(lines) > MAX_SEED_LINES:
        raise ValueError(f"Add at most {MAX_SEED_LINES} lines at a time.")
    for line in lines:
        m = _URL_RE.search(line)
        if not m:
            rejected.append((line[:80], "no website address (start it with https:// or www.)"))
            continue
        url = normalize_url(m.group(1).rstrip(".,)"))
        name = re.sub(r"[|,;\t\-:]+$", "", line[:m.start()].strip()).strip()
        country = re.sub(r"^[|,;\t\-:\s]+", "", line[m.end():]).strip(" |,;\t")
        if not url:
            rejected.append((line[:80], "invalid website address"))
        elif not name:
            rejected.append((line[:80], "missing company name before the address"))
        elif country and not countries.to_iso3(country):
            rejected.append((line[:80], f"unknown country '{country[:30]}' (leave blank or use a standard name)"))
        elif url.rstrip("/").lower() in existing:
            dups += 1
        else:
            existing.add(url.rstrip("/").lower())
            rows.append({"raw_company_name": name[:120], "source_url": url,
                         "source_type": classify_domain(domain_of(url))[0], "country": country,
                         "source_name": "pasted in web app"})
    return rows, rejected, dups


def add_seeds(paths, text):
    import csv
    rows, rejected, dups = parse_seed_text(text, paths)
    if rows:
        paths.inbox.mkdir(parents=True, exist_ok=True)
        new_file = not paths.discovery_seeds.exists() or paths.discovery_seeds.stat().st_size == 0
        with paths.discovery_seeds.open("a", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=SEED_FIELDS)
            if new_file:
                w.writeheader()
            w.writerows(rows)
    return {"added": len(rows), "duplicates_skipped": dups,
            "rejected": [{"line": l, "reason": r} for l, r in rejected]}


def start_run(state: AppState, opts, selected):
    """Start a background run. Returns (ok, message)."""
    with state.lock:
        if state.status["state"] == "running":
            return False, "A run is already in progress."
        now = utcnow()
        run_id = new_run_id(now)
        state.status = {"state": "running", "run_id": run_id, "message": "Starting...", "stages": [], "summary": None}

    def work():
        try:
            ctx = Context(paths=state.paths, run_id=run_id, now=now, offline=opts.get("offline", False))
            if "profile_input" in opts:
                ctx.options["profile_input"] = opts["profile_input"]
            results, summary = runner.execute(ctx, selected)
            with state.lock:
                state.status = {"state": "done", "run_id": run_id, "message": "Run finished.",
                                "stages": [{"stage": r.stage, "status": r.status, "counts": r.counts,
                                            "warnings": r.warnings[:5], "errors": r.errors} for r in results],
                                "summary": {k: summary[k] for k in ("candidate_count", "company_count",
                                            "qualified_company_count", "classification_counts",
                                            "stages_failed", "warnings")}}
        except Exception as exc:  # never leave the UI stuck in 'running'
            with state.lock:
                state.status = {"state": "error", "run_id": run_id, "message": f"{type(exc).__name__}: {exc}",
                                "stages": [], "summary": None}

    threading.Thread(target=work, daemon=True).start()
    return True, run_id


def make_handler(state: AppState):
    page = Path(export.TEMPLATE)

    class Handler(BaseHTTPRequestHandler):
        server_version = "AVYUKT"

        def log_message(self, *a):
            pass

        def _host_ok(self):
            if state.password:  # public mode: the password protects it; any hostname is fine
                return True
            host = (self.headers.get("Host") or "").split(":")[0]
            return host in LOCAL_HOSTS  # local mode: blocks DNS-rebinding style access

        def _authorized(self):
            if not state.password:
                return True
            hdr = self.headers.get("Authorization") or ""
            if hdr.startswith("Basic "):
                try:
                    user, _, pw = base64.b64decode(hdr[6:]).decode("utf-8").partition(":")
                except Exception:
                    return False
                ok_user = hmac.compare_digest(user.encode(), AUTH_USER.encode())
                ok_pw = hmac.compare_digest(pw.encode(), state.password.encode())
                return ok_user and ok_pw
            return False

        def _deny_auth(self):
            body = b'{"error": "authentication required"}'
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Basic realm="AVYUKT", charset="UTF-8"')
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _same_origin(self):
            origin = self.headers.get("Origin")
            if not origin:
                return True
            if state.password:
                return origin.split("://", 1)[-1] == (self.headers.get("Host") or "")
            return origin.split("://", 1)[-1].split(":")[0] in LOCAL_HOSTS

        def _send(self, code, body, ctype="application/json"):
            data = body if isinstance(body, bytes) else body.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", ctype + "; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(data)

        def _json(self, code, obj):
            self._send(code, json.dumps(obj, ensure_ascii=False))

        def _payload(self):
            ctx = Context(paths=state.paths, now=utcnow())
            summary = read_json(state.paths.run_summary) or {}
            info = {"run_id": summary.get("run_id", ""), "warnings": summary.get("warnings", []),
                    "stages_failed": summary.get("stages_failed", [])}
            p = export.build_dashboard_payload(ctx, info)
            p["live"] = True
            p["last_run"] = {k: summary.get(k) for k in ("run_id", "started_at", "completed_at", "stages_failed")}
            return p

        def _locked_reply(self):
            msg = ("AVYUKT is running but locked: the AVYUKT_PASSWORD environment variable is missing or empty. "
                   "Set it to a long random value in your hosting dashboard (Environment tab) and redeploy.")
            self._send(503, msg, "text/plain")

        def do_GET(self):
            path = self.path.split("?")[0]
            if path == "/healthz":  # for hosting platforms; reveals nothing
                return self._json(200, {"ok": True})
            if state.locked:
                return self._locked_reply()
            if not self._host_ok():
                return self._json(403, {"error": "forbidden host"})
            if not self._authorized():
                return self._deny_auth()
            if path == "/":
                html = page.read_text(encoding="utf-8").replace("/*__DATA__*/null", "null")
                return self._send(200, html, "text/html")
            if path == "/api/data":
                return self._json(200, self._payload())
            if path == "/api/status":
                return self._json(200, state.snapshot())
            if path == "/api/seeds":
                from src.pipeline.common import read_csv
                return self._json(200, {"count": len(read_csv(state.paths.discovery_seeds))})
            if path == "/api/profile":
                return self._json(200, read_json(state.paths.input_profile) or {})
            self._json(404, {"error": "not found"})

        def do_POST(self):
            if state.locked:
                return self._locked_reply()
            if not self._host_ok():
                return self._json(403, {"error": "forbidden host"})
            if not self._authorized():
                return self._deny_auth()
            if not self._same_origin():
                return self._json(403, {"error": "cross-origin request refused"})
            if self.path not in ("/api/run", "/api/seeds"):
                return self._json(404, {"error": "not found"})
            if "application/json" not in (self.headers.get("Content-Type") or ""):
                return self._json(415, {"error": "send application/json"})
            n = int(self.headers.get("Content-Length") or 0)
            if n > MAX_BODY:
                return self._json(413, {"error": "body too large"})
            if self.path == "/api/seeds":
                try:
                    body = json.loads(self.rfile.read(n) or b"{}")
                    if not isinstance(body, dict):
                        raise ValueError("Request body must be a JSON object.")
                    return self._json(200, add_seeds(state.paths, body.get("text", "")))
                except (ValueError, json.JSONDecodeError) as exc:
                    return self._json(400, {"error": str(exc)})
            try:
                opts, selected = validate_run_request(json.loads(self.rfile.read(n) or b"{}"))
            except (ValueError, json.JSONDecodeError) as exc:
                return self._json(400, {"error": str(exc)})
            ok, info = start_run(state, opts, selected)
            self._json(202 if ok else 409, {"run_id": info} if ok else {"error": info})

    return Handler


def make_server(paths: Paths, port=8000, host="127.0.0.1", password=""):
    """Public address without a password => the server still starts but only shows a setup message.
    (Exiting instead made hosting platforms report a silent 'port timeout'.)"""
    locked = host not in LOCAL_HOSTS and not password
    paths.ensure()
    return ThreadingHTTPServer((host, port), make_handler(AppState(paths, password, locked)))


def main(argv=None):
    ap = argparse.ArgumentParser(prog="python -m src.web")
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8000)))
    ap.add_argument("--host", default=None, help="default 127.0.0.1; 0.0.0.0 when the PORT env var is set")
    ap.add_argument("--data-dir", default=os.environ.get("AVYUKT_DATA_DIR"))
    args = ap.parse_args(argv)
    host = args.host or ("0.0.0.0" if os.environ.get("PORT") else "127.0.0.1")
    password = os.environ.get("AVYUKT_PASSWORD", "")
    print(f"AVYUKT starting: host={host} port={args.port} password_set={bool(password)} "
          f"data_dir={args.data_dir or 'default'}", flush=True)
    srv = make_server(Paths(args.data_dir), args.port, host, password)
    mode = ("password-protected (user: avyukt)" if password else
            "LOCKED - set AVYUKT_PASSWORD" if host not in LOCAL_HOSTS else "local, no login")
    print(f"AVYUKT running on {host}:{srv.server_port} - {mode}  (Ctrl+C to stop)", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
