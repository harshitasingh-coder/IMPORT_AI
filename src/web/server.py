"""AVYUKT local web app: enter a product profile, run the pipeline, browse results.

    python -m src.web [--port 8000] [--data-dir PATH]

Standard library only. Binds to 127.0.0.1; POST requests must come from the same origin
(blocks other websites from triggering runs). One pipeline run at a time.
"""

from __future__ import annotations

import argparse
import json
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from src.pipeline import export, runner
from src.pipeline.common import Context, Paths, new_run_id, read_json, utcnow

MAX_BODY = 20_000


class AppState:
    def __init__(self, paths: Paths):
        self.paths = paths
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
            host = (self.headers.get("Host") or "").split(":")[0]
            return host in ("127.0.0.1", "localhost")  # blocks DNS-rebinding style access

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

        def do_GET(self):
            if not self._host_ok():
                return self._json(403, {"error": "forbidden host"})
            path = self.path.split("?")[0]
            if path == "/":
                html = page.read_text(encoding="utf-8").replace("/*__DATA__*/null", "null")
                return self._send(200, html, "text/html")
            if path == "/api/data":
                return self._json(200, self._payload())
            if path == "/api/status":
                return self._json(200, state.snapshot())
            if path == "/api/profile":
                return self._json(200, read_json(state.paths.input_profile) or {})
            self._json(404, {"error": "not found"})

        def do_POST(self):
            if not self._host_ok():
                return self._json(403, {"error": "forbidden host"})
            origin = self.headers.get("Origin")
            if origin and origin.split("://")[-1].split(":")[0] not in ("127.0.0.1", "localhost"):
                return self._json(403, {"error": "cross-origin request refused"})
            if self.path != "/api/run":
                return self._json(404, {"error": "not found"})
            if "application/json" not in (self.headers.get("Content-Type") or ""):
                return self._json(415, {"error": "send application/json"})
            n = int(self.headers.get("Content-Length") or 0)
            if n > MAX_BODY:
                return self._json(413, {"error": "body too large"})
            try:
                opts, selected = validate_run_request(json.loads(self.rfile.read(n) or b"{}"))
            except (ValueError, json.JSONDecodeError) as exc:
                return self._json(400, {"error": str(exc)})
            ok, info = start_run(state, opts, selected)
            self._json(202 if ok else 409, {"run_id": info} if ok else {"error": info})

    return Handler


def make_server(paths: Paths, port=8000):
    paths.ensure()
    return ThreadingHTTPServer(("127.0.0.1", port), make_handler(AppState(paths)))


def main(argv=None):
    ap = argparse.ArgumentParser(prog="python -m src.web")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--data-dir")
    args = ap.parse_args(argv)
    srv = make_server(Paths(args.data_dir), args.port)
    print(f"AVYUKT running at http://127.0.0.1:{srv.server_port}  (Ctrl+C to stop)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
