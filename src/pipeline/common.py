"""Shared pipeline plumbing: paths, IDs, CSV/JSON IO, stage results, audit log."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import shutil
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from src import config

STATUS_COMPLETED = "completed"
STATUS_WARNINGS = "completed_with_warnings"
STATUS_FAILED = "failed"
STATUS_SKIPPED = "skipped"


class Paths:
    """All data locations. Pass a different root for tests/demos."""

    def __init__(self, data: Path | str | None = None):
        self.data = Path(data) if data else config.DATA_DIR

    raw = property(lambda s: s.data / "raw")
    processed = property(lambda s: s.data / "processed")
    logs = property(lambda s: s.data / "logs")
    inbox = property(lambda s: s.data / "inbox")
    exports = property(lambda s: s.data / "exports")
    manual = property(lambda s: s.data / "manual")  # human decisions; never written by code
    enrichment = property(lambda s: s.data / "raw" / "enrichment")
    discovery_cache = property(lambda s: s.data / "raw" / "discovery")
    archive = property(lambda s: s.data / "processed" / "archive")
    input_profile = property(lambda s: s.data / "product_profile.json")

    def ensure(self):
        for p in (self.raw, self.processed, self.logs, self.inbox, self.exports,
                  self.manual, self.enrichment, self.discovery_cache):
            p.mkdir(parents=True, exist_ok=True)

    # canonical output files
    product_profile = property(lambda s: s.processed / "product_profile.json")
    market_data = property(lambda s: s.processed / "market_data.csv")
    market_analysis = property(lambda s: s.processed / "market_analysis.csv")
    market_shortlist = property(lambda s: s.processed / "market_shortlist.csv")
    candidates = property(lambda s: s.processed / "company_candidates_raw.csv")
    discovery_rejects = property(lambda s: s.processed / "discovery_rejects.csv")
    company_profiles = property(lambda s: s.processed / "company_profiles.json")
    companies = property(lambda s: s.processed / "companies.csv")
    duplicate_suggestions = property(lambda s: s.processed / "duplicate_suggestions.csv")
    record_company_map = property(lambda s: s.processed / "record_company_map.csv")
    evidence_log = property(lambda s: s.processed / "evidence_log.csv")
    company_claims = property(lambda s: s.processed / "company_claims.csv")
    buyer_qualification = property(lambda s: s.processed / "buyer_qualification.csv")
    classification_changes = property(lambda s: s.processed / "classification_changes.csv")
    ranked_buyers = property(lambda s: s.processed / "ranked_buyers.csv")
    run_summary = property(lambda s: s.processed / "pipeline_run_summary.json")
    audit_log = property(lambda s: s.logs / "pipeline_audit.jsonl")
    # human-maintained decision files (read-only for the pipeline)
    manual_merges = property(lambda s: s.manual / "company_merge_decisions.csv")
    manual_qualification = property(lambda s: s.manual / "qualification_overrides.csv")
    discovery_seeds = property(lambda s: s.inbox / "discovery_seeds.csv")
    raw_importer_records = property(lambda s: s.processed / "raw_importer_records.csv")


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def parse_dt(value) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).strip()
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
            try:
                dt = datetime.strptime(text[:10], fmt)
                break
            except ValueError:
                dt = None
        if dt is None:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def age_days(value, now: datetime):
    dt = parse_dt(value)
    return None if dt is None else (now - dt).days


def fold(text) -> str:
    """Casefold, strip accents, collapse whitespace (deterministic)."""
    text = unicodedata.normalize("NFKD", str(text or ""))
    text = "".join(c for c in text if not unicodedata.combining(c))
    return " ".join(text.casefold().split())


def stable_id(prefix: str, *parts) -> str:
    key = "|".join(fold(p) for p in parts)
    return f"{prefix}-{hashlib.sha1(key.encode('utf-8')).hexdigest()[:10]}"


def slug(text, default="x") -> str:
    return re.sub(r"[^a-z0-9]+", "-", fold(text)).strip("-") or default


# ---------------------------------------------------------------- IO

def read_csv(path: Path) -> list[dict]:
    path = Path(path)
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


def read_csv_header(path: Path) -> list[str]:
    path = Path(path)
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as fh:
        return next(csv.reader(fh), [])


def _archive_existing(path: Path, paths: Paths, now: datetime):
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    paths.archive.mkdir(parents=True, exist_ok=True)
    dest = paths.archive / f"{path.stem}.{stamp}{path.suffix}"
    n = 1
    while dest.exists():
        dest = paths.archive / f"{path.stem}.{stamp}-{n}{path.suffix}"
        n += 1
    shutil.copy2(path, dest)
    return dest


def _render_csv(rows, fields) -> str:
    import io
    buf = io.StringIO(newline="")
    w = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
    w.writeheader()
    for r in rows:
        w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in fields})
    return buf.getvalue()


def write_output(path: Path, content: str, paths: Paths, now: datetime):
    """Write a derived output. A differing previous version is archived, never lost."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        old = path.read_text(encoding="utf-8-sig") if path.suffix == ".csv" else path.read_text(encoding="utf-8")
        new_cmp = content
        if old == new_cmp:
            return path
        _archive_existing(path, paths, now)
    path.write_text(content, encoding="utf-8-sig" if path.suffix == ".csv" else "utf-8")
    return path


def write_csv(path, rows, fields, paths: Paths, now: datetime):
    return write_output(path, _render_csv(rows, fields), paths, now)


def write_json(path, obj, paths: Paths, now: datetime):
    return write_output(path, json.dumps(obj, indent=2, ensure_ascii=False, sort_keys=False) + "\n", paths, now)


def read_json(path, default=None):
    path = Path(path)
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------- context / results

@dataclass
class StageResult:
    stage: str
    status: str = STATUS_COMPLETED
    started_at: str = ""
    completed_at: str = ""
    counts: dict = field(default_factory=dict)
    outputs: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    errors: list = field(default_factory=list)

    def warn(self, msg):
        self.warnings.append(msg)

    def to_dict(self):
        return {
            "stage": self.stage, "status": self.status, "started_at": self.started_at,
            "completed_at": self.completed_at, "counts": self.counts,
            "outputs": self.outputs, "warnings": self.warnings, "errors": self.errors,
        }


@dataclass
class Context:
    paths: Paths = field(default_factory=Paths)
    run_id: str = ""
    now: datetime = field(default_factory=utcnow)
    offline: bool = False
    fetcher: Optional[Callable] = None        # url -> FetchResult (network layer)
    search_provider: Optional[object] = None  # .search(query) -> list[dict]
    options: dict = field(default_factory=dict)

    def log(self, stage: str, event: str, **detail):
        self.paths.logs.mkdir(parents=True, exist_ok=True)
        rec = {"ts": iso(utcnow()), "run_id": self.run_id, "stage": stage, "event": event, **detail}
        with self.paths.audit_log.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def new_run_id(now: datetime) -> str:
    return "run-" + now.strftime("%Y%m%dT%H%M%SZ") + "-" + hashlib.sha1(
        str(datetime.now().timestamp()).encode()).hexdigest()[:4]
