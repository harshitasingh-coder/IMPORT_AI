"""Add candidate buyer companies to raw_importer_records.csv.

APEDA buy leads sit behind an exporter login and its disclaimer restricts
commercial reuse, so leads are collected by hand (APEDA after login, trade
fair exhibitor lists, embassy buyer lists, ...) into a CSV based on
data/templates/buyer_leads_template.csv. This module ingests those files.

Ingest a file:
    python -m src.buyer_discovery ingest data/inbox/apeda_2026-10-18.csv \
        --source APEDA --source-type buyer_lead --collected-by Shubhi

Show what has been collected so far:
    python -m src.buyer_discovery summary

Every ingested input file is copied to data/raw/manual/ unchanged.
Rows already in the dataset (same source_record_id) are skipped.
"""

import argparse
import csv
import hashlib
import json
import re
import shutil
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from src.config import PROCESSED_DIR, PROFILE_PATH, RAW_DIR, ensure_data_dirs
from src.schemas import RAW_IMPORTER_FIELDS

RAW_RECORDS_PATH = PROCESSED_DIR / "raw_importer_records.csv"
MANUAL_RAW_DIR = RAW_DIR / "manual"

SOURCE_TYPES = ["buyer_lead", "shipment_record", "directory_listing", "company_website"]
REQUIRED_COLUMNS = ["company_name_raw", "country_raw", "source_url"]

# Extra spellings seen in sources, on top of the names in product_profile.json.
COUNTRY_ALIASES = {
    "IDN": ["indonesia", "republic of indonesia", "id", "idn"],
    "VNM": ["viet nam", "vietnam", "vn", "vnm", "socialist republic of vietnam"],
    "NLD": ["netherlands", "the netherlands", "holland", "nl", "nld"],
}


def load_profile():
    with PROFILE_PATH.open("r", encoding="utf-8") as file:
        return json.load(file)


def target_market_lookup(profile):
    lookup = {}
    for market in profile.get("target_markets", []):
        iso3 = market["iso3"]
        for name in [market["country"], *COUNTRY_ALIASES.get(iso3, [])]:
            lookup[name.strip().lower()] = iso3
    return lookup


def mentions_product(row, synonyms):
    text = " ".join(
        row.get(field) or ""
        for field in ("product_description_raw", "buying_requirement_raw")
    ).lower()
    return any(re.search(rf"\b{re.escape(word)}\b", text) for word in synonyms)


def make_record_id(source, row):
    """Stable ID so the same lead ingested twice gets the same ID."""
    key = "|".join(
        (row.get(field) or "").strip().lower()
        for field in ("company_name_raw", "country_raw", "buying_requirement_raw",
                      "product_description_raw", "source_url")
    )
    slug = re.sub(r"[^a-z0-9]+", "-", source.lower()).strip("-")
    return f"{slug}-{hashlib.sha1(key.encode('utf-8')).hexdigest()[:10]}"


def read_csv(path):
    with Path(path).open("r", newline="", encoding="utf-8-sig") as file:
        return list(csv.DictReader(file))


def existing_ids(raw_path):
    if not raw_path.exists():
        return set()
    return {row["source_record_id"] for row in read_csv(raw_path)}


def ensure_raw_file(raw_path):
    if not raw_path.exists() or raw_path.stat().st_size == 0:
        with raw_path.open("w", newline="", encoding="utf-8") as file:
            csv.writer(file).writerow(RAW_IMPORTER_FIELDS)


def ingest(input_path, source, source_type, collected_by, raw_path=RAW_RECORDS_PATH, dry_run=False):
    profile = load_profile()
    markets = target_market_lookup(profile)
    synonyms = [s.lower() for s in profile.get("synonyms", [])]

    rows = read_csv(input_path)
    if not rows:
        print("The input file has no rows.")
        return []

    missing = [c for c in REQUIRED_COLUMNS if c not in rows[0]]
    if missing:
        raise SystemExit(f"Input is missing required columns: {', '.join(missing)}")
    unknown = [c for c in rows[0] if c not in RAW_IMPORTER_FIELDS]
    if unknown:
        print(f"Ignoring columns not in the schema: {', '.join(unknown)}")

    seen = existing_ids(raw_path)
    collected_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    new_records, problems = [], Counter()

    for line_no, row in enumerate(rows, start=2):
        if not all((row.get(c) or "").strip() for c in REQUIRED_COLUMNS):
            problems["missing company, country or source_url"] += 1
            print(f"  line {line_no}: skipped, missing company, country or source_url")
            continue

        record = {field: (row.get(field) or "").strip() for field in RAW_IMPORTER_FIELDS}
        record["source_name"] = source
        record["source_type"] = source_type
        record["collected_at"] = record["collected_at"] or collected_at
        record["collected_by"] = record["collected_by"] or collected_by
        record["source_record_id"] = record["source_record_id"] or make_record_id(source, record)
        record["target_market"] = markets.get(record["country_raw"].lower(), "")

        if record["source_record_id"] in seen:
            problems["already in dataset"] += 1
            continue
        if not record["target_market"]:
            problems["country is not a target market (kept)"] += 1
        if not mentions_product(record, synonyms):
            problems["no groundnut/peanut mention (kept)"] += 1

        seen.add(record["source_record_id"])
        new_records.append(record)

    if not dry_run and new_records:
        MANUAL_RAW_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        shutil.copy2(input_path, MANUAL_RAW_DIR / f"{stamp}_{Path(input_path).name}")

        ensure_raw_file(raw_path)
        with raw_path.open("a", newline="", encoding="utf-8") as file:
            csv.DictWriter(file, fieldnames=RAW_IMPORTER_FIELDS).writerows(new_records)

    verb = "Would add" if dry_run else "Added"
    print(f"{verb} {len(new_records)} of {len(rows)} rows from {Path(input_path).name}")
    for reason, count in problems.items():
        print(f"  {count} row(s): {reason}")
    return new_records


def summary(raw_path=RAW_RECORDS_PATH):
    rows = read_csv(raw_path) if raw_path.exists() else []
    if not rows:
        print("raw_importer_records.csv is empty. Ingest a leads file first.")
        return
    profile = load_profile()
    names = {m["iso3"]: m["country"] for m in profile.get("target_markets", [])}

    print(f"{len(rows)} raw records\n")
    print(f"{'Market':<16}{'Records':>8}{'Companies':>11}  Sources")
    by_market = Counter(r["target_market"] or "OTHER" for r in rows)
    for market in [*names, "OTHER"]:
        market_rows = [r for r in rows if (r["target_market"] or "OTHER") == market]
        if not market_rows and market == "OTHER":
            continue
        companies = {r["company_name_raw"].strip().lower() for r in market_rows}
        sources = Counter(r["source_name"] for r in market_rows)
        source_text = ", ".join(f"{s} {n}" for s, n in sources.most_common()) or "-"
        print(f"{names.get(market, 'Other'):<16}{by_market.get(market, 0):>8}{len(companies):>11}  {source_text}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    p_ingest = sub.add_parser("ingest", help="add a hand-collected leads CSV")
    p_ingest.add_argument("input", help="CSV based on data/templates/buyer_leads_template.csv")
    p_ingest.add_argument("--source", required=True, help="e.g. APEDA, Gulfood 2026, Embassy Jakarta")
    p_ingest.add_argument("--source-type", required=True, choices=SOURCE_TYPES)
    p_ingest.add_argument("--collected-by", required=True)
    p_ingest.add_argument("--dry-run", action="store_true", help="check the file without writing")
    p_ingest.add_argument("--raw-file", type=Path, default=RAW_RECORDS_PATH, help=argparse.SUPPRESS)

    p_summary = sub.add_parser("summary", help="country-wise count of collected records")
    p_summary.add_argument("--raw-file", type=Path, default=RAW_RECORDS_PATH, help=argparse.SUPPRESS)

    args = parser.parse_args(argv)
    ensure_data_dirs()

    if args.command == "ingest":
        ingest(args.input, args.source, args.source_type, args.collected_by,
               raw_path=args.raw_file, dry_run=args.dry_run)
    else:
        summary(raw_path=args.raw_file)


if __name__ == "__main__":
    main()
