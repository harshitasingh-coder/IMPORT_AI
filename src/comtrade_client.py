"""UN Comtrade client: collects country-level groundnut trade statistics.

For each year it makes two queries:
  1. Imports of the product by every reporting country from the World.
  2. India's exports of the product to each partner country.

Every raw API response is saved to data/raw/ for traceability, and the
combined, flattened rows are written to data/processed/market_data.csv.

Run:  python -m src.comtrade_client
      python -m src.comtrade_client --years 2022 2023
"""

import argparse
import csv
import json
import logging
import time
from datetime import datetime, timezone

import requests

from src.config import (
    LOGS_DIR,
    PROCESSED_DIR,
    PROFILE_PATH,
    RAW_DIR,
    UN_COMTRADE_API_KEY,
    ensure_data_dirs,
)

API_URL = "https://comtradeapi.un.org/data/v1/get/C/A/HS"
INDIA_REPORTER_CODE = "699"
WORLD_PARTNER_CODE = 0
SECONDS_BETWEEN_CALLS = 1.5
MAX_RETRIES = 3

# Without these filters the API repeats each figure across customs-procedure,
# transport-mode and second-partner breakdowns. These select the totals.
TOTALS_ONLY = {"customsCode": "C00", "motCode": "0", "partner2Code": "0"}

CSV_FIELDS = [
    "product_name",
    "hs_code",
    "hs_description",
    "trade_flow",
    "period",
    "reporter_country",
    "reporter_iso",
    "partner_country",
    "partner_iso",
    "trade_value_usd",
    "net_weight_kg",
    "quantity",
    "quantity_unit",
    "is_reported",
    "source_url_or_id",
    "retrieved_at",
]

log = logging.getLogger("comtrade")


class ComtradeError(RuntimeError):
    pass


def load_profile():
    with PROFILE_PATH.open("r", encoding="utf-8") as file:
        return json.load(file)


def hs_codes_from_profile(profile):
    return [form["candidate_hs_code"] for form in profile["product_forms"]]


def fetch(params):
    """Call the API with retries. Returns (response_json, request_url)."""
    if not UN_COMTRADE_API_KEY:
        raise ComtradeError("UN_COMTRADE_API_KEY is missing. Add it to .env.")

    headers = {"Ocp-Apim-Subscription-Key": UN_COMTRADE_API_KEY}
    for attempt in range(1, MAX_RETRIES + 1):
        response = requests.get(API_URL, params=params, headers=headers, timeout=90)
        if response.status_code == 429 or response.status_code >= 500:
            wait = 10 * attempt
            log.warning("HTTP %s, retrying in %ss", response.status_code, wait)
            time.sleep(wait)
            continue
        if response.status_code != 200:
            raise ComtradeError(f"HTTP {response.status_code}: {response.text[:300]}")
        body = response.json()
        if body.get("error"):
            raise ComtradeError(f"API error: {body['error']}")
        return body, response.url
    raise ComtradeError(f"Gave up after {MAX_RETRIES} attempts")


def save_raw(body, request_url, name):
    """Save the untouched response plus the request it came from."""
    path = RAW_DIR / f"{name}.json"
    record = {
        "request_url": request_url,
        "retrieved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "response": body,
    }
    path.write_text(json.dumps(record, indent=1), encoding="utf-8")
    return path, record["retrieved_at"]


def to_rows(body, product_name, source_id, retrieved_at):
    rows = []
    for item in body.get("data") or []:
        rows.append({
            "product_name": product_name,
            "hs_code": item["cmdCode"],
            "hs_description": item.get("cmdDesc"),
            "trade_flow": item.get("flowDesc"),
            "period": item["period"],
            "reporter_country": item.get("reporterDesc"),
            "reporter_iso": item.get("reporterISO"),
            "partner_country": item.get("partnerDesc"),
            "partner_iso": item.get("partnerISO"),
            "trade_value_usd": item.get("primaryValue"),
            "net_weight_kg": item.get("netWgt") or None,
            "quantity": item.get("qty") or None,
            "quantity_unit": item.get("qtyUnitAbbr"),
            "is_reported": item.get("isReported"),
            "source_url_or_id": source_id,
            "retrieved_at": retrieved_at,
        })
    return rows


def collect_year(year, hs_codes, product_name):
    """Run both queries for one year. Returns flattened rows."""
    queries = {
        f"imports_world_{year}": {
            "period": str(year),
            "flowCode": "M",
            "partnerCode": str(WORLD_PARTNER_CODE),
        },
        f"india_exports_{year}": {
            "period": str(year),
            "flowCode": "X",
            "reporterCode": INDIA_REPORTER_CODE,
        },
    }

    rows = []
    for name, query in queries.items():
        params = {
            "cmdCode": ",".join(hs_codes),
            "includeDesc": "true",
            **TOTALS_ONLY,
            **query,
        }
        body, url = fetch(params)
        raw_path, retrieved_at = save_raw(body, url, f"comtrade_{name}")
        source_id = f"UN Comtrade API v1 | {raw_path.name}"
        new_rows = to_rows(body, product_name, source_id, retrieved_at)
        if name.startswith("india_exports"):
            # Keep partner-level rows only; the World total is derivable.
            new_rows = [r for r in new_rows if r["partner_iso"] != "W00"]
        log.info("%s: %d rows", name, len(new_rows))
        rows.extend(new_rows)
        time.sleep(SECONDS_BETWEEN_CALLS)
    return rows


def write_csv(rows, path):
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def setup_logging():
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(LOGS_DIR / "comtrade.log", encoding="utf-8"),
        ],
    )


def main(argv=None):
    profile = load_profile()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--years", nargs="+", type=int, default=profile["years_to_collect"])
    args = parser.parse_args(argv)

    ensure_data_dirs()
    setup_logging()

    hs_codes = hs_codes_from_profile(profile)
    log.info("Collecting %s, HS %s, years %s", profile["product_name"], hs_codes, args.years)

    all_rows = []
    for year in args.years:
        all_rows.extend(collect_year(year, hs_codes, profile["product_name"]))

    out_path = PROCESSED_DIR / "market_data.csv"
    write_csv(all_rows, out_path)

    print(f"\nSaved {len(all_rows)} rows to {out_path}")
    for year in args.years:
        reporters = {r["reporter_iso"] for r in all_rows
                     if r["period"] == str(year) and r["trade_flow"] == "Import"}
        print(f"  {year}: {len(reporters)} importing countries reported")


if __name__ == "__main__":
    main()
