"""Rank importing countries from market_data.csv to support market selection.

Run:  python -m src.market_shortlist

Writes data/processed/market_shortlist.csv. The latest year is left out by
default because many countries have not reported it yet.

Notes for reading the output:
- india_share_pct compares India's reported exports with the partner's
  reported imports. These come from two different reporters (FOB vs CIF,
  timing, re-exports), so values above 100% can occur.
- usd_per_kg is unreliable where a country reports weight inconsistently.
"""

import argparse

import pandas as pd

from src.config import PROCESSED_DIR

MARKET_DATA_PATH = PROCESSED_DIR / "market_data.csv"
SHORTLIST_PATH = PROCESSED_DIR / "market_shortlist.csv"


def build_shortlist(market_data, years):
    data = market_data[market_data["period"].isin(years)]
    first, last = years[0], years[-1]

    imports = data[data["trade_flow"] == "Import"]
    by_year = imports.pivot_table(
        index=["reporter_iso", "reporter_country"],
        columns="period",
        values="trade_value_usd",
        aggfunc="sum",
    )
    weight = imports[imports["period"] == last].groupby("reporter_iso")["net_weight_kg"].sum()

    exports = data[data["trade_flow"] == "Export"]
    india_by_year = exports.pivot_table(
        index="partner_iso", columns="period", values="trade_value_usd", aggfunc="sum"
    )

    iso = by_year.index.get_level_values("reporter_iso")
    india = india_by_year.reindex(iso)

    shortlist = pd.DataFrame({
        "country": by_year.index.get_level_values("reporter_country"),
        "iso3": iso,
        "years_reported": by_year.notna().sum(axis=1).values,
        "avg_import_usd_m": by_year.mean(axis=1).values / 1e6,
        f"import_{last}_usd_m": by_year.get(last).values / 1e6,
        f"growth_{first}_{last}_pct": (by_year.get(last) / by_year.get(first) - 1).values * 100,
        f"india_export_{last}_usd_m": india.get(last).values / 1e6,
        f"india_share_{last}_pct": (india.get(last) / by_year.get(last)).values * 100,
        f"usd_per_kg_{last}": (by_year.get(last) / weight.reindex(iso)).values,
    })
    return shortlist.sort_values("avg_import_usd_m", ascending=False).round(2)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--years", nargs="+", type=int, default=[2021, 2022, 2023, 2024])
    parser.add_argument("--top", type=int, default=15)
    args = parser.parse_args(argv)

    market_data = pd.read_csv(MARKET_DATA_PATH)
    shortlist = build_shortlist(market_data, sorted(args.years))
    shortlist.to_csv(SHORTLIST_PATH, index=False, encoding="utf-8-sig")

    with pd.option_context("display.width", 200, "display.max_columns", None):
        print(shortlist.head(args.top).to_string(index=False))
    print(f"\nSaved {len(shortlist)} countries to {SHORTLIST_PATH}")


if __name__ == "__main__":
    main()
