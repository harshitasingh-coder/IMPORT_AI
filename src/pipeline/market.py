"""STAGE 2 - Market and trade analysis (country-level statistics only).

Reuses src.comtrade_client for retrieval. Never invents statistics: when the API is
unavailable the error is recorded and previously retrieved rows are used with their
ORIGINAL retrieval date. Country-level imports say nothing about any single company.

market_relevance_score (0-100, deterministic):
  size   50  percentile rank of average annual import value among reporting countries
  trend  20  growth first->latest analysed year (<= -20% => 0, >= +40% => 20, linear between)
  india  30  India's export value as a share of the market's imports (>= 20% => 30, linear)
"""

from __future__ import annotations

import csv
import logging
from collections import defaultdict

from src import comtrade_client, config
from src.pipeline.common import (Context, StageResult, STATUS_COMPLETED, STATUS_WARNINGS,
                                 age_days, parse_dt, read_csv, write_csv)
from src.pipeline.profile import load as load_profile
from src.pipeline.schemas import MARKET_ANALYSIS_FIELDS

log = logging.getLogger("pipeline.market")
MAX_AGE_DAYS_BEFORE_REFRESH = 30
SHORTLIST_SIZE = 15


def _f(v):
    try:
        return float(v) if v not in (None, "") else None
    except ValueError:
        return None


def parse_trade_rows(rows):
    """Typed trade rows from market_data.csv; drops rows without period/reporter."""
    out = []
    for r in rows:
        try:
            period = int(r["period"])
        except (KeyError, ValueError, TypeError):
            continue
        if not r.get("reporter_iso"):
            continue
        out.append({**r, "period": period, "value": _f(r.get("trade_value_usd")),
                    "weight": _f(r.get("net_weight_kg"))})
    return out


def refresh_from_api(ctx: Context, profile, res: StageResult):
    """Try to (re)collect data; on any failure keep prior data. Returns True if refreshed."""
    if ctx.offline:
        res.warn("Offline mode: trade API not called.")
        return False
    if not config.has_comtrade_key():
        res.warn("UN_COMTRADE_API_KEY not set: trade API not called.")
        return False
    codes = [f["hs_code"] for f in profile["product_forms"] if f["hs_code"]]
    years = ctx.options.get("years") or profile["years_to_collect"]
    if not codes or not years:
        res.warn("No HS codes or years in profile: trade API not called.")
        return False
    existing = read_csv(ctx.paths.market_data)
    new_rows, ok_years = [], []
    for year in years:
        try:
            new_rows.extend(comtrade_client.collect_year(year, codes, profile["product_name"]))
            ok_years.append(str(year))
        except Exception as exc:  # network, API, key errors: record, keep old data for that year
            msg = f"Comtrade collection failed for {year}: {exc}"
            res.warn(msg)
            res.errors.append(msg)
            ctx.log("market", "api_error", year=year, error=str(exc))
    if not new_rows:
        return False
    kept = [r for r in existing if r.get("period") not in ok_years]
    write_csv(ctx.paths.market_data, kept + new_rows, comtrade_client.CSV_FIELDS, ctx.paths, ctx.now)
    return True


def _clip01(x):
    return max(0.0, min(1.0, x))


def analyze(trade_rows, profile, now, years=None):
    """Return market_analysis rows (sorted by rank). Pure function."""
    targets = {m["iso3"] for m in profile["target_markets"]}
    market_type = {m["iso3"]: m.get("market_type", "") for m in profile["target_markets"]}
    imports = [r for r in trade_rows if r["trade_flow"] == "Import"
               and r.get("partner_iso") in ("W00", "", None)]
    if not imports:
        imports = [r for r in trade_rows if r["trade_flow"] == "Import"]
    exports = [r for r in trade_rows if r["trade_flow"] == "Export" and r["reporter_iso"] == "IND"]

    all_years = sorted({r["period"] for r in imports})
    if years:
        all_years = [y for y in all_years if y in years]
    # drop a latest year that most countries have not reported yet
    if len(all_years) >= 2:
        last, prev = all_years[-1], all_years[-2]
        n_last = len({r["reporter_iso"] for r in imports if r["period"] == last})
        n_prev = len({r["reporter_iso"] for r in imports if r["period"] == prev})
        if n_prev and n_last < 0.6 * n_prev:
            all_years = all_years[:-1]
    imports = [r for r in imports if r["period"] in all_years]

    by = defaultdict(lambda: defaultdict(lambda: {"v": 0.0, "w": 0.0, "src": set(), "ret": set(),
                                                  "name": "", "hs": set()}))
    for r in imports:
        if r["value"] is None:
            continue
        cell = by[r["reporter_iso"]][r["period"]]
        cell["v"] += r["value"]
        cell["w"] += r["weight"] or 0.0
        cell["src"].add(r.get("source_url_or_id") or "")
        cell["ret"].add(r.get("retrieved_at") or "")
        cell["name"] = r.get("reporter_country") or r["reporter_iso"]
        cell["hs"].add(r["hs_code"])
    india = defaultdict(lambda: defaultdict(float))
    for r in exports:
        if r["value"] is not None:
            india[r["partner_iso"]][r["period"]] += r["value"]

    rows = []
    avg_values = {}
    for iso3, yrs in by.items():
        vals = {y: c["v"] for y, c in yrs.items()}
        avg_values[iso3] = sum(vals.values()) / len(vals)
    ordered = sorted(avg_values.values())

    for iso3, yrs in by.items():
        ys = sorted(yrs)
        first, latest = ys[0], ys[-1]
        v_first, v_latest = yrs[first]["v"], yrs[latest]["v"]
        growth = ((v_latest / v_first - 1) * 100) if len(ys) >= 2 and v_first else None
        if growth is None:
            trend = "insufficient_data"
        elif growth > 10:
            trend = "growing"
        elif growth < -10:
            trend = "declining"
        else:
            trend = "stable"
        ind_latest = india.get(iso3, {}).get(latest)
        share = (ind_latest / v_latest * 100) if (ind_latest is not None and v_latest) else None
        if share is not None:
            presence = "present" if ind_latest > 0 else "absent_in_reported_data"
        else:
            presence = "unknown"  # India export data missing is NOT absence of India
        size_pct = sum(1 for v in ordered if v <= avg_values[iso3]) / len(ordered)
        trend_pts = 0.0 if growth is None else _clip01((growth + 20) / 60) * 20
        india_pts = 0.0 if share is None else _clip01(share / 20) * 30
        score = round(size_pct * 50 + trend_pts + india_pts, 1)
        retrieved = sorted(x for c in yrs.values() for x in c["ret"] if x)
        oldest = retrieved[0] if retrieved else ""
        d = age_days(oldest, now)
        rows.append({
            "country": yrs[latest]["name"], "iso3": iso3,
            "is_target_market": "yes" if iso3 in targets else "no",
            "exporting_country": "World (all partners)", "hs_codes": ";".join(sorted(yrs[latest]["hs"])),
            "trade_flow": "Import", "years_reported": len(ys), "first_year": first, "latest_year": latest,
            "latest_import_value_usd": round(v_latest), "latest_import_quantity_kg": round(yrs[latest]["w"]) or "",
            "avg_import_value_usd": round(avg_values[iso3]),
            "growth_pct_first_to_latest": round(growth, 1) if growth is not None else "",
            "trend": trend,
            "import_history_usd": ";".join(f"{y}:{round(yrs[y]['v'])}" for y in ys),
            "india_export_latest_usd": round(ind_latest) if ind_latest is not None else "",
            "india_share_latest_pct": round(share, 1) if share is not None else "",
            "india_presence": presence, "market_relevance_score": score,
            "relevance_explanation": (f"size {size_pct*50:.0f}/50, trend {trend_pts:.0f}/20, "
                                      f"India presence {india_pts:.0f}/30"
                                      + (f"; profile market type: {market_type[iso3]}" if iso3 in market_type else "")),
            "data_source": ";".join(sorted({s for c in yrs.values() for s in c["src"] if s}))[:200],
            "retrieved_at": oldest, "data_age_days": d if d is not None else "",
            "freshness_status": ("unknown" if d is None else
                                 "fresh" if d <= MAX_AGE_DAYS_BEFORE_REFRESH else "stale"),
        })
    rows.sort(key=lambda r: (-r["market_relevance_score"], r["country"]))
    for i, r in enumerate(rows, 1):
        r["market_rank"] = i
    return rows


def build_shortlist(rows):
    return [r for r in rows if r["market_rank"] <= SHORTLIST_SIZE or r["is_target_market"] == "yes"]


def run(ctx: Context) -> StageResult:
    res = StageResult("market")
    profile = load_profile(ctx)
    refreshed = False
    existing = read_csv(ctx.paths.market_data)
    oldest = min((r.get("retrieved_at") or "" for r in existing if r.get("retrieved_at")), default="")
    if ctx.options.get("refresh_market") or not existing or (
            (age_days(oldest, ctx.now) or 0) > MAX_AGE_DAYS_BEFORE_REFRESH):
        refreshed = refresh_from_api(ctx, profile, res)
    trade = parse_trade_rows(read_csv(ctx.paths.market_data))
    if not trade:
        res.status = STATUS_WARNINGS
        res.counts = {"trade_rows": 0, "data_available": False}
        res.warn("Trade data unavailable: no data/processed/market_data.csv rows and the API was not "
                 "usable. No market statistics were produced or invented.")
        ctx.log("market", "unavailable")
        return res
    rows = analyze(trade, profile, ctx.now)
    short = build_shortlist(rows)
    res.outputs += [str(write_csv(ctx.paths.market_analysis, rows, MARKET_ANALYSIS_FIELDS, ctx.paths, ctx.now)),
                    str(write_csv(ctx.paths.market_shortlist, short, MARKET_ANALYSIS_FIELDS, ctx.paths, ctx.now))]
    found = {r["iso3"] for r in rows}
    for m in profile["target_markets"]:
        if m["iso3"] not in found:
            res.warn(f"No import data for target market {m['country']} in the available trade data.")
    stale = [r for r in rows if r["is_target_market"] == "yes" and r["freshness_status"] != "fresh"]
    if stale and not refreshed:
        res.warn("Trade data not refreshed this run; target-market statistics carry their original "
                 f"retrieval date (oldest: {min(r['retrieved_at'] for r in stale)}).")
    res.counts = {"trade_rows": len(trade), "countries": len(rows), "shortlisted": len(short),
                  "data_available": True, "refreshed_this_run": refreshed}
    res.status = STATUS_WARNINGS if res.warnings else STATUS_COMPLETED
    return res
