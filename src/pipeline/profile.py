"""STAGE 1 - Product profile.

Input : data/product_profile.json (existing pilot file, left untouched) or CLI/dict input.
Output: data/processed/product_profile.json  (normalized; HS verification status preserved).
An HS code is only 'verified' if the input says so AND names a verification source;
otherwise it stays 'needs_verification'. The code never verifies a code on its own.
"""

from __future__ import annotations

import re

from src.pipeline import countries
from src.pipeline.common import (Context, StageResult, STATUS_COMPLETED, STATUS_FAILED,
                                 STATUS_WARNINGS, fold, iso, read_json, stable_id, write_json)

SCHEMA_VERSION = "1.0"
# Strong terms identify the product; weak trade-slang terms are kept but never used to match text.
WEAK_TERMS = {"bold", "java"}


class ProfileError(ValueError):
    pass


def _hs(code):
    digits = re.sub(r"\D", "", str(code or ""))
    return digits


def _form_from_input(f, warnings):
    code = _hs(f.get("candidate_hs_code") or f.get("hs_code"))
    if code and len(code) not in (4, 6, 8, 10):
        raise ProfileError(f"Invalid HS code '{f.get('candidate_hs_code') or f.get('hs_code')}'")
    status = f.get("verification_status") or "needs_verification"
    source = f.get("verification_source") or ""
    if status == "verified" and not source:
        warnings.append(f"HS {code}: input claims 'verified' without a verification_source; "
                        "reset to needs_verification.")
        status = "needs_verification"
    if status not in ("verified", "needs_verification"):
        status = "needs_verification"
    return {
        "form": (f.get("form") or "").strip(),
        "hs_code": code,
        "description": (f.get("description") or "").strip(),
        "verification_status": status,
        "verification_source": source,
    }


def normalize_profile(raw: dict, now) -> dict:
    """Normalize a raw profile dict (existing pilot schema or the minimal CLI schema)."""
    warnings = []
    name = (raw.get("product_name") or "").strip()
    if not name:
        raise ProfileError("product_name is required")

    origin_raw = raw.get("exporting_country") or raw.get("origin") or ""
    origin_iso = countries.to_iso3(raw.get("exporting_country_iso3") or origin_raw)
    if not origin_iso:
        raise ProfileError(f"Unrecognised product origin '{origin_raw}'")

    forms_in = raw.get("product_forms")
    if forms_in is None:
        forms_in = [{"form": raw.get("product_form", ""), "hs_code": raw.get("hs_code", "")}]
    forms = [_form_from_input(f, warnings) for f in forms_in]
    for f in forms:
        if f["verification_status"] != "verified":
            warnings.append(f"HS {f['hs_code'] or '(none)'} ({f['form'] or 'form n/a'}) is unverified: "
                            "confirm against the importing country's tariff schedule.")

    broad = _hs(raw.get("broad_hs_code_for_initial_market_scan") or raw.get("broad_hs_heading"))
    if not broad:
        codes = [f["hs_code"] for f in forms if f["hs_code"]]
        broad = codes[0][:4] if codes else ""
    if not broad and not any(f["hs_code"] for f in forms):
        warnings.append("No HS code supplied: trade analysis cannot run.")

    markets_in = raw.get("target_markets") or [{"country": c} for c in raw.get("target_countries", [])]
    markets, seen = [], set()
    for m in markets_in:
        m = m if isinstance(m, dict) else {"country": m}
        iso3 = countries.to_iso3(m.get("iso3") or m.get("country"))
        if not iso3:
            raise ProfileError(f"Unrecognised target country '{m.get('country')}'")
        if iso3 in seen:
            continue
        seen.add(iso3)
        code = m.get("comtrade_code") or countries.comtrade_code(iso3)
        if code is None:
            warnings.append(f"No Comtrade code for {iso3}; market statistics unavailable for it.")
        markets.append({
            "country": m.get("country") or countries.country_name(iso3),
            "iso3": iso3, "comtrade_code": code,
            "market_type": m.get("market_type") or "unclassified",
            "reason": m.get("reason") or "",
        })
    if not markets:
        raise ProfileError("At least one target country is required")

    synonyms = []
    for s in [name, *raw.get("synonyms", [])]:
        s = fold(s)
        if s and s not in synonyms:
            synonyms.append(s)

    profile = {
        "schema_version": SCHEMA_VERSION,
        "normalized_at": iso(now),
        "product_name": name,
        "product_name_normalized": fold(name),
        "origin_country": countries.country_name(origin_iso) or origin_raw,
        "origin_iso3": origin_iso,
        "broad_hs_heading": broad,
        "hs_nomenclature": raw.get("hs_nomenclature") or "",
        "product_forms": forms,
        "hs_verification_summary": (
            "all_verified" if forms and all(f["verification_status"] == "verified" for f in forms)
            else "unverified"),
        "excluded_forms": raw.get("excluded_forms", []),
        "synonyms": synonyms,
        "strong_terms": [s for s in synonyms if s not in WEAK_TERMS],
        "weak_terms": [s for s in synonyms if s in WEAK_TERMS],
        "target_markets": markets,
        "specifications": raw.get("specifications") or {},
        "trade_flow_of_interest": raw.get("trade_flow_of_interest") or "imports",
        "years_to_collect": raw.get("years_to_collect") or [],
        "notes": raw.get("notes") or "",
        "warnings": warnings,
    }
    profile["profile_id"] = stable_id(
        "prof", name, origin_iso, ",".join(sorted(f["hs_code"] for f in forms)),
        ",".join(sorted(m["iso3"] for m in markets)))
    return profile


def run(ctx: Context) -> StageResult:
    res = StageResult("profile")
    raw = ctx.options.get("profile_input")
    source = "cli/dict input"
    if raw is None:
        raw = read_json(ctx.paths.input_profile)
        source = str(ctx.paths.input_profile)
    if raw is None:
        res.status = STATUS_FAILED
        res.errors.append(f"No product profile input found at {ctx.paths.input_profile}")
        return res
    try:
        profile = normalize_profile(raw, ctx.now)
    except ProfileError as exc:
        res.status = STATUS_FAILED
        res.errors.append(str(exc))
        return res
    profile["source_file"] = source
    out = write_json(ctx.paths.product_profile, profile, ctx.paths, ctx.now)
    res.outputs.append(str(out))
    res.counts = {"forms": len(profile["product_forms"]), "target_markets": len(profile["target_markets"]),
                  "unverified_hs_codes": sum(f["verification_status"] != "verified" for f in profile["product_forms"])}
    res.warnings.extend(profile["warnings"])
    res.status = STATUS_WARNINGS if res.warnings else STATUS_COMPLETED
    ctx.log("profile", "written", profile_id=profile["profile_id"])
    return res


def load(ctx: Context) -> dict:
    prof = read_json(ctx.paths.product_profile)
    if prof is None:
        raise FileNotFoundError("product_profile.json missing: run stage 'profile' first")
    return prof
