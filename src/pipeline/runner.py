"""Pipeline orchestrator: runs stages in order, isolates failures, writes the run summary."""

from __future__ import annotations

import argparse
import sys
import traceback
from collections import Counter

from src.pipeline import (audit as audit_mod, claims, discovery, export, extraction, market, profile,
                          qualification, resolution, scoring)
from src.pipeline.common import (Context, Paths, StageResult, STATUS_FAILED, STATUS_SKIPPED, iso, new_run_id,
                                 read_csv, read_json, utcnow, write_json)

STAGES = [("profile", profile), ("market", market), ("discovery", discovery), ("extraction", extraction),
          ("resolution", resolution), ("claims", claims), ("qualification", qualification),
          ("scoring", scoring), ("export", export)]
STAGE_NAMES = [n for n, _ in STAGES]


def run_stages(ctx: Context, selected=None):
    ctx.paths.ensure()
    started = ctx.now
    results, abort = [], False
    for name, mod in STAGES:
        if selected is not None and name not in selected:
            continue
        if abort:
            r = StageResult(name, STATUS_SKIPPED)
            r.warn("skipped: the product profile stage failed")
            results.append(r)
            continue
        r = StageResult(name, started_at=iso(utcnow()))
        ctx.log(name, "start")
        try:
            r = mod.run(ctx)
            r.started_at = r.started_at or iso(started)
        except Exception as exc:  # a stage crash must be recorded, never hidden
            r.status = STATUS_FAILED
            r.errors.append(f"{type(exc).__name__}: {exc}")
            ctx.log(name, "crash", error=str(exc), trace=traceback.format_exc()[-1500:])
        r.completed_at = iso(utcnow())
        ctx.log(name, "end", status=r.status, counts=r.counts, warnings=len(r.warnings), errors=len(r.errors))
        results.append(r)
        if name == "profile" and r.status == STATUS_FAILED:
            abort = True
    return results


def build_summary(ctx: Context, results, started_at):
    P = ctx.paths
    prof = read_json(P.product_profile) or {}
    quals = read_csv(P.buyer_qualification)
    cands = read_csv(P.candidates)
    cls = Counter(q["final_classification"] for q in quals)
    claims_rows = read_csv(P.company_claims)
    market_rows = read_csv(P.market_analysis)
    outputs = {}
    for r in results:
        for o in r.outputs:
            outputs[o] = True
    warnings = [f"[{r.stage}] {w}" for r in results for w in r.warnings]
    return {
        "run_id": ctx.run_id, "started_at": iso(started_at), "completed_at": iso(utcnow()),
        "product_profile_id": prof.get("profile_id", ""),
        "stages_completed": [r.stage for r in results if r.status in ("completed", "completed_with_warnings")],
        "stages_failed": [r.stage for r in results if r.status == STATUS_FAILED],
        "stages_skipped": [r.stage for r in results if r.status == STATUS_SKIPPED],
        "candidate_count": len(cands), "company_count": len(quals),
        "qualified_company_count": cls.get("potential_buyer", 0),
        "classification_counts": {k: cls.get(k, 0) for k in
                                  ("potential_buyer", "relevant_business", "weak_lead", "undetermined", "excluded")},
        "stage_results": [r.to_dict() for r in results], "output_locations": sorted(outputs),
        "warnings": warnings, "offline": ctx.offline,
        "source_coverage": {
            "candidates_by_source_type": dict(Counter(c["source_type"] for c in cands)),
            "candidates_by_discovery_method": dict(Counter(c["discovery_method"].split(":")[0] for c in cands)),
            "supported_claims_by_source_tier": dict(sorted(Counter(
                f"tier_{c['source_authority_tier']}" for c in claims_rows if c["claim_status"] == "supported").items())),
        },
        "data_freshness": {
            "trade_data_available": bool(market_rows),
            "trade_data_oldest_retrieval": min((m["retrieved_at"] for m in market_rows if m["retrieved_at"]), default=""),
            "product_profile_normalized_at": prof.get("normalized_at", ""),
        },
        "hs_verification": prof.get("hs_verification_summary", ""),
        "notes": ["Scores are lead-prioritization indicators, not purchase probabilities.",
                  "No claim is independently cross-verified; single-source evidence is labelled as such."],
    }


def execute(ctx: Context, selected=None):
    """Run stages, refresh the dashboard with run info, write run summaries. -> (results, summary)."""
    started = ctx.now
    results = run_stages(ctx, selected)
    summary = build_summary(ctx, results, started)
    if any(r.stage == "export" for r in results):
        ctx.options["run_info"] = {"run_id": ctx.run_id, "warnings": summary["warnings"],
                                   "stages_failed": summary["stages_failed"]}
        export.run(ctx)  # export ran before the summary existed; redo so the dashboard embeds run info
    write_json(ctx.paths.run_summary, summary, ctx.paths, started)
    if ctx.paths.exports.exists():
        write_json(ctx.paths.exports / "pipeline_run_summary.json", summary, ctx.paths, started)
    return results, summary


def main(argv=None):
    ap = argparse.ArgumentParser(prog="python -m src.pipeline", description="AVYUKT buyer-discovery pipeline")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("run", "audit"):
        sp = sub.add_parser(name)
        sp.add_argument("--data-dir", help="use a different data folder (demos/tests)")
        sp.add_argument("--offline", action="store_true", help="no network: use cached/ingested data only")
    run_p = sub.choices["run"]
    run_p.add_argument("--stage", choices=STAGE_NAMES + ["enrichment"], help="run a single stage")
    run_p.add_argument("--from-stage", choices=STAGE_NAMES, help="run this stage and everything after it")
    run_p.add_argument("--refresh-market", action="store_true")
    run_p.add_argument("--max-queries", type=int, default=30)
    run_p.add_argument("--max-sites", type=int, default=60)
    run_p.add_argument("--product"); run_p.add_argument("--form"); run_p.add_argument("--origin")
    run_p.add_argument("--hs"); run_p.add_argument("--countries", nargs="+")
    args = ap.parse_args(argv)

    now = utcnow()
    ctx = Context(paths=Paths(args.data_dir), run_id=new_run_id(now), now=now, offline=args.offline)
    if args.cmd == "audit":
        ctx.paths.ensure()
        return audit_mod.print_report(audit_mod.audit(ctx))

    ctx.options.update(max_queries=args.max_queries, max_sites=args.max_sites, refresh_market=args.refresh_market)
    if args.product:
        ctx.options["profile_input"] = {
            "product_name": args.product, "product_form": args.form or "", "origin": args.origin or "India",
            "hs_code": args.hs or "", "target_countries": args.countries or []}
    selected = None
    if args.stage:
        selected = {"extraction" if args.stage == "enrichment" else args.stage}
    elif args.from_stage:
        selected = set(STAGE_NAMES[STAGE_NAMES.index(args.from_stage):])
    results, summary = execute(ctx, selected)
    print(f"Run {ctx.run_id}")
    for r in results:
        print(f"  {r.stage:<14}{r.status:<26}{r.counts}")
    print(f"Candidates: {summary['candidate_count']}  Companies: {summary['company_count']}  "
          f"Potential buyers: {summary['qualified_company_count']}")
    for w in summary["warnings"][:15]:
        print("  warning:", w)
    return 1 if summary["stages_failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
