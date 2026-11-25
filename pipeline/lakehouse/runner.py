"""The runner: executes the plan derived from the contracts and guarantees a report.

Why a runner (and not a script per table): the runner holds the ORDER and the FAILURE POLICY; each stage script holds one
kind of work. Adding a table means adding contracts (and for gold a transform SQL), not editing the runner.
Failure policy: stop at the first hard failure, skip what depended on it, ALWAYS write the report, then exit non-zero.
"""
import argparse
import dataclasses
import json
import os
import sys
from datetime import datetime

from lakehouse import plan as planner
from lakehouse.cli import add_common_args
from lakehouse.context import make_context
from lakehouse.contract_store import load_contracts, load_definitions
from lakehouse.governance import ensure_objects, log_step, publish_contracts
from lakehouse.stages import bronze, gold, report, semantic, silver

RUNNERS = {
    "bronze": lambda ctx, s: bronze.run(ctx, s.entity),
    "silver": lambda ctx, s: silver.run(ctx, s.entity),
    "gold": lambda ctx, s: gold.build(ctx, s.entity),
    "gold-publish": lambda ctx, s: gold.publish(ctx),
    "semantic": lambda ctx, s: semantic.run(ctx, s.entity),
}


def parse_args(argv):
    p = add_common_args(argparse.ArgumentParser(description=__doc__))
    p.add_argument("--code-dir", help="folder holding lakehouse.zip (used by run_pipeline.py before this module is imported)")
    p.add_argument("--failure-policy", choices=["stop", "isolate"], default="stop",
                   help="stop: after the first failed step skip everything (default). isolate: skip only steps that depend on a failed one")
    p.add_argument("--stages", default="", help="comma list of bronze,silver,gold,semantic (default or 'all': every stage)")
    p.add_argument("--entities", default="", help="comma list of entities to limit the run to (default or 'all': every entity)")
    p.add_argument("--steps", default="", help="explicit steps such as silver:team,gold-publish (overrides --stages and --entities)")
    p.add_argument("--plan-only", nargs="?", const="true", default="false",
                   help="print the execution plan and stop (no Spark, nothing runs). Accepts true/false so a Job parameter can drive it")
    return p.parse_args(argv)


def version_note(pipeline_root):
    """Which code and files this run is using, from the VERSION.json that tools/sync_to_volume.py writes last."""
    try:
        v = json.load(open(os.path.join(pipeline_root, "VERSION.json")))
        return f"code stamp {str(v['content_fingerprint'])[:12]} | commit {str(v.get('commit', 'unknown'))[:12]} | synced {v.get('synced_at', '?')}"
    except Exception:  # noqa: BLE001
        return "no VERSION.json: the files were uploaded by hand, not synced from Git"


def blocked(key, deps, bad, policy, startup_failed, any_failed):
    """Should this step be skipped?  policy 'stop': after the first failure everything is skipped (the original behaviour).
    policy 'isolate': only steps that depend, directly or through other skipped steps, on something that failed. A failure in setup or
    contracts always skips everything, because nothing can be trusted without them."""
    if startup_failed:
        return True
    if policy == "stop":
        return any_failed
    return any(d in bad for d in deps.get(key, []))


def resolve_products(value, cs):
    """`--product fbl`, `--product fbl,tix` or `--product all` (every product that has a source contract, that is, can actually run)."""
    known = sorted({c.get("dataProduct") for c in cs.contracts if c.get("dataProduct")})
    runnable = sorted({c.get("dataProduct") for c in cs.layer("source")})
    v = (value or "fbl").strip()
    chosen = runnable if v.lower() == "all" else [x.strip() for x in v.split(",") if x.strip()]
    bad = [x for x in chosen if x not in known]
    if bad or not chosen:
        raise SystemExit(f"unknown product(s) {bad or v!r}. Products in these contracts: {', '.join(known)}")
    return chosen


def step_ctx(ctx, args, products, step):
    """The context a step runs with: same session and contracts, but THIS step's product and its own landing folder.
    Why a copy per step: stages read ctx.product to find their contract, so one run can cover several products safely."""
    product = step.product or products[0]
    landing = args.landing_dir if (args.landing_dir and len(products) == 1) else f"{ctx.data_root}/{product}"
    return dataclasses.replace(ctx, product=product, landing_dir=landing)


def _csv(value):
    """Job parameters cannot be 'unset', so 'all' (or '-') means 'no filter'."""
    v = (value or "").strip()
    return "" if v.lower() in ("", "all", "-", "none") else v


def select_steps(ordered, args):
    steps_arg, stages_arg, entities_arg = _csv(args.steps), _csv(args.stages), _csv(args.entities)
    if steps_arg:
        wanted = {s.strip() for s in steps_arg.split(",") if s.strip()}
        chosen = [s for s in ordered if s.key in wanted]
        unknown = wanted - {s.key for s in chosen}
        if unknown:
            raise SystemExit(f"unknown step(s): {', '.join(sorted(unknown))}. Known: {', '.join(s.key for s in ordered)}")
        return chosen
    stages = {s.strip() for s in stages_arg.split(",") if s.strip()}
    entities = {s.strip() for s in entities_arg.split(",") if s.strip()}
    out = []
    for s in ordered:
        kind = "gold" if s.kind == "gold-publish" else s.kind
        if stages and kind not in stages:
            continue
        if entities and s.entity and s.entity not in entities:
            continue
        out.append(s)
    return out


def print_plan(ctx, ordered, deps, chosen):
    ctx.log(f"PLAN for product(s) '{', '.join(sorted({s.product or ctx.product for s in ordered}))}' (contracts from {ctx.contracts_dir})")
    ctx.log("  0. setup               create schemas, governance tables and views")
    ctx.log("  1. contracts           lint every contract, record the versions, compile the rules")
    chosen_keys = {s.key for s in chosen}
    for i, s in enumerate(ordered, 2):
        mark = "" if s.key in chosen_keys else "   (not selected)"
        d = f" after {', '.join(deps[s.key])}" if deps[s.key] else ""
        ctx.log(f"  {i}. {s.key:<24}{d}{mark}")
        ctx.log(f"       {planner.explain(ctx.contracts, s, s.product or ctx.product)}")
    ctx.log(f"  {len(ordered) + 2}. report              summarise the run; fail it if rows were rejected or a contract is broken")


def main(argv=None, spark=None, semantic_executor_factory=None):
    args = parse_args(argv)
    ctx = make_context(args, spark)
    ctx.semantic_executor_factory = semantic_executor_factory
    ctx.contracts = load_contracts(ctx.contracts_dir)
    ctx.definitions = load_definitions(ctx.definitions_dir)
    products = resolve_products(args.product, ctx.contracts)
    ctx.product = products[0]
    ordered, deps = planner.build_multi_plan(ctx.contracts, products)
    chosen = select_steps(ordered, args)
    if str(args.plan_only).lower() == "true":
        print_plan(ctx, ordered, deps, chosen)
        return 0

    if ctx.spark is None:
        from pyspark.sql import SparkSession
        ctx.spark = SparkSession.builder.getOrCreate()
    ctx.use_catalog()
    ctx.log(f"run {ctx.run_id} | catalog {ctx.catalog} | {len(chosen)} of {len(ordered)} steps selected")

    failure, failed_step, logging_ready = None, None, False
    bad = set()                                       # steps that failed or were skipped (used by --failure-policy isolate)

    def execute(name, fn):
        nonlocal failure, failed_step, logging_ready
        started = datetime.now()
        ctx.log(f"[{name}] start")
        try:
            fn()
            if name == "setup":
                logging_ready = True          # the step log table exists from here on
            note = version_note(ctx.pipeline_root) if name == "setup" else ""
            if logging_ready:
                log_step(ctx, name, "ok", started, note)
            ctx.log(f"[{name}] ok" + (f" ({note})" if note else ""))
        except Exception as e:  # noqa: BLE001
            failure, failed_step = e, name
            bad.add(name)
            ctx.log(f"[{name}] FAILED: {str(e)[:300]}")
            if logging_ready:
                log_step(ctx, name, "failed", started, f"{type(e).__name__}: {e}")

    execute("setup", lambda: ensure_objects(ctx))
    if failure is None:
        execute("contracts", lambda: publish_contracts(ctx))
    startup_failed = failure is not None
    for s in chosen:
        if blocked(s.key, deps, bad, args.failure_policy, startup_failed, failure is not None):
            bad.add(s.key)
            if logging_ready:
                why = failed_step if args.failure_policy == "stop" or startup_failed else ", ".join(d for d in deps.get(s.key, []) if d in bad)
                log_step(ctx, s.key, "skipped", datetime.now(), f"skipped because {why} failed")
            continue
        execute(s.key, lambda s=s: RUNNERS[s.kind](step_ctx(ctx, args, products, s), s))
    if logging_ready:
        try:
            ctx.log("[report] start")
            failure = report.run(ctx, failure)
        except Exception as e:  # noqa: BLE001
            ctx.log(f"[report] FAILED: {str(e)[:300]}")
            failure = failure or e
    if failure is not None:
        ctx.log(f"PIPELINE FAILED ({type(failure).__name__}): {str(failure)[:400]}")
        return 1
    ctx.log("PIPELINE OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
