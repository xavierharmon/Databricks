"""The runner: executes the plan derived from the contracts and guarantees a report.

Why a runner (and not a script per table): the runner holds the ORDER and the FAILURE POLICY; each stage script holds one
kind of work. Adding a table means adding contracts (and for gold a transform SQL), not editing the runner.
Failure policy: stop at the first hard failure, skip what depended on it, ALWAYS write the report, then exit non-zero.
"""
import argparse
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
    p.add_argument("--stages", default="", help="comma list of bronze,silver,gold,semantic (default or 'all': every stage)")
    p.add_argument("--entities", default="", help="comma list of entities to limit the run to (default or 'all': every entity)")
    p.add_argument("--steps", default="", help="explicit steps such as silver:team,gold-publish (overrides --stages and --entities)")
    p.add_argument("--plan-only", nargs="?", const="true", default="false",
                   help="print the execution plan and stop (no Spark, nothing runs). Accepts true/false so a Job parameter can drive it")
    return p.parse_args(argv)


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
    ctx.log(f"PLAN for product '{ctx.product}' (contracts from {ctx.contracts_dir})")
    ctx.log("  0. setup               create schemas, governance tables and views")
    ctx.log("  1. contracts           lint every contract, record the versions, compile the rules")
    chosen_keys = {s.key for s in chosen}
    for i, s in enumerate(ordered, 2):
        mark = "" if s.key in chosen_keys else "   (not selected)"
        d = f" after {', '.join(deps[s.key])}" if deps[s.key] else ""
        ctx.log(f"  {i}. {s.key:<24}{d}{mark}")
        ctx.log(f"       {planner.explain(ctx.contracts, s, ctx.product)}")
    ctx.log(f"  {len(ordered) + 2}. report              summarise the run; fail it if rows were rejected or a contract is broken")


def main(argv=None, spark=None, semantic_executor_factory=None):
    args = parse_args(argv)
    ctx = make_context(args, spark)
    ctx.semantic_executor_factory = semantic_executor_factory
    ctx.contracts = load_contracts(ctx.contracts_dir)
    ctx.definitions = load_definitions(ctx.definitions_dir)
    ordered, deps = planner.build_plan(ctx.contracts, ctx.product)
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

    def execute(name, fn):
        nonlocal failure, failed_step, logging_ready
        started = datetime.now()
        ctx.log(f"[{name}] start")
        try:
            fn()
            if name == "setup":
                logging_ready = True          # the step log table exists from here on
            if logging_ready:
                log_step(ctx, name, "ok", started)
            ctx.log(f"[{name}] ok")
        except Exception as e:  # noqa: BLE001
            failure, failed_step = e, name
            ctx.log(f"[{name}] FAILED: {str(e)[:300]}")
            if logging_ready:
                log_step(ctx, name, "failed", started, f"{type(e).__name__}: {e}")

    execute("setup", lambda: ensure_objects(ctx))
    if failure is None:
        execute("contracts", lambda: publish_contracts(ctx))
    for s in chosen:
        if failure is not None:
            if logging_ready:
                log_step(ctx, s.key, "skipped", datetime.now(), f"skipped because {failed_step} failed")
            continue
        execute(s.key, lambda s=s: RUNNERS[s.kind](ctx, s))
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
