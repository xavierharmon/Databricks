"""SEMANTIC: create one metric view from its definition and test it against its semantic contract.

Why test the view, not just create it: a metric view can change its meaning while every column name stays the same.
The contract's interface check, reconcile/expect/bounds tests and freshness promise are what notice.
"""
import argparse
import sys

from lakehouse.errors import StageFailure
from lib.contract_checks import make_executor, raise_if_broken, semantic_checks, write_results
from lib.contracts import cprops, main_object, render


def run(ctx, view):
    cs = ctx.contracts
    c = next((x for x in cs.layer("semantic", ctx.product) if x["id"].endswith("." + view)), None)
    if c is None:
        raise StageFailure(f"no semantic contract for view {view!r}")
    obj = main_object(c)
    view_fqn = f"{ctx.catalog}.{obj['physicalName']}"
    if not ctx.local_mode:
        import os
        name = os.path.basename(cprops(obj)["definition"])
        sql = ctx.definitions.get(name)
        if not sql:
            raise StageFailure(f"metric view definition {name} not found in {ctx.definitions_dir}")
        ctx.spark.sql(render(sql, ctx.catalog).rstrip().rstrip(";"))
        ctx.log(f"  created {view_fqn}")
    executor = ctx.semantic_executor_factory(c, view_fqn) if ctx.semantic_executor_factory else make_executor(ctx.spark, view_fqn)
    rows = semantic_checks(ctx.spark, ctx.run_id, c, executor)
    write_results(ctx.spark, rows)
    bad = [r for r in rows if r[7] == "error" and not r[8]]
    ctx.log(f"  {c['id']} v{c['version']}: {len(rows)} checks, {len(bad)} broken")
    raise_if_broken(rows)


def main(argv=None, spark=None):
    from lakehouse.cli import add_common_args
    from lakehouse.contract_store import load_contracts, load_definitions
    from lakehouse.context import make_context
    p = add_common_args(argparse.ArgumentParser(description=__doc__))
    p.add_argument("--view", required=True)
    args = p.parse_args(argv)
    from pyspark.sql import SparkSession
    ctx = make_context(args, spark or SparkSession.builder.getOrCreate())
    ctx.contracts = load_contracts(ctx.contracts_dir)
    ctx.definitions = load_definitions(ctx.definitions_dir)
    ctx.use_catalog()
    run(ctx, args.view)


if __name__ == "__main__":
    sys.exit(main())
