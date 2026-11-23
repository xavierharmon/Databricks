"""GOLD: build and audit into a staging schema, then publish. (Write - Audit - Publish.)

Why not write straight into gold: if an assertion fails AFTER the table was overwritten, consumers can read bad data until
someone fixes it. Here every gold table is built in `gold_stage`, audited there, and only copied into `gold` by the separate
`gold-publish` step when every audit has passed. A failed audit leaves the published tables exactly as they were.
The transform for each table is a SQL file (default transforms/gold/<entity>.sql, or the contract's `transformSql`):
the contract points at the code, and the code never needs to know about the contract's rules.
"""
import argparse
import os
import sys
from datetime import datetime

from lakehouse.errors import StageFailure
from lakehouse.governance import append, stats_df
from lib.contract_checks import raise_if_broken, schema_conformance, write_results
from lib.contracts import cprops, main_object
from lib.dq_engine import RESULTS_DDL, apply_rules, load_rules, raise_if_failed


def stage_table(physical: str) -> str:
    schema, rest = physical.split(".", 1)
    return f"{schema}_stage.{rest}"


def _transform_sql(ctx, gc):
    obj = main_object(gc)
    rel = cprops(gc).get("transformSql") or cprops(obj).get("transformSql") or f"gold/{cprops(obj)['entity']}.sql"
    path = os.path.join(ctx.transforms_dir, rel)
    if not os.path.exists(path):
        raise StageFailure(f"transform SQL not found: {path}. Gold contracts need a SQL file (default transforms/gold/<entity>.sql).")
    with open(path) as fh:
        return fh.read().strip().rstrip(";")


def build(ctx, entity):
    """Build one gold table into the staging schema and audit it. Does NOT touch the published table."""
    gc = ctx.contracts.entity("gold", entity, ctx.product)
    if gc is None:
        raise StageFailure(f"no gold contract for entity {entity!r}")
    spark = ctx.spark
    physical = main_object(gc)["physicalName"]
    stage = stage_table(physical)
    spark.sql(_transform_sql(ctx, gc)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(stage)
    built = spark.table(stage)

    rules = load_rules(spark, "gold", entity)
    pub_schema, stg_schema = physical.split(".")[0], stage.split(".")[0]
    for r in rules:   # the audit must look at the STAGED parents, not the published ones
        if r["rule_type"] == "referential":
            r["expression"] = r["expression"].replace(f"{pub_schema}.", f"{stg_schema}.", 1)
            parent = r["expression"].rsplit(".", 1)[0]
            if not spark.catalog.tableExists(parent):
                raise StageFailure(f"{r['rule_id']} needs {parent}: build that gold table first")
    _, _, res, _stats = apply_rules(spark, built, rules, ctx.run_id, "gold", entity, mode="assert")

    recon_table = cprops(main_object(gc)).get("reconcilesTo")
    silver_n, gold_n = spark.table(recon_table).count(), built.count()
    rec = spark.createDataFrame([(ctx.run_id, datetime.now(), "gold", entity, "GLD-RECON", "reconcile", "fail", abs(silver_n - gold_n),
                                  gold_n, f"Row count gold = silver ({silver_n} vs {gold_n})")], RESULTS_DDL)
    res = res.unionByName(rec)
    append(res, "governance.dq_rule_results")
    rows = schema_conformance(spark, ctx.run_id, gc, table=stage)
    write_results(spark, rows)
    ctx.log(f"  {stage}: {gold_n} rows audited  [contract {gc['id']} v{gc['version']}]")
    raise_if_failed(res)
    raise_if_broken(rows)


def publish(ctx):
    """Copy every audited staging table into gold. Reached only if all gold audits in this run passed."""
    spark = ctx.spark
    rr = spark.table("governance.dq_rule_results")
    audited = {r.table_name for r in rr.where((rr.run_id == ctx.run_id) & (rr.layer == "gold")).select("table_name").distinct().collect()}
    if not audited:
        raise StageFailure("nothing to publish: no gold audit was recorded for this run")
    for gc in ctx.contracts.layer("gold", ctx.product):
        entity = cprops(main_object(gc))["entity"]
        if entity not in audited:
            continue
        physical = main_object(gc)["physicalName"]
        df = spark.table(stage_table(physical))
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(physical)
        n = spark.table(physical).count()
        append(stats_df(spark, dict(run_id=ctx.run_id, layer="gold", table_name=entity, rows_in=n, rows_deduped=0,
                                    rows_standardized=0, rows_warned=0, rows_rejected=0, rows_out=n)), "governance.dq_table_stats")
        ctx.log(f"  published {physical}: {n} rows")


def main(argv=None, spark=None):
    from lakehouse.cli import add_common_args
    from lakehouse.contract_store import load_contracts
    from lakehouse.context import make_context
    p = add_common_args(argparse.ArgumentParser(description=__doc__))
    p.add_argument("--entity", help="build and audit this gold entity (omit with --publish)")
    p.add_argument("--publish", action="store_true", help="publish the audited staging tables")
    args = p.parse_args(argv)
    from pyspark.sql import SparkSession
    ctx = make_context(args, spark or SparkSession.builder.getOrCreate())
    ctx.contracts = load_contracts(ctx.contracts_dir)
    ctx.use_catalog()
    publish(ctx) if args.publish else build(ctx, args.entity)


if __name__ == "__main__":
    sys.exit(main())
