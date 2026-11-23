"""SILVER: one entity per call. Normalize (type, clean, standardize) and validate (apply the contract's rules).

Why normalize and validate together: the typing SELECT keeps the ORIGINAL text of each row (`_raw`), so a row rejected by a
rule can be returned to its owner exactly as it was sent. Doing the two in one step is what makes that possible.
Rules never delete data: rejected rows are written to governance.dq_quarantine with the reason.
Run on its own:  python -m lakehouse.stages.silver --entity team --catalog ... --pipeline-root ... --data-root ...
"""
import argparse
import sys

from lakehouse.errors import StageFailure
from lakehouse.governance import append, stats_df
from lib.contract_checks import raise_if_broken, schema_conformance, write_results
from lib.contracts import main_object, typed_select
from lib.dq_engine import apply_rules, load_rules, raise_if_failed


def run(ctx, entity):
    cs = ctx.contracts
    sv, src = cs.entity("silver", entity, ctx.product), cs.entity("source", entity, ctx.product)
    if sv is None or src is None:
        raise StageFailure(f"silver:{entity} needs both a silver and a source contract for entity {entity!r}")
    spark = ctx.spark
    bronze = main_object(src)["physicalName"]
    if not spark.catalog.tableExists(bronze):
        raise StageFailure(f"{bronze} does not exist: run bronze:{entity} first")
    typed = spark.sql(typed_select(sv, src))              # SQL generated from the contract, not hand-written
    rules = load_rules(spark, "silver", entity)           # compiled from the contracts by the 'contracts' step
    for r in rules:                                       # a referential rule reads the PARENT table: it must exist already
        if r["rule_type"] == "referential":
            parent = r["expression"].rsplit(".", 1)[0]
            if not spark.catalog.tableExists(parent):
                raise StageFailure(f"{r['rule_id']} checks references against {parent}, which does not exist: run its silver step first")
    clean, log_df, res, stats = apply_rules(spark, typed, rules, ctx.run_id, "silver", entity, mode="filter")
    target = main_object(sv)["physicalName"]
    clean.drop("_raw").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    append(log_df, "governance.dq_quarantine")
    append(res, "governance.dq_rule_results")
    append(stats_df(spark, stats), "governance.dq_table_stats")
    rows = schema_conformance(spark, ctx.run_id, sv)
    write_results(spark, rows)
    ctx.log(f"  {target}: in={stats['rows_in']} deduped={stats['rows_deduped']} rejected={stats['rows_rejected']} "
            f"out={stats['rows_out']}  [contract {sv['id']} v{sv['version']}]")
    raise_if_failed(res)          # only `fail` rules stop the step; rejected rows do not
    raise_if_broken(rows)


def main(argv=None, spark=None):
    from lakehouse.cli import add_common_args
    from lakehouse.contract_store import load_contracts
    from lakehouse.context import make_context
    p = add_common_args(argparse.ArgumentParser(description=__doc__))
    p.add_argument("--entity", required=True)
    args = p.parse_args(argv)
    from pyspark.sql import SparkSession
    ctx = make_context(args, spark or SparkSession.builder.getOrCreate())
    ctx.contracts = load_contracts(ctx.contracts_dir)
    ctx.use_catalog()
    run(ctx, args.entity)


if __name__ == "__main__":
    sys.exit(main())
