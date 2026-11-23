"""BRONZE: one entity per call. Land the file exactly as delivered and verify its shape against the source contract.

Why this stage is deliberately dumb: it must not convert, clean or filter. Everything arrives as text, so a bad value can
never fail the load or be silently altered. All judgment happens in silver, where bad values can be counted and reported.
Run on its own:  python -m lakehouse.stages.bronze --entity team --catalog ... --pipeline-root ... --data-root ...
"""
import argparse
import os
import sys
from datetime import datetime

from pyspark.sql import functions as F

from lakehouse.errors import StageFailure
from lib.contract_checks import raise_if_broken, schema_conformance, write_results
from lib.contracts import cprops, expected_columns, main_object
from lib.dq_engine import RESULTS_DDL, raise_if_failed


def run(ctx, entity):
    sc = ctx.contracts.entity("source", entity, ctx.product)
    if sc is None:
        raise StageFailure(f"no source contract for entity {entity!r}: write contracts/source/{ctx.product}_source_{entity}.yaml")
    obj = main_object(sc)
    folder = cprops(obj)["landingFolder"]
    path = f"{ctx.landing_dir}/{folder}"
    if not os.path.isdir(path):
        raise StageFailure(f"landing folder not found: {path}. Upload the file there (the contract says landingFolder = {folder}).")
    # 1) read everything as text: typing is silver's job
    df = ctx.spark.read.option("header", "true").option("inferSchema", "false").csv(path + "/")
    expected = expected_columns(sc)
    missing = [c for c in expected if c not in df.columns]
    n = df.count()
    now = datetime.now()
    # 2) structural checks promised by the source contract. A structural problem stops the run: nothing downstream can be trusted.
    results = [
        (ctx.run_id, now, "bronze", entity, "BRZ-001", "columns_present", "fail", len(missing), n,
         "Expected columns are present" + (f" (missing: {', '.join(missing)})" if missing else "")),
        (ctx.run_id, now, "bronze", entity, "BRZ-002", "not_empty", "fail", 1 if n == 0 else 0, n, "Source file is not empty"),
    ]
    res = ctx.spark.createDataFrame(results, RESULTS_DDL)
    res.write.mode("append").saveAsTable("governance.dq_rule_results")
    raise_if_failed(res)
    # 3) write the bronze table with lineage columns: where each row came from, and a stable position in the file
    target = obj["physicalName"]
    (df.select(*[F.col(c) for c in df.columns],
               F.monotonically_increasing_id().alias("_row_id"),
               F.col("_metadata.file_path").alias("_source_file"),
               F.current_timestamp().alias("_ingested_at"))
       .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target))
    # 4) check the table that was actually written against the contract
    rows = schema_conformance(ctx.spark, ctx.run_id, sc)
    write_results(ctx.spark, rows)
    raise_if_broken(rows)
    ctx.log(f"  {target}: {n} rows  [contract {sc['id']} v{sc['version']}]")


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
