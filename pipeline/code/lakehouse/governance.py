"""Governance plumbing: the tables the pipeline writes to, contract publishing, and the step log.

Why governance tables: every check writes its evidence to a table keyed by run_id. A run is then explainable after the
fact with SQL, not by scrolling a log.
"""
import json
from datetime import datetime

from pyspark.sql import functions as F

from lib.contract_checks import CONTRACT_RESULTS_DDL, REGISTRY_DDL, ContractFailure, write_results
from lib.contracts import (checksum, compile_all_rules, cprops, layer_of, lint, load_metric_view_definition, main_object)
from lib.dq_engine import QUARANTINE_DDL, RESULTS_DDL, RULES_DDL, STATS_DDL

STEP_LOG_DDL = "run_id STRING, step STRING, status STRING, started_at TIMESTAMP, ended_at TIMESTAMP, detail STRING"
SCHEMAS = ["bronze", "silver", "gold", "gold_stage", "semantic", "governance"]


def ensure_objects(ctx):
    """Create schemas, governance tables and views. Idempotent: safe to run every time."""
    s = ctx.spark
    for name in SCHEMAS:
        s.sql(f"CREATE SCHEMA IF NOT EXISTS {name}")
    for table, ddl in [("dq_rules", RULES_DDL), ("dq_rule_results", RESULTS_DDL), ("dq_table_stats", STATS_DDL),
                       ("dq_quarantine", QUARANTINE_DDL), ("data_contracts", REGISTRY_DDL),
                       ("data_contracts_history", REGISTRY_DDL), ("contract_results", CONTRACT_RESULTS_DDL),
                       ("pipeline_step_log", STEP_LOG_DDL)]:
        s.sql(f"CREATE TABLE IF NOT EXISTS governance.{table} ({ddl})")
    s.sql("""CREATE OR REPLACE VIEW governance.v_dq_summary AS
        SELECT * FROM governance.dq_table_stats
        WHERE run_id = (SELECT run_id FROM governance.dq_table_stats ORDER BY checked_at DESC LIMIT 1)""")
    s.sql("""CREATE OR REPLACE VIEW governance.v_dq_open_rejects AS
        SELECT run_id, source_table, row_id, rule_ids, payload, status, fix_notes
        FROM governance.dq_quarantine WHERE disposition = 'rejected' AND status = 'open'""")
    s.sql("""CREATE OR REPLACE VIEW governance.v_contract_status AS
        SELECT contract_id, name, version, status, layer, dataset, owner, loaded_at FROM governance.data_contracts""")
    s.sql("""CREATE OR REPLACE VIEW governance.v_contract_failures AS
        SELECT * FROM governance.contract_results WHERE passed = FALSE
        AND run_id = (SELECT run_id FROM governance.contract_results ORDER BY checked_at DESC LIMIT 1)""")
    ctx.log(f"  governance objects ready in catalog {ctx.catalog}")


def publish_contracts(ctx):
    """Lint the contract set, record it (registry + history) and compile its quality rules into governance.dq_rules.

    Why publish every run: the registry then records exactly which contract versions governed each run, and the rules
    the engine reads are always compiled from the contracts that were just linted.
    """
    cs = ctx.contracts
    parsed = {}
    for name, sql in ctx.definitions.items():
        try:
            parsed[name] = load_metric_view_definition(sql, ctx.catalog)
        except Exception:  # noqa: BLE001  (a file that is not a metric view definition)
            continue
    findings = lint(cs, parsed)
    for sev, cid, msg in findings:
        ctx.log(f"  lint {sev}: {cid}: {msg}")
    now = datetime.now()
    rows = [(ctx.run_id, now, cid, "", "", "lint", msg[:200], sev, False, "") for sev, cid, msg in findings]
    errors = [f for f in findings if f[0] == "error"]
    if errors:
        write_results(ctx.spark, rows)
        raise ContractFailure(f"{len(errors)} contract lint error(s): " + "; ".join(f"{c}: {m}" for _, c, m in errors[:5]))

    reg = [(c["id"], c.get("name"), str(c["version"]), c["status"], layer_of(c), main_object(c)["physicalName"],
            cprops(c).get("owner"), checksum(c), now, json.dumps(c, sort_keys=True)) for c in cs.contracts]
    reg_df = ctx.spark.createDataFrame(reg, REGISTRY_DDL)
    reg_df.write.mode("overwrite").saveAsTable("governance.data_contracts")
    hist = ctx.spark.table("governance.data_contracts_history")
    new = reg_df.join(hist.select("contract_id", "checksum"), ["contract_id", "checksum"], "left_anti")
    n_new = new.count()
    if n_new:
        new.write.mode("append").saveAsTable("governance.data_contracts_history")
    rules = compile_all_rules(cs)
    cols = [c.split()[0] for c in RULES_DDL.split(", ")]
    ctx.spark.createDataFrame([tuple(r[c] for c in cols) for r in rules], RULES_DDL).write.mode("overwrite").saveAsTable("governance.dq_rules")
    rows.append((ctx.run_id, now, "(all)", "", "contract", "publish", f"published {len(cs.contracts)} contracts from {ctx.contracts_dir}",
                 "info", True, f"{n_new} new or changed versions; {len(rules)} rules compiled"))
    write_results(ctx.spark, rows)
    ctx.log(f"  published {len(cs.contracts)} contracts ({n_new} new/changed); compiled {len(rules)} rules")


def stats_df(spark, s: dict):
    return (spark.createDataFrame([(s["run_id"], s["layer"], s["table_name"], s["rows_in"], s["rows_deduped"], s["rows_standardized"],
                                    s["rows_warned"], s["rows_rejected"], s["rows_out"])],
                                  "run_id STRING, layer STRING, table_name STRING, rows_in BIGINT, rows_deduped BIGINT, "
                                  "rows_standardized BIGINT, rows_warned BIGINT, rows_rejected BIGINT, rows_out BIGINT")
            .withColumn("checked_at", F.current_timestamp())
            .select(*[c.split()[0] for c in STATS_DDL.split(", ")]))


def append(df, table):
    df.write.mode("append").saveAsTable(table)


def log_step(ctx, step, status, started, detail=""):
    row = [(ctx.run_id, step, status, started, datetime.now(), (detail or "")[:900])]
    try:
        ctx.spark.createDataFrame(row, STEP_LOG_DDL).write.mode("append").saveAsTable("governance.pipeline_step_log")
    except Exception as e:  # noqa: BLE001  (logging must never hide the real result)
        ctx.log(f"  could not write step log: {str(e)[:120]}")
