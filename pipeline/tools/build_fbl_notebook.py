#!/usr/bin/env python3
"""Inline the pipeline libraries + the contract bundle + the default rules into ONE import-ready Databricks notebook.

    python tools/build_fbl_notebook.py   ->  src/notebooks/fbl_dq_pipeline.py

In manual mode you import this single file (Workspace > Import) and run it; nothing else has to be uploaded.
It carries a built-in copy of every contract and metric-view definition, so the first run works with zero uploads.
To use YOUR edited contracts instead, upload contracts_bundle.json (or the YAML files) to <volume>/fbl/contracts/.
Re-run this script after editing src/lib/*.py, contracts/ or sql/fbl/metric_views/.
Needs PyYAML on the machine that builds it (pip install pyyaml).
"""
import glob
import json
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from lib import contracts as C  # noqa: E402


def lib(name: str) -> str:
    text = (ROOT / "src/lib" / f"{name}.py").read_text()
    text = re.sub(r"from lib\.\w+ import \([^)]*\)\n", "", text)      # parenthesised imports
    return re.sub(r"from lib\.\w+ import [^\n(]*\n", "", text)         # single-line imports


contracts = [yaml.safe_load(open(f)) for f in sorted(glob.glob(str(ROOT / "contracts/**/*.yaml"), recursive=True))]
defs = {}
for c in contracts:
    if C.layer_of(c) == "semantic":
        p = ROOT / C.cprops(C.main_object(c))["definition"]
        defs[p.name] = p.read_text()
bundle_json = json.dumps(C.make_bundle(contracts, defs), sort_keys=True)
bundle_lit = "EMBEDDED_BUNDLE_JSON = (\n" + "".join(f"    {bundle_json[i:i + 100]!r}\n" for i in range(0, len(bundle_json), 100)) + ")\n"

rules = (ROOT / "data/dq/dq_rules.csv").read_text()
rules_lit = "DEFAULT_RULES_CSV = (\n" + "".join(f"    {line!r}\n" for line in rules.splitlines(keepends=True)) + ")\n"

glue = '''
import json
import os
from datetime import datetime

dbutils.widgets.text("catalog", "mlb_dev")
dbutils.widgets.dropdown("stage", "all", ["all", "setup", "contracts", "bronze", "silver", "gold", "semantic", "report"])
dbutils.widgets.text("run_id", "")
dbutils.widgets.dropdown("fail_on_rejects", "true", ["true", "false"])
dbutils.widgets.dropdown("rules_source", "contracts", ["contracts", "csv"])
dbutils.widgets.dropdown("include_semantic", "true", ["true", "false"])
dbutils.widgets.dropdown("reload_rules", "false", ["false", "true"])
dbutils.widgets.text("landing_dir", "")
dbutils.widgets.text("contracts_dir", "")

catalog = dbutils.widgets.get("catalog").strip()
stage = dbutils.widgets.get("stage")
run_id = dbutils.widgets.get("run_id").strip() or datetime.now().strftime("%Y%m%d-%H%M%S")
fail_on_rejects = dbutils.widgets.get("fail_on_rejects") == "true"
rules_source = dbutils.widgets.get("rules_source")
include_semantic = dbutils.widgets.get("include_semantic") == "true"
landing = dbutils.widgets.get("landing_dir").strip() or f"/Volumes/{catalog}/bronze/landing/fbl"
contracts_dir = dbutils.widgets.get("contracts_dir").strip() or f"{landing}/contracts"
spark.sql(f"USE CATALOG {catalog}")


def load_contract_sources():
    """Contracts you uploaded (bundle JSON or YAML) win; otherwise the copy built into this notebook."""
    found = discover_contracts(contracts_dir) if os.path.isdir(contracts_dir) else None
    if found:
        return found
    b = json.loads(EMBEDDED_BUNDLE_JSON)
    return contracts_from_bundle(b), b.get("definitions", {}), "built-in bundle (nothing found in " + contracts_dir + ")"


rules_path = f"{landing}/rules/dq_rules.csv"          # legacy CSV mode only
rules_text = open(rules_path).read() if os.path.exists(rules_path) else DEFAULT_RULES_CSV
print(f"catalog={catalog} stage={stage} run_id={run_id} rules_source={rules_source}")

# COMMAND ----------
failure = None
try:
    if stage in ("all", "setup"):
        stage_setup(spark, rules_text if rules_source == "csv" else None, dbutils.widgets.get("reload_rules") == "true")
    if stage in ("all", "contracts") and rules_source == "contracts":
        contracts, definitions, origin = load_contract_sources()
        stage_contracts(spark, run_id, contracts, definitions, origin)
    if stage in ("all", "bronze"):
        stage_bronze(spark, landing, run_id)
    if stage in ("all", "silver"):
        stage_silver(spark, run_id)
    if stage in ("all", "gold"):
        stage_gold(spark, run_id)
    if stage == "semantic" or (stage == "all" and include_semantic):
        _, definitions, _ = load_contract_sources()
        stage_semantic(spark, run_id, catalog, definitions)
except Exception as e:                       # keep going so the report still gets written
    failure = e
    print("STAGE FAILED:", e)

# COMMAND ----------
if stage in ("all", "report"):
    report = build_report(spark, run_id)
    print(report["text"])
    displayHTML(report["html"])
    try:
        out_dir = os.path.dirname(landing) + "/reports"
        os.makedirs(out_dir, exist_ok=True)
        with open(f"{out_dir}/dq_report_{run_id}.html", "w") as fh:
            fh.write(report["html"])
        print(f"report file: {out_dir}/dq_report_{run_id}.html")
    except Exception as e:                   # reporting must never hide the real result
        print("could not write report file:", e)
    if failure is None and fail_on_rejects and (report["rejected"] > 0 or report["failed_rules"] > 0 or report["contract_failures"] > 0):
        failure = DQFailure("KICK BACK: " + report["text"].splitlines()[0])

if failure is not None:
    raise failure
'''
cells = [
    "# Databricks notebook source\n# MAGIC %md\n# MAGIC # FBL contract-driven data-quality pipeline\n"
    "# MAGIC contracts > bronze > silver (+DQ) > gold (+assertions) > semantic (metric views + contracts) > report.\n"
    "# MAGIC One notebook; set the `stage` widget (or run `all`). See docs/DQ_PIPELINE.md and docs/DATA_CONTRACTS.md.\n"
    "# MAGIC Generated by tools/build_fbl_notebook.py. Edit src/lib/*.py or contracts/ and rebuild instead of editing this file.\n",
    lib("dq_engine"), lib("contracts"), lib("contract_checks"), lib("fbl_pipeline"), bundle_lit + "\n" + rules_lit, glue]
out = ROOT / "src/notebooks/fbl_dq_pipeline.py"
out.write_text("\n\n# COMMAND ----------\n\n".join(cells))
print(f"wrote {out} ({out.stat().st_size // 1024} KB), {len(contracts)} contracts embedded")
