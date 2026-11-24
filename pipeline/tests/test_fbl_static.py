"""Static checks for the FBL data-quality add-on (need pyspark installed only because dq_engine imports it)."""
import csv
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from lib.dq_engine import parse_rules_csv  # noqa: E402


def test_rules_csv_is_valid_and_complete():
    rules = parse_rules_csv((ROOT / "data/dq/dq_rules.csv").read_text())   # raises on bad type/action/duplicate id
    layers = {(r["layer"], r["table_name"]) for r in rules}
    assert {("silver", "team"), ("silver", "player"), ("silver", "batting"), ("gold", "dim_team"),
            ("gold", "dim_player"), ("gold", "fact_batting")} <= layers
    assert all(r["description"] for r in rules)
    assert all(r["action"] == "fail" for r in rules if r["layer"] == "gold"), "gold rules must be hard failures"


def test_every_silver_table_dedupes_and_checks_keys_and_nulls():
    rules = parse_rules_csv((ROOT / "data/dq/dq_rules.csv").read_text())
    for t in ("team", "player", "batting"):
        types = {r["rule_type"] for r in rules if r["layer"] == "silver" and r["table_name"] == t}
        assert {"exact_duplicate", "unique_strict", "not_null"} <= types, t


def test_generated_notebook_is_current():
    nb = (ROOT / "src/notebooks/fbl_dq_pipeline.py").read_text()
    assert nb.startswith("# Databricks notebook source")
    assert "def apply_rules" in nb and "def stage_silver" in nb
    for line in (ROOT / "data/dq/dq_rules.csv").read_text().splitlines(keepends=True):
        assert repr(line) in nb, "rules CSV changed: run python tools/build_fbl_notebook.py"
    assert (ROOT / "src/lib/dq_engine.py").read_text().split('"""', 2)[2].strip().splitlines()[0] in nb


def test_fbl_data_has_the_planted_problems():
    rows = list(csv.DictReader(open(ROOT / "data/fbl/teams.csv")))
    assert len(rows) == 15 and sum(r["team_id"] == "T09" for r in rows) == 2
    assert any(r["founded_year"] == "2099" for r in rows)


def test_fbl_job_graph():
    job = yaml.safe_load((ROOT / "resources/fbl_dq_pipeline.job.yml").read_text())["resources"]["jobs"]["fbl_dq_pipeline"]
    tasks = {t["task_key"]: t for t in job["tasks"]}
    assert [t for t in tasks] == ["setup", "contracts_publish", "bronze", "silver_dq", "gold_assert", "semantic_contracts", "report_and_notify"]
    assert tasks["report_and_notify"]["run_if"] == "ALL_DONE"
    for t in tasks.values():
        assert (ROOT / "resources" / t["notebook_task"]["notebook_path"]).exists()
