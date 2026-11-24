"""Fast static tests (no Spark, no workspace). Run in CI on every pull request: `pytest -q`."""
import csv
import re
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from lib.sql_runner import CELL_SEP, render, split_statements, statements_for  # noqa: E402

PARAMS = {"catalog": "mlb_test", "analyst_group": "analysts"}
SQL_FILES = sorted((ROOT / "sql").rglob("*.sql"))
MV_FILES = sorted((ROOT / "sql" / "metric_views").glob("*.sql"))


# ---------- sql_runner ----------
def test_render_replaces_tokens():
    assert render("USE {{catalog}}.x", {"catalog": "c"}) == "USE c.x"


def test_render_fails_on_unknown_token():
    with pytest.raises(KeyError):
        render("{{nope}}", {"catalog": "c"})


def test_split_drops_comment_only_cells_and_trailing_semicolons():
    text = f"-- header only\n{CELL_SEP}\nSELECT 1;\n{CELL_SEP}\n-- nothing\n{CELL_SEP}\nSELECT 2;"
    assert split_statements(text) == ["SELECT 1", "SELECT 2"]


# ---------- every SQL file ----------
@pytest.mark.parametrize("path", SQL_FILES, ids=lambda p: str(p.relative_to(ROOT)))
def test_sql_files_render_and_have_statements(path):
    stmts = statements_for(path, PARAMS)
    assert stmts, f"{path.name} has no executable statements"
    for s in stmts:
        assert "{{" not in s and "}}" not in s, f"unrendered token in {path.name}"
        assert s.count("(") == s.count(")"), f"unbalanced parentheses in {path.name}: {s[:80]}"


@pytest.mark.parametrize("path", sorted((ROOT / "sql" / "dq").glob("*.sql")), ids=lambda p: p.name)
def test_dq_cells_return_contract_columns(path):
    for s in statements_for(path, PARAMS):
        low = s.lower()
        assert "check_name" in low and "severity" in low and "violations" in low, s[:80]
        assert "'error'" in low or "'warn'" in low


def test_every_ctas_has_a_comment():
    for path in sorted((ROOT / "sql" / "silver").glob("*.sql")) + sorted((ROOT / "sql" / "gold").glob("*.sql")):
        for s in statements_for(path, PARAMS):
            if s.upper().startswith("CREATE OR REPLACE TABLE"):
                assert re.search(r"\bCOMMENT\s+'", s), f"table in {path.name} has no COMMENT: {s[:70]}"


# ---------- metric views ----------
def load_mv(path):
    raw = statements_for(path, PARAMS)[0]
    return yaml.safe_load(raw.split("$$")[1])


@pytest.mark.parametrize("path", MV_FILES, ids=lambda p: p.name)
def test_metric_view_structure(path):
    spec = load_mv(path)
    assert str(spec["version"]) == "1.1"
    assert spec["source"].startswith("mlb_test.gold.")
    assert spec.get("comment"), "metric view needs a top-level comment"
    join_names = [j["name"] for j in spec.get("joins", [])]
    assert len(join_names) == len(set(join_names))
    for j in spec.get("joins", []):
        assert {"name", "source", "on"} <= set(j), j
        assert "source." in j["on"], "join condition should anchor on source."

    names = [f["name"] for f in spec["fields"]] + [m["name"] for m in spec["measures"]]
    assert len(names) == len({n.lower() for n in names}), "field/measure names must be unique (case-insensitive)"

    for item in spec["fields"] + spec["measures"]:
        assert item.get("display_name"), f"{item['name']} needs a display_name"

    # joins referenced in expressions must exist
    for item in spec["fields"] + spec["measures"]:
        for alias in re.findall(r"\b([a-z_]+)\.[a-z_]+", item["expr"]):
            assert alias in join_names + ["source"], f"{item['name']} references unknown alias {alias}"

    # MEASURE() may only reference EARLIER measures
    seen = set()
    for m in spec["measures"]:
        for ref in re.findall(r"MEASURE\((\w+)\)", m["expr"]):
            assert ref in seen, f"{m['name']} references {ref} before it is defined"
        seen.add(m["name"])

    # window order must be a field
    field_names = {f["name"] for f in spec["fields"]}
    for m in spec["measures"]:
        for w in m.get("window", []):
            assert w["order"] in field_names


def test_measure_names_are_snake_case():
    for path in MV_FILES:
        for m in load_mv(path)["measures"]:
            assert re.fullmatch(r"[a-z][a-z0-9_]*", m["name"]), m["name"]


# ---------- glossary links to real semantic assets ----------
def test_glossary_measures_exist():
    measures = {}
    for path in MV_FILES:
        view = re.search(r"semantic\.(\w+)", statements_for(path, PARAMS)[0]).group(1)
        measures[view] = {m["name"] for m in load_mv(path)["measures"]}
    rows = list(csv.DictReader(open(ROOT / "data" / "glossary" / "glossary_terms.csv")))
    assert len(rows) >= 20
    assert len({r["term"] for r in rows}) == len(rows), "duplicate glossary terms"
    for r in rows:
        assert r["definition"].strip(), r["term"]
        m = re.fullmatch(r"semantic\.(\w+)\.(\w+)", r["related_asset"])
        if m:
            assert m.group(2) in measures.get(m.group(1), set()), f"glossary term {r['term']} points at missing measure"


# ---------- bundle ----------
def test_bundle_job_dag_is_valid():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert {"dev", "staging", "prod"} <= set(bundle["targets"])
    job = yaml.safe_load((ROOT / "resources" / "mlb_pipeline.job.yml").read_text())["resources"]["jobs"]["mlb_medallion"]
    tasks = {t["task_key"]: t for t in job["tasks"]}
    for t in tasks.values():
        nb = (ROOT / "resources" / t["notebook_task"]["notebook_path"]).resolve()
        assert nb.exists(), nb
        assert nb.read_text().startswith("# Databricks notebook source")
        for d in t.get("depends_on", []):
            assert d["task_key"] in tasks
    # acyclic: repeatedly peel tasks whose dependencies are done
    done, remaining = set(), dict(tasks)
    while remaining:
        ready = [k for k, t in remaining.items() if all(d["task_key"] in done for d in t.get("depends_on", []))]
        assert ready, "cycle in job DAG"
        for k in ready:
            done.add(k)
            remaining.pop(k)


def test_quality_gates_sit_between_layers():
    job = yaml.safe_load((ROOT / "resources" / "mlb_pipeline.job.yml").read_text())["resources"]["jobs"]["mlb_medallion"]
    deps = {t["task_key"]: [d["task_key"] for d in t.get("depends_on", [])] for t in job["tasks"]}
    assert deps["gold"] == ["silver_quality_gate"]
    assert deps["metric_views"] == ["gold_quality_gate"]
