"""Static tests for the script runner (no Spark): the plan, step selection, the launcher, the kit."""
import glob
import subprocess
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "src"))
pytest.importorskip("pyspark")   # the stage modules import pyspark at import time
from lakehouse import plan as planner, runner  # noqa: E402
from lib.contracts import ContractSet  # noqa: E402


def contracts():
    return ContractSet([yaml.safe_load(open(f)) for f in sorted(glob.glob(str(ROOT / "contracts/**/*.yaml"), recursive=True))])


def test_plan_orders_parents_before_children_and_publish_after_all_audits():
    ordered, deps = planner.build_plan(contracts(), "fbl")
    keys = [s.key for s in ordered]
    assert keys.index("silver:team") < keys.index("silver:player") < keys.index("silver:batting")
    assert keys.index("gold:dim_team") < keys.index("gold:dim_player") and keys.index("gold:dim_player") < keys.index("gold:fact_batting")
    assert keys.index("gold-publish") > max(keys.index(k) for k in keys if k.startswith("gold:"))
    assert all(keys.index(k) > keys.index("gold-publish") for k in keys if k.startswith("semantic:"))
    assert deps["silver:player"] == ["bronze:player", "silver:team"]


def test_plan_follows_the_contracts_when_only_one_slice_exists():
    cs = contracts()
    only_team = ContractSet([c for c in cs.contracts if c["id"].endswith(".team") or c["id"].endswith("dim_team")])
    keys = [s.key for s in planner.build_plan(only_team, "fbl")[0]]
    assert keys == ["bronze:team", "silver:team", "gold:dim_team", "gold-publish"]


def test_every_step_has_an_explanation():
    cs = contracts()
    for s in planner.build_plan(cs, "fbl")[0]:
        assert len(planner.explain(cs, s, "fbl")) > 20


def test_cycle_is_detected():
    cs = contracts()
    cyc = [c for c in cs.contracts]
    for c in cyc:                       # make team depend on player: team -> player -> team
        if c["id"] == "fbl.silver.team":
            c["schema"][0]["properties"][0]["relationships"] = [{"to": "fbl_player.player_id", "type": "foreignKey"}]
    with pytest.raises(ValueError, match="cycle"):
        planner.build_plan(ContractSet(cyc), "fbl")


def test_select_steps():
    ordered, _ = planner.build_plan(contracts(), "fbl")
    a = SimpleNamespace(steps="", stages="silver", entities="team,player")
    assert [s.key for s in runner.select_steps(ordered, a)] == ["silver:player", "silver:team"] or \
           {s.key for s in runner.select_steps(ordered, a)} == {"silver:team", "silver:player"}
    a = SimpleNamespace(steps="silver:team,gold-publish", stages="", entities="")
    assert [s.key for s in runner.select_steps(ordered, a)] == ["silver:team", "gold-publish"]
    with pytest.raises(SystemExit, match="unknown step"):
        runner.select_steps(ordered, SimpleNamespace(steps="silver:nope", stages="", entities=""))


def test_launcher_finds_the_code_zip_and_fails_clearly_without_it(tmp_path):
    kit = tmp_path / "kit"
    subprocess.run([sys.executable, str(ROOT / "tools" / "build_runner_kit.py"), "--out", str(kit)], check=True, capture_output=True)
    z = kit / "pipeline_volume" / "code" / "lakehouse.zip"
    names = set(zipfile.ZipFile(z).namelist())
    assert {"lakehouse/runner.py", "lakehouse/stages/silver.py", "lib/dq_engine.py", "lib/contracts.py"} <= names
    r = subprocess.run([sys.executable, str(ROOT / "run_pipeline.py"), "--catalog", "x", "--pipeline-root", str(tmp_path / "nowhere"),
                        "--data-root", str(tmp_path), "--code-dir", str(tmp_path / "empty")], capture_output=True, text=True, cwd=tmp_path)
    # run from the repo copy the code is importable via __file__, so only assert the clear-failure path with a copied launcher
    lone = tmp_path / "lone"; lone.mkdir()
    (lone / "run_pipeline.py").write_text((ROOT / "run_pipeline.py").read_text())
    r = subprocess.run([sys.executable, str(lone / "run_pipeline.py"), "--catalog", "x", "--pipeline-root", str(tmp_path / "nowhere"),
                        "--data-root", str(tmp_path)], capture_output=True, text=True)
    assert r.returncode != 0 and "cannot find the pipeline code" in (r.stderr + r.stdout)


def test_kit_contains_everything_the_labs_tell_you_to_upload(tmp_path):
    kit = tmp_path / "kit"
    subprocess.run([sys.executable, str(ROOT / "tools" / "build_runner_kit.py"), "--out", str(kit)], check=True, capture_output=True)
    pv, dv = kit / "pipeline_volume", kit / "data_volume"
    for rel in ["run_pipeline.py", "code/lakehouse.zip", "transforms/gold/dim_team.sql", "transforms/gold/dim_player.sql",
                "transforms/gold/fact_batting.sql", "definitions/mv_fbl_batting.sql", "contracts_reference/silver/fbl_silver_team.yaml"]:
        assert (pv / rel).exists(), rel
    for rel in ["fbl/teams/teams.csv", "fbl/players/players.csv", "fbl/batting/batting.csv"]:
        assert (dv / rel).exists(), rel
    assert (kit / "UPLOAD_MAP.md").exists()


def test_gold_transforms_exist_for_every_gold_contract():
    from lib.contracts import cprops, main_object
    for c in contracts().layer("gold", "fbl"):
        assert (ROOT / "transforms" / "gold" / f"{cprops(main_object(c))['entity']}.sql").exists()


def test_no_notebook_headers_in_the_runner_code():
    for f in list((ROOT / "lakehouse").rglob("*.py")) + [ROOT / "run_pipeline.py"]:
        assert "Databricks notebook source" not in f.read_text().split("\n")[0], f
        assert "dbutils" not in f.read_text(), f"{f} must not depend on notebook-only objects"
