"""Tier A: no Spark. Drives your existing tools as a user would, through the CLI.

ASSUMPTIONS to confirm on first run (edit the constants if they are off):
  * `contract_tool.py lint` exits 0 on a healthy repo.
  * the runner's --plan-only prints numbered lines and needs no Spark.
"""
import re
from pathlib import Path
import subprocess
import sys
import pytest

EXPECTED_PLAN_LINES = 53          # your recorded number for --product fbl,tix
NUMBERED = re.compile(r"^\s*\d+[\.\)]?\s")


def run(pipeline_root, *args):
    return subprocess.run(
        [sys.executable, *args], cwd=pipeline_root,
        capture_output=True, text=True, timeout=120,
    )


@pytest.mark.skipif(not (Path(__file__).resolve().parents[1] / "tools" / "contract_tool.py").exists(),
                    reason="tools/ is not synced to the Volume: run this one from the repo")
def test_contract_lint_is_clean(pipeline_root):
    r = run(pipeline_root, "tools/contract_tool.py", "lint")
    assert r.returncode == 0, r.stdout + r.stderr

@pytest.mark.spark
def test_plan_only_has_expected_step_count(pipeline_root):
    r = run(
        pipeline_root, "run_pipeline.py",
        "--catalog", "pipeline_mlb_dev",
        "--pipeline-root", str(pipeline_root),
        "--data-root", str(pipeline_root / "data"),
        "--product", "fbl,tix", "--plan-only",
    )
    assert r.returncode == 0, r.stdout + r.stderr
    lines = [l for l in r.stdout.splitlines() if NUMBERED.match(l)]
    assert len(lines) == EXPECTED_PLAN_LINES, "\n".join(lines)

@pytest.mark.spark
def test_launcher_works_without___file__(pipeline_root):
    """Databricks script tasks run files via exec(), so __file__ is undefined."""
    # With no __file__ the launcher cannot guess where the code is, so it must be told (exactly the lesson from Databricks):
    # a synced Volume keeps lakehouse.zip in <root>/code, a Git checkout keeps the package in <root> itself.
    code_dir = pipeline_root / "code" if (pipeline_root / "code" / "lakehouse.zip").exists() else pipeline_root
    code = (
        "import sys; sys.argv=['run_pipeline.py','--plan-only','--product','fbl',"
        f"'--catalog','pipeline_mlb_dev','--pipeline-root',r'{pipeline_root}','--code-dir',r'{code_dir}',"
        f"'--data-root',r'{pipeline_root / 'data'}']\n"
        f"src=open(r'{pipeline_root / 'run_pipeline.py'}').read()\n"
        "g={'__name__':'__main__'}\n"            # deliberately no __file__
        "exec(compile(src,'run_pipeline.py','exec'), g)\n"
    )
    r = subprocess.run([sys.executable, "-c", code], cwd=pipeline_root,
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr
    assert "Traceback" not in r.stderr, r.stderr
    assert "PLAN for product" in r.stdout, "the launcher exited 0 without printing a plan: it proved nothing\n" + r.stdout[-400:]
