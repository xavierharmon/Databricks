#!/usr/bin/env python3
"""Assemble dist/runner_kit: the exact files and folders to upload to your Databricks Volumes.

    python tools/build_runner_kit.py [--contracts DIR]      (default: the reference contracts in ./contracts)

Output:
  dist/runner_kit/pipeline_volume/   -> upload into  /Volumes/<catalog>/governance/pipeline/
      run_pipeline.py                   the launcher a Job runs
      code/lakehouse.zip                the pipeline code (packages `lakehouse` and `lib`), one file to upload
      contracts/                        YOUR contract YAML files (empty at first)
      contracts_reference/              the answer key: a complete reference contract set
      transforms/gold/*.sql             one SELECT per gold table
      definitions/*.sql                 metric view SQL
  dist/runner_kit/data_volume/       -> upload into  /Volumes/<catalog>/bronze/landing/
      fbl/teams|players|batting/*.csv   the source files
  dist/runner_kit/UPLOAD_MAP.md
  dist/runner_kit/Script_Runner_Lab_Handbook.pdf   (the lab handbook, if docs/ has it)
Why a zip for the code: a Volume upload screen is slow with many small files, and a zip on sys.path imports fine. It is also
the shape a CI job would build and publish automatically later (a versioned artifact).
"""
import argparse
import os
import shutil
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def build_zip(dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as z:
        for pkg, src in (("lakehouse", ROOT / "lakehouse"),):
            for f in sorted(src.rglob("*.py")):
                z.write(f, f"{pkg}/{f.relative_to(src)}")
        notebook_route_only = {"fbl_pipeline.py", "sql_runner.py"}     # used by the older notebook route, not by the runner
        z.writestr("toolkit/__init__.py", "")
        for name in ("generate_tix_data.py", "check_tix_data.py", "identity_reference.py", "catalog_tool.py", "stage_landing.py", "train_models.py"):
            z.write(ROOT / "tools" / name, f"toolkit/{name}")      # the helper tools, runnable as Job tasks through run_tool.py
        for f in sorted((ROOT / "src" / "lib").glob("*.py")):          # every other module in lib: a new helper needs no list edit
            if f.name not in notebook_route_only:
                z.write(f, f"lib/{f.name}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--contracts", default=str(ROOT / "contracts"))
    ap.add_argument("--out", default=str(ROOT / "dist" / "runner_kit"))
    a = ap.parse_args()
    out = Path(a.out)
    if out.exists():
        shutil.rmtree(out)
    pv, dv = out / "pipeline_volume", out / "data_volume"
    pv.mkdir(parents=True); dv.mkdir(parents=True)
    shutil.copy(ROOT / "run_pipeline.py", pv / "run_pipeline.py")
    shutil.copy(ROOT / "run_tool.py", pv / "run_tool.py")
    shutil.copytree(ROOT / "catalog", pv / "catalog", ignore=shutil.ignore_patterns("__pycache__", "principals.yml"))
    build_zip(pv / "code" / "lakehouse.zip")
    shutil.copytree(a.contracts, pv / "contracts_reference", ignore=shutil.ignore_patterns("__pycache__"))
    (pv / "contracts").mkdir()
    (pv / "contracts" / "PUT_YOUR_CONTRACT_YAML_HERE.txt").write_text("Upload the contract YAML files YOU write into this folder. The reference answer key is in ../contracts_reference/.\n")
    shutil.copytree(ROOT / "transforms", pv / "transforms", ignore=shutil.ignore_patterns("__pycache__"))
    (pv / "definitions").mkdir()
    for d in (ROOT / "sql" / "fbl" / "metric_views", ROOT / "sql" / "metric_views"):
        for f in d.glob("*.sql"):
            shutil.copy(f, pv / "definitions" / f.name)
    for name, folder in (("teams", "teams"), ("players", "players"), ("batting", "batting")):
        (dv / "fbl" / folder).mkdir(parents=True)
        shutil.copy(ROOT / "data" / "fbl" / f"{name}.csv", dv / "fbl" / folder / f"{name}.csv")
    handbook = ROOT / "docs" / "Script_Runner_Lab_Handbook.pdf"
    if handbook.exists():
        shutil.copy(handbook, out / "Script_Runner_Lab_Handbook.pdf")
    (out / "UPLOAD_MAP.md").write_text("""# Upload map

| From this folder | To this Volume path |
| --- | --- |
| pipeline_volume/run_pipeline.py | /Volumes/<catalog>/governance/pipeline/run_pipeline.py |
| pipeline_volume/run_tool.py | /Volumes/<catalog>/governance/pipeline/run_tool.py |
| pipeline_volume/catalog/ (all folders) | /Volumes/<catalog>/governance/pipeline/catalog/ |
| pipeline_volume/code/lakehouse.zip | /Volumes/<catalog>/governance/pipeline/code/lakehouse.zip |
| pipeline_volume/contracts_reference/ (all folders) | /Volumes/<catalog>/governance/pipeline/contracts_reference/ |
| (your own YAML, written in the labs) | /Volumes/<catalog>/governance/pipeline/contracts/ |
| pipeline_volume/transforms/gold/*.sql | /Volumes/<catalog>/governance/pipeline/transforms/gold/ |
| pipeline_volume/transforms/audits/*.sql | /Volumes/<catalog>/governance/pipeline/transforms/audits/ |
| pipeline_volume/definitions/*.sql | /Volumes/<catalog>/governance/pipeline/definitions/ |
| data_volume/fbl/teams/teams.csv | /Volumes/<catalog>/bronze/landing/fbl/teams/teams.csv |
| data_volume/fbl/players/players.csv | /Volumes/<catalog>/bronze/landing/fbl/players/players.csv |
| data_volume/fbl/batting/batting.csv | /Volumes/<catalog>/bronze/landing/fbl/batting/batting.csv |

Create the two Volumes first (see the labs): `<catalog>.governance.pipeline` and `<catalog>.bronze.landing`.
""")
    print(f"built {out}")
    for p in sorted(out.rglob("*")):
        if p.is_file():
            print("  ", p.relative_to(out))


if __name__ == "__main__":
    main()
