"""Run the pytest suite inside Databricks (or anywhere): the Tier B route.

    run_tool.py --code-dir <.../code> test --pipeline-root /Volumes/<cat>/governance/pipeline -m spark

Why a tool and not a notebook: it follows the pattern you already use (a Python script task through run_tool.py), reads the
SAME synced copy of the code and contracts your pipeline runs from, and fails the Job task when a test fails.

Everything except --pipeline-root is passed straight to pytest, so `-m spark`, `-k name`, `-x` and `-v` all work.
"""
import os
import sys


def main(argv):
    argv = list(argv)
    root = None
    for i, a in enumerate(argv):
        if a == "--pipeline-root" and i + 1 < len(argv):
            root = argv[i + 1]
            del argv[i:i + 2]
            break
        if a.startswith("--pipeline-root="):
            root = a.split("=", 1)[1]
            del argv[i]
            break
    if not root:
        raise SystemExit("usage: run_tool.py test --pipeline-root <folder holding tests/ and pytest.ini> [pytest arguments]")
    root = root.rstrip("/")
    if not os.path.isdir(os.path.join(root, "tests")):
        raise SystemExit(f"no tests/ folder in {root}. Run tools/sync_to_volume.py first: it copies tests/ next to the pipeline.")
    try:
        import pytest
    except ImportError:
        raise SystemExit("pytest is not installed in this environment. On a serverless Job, add `pytest` and `pyyaml` under "
                         "the task's Environment and libraries (dependencies).")
    # no-cacheprovider: pytest would otherwise try to write .pytest_cache into the Volume
    ini = os.path.join(root, "pytest.ini")           # pytest only accepts a lower-case .ini extension for -c
    ini = ini if os.path.exists(ini) else None
    if ini is None:
        print("warning: no pytest.ini in the pipeline root, so markers are not registered (warnings only). "
              "The sync copies it from the repo: on a case-sensitive system rename pytest.INI to pytest.ini")
    args = [os.path.join(root, "tests")] + (["-c", ini] if ini else []) + ["--rootdir", root, "-p", "no:cacheprovider", "-ra"] + argv
    print("pytest " + " ".join(args))
    return int(pytest.main(args))
