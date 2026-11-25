"""Run one of the project's tools as a Databricks Job task (Python script task), or on a laptop.

    run_tool.py [--code-dir <folder holding lakehouse.zip>] generate  --out /Volumes/<cat>/bronze/landing/tix_generated --seed 7 --batches 6
    run_tool.py [--code-dir ...] stage     --src  /Volumes/<cat>/bronze/landing/tix_generated --dst /Volumes/<cat>/bronze/landing/tix --through 3
    run_tool.py [--code-dir ...] check     /Volumes/<cat>/bronze/landing/tix_generated
    run_tool.py [--code-dir ...] identity  /Volumes/<cat>/bronze/landing/tix_generated
    run_tool.py [--code-dir ...] train     --catalog pipeline_mlb_dev [--model-dir /Volumes/<cat>/governance/pipeline/models]
    run_tool.py [--code-dir ...] test      --pipeline-root /Volumes/<cat>/governance/pipeline -m spark
    run_tool.py [--code-dir ...] catalog   --catalog-dir <folder> --contracts <folder> lint | render --out <folder> | tags-sql --catalog X | tags-apply --catalog X

Why one launcher: your Job already runs run_pipeline.py with lakehouse.zip. This does the same for the helper tools, so the whole project
(make data, deliver it, check it, run the pipeline, tag the tables) lives inside Databricks and needs no laptop.
--code-dir is needed when the zip is NOT next to this file, for example when this file is in /Workspace and the zip is in a Volume.
"""
import os
import sys

TOOLS = {"test": ("toolkit.run_tests", "main", "argv"), "generate": ("toolkit.generate_tix_data", "main", "argv"), "stage": ("toolkit.stage_landing", "main", "argv"),
         "check": ("toolkit.check_tix_data", "main", "path"), "identity": ("toolkit.identity_reference", "run", "path"),
         "catalog": ("toolkit.catalog_tool", "main", "argv"), "train": ("toolkit.train_models", "main", "argv")}


def _take(args, name):
    """Remove `--name value` or `--name=value` from args and return (value, remaining args)."""
    out, val, i = [], None, 0
    while i < len(args):
        a = args[i]
        if a == name and i + 1 < len(args):
            val, i = args[i + 1], i + 2
            continue
        if a.startswith(name + "="):
            val, i = a.split("=", 1)[1], i + 1
            continue
        out.append(a)
        i += 1
    return val, out


def _make_importable(code_dir):
    here = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
    tried = []
    for c in [x for x in (code_dir, os.path.join(here, "code"), here) if x]:
        tried.append(c)
        z = os.path.join(c, "lakehouse.zip")
        if os.path.exists(z):
            sys.path.insert(0, z)
            return z
        if os.path.isdir(os.path.join(c, "tools")):                 # a Git checkout: tools/ holds the modules
            import types
            sys.path.insert(0, c)
            pkg = types.ModuleType("toolkit"); pkg.__path__ = [os.path.join(c, "tools")]; sys.modules["toolkit"] = pkg
            return c
    raise SystemExit("cannot find lakehouse.zip. Looked in: " + ", ".join(tried) + ". Pass --code-dir <folder that holds lakehouse.zip>.")


if __name__ == "__main__":
    args = sys.argv[1:]
    code_dir, args = _take(args, "--code-dir")
    if not args or args[0] not in TOOLS:
        raise SystemExit(f"usage: run_tool.py [--code-dir DIR] {{{'|'.join(TOOLS)}}} ...")
    name, rest = args[0], args[1:]
    _make_importable(code_dir)
    import importlib
    mod, fn, style = TOOLS[name]
    f = getattr(importlib.import_module(mod), fn)
    rc = f(rest) if style == "argv" else f(rest[0] if rest else "data/tix")
    if isinstance(rc, int) and rc:            # only a non-zero integer means failure
        sys.exit(rc)
