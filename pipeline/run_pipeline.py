#!/usr/bin/env python3
"""Entry point: the file a Databricks Job (Python script task) or a person runs.

Deliberately tiny. All it does is make the code importable, then hand over to lakehouse.runner.main().
Why: the code lives in a versioned artifact (lakehouse.zip) next to this file, so this launcher never changes when the
pipeline does, and the same launcher works on a laptop and in a Job.

  python run_pipeline.py --catalog mlb_dev --pipeline-root /Volumes/mlb_dev/governance/pipeline \
         --data-root /Volumes/mlb_dev/bronze/landing --plan-only
"""
import os
import sys


def _arg(name, argv):
    for i, a in enumerate(argv):
        if a == name and i + 1 < len(argv):
            return argv[i + 1]
        if a.startswith(name + "="):
            return a.split("=", 1)[1]
    return None


def _make_importable(argv):
    candidates = []
    explicit = _arg("--code-dir", argv)
    if explicit:
        candidates.append(explicit)
    root = _arg("--pipeline-root", argv)
    if root:
        candidates.append(os.path.join(root, "code"))
    try:                                   # __file__ may not be defined by every launcher, so it is only a fallback
        here = os.path.dirname(os.path.abspath(__file__))
        candidates += [here, os.path.join(here, "code")]
    except NameError:
        pass
    for c in candidates:
        if not c or not os.path.isdir(c):
            continue
        zipped = os.path.join(c, "lakehouse.zip")
        if os.path.exists(zipped):
            sys.path.insert(0, zipped)
            return zipped
        if os.path.isdir(os.path.join(c, "lakehouse")):
            sys.path.insert(0, c)
            if os.path.isdir(os.path.join(c, "src", "lib")):      # a Git checkout keeps `lib` under src/; the zip keeps it at the top
                sys.path.insert(1, os.path.join(c, "src"))
            return c
    raise SystemExit("cannot find the pipeline code: pass --code-dir <folder containing lakehouse.zip> "
                     "(tried: " + ", ".join(c for c in candidates if c) + ")")


if __name__ == "__main__":
    _make_importable(sys.argv[1:])
    from lakehouse.runner import main
    rc = main()
    if rc:                    # exit only on failure: Databricks marks a script task that calls sys.exit(0) as failed
        sys.exit(rc)
