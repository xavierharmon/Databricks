#!/usr/bin/env python3
"""Copy generated tix batches into the landing folders the pipeline reads.   (runs on a laptop or as a Databricks Job task)

    python run_tool.py stage --src /Volumes/<cat>/bronze/landing/tix_generated --dst /Volumes/<cat>/bronze/landing/tix --through 3

Why a step of its own: the generator writes one folder per BATCH; the pipeline reads one folder per ENTITY. Copying batches 0..N in
order reproduces what real deliveries would have built up: numbered stream files accumulate, snapshot files (customer.csv, event.csv)
are replaced by the later batch. Run it again with a bigger --through to 'deliver' more days. Never copy truth/ or answer_key/.
"""
import argparse
import re
import shutil
from pathlib import Path


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", required=True, help="the generator's --out folder (holds batches/)")
    ap.add_argument("--dst", required=True, help="landing folder for the product, e.g. <data-root>/tix")
    ap.add_argument("--through", type=int, default=0, help="deliver batches 0..N")
    ap.add_argument("--reset", action="store_true", help="empty --dst first")
    a = ap.parse_args(argv)
    src, dst = Path(a.src) / "batches", Path(a.dst)
    if not src.is_dir():
        raise SystemExit(f"no batches folder under {a.src}: run the generator first")
    if a.reset and dst.exists():
        shutil.rmtree(dst)
    copied = replaced = 0
    for b in sorted(src.glob("batch_*")):
        if int(re.sub(r"\D", "", b.name)) > a.through:
            continue
        for ent in sorted(p for p in b.iterdir() if p.is_dir()):
            for f in sorted(ent.iterdir()):
                target = dst / ent.name / f.name
                replaced += target.exists()
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(f, target)
                copied += 1
    print(f"delivered batches 0..{a.through}: {copied} files copied ({replaced} replaced snapshots) into {dst}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
