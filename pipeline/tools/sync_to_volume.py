#!/usr/bin/env python3
"""Copy what the repository says should run into the place your Jobs read, and stamp it so you can prove it.

    python tools/sync_to_volume.py --dry-run               # show what would change; write nothing
    python tools/sync_to_volume.py                         # lint, then copy changed files, then write VERSION.json
    python tools/sync_to_volume.py --verify                # compare only; exit 1 if the runtime copy is not what the repo says
    python tools/sync_to_volume.py --prune                 # also delete files that were removed from the repo (entries with prune: true)

Why this exists: a Git pull updates a Git folder. Your Job reads a Volume (and maybe a /Workspace folder). Without a step that copies
between them, "I pulled" and "the Job runs the new version" are two different facts. This step makes them one fact, and leaves a
fingerprint (VERSION.json) that the runner logs at the start of every run.

How it runs inside Databricks: a Python script task whose source is the Git folder (or a Job with a Git source), for example
    /Workspace/Users/<me>/<git folder>/tools/sync_to_volume.py   with parameters  --dest /Volumes/pipeline_mlb_dev/governance/pipeline
It needs only the repository checkout: no zip, no Spark (Spark is used only with --catalog, to write a row to governance.sync_log).

Safety rules built in:
  * Lint first. If any contract or metric view fails lint, NOTHING is copied.
  * Two sources that would land on the same destination name with different content stop the sync.
  * Files are compared by content hash, so unchanged files are not rewritten.
  * VERSION.json is written LAST: a half-finished sync leaves the old stamp, which --verify then reports as stale.
  * Nothing is deleted unless you ask (--prune) and the entry says prune: true.

What the stamp means, honestly: `content_fingerprint` is a hash of every file the sync manages, so it is reliable. `commit` is best effort:
it is read from --commit, or from the checkout's .git folder if one is visible, and is 'unknown' otherwise (a Databricks Git folder does
not expose .git, and I found no Jobs dynamic value reference that carries the commit). Pass --commit from your own automation if you can.
"""
import argparse
import fnmatch
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import yaml

TOOLS = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS))


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def zip_digest(path):
    """A zip's bytes change with build time even when its contents do not, so compare names, sizes and CRCs instead."""
    with zipfile.ZipFile(path) as z:
        rows = sorted((i.filename, i.file_size, i.CRC) for i in z.infolist() if not i.is_dir())
    return hashlib.sha256(json.dumps(rows).encode()).hexdigest()


def load_config(path):
    cfg = yaml.safe_load(open(path)) or {}
    for k in ("dest_root", "zip", "sync"):
        if k not in cfg:
            raise SystemExit(f"{path}: missing '{k}'")
    return cfg


def gather(repo, cfg):
    """[(source file, destination relative to dest_root or absolute, entry)] for every file the sync manages."""
    items, seen = [], {}
    for entry in cfg["sync"]:
        srcs = entry["src"] if isinstance(entry["src"], list) else [entry["src"]]
        for s in srcs:
            base = Path(repo) / s
            if not base.is_dir():
                raise SystemExit(f"sync entry '{entry['name']}': source folder not found: {base}")
            for f in sorted(p for p in base.rglob("*") if p.is_file()):
                rel = f.relative_to(base)
                if any(fnmatch.fnmatch(part, pat) or fnmatch.fnmatch(str(rel), pat) for pat in entry.get("exclude", []) for part in rel.parts):
                    continue
                if "__pycache__" in rel.parts or f.suffix == ".pyc":
                    continue
                dst = os.path.join(entry["dst"], f.name if entry.get("flatten") else str(rel))
                if dst in seen and sha(seen[dst]) != sha(f):
                    raise SystemExit(f"two different files would both become {dst}: {seen[dst]} and {f}. Rename one.")
                if dst not in seen:
                    seen[dst] = f
                    items.append((f, dst, entry))
    for name in cfg.get("launchers", []):
        f = Path(repo) / name
        if not f.is_file():
            raise SystemExit(f"launcher not found: {f}")
        items.append((f, name, {"name": "launchers"}))
    return items


def resolve(dest_root, dst):
    return dst if os.path.isabs(dst) else os.path.join(dest_root, dst)


def preflight_lint(items):
    """Lint the contracts and metric view definitions that are about to be deployed, with the same linter the runner uses."""
    sys.path.insert(0, str(TOOLS.parent / "src"))
    from lib.contracts import ContractSet, lint, load_metric_view_definition, parse_yaml
    docs, parsed = [], {}
    for f, dst, entry in items:
        if entry.get("kind") == "contracts" and f.suffix in (".yaml", ".yml"):
            d = parse_yaml(open(f).read())
            if isinstance(d, dict) and d.get("kind") == "DataContract":
                docs.append(d)
        elif entry.get("kind") == "definitions" and f.suffix == ".sql":
            try:
                parsed[f.name] = load_metric_view_definition(open(f).read(), "x")
            except Exception:  # noqa: BLE001  (not a metric view file)
                pass
    if not docs:
        return [], 0
    return [x for x in lint(ContractSet(docs), parsed) if x[0] == "error"], len(docs)


def build_zip_file(workdir):
    from build_runner_kit import build_zip
    out = Path(workdir) / "lakehouse.zip"
    build_zip(out)
    return out


def find_commit(repo, given):
    if given:
        return given
    try:
        r = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"], capture_output=True, text=True, timeout=10)
        if r.returncode == 0 and r.stdout.strip():
            return r.stdout.strip()
    except Exception:  # noqa: BLE001
        pass
    head = Path(repo) / ".git" / "HEAD"
    try:
        ref = head.read_text().strip()
        if ref.startswith("ref:"):
            p = Path(repo) / ".git" / ref.split(" ", 1)[1]
            return p.read_text().strip() if p.exists() else "unknown"
        return ref
    except Exception:  # noqa: BLE001
        return "unknown"


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo-root", default=str(TOOLS.parent))
    ap.add_argument("--config", default=None, help="default <repo>/deploy/sync.yml")
    ap.add_argument("--dest", default=None, help="overrides dest_root in the config")
    ap.add_argument("--commit", default="", help="git commit to record (best effort if omitted)")
    ap.add_argument("--branch", default="")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verify", action="store_true", help="compare only; exit 1 if the runtime copy differs from the repo")
    ap.add_argument("--prune", action="store_true", help="delete stale files in entries that set prune: true")
    ap.add_argument("--no-lint", action="store_true")
    ap.add_argument("--catalog", default="", help="also write a row to <catalog>.governance.sync_log (needs Spark)")
    a = ap.parse_args(argv)

    repo = Path(a.repo_root)
    cfg = load_config(a.config or repo / "deploy" / "sync.yml")
    dest = a.dest or cfg["dest_root"]
    items = gather(repo, cfg)
    print(f"repository {repo}: {len(items)} files managed; destination {dest}")

    if not a.no_lint:
        errs, n = preflight_lint(items)
        for sev, cid, msg in errs:
            print(f"  lint error: {cid}: {msg}")
        if errs:
            print(f"STOPPED: {len(errs)} lint error(s) in {n} contracts. Nothing was copied.")
            return 1
        print(f"lint ok: {n} contracts")

    work = tempfile.mkdtemp()
    zpath = build_zip_file(work)
    zdig = zip_digest(zpath)

    status = {"new": [], "changed": [], "same": []}
    lines = []
    for f, dst, entry in items:
        target = resolve(dest, dst)
        h = sha(f)
        lines.append(f"{dst} {h}")
        if not os.path.exists(target):
            status["new"].append((f, target, dst))
        elif sha(target) != h:
            status["changed"].append((f, target, dst))
        else:
            status["same"].append((f, target, dst))
    ztarget = resolve(dest, cfg["zip"])
    zstate = "new" if not os.path.exists(ztarget) else ("same" if zip_digest(ztarget) == zdig else "changed")
    fingerprint = hashlib.sha256("\n".join(sorted(lines) + [f"zip {zdig}"]).encode()).hexdigest()

    stale = []
    for entry in cfg["sync"]:
        want = {resolve(dest, d) for _, d, e in items if e is entry}
        root = resolve(dest, entry["dst"])
        if os.path.isdir(root):
            for p in Path(root).rglob("*"):
                if p.is_file() and str(p) not in want and "__pycache__" not in p.parts:
                    stale.append((str(p), bool(entry.get("prune"))))

    vpath = os.path.join(dest, "VERSION.json")
    old = json.load(open(vpath)) if os.path.exists(vpath) else {}
    print(f"files: {len(status['new'])} new, {len(status['changed'])} changed, {len(status['same'])} unchanged; "
          f"zip {zstate}; {len(stale)} stale in destination; stamp {'matches' if old.get('content_fingerprint') == fingerprint else 'differs'}")
    for k in ("new", "changed"):
        for _, target, dst in status[k][:25]:
            print(f"  {k:8} {dst}")
        if len(status[k]) > 25:
            print(f"  ... and {len(status[k]) - 25} more {k}")
    for p, pr in stale[:15]:
        print(f"  stale    {p}{'  (will be deleted)' if pr and a.prune else ''}")

    differs = bool(status["new"] or status["changed"] or zstate != "same" or old.get("content_fingerprint") != fingerprint)
    if a.verify:
        print("VERIFY FAILED: the runtime copy is not what the repository says. Run the sync." if differs else "VERIFY OK: the runtime copy matches the repository.")
        return 1 if differs else 0
    if a.dry_run:
        print("dry run only; nothing written.")
        return 0

    commit_now = find_commit(repo, a.commit)
    if not differs and (commit_now == "unknown" or old.get("commit") == commit_now):
        print(f"already up to date (stamp {fingerprint[:12]}, commit {str(old.get('commit'))[:12]}); nothing written.")
        return 0
    for f, target, _ in status["new"] + status["changed"]:
        os.makedirs(os.path.dirname(target), exist_ok=True)
        shutil.copyfile(f, target)
    if zstate != "same":
        os.makedirs(os.path.dirname(ztarget), exist_ok=True)
        shutil.copyfile(zpath, ztarget)
    deleted = 0
    if a.prune:
        for p, pr in stale:
            if pr:
                os.remove(p)
                deleted += 1
    stamp = dict(content_fingerprint=fingerprint, commit=commit_now, branch=a.branch or "unknown",
                 synced_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), files=len(items), zip_digest=zdig,
                 files_new=len(status["new"]), files_changed=len(status["changed"]), files_deleted=deleted)
    os.makedirs(dest, exist_ok=True)
    with open(vpath, "w") as fh:
        json.dump(stamp, fh, indent=1)                      # last, so a half-finished sync cannot look complete
    print(f"synced: {len(status['new'])} new, {len(status['changed'])} changed, zip {zstate}, {deleted} deleted. "
          f"stamp {fingerprint[:12]} commit {stamp['commit'][:12]}")

    if a.catalog:
        from pyspark.sql import SparkSession
        sp = SparkSession.builder.getOrCreate()
        sp.sql(f"USE CATALOG {a.catalog}")
        sp.sql("CREATE SCHEMA IF NOT EXISTS governance")
        sp.sql("CREATE TABLE IF NOT EXISTS governance.sync_log (synced_at STRING, content_fingerprint STRING, commit STRING, branch STRING, "
               "files_new INT, files_changed INT, files_deleted INT, destination STRING)")
        sp.createDataFrame([(stamp["synced_at"], fingerprint, stamp["commit"], stamp["branch"], stamp["files_new"], stamp["files_changed"], deleted, dest)],
                           "synced_at STRING, content_fingerprint STRING, commit STRING, branch STRING, files_new INT, files_changed INT, files_deleted INT, destination STRING"
                           ).write.mode("append").saveAsTable("governance.sync_log")
        print("recorded in governance.sync_log")
    return 0


if __name__ == "__main__":
    sys.exit(main())
