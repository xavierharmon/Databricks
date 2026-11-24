#!/usr/bin/env python3
"""Data-contract toolbox: lint, bundle (for upload), stamp, diff (breaking changes), export-rules.

  python tools/contract_tool.py lint                       # validate every contract in contracts/ (+ metric view definitions)
  python tools/contract_tool.py bundle                     # lint, then write contracts_bundle.json  <- upload this one file
  python tools/contract_tool.py stamp                      # refresh formula fingerprints (exprHash) after a deliberate change
  python tools/contract_tool.py diff --git-ref origin/main # breaking-change check against another version (CI uses this)
  python tools/contract_tool.py diff --old DIR --new DIR   # same, comparing two folders
  python tools/contract_tool.py export-rules               # regenerate data/dq/dq_rules.csv from the contracts
  python tools/contract_tool.py compare --mine student/contracts   # compare the contracts YOU wrote with the reference set
  python tools/contract_tool.py --contracts student/contracts plan # print the execution plan the runner would follow
  python tools/contract_tool.py --contracts student/contracts typed-sql team   # print the SELECT silver will run for an entity

Needs PyYAML (pip install pyyaml) on the machine that runs it. Databricks does not need it if you upload the bundle.
"""
import argparse
import csv
import glob
import json
import os
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from lib import contracts as C  # noqa: E402


def load(dir_):
    files = sorted(glob.glob(os.path.join(dir_, "**", "*.yaml"), recursive=True))
    return files, [yaml.safe_load(open(f)) for f in files]


def definitions_for(contracts, root):
    defs, parsed = {}, {}
    for c in contracts:
        if C.layer_of(c) == "semantic":
            path = C.cprops(C.main_object(c)).get("definition", "")
            p = Path(root) / path
            if p.exists():
                text = p.read_text()
                defs[os.path.basename(path)] = text
                parsed[path] = C.load_metric_view_definition(text)
    return defs, parsed


def cmd_lint(a):
    files, contracts = load(a.contracts)
    cs = C.ContractSet(contracts)
    defs, parsed = definitions_for(contracts, ROOT)
    findings = C.lint(cs, parsed)
    for sev, cid, msg in findings:
        print(f"{sev.upper():5} {cid}: {msg}")
    errors = sum(1 for f in findings if f[0] == "error")
    print(f"{len(contracts)} contracts, {errors} errors, {len(findings) - errors} warnings")
    return 1 if errors else 0


def cmd_bundle(a):
    if cmd_lint(a):
        print("fix lint errors before bundling")
        return 1
    _, contracts = load(a.contracts)
    defs, _ = definitions_for(contracts, ROOT)
    bundle = C.make_bundle(contracts, defs)
    Path(a.out).write_text(json.dumps(bundle, indent=1))
    print(f"wrote {a.out}: {len(contracts)} contracts, {len(defs)} metric view definitions. Upload it to <volume>/fbl/contracts/")
    return 0


def cmd_stamp(a):
    files, contracts = load(a.contracts)
    n = 0
    for f, c in zip(files, contracts):
        if C.layer_of(c) != "semantic":
            continue
        path = C.cprops(C.main_object(c))["definition"]
        spec = C.load_metric_view_definition((ROOT / path).read_text())
        old = Path(f).read_text()
        new = C.stamp_expr_hashes(old, spec)
        if new != old:
            Path(f).write_text(new)
            n += 1
            print(f"stamped {f}  (now bump `version` and describe the change)")
    print(f"{n} contract(s) updated")
    return 0


def cmd_diff(a):
    if a.git_ref:
        new_files, new = load(a.contracts)
        old = []
        for f in new_files:
            rel = os.path.relpath(f, ROOT)
            out = subprocess.run(["git", "show", f"{a.git_ref}:{rel}"], cwd=ROOT, capture_output=True, text=True)
            if out.returncode == 0:
                old.append(yaml.safe_load(out.stdout))
        # contracts deleted on this branch
        listing = subprocess.run(["git", "ls-tree", "-r", "--name-only", a.git_ref, "contracts/"], cwd=ROOT, capture_output=True, text=True)
        existing = {os.path.relpath(f, ROOT) for f in new_files}
        for rel in listing.stdout.split():
            if rel.endswith(".yaml") and rel not in existing:
                gone = yaml.safe_load(subprocess.run(["git", "show", f"{a.git_ref}:{rel}"], cwd=ROOT, capture_output=True, text=True).stdout)
                old.append(gone)
    else:
        _, old = load(a.old)
        _, new = load(a.new)
    old_by, new_by = {c["id"]: c for c in old}, {c["id"]: c for c in new}
    bad = 0
    for cid in sorted(old_by.keys() - new_by.keys()):
        print(f"BREAKING  {cid}: contract removed")
        bad += 1
    for cid in sorted(new_by.keys() - old_by.keys()):
        print(f"new       {cid} v{new_by[cid]['version']}")
    for cid in sorted(old_by.keys() & new_by.keys()):
        d = C.diff_contracts(old_by[cid], new_by[cid])
        if not (d["breaking"] or d["non_breaking"] or d["policy_errors"]) and C.checksum(old_by[cid]) == C.checksum(new_by[cid]):
            continue
        print(f"{cid}: {old_by[cid]['version']} -> {new_by[cid]['version']}")
        for m in d["breaking"]: print(f"  BREAKING      {m}")
        for m in d["non_breaking"]: print(f"  non-breaking  {m}")
        for m in d["policy_errors"]:
            print(f"  POLICY ERROR  {m}")
            bad += 1
    print("version policy OK" if not bad else f"{bad} problem(s): bump versions (MAJOR for breaking changes) or revert")
    return 1 if bad else 0


def _summary(c):
    o = C.main_object(c)
    props = {p["name"]: (p.get("logicalType"), bool(p.get("required")), bool(p.get("primaryKey")), tuple(sorted(C.cprops(p).items()))) for p in o.get("properties", [])}
    rules = {q["name"]: (q.get("implementation") or {}).copy() for q in o.get("quality", []) or [] if q.get("name")}
    return props, rules, C.cprops(o), C.cprops(c)


def cmd_compare(a):
    """Compare contracts you wrote (--mine) with the reference set (--contracts). A learning aid: it never fails the build."""
    _, ref = load(a.contracts)
    _, mine = load(a.mine)
    ref_by, mine_by = {c["id"]: c for c in ref}, {c["id"]: c for c in mine}
    if not mine:
        print(f"no contracts found in {a.mine}")
        return 0
    for cid in sorted(ref_by):
        if cid not in mine_by:
            print(f"-- {cid}: not written yet")
            continue
        (rp, rr, rso, rco), (mp, mr, mso, mco) = _summary(ref_by[cid]), _summary(mine_by[cid])
        diffs = []
        for n in sorted(rp.keys() - mp.keys()): diffs.append(f"property missing: {n}")
        for n in sorted(mp.keys() - rp.keys()): diffs.append(f"extra property: {n}")
        for n in sorted(rp.keys() & mp.keys()):
            if rp[n][:3] != mp[n][:3]:
                diffs.append(f"property {n}: reference (type, required, primaryKey) = {rp[n][:3]}, yours = {mp[n][:3]}")
            elif rp[n][3] != mp[n][3]:
                diffs.append(f"property {n}: customProperties differ (reference {dict(rp[n][3])}, yours {dict(mp[n][3])})")
        for n in sorted(rr.keys() - mr.keys()): diffs.append(f"rule missing: {n}")
        for n in sorted(mr.keys() - rr.keys()): diffs.append(f"extra rule: {n}")
        for n in sorted(rr.keys() & mr.keys()):
            for k in ("rule_type", "columns", "expression", "action"):
                if str(rr[n].get(k)) != str(mr[n].get(k)):
                    diffs.append(f"rule {n}: {k} reference = {rr[n].get(k)!r}, yours = {mr[n].get(k)!r}")
        for k in sorted(set(rso) | set(mso)):
            if rso.get(k) != mso.get(k): diffs.append(f"schema customProperty {k}: reference {rso.get(k)!r}, yours {mso.get(k)!r}")
        for k in ("entity",):
            pass
        print(f"== {cid}: " + ("matches the reference" if not diffs else f"{len(diffs)} difference(s)"))
        for d in diffs: print("     " + d)
    for cid in sorted(mine_by.keys() - ref_by.keys()):
        print(f"++ {cid}: not in the reference set")
    return 0


def cmd_plan(a):
    sys.path.insert(0, str(ROOT))
    from lakehouse.plan import build_plan, explain
    _, contracts = load(a.contracts)
    cs = C.ContractSet(contracts)
    ordered, deps = build_plan(cs, a.product)
    for i, s in enumerate(ordered, 1):
        d = f" after {', '.join(deps[s.key])}" if deps[s.key] else ""
        print(f"{i:>2}. {s.key:<26}{d}\n      {explain(cs, s, a.product)}")
    return 0


def cmd_typed_sql(a):
    _, contracts = load(a.contracts)
    cs = C.ContractSet(contracts)
    sv, src = cs.entity("silver", a.entity, a.product), cs.entity("source", a.entity, a.product)
    if not sv or not src:
        print(f"need both a silver and a source contract for entity {a.entity!r}")
        return 1
    print(C.typed_select(sv, src))
    return 0


def cmd_export_rules(a):
    _, contracts = load(a.contracts)
    rules = C.compile_all_rules(C.ContractSet(contracts))
    cols = ["rule_id", "layer", "table_name", "rule_type", "columns", "expression", "action", "description", "owner", "active"]
    with open(a.out, "w", newline="") as fh:
        w = csv.writer(fh, quoting=csv.QUOTE_ALL)
        w.writerow(cols)
        w.writerows([[r[c] for c in cols] for r in rules])
    print(f"wrote {a.out}: {len(rules)} rules compiled from {len(contracts)} contracts")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--contracts", default=str(ROOT / "contracts"))
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("lint")
    b = sub.add_parser("bundle"); b.add_argument("--out", default=str(ROOT / "contracts_bundle.json"))
    sub.add_parser("stamp")
    d = sub.add_parser("diff"); d.add_argument("--git-ref"); d.add_argument("--old"); d.add_argument("--new")
    cm = sub.add_parser("compare"); cm.add_argument("--mine", required=True)
    pl = sub.add_parser("plan"); pl.add_argument("--product", default="fbl")
    ts = sub.add_parser("typed-sql"); ts.add_argument("entity"); ts.add_argument("--product", default="fbl")
    e = sub.add_parser("export-rules"); e.add_argument("--out", default=str(ROOT / "data" / "dq" / "dq_rules.csv"))
    args = ap.parse_args()
    sys.exit({"lint": cmd_lint, "bundle": cmd_bundle, "stamp": cmd_stamp, "diff": cmd_diff, "export-rules": cmd_export_rules, "compare": cmd_compare, "plan": cmd_plan, "typed-sql": cmd_typed_sql}[args.cmd](args))
