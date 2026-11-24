"""Static tests for the data contracts (no Spark, no workspace). Run in CI on every pull request."""
import copy
import csv
import glob
import json
import re
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from lib import contracts as C  # noqa: E402

FILES = sorted(glob.glob(str(ROOT / "contracts/**/*.yaml"), recursive=True))


def load():
    return [yaml.safe_load(open(f)) for f in FILES]


def defs_parsed(cs):
    out = {}
    for c in cs.layer("semantic"):
        p = C.cprops(C.main_object(c))["definition"]
        out[p] = C.load_metric_view_definition((ROOT / p).read_text())
    return out


def by_id(cid):
    return copy.deepcopy(next(c for c in load() if c["id"] == cid))


def test_there_is_a_contract_for_every_layer_of_the_fbl_pipeline():
    cs = C.ContractSet(load())
    assert {C.cprops(C.main_object(c))["entity"] for c in cs.layer("source", "fbl")} == {"team", "player", "batting"}
    assert {C.cprops(C.main_object(c))["entity"] for c in cs.layer("silver", "fbl")} == {"team", "player", "batting"}
    assert {C.cprops(C.main_object(c))["entity"] for c in cs.layer("gold", "fbl")} == {"dim_team", "dim_player", "fact_batting"}
    assert {c["id"] for c in cs.layer("semantic")} >= {"fbl.semantic.mv_fbl_batting", "fbl.semantic.mv_fbl_roster",
                                                       "lahman.semantic.mv_batting", "lahman.semantic.mv_pitching",
                                                       "lahman.semantic.mv_team_season"}


def test_all_contracts_lint_clean_against_the_metric_view_definitions():
    cs = C.ContractSet(load())
    errors = [f for f in C.lint(cs, defs_parsed(cs)) if f[0] == "error"]
    assert not errors, errors


def test_rules_compiled_from_contracts_equal_the_legacy_csv():
    key = lambda r: (r["rule_id"], r["layer"], r["table_name"], r["rule_type"], r["columns"], r["expression"], r["action"],
                     r["description"], r["owner"])
    compiled = {key(r) for r in C.compile_all_rules(C.ContractSet(load()))}
    legacy = {key(r) for r in csv.DictReader(open(ROOT / "data/dq/dq_rules.csv"))}
    assert compiled == legacy, "contracts and data/dq/dq_rules.csv disagree: run `python tools/contract_tool.py export-rules`"


def test_typed_select_is_generated_from_the_contracts():
    cs = C.ContractSet(load())
    sql = C.typed_select(cs.entity("silver", "batting", "fbl"), cs.entity("source", "batting", "fbl"))
    assert "try_cast(NULLIF(trim(`ab`), '') AS INT) AS `ab`" in sql and "FROM bronze.fbl_batting" in sql and "_raw" in sql
    sql = C.typed_select(cs.entity("silver", "player", "fbl"), cs.entity("source", "player", "fbl"))
    assert "upper(NULLIF(trim(`bats`), ''))" in sql and "AS DATE" in sql


def test_bundle_roundtrip_and_notebook_embeds_the_current_contracts():
    cs = load()
    bundle = C.make_bundle(cs, {"x.sql": "select 1"})
    assert C.contracts_from_bundle(json.dumps(bundle)) == cs
    nb = (ROOT / "src/notebooks/fbl_dq_pipeline.py").read_text()
    for c in cs:
        assert C.checksum(c) in json.dumps(C.make_bundle([c])), "sanity"
    embedded = "".join(re.findall(r"^    '(.*)'$", nb.split("EMBEDDED_BUNDLE_JSON = (")[1].split(")\n")[0], re.M))
    assert {b["id"]: b["checksum"] for b in json.loads(embedded.encode().decode("unicode_escape"))["contracts"]} == \
           {c["id"]: C.checksum(c) for c in cs}, "notebook is stale: run python tools/build_fbl_notebook.py"


def test_discover_contracts_reads_bundle_and_yaml(tmp_path):
    cs = load()
    (tmp_path / "contracts_bundle.json").write_text(json.dumps(C.make_bundle(cs, {"a.sql": "s"})))
    got, defs, origin = C.discover_contracts(str(tmp_path))
    assert len(got) == len(cs) and defs == {"a.sql": "s"} and "bundle" in origin
    (tmp_path / "contracts_bundle.json").unlink()
    (tmp_path / "one.yaml").write_text(yaml.safe_dump(cs[0]))
    got, _, origin = C.discover_contracts(str(tmp_path))
    assert len(got) == 1 and "yaml" in origin
    assert C.discover_contracts(str(tmp_path / "missing")) is None


# ---------------------------------------------------------------- lint catches real mistakes
def lint_one(mutate, cid="fbl.silver.player"):
    cs = load()
    for c in cs:
        if c["id"] == cid:
            mutate(c)
    return C.lint(C.ContractSet(cs), defs_parsed(C.ContractSet(cs)))


def test_lint_required_column_without_a_rule():
    def m(c):
        for q in list(c["schema"][0]["quality"]):
            if q["name"] == "PLR-003":
                q["implementation"]["columns"] = ["player_id"]
    assert any("last_name is required but no not_null" in msg for s, i, msg in lint_one(m))


def test_lint_primary_key_without_unique_rule():
    def m(c):
        c["schema"][0]["quality"] = [q for q in c["schema"][0]["quality"] if q["name"] != "PLR-002"]
    assert any("primary key" in msg for s, i, msg in lint_one(m))


def test_lint_foreign_key_without_referential_rule():
    def m(c):
        c["schema"][0]["quality"] = [q for q in c["schema"][0]["quality"] if q["name"] != "PLR-010"]
    assert any("foreignKey" in msg for s, i, msg in lint_one(m))


def test_lint_gold_rules_must_fail_hard():
    def m(c):
        c["schema"][0]["quality"][1]["implementation"]["action"] = "warn"
    assert any("gold rules must use action 'fail'" in msg for s, i, msg in lint_one(m, "fbl.gold.dim_team"))


def test_lint_bad_version_status_and_duplicate_rule_ids():
    def m(c):
        c["version"] = "one"
        c["status"] = "live"
        c["schema"][0]["quality"].append(copy.deepcopy(c["schema"][0]["quality"][0]))
    msgs = [msg for s, i, msg in lint_one(m)]
    assert any("semantic" in x for x in msgs) and any("status" in x for x in msgs) and any("also used" in x for x in msgs)


def test_lint_semantic_unknown_measure_in_test():
    def m(c):
        c["schema"][0]["quality"][0]["implementation"]["measures"] = ["not_a_measure"]
    assert any("not_a_measure" in msg for s, i, msg in lint_one(m, "fbl.semantic.mv_fbl_batting"))


def test_formula_change_is_detected_and_stamp_fixes_it():
    cs = C.ContractSet(load())
    path = "sql/fbl/metric_views/mv_fbl_batting.sql"
    text = (ROOT / path).read_text().replace("SUM(h) / NULLIF(SUM(ab), 0)", "AVG(h) / NULLIF(AVG(ab), 0)")
    spec = C.load_metric_view_definition(text)
    defs = defs_parsed(cs); defs[path] = spec
    errors = [m for s, i, m in C.lint(cs, defs) if s == "error"]
    assert any("batting_avg: definition changed" in m for m in errors)
    stamped = C.stamp_expr_hashes((ROOT / "contracts/semantic/fbl_semantic_mv_fbl_batting.yaml").read_text(), spec)
    new = [yaml.safe_load(stamped) if c["id"] == "fbl.semantic.mv_fbl_batting" else c for c in load()]
    assert not [m for s, i, m in C.lint(C.ContractSet(new), defs) if s == "error"]


def test_removed_view_column_is_a_breaking_lint_error():
    cs = C.ContractSet(load())
    path = "sql/fbl/metric_views/mv_fbl_roster.sql"
    spec = C.load_metric_view_definition((ROOT / path).read_text())
    spec["measures"] = [m for m in spec["measures"] if m["name"] != "avg_height_in"]
    defs = defs_parsed(cs); defs[path] = spec
    errors = [m for s, i, m in C.lint(cs, defs) if s == "error"]
    assert any("avg_height_in" in m and "breaking removal" in m for m in errors)


# ---------------------------------------------------------------- diff: breaking vs non-breaking
def bump(c, v):
    c = copy.deepcopy(c); c["version"] = v; return c


def test_diff_classification_and_version_policy():
    old = by_id("fbl.gold.dim_team")
    # non-breaking: add a column and a rule, minor bump
    new = bump(old, "1.1.0")
    new["schema"][0]["properties"].append({"name": "stadium_capacity", "logicalType": "integer", "required": True, "description": "x"})
    d = C.diff_contracts(old, new)
    assert d["non_breaking"] and not d["breaking"] and not d["policy_errors"]
    # breaking: remove a column but only a minor bump -> policy error
    new = bump(old, "1.1.0")
    new["schema"][0]["properties"] = [p for p in new["schema"][0]["properties"] if p["name"] != "city"]
    d = C.diff_contracts(old, new)
    assert any("city" in m for m in d["breaking"]) and any("MAJOR" in m for m in d["policy_errors"])
    # same change with a major bump is allowed
    new = bump(new, "2.0.0")
    assert not C.diff_contracts(old, new)["policy_errors"]
    # type change, required relaxed, rule loosened, SLA loosened
    new = bump(old, "2.0.0")
    new["schema"][0]["properties"][4]["logicalType"] = "string"
    new["schema"][0]["properties"][1]["required"] = False
    new["schema"][0]["quality"][2]["implementation"]["action"] = "warn"
    new["slaProperties"][0]["value"] = 72
    d = C.diff_contracts(old, new)
    text = " | ".join(d["breaking"])
    assert "type changed" in text and "no longer required" in text and "loosened" in text and "freshness SLA loosened" in text
    # changed but version not bumped
    new = copy.deepcopy(old); new["description"]["purpose"] = "changed"
    assert C.diff_contracts(old, new)["policy_errors"]
    # formula change on a semantic contract is breaking
    o = by_id("fbl.semantic.mv_fbl_batting"); n = bump(o, "2.0.0")
    for p in n["schema"][0]["properties"]:
        if p["name"] == "ops_approx":
            for cp in p["customProperties"]:
                if cp["property"] == "exprHash": cp["value"] = "000000000000"
    assert any("formula" in m for m in C.diff_contracts(o, n)["breaking"])


def test_unchanged_contract_has_no_diff():
    c = by_id("fbl.silver.team")
    d = C.diff_contracts(c, copy.deepcopy(c))
    assert d == {"breaking": [], "non_breaking": [], "policy_errors": []}


def test_lint_referential_rule_without_a_declared_foreign_key():
    """The runner derives its build order from foreignKey relationships, so a referential rule that points at another
    contract's table without one must be an error (otherwise the order silently ignores the dependency)."""
    def m(c):
        for p in c["schema"][0]["properties"]:
            if p["name"] == "team_id":
                p.pop("relationships", None)
    msgs = [msg for s, i, msg in lint_one(m, "fbl.silver.player") if s == "error"]
    assert any("declares no foreignKey relationship" in x for x in msgs), msgs
