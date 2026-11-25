"""Tier A: the data-quality action policy. No Spark. Uses your real contracts' compiled rules.

The rules being protected:
  1. A downgrade (lets more bad data through) is never silent: reason, approver, expiry, MAJOR bump.
  2. An override applies only in the environment it names, and never when no environment is given.
  3. What the runner records is exactly what ran.
"""
import copy
import re
from datetime import date, datetime

import pytest

from lib import policy as P
from lib.contracts import compile_all_rules, parse_yaml

TODAY = date(2026, 11, 25)
NOW = datetime(2026, 11, 25, 9, 0, 0)


@pytest.fixture(scope="module")
def rules(contract_set):
    return compile_all_rules(contract_set)


def rule_with(rules, action, layer="silver", rule_type=None):
    return next(r for r in rules if r["layer"] == layer and r["action"] == action and (rule_type is None or r["rule_type"] == rule_type))


def pol(*overrides, version="1.0.0"):
    return {"version": version, "overrides": list(overrides)}


def ov(rule, action, env=("dev",), **kw):
    base = {"rule": rule["rule_id"] if isinstance(rule, dict) else rule, "env": list(env), "action": action,
            "reason": "test", "approved_by": "tester"}
    base.update(kw)
    return base


def errors(findings):
    return [f for f in findings if f[0] == "error"]


# ---- validation ------------------------------------------------------------------------------
def test_the_shipped_policy_file_is_valid(pipeline_root, rules):
    policy = P.load_policy(str(pipeline_root / "policy" / "dq_actions.yml"), parse_yaml)
    assert str(policy["version"]) == "1.0.0" and policy["overrides"] == []
    assert errors(P.validate(policy, rules, TODAY)) == []


def test_a_valid_downgrade_and_a_valid_upgrade_pass(rules):
    q, w = rule_with(rules, "quarantine"), rule_with(rules, "warn")
    ok = pol(ov(q, "warn", expires="2026-12-31"), ov(w, "quarantine"))
    assert errors(P.validate(ok, rules, TODAY)) == []


@pytest.mark.parametrize("mutate,expect", [
    (lambda o: o.pop("reason"), "reason is required"),
    (lambda o: o.pop("approved_by"), "approved_by is required"),
    (lambda o: o.pop("expires"), "needs `expires"),
    (lambda o: o.update(env=["qa"]), "env must be"),
    (lambda o: o.update(env=[]), "env must be"),
    (lambda o: o.update(action="ignore"), "action must be one of"),
    (lambda o: o.update(action="quarantine"), "does nothing"),
    (lambda o: o.update(rule="NOPE-999"), "no such rule"),
])
def test_bad_overrides_are_rejected_with_a_clear_reason(rules, mutate, expect):
    q = rule_with(rules, "quarantine")
    o = ov(q, "warn", expires="2026-12-31")
    mutate(o)
    msgs = " | ".join(m for _, _, m in errors(P.validate(pol(o), rules, TODAY)))
    assert expect in msgs, msgs or "no error was raised: the test would pass for the wrong reason"


def test_gold_and_mechanical_rules_cannot_be_overridden(rules):
    gold = rule_with(rules, "fail", layer="gold")
    dedupe = rule_with(rules, "dedupe")
    std = rule_with(rules, "standardize")
    for r, text in ((gold, "only silver"), (dedupe, "mechanical"), (std, "mechanical")):
        msgs = " | ".join(m for _, _, m in errors(P.validate(pol(ov(r, "warn", expires="2026-12-31")), rules, TODAY)))
        assert text in msgs, f"{r['rule_id']}: {msgs}"


def test_two_overrides_for_the_same_rule_and_env_are_rejected(rules):
    q = rule_with(rules, "quarantine")
    p = pol(ov(q, "warn", expires="2026-12-31"), ov(q, "fail"))
    assert any("more than one" in m for _, _, m in errors(P.validate(p, rules, TODAY)))


def test_expiry_warnings(rules):
    q = rule_with(rules, "quarantine")
    old = P.validate(pol(ov(q, "warn", expires="2026-01-01")), rules, TODAY)
    assert errors(old) == [] and any("expired" in m for _, _, m in old)
    far = P.validate(pol(ov(q, "warn", expires="2028-01-01")), rules, TODAY)
    assert errors(far) == [] and any("days away" in m for _, _, m in far)


def test_policy_version_must_be_semver(rules):
    assert any("version" in m for _, _, m in errors(P.validate(pol(version="v1"), rules, TODAY)))


# ---- applying --------------------------------------------------------------------------------
def test_override_applies_only_in_its_own_environment(rules):
    q = rule_with(rules, "quarantine")
    p = pol(ov(q, "warn", env=("dev",), expires="2026-12-31"))
    eff_dev, rows_dev = P.apply(rules, p, "dev", "r1", NOW, TODAY)
    eff_prod, rows_prod = P.apply(rules, p, "prod", "r1", NOW, TODAY)
    act = lambda rs: {r["rule_id"]: r["action"] for r in rs}   # noqa: E731
    assert act(eff_dev)[q["rule_id"]] == "warn" and len(rows_dev) == 1
    assert act(eff_prod)[q["rule_id"]] == q["action"] and rows_prod == []
    changed = {k for k, v in act(eff_dev).items() if v != act(rules)[k]}
    assert changed == {q["rule_id"]}, "an override must change exactly the rule it names"


def test_no_env_means_no_overrides(rules):
    q = rule_with(rules, "quarantine")
    eff, rows = P.apply(rules, pol(ov(q, "warn", expires="2026-12-31")), "", "r1", NOW, TODAY)
    assert eff == rules and rows == []


def test_an_expired_downgrade_is_ignored_but_still_logged(rules):
    q = rule_with(rules, "quarantine")
    eff, rows = P.apply(rules, pol(ov(q, "warn", expires="2026-11-24")), "dev", "r1", NOW, TODAY)
    assert {r["rule_id"]: r["action"] for r in eff}[q["rule_id"]] == q["action"], "an expired downgrade must fall back to the contract"
    assert len(rows) == 1 and rows[0][10] == "expired_ignored" and rows[0][8] == q["action"]


def test_apply_never_mutates_the_input_rules(rules):
    before = copy.deepcopy(rules)
    q = rule_with(rules, "quarantine")
    P.apply(rules, pol(ov(q, "warn", expires="2026-12-31")), "dev", "r1", NOW, TODAY)
    assert rules == before


def test_audit_row_matches_its_table_definition_and_tells_the_whole_story(rules):
    q = rule_with(rules, "quarantine")
    _, rows = P.apply(rules, pol(ov(q, "warn", expires="2026-12-31", ticket="T-1")), "dev", "run-9", NOW, TODAY)
    row = rows[0]
    assert len(row) == len(P.OVERRIDES_DDL.split(", ")), "row shape drifted from OVERRIDES_DDL: the Spark write would fail"
    cols = [c.split()[0] for c in P.OVERRIDES_DDL.split(", ")]
    rec = dict(zip(cols, row))
    assert (rec["run_id"], rec["env"], rec["declared_action"], rec["effective_action"], rec["direction"], rec["status"]) == \
           ("run-9", "dev", q["action"], "warn", "downgrade", "applied")
    assert (rec["reason"], rec["approved_by"], rec["ticket"], rec["expires"], rec["policy_version"]) == ("test", "tester", "T-1", "2026-12-31", "1.0.0")
    assert len(rec["policy_checksum"]) == 16


def test_overridable_actions_are_a_subset_of_the_engines_actions(pipeline_root):
    """dq_engine imports pyspark, so read its source instead of importing it."""
    src = (pipeline_root / "src" / "lib" / "dq_engine.py").read_text()
    engine = set(re.findall(r'"(\w+)"', re.search(r"^ACTIONS = \{(.*?)\}", src, re.M).group(1)))
    assert set(P.OVERRIDABLE) <= engine


# ---- change classification -------------------------------------------------------------------
def classify(rules, old, new):
    return P.classify(old, new, rules)


def test_adding_a_downgrade_is_breaking_and_needs_a_major_bump(rules):
    q = rule_with(rules, "quarantine")
    new = pol(ov(q, "warn", expires="2026-12-31"), version="1.1.0")
    changes, problems = classify(rules, pol(), new)
    assert any(b for b, _ in changes) and any("MAJOR" in m for m in problems)
    _, ok = classify(rules, pol(), pol(ov(q, "warn", expires="2026-12-31"), version="2.0.0"))
    assert ok == []


def test_adding_an_upgrade_is_not_breaking(rules):
    w = rule_with(rules, "warn")
    changes, problems = classify(rules, pol(), pol(ov(w, "quarantine"), version="1.1.0"))
    assert changes and not any(b for b, _ in changes) and problems == []


def test_removing_an_upgrade_is_breaking(rules):
    """The old policy was stricter than the contract; removing it lets more through than before."""
    w = rule_with(rules, "warn")
    changes, problems = classify(rules, pol(ov(w, "quarantine")), pol(version="1.0.1"))
    assert any(b for b, _ in changes) and any("MAJOR" in m for m in problems)


def test_removing_a_downgrade_is_not_breaking(rules):
    q = rule_with(rules, "quarantine")
    changes, problems = classify(rules, pol(ov(q, "warn", expires="2026-12-31")), pol(version="1.0.1"))
    assert changes and not any(b for b, _ in changes) and problems == []


def test_extending_a_downgrade_expiry_is_breaking(rules):
    q = rule_with(rules, "quarantine")
    old, new = pol(ov(q, "warn", expires="2026-12-31")), pol(ov(q, "warn", expires="2027-02-01"), version="1.0.1")
    changes, problems = classify(rules, old, new)
    assert any(b and "extended" in m for b, m in changes) and any("MAJOR" in m for m in problems)


def test_widening_a_downgrade_to_another_environment_is_breaking(rules):
    q = rule_with(rules, "quarantine")
    old, new = pol(ov(q, "warn", env=("dev",), expires="2026-12-31")), pol(ov(q, "warn", env=("dev", "prod"), expires="2026-12-31"), version="1.0.1")
    changes, _ = classify(rules, old, new)
    assert any(b and "[prod]" in m for b, m in changes)


def test_a_reason_only_edit_is_non_breaking_but_still_needs_a_version_change(rules):
    q = rule_with(rules, "quarantine")
    a = pol(ov(q, "warn", expires="2026-12-31"))
    b = copy.deepcopy(a)
    b["overrides"][0]["reason"] = "reworded"
    changes, problems = classify(rules, a, b)
    assert changes and not any(x for x, _ in changes)
    assert any("still 1.0.0" in m for m in problems), "an edit with no version change must be flagged"
    b["version"] = "1.0.1"
    assert classify(rules, a, b)[1] == []


def test_identical_policies_have_no_findings(rules):
    q = rule_with(rules, "quarantine")
    a = pol(ov(q, "warn", expires="2026-12-31"))
    assert classify(rules, a, copy.deepcopy(a)) == ([], [])
