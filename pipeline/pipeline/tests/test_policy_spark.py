"""Tier B (local only): an override really reaches the rules the engine reads, and every decision is recorded.

This test runs the real governance setup and publish_contracts against a throwaway local Spark. It is SKIPPED inside Databricks
on purpose: publish_contracts overwrites governance.dq_rules, and a test must never do that to your real workspace.
"""
import os
from datetime import date, timedelta

import pytest

pytest.importorskip("pyspark")
pytestmark = [pytest.mark.spark,
              pytest.mark.skipif("DATABRICKS_RUNTIME_VERSION" in os.environ or os.path.exists("/databricks"),
                                 reason="writes governance.* tables: local Spark only")]

from lakehouse import governance as G          # noqa: E402
from lakehouse.context import RunContext       # noqa: E402
from lakehouse.contract_store import load_definitions   # noqa: E402
from lib.contracts import compile_all_rules    # noqa: E402


def flat_definitions(root, dest):
    """What tools/sync_to_volume.py does for the runner: every metric view SQL file, flattened into one folder."""
    dest.mkdir(exist_ok=True)
    for pattern in ("sql/metric_views/*.sql", "sql/fbl/metric_views/*.sql"):
        for f in root.glob(pattern):
            (dest / f.name).write_text(f.read_text())
    return load_definitions(str(dest))


def make_ctx(spark, contract_set, root, run_id, env, policy_path, definitions):
    return RunContext(catalog="test", run_id=run_id, pipeline_root=str(root), data_root=str(root / "data"),
                      contracts_dir=str(root / "contracts"), transforms_dir=str(root / "transforms"),
                      definitions_dir=str(root / "definitions"), landing_dir=str(root / "data" / "fbl"),
                      reports_dir=str(root / "data" / "reports"), local_mode=True, spark=spark, contracts=contract_set,
                      definitions=definitions, env=env, policy_path=str(policy_path), log=lambda *_: None)


def test_override_is_applied_recorded_and_scoped(spark, contract_set, pipeline_root, tmp_path):
    spark.sql("DROP SCHEMA IF EXISTS governance CASCADE")
    rule = next(r for r in compile_all_rules(contract_set) if r["layer"] == "silver" and r["action"] == "quarantine")
    expires = (date.today() + timedelta(days=30)).isoformat()
    policy = tmp_path / "dq_actions.yml"
    policy.write_text(f"""version: 2.0.0
overrides:
  - rule: {rule['rule_id']}
    env: [dev]
    action: warn
    reason: test downgrade
    approved_by: tester
    expires: {expires}
""")
    defs = flat_definitions(pipeline_root, tmp_path / "definitions")
    action_in_rules = lambda: spark.table("governance.dq_rules").where(f"rule_id = '{rule['rule_id']}'").first()["action"]   # noqa: E731

    # 1. dev: the engine's rules table carries the EFFECTIVE action, and the decision is on record
    ctx = make_ctx(spark, contract_set, pipeline_root, "run-dev", "dev", policy, defs)
    G.ensure_objects(ctx)
    G.publish_contracts(ctx)
    assert action_in_rules() == "warn"
    ov = spark.table("governance.dq_action_overrides").where("run_id = 'run-dev'").collect()
    assert len(ov) == 1
    o = ov[0].asDict()
    assert (o["rule_id"], o["declared_action"], o["effective_action"], o["direction"], o["status"], o["env"]) == \
           (rule["rule_id"], "quarantine", "warn", "downgrade", "applied", "dev")
    assert (o["reason"], o["approved_by"], o["expires"], o["policy_version"]) == ("test downgrade", "tester", expires, "2.0.0")
    assert spark.table("governance.dq_policy_history").where(f"checksum = '{o['policy_checksum']}'").count() == 1

    # 2. same policy, run for prod: the contract's action is back and nothing is recorded as applied
    G.publish_contracts(make_ctx(spark, contract_set, pipeline_root, "run-prod", "prod", policy, defs))
    assert action_in_rules() == "quarantine"
    assert spark.table("governance.dq_action_overrides").where("run_id = 'run-prod'").count() == 0

    # 3. no --env: also the contract's action (a run that forgot where it is gets no leniency)
    G.publish_contracts(make_ctx(spark, contract_set, pipeline_root, "run-noenv", "", policy, defs))
    assert action_in_rules() == "quarantine"

    # 4. the same policy is not recorded twice in the history
    assert spark.table("governance.dq_policy_history").count() == 1

    # 5. an invalid policy stops the run before any rules are written
    policy.write_text("version: 2.0.1\noverrides:\n  - {rule: NOPE-1, env: [dev], action: warn, reason: x, approved_by: y}\n")
    with pytest.raises(Exception, match="policy error"):
        G.publish_contracts(make_ctx(spark, contract_set, pipeline_root, "run-bad", "dev", policy, defs))
