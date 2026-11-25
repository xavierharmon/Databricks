"""Tier A: the execution plan. No Spark, no JVM. Reads your real contracts.

Why this matters: the plan is DERIVED from the contracts (dependsOn, alsoDependsOn, foreign keys). Nobody writes the order
by hand, so nothing but a test notices when a contract edit quietly reorders or breaks it.
"""
import copy

import pytest

from lakehouse import plan as planner
from lib.contracts import ContractSet, cprops, layer_of, main_object

BOTH = ["fbl", "tix"]
# Your recorded baseline: `--product fbl,tix --plan-only` prints 53 numbered lines = 50 plan steps + setup + contracts + report.
EXPECTED_STEPS = {"fbl": 12, "tix": 38, "fbl,tix": 50}


def keys(steps):
    return [s.key for s in steps]


def test_step_counts_match_the_recorded_baseline(contract_set):
    for products, expected in EXPECTED_STEPS.items():
        ordered, _ = planner.build_multi_plan(contract_set, products.split(","))
        assert len(ordered) == expected, f"{products}: {len(ordered)} steps, expected {expected}"


def test_every_dependency_runs_before_the_step_that_needs_it(contract_set):
    ordered, deps = planner.build_multi_plan(contract_set, BOTH)
    assert ordered, "an empty plan would make this test pass for the wrong reason"
    position = {k: i for i, k in enumerate(keys(ordered))}
    checked = 0
    for step_key, parents in deps.items():
        for parent in parents:
            assert parent in position, f"{step_key} depends on {parent}, which is not in the plan"
            assert position[parent] < position[step_key], f"{parent} must run before {step_key}"
            checked += 1
    assert checked > 20, "suspiciously few dependency edges were checked"


def test_known_chains_are_in_layer_order(contract_set):
    k = keys(planner.build_multi_plan(contract_set, BOTH)[0])
    assert k.index("fbl:bronze:team") < k.index("fbl:silver:team") < k.index("fbl:gold:dim_team") < k.index("fbl:gold-publish")
    assert k.index("tix:gold-publish") == max(i for i, x in enumerate(k) if x.startswith("tix:") and not x.startswith("tix:semantic"))


def test_gold_built_from_gold_waits_for_its_parent(contract_set):
    """fact_purchase_journey reads customer_person and fact_event_visit; those must be built (and staged) first."""
    k = keys(planner.build_multi_plan(contract_set, BOTH)[0])
    journey = k.index("tix:gold:fact_purchase_journey")
    assert k.index("tix:gold:customer_person") < journey
    assert k.index("tix:gold:fact_event_visit") < journey


def test_publish_comes_after_every_gold_build_of_its_product(contract_set):
    k = keys(planner.build_multi_plan(contract_set, BOTH)[0])
    for p in BOTH:
        builds = [i for i, x in enumerate(k) if x.startswith(f"{p}:gold:")]
        assert builds, f"no gold builds found for {p}"
        assert k.index(f"{p}:gold-publish") > max(builds)


def test_plan_is_stable_run_to_run(contract_set):
    assert keys(planner.build_multi_plan(contract_set, BOTH)[0]) == keys(planner.build_multi_plan(contract_set, BOTH)[0])


def test_entity_names_are_unique_across_products(contract_set):
    """A design rule you had to remember. Stage tables and step keys assume it."""
    seen = {}
    for c in contract_set.contracts:
        layer = layer_of(c)
        if layer not in ("source", "silver", "gold"):
            continue
        ent = cprops(main_object(c)).get("entity")
        key = (layer, ent)
        assert key not in seen, f"entity {ent!r} at layer {layer} is used by both {seen[key]} and {c['dataProduct']}"
        seen[key] = c["dataProduct"]
    assert len(seen) > 30


def _tiny(contract_id, layer, entity, depends_on):
    return {"id": contract_id, "dataProduct": "t", "version": "1.0.0",
            "customProperties": [{"property": "layer", "value": layer}, {"property": "dependsOn", "value": depends_on}],
            "schema": [{"name": entity, "physicalName": f"{layer}.{entity}",
                        "customProperties": [{"property": "entity", "value": entity}]}]}


def test_a_dependency_cycle_fails_loudly_instead_of_hanging():
    cs = ContractSet([_tiny("t.gold.a", "gold", "a", ["t.gold.b"]), _tiny("t.gold.b", "gold", "b", ["t.gold.a"])])
    with pytest.raises(ValueError, match="cycle"):
        planner.build_plan(cs, "t")


def test_removing_a_real_dependency_changes_the_plan(contract_set):
    """Sensitivity check: prove the ordering test CAN fail. Drop a parent and the order is no longer forced."""
    broken = copy.deepcopy(contract_set.contracts)
    for c in broken:
        if c["id"] == "tix.gold.fact_purchase_journey":
            for p in c["customProperties"]:
                if p["property"] == "dependsOn":
                    p["value"] = [v for v in p["value"] if v != "tix.gold.customer_person"]
    _, deps_before = planner.build_multi_plan(contract_set, BOTH)
    _, deps_after = planner.build_multi_plan(ContractSet(broken), BOTH)
    assert "tix:gold:customer_person" in deps_before["tix:gold:fact_purchase_journey"]
    assert "tix:gold:customer_person" not in deps_after["tix:gold:fact_purchase_journey"]


def _dep_ids(c):
    out = []
    for k in ("dependsOn", "alsoDependsOn"):
        v = cprops(c).get(k)
        out += [] if not v else ([v] if isinstance(v, str) else list(v))
    return out


def test_every_declared_dependency_becomes_an_edge(contract_set):
    """The ordering test above only checks edges the planner chose to create. This one recomputes the expected edges straight
    from the contract YAML, so a planner that silently DROPS a dependency is caught (a mutation check proved the gap)."""
    _, deps = planner.build_multi_plan(contract_set, BOTH)
    expected = []
    for c in contract_set.contracts:
        p = c.get("dataProduct")
        if p not in BOTH:
            continue
        layer, ent = layer_of(c), cprops(main_object(c)).get("entity")
        if layer == "silver" and contract_set.entity("source", ent, p):
            expected.append((f"{p}:silver:{ent}", f"{p}:bronze:{ent}"))
        if layer == "silver":          # cross-product edges (tix participants need fbl teams) come from alsoDependsOn
            for dep_id in _dep_ids(c):
                other = contract_set.by_id.get(dep_id)
                if other is not None and other.get("dataProduct") in BOTH and other.get("dataProduct") != p:
                    expected.append((f"{p}:silver:{ent}", f"{other['dataProduct']}:{layer_of(other)}:{cprops(main_object(other))['entity']}"))
        if layer == "gold":
            for dep_id in _dep_ids(c):
                other = contract_set.by_id.get(dep_id)
                if other is not None and layer_of(other) in ("silver", "gold") and other.get("dataProduct") in BOTH:
                    expected.append((f"{p}:gold:{ent}", f"{other['dataProduct']}:{layer_of(other)}:{cprops(main_object(other))['entity']}"))
    assert len(expected) > 30, "too few edges derived from the contracts; the test is not looking at enough"
    missing = [(a, b) for a, b in expected if b not in deps[a]]
    assert not missing, f"dependencies declared in contracts but absent from the plan: {missing[:5]}"
