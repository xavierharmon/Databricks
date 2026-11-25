"""Planning several products in one run (pure Python: no Spark session is started)."""
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "src"))
from lakehouse import plan as P  # noqa: E402
from lakehouse import runner  # noqa: E402
from lakehouse.context import RunContext  # noqa: E402
from lib.contracts import ContractSet  # noqa: E402


def mk(product, layer, entity, deps=None, fk_to=None):
    cid = f"{product}.{layer}.{entity}"
    props = [{"name": "k", "logicalType": "string"}]
    if fk_to:
        props[0]["relationships"] = [{"type": "foreignKey", "to": f"{fk_to}.k"}]
    cp = [{"property": "layer", "value": layer}]
    if deps:
        cp.append({"property": "dependsOn", "value": deps})
    return {"id": cid, "dataProduct": product, "customProperties": cp,
            "schema": [{"name": entity, "physicalName": f"{layer}.{product}_{entity}",
                        "customProperties": [{"property": "entity", "value": entity}], "properties": props}]}


def fbl_like():
    return [mk("fbl", "source", "team"), mk("fbl", "silver", "team", "fbl.source.team"), mk("fbl", "gold", "dim_team", "fbl.silver.team")]


def tix_like(extra=None):
    cs = [mk("tix", "source", "event"), mk("tix", "silver", "event", "tix.source.event"),
          mk("tix", "source", "participant"), mk("tix", "silver", "participant", "tix.source.participant"),
          mk("tix", "gold", "fact_x", ["tix.silver.event", "tix.silver.participant"])]
    return cs + (extra or [])


def keys(plan):
    return [s.key for s in plan[0]]


def test_one_product_is_exactly_the_old_plan():
    cs = ContractSet(fbl_like())
    assert keys(P.build_multi_plan(cs, ["fbl"])) == keys(P.build_plan(cs, "fbl"))
    assert "silver:team" in keys(P.build_multi_plan(cs, ["fbl"]))


def test_two_products_get_qualified_keys_and_keep_their_own_order():
    cs = ContractSet(fbl_like() + tix_like())
    ks = keys(P.build_multi_plan(cs, ["fbl", "tix"]))
    assert all(k.split(":")[0] in ("fbl", "tix") for k in ks)
    assert ks.index("fbl:bronze:team") < ks.index("fbl:silver:team") < ks.index("fbl:gold:dim_team")
    assert ks.index("tix:silver:event") < ks.index("tix:gold:fact_x") < ks.index("tix:gold-publish")


def test_gold_table_waits_for_every_silver_table_it_declares():
    cs = ContractSet(tix_like())
    order, deps = P.build_plan(cs, "tix")
    assert set(deps["gold:fact_x"]) == {"silver:event", "silver:participant"}


def test_cross_product_dependency_orders_steps_across_products():
    link = mk("tix", "silver", "event_participant", ["tix.source.event", "fbl.silver.team"])
    cs = ContractSet(fbl_like() + tix_like([link, mk("tix", "source", "event_participant")]))
    order, deps = P.build_multi_plan(cs, ["fbl", "tix"])
    ks = [s.key for s in order]
    assert "fbl:silver:team" in deps["tix:silver:event_participant"]
    assert ks.index("fbl:silver:team") < ks.index("tix:silver:event_participant")


def test_dependency_on_a_product_that_is_not_selected_is_ignored():
    link = mk("tix", "silver", "event_participant", ["fbl.silver.team"])
    cs = ContractSet(fbl_like() + tix_like([link]))
    order, deps = P.build_multi_plan(cs, ["tix", "fbl"][:1] * 1 + ["tix"][:0])  # only tix
    assert all(k.startswith(("bronze", "silver", "gold")) for k in deps)


def test_cycle_across_products_is_reported():
    a = mk("aa", "silver", "t1", ["bb.silver.t2"])
    b = mk("bb", "silver", "t2", ["aa.silver.t1"])
    with pytest.raises(ValueError, match="cycle"):
        P.build_multi_plan(ContractSet([a, b]), ["aa", "bb"])


def test_resolve_products_all_means_every_runnable_product():
    cs = ContractSet(fbl_like() + tix_like() + [mk("lahman", "semantic", "mv_batting")])
    assert runner.resolve_products("all", cs) == ["fbl", "tix"]
    assert runner.resolve_products("tix,fbl", cs) == ["tix", "fbl"]
    with pytest.raises(SystemExit):
        runner.resolve_products("nope", cs)


def test_each_step_runs_with_its_own_product_and_landing_folder():
    ctx = RunContext(catalog="c", run_id="r", pipeline_root="/p", data_root="/d", contracts_dir="/p/c", transforms_dir="/p/t",
                     definitions_dir="/p/d", landing_dir="/d/fbl", reports_dir="/d/reports", product="fbl")
    args = SimpleNamespace(landing_dir="")
    s = P.Step("bronze", "event", "tix")
    c2 = runner.step_ctx(ctx, args, ["fbl", "tix"], s)
    assert (c2.product, c2.landing_dir) == ("tix", "/d/tix") and ctx.product == "fbl"
    only = runner.step_ctx(ctx, SimpleNamespace(landing_dir="/custom"), ["fbl"], P.Step("bronze", "team"))
    assert (only.product, only.landing_dir) == ("fbl", "/custom")


def test_lint_rejects_the_same_entity_name_in_two_products():
    import copy
    import glob
    import yaml
    from lib.contracts import lint
    docs = [yaml.safe_load(open(f)) for f in glob.glob(str(ROOT / "contracts" / "**" / "fbl_*.yaml"), recursive=True)]
    assert not [f for f in lint(ContractSet(docs)) if f[0] == "error"]
    twin = copy.deepcopy(next(d for d in docs if d["id"] == "fbl.silver.team"))
    twin["id"], twin["dataProduct"] = "tix.silver.team", "tix"
    errs = [f for f in lint(ContractSet(docs + [twin])) if "unique across products" in f[2]]
    assert errs


def test_failure_policy_stop_skips_everything_isolate_skips_only_dependents():
    deps = {"silver:a": ["bronze:a"], "silver:b": ["bronze:b"], "gold:x": ["silver:a"], "gold-publish": ["gold:x"]}
    bad = {"bronze:a"}
    assert runner.blocked("silver:b", deps, bad, "stop", False, True) is True          # stop: unrelated step skipped too
    assert runner.blocked("silver:b", deps, bad, "isolate", False, True) is False      # isolate: unrelated step still runs
    assert runner.blocked("silver:a", deps, bad, "isolate", False, True) is True
    bad.add("silver:a")
    assert runner.blocked("gold:x", deps, bad, "isolate", False, True) is True         # skips travel down the chain
    bad.add("gold:x")
    assert runner.blocked("gold-publish", deps, bad, "isolate", False, True) is True
    assert runner.blocked("silver:b", deps, set(), "isolate", True, True) is True       # a failed setup or contracts step blocks everything
