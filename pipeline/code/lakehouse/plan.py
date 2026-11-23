"""Derive the execution order from the contracts themselves.

Why derive instead of hard-code: the order in which tables must be built is already written down in the contracts
(`dependsOn`, and foreign-key `relationships`). If the order lives in the contracts, adding a table means adding a
contract, not editing a script. It is also what lets a Job later run independent steps in parallel.
"""
from dataclasses import dataclass

from lib.contracts import cprops, main_object


@dataclass(frozen=True)
class Step:
    kind: str            # bronze | silver | gold | gold-publish | semantic
    entity: str = ""

    @property
    def key(self):
        return f"{self.kind}:{self.entity}" if self.entity else self.kind


def _entity(c):
    return cprops(main_object(c)).get("entity", "")


def _parents(cs, product, layer, c):
    """Entities this contract points at through foreign-key relationships, within the same layer."""
    out = []
    for p in main_object(c).get("properties", []):
        for rel in p.get("relationships", []) or []:
            target_schema = str(rel.get("to", "")).split(".")[0]
            for other in cs.layer(layer, product):
                if main_object(other).get("name") == target_schema and other is not c:
                    out.append(_entity(other))
    return sorted(set(out))


def build_plan(cs, product="fbl"):
    """Return (steps in a valid execution order, {step key: [dependency keys]})."""
    deps, nodes = {}, []
    def add(step, d):
        nodes.append(step); deps[step.key] = sorted(set(d))

    source_entities = {_entity(c) for c in cs.layer("source", product)}
    silver_by_id = {c["id"]: c for c in cs.layer("silver", product)}
    for c in sorted(cs.layer("source", product), key=_entity):
        add(Step("bronze", _entity(c)), [])
    for c in sorted(cs.layer("silver", product), key=_entity):
        e = _entity(c)
        d = [f"silver:{p}" for p in _parents(cs, product, "silver", c)]
        if e in source_entities:
            d.append(f"bronze:{e}")
        add(Step("silver", e), d)
    gold = sorted(cs.layer("gold", product), key=_entity)
    for c in gold:
        e = _entity(c)
        d = [f"gold:{p}" for p in _parents(cs, product, "gold", c)]
        src = silver_by_id.get(cprops(c).get("dependsOn"))
        if src is not None:
            d.append(f"silver:{_entity(src)}")
        add(Step("gold", e), d)
    if gold:
        add(Step("gold-publish"), [f"gold:{_entity(c)}" for c in gold])
    for c in sorted(cs.layer("semantic", product), key=lambda x: x["id"]):
        add(Step("semantic", c["id"].split(".")[-1]), ["gold-publish"] if gold else [])

    present = {n.key for n in nodes}
    for k in deps:
        deps[k] = [x for x in deps[k] if x in present]
    # Kahn's algorithm, always picking the alphabetically first ready step so the order is stable run to run
    ordered, done, pending = [], set(), {n.key: n for n in nodes}
    while pending:
        ready = sorted(k for k in pending if all(d in done for d in deps[k]))
        if not ready:
            raise ValueError("cycle in the contract dependencies: " + ", ".join(sorted(pending)))
        k = ready[0]
        ordered.append(pending.pop(k)); done.add(k)
    return ordered, deps


def explain(cs, step, product="fbl"):
    """One sentence on what a step will do and check (shown by --plan-only)."""
    if step.kind == "bronze":
        c = cs.entity("source", step.entity, product)
        return f"read the {cprops(main_object(c))['landingFolder']} files as text; require the columns promised by {c['id']} v{c['version']}; not empty; write {main_object(c)['physicalName']}"
    if step.kind == "silver":
        c = cs.entity("silver", step.entity, product)
        n = sum(1 for q in main_object(c).get("quality", []) or [] if q.get("engine") == "fbl-dq-engine")
        return f"type and clean the bronze rows (SQL generated from {c['id']} v{c['version']}); apply {n} rules; clean rows -> {main_object(c)['physicalName']}, rejects -> governance.dq_quarantine"
    if step.kind == "gold":
        c = cs.entity("gold", step.entity, product)
        n = sum(1 for q in main_object(c).get("quality", []) or [] if q.get("engine") == "fbl-dq-engine")
        return f"build into the gold_stage schema from the transform SQL; audit with {n} assertions + row-count reconciliation + schema check ({c['id']} v{c['version']}); nothing is published yet"
    if step.kind == "gold-publish":
        return "only if every gold audit passed: copy the staged tables into gold (write-audit-PUBLISH)"
    if step.kind == "semantic":
        c = next(x for x in cs.layer("semantic", product) if x["id"].endswith("." + step.entity))
        return f"create the metric view from its definition; test the interface, behaviour tests and freshness promised by {c['id']} v{c['version']}"
    return ""
