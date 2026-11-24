"""Data contracts: load, lint, compile, bundle, diff.  Pure Python (no Spark), so it runs in CI, locally and in Databricks.

Contracts are YAML modeled on the Open Data Contract Standard v3.1 (apiVersion/kind/version, schema[] objects with
properties and nested quality, slaProperties, extensions ONLY via customProperties). Anything specific to this project lives
in customProperties or in `type: custom` quality rules, so the files stay valid ODCS-shaped documents. Verify field names
against the ODCS version your organisation standardises on; this module is the single place that knows the mapping.

Contract layers (customProperties.layer):
  source    the file/table as delivered (bronze).  properties = columns that must be present, all strings
  silver    typed + cleaned.                       drives typed SELECT generation and silver DQ rules
  gold      the published star schema.             drives schema conformance and gold assertions
  semantic  a metric view.                         interface (fields/measures), golden tests, SLA, definition hash

Runtime flow:  YAML (git) -> lint -> bundle JSON  -> publish to governance.data_contracts -> pipeline reads the registry.
"""
import glob
import hashlib
import json
import os
import re

try:                                    # PyYAML is optional at runtime: Databricks can consume the JSON bundle instead
    import yaml
except ImportError:                     # pragma: no cover
    yaml = None

LAYERS = {"source", "silver", "gold", "semantic"}
LOGICAL_TYPES = {"string", "integer", "number", "boolean", "date", "timestamp"}
STATUSES = {"draft", "active", "deprecated"}
ENGINE_DQ = "fbl-dq-engine"
ENGINE_SEM = "fbl-semantic-checks"
SEMVER = re.compile(r"^\d+\.\d+\.\d+$")
RULE_TYPES = {"not_null", "allowed_values", "range", "custom", "referential", "exact_duplicate", "unique", "unique_strict",
              "standardized", "no_nulls"}
ACTIONS = {"dedupe", "standardize", "quarantine", "warn", "fail"}
TEST_TYPES = {"reconcile", "expect", "bounds"}
STRICTNESS = {"fail": 4, "quarantine": 3, "dedupe": 2, "standardize": 1, "warn": 0}


# --------------------------------------------------------------------------- loading
def parse_yaml(text: str) -> dict:
    if yaml is None:
        raise ImportError("PyYAML is not installed. In a notebook run `%pip install pyyaml`, or upload the JSON bundle "
                          "(python tools/contract_tool.py bundle) instead of the YAML files.")
    return yaml.safe_load(text)


def checksum(contract: dict) -> str:
    return hashlib.sha256(json.dumps(contract, sort_keys=True).encode()).hexdigest()[:16]


def make_bundle(contracts: list, definitions: dict = None) -> dict:
    """One JSON file holding every contract plus the metric-view SQL they point at: the easiest thing to upload by hand."""
    return {"bundleVersion": 1, "contracts": [{"id": c["id"], "checksum": checksum(c), "contract": c} for c in contracts],
            "definitions": definitions or {}}


def discover_contracts(directory: str):
    """Find contracts in a folder (e.g. a Volume): contracts_bundle.json wins; otherwise *.yaml files (needs PyYAML).
    Returns (contracts, definitions {basename: sql text}, origin) or None if nothing is there."""
    bpath = os.path.join(directory, "contracts_bundle.json")
    if os.path.exists(bpath):
        with open(bpath) as fh:
            b = json.load(fh)
        return contracts_from_bundle(b), b.get("definitions", {}), f"bundle {bpath}"
    files = sorted(glob.glob(os.path.join(directory, "**", "*.y*ml"), recursive=True))
    if not files:
        return None
    contracts = []
    for f in files:
        with open(f) as fh:
            contracts.append(parse_yaml(fh.read()))
    defs = {}
    for f in glob.glob(os.path.join(directory, "**", "definitions", "*.sql"), recursive=True):
        with open(f) as fh:
            defs[os.path.basename(f)] = fh.read()
    return contracts, defs, f"{len(files)} yaml files in {directory}"


def contracts_from_bundle(bundle) -> list:
    if isinstance(bundle, str):
        bundle = json.loads(bundle)
    return [b["contract"] for b in bundle["contracts"]]


def cprops(obj: dict) -> dict:
    """customProperties list -> dict."""
    return {p["property"]: p.get("value") for p in (obj or {}).get("customProperties", []) or []}


def layer_of(c: dict) -> str:
    return cprops(c).get("layer", "")


def main_object(c: dict) -> dict:
    return c["schema"][0]


def render(text: str, catalog: str) -> str:
    return re.sub(r"\{\{\s*catalog\s*\}\}", catalog, text)


class ContractSet:
    def __init__(self, contracts: list):
        self.contracts = list(contracts)
        self.by_id = {c["id"]: c for c in contracts}

    def layer(self, layer: str, product: str = None) -> list:
        return [c for c in self.contracts if layer_of(c) == layer and (product is None or c.get("dataProduct") == product)]

    def entity(self, layer: str, entity: str, product: str = None):
        for c in self.layer(layer, product):
            if cprops(main_object(c)).get("entity") == entity:
                return c
        return None

    def __len__(self):
        return len(self.contracts)


# --------------------------------------------------------------------------- compile
def compile_rules(contract: dict) -> list:
    """Silver/gold contract -> rows for governance.dq_rules (same shape the engine already consumes)."""
    layer = layer_of(contract)
    if layer not in ("silver", "gold"):
        return []
    obj = main_object(contract)
    entity = cprops(obj)["entity"]
    rules = []
    for q in obj.get("quality", []) or []:
        if q.get("type") == "custom" and q.get("engine") == ENGINE_DQ:
            i = q["implementation"]
            cols = i.get("columns", "")
            rules.append({
                "rule_id": q["name"], "layer": layer, "table_name": entity, "rule_type": i["rule_type"],
                "columns": ",".join(cols) if isinstance(cols, list) else str(cols),
                "expression": str(i.get("expression", "") or ""), "action": i["action"],
                "description": q.get("description", ""), "owner": i.get("owner", "Data Engineering"), "active": "Y"})
    return rules


def compile_all_rules(cs: ContractSet) -> list:
    return [r for c in cs.contracts for r in compile_rules(c)]


def expected_columns(source_contract: dict) -> list:
    return [p["name"] for p in main_object(source_contract)["properties"]]


def typed_select(silver: dict, source: dict) -> str:
    """Generate the silver typing SELECT from the contracts: trim, blank -> NULL, standardize, try_cast."""
    cast = {"integer": "INT", "number": "DOUBLE", "date": "DATE", "boolean": "BOOLEAN", "timestamp": "TIMESTAMP"}
    sobj, bobj = main_object(silver), main_object(source)
    cols = []
    for p in sobj["properties"]:
        cp = cprops(p)
        src = cp.get("sourceColumn", p["name"])
        base = f"NULLIF(trim(`{src}`), '')"
        t = p["logicalType"]
        if t == "string":
            std = cp.get("standardize")
            expr = {"upper": f"upper({base})", "lower": f"lower({base})", "initcap": f"initcap({base})"}.get(std, base)
        else:
            expr = f"try_cast({base} AS {cast[t]})"
        cols.append(f"{expr} AS `{p['name']}`")
    raw = ", ".join(f"t.`{p['name']}`" for p in bobj["properties"])
    cols += ["_row_id", f"to_json(struct({raw})) AS _raw", "_ingested_at"]
    return "SELECT\n  " + ",\n  ".join(cols) + f"\nFROM {bobj['physicalName']} t"


# --------------------------------------------------------------------------- metric view definitions (for semantic contracts)
def load_metric_view_definition(sql_text: str, catalog: str = "cat") -> dict:
    """Pull the YAML out of a CREATE VIEW ... WITH METRICS ... AS $$ yaml $$ file."""
    parts = render(sql_text, catalog).split("$$")
    if len(parts) < 3:
        raise ValueError("no $$ ... $$ YAML block found")
    return parse_yaml(parts[1])


def expr_hash(expr: str) -> str:
    return hashlib.sha256(" ".join(str(expr).split()).encode()).hexdigest()[:12]


def definition_hashes(spec: dict) -> dict:
    out = {}
    for f in spec.get("fields", spec.get("dimensions", [])) or []:
        out[f["name"]] = expr_hash(f["expr"])
    for m in spec.get("measures", []) or []:
        out[m["name"]] = expr_hash(m["expr"])
    return out


def stamp_expr_hashes(yaml_text: str, spec: dict) -> str:
    """Rewrite each property's exprHash in a semantic contract (text patch, keeps comments and layout)."""
    for name, h in definition_hashes(spec).items():
        block = re.compile(rf"(\n[ ]*- name: {re.escape(name)}\n(?:(?![ ]*- name: ).*\n)*?[ ]*- property: exprHash\n[ ]*value: )[0-9a-f]+")
        yaml_text = block.sub(lambda m: m.group(1) + h, yaml_text, count=1)
    return yaml_text


# --------------------------------------------------------------------------- lint
def lint(cs: ContractSet, definitions: dict = None) -> list:
    """Return [(severity, contract_id, message)]. severity is 'error' or 'warn'. definitions: {path: parsed metric-view spec}."""
    out, rule_ids = [], {}
    def err(c, m): out.append(("error", c.get("id", "?"), m))
    def warn(c, m): out.append(("warn", c.get("id", "?"), m))

    ids = [c.get("id") for c in cs.contracts]
    for i in {x for x in ids if ids.count(x) > 1}:
        out.append(("error", i, "duplicate contract id"))

    for c in cs.contracts:
        for k in ("apiVersion", "kind", "id", "version", "status", "schema", "dataProduct"):
            if k not in c:
                err(c, f"missing required key {k!r}")
        if "schema" not in c or not c["schema"]:
            continue
        cp = cprops(c)
        layer = cp.get("layer", "")
        if c.get("kind") != "DataContract":
            err(c, "kind must be DataContract")
        if layer not in LAYERS:
            err(c, f"customProperties.layer must be one of {sorted(LAYERS)}"); continue
        if c.get("status") not in STATUSES:
            err(c, f"status must be one of {sorted(STATUSES)}")
        if not SEMVER.match(str(c.get("version", ""))):
            err(c, "version must be semantic (1.2.3)")
        if c.get("status") == "deprecated" and not cp.get("replacedBy"):
            warn(c, "deprecated contract should name customProperties.replacedBy")
        if not (c.get("description") or {}).get("purpose"):
            warn(c, "description.purpose is empty")
        if not cp.get("owner"):
            err(c, "customProperties.owner is required")
        parts = str(c["id"]).split(".")
        if len(parts) != 3 or parts[0] != c.get("dataProduct") or parts[1] != layer:
            err(c, "id must look like <dataProduct>.<layer>.<name>")

        obj = main_object(c)
        ocp = cprops(obj)
        if not obj.get("physicalName"):
            err(c, "schema[0].physicalName is required")
        names = [p.get("name") for p in obj.get("properties", [])]
        if not names:
            err(c, "schema[0].properties is empty")
        if len(names) != len(set(names)):
            err(c, "duplicate property names")
        for p in obj.get("properties", []):
            if p.get("logicalType") not in LOGICAL_TYPES:
                err(c, f"property {p.get('name')}: logicalType must be one of {sorted(LOGICAL_TYPES)}")
            if not p.get("description"):
                warn(c, f"property {p.get('name')}: missing description")

        # quality rules
        rules = []
        for q in obj.get("quality", []) or []:
            if q.get("type") == "custom" and q.get("engine") == ENGINE_DQ:
                i = q.get("implementation") or {}
                rid = q.get("name", "")
                if not rid:
                    err(c, "quality rule without name (rule id)"); continue
                if rid in rule_ids:
                    err(c, f"rule id {rid} also used in {rule_ids[rid]}")
                rule_ids[rid] = c["id"]
                if i.get("rule_type") not in RULE_TYPES:
                    err(c, f"{rid}: unknown rule_type {i.get('rule_type')!r}")
                if i.get("action") not in ACTIONS:
                    err(c, f"{rid}: unknown action {i.get('action')!r}")
                if layer == "gold" and i.get("action") != "fail":
                    err(c, f"{rid}: gold rules must use action 'fail'")
                if i.get("rule_type") in {"allowed_values", "range", "custom", "referential"} and not i.get("expression"):
                    err(c, f"{rid}: {i.get('rule_type')} needs an expression")
                if not q.get("description"):
                    warn(c, f"{rid}: missing description")
                rules.append(i)
                cols = i.get("columns", "")
                cols = cols if isinstance(cols, list) else [x.strip() for x in str(cols).split(",") if x.strip()]
                for col in cols:
                    if col != "*" and col not in names:
                        err(c, f"{rid}: column {col!r} is not a property of this schema")
            elif q.get("type") == "custom" and q.get("engine") == ENGINE_SEM:
                if layer != "semantic":
                    err(c, f"{q.get('name')}: semantic tests belong in semantic contracts")
            elif q.get("type") == "library" and q.get("metric") == "rowCount":
                pass
            else:
                warn(c, f"quality item {q.get('name') or q.get('metric') or '?'} is not enforced by this toolchain")

        if layer in ("silver", "gold"):
            if not ocp.get("entity"):
                err(c, "schema[0].customProperties.entity is required")
            covered_nn = set()
            keys_covered = []
            for i in rules:
                cols = i.get("columns", "")
                cols = cols if isinstance(cols, list) else [x.strip() for x in str(cols).split(",") if x.strip()]
                if i.get("rule_type") == "not_null":
                    covered_nn |= set(cols)
                if i.get("rule_type") == "no_nulls":
                    covered_nn |= set(names) if cols == ["*"] or not cols else set(cols)
                if i.get("rule_type") in ("unique", "unique_strict"):
                    keys_covered.append(set(cols))
            for p in obj.get("properties", []):
                if p.get("required") and p["name"] not in covered_nn:
                    err(c, f"property {p['name']} is required but no not_null/no_nulls rule covers it")
                if not p.get("required") and p["name"] in covered_nn:
                    warn(c, f"property {p['name']} is checked as non-null but not marked required")
            pk = {p["name"] for p in obj.get("properties", []) if p.get("primaryKey")}
            if pk and pk not in keys_covered:
                err(c, f"primary key {sorted(pk)} has no matching unique/unique_strict rule")
            for p in obj.get("properties", []):
                for r in p.get("relationships", []) or []:
                    has_ref = any(i.get("rule_type") == "referential" and p["name"] in str(i.get("columns", "")) for i in rules)
                    if not has_ref:
                        err(c, f"property {p['name']} has a foreignKey relationship but no referential rule")
            # the other direction: a referential rule that points at another contract's table must be declared as a foreignKey
            # relationship, because the runner derives its execution order from the relationships
            by_physical = {main_object(o).get("physicalName"): o for o in cs.contracts if o.get("schema")}
            for i in rules:
                if i.get("rule_type") != "referential":
                    continue
                ref_table = str(i.get("expression", "")).rsplit(".", 1)[0]
                other = by_physical.get(ref_table)
                if other is None or other is c:
                    continue
                cols = i.get("columns", "")
                col = (cols[0] if isinstance(cols, list) else str(cols).split(",")[0]).strip()
                prop = next((p for p in obj.get("properties", []) if p["name"] == col), None)
                target = main_object(other).get("name")
                declared = prop is not None and any(str(r.get("to", "")).split(".")[0] == target for r in prop.get("relationships", []) or [])
                if not declared:
                    err(c, f"a referential rule checks {col} against {ref_table} ({other['id']}) but property {col} declares no foreignKey "
                           f"relationship to {target}. Add one: the runner derives the build order from relationships")
            if layer == "silver":
                src_id = cp.get("dependsOn") or ocp.get("dependsOn")
                src = cs.by_id.get(src_id)
                if not src:
                    err(c, f"dependsOn {src_id!r} is not a known contract")
                else:
                    src_names = {p["name"] for p in main_object(src)["properties"]}
                    for p in obj.get("properties", []):
                        sc = cprops(p).get("sourceColumn", p["name"])
                        if sc not in src_names:
                            err(c, f"property {p['name']}: source column {sc!r} not in {src_id}")
            if layer == "gold" and ocp.get("reconcilesTo") and not ocp["reconcilesTo"].startswith("silver."):
                err(c, "reconcilesTo should name a silver table")
        if layer == "source":
            for p in obj.get("properties", []):
                if p.get("logicalType") != "string":
                    err(c, f"source property {p['name']} must be string (typing happens in silver)")
            if not cprops(obj).get("landingFolder"):
                err(c, "source schema needs customProperties.landingFolder")

        if layer == "semantic":
            kinds = {p["name"]: cprops(p).get("kind") for p in obj.get("properties", [])}
            for n, k in kinds.items():
                if k not in ("field", "measure"):
                    err(c, f"property {n}: customProperties.kind must be field or measure")
            if not ocp.get("definition"):
                err(c, "schema[0].customProperties.definition (path to the metric view SQL) is required")
            if not any(s.get("property") == "freshness" for s in c.get("slaProperties", []) or []):
                warn(c, "no freshness SLA declared")
            for dep in ocp.get("dependsOn", []) or []:
                if dep not in cs.by_id and dep.split(".")[0] == c.get("dataProduct"):
                    warn(c, f"dependsOn {dep} has no contract")
            measures = {n for n, k in kinds.items() if k == "measure"}
            fields = {n for n, k in kinds.items() if k == "field"}
            for q in obj.get("quality", []) or []:
                if q.get("engine") != ENGINE_SEM:
                    continue
                i = q.get("implementation") or {}
                if i.get("test_type") not in TEST_TYPES:
                    err(c, f"{q.get('name')}: test_type must be one of {sorted(TEST_TYPES)}"); continue
                for d in i.get("dimensions", []) or []:
                    if d not in fields:
                        err(c, f"{q.get('name')}: dimension {d!r} is not a field of this contract")
                ms = list(i.get("bounds", {}).keys()) if i["test_type"] == "bounds" else (i.get("measures", []) or [])
                for m in ms:
                    if m not in measures:
                        err(c, f"{q.get('name')}: measure {m!r} is not a measure of this contract")
                if i["test_type"] == "reconcile" and not i.get("compare_sql"):
                    err(c, f"{q.get('name')}: reconcile needs compare_sql")
                if i["test_type"] == "expect" and not i.get("expect"):
                    err(c, f"{q.get('name')}: expect needs expected rows")
            if definitions is not None:
                spec = definitions.get(ocp.get("definition")) or definitions.get(os.path.basename(str(ocp.get("definition"))))
                if spec is None:
                    err(c, f"definition file {ocp.get('definition')!r} not found")
                else:
                    live = definition_hashes(spec)
                    for n in kinds:
                        if n not in live:
                            err(c, f"{n} is in the contract but not in the metric view definition (breaking removal)")
                    for n in live:
                        if n not in kinds:
                            warn(c, f"{n} exists in the metric view but is not in the contract (undocumented)")
                    for p in obj.get("properties", []):
                        want = cprops(p).get("exprHash")
                        if p["name"] in live and want and want != live[p["name"]]:
                            err(c, f"{p['name']}: definition changed (hash {live[p['name']]} != contract {want}). "
                                   "Bump the contract version (major if consumers are affected) and run `contract_tool.py stamp`")
    return out


# --------------------------------------------------------------------------- diff (breaking-change detection)
def diff_contracts(old: dict, new: dict) -> dict:
    """Compare two versions of the SAME contract. Returns {'breaking': [...], 'non_breaking': [...], 'policy_errors': [...]}."""
    br, nb, pol = [], [], []
    oo, no = main_object(old), main_object(new)
    op = {p["name"]: p for p in oo.get("properties", [])}
    np_ = {p["name"]: p for p in no.get("properties", [])}
    for n in op:
        if n not in np_:
            br.append(f"property removed: {n}")
    for n in np_:
        if n not in op:
            nb.append(f"property added: {n}")
    for n in op.keys() & np_.keys():
        a, b = op[n], np_[n]
        if a.get("logicalType") != b.get("logicalType"):
            br.append(f"{n}: type changed {a.get('logicalType')} -> {b.get('logicalType')}")
        if a.get("required") and not b.get("required"):
            br.append(f"{n}: no longer required (consumers may now see NULLs)")
        if not a.get("required") and b.get("required"):
            nb.append(f"{n}: now required")
        if a.get("primaryKey") and not b.get("primaryKey"):
            br.append(f"{n}: no longer part of the primary key")
        ha, hb = cprops(a).get("exprHash"), cprops(b).get("exprHash")
        if ha and hb and ha != hb:
            br.append(f"{n}: definition (formula) changed")
        if cprops(a).get("kind") != cprops(b).get("kind"):
            br.append(f"{n}: kind changed")
    orules = {q["name"]: q for q in oo.get("quality", []) or [] if q.get("name")}
    nrules = {q["name"]: q for q in no.get("quality", []) or [] if q.get("name")}
    for r in orules:
        if r not in nrules:
            br.append(f"rule removed: {r}")
        else:
            a = (orules[r].get("implementation") or {}).get("action")
            b = (nrules[r].get("implementation") or {}).get("action")
            if a and b and STRICTNESS.get(b, 0) < STRICTNESS.get(a, 0):
                br.append(f"rule {r} loosened: {a} -> {b}")
            elif a and b and a != b:
                nb.append(f"rule {r} tightened: {a} -> {b}")
    for r in nrules:
        if r not in orules:
            nb.append(f"rule added: {r}")
    sla = lambda c: {s["property"]: s.get("value") for s in c.get("slaProperties", []) or []}
    for k, v in sla(old).items():
        nv = sla(new).get(k)
        if k == "freshness" and nv is not None and v is not None and float(nv) > float(v):
            br.append(f"freshness SLA loosened: {v} -> {nv}")
        elif nv is None:
            br.append(f"SLA removed: {k}")
    if old.get("status") != "deprecated" and new.get("status") == "deprecated":
        nb.append("contract deprecated")

    def ver(c): return tuple(int(x) for x in str(c["version"]).split("."))
    changed = bool(br or nb) or checksum(old) != checksum(new)
    if changed and ver(new) <= ver(old):
        pol.append(f"contract changed but version not bumped ({old['version']} -> {new['version']})")
    if br and ver(new)[0] <= ver(old)[0]:
        pol.append(f"breaking changes require a MAJOR version bump ({old['version']} -> {new['version']})")
    return {"breaking": br, "non_breaking": nb, "policy_errors": pol}
