"""Run contracts against live Databricks objects: schema conformance (tables), semantic interface + tests + SLA (metric views).

Everything returns rows for governance.contract_results:
  (run_id, checked_at, contract_id, contract_version, layer, check_type, check_name, severity, passed, detail)
severity 'error' + passed=False means the contract is broken. 'warn' is recorded but never stops the job.
"""
from datetime import datetime

from pyspark.sql import SparkSession

from lib.contracts import ENGINE_SEM, cprops, layer_of, main_object

CONTRACT_RESULTS_DDL = ("run_id STRING, checked_at TIMESTAMP, contract_id STRING, contract_version STRING, layer STRING, "
                        "check_type STRING, check_name STRING, severity STRING, passed BOOLEAN, detail STRING")
REGISTRY_DDL = ("contract_id STRING, name STRING, version STRING, status STRING, layer STRING, dataset STRING, owner STRING, "
                "checksum STRING, loaded_at TIMESTAMP, contract_json STRING")

SPARK_TYPES = {"string": {"string"}, "integer": {"int", "bigint", "smallint", "tinyint"}, "number": {"double", "float"},
               "boolean": {"boolean"}, "date": {"date"}, "timestamp": {"timestamp", "timestamp_ntz"}}


class ContractFailure(Exception):
    """A contract with severity=error was broken."""


def type_ok(logical: str, spark_type: str) -> bool:
    t = spark_type.lower()
    return t in SPARK_TYPES.get(logical, set()) or (logical == "number" and t.startswith("decimal"))


def _row(run_id, c, check_type, name, severity, passed, detail=""):
    return (run_id, datetime.now(), c["id"], str(c["version"]), layer_of(c), check_type, name, severity, bool(passed), str(detail)[:900])


def write_results(spark: SparkSession, rows: list):
    if rows:
        spark.createDataFrame(rows, CONTRACT_RESULTS_DDL).write.mode("append").saveAsTable("governance.contract_results")


def broken(rows: list) -> list:
    return [r for r in rows if r[7] == "error" and not r[8]]


def raise_if_broken(rows: list):
    bad = broken(rows)
    if bad:
        raise ContractFailure("Contract broken: " + "; ".join(f"{r[2]} {r[6]} ({r[9]})" for r in bad[:8]))


# --------------------------------------------------------------------------- tables (source / silver / gold)
def schema_conformance(spark: SparkSession, run_id: str, c: dict, table: str = None) -> list:
    """Check a table against its contract. `table` overrides the contract's physicalName (e.g. a staging copy)."""
    obj = main_object(c)
    table = table or obj["physicalName"]
    rows = []
    try:
        df = spark.table(table)
    except Exception as e:  # noqa: BLE001
        return [_row(run_id, c, "schema", f"{table} exists", "error", False, str(e)[:300])]
    actual = {f.name.lower(): f.dataType.simpleString() for f in df.schema.fields}
    declared = [p["name"] for p in obj["properties"]]
    missing = [n for n in declared if n.lower() not in actual]
    rows.append(_row(run_id, c, "schema", "declared columns exist", "error", not missing,
                     f"missing: {', '.join(missing)}" if missing else f"{len(declared)} columns present"))
    wrong = [f"{p['name']} is {actual[p['name'].lower()]}, contract says {p['logicalType']}" for p in obj["properties"]
             if p["name"].lower() in actual and not type_ok(p["logicalType"], actual[p["name"].lower()])]
    rows.append(_row(run_id, c, "schema", "column types match", "error", not wrong, "; ".join(wrong) or "types match"))
    extra = [n for n in actual if not n.startswith("_") and n not in {d.lower() for d in declared}]
    rows.append(_row(run_id, c, "schema", "no undeclared columns", "warn", not extra,
                     f"undeclared: {', '.join(extra)}" if extra else "none"))
    for q in obj.get("quality", []) or []:
        if q.get("type") == "library" and q.get("metric") == "rowCount":
            n = df.count()
            floor = q.get("mustBeGreaterThan", 0)
            rows.append(_row(run_id, c, "schema", f"rowCount > {floor}", q.get("severity", "error"), n > floor, f"{n} rows"))
    return rows


# --------------------------------------------------------------------------- semantic views
def view_sql(view_fqn: str, dims=(), measures=(), where=None, limit=None) -> str:
    sel = [f"`{d}`" for d in dims] + [f"MEASURE(`{m}`) AS `{m}`" for m in measures]
    sql = f"SELECT {', '.join(sel)} FROM {view_fqn}"
    if where:
        sql += f" WHERE {where}"
    if dims:
        sql += " GROUP BY " + ", ".join(f"`{d}`" for d in dims)
    if limit:
        sql += f" LIMIT {int(limit)}"
    return sql


def make_executor(spark: SparkSession, view_fqn: str):
    """executor(request) -> DataFrame. request = {dims, measures, where?, limit?}. Tests can swap this out."""
    return lambda req: spark.sql(view_sql(view_fqn, req.get("dims", []), req.get("measures", []), req.get("where"), req.get("limit")))


def _num(x):
    return None if x is None else float(x)


def _close(a, b, tol):
    if a is None or b is None:
        return a is None and b is None
    return abs(float(a) - float(b)) <= tol


def semantic_checks(spark: SparkSession, run_id: str, c: dict, executor, now_sql_ok=True) -> list:
    obj = main_object(c)
    cp = cprops(c)
    strict = bool(cp.get("strictTypes"))
    props = obj["properties"]
    fields = [p for p in props if cprops(p).get("kind") == "field"]
    measures = [p for p in props if cprops(p).get("kind") == "measure"]
    first_measure = measures[0]["name"] if measures else None
    rows = []

    # 1) interface: every promised field and measure can be queried, with the promised type
    for p in props:
        kind = cprops(p).get("kind")
        req = {"dims": [p["name"]], "measures": [first_measure], "limit": 1} if kind == "field" else {"dims": [], "measures": [p["name"]]}
        try:
            df = executor(req)
            col = {f.name: f.dataType.simpleString() for f in df.schema.fields}[p["name"]]
            df.limit(1).collect()
            ok = type_ok(p["logicalType"], col)
            rows.append(_row(run_id, c, "interface", f"{kind} {p['name']} is queryable", "error", True))
            rows.append(_row(run_id, c, "interface", f"{kind} {p['name']} type is {p['logicalType']}",
                             "error" if strict else "warn", ok, f"actual {col}"))
        except Exception as e:  # noqa: BLE001
            rows.append(_row(run_id, c, "interface", f"{kind} {p['name']} is queryable", "error", False, str(e)[:300]))

    # 2) behaviour tests
    for q in obj.get("quality", []) or []:
        if q.get("engine") != ENGINE_SEM:
            continue
        i, name = q["implementation"], q["name"]
        sev = "warn" if q.get("severity") == "warning" else "error"      # warning-level tests are recorded, never block
        tol = float(i.get("tolerance", 0) or 0)
        dims = i.get("dimensions", []) or []
        try:
            if i["test_type"] == "reconcile":
                a = executor({"dims": dims, "measures": i["measures"]}).collect()
                b = spark.sql(i["compare_sql"]).collect()
                ka = {tuple(r[d] for d in dims): r for r in a}
                kb = {tuple(r[d] for d in dims): r for r in b}
                bad = [f"{k} missing on one side" for k in ka.keys() ^ kb.keys()]
                for k in ka.keys() & kb.keys():
                    for m in i["measures"]:
                        if not _close(ka[k][m], kb[k][m], tol):
                            bad.append(f"{k} {m}: view={ka[k][m]} gold={kb[k][m]}")
                rows.append(_row(run_id, c, "test", f"{name} {q.get('description', '')[:70]}", sev, not bad,
                                 f"{len(ka)} groups compared" if not bad else f"{len(bad)} mismatches, e.g. {bad[0]}"))
            elif i["test_type"] == "expect":
                got = executor({"dims": dims, "measures": i["measures"], "where": i.get("where")}).collect()
                index = {tuple(r[d] for d in dims): r for r in got}
                bad = []
                for e in i["expect"]:
                    k = tuple(e[d] for d in dims)
                    if k not in index:
                        bad.append(f"{k} not returned")
                        continue
                    for m in i["measures"]:
                        if not _close(index[k][m], e[m], tol):
                            bad.append(f"{k} {m}: got {index[k][m]}, expected {e[m]}")
                rows.append(_row(run_id, c, "test", f"{name} {q.get('description', '')[:70]}", sev, not bad,
                                 f"{len(i['expect'])} expected rows matched" if not bad else "; ".join(bad[:3])))
            elif i["test_type"] == "bounds":
                got = executor({"dims": dims, "measures": list(i["bounds"])}).collect()
                bad = [f"{[r[d] for d in dims]} {m}={r[m]}" for r in got for m, (lo, hi) in i["bounds"].items()
                       if r[m] is not None and not (lo <= float(r[m]) <= hi)]
                rows.append(_row(run_id, c, "test", f"{name} {q.get('description', '')[:70]}", sev, not bad,
                                 f"{len(got)} groups within bounds" if not bad else f"{len(bad)} out of bounds, e.g. {bad[0]}"))
        except Exception as e:  # noqa: BLE001
            rows.append(_row(run_id, c, "test", f"{name} ran", sev, False, str(e)[:300]))

    # 3) SLA: freshness of the gold tables this view depends on, and that their last gate was clean
    for s in c.get("slaProperties", []) or []:
        if s.get("property") != "freshness":
            continue
        hours = float(s["value"])
        for dep in cprops(obj).get("dependsOn", []) or []:
            table = dep.split(".")[-1]
            table = table[4:] if table.startswith("fbl_") else table
            try:
                n = spark.sql(f"SELECT COUNT(*) FROM governance.dq_table_stats WHERE layer = 'gold' AND table_name = '{table}' "
                              f"AND checked_at >= current_timestamp() - INTERVAL {int(hours)} HOURS").first()[0]
                rows.append(_row(run_id, c, "sla", f"{dep} refreshed within {int(hours)}h", "error", n > 0,
                                 "recent build found" if n else "no gold build recorded in the window"))
                bad = spark.sql(f"""SELECT COUNT(*) FROM governance.dq_rule_results r
                    WHERE r.layer = 'gold' AND r.table_name = '{table}' AND r.action = 'fail' AND r.violations > 0
                      AND r.run_id = (SELECT run_id FROM governance.dq_table_stats WHERE layer = 'gold' AND table_name = '{table}'
                                      ORDER BY checked_at DESC LIMIT 1)""").first()[0]
                rows.append(_row(run_id, c, "sla", f"{dep} last quality gate was clean", "error", bad == 0,
                                 "clean" if bad == 0 else f"{bad} failing rules in the latest build"))
            except Exception as e:  # noqa: BLE001
                rows.append(_row(run_id, c, "sla", f"{dep} freshness", "error", False, str(e)[:300]))
    return rows
