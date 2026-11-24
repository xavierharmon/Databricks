"""Run the FBL pipeline end to end on local Spark with the generated CSVs.   python tests/check_fbl_pipeline.py"""
import shutil, sys, tempfile
from pathlib import Path
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from lib import fbl_pipeline as P
from lib.dq_engine import DQFailure


def make_spark(tmp):
    spark = (SparkSession.builder.master("local[2]").config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.warehouse.dir", str(tmp / "wh")).config("spark.sql.ansi.enabled", "true").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    for db in ["bronze", "silver", "gold", "governance"]:
        spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")
    return spark


def land(tmp, data_dir):
    land = tmp / "landing"
    for name, folder in (("teams", "teams"), ("players", "players"), ("batting", "batting")):
        (land / folder).mkdir(parents=True, exist_ok=True)
        shutil.copy(data_dir / f"{name}.csv", land / folder / f"{name}.csv")
    return str(land)


def contract_bundle():
    """YAML contracts + metric-view SQL -> the same bundle a user would upload."""
    import yaml
    from lib.contracts import cprops, main_object, make_bundle
    contracts = [yaml.safe_load(f.read_text()) for f in sorted((ROOT / "contracts").rglob("*.yaml"))]
    defs = {}
    for c in contracts:
        if cprops(c).get("layer") == "semantic":
            path = ROOT / cprops(main_object(c))["definition"]
            defs[path.name] = path.read_text()
    return make_bundle(contracts, defs)


def run(spark, landing, run_id, expect_gold_ok=True):
    P.stage_setup(spark, (ROOT / "data/dq/dq_rules.csv").read_text())
    P.stage_bronze(spark, landing, run_id)
    P.stage_silver(spark, run_id)
    P.stage_gold(spark, run_id)
    return P.build_report(spark, run_id)


if __name__ == "__main__":
    tmp = Path(tempfile.mkdtemp())
    spark = make_spark(tmp)
    landing = land(tmp, ROOT / "data" / "fbl")
    rep = run(spark, landing, "run-1")
    print("\n" + rep["text"])
    print("\nRULES WITH VIOLATIONS")
    spark.sql("SELECT table_name, rule_id, action, violations FROM governance.dq_rule_results WHERE run_id='run-1' AND violations>0 ORDER BY layer, table_name, rule_id").show(60, False)
    print("QUARANTINE BY DISPOSITION")
    spark.sql("SELECT source_table, disposition, count(*) n FROM governance.dq_quarantine GROUP BY 1,2 ORDER BY 1,2").show(30, False)
    for t in ["dim_team", "dim_player", "fact_batting"]:
        print(t, spark.table(f"gold.fbl_{t}").count())
    shutil.rmtree(tmp, ignore_errors=True)


def scenario_notebook_and_failure():
    """1) run the GENERATED single-file notebook exactly as Databricks would (stubbed dbutils)  2) prove gold fails on a NULL."""
    tmp = Path(tempfile.mkdtemp())
    spark = make_spark(tmp)
    landing = land(tmp, ROOT / "data" / "fbl")
    widgets = {"catalog": "spark_catalog", "stage": "all", "run_id": "nb-run", "fail_on_rejects": "true",
               "reload_rules": "false", "landing_dir": landing, "rules_source": "contracts", "include_semantic": "false", "contracts_dir": ""}
    class W:  # minimal dbutils.widgets
        def text(self, *a, **k): pass
        dropdown = text
        def get(self, k): return widgets[k]
    class D: widgets = W()
    html = []
    class SparkProxy:  # open-source Spark has no USE CATALOG; Databricks does. Ignore only that statement.
        def __init__(self, s): self._s = s
        def sql(self, q, *a, **k): return None if q.strip().upper().startswith("USE CATALOG") else self._s.sql(q, *a, **k)
        def __getattr__(self, n): return getattr(self._s, n)
    env = {"spark": SparkProxy(spark), "dbutils": D(), "displayHTML": html.append, "__name__": "nb"}
    try:
        exec(compile((ROOT / "src/notebooks/fbl_dq_pipeline.py").read_text(), "fbl_dq_pipeline.py", "exec"), env)
        raise SystemExit("expected the notebook to raise KICK BACK because rows were rejected")
    except Exception as e:
        assert "KICK BACK" in str(e), e
        print("notebook raised as designed:", str(e)[:110])
    assert html and "Rejected rows" in html[0]
    assert Path(landing).parent.joinpath("reports/dq_report_nb-run.html").exists()
    print("notebook wrote report HTML and file")

    # gold must fail if a NULL sneaks into silver
    spark.sql("INSERT INTO silver.fbl_team SELECT 'T98', 'Null City FC', NULL, 'North', 2020, 'X Park', 100.0, NULL, 999, current_timestamp()")
    try:
        P.stage_gold(spark, "null-run")
        raise SystemExit("gold should have failed on NULL")
    except DQFailure as e:
        assert "GLD-001" in str(e), e
        print("gold gate caught the NULL:", str(e)[:120])
    shutil.rmtree(tmp, ignore_errors=True)




def local_executor_factory(spark, defs_by_name):
    """Stand-in for MEASURE() queries on open-source Spark: inline measure expressions and left-join the dimensions.
    Proves contract tests and metric view definitions agree; does not test Databricks' metric-view engine."""
    import re, yaml
    from lib.contracts import cprops, main_object, load_metric_view_definition

    def flat(s): return re.sub(r"\blocal\.(\w+)\.(\w+)", r"\1.\2", s)

    def factory(contract, view_fqn):
        name = Path(cprops(main_object(contract))["definition"]).name
        spec = load_metric_view_definition(defs_by_name[name], "local")
        fields = {f["name"]: f["expr"] for f in spec["fields"]}
        measures = {m["name"]: m["expr"] for m in spec["measures"]}
        def inline(e):
            while "MEASURE(" in e:
                e = re.sub(r"MEASURE\((\w+)\)", lambda m: f"({measures[m.group(1)]})", e)
            return e
        joins = " ".join(f"LEFT JOIN {flat(j['source'])} AS {j['name']} ON {j['on']}" for j in spec.get("joins", []))
        def run(req):
            dims, ms = req.get("dims", []), req.get("measures", [])
            where = req.get("where")
            if where:
                for f in sorted(fields, key=len, reverse=True):
                    where = re.sub(rf"\b{f}\b", f"({fields[f]})", where)
            sel = [f"{fields[d]} AS `{d}`" for d in dims] + [f"{inline(measures[m])} AS `{m}`" for m in ms]
            sql = f"SELECT {', '.join(sel)} FROM {flat(spec['source'])} AS source {joins}"
            if where: sql += f" WHERE {where}"
            if dims: sql += " GROUP BY " + ", ".join(fields[d] for d in dims)
            if req.get("limit"): sql += f" LIMIT {req['limit']}"
            return spark.sql(sql)
        return run
    return factory


def scenario_contracts():
    """Contract mode must give IDENTICAL results to the built-in rules, and the semantic contracts must pass on real gold data."""
    from lib import contracts as C
    from lib.contract_checks import ContractFailure
    bundle = contract_bundle()
    contracts, defs = C.contracts_from_bundle(bundle), bundle["definitions"]
    tmp = Path(tempfile.mkdtemp())
    spark = make_spark(tmp)
    landing = land(tmp, ROOT / "data" / "fbl")
    P.stage_setup(spark)                                     # no CSV: rules must come from the contracts
    P.stage_contracts(spark, "c-run", contracts, defs, "test bundle")
    assert spark.table("governance.dq_rules").count() == 41, "contracts must compile to the same 41 rules"
    P.stage_bronze(spark, landing, "c-run"); P.stage_silver(spark, "c-run"); P.stage_gold(spark, "c-run")
    got = {(r.layer, r.table_name): (r.rows_in, r.rows_deduped, r.rows_rejected, r.rows_out) for r in spark.table("governance.dq_table_stats").collect()}
    want = {("silver", "team"): (15, 1, 6, 8), ("silver", "player"): (42, 1, 10, 31), ("silver", "batting"): (84, 1, 30, 53),
            ("gold", "dim_team"): (8, 0, 0, 8), ("gold", "dim_player"): (31, 0, 0, 31), ("gold", "fact_batting"): (53, 0, 0, 53)}
    assert got == want, got
    print("contract-driven run matches the built-in run exactly")
    spark.sql("SELECT check_type, passed, count(*) n FROM governance.contract_results WHERE run_id='c-run' GROUP BY 1,2 ORDER BY 1,2").show()
    assert spark.table("governance.contract_results").where("run_id='c-run' AND passed=false AND severity='error'").count() == 0

    # semantic contracts: views are checked with the local stand-in executor
    P.stage_semantic(spark, "c-run", "local", defs, build=False, executor_factory=local_executor_factory(spark, defs))
    n = spark.table("governance.contract_results").where("run_id='c-run' AND layer='semantic'").count()
    print(f"semantic contracts: {n} checks, none broken")

    # negative 1: a gold table loses a promised column -> the schema contract must report an error
    import copy
    from lib.contract_checks import schema_conformance
    slim = copy.deepcopy(C.ContractSet(contracts).entity("gold", "fact_batting", "fbl"))
    spark.sql("CREATE OR REPLACE TEMP VIEW fbl_fact_slim AS SELECT player_id, season FROM gold.fbl_fact_batting")
    slim["schema"][0]["physicalName"] = "fbl_fact_slim"
    rows = schema_conformance(spark, "neg", slim)
    assert any(r[7] == "error" and not r[8] and "declared columns exist" in r[6] for r in rows), rows
    print("missing gold column is caught by the schema contract:", [r[9] for r in rows if not r[8]][0][:80])
    # negative 2: a changed formula must break the lint (exprHash)
    bad = {k: v for k, v in defs.items()}
    bad["mv_fbl_batting.sql"] = defs["mv_fbl_batting.sql"].replace("SUM(h) / NULLIF(SUM(ab), 0)", "AVG(h) / NULLIF(AVG(ab), 0)")
    parsed = {k: C.load_metric_view_definition(v) for k, v in bad.items()}
    findings = C.lint(C.ContractSet(contracts), parsed)
    assert any("batting_avg" in m and "definition changed" in m for s, cid, m in findings if s == "error"), findings
    print("changed formula is caught by lint:", [m[:70] for s, cid, m in findings if s == 'error'][0])
    # negative 3: wrong expectation must fail the semantic stage
    wrong = copy.deepcopy(contracts)
    for c in wrong:
        if c["id"] == "fbl.semantic.mv_fbl_roster":
            for q in c["schema"][0]["quality"]:
                if q["implementation"]["test_type"] == "bounds":
                    q["implementation"]["bounds"]["avg_height_in"] = [100, 120]
    P.stage_contracts(spark, "c-run2", wrong, defs, "tampered")
    try:
        P.stage_semantic(spark, "c-run2", "local", defs, build=False, executor_factory=local_executor_factory(spark, defs))
        raise SystemExit("semantic stage should have failed on the impossible bound")
    except ContractFailure as e:
        print("semantic contract failure caught:", str(e)[:120])
    shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    scenario_notebook_and_failure()
    scenario_contracts()
    print("FBL CHECKS PASSED")
