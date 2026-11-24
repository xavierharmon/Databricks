"""Run bronze -> silver -> DQ -> gold SQL on a LOCAL Spark session against the real CSVs.

Delta/Unity-Catalog-only clauses (COMMENT, CLUSTER BY, constraints, tags) are stripped, and
`{{catalog}}.layer.table` is flattened to `layer_table` temp views. This catches SQL syntax and logic errors
(typing, dedupe, joins, reconciliation) in seconds, before anything touches a workspace.

    pip install pyspark pytest pyyaml
    python tests/local_pipeline_check.py
"""
import re
import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from lib.sql_runner import statements_for  # noqa: E402

TABLES = {"People": "people", "Teams": "teams", "TeamsFranchises": "teams_franchises", "Batting": "batting",
          "Pitching": "pitching", "Salaries": "salaries", "Parks": "parks", "AwardsPlayers": "awards_players"}


def flatten(sql: str) -> str:
    return re.sub(r"\blocal\.(\w+)\.(\w+)", r"\1_\2", sql)


def to_local(stmt: str):
    """Translate one UC/Delta statement to local-Spark SQL, or None to skip it."""
    s = "\n".join(l for l in flatten(stmt).splitlines() if not l.strip().startswith("--"))
    # Open-source Spark has no QUALIFY (Databricks does): rewrite to the equivalent subquery for local runs.
    s = re.sub(r"SELECT \* FROM (\S+)\s+(WHERE .*?)\s+QUALIFY (row_number\(\) OVER \(.*?\)) = 1\s*\)",
               r"SELECT * FROM (SELECT *, \3 AS _rn FROM \1 \2) WHERE _rn = 1)", s, flags=re.S)
    if re.match(r"\s*(ALTER|COMMENT ON|GRANT|SET TAG)", s, re.I):
        return None
    m = re.match(r"\s*CREATE OR REPLACE TABLE (\w+)\s*(?:COMMENT '[^']*')?\s*(?:CLUSTER BY \([^)]*\))?\s*AS\s*(.*)", s, re.S | re.I)
    if m:
        return f"CREATE OR REPLACE TEMP VIEW {m.group(1)} AS {m.group(2)}"
    return s


def check_metric_views(spark):
    """Execute every metric-view field and measure expression as plain SQL (MEASURE() inlined, joins as LEFT JOINs).

    This does not test Databricks' metric-view engine, but it proves every column reference, join key and aggregate
    expression in the YAML is valid against the real gold tables.
    """
    import yaml
    bad = []
    for f in sorted((ROOT / "sql" / "metric_views").glob("*.sql")):
        raw = statements_for(f, {"catalog": "local"})[0]
        spec = yaml.safe_load(raw.split("$$")[1])
        name = re.search(r"semantic\.(\w+)", raw).group(1)
        src = flatten(spec["source"])
        joins = "\n".join(f"LEFT JOIN {flatten(j['source'])} AS {j['name']} ON {j['on']}" for j in spec.get("joins", []))
        measures = {m["name"]: m["expr"] for m in spec["measures"]}

        def inline(expr):
            while "MEASURE(" in expr:
                expr = re.sub(r"MEASURE\((\w+)\)", lambda m: f"({measures[m.group(1)]})", expr)
            return expr

        select_measures = ", ".join(f"{inline(e)} AS {n}" for n, e in measures.items())
        for fld in spec["fields"]:
            sql = f"SELECT {fld['expr']} AS dim, {select_measures} FROM {src} AS source {joins} GROUP BY {fld['expr']} ORDER BY 1 LIMIT 3"
            try:
                spark.sql(sql).collect()
            except Exception as e:  # noqa: BLE001
                bad.append(f"{name}.{fld['name']}: {str(e)[:200]}")
        print(f"ok  metric view {name}: {len(spec['fields'])} fields x {len(measures)} measures executed")
    return bad


def check_lahman_contracts(spark):
    """Run the Lahman semantic contracts (golden values from the raw CSVs) against locally built gold tables."""
    import yaml
    sys.path.insert(0, str(ROOT / "tests"))
    from check_fbl_pipeline import local_executor_factory
    from lib.contracts import cprops, main_object
    from lib.contract_checks import semantic_checks
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    for t in ["fact_batting_season", "fact_pitching_season", "fact_team_season", "dim_player", "dim_team_season", "dim_season"]:
        spark.table(f"gold_{t}").write.mode("overwrite").saveAsTable(f"gold.{t}")
    contracts = [yaml.safe_load(f.read_text()) for f in sorted((ROOT / "contracts" / "semantic").glob("lahman_*.yaml"))]
    defs = {Path(cprops(main_object(c))["definition"]).name: (ROOT / cprops(main_object(c))["definition"]).read_text() for c in contracts}
    factory = local_executor_factory(spark, defs)
    bad = []
    for c in contracts:
        rows = semantic_checks(spark, "lahman-local", c, factory(c, "unused"))
        broken = [r for r in rows if r[7] == "error" and not r[8]]
        print(f"ok  lahman contract {c['id']}: {len(rows)} checks, {len(broken)} broken")
        for r in rows:
            if r[5] == "test":
                print(f"      {'PASS' if r[8] else 'FAIL'} {r[6][:90]} -> {r[9][:90]}")
        bad += [f"{c['id']}: {r[6]} {r[9]}" for r in broken]
    return bad


def main():
    spark = SparkSession.builder.master("local[2]").config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.ansi.enabled", "true").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # bronze: strings only, punctuation cleaned, lineage columns, blanks -> NULL (Spark CSV default)
    for src, t in TABLES.items():
        df = spark.read.option("header", "true").csv(str(ROOT / "data" / "lahman" / f"{src}.csv"))
        df = df.select([F.col(f"`{c}`").alias(re.sub(r"[^0-9a-zA-Z]+", "_", c).strip("_")) for c in df.columns])
        df.withColumn("_rescued_data", F.lit(None).cast("string")).withColumn("_ingested_at", F.current_timestamp()) \
          .createOrReplaceTempView(f"bronze_lahman_{t}")

    def run_dir(d):
        for f in sorted((ROOT / "sql" / d).glob("*.sql")):
            for stmt in statements_for(f, {"catalog": "local"}):
                local = to_local(stmt)
                if local:
                    spark.sql(local)
            print(f"ok  {d}/{f.name}")

    run_dir("silver")
    for t in ["player", "franchise", "park", "team_season", "batting_season", "pitching_season", "salary", "award"]:
        print(f"    silver_{t}: {spark.table('silver_' + t).count():,} rows")

    def run_checks(suite):
        bad = []
        for stmt in statements_for(ROOT / "sql" / "dq" / f"{suite}.sql", {"catalog": "local"}):
            r = spark.sql(flatten(stmt)).collect()[0]
            flag = "FAIL" if r.severity == "error" and r.violations > 0 else ("warn" if r.violations > 0 else "pass")
            print(f"    [{flag}] {r.check_name}: {r.violations}")
            if flag == "FAIL":
                bad.append(r.check_name)
        return bad

    failures = run_checks("silver_checks")
    run_dir("gold")
    failures += run_checks("gold_checks")
    failures += check_metric_views(spark)
    failures += check_lahman_contracts(spark)

    print("\nSpot check: 2001 Barry Bonds / Ichiro")
    print("Sample semantic query (inlined):")
    spark.sql("""
      SELECT p.full_name, f.season, SUM(f.hr) hr, ROUND(SUM(f.h)/SUM(f.ab),3) avg
      FROM gold_fact_batting_season f JOIN gold_dim_player p USING (player_key)
      WHERE p.player_id IN ('bondsba01','suzukic01') AND f.season = 2001 GROUP BY 1,2
    """).show()
    print("gold_fact_team_season payroll rows:", spark.sql("SELECT COUNT(payroll_usd) FROM gold_fact_team_season").collect()[0][0])
    if failures:
        print("FAILED:", failures)
        sys.exit(1)
    print("LOCAL PIPELINE CHECK PASSED")


if __name__ == "__main__":
    main()
