"""Runs the tested code blocks in docs/PYSPARK_STARTER.md on local Spark (real CSVs). python tests/check_pyspark_doc.py"""
import re, sys, shutil, tempfile
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src")); sys.path.insert(0, str(ROOT / "tests"))
from lib.sql_runner import statements_for
import local_pipeline_check as L

tmp = Path(tempfile.mkdtemp())
spark = (SparkSession.builder.master("local[2]").config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.warehouse.dir", str(tmp / "wh")).config("spark.sql.ansi.enabled", "true").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
for db in ["bronze", "silver", "governance"]:
    spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE"); spark.sql(f"CREATE DATABASE {db}")

# landing folders like the Volume
land = tmp / "landing"
for src, t in L.TABLES.items():
    (land / t).mkdir(parents=True); shutil.copy(ROOT / "data/lahman" / f"{src}.csv", land / t / f"{src}.csv")

# SQL-built bronze (the 'manual mode' equivalent) + SQL silver as the comparison baseline
import re as _re
for src, t in L.TABLES.items():
    df = spark.read.option("header", "true").csv(str(land / t))
    df = df.select([F.col(f"`{c}`").alias(_re.sub(r"[^0-9a-zA-Z]+", "_", c).strip("_")) for c in df.columns])
    df.withColumn("_rescued_data", F.lit(None).cast("string")).withColumn("_ingested_at", F.current_timestamp()) \
      .write.saveAsTable(f"bronze.lahman_{t}")
    spark.table(f"bronze.lahman_{t}").createOrReplaceTempView(f"bronze_lahman_{t}")
for f in sorted((ROOT / "sql/silver").glob("*.sql")):
    for stmt in statements_for(f, {"catalog": "local"}):
        loc = L.to_local(stmt)
        if loc: spark.sql(loc)
for t in ["player", "batting_season", "salary"]:
    spark.table(f"silver_{t}").write.saveAsTable(f"silver.{t}")
spark.sql("CREATE TABLE governance.dq_results (check_name string, severity string, violations bigint, suite string, checked_at timestamp)")

md = (ROOT / "docs/PYSPARK_STARTER.md").read_text()
blocks = dict(re.findall(r"<!-- test:(\w+) -->\n```python\n(.*?)```", md, re.S))
env = {"spark": spark, "catalog": "", "F": F, "display": lambda d: d.show(3)}
from pyspark.sql.window import Window; env["Window"] = Window
for name in ["profile", "silver_batting", "compare", "dq_gate"]:
    code = blocks[name].replace("{catalog}.", "")
    exec(code, env); print(f"ok  block: {name}")
code = blocks["bronze_loop"].replace("{catalog}.", "").replace('f"/Volumes/{catalog}/bronze/landing/lahman"', f'"{land}"')
exec(code, env); print("ok  block: bronze_loop")
shutil.rmtree(tmp, ignore_errors=True)
