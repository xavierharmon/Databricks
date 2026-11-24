# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Metric views (the semantic layer)
# MAGIC Creates governed metric views in `<catalog>.semantic` from the YAML in `sql/metric_views/`.
# MAGIC Needs serverless / SQL warehouse compute and a runtime that supports metric view YAML 1.1.

# COMMAND ----------
import sys
from pathlib import Path
dbutils.widgets.text("catalog", "mlb_dev")
catalog = dbutils.widgets.get("catalog")
nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
nb = nb[len("/Workspace"):] if nb.startswith("/Workspace/") else nb
ROOT = Path("/Workspace") / Path(nb.lstrip("/")).parents[2]   # <root>/src/notebooks/<this> -> <root>
sys.path.insert(0, str(ROOT / "src"))
from lib.sql_runner import run_sql_dir

# COMMAND ----------
run_sql_dir(spark, ROOT / "sql" / "metric_views", {"catalog": catalog})

# COMMAND ----------
# Smoke test: query the semantic layer the way a BI tool or Genie would.
display(spark.sql(f"""
  SELECT season, MEASURE(home_runs) AS home_runs, MEASURE(batting_avg) AS batting_avg
  FROM {catalog}.semantic.mv_batting
  WHERE season >= 2015
  GROUP BY season ORDER BY season
"""))
