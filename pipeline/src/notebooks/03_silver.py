# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Silver: typed, cleaned, conformed
# MAGIC Runs every file in `sql/silver/` in name order. Open those files: they ARE the lesson.

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
run_sql_dir(spark, ROOT / "sql" / "silver", {"catalog": catalog})
