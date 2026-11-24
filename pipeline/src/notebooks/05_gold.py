# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Gold: star schema (dimensions first, then facts)
# MAGIC Runs every file in `sql/gold/` in name order (dims 1x, facts 2x).

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
run_sql_dir(spark, ROOT / "sql" / "gold", {"catalog": catalog})
