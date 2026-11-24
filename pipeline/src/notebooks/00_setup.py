# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Setup: catalog, schemas, landing volume
# MAGIC Creates the Unity Catalog objects every later step writes to. Safe to re-run.
# MAGIC 
# MAGIC If `CREATE CATALOG` fails with a storage-root error, create the catalog once in Catalog Explorer
# MAGIC (or add `MANAGED LOCATION` in `sql/setup/00_objects.sql`) and re-run.

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
run_sql_dir(spark, ROOT / "sql" / "setup", {"catalog": catalog})
