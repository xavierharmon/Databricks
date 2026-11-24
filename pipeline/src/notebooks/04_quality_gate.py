# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Quality gate
# MAGIC Runs `sql/dq/<suite>.sql`. Each cell returns `(check_name, severity, violations)`.
# MAGIC Results are appended to `<catalog>.governance.dq_results`. The task FAILS if any `error` check has
# MAGIC violations, which stops downstream tasks. `warn` checks are recorded but never block.

# COMMAND ----------
import sys
from pathlib import Path
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "mlb_dev")
dbutils.widgets.text("suite", "silver_checks")
catalog, suite = dbutils.widgets.get("catalog"), dbutils.widgets.get("suite")
nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
nb = nb[len("/Workspace"):] if nb.startswith("/Workspace/") else nb
ROOT = Path("/Workspace") / Path(nb.lstrip("/")).parents[2]
sys.path.insert(0, str(ROOT / "src"))
from lib.sql_runner import statements_for

# COMMAND ----------
results = None
for stmt in statements_for(ROOT / "sql" / "dq" / f"{suite}.sql", {"catalog": catalog}):
    df = spark.sql(stmt)
    results = df if results is None else results.unionByName(df)

results = (results.withColumn("suite", F.lit(suite))
                  .withColumn("checked_at", F.current_timestamp()))
results.write.mode("append").saveAsTable(f"{catalog}.governance.dq_results")
display(results)

# COMMAND ----------
failed = results.where("severity = 'error' AND violations > 0").collect()
if failed:
    raise Exception("Data quality gate failed: " + ", ".join(f"{r.check_name} ({r.violations})" for r in failed))
print("Quality gate passed.")
