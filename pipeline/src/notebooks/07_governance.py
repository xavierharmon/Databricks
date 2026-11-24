# Databricks notebook source
# MAGIC %md
# MAGIC # 07 Governance: glossary, comments, tags, grants, certification
# MAGIC Everything here is code, so it is reviewed in pull requests and identical in every environment.

# COMMAND ----------
import sys
from pathlib import Path
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "mlb_dev")
dbutils.widgets.text("analyst_group", "")
dbutils.widgets.dropdown("apply_certification", "false", ["false", "true"])
catalog = dbutils.widgets.get("catalog")
analyst_group = dbutils.widgets.get("analyst_group").strip()
apply_cert = dbutils.widgets.get("apply_certification") == "true"

nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
nb = nb[len("/Workspace"):] if nb.startswith("/Workspace/") else nb
ROOT = Path("/Workspace") / Path(nb.lstrip("/")).parents[2]
sys.path.insert(0, str(ROOT / "src"))
from lib.sql_runner import run_sql_file

params = {"catalog": catalog, "analyst_group": analyst_group or "unused"}
GOV = ROOT / "sql" / "governance"

# COMMAND ----------
# 1. Business glossary as code: CSV in git -> governed Delta table (also feeds Ardoq).
import csv
with open(ROOT / "data" / "glossary" / "glossary_terms.csv", newline="") as fh:   # plain Python: works on any compute
    rows = list(csv.DictReader(fh))
glossary = (spark.createDataFrame(rows, schema="term string, category string, definition string, formula string, "
                                                "owner string, related_asset string, status string")
            .withColumn("loaded_at", F.current_timestamp()))
glossary.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.governance.business_glossary")
spark.sql(f"COMMENT ON TABLE {catalog}.governance.business_glossary IS 'Business glossary managed as code (data/glossary/glossary_terms.csv).'")
print(f"glossary terms loaded: {glossary.count()}")

# COMMAND ----------
# 2. Comments + tags
run_sql_file(spark, GOV / "10_comments.sql", params)
run_sql_file(spark, GOV / "20_tags.sql", params)

# COMMAND ----------
# 3. Grants (only if a group was supplied for this environment)
if analyst_group:
    run_sql_file(spark, GOV / "30_grants.sql", params)
else:
    print("analyst_group blank: skipping grants")

# COMMAND ----------
# 4. Certification (needs ASSIGN on the governed tag system.certification_status)
if apply_cert:
    run_sql_file(spark, GOV / "40_certification.sql", params)
else:
    print("apply_certification=false: skipping")
