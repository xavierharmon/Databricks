# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Bronze: raw, append-only, replayable
# MAGIC **Rules of bronze:** keep the source as-is (all STRING), add lineage columns, never clean here.
# MAGIC The only edit: punctuation in column names (`park.key` becomes `park_key`) because dots break SQL.
# MAGIC
# MAGIC Uses **Auto Loader** (`cloudFiles`) with `availableNow`: it remembers which files it already loaded,
# MAGIC so re-running never duplicates data. Drop a new file in the volume and run again to see incremental load.

# COMMAND ----------
import re
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "mlb_dev")
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"])
catalog = dbutils.widgets.get("catalog")
full_refresh = dbutils.widgets.get("full_refresh") == "true"

TABLES = ["people", "teams", "teams_franchises", "batting", "pitching", "salaries", "parks", "awards_players"]
LANDING = f"/Volumes/{catalog}/bronze/landing/lahman"
CHECKPOINTS = f"/Volumes/{catalog}/bronze/landing/_checkpoints/lahman"

def clean(col: str) -> str:
    return re.sub(r"[^0-9a-zA-Z]+", "_", col).strip("_")

# COMMAND ----------
for t in TABLES:
    target = f"{catalog}.bronze.lahman_{t}"
    if full_refresh:
        spark.sql(f"DROP TABLE IF EXISTS {target}")
        dbutils.fs.rm(f"{CHECKPOINTS}/{t}", True)

    raw = (spark.readStream.format("cloudFiles")
           .option("cloudFiles.format", "csv")
           .option("header", "true")
           .option("cloudFiles.inferColumnTypes", "false")      # bronze = strings; typing happens in silver
           .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/{t}/schema")
           .load(f"{LANDING}/{t}/"))

    cols = [F.col(f"`{c}`").alias(clean(c)) for c in raw.columns if c != "_rescued_data"]
    df = raw.select(
        *cols,
        F.col("_rescued_data"),                                  # anything that did not fit the schema lands here
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )

    (df.writeStream
       .option("checkpointLocation", f"{CHECKPOINTS}/{t}/checkpoint")
       .trigger(availableNow=True)
       .toTable(target)
       .awaitTermination())

    spark.sql(f"COMMENT ON TABLE {target} IS 'BRONZE: raw Lahman {t} CSV, all STRING, append-only via Auto Loader.'")
    print(f"{target}: {spark.table(target).count():,} rows")
