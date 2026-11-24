# Where to add PySpark to your manual setup

You already have bronze tables built with SQL. Keep SQL for the business logic (silver, gold, metric views). Add PySpark in the four places where it is genuinely better: **profiling, loops, quality gates, and running your SQL files for you.**

## 0. How to run PySpark (one-time, 2 minutes)

1. In the workspace: **New > Notebook**. Set the language to **Python** and attach serverless compute.
2. Cell 1 of every notebook:
```python
catalog = "mlb_dev"                      # your catalog
from pyspark.sql import functions as F
from pyspark.sql.window import Window
```
3. `spark` is already defined for you. `spark.table("cat.schema.table")` gives you a DataFrame, and `spark.sql("...")` runs any SQL and also returns a DataFrame. That second one is the bridge: **anything you can do in SQL you can do from Python, and you can mix both in one notebook.**
4. Run a cell with Shift+Enter. Use `display(df)` to see a DataFrame.

> Rule of thumb: SQL says *what* you want. PySpark lets you add loops, variables and functions around it.

## Where it fits in your pipeline

| Layer / step | Keep as SQL | Add PySpark for |
| --- | --- | --- |
| Bronze | the 8 `read_files` statements work | loop over tables (section 3) and add a profile (section 1) |
| Silver | `sql/silver/*.sql` | wide tables with 20 similar columns (section 2) |
| Quality gate | the check SQL | **a gate that actually fails the job** (section 4) |
| Gold | `sql/gold/*.sql` | nothing needed yet |
| Metric views | YAML in SQL | nothing, they are SQL objects |
| Orchestration | | a notebook that runs all your SQL files in order (section 5) |

## 1. Profile a bronze table (find the mess before you clean it)

Counts blanks and NULLs in every column in one pass. Run it on `lahman_batting` and see which stats are missing for old seasons.

<!-- test:profile -->
```python
df = spark.table(f"{catalog}.bronze.lahman_batting")
data_cols = [c for c in df.columns if not c.startswith("_")]

profile = df.select([
    F.sum(F.when(F.col(f"`{c}`").isNull() | (F.trim(F.col(f"`{c}`")) == ""), 1).otherwise(0)).alias(c)
    for c in data_cols
])
display(profile)
```

How it works: a list comprehension builds one `SUM(CASE WHEN blank THEN 1 ELSE 0 END)` expression per column, so you wrote one line instead of 22. The backticks protect names like `2B`.

## 2. Silver with a loop (one rule applied to many columns)

Silver batting converts 17 count columns with the same rule. In SQL that is 17 near-identical lines. In PySpark it is one dictionary and one loop.

This writes to a **new table, `batting_season_py`**, so you can compare it with your SQL version without overwriting anything.

<!-- test:silver_batting -->
```python
INT_COLS = {"G": "g", "AB": "ab", "R": "r", "H": "h", "2B": "doubles", "3B": "triples", "HR": "hr",
            "RBI": "rbi", "SB": "sb", "CS": "cs", "BB": "bb", "SO": "so", "IBB": "ibb",
            "HBP": "hbp", "SH": "sh", "SF": "sf", "GIDP": "gidp"}

def to_int(src: str):
    # same rule as the SQL files: trim, blank -> NULL, bad value -> NULL (try_cast)
    return F.expr(f"try_cast(NULLIF(trim(`{src}`), '') AS INT)")

b = spark.table(f"{catalog}.bronze.lahman_batting").where("NULLIF(trim(playerID), '') IS NOT NULL")

typed = b.select(
    F.trim("playerID").alias("player_id"),
    F.expr("try_cast(trim(yearID) AS INT)").alias("year_id"),
    F.expr("try_cast(trim(stint) AS INT)").alias("stint"),
    F.trim("teamID").alias("team_id"),
    F.trim("lgID").alias("league_id"),
    *[to_int(src).alias(dst) for src, dst in INT_COLS.items()],
    F.col("_ingested_at"),
)

# keep the newest row per business key
w = Window.partitionBy("player_id", "year_id", "stint").orderBy(F.col("_ingested_at").desc())
silver_py = (typed.withColumn("_rn", F.row_number().over(w))
                  .where("_rn = 1")
                  .drop("_rn", "_ingested_at")
                  .withColumn("_loaded_at", F.current_timestamp()))

silver_py.write.mode("overwrite").saveAsTable(f"{catalog}.silver.batting_season_py")
```

**Prove the two versions agree.** This is the best habit PySpark gives you: two implementations, one comparison.

<!-- test:compare -->
```python
sql_v = spark.table(f"{catalog}.silver.batting_season").drop("_loaded_at")
py_v  = spark.table(f"{catalog}.silver.batting_season_py").drop("_loaded_at")

print("rows only in SQL version:   ", sql_v.exceptAll(py_v).count())
print("rows only in PySpark version:", py_v.exceptAll(sql_v).count())
```
Both numbers should be 0.

Note that this creates a plain table. Your SQL version also added `NOT NULL`, a primary key and a CHECK constraint with `ALTER TABLE`. After writing a table from PySpark, you can still run those `ALTER TABLE` statements with `spark.sql(...)`.

## 3. Loop over all eight bronze tables

Replaces eight copy-pasted `read_files` statements with one loop, and shows the PySpark way to read CSV.

<!-- test:bronze_loop -->
```python
import re

LANDING = f"/Volumes/{catalog}/bronze/landing/lahman"
TABLES = ["people", "teams", "teams_franchises", "batting", "pitching", "salaries", "parks", "awards_players"]

def clean_name(c: str) -> str:
    return re.sub(r"[^0-9a-zA-Z]+", "_", c).strip("_")      # park.key -> park_key

for t in TABLES:
    raw = (spark.read.option("header", "true")
                     .option("inferSchema", "false")        # keep everything as strings
                     .csv(f"{LANDING}/{t}/"))
    df = raw.select(
        *[F.col(f"`{c}`").alias(clean_name(c)) for c in raw.columns],
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.bronze.lahman_{t}_py")
    print(t, df.count())
```
It writes `lahman_<name>_py` tables so you can compare against your SQL-built bronze. Differences to expect: this version has no `_rescued_data` column, because that comes from `read_files` and Auto Loader, not plain `spark.read`.

## 4. A quality gate that actually fails (the most valuable one)

A SQL check just returns a number. Nothing stops the next step. This notebook runs checks, records them, and **raises an error if any `error`-severity check fails**. When you later put it in a Job, a failure stops everything after it.

<!-- test:dq_gate -->
```python
checks = [
    # (name, severity, SQL that returns ONE number = how many violations)
    ("silver.player: player_id unique", "error",
     f"SELECT COUNT(*) - COUNT(DISTINCT player_id) FROM {catalog}.silver.player"),
    ("silver.batting_season: hits <= at bats", "error",
     f"SELECT COUNT(*) FROM {catalog}.silver.batting_season WHERE h > ab"),
    ("silver.batting_season: orphan player_id", "error",
     f"""SELECT COUNT(*) FROM {catalog}.silver.batting_season b
         LEFT ANTI JOIN {catalog}.silver.player p ON b.player_id = p.player_id"""),
    ("silver.salary: salary > 0", "warn",
     f"SELECT COUNT(*) FROM {catalog}.silver.salary WHERE salary_usd <= 0"),
]

rows = [(name, sev, int(spark.sql(sql).first()[0])) for name, sev, sql in checks]

results = (spark.createDataFrame(rows, "check_name string, severity string, violations bigint")
                .withColumn("suite", F.lit("silver_py"))
                .withColumn("checked_at", F.current_timestamp()))
results.write.mode("append").saveAsTable(f"{catalog}.governance.dq_results")
display(results)

failed = [r for r in rows if r[1] == "error" and r[2] > 0]
if failed:
    raise Exception("Quality gate failed: " + ", ".join(f"{n} ({v})" for n, _, v in failed))
print("Quality gate passed")
```
Add more checks by adding one tuple to the list. You can copy the SQL from `sql/dq/silver_checks.sql`.

## 5. Run all your SQL files from one notebook (a home-made pipeline runner)

In manual mode you paste each file into the SQL editor. This notebook does that for you, in order, so one click rebuilds silver and gold.

1. On your computer run `python tools/render_sql.py --catalog <your_catalog> --skip-create-catalog`.
2. Upload the `rendered/<your_catalog>/` folders (`silver`, `gold`, `metric_views`) to the Volume, for example into `/Volumes/<catalog>/bronze/landing/sql/`.
3. Run this in a Python notebook:

```python
from pathlib import Path

SQL_ROOT = Path(f"/Volumes/{catalog}/bronze/landing/sql")
CELL_SEP = "-- COMMAND ----------"

def run_sql_file(path: Path):
    for cell in path.read_text().split(CELL_SEP):
        body = [l for l in cell.splitlines() if l.strip() and not l.strip().startswith("--")]
        if body:                                   # skip comment-only cells
            print(f"{path.name}: {body[0][:80]}")
            spark.sql(cell.strip().rstrip(";"))

for folder in ["silver", "gold", "metric_views"]:
    for f in sorted((SQL_ROOT / folder).glob("*.sql")):
        run_sql_file(f)
```

Put the quality gate from section 4 between the `silver` and `gold` loops and you have a real ordered pipeline with a stop-on-failure check. **Then turn the notebook into a Job** (Jobs & Pipelines > Create job > notebook task) and you have scheduled automation without the CLI or GitHub.

## What to try next
1. Run section 1 on every bronze table. Which columns are mostly blank?
2. Build `pitching_season_py` the way section 2 does, then compare with `exceptAll`.
3. Add three checks of your own to section 4 and break one on purpose to see the failure.
4. Put sections 4 and 5 in one notebook and run it as a Job.

## Honest limits
Sections 1 to 4 were run on a local Spark session against the real CSVs before I wrote this. Anything that reads from or writes to `/Volumes` (sections 3 and 5) and anything that touches Databricks-only features has not been run in your workspace. If a cell errors, paste the message and I will fix it.
