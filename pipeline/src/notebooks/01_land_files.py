# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Land files
# MAGIC Simulates an upstream system dropping files: copies the Lahman CSVs from the deployed bundle
# MAGIC into a Unity Catalog **Volume** (`<catalog>.bronze.landing`). Auto Loader reads from there next.
# MAGIC In real life this step is SFTP / cloud storage / a vendor feed, and you would skip it.

# COMMAND ----------
import shutil
from pathlib import Path

dbutils.widgets.text("catalog", "mlb_dev")
catalog = dbutils.widgets.get("catalog")
nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
nb = nb[len("/Workspace"):] if nb.startswith("/Workspace/") else nb
ROOT = Path("/Workspace") / Path(nb.lstrip("/")).parents[2]

# source CSV name -> landing folder (one folder per entity keeps Auto Loader schemas separate)
FILES = {
    "People": "people", "Teams": "teams", "TeamsFranchises": "teams_franchises",
    "Batting": "batting", "Pitching": "pitching", "Salaries": "salaries",
    "Parks": "parks", "AwardsPlayers": "awards_players",
}

# COMMAND ----------
landing = Path(f"/Volumes/{catalog}/bronze/landing/lahman")
for src, folder in FILES.items():
    dest = landing / folder
    dest.mkdir(parents=True, exist_ok=True)
    shutil.copy(ROOT / "data" / "lahman" / f"{src}.csv", dest / f"{src}.csv")
    print(f"landed {src}.csv -> {dest}")
