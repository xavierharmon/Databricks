"""Tiny SQL-file runner so the SQL stays the learning artifact (readable, reviewable, testable).

Conventions
  * One .sql file = many statements separated by the Databricks cell marker `-- COMMAND ----------`
    (so every file also imports cleanly as a SQL notebook).
  * `{{catalog}}`-style tokens are substituted before execution.
"""
import re
from pathlib import Path

CELL_SEP = "-- COMMAND ----------"
TOKEN = re.compile(r"\{\{\s*(\w+)\s*\}\}")


def render(sql: str, params: dict) -> str:
    def repl(m):
        key = m.group(1)
        if key not in params:
            raise KeyError(f"SQL token {{{{{key}}}}} has no value; known: {sorted(params)}")
        return str(params[key])
    return TOKEN.sub(repl, sql)


def split_statements(text: str) -> list:
    """Split on cell markers; drop cells that contain only comments/whitespace."""
    cells = []
    for cell in text.split(CELL_SEP):
        body = [ln for ln in cell.splitlines() if ln.strip() and not ln.strip().startswith("--")]
        if body:
            cells.append(cell.strip().rstrip(";").strip())
    return cells


def statements_for(path, params: dict) -> list:
    return split_statements(render(Path(path).read_text(), params))


def run_sql_file(spark, path, params: dict, log=print):
    for i, stmt in enumerate(statements_for(path, params), 1):
        first = next(ln for ln in stmt.splitlines() if ln.strip())[:100]
        log(f"[{Path(path).name} #{i}] {first}")
        spark.sql(stmt)


def run_sql_dir(spark, directory, params: dict, log=print):
    for f in sorted(Path(directory).glob("*.sql")):
        run_sql_file(spark, f, params, log)
