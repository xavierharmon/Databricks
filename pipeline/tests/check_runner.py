"""End-to-end local checks for the script-based runner (no notebooks).   python tests/check_runner.py

Runs the real runner against a local Spark session with the planted-error FBL data. Covers: the plan, a full run, stage and
step selection, hard failures (missing file, bad contract, bad data at gold), write-audit-publish, and running from the zip.
"""
import contextlib
import io
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "src")); sys.path.insert(0, str(ROOT / "tests"))
from pyspark.sql import SparkSession
import check_fbl_pipeline as T
from lakehouse import runner

DBS = ["bronze", "silver", "gold", "gold_stage", "semantic", "governance"]
spark = None


def new_spark(tmp):
    global spark
    spark = (SparkSession.builder.master("local[2]").config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.warehouse.dir", str(tmp / "wh")).config("spark.sql.ansi.enabled", "true").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")


def reset():
    for d in DBS:
        spark.sql(f"DROP DATABASE IF EXISTS {d} CASCADE")


def defs_dir(tmp):
    d = tmp / "definitions"
    d.mkdir(exist_ok=True)
    for src in (ROOT / "sql" / "fbl" / "metric_views", ROOT / "sql" / "metric_views"):
        for f in src.glob("*.sql"):
            shutil.copy(f, d / f.name)
    return d


def argv(tmp, landing, run_id, contracts=None, extra=()):
    return ["--catalog", "local", "--pipeline-root", str(tmp), "--data-root", str(tmp), "--landing-dir", landing,
            "--contracts-dir", str(contracts or ROOT / "contracts"), "--transforms-dir", str(ROOT / "transforms"),
            "--definitions-dir", str(defs_dir(tmp)), "--local-mode", "--run-id", run_id, *extra]


def run(tmp, landing, run_id, contracts=None, extra=(), **kw):
    defs = {p.name: p.read_text() for p in defs_dir(tmp).glob("*.sql")}
    factory = T.local_executor_factory(spark, defs)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = runner.main(argv(tmp, landing, run_id, contracts, extra), spark=spark, semantic_executor_factory=factory, **kw)
    return rc, buf.getvalue()


def stats(run_id):
    return {(r.layer, r.table_name): (r.rows_in, r.rows_deduped, r.rows_rejected, r.rows_out)
            for r in spark.table("governance.dq_table_stats").where(f"run_id = '{run_id}'").collect()}


def steps(run_id):
    return {r.step: r.status for r in spark.table("governance.pipeline_step_log").where(f"run_id = '{run_id}'").collect()}


def check(cond, msg):
    if not cond:
        raise SystemExit("FAILED: " + msg)
    print("ok  ", msg)


def main():
    tmp = Path(tempfile.mkdtemp())
    new_spark(tmp)
    landing = T.land(tmp, ROOT / "data" / "fbl")

    # 1. the plan needs no Spark and comes from the contracts
    rc, out = run(tmp, landing, "plan", extra=["--plan-only"])
    order = [l.split()[1] for l in out.splitlines() if l.strip()[:2].rstrip(".").isdigit() and len(l.split()) > 1]
    check(rc == 0 and "silver:team" in out and out.index("silver:team") < out.index("silver:player") < out.index("silver:batting"),
          "plan orders silver parents before children, derived from the contracts")
    check(out.index("gold:dim_team") < out.index("gold:dim_player") and out.index("gold-publish") > out.index("gold:fact_batting")
          and out.index("semantic:mv_fbl_batting") > out.index("gold-publish"), "plan: dims before facts, publish after all audits, semantic last")

    # 2. a full run: KICK BACK (rows rejected) is the intended outcome on the sample data
    reset()
    rc, out = run(tmp, landing, "full")
    s = stats("full")
    check(rc == 1 and "KICK BACK" in out, "full run ends in KICK BACK (exit 1) because rows were rejected")
    check(s[("silver", "team")] == (15, 1, 6, 8) and s[("silver", "player")] == (42, 1, 10, 31) and s[("silver", "batting")] == (84, 1, 30, 53),
          "silver counts match the answer key (15/1/6/8, 42/1/10/31, 84/1/30/53)")
    check((spark.table("gold.fbl_dim_team").count(), spark.table("gold.fbl_dim_player").count(), spark.table("gold.fbl_fact_batting").count()) == (8, 31, 53),
          "gold published 8 / 31 / 53 rows")
    st = steps("full")
    check(all(v == "ok" for v in st.values()) and {"setup", "contracts", "gold-publish", "semantic:mv_fbl_batting"} <= set(st), "every step logged ok in pipeline_step_log")
    check((tmp / "reports" / "pipeline_report_full.html").exists(), "the HTML report was written")
    rc, out = run(tmp, landing, "full-soft", extra=["--fail-on-rejects", "false"])
    check(rc == 0 and "PIPELINE OK" in out, "--fail-on-rejects false gives exit 0 with the same data")

    # 3. selecting stages and steps
    reset()
    rc, out = run(tmp, landing, "partial", extra=["--stages", "bronze,silver"])
    check(rc == 1 and spark.catalog.tableExists("silver.fbl_batting") and not spark.catalog.tableExists("gold_stage.fbl_dim_team"), "--stages bronze,silver stops before gold")
    reset()
    rc, out = run(tmp, landing, "team-only", extra=["--steps", "bronze:team,silver:team,gold:dim_team,gold-publish", "--fail-on-rejects", "false"])
    check(rc == 0 and spark.table("gold.fbl_dim_team").count() == 8 and not spark.catalog.tableExists("gold.fbl_dim_player"), "--steps runs just the team slice end to end")

    # 4. hard failures
    reset()
    broken_landing = tmp / "landing_broken"
    shutil.copytree(landing, broken_landing)
    shutil.rmtree(broken_landing / "players")
    rc, out = run(tmp, str(broken_landing), "nofile")
    st = steps("nofile")
    check(rc == 1 and st.get("bronze:player") == "failed" and st.get("silver:team") == "skipped" and (tmp / "reports" / "pipeline_report_nofile.html").exists(),
          "a missing landing folder fails bronze, skips what depended on it, and the report is still written")

    reset()
    bad = tmp / "bad_contracts"
    shutil.copytree(ROOT / "contracts", bad)
    p = bad / "silver" / "fbl_silver_team.yaml"
    txt = p.read_text().replace("      - league\n", "", 1)
    p.write_text(txt)
    rc, out = run(tmp, landing, "badlint", contracts=bad)
    st = steps("badlint")
    check(rc == 1 and st.get("contracts") == "failed" and not spark.catalog.tableExists("bronze.fbl_team"), "an inconsistent contract fails lint BEFORE any data is touched")

    # 5. write-audit-publish: bad data must never reach the published gold table
    reset()
    rc, out = run(tmp, landing, "wap-base", extra=["--fail-on-rejects", "false"])
    before = spark.table("gold.fbl_dim_team").count()
    spark.sql("INSERT INTO silver.fbl_team SELECT 'T98', 'Null City FC', NULL, 'North', 2020, 'X Park', 100.0, NULL, 999, current_timestamp()")
    rc, out = run(tmp, landing, "wap-bad", extra=["--steps", "gold:dim_team,gold-publish"])
    check(rc == 1 and "GLD-001" in out, "a NULL in silver fails the gold audit")
    check(spark.table("gold.fbl_dim_team").count() == before and spark.table("gold_stage.fbl_dim_team").count() == before + 1,
          "...and the PUBLISHED gold table is untouched while the staged copy holds the bad rows (write-audit-publish)")
    check(steps("wap-bad").get("gold-publish") == "skipped", "...and gold-publish was skipped")

    # 6. a contract that promises something the table lacks fails the audit, not the consumers
    bad2 = tmp / "bad_contracts2"
    shutil.copytree(ROOT / "contracts", bad2)
    g = bad2 / "gold" / "fbl_gold_dim_team.yaml"
    g.write_text(g.read_text().replace("  quality:\n  - metric: rowCount", "  - name: stadium_capacity\n    logicalType: integer\n    physicalType: INT\n    required: true\n    description: Seats\n  quality:\n  - metric: rowCount", 1))
    reset()
    run(tmp, landing, "c2-base", extra=["--fail-on-rejects", "false"])
    before = spark.table("gold.fbl_dim_team").count()
    rc, out = run(tmp, landing, "c2", contracts=bad2, extra=["--steps", "bronze:team,silver:team,gold:dim_team,gold-publish"])
    check(rc == 1 and "declared columns exist" in out, "an unmet schema promise fails the gold audit")

    # 7. the zip works: launcher + code zip + YAML read from folders, in a separate process
    kit = tmp / "kit"
    subprocess.run([sys.executable, str(ROOT / "tools" / "build_runner_kit.py"), "--out", str(kit)], check=True, capture_output=True)
    cmd = [sys.executable, str(kit / "pipeline_volume" / "run_pipeline.py"), "--catalog", "local", "--pipeline-root", str(kit / "pipeline_volume"),
           "--data-root", str(kit / "data_volume"), "--contracts-dir", str(kit / "pipeline_volume" / "contracts_reference"), "--local-mode", "--plan-only"]
    r = subprocess.run(cmd, capture_output=True, text=True)
    check(r.returncode == 0 and "silver:player" in r.stdout, "run_pipeline.py finds lakehouse.zip and reads the contract YAML (plan-only, separate process)")
    # 8. a REAL step through the launcher in a separate process, importing everything from the zip (Spark started by the script itself)
    cmd = [sys.executable, str(kit / "pipeline_volume" / "run_pipeline.py"), "--catalog", "local", "--pipeline-root", str(kit / "pipeline_volume"),
           "--data-root", str(kit / "data_volume"), "--contracts-dir", str(kit / "pipeline_volume" / "contracts_reference"), "--local-mode", "--steps", "bronze:team", "--fail-on-rejects", "false", "--run-id", "zip-run"]
    r = subprocess.run(cmd, capture_output=True, text=True, cwd=str(tmp))
    check(r.returncode == 0 and "PIPELINE OK" in r.stdout and "bronze.fbl_team: 15 rows" in r.stdout,
          "a real step runs through the launcher from lakehouse.zip in a fresh process (Spark created by the script)")
    shutil.rmtree(tmp, ignore_errors=True)
    print("\nRUNNER CHECKS PASSED")


if __name__ == "__main__":
    main()
