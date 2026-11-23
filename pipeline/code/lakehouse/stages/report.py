"""REPORT: turn the evidence the other steps left behind into one readable result, and decide whether the run 'failed'.

Why a separate report step: the other steps record facts (rows in, rows rejected, checks broken). This step is the only
one that decides what those facts MEAN for the run. If rows were rejected the run is marked failed on purpose (KICK BACK),
because a pipeline that ends green while it silently set rows aside teaches people to ignore it.
"""
import html as _html
import os

from pyspark.sql import functions as F

from lib.dq_engine import DQFailure


def build_report(spark, run_id):
    stats = spark.table("governance.dq_table_stats").where(F.col("run_id") == run_id).orderBy("checked_at").collect()
    viol = (spark.table("governance.dq_rule_results").where((F.col("run_id") == run_id) & (F.col("violations") > 0))
            .orderBy("layer", "table_name", "rule_id").collect())
    rej = (spark.table("governance.dq_quarantine").where((F.col("run_id") == run_id) & (F.col("disposition") == "rejected"))
           .orderBy("source_table", "row_id").collect())
    fixed = (spark.table("governance.dq_quarantine").where((F.col("run_id") == run_id) & (F.col("disposition") == "auto_corrected"))
             .groupBy("source_table", "rule_ids").count().orderBy("source_table", "rule_ids").collect())
    cres = spark.table("governance.contract_results").where((F.col("run_id") == run_id) & (F.col("passed") == False)).collect()  # noqa: E712
    steps = spark.table("governance.pipeline_step_log").where(F.col("run_id") == run_id).orderBy("started_at").collect()
    n_rej = len(rej)
    n_failed = sum(1 for v in viol if v["action"] == "fail")
    n_cbroken = sum(1 for r in cres if r["severity"] == "error")
    n_stepfail = sum(1 for s in steps if s["status"] == "failed")
    lines = [f"run {run_id}: {n_rej} rows rejected, {sum(f['count'] for f in fixed)} auto-corrections, {n_failed} failed rules, "
             f"{n_cbroken} broken contract checks, {n_stepfail} failed steps"]
    for s in steps:
        lines.append(f"  step {s['step']:<22} {s['status']:<8} {(s['detail'] or '')[:100]}")
    for s in stats:
        lines.append(f"  {s['layer']}.{s['table_name']}: in={s['rows_in']} deduped={s['rows_deduped']} rejected={s['rows_rejected']} out={s['rows_out']}")
    for r in cres[:30]:
        lines.append(f"  CONTRACT {r['severity'].upper()} {r['contract_id']} [{r['check_type']}] {r['check_name']}: {r['detail'][:120]}")
    for r in rej[:50]:
        lines.append(f"  REJECT {r['source_table']} row {r['row_id']} [{r['rule_ids']}] {r['payload'][:140]}")
    esc = _html.escape

    def table(head, rows):
        return ("<table border='1' cellpadding='4' style='border-collapse:collapse;font-family:sans-serif;font-size:13px'><tr>"
                + "".join(f"<th>{esc(h)}</th>" for h in head) + "</tr>"
                + "".join("<tr>" + "".join(f"<td>{esc(str(c))}</td>" for c in row) + "</tr>" for row in rows) + "</table>")
    page = (f"<h2>Pipeline run report</h2><p>Run <code>{esc(run_id)}</code>: <b>{n_rej}</b> rows rejected, <b>{n_failed}</b> failed rules, "
            f"<b>{n_cbroken}</b> broken contract checks, <b>{n_stepfail}</b> failed steps.</p>"
            "<h3>Steps</h3>" + table(["step", "status", "detail"], [(s["step"], s["status"], s["detail"]) for s in steps])
            + "<h3>Row flow by table</h3>" + table(["layer", "table", "in", "deduped", "standardized", "warned", "rejected", "out"],
               [(s["layer"], s["table_name"], s["rows_in"], s["rows_deduped"], s["rows_standardized"], s["rows_warned"], s["rows_rejected"], s["rows_out"]) for s in stats])
            + "<h3>Rules with violations</h3>" + table(["layer", "table", "rule", "action", "violations", "description"],
               [(v["layer"], v["table_name"], v["rule_id"], v["action"], v["violations"], v["description"]) for v in viol])
            + "<h3>Auto-corrected (no action needed)</h3>" + table(["table", "rule", "rows"], [(f["source_table"], f["rule_ids"], f["count"]) for f in fixed])
            + "<h3>Contract checks that did not pass</h3>" + table(["contract", "type", "check", "severity", "detail"],
               [(r["contract_id"], r["check_type"], r["check_name"], r["severity"], r["detail"]) for r in cres])
            + "<h3>Rejected rows: kick back to the source owner</h3>" + table(["table", "row", "rules", "original record"],
               [(r["source_table"], r["row_id"], r["rule_ids"], r["payload"]) for r in rej]))
    return {"text": "\n".join(lines), "html": page, "rejected": n_rej, "failed_rules": n_failed, "contract_failures": n_cbroken}


def run(ctx, prior_failure=None):
    """Write the report. Returns the exception that should end the run (or None)."""
    report = build_report(ctx.spark, ctx.run_id)
    ctx.log(report["text"])
    try:
        os.makedirs(ctx.reports_dir, exist_ok=True)
        path = f"{ctx.reports_dir}/pipeline_report_{ctx.run_id}.html"
        with open(path, "w") as fh:
            fh.write(report["html"])
        ctx.log(f"  report file: {path}")
    except Exception as e:  # noqa: BLE001  (reporting must never hide the real result)
        ctx.log(f"  could not write the report file: {str(e)[:120]}")
    if prior_failure is not None:
        return prior_failure
    if ctx.fail_on_rejects and (report["rejected"] or report["failed_rules"] or report["contract_failures"]):
        return DQFailure("KICK BACK: " + report["text"].splitlines()[0])
    return None
