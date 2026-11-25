"""End-to-end local check: ONE run covering BOTH products (fbl and tix) on local Spark.   python tests/check_tix_slice.py

Proves: the combined plan runs, tix reads its own landing folder, the cross-product rule (a baseball participant must exist in the
shared FBL team table) fires, planted errors are caught with the expected counts, and gold dimensions are published for both products.
"""
import csv
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "src")); sys.path.insert(0, str(ROOT / "tests")); sys.path.insert(0, str(ROOT / "tools"))
import check_runner as CR  # noqa: E402
import generate_tix_data as G  # noqa: E402

REF = ["venue", "participant", "event_participant", "event_tag", "ticket_inventory", "fee_schedule"]


def lines(p):
    return [l for l in Path(p).read_text().splitlines() if l.strip()]


def main():
    tmp = Path(tempfile.mkdtemp())
    CR.new_spark(tmp)
    CR.reset()
    spark = CR.spark
    for d in ("bronze", "silver", "gold", "gold_stage", "semantic", "governance"):
        spark.sql(f"DROP DATABASE IF EXISTS {d} CASCADE")
    for name in ("teams", "players", "batting"):                          # FBL landing: <data-root>/fbl/<folder>/<file>
        (tmp / "fbl" / name).mkdir(parents=True)
        shutil.copy(ROOT / "data" / "fbl" / f"{name}.csv", tmp / "fbl" / name / f"{name}.csv")
    G.main(["--out", str(tmp / "gen"), "--seed", "5", "--events", "20", "--persons", "60", "--batches", "1", "--history-days", "30"])
    b0 = tmp / "gen" / "batches" / "batch_0000"
    for ent in REF + ["customer", "event"]:                               # tix landing: <data-root>/tix/<entity>/
        (tmp / "tix" / ent).mkdir(parents=True)
        for f in (b0 / ent).glob("*.csv"):
            shutil.copy(f, tmp / "tix" / ent / f.name)
    n = {e: len(lines(next((tmp / "tix" / e).glob("*.csv")))) - 1 for e in REF + ["customer", "event"]}
    # planted errors, appended to copies of the landed files
    def add(ent, *rows):
        f = next((tmp / "tix" / ent).glob("*.csv"))
        with open(f, "a") as fh:
            fh.write("\n".join(rows) + "\n")
    add("venue", lines(next((tmp / "tix" / "venue").glob("*.csv")))[1])                        # exact duplicate venue row  -> deduped 1
    add("participant", "T99,Ghost Team,team,baseball", "FB01,Conflict A,team,football", "FB01,Conflict B,team,football")  # unknown FBL team; conflicting id (FB01 had one row already)
    add("event", "E9001,Bad Venue Game,sport,baseball,FBL,V99,2026-12-01T19:00:00Z,2026-10-01T00:00:00Z,scheduled",
        "E9002,Bad Category,festival,,,V01,2026-12-01T19:00:00Z,2026-10-01T00:00:00Z,scheduled")
    add("customer", "C900001,Short,Zip,aaaa,bbbb,123,,2026-01-01T00:00:00Z")

    rc, out = CR.run(tmp, str(tmp / "fbl"), "slice1", extra=["--product", "fbl,tix", "--fail-on-rejects", "false"])
    fails = []

    def check(name, got, want):
        (fails.append if got != want else print)(f"FAIL {name}: got {got!r}, want {want!r}" if got != want else f"ok   {name}: {got}")

    check("run exit code (rejects allowed)", rc, 0)
    st = {(r.layer, r.table_name): r for r in spark.table("governance.dq_table_stats").where("run_id = 'slice1'").collect()}
    g = lambda layer, ent: st[(layer, ent)]
    check("fbl team 15/1/6/8", (g("silver", "team").rows_in, g("silver", "team").rows_deduped, g("silver", "team").rows_rejected, g("silver", "team").rows_out), (15, 1, 6, 8))
    check("fbl gold published 8/31/53", [spark.table(t).count() for t in ("gold.fbl_dim_team", "gold.fbl_dim_player", "gold.fbl_fact_batting")], [8, 31, 53])
    check("tix venue: 1 duplicate removed", (g("silver", "venue").rows_in, g("silver", "venue").rows_deduped, g("silver", "venue").rows_rejected), (n["venue"] + 1, 1, 0))
    p = g("silver", "participant")
    check("tix participant: T99 unknown FBL team + all three FB01 rows (one original, two conflicting) rejected = 4", (p.rows_in, p.rows_rejected, p.rows_out), (n["participant"] + 3, 4, n["participant"] - 1))
    e = g("silver", "event")
    check("tix event: bad venue and bad category rejected = 2", (e.rows_in, e.rows_rejected, e.rows_out), (n["event"] + 2, 2, n["event"]))
    c = g("silver", "customer")
    check("tix customer: short postal code rejected = 1", (c.rows_rejected, c.rows_out), (1, n["customer"]))
    for ent in ("event_participant", "event_tag", "ticket_inventory", "fee_schedule"):
        s = g("silver", ent)
        if s.rows_rejected:
            fails.append(f"FAIL tix {ent} rejected {s.rows_rejected} rows, expected 0")
    # the participant that was rejected by the cross-product rule must be traceable to PAR-005 and PAR-002
    q = {r.rule_ids for r in spark.table("governance.dq_quarantine").where("run_id = 'slice1' AND source_table = 'participant' AND disposition = 'rejected'").collect()}
    check("participant rejection reasons", sorted(q), ["PAR-002", "PAR-005"])
    check("tix gold published: dim_event / dim_customer", [spark.table("gold.tix_dim_event").count(), spark.table("gold.tix_dim_customer").count()], [n["event"], n["customer"]])
    check("tix gold has no NULL (concert sport labelled none)", spark.table("gold.tix_dim_event").where("sport IS NULL OR league IS NULL").count(), 0)
    steps = {r.step: r.status for r in spark.table("governance.pipeline_step_log").where("run_id = 'slice1'").collect()}
    bad = {k: v for k, v in steps.items() if v != "ok"}
    check("every step ok in the log (fbl and tix steps both present)", (bad, "fbl:gold-publish" in steps and "tix:gold-publish" in steps), ({}, True))
    order = [r.step for r in spark.table("governance.pipeline_step_log").where("run_id = 'slice1'").orderBy("started_at").collect()]
    check("fbl team silver built before tix participant silver", order.index("fbl:silver:team") < order.index("tix:silver:participant"), True)
    print()
    if fails:
        print("\n".join(fails))
        print(out[-2500:])
        sys.exit(1)
    print("TIX SLICE CHECKS PASSED")


if __name__ == "__main__":
    main()
