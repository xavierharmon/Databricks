"""End-to-end local check of the WHOLE tix pipeline against the generator's answer key and ground truth.   python tests/check_tix_full.py

Runs FBL and tix in one run on local Spark with the default generated data (seed 7, six daily batches), then compares what the pipeline
built with numbers derived independently: ledger totals, fees, profit, every order's closed-sale result, rule-by-rule rejection counts for
the planted errors, be-backs and price-at-visit against the journey truth, and identity attribution against who was really who.
Finally it plants an ownership-chain break and checks that the gold audit stops publication.
"""
import csv
import json
import sys
import tempfile
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for sub in ("", "src", "tests", "tools"):
    sys.path.insert(0, str(ROOT / sub) if sub else str(ROOT))
import check_runner as CR  # noqa: E402
import generate_tix_data as G  # noqa: E402
import stage_landing as ST  # noqa: E402

fails = []


def check(name, got, want):
    if got != want:
        fails.append(f"FAIL {name}: got {got!r}, want {want!r}")
        print(fails[-1])
    else:
        print(f"ok   {name}: {got}")


def atleast(name, got, floor):
    if got < floor:
        fails.append(f"FAIL {name}: got {got}, want at least {floor}")
        print(fails[-1])
    else:
        print(f"ok   {name}: {got} (at least {floor})")


def rows(p):
    return list(csv.DictReader(open(p)))


def main():
    tmp = Path(tempfile.mkdtemp())
    CR.new_spark(tmp)
    CR.reset()
    spark = CR.spark
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    for d in ("bronze", "silver", "gold", "gold_stage", "semantic", "governance"):
        spark.sql(f"DROP DATABASE IF EXISTS {d} CASCADE")
    import shutil
    for name in ("teams", "players", "batting"):
        (tmp / "fbl" / name).mkdir(parents=True)
        shutil.copy(ROOT / "data" / "fbl" / f"{name}.csv", tmp / "fbl" / name / f"{name}.csv")
    import os
    G.main(["--out", str(tmp / "gen"), "--seed", "7"] + os.environ.get("TIX_GEN_ARGS", "").split())      # e.g. "--signal strong --nonbuyer-ratio 3"
    ST.main(["--src", str(tmp / "gen"), "--dst", str(tmp / "tix"), "--through", "6", "--reset"])
    key = json.load(open(tmp / "gen" / "answer_key" / "answer_key.json"))

    rc, out = CR.run(tmp, str(tmp / "fbl"), "full1", extra=["--product", "fbl,tix", "--fail-on-rejects", "false"])
    check("run exit code", rc, 0)
    if rc:
        print(out[-3000:])
        sys.exit(1)
    steps = {r.step: r.status for r in spark.table("governance.pipeline_step_log").where("run_id = 'full1'").collect()}
    check("every step ok", {k: v for k, v in steps.items() if v != "ok"}, {})
    st = {(r.layer, r.table_name): r for r in spark.table("governance.dq_table_stats").where("run_id = 'full1'").collect()}

    # ---- the ledger: row-level rules, rule by rule
    planted = Counter()
    for b in key["planted_counts_by_batch"].values():
        planted.update(b)
    rr = {r.rule_id: r.violations for r in spark.table("governance.dq_rule_results").where("run_id = 'full1' AND table_name = 'ticket_transaction' AND layer = 'silver'").collect()}
    t = st[("silver", "ticket_transaction")]
    check("silver ledger: out = valid transactions in the answer key", t.rows_out, key["ledger"]["valid_transactions"])
    check("duplicates removed = E01 + E02 plants", t.rows_deduped, planted["TXN-E01_exact_duplicate_same_file"] + planted["TXN-E02_redelivered_from_earlier_batch"])
    check("TXN-002 conflicting copies (both rejected)", rr["TXN-002"], 2 * planted["TXN-E03_conflicting_duplicate"])
    check("TXN-004 invalid sale type = E07", rr["TXN-004"], planted["TXN-E07_invalid_sale_type"])
    check("TXN-006 price out of range = E08", rr["TXN-006"], planted["TXN-E08_negative_or_missing_price"])
    check("TXN-007 arithmetic = E04 + E08", rr["TXN-007"], planted["TXN-E04_total_does_not_equal_price_plus_fee"] + planted["TXN-E08_negative_or_missing_price"])
    e5, e6c = planted["TXN-E05_buyback_buyer_is_not_company"], planted["TXN-E06_unknown_customer"]
    atleast("TXN-008 buyback buyer: at least the E05 plants", rr["TXN-008"], e5)
    check("TXN-008 buyback buyer: at most E05 plus E06 copies that happened to be buybacks", rr["TXN-008"] <= e5 + e6c, True)
    check("TXN-011 unknown ticket = E06", rr["TXN-011"], planted["TXN-E06_unknown_ticket"])
    check("TXN-013 unknown customer = E06", rr["TXN-013"], planted["TXN-E06_unknown_customer"])
    check("TXN-015 sale after event start = E09", rr["TXN-015"], planted["TXN-E09_sale_after_event_start"])

    # ---- ledger totals from gold against the answer key
    exp = key["ledger"]
    g = spark.sql("""SELECT sale_type, count(*) n, sum(price) gross, sum(fee_total) fees, sum(owned_inventory_profit) profit FROM gold.tix_fact_ticket_transfer GROUP BY sale_type""").collect()
    for r in g:
        e = exp["by_sale_type"][r.sale_type]
        check(f"{r.sale_type}: count / gross / fees", [r.n, str(r.gross), str(r.fees)], [e["count"], e["gross"], e["fees"]])
    tot = spark.sql("SELECT sum(fee_total) f, sum(owned_inventory_profit) p FROM gold.tix_fact_ticket_transfer").first()
    check("fee revenue", str(tot.f), exp["fee_revenue"])
    check("owned-inventory profit", str(tot.p), exp["owned_inventory_profit"])
    check("total earnings", str(tot.f + tot.p), exp["total_earnings"])

    # ---- installments: every order's closed-sale result
    det = {d["order_id"]: d for d in key["closed_sales"]["detail"]}
    gold = spark.sql("""SELECT order_id, total_amount, net_paid, closed_ever, closed_now, is_plan,
                        date_format(first_closed_ts, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS fc FROM gold.tix_fact_order_payment_status""").collect()
    bad = []
    for r in gold:
        d = det.get(r.order_id)
        mine = (int(r.total_amount * 100), int(r.net_paid * 100), r.fc if r.closed_ever else "", bool(r.closed_ever), bool(r.closed_now))
        want = (d["total"], d["net_paid"], d["first_closed_ts"], d["closed_ever"], d["closed_now"]) if d else None
        if mine != want:
            bad.append((r.order_id, mine, want))
    check("orders whose closed-sale result differs from the answer key", len(bad), 0)
    if bad:
        print("  e.g.", bad[:3])
    check("orders in gold = orders in the key", len(gold), len(det))
    check("closed ever / now", [sum(r.closed_ever for r in gold), sum(r.closed_now for r in gold)], [key["closed_sales"]["closed_ever"], key["closed_sales"]["closed_now"]])
    plans = [r for r in gold if r.is_plan]
    check("installment plans closed now / open", [sum(r.closed_now for r in plans), sum(1 - r.closed_now for r in plans)],
          [key["closed_sales"]["plan_orders_closed_now"], key["closed_sales"]["plan_orders_open"]])

    # ---- identity stitching against who was really who
    tc = {r["customer_id"]: r["person_id"] for r in rows(tmp / "gen" / "truth" / "identity_customers.csv")}
    cp = {r.customer_id: r.person_id for r in spark.table("gold.tix_customer_person").collect()}
    cluster_people = defaultdict(Counter)
    for c, p in cp.items():
        cluster_people[p][tc[c]] += 1
    check("customer clusters each hold one real person", sum(1 for c in cluster_people.values() if len(c) > 1), 0)
    true_pairs = Counter(tc.values())
    check("duplicate customer records merged", len(set(cp.values())), len(set(tc.values())))
    truth = {(r["user_pseudo_id"], int(r["ga_session_id"])): r["person_id"] for r in rows(tmp / "gen" / "truth" / "identity_sessions.csv")}
    correct = wrong = unresolved = 0
    for r in spark.table("gold.tix_fact_session_identity").collect():
        t_p = truth.get((r.user_pseudo_id, r.ga_session_id))
        if t_p is None:
            continue
        if r.person_id in ("UNRESOLVED", "BOT"):
            unresolved += 1
        elif cluster_people[r.person_id].most_common(1)[0][0] == t_p:
            correct += 1
        else:
            wrong += 1
    atleast("identity: attributed sessions", correct + wrong, 3000)
    atleast("identity: precision x1000", int(1000 * correct / max(1, correct + wrong)), 990)
    print(f"     correct {correct}, wrong {wrong}, left unresolved {unresolved}")

    # ---- be-backs and price at visit against the journey truth
    tj = {j["order_id"]: j for j in json.load(open(tmp / "gen" / "truth" / "journeys.json")) if j.get("order_id")}
    gj = {r.order_id: r for r in spark.sql("SELECT * FROM gold.tix_fact_purchase_journey WHERE purchased = 1").collect()}
    both = [o for o in tj if o in gj]
    exact = sum(1 for o in both if gj[o].be_backs == tj[o]["be_backs_within_window"])
    atleast("journeys matched to a truth journey", len(both), int(0.7 * len(tj)))
    atleast("be-backs exact match x1000 (depends on how often a person re-buys the same event: 0.86 on the default data, 0.73 on the larger ML data)", int(1000 * exact / len(both)), 650)
    clean = [o for o in both if tj[o]["be_backs_all"] == tj[o]["be_backs_within_window"]]
    pexact = sum(1 for o in clean if f"{gj[o].price_at_first_visit}" == tj[o]["price_at_first_visit"])
    atleast("price at first visit exact x1000 (journeys with no visit outside the window)", int(1000 * pexact / len(clean)), 800)
    print(f"     be-backs exact {exact}/{len(both)}; price at first visit exact {pexact}/{len(clean)}")
    drops = spark.sql("SELECT count(*) n, sum(CASE WHEN price_drops_since_first_visit > 0 THEN 1 ELSE 0 END) d FROM gold.tix_fact_purchase_journey WHERE purchased = 1").first()
    atleast("buyers who saw a price drop after first visit", drops.d, 1)
    nb = spark.sql("SELECT count(*) FROM gold.tix_fact_purchase_journey WHERE purchased = 0").first()[0]
    atleast("non-buyer journeys kept", nb, 100)

    # ---- features
    f = spark.sql("SELECT count(*) n, sum(label_mature) m, sum(CASE WHEN label_mature = 1 THEN label_bought_30d ELSE 0 END) pos FROM gold.tix_feature_purchase_propensity").first()
    atleast("propensity feature rows", f.n, 500)
    atleast("propensity mature rows", f.m, 300)
    print(f"     propensity rows {f.n}, mature {f.m}, positives among mature {f.pos}")
    r2 = spark.sql("SELECT count(*) n, sum(label_mature) m, sum(CASE WHEN label_mature = 1 THEN label_reached_80 ELSE 0 END) pos FROM gold.tix_feature_plan_risk").first()
    atleast("plan-risk feature rows", r2.n, 200)
    print(f"     plan-risk rows {r2.n}, mature {r2.m}, reached 80 percent among mature {r2.pos}")

    # ---- optional: train both models on the gold features (set TIX_TRAIN=1; needs pandas and scikit-learn)
    if os.environ.get("TIX_TRAIN"):
        import train_models as TM
        for name, spec in TM.SPECS.items():
            m, model = TM.fit_evaluate(TM._to_pandas(spark, spec["table"]), spec)
            print(f"     {name}: " + json.dumps({k: (round(v, 3) if isinstance(v, float) else v) for k, v in m.items() if k != "features"}))
            check(f"{name}: trained", model is not None, True)
            if model is not None:
                atleast(f"{name}: AUC minus baseline AUC, x1000 (floor {int(1000 * spec['lift_floor'])})", int(1000 * (m["auc"] - m["baseline_auc"])), int(1000 * spec["lift_floor"]))
                atleast(f"{name}: AUC x1000", int(1000 * m["auc"]), 620)

    # ---- plant an ownership-chain break: the gold audit must stop publication and leave the published table alone
    before = spark.table("gold.tix_fact_ticket_transfer").count()
    f1 = sorted((tmp / "tix" / "ticket_transaction").glob("*.csv"))[-1]
    src = next(r for p in sorted((tmp / "tix" / "ticket_transaction").glob("*.csv")) for r in rows(p) if r["sale_type"] == "resale")
    with open(f1, "a", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        for i in range(2):
            r = dict(src)
            r["transaction_id"], r["seller_id"] = f"TXB90000{i}", "C000001" if i == 0 else "C000002"
            ts = G.datetime.strptime(r["sale_ts"], "%Y-%m-%dT%H:%M:%SZ") + G.timedelta(minutes=1 + i)
            r["sale_ts"] = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
            w.writerow([r[k] for k in G.TXN_COLS])
    rc2, out2 = CR.run(tmp, str(tmp / "tix"), "full2", extra=["--product", "tix", "--steps", "bronze:ticket_transaction,silver:ticket_transaction,gold:fact_ticket_transfer,gold-publish", "--fail-on-rejects", "false"])
    check("a chain break ends the run in failure", rc2, 1)
    if rc2 != 1:
        print(out2[-1500:])
    s2 = {r.step: r.status for r in spark.table("governance.pipeline_step_log").where("run_id = 'full2'").collect()}
    check("gold audit failed and publication was skipped", [s2.get("gold:fact_ticket_transfer"), s2.get("gold-publish")], ["failed", "skipped"])
    if s2.get("gold:fact_ticket_transfer") != "failed":
        print(out2[-2500:])
    aud = {r.rule_id: r.violations for r in spark.table("governance.dq_rule_results").where("run_id = 'full2' AND layer = 'gold'").collect()}
    check("chain audit violations", aud.get("FTR-003"), 2)
    check("published ledger untouched", spark.table("gold.tix_fact_ticket_transfer").count(), before)

    print()
    if fails:
        print("\n".join(fails))
        sys.exit(1)
    print("TIX FULL CHECKS PASSED")


if __name__ == "__main__":
    main()
