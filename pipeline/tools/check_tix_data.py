#!/usr/bin/env python3
"""Independent check of the generated tix data: re-derive the answers from the FILES ONLY and compare with answer_key.json.

    python tools/check_tix_data.py data/tix

This is a tiny pure-Python 'reference pipeline'. It does not share code with the generator, so a bug in one shows up as a
disagreement with the other. When your real pipeline runs on Databricks, its numbers must match these too.
"""
import csv
import glob
import json
import os
import sys
from collections import defaultdict
from datetime import datetime


def rows(pattern):
    out = []
    for p in sorted(glob.glob(pattern)):
        with open(p, newline="") as f:
            out.extend(csv.DictReader(f))
    return out


def cents(s):
    neg = s.startswith("-")
    a, _, b = s.lstrip("-").partition(".")
    return (-1 if neg else 1) * (int(a) * 100 + int((b + "00")[:2]))


def ts(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")


def main(root):
    key = json.load(open(f"{root}/answer_key/answer_key.json"))
    errs = []

    def check(name, got, want):
        if got != want:
            errs.append(f"{name}: got {got!r}, expected {want!r}")
        else:
            print(f"  ok   {name}: {got}")

    raw = rows(f"{root}/batches/batch_*/ticket_transaction/*.csv")
    events = {r["event_id"]: r for r in rows(f"{root}/batches/batch_0000/event/event.csv")}
    tickets = {r["ticket_id"] for r in rows(f"{root}/batches/batch_0000/ticket_inventory/*.csv")}
    customers = {r["customer_id"] for r in rows(f"{root}/batches/batch_{key['generated_for_batches']:04d}/customer/customer.csv")}
    print(f"raw transaction rows delivered: {len(raw)}")

    # 1) exact duplicates collapse, conflicting duplicates (same id, different content) are rejected in full
    by_id = defaultdict(set)
    for r in raw:
        by_id[r["transaction_id"]].add(tuple(r.items()))
    conflicts = {i for i, v in by_id.items() if len(v) > 1}
    check("conflicting transaction ids", sorted(conflicts), key["conflict_transaction_ids_excluded"])
    uniq = [dict(next(iter(v))) for i, v in by_id.items() if i not in conflicts]

    # 2) row-level rules
    good, rejected = [], defaultdict(int)
    for r in uniq:
        if r["sale_type"] not in ("primary", "resale", "buyback", "company_resale"):
            rejected["invalid_sale_type"] += 1; continue
        if not r["price"] or cents(r["price"]) <= 0:
            rejected["bad_price"] += 1; continue
        if r["ticket_id"] not in tickets:
            rejected["unknown_ticket"] += 1; continue
        if r["buyer_id"] not in customers | {"COMPANY"} or (r["seller_id"] not in customers | {"COMPANY", "PRIMARY"}):
            rejected["unknown_party"] += 1; continue
        if cents(r["total_paid"]) != cents(r["price"]) + cents(r["buyer_fee"]) or cents(r["seller_payout"]) != cents(r["price"]) - cents(r["seller_fee"]):
            rejected["fee_arithmetic"] += 1; continue
        if r["sale_type"] == "buyback" and r["buyer_id"] != "COMPANY":
            rejected["buyback_buyer"] += 1; continue
        if r["sale_type"] == "company_resale" and r["seller_id"] != "COMPANY":
            rejected["company_resale_seller"] += 1; continue
        if ts(r["sale_ts"]) >= ts(events[r["event_id"]]["start_ts"]):
            rejected["after_event_start"] += 1; continue
        good.append(r)
    print("rejected by row rules:", dict(rejected))
    planted = defaultdict(int)
    for b in key["planted_counts_by_batch"].values():
        for k, v in b.items():
            planted[k] += v
    check("E04 fee arithmetic rows", rejected["fee_arithmetic"], planted["TXN-E04_total_does_not_equal_price_plus_fee"])
    check("E05 buyback buyer rows", rejected["buyback_buyer"], planted["TXN-E05_buyback_buyer_is_not_company"])
    check("E07 invalid sale type rows", rejected["invalid_sale_type"], planted["TXN-E07_invalid_sale_type"])
    check("E08 bad price rows", rejected["bad_price"], planted["TXN-E08_negative_or_missing_price"])
    check("E09 after event start rows", rejected["after_event_start"], planted["TXN-E09_sale_after_event_start"])
    check("E06 unknown ticket rows", rejected["unknown_ticket"], planted["TXN-E06_unknown_ticket"])
    check("E06 unknown customer rows", rejected["unknown_party"], planted["TXN-E06_unknown_customer"])

    # 3) ownership chain (gold audit): this seller is the previous buyer
    chain_breaks = 0
    last = {}
    for r in sorted(good, key=lambda r: (r["sale_ts"], r["transaction_id"])):
        prev = last.get(r["ticket_id"], "PRIMARY")
        if r["seller_id"] != prev:
            chain_breaks += 1
        last[r["ticket_id"]] = r["buyer_id"]
    check("chain breaks found (planted E10 rows)", chain_breaks, planted["TXN-E10_broken_ownership_chain"])

    # 4) ledger totals
    exp = key["ledger"]
    clean = [r for r in good if not r["transaction_id"].startswith("TXB")]
    check("valid transactions", len(clean), exp["valid_transactions"])
    by = defaultdict(lambda: [0, 0, 0])
    profit, cost = 0, {}
    for r in sorted(clean, key=lambda r: (r["sale_ts"], r["transaction_id"])):
        a = by[r["sale_type"]]
        a[0] += 1; a[1] += cents(r["price"]); a[2] += cents(r["buyer_fee"]) + cents(r["seller_fee"])
        if r["sale_type"] == "buyback":
            cost[r["ticket_id"]] = cents(r["price"])
        elif r["sale_type"] == "company_resale":
            profit += cents(r["price"]) - cost[r["ticket_id"]]
    fmt = lambda c: f"{c // 100}.{c % 100:02d}"
    for st, v in exp["by_sale_type"].items():
        check(f"{st} count / gross / fees", [by[st][0], fmt(by[st][1]), fmt(by[st][2])], [v["count"], v["gross"], v["fees"]])
    check("owned-inventory profit", fmt(profit), exp["owned_inventory_profit"])
    check("total earnings", fmt(sum(v[2] for v in by.values()) + profit), exp["total_earnings"])

    # 5) installment plans: closed sales from the payment files
    plans = {r["order_id"]: r for r in rows(f"{root}/batches/batch_*/order_plan/*.csv")}
    pays = defaultdict(list)
    for p in rows(f"{root}/batches/batch_*/payment/*.csv"):
        pays[p["order_id"]].append(p)
    closed_ever = closed_now = plan_closed = plan_all = 0
    for oid, o in plans.items():
        if o["sale_type"] != "primary":
            continue
        net, first, total = 0, None, cents(o["total_amount"])
        for p in sorted(pays[oid], key=lambda p: (p["event_ts"], p["payment_id"])):
            if p["status"] == "captured":
                net += cents(p["amount"])
            elif p["status"] in ("refunded", "chargeback"):
                net -= cents(p["amount"])
            if first is None and net * 5 >= total * 4:
                first = p["event_ts"]
        closed_ever += first is not None
        closed_now += net * 5 >= total * 4
        if int(o["num_payments"]) > 1:
            plan_closed += net * 5 >= total * 4
            plan_all += 1
    check("closed sales (ever)", closed_ever, key["closed_sales"]["closed_ever"])
    check("closed sales (now)", closed_now, key["closed_sales"]["closed_now"])
    check("installment orders closed now / open", [plan_closed, plan_all - plan_closed], [key["closed_sales"]["plan_orders_closed_now"], key["closed_sales"]["plan_orders_open"]])
    bad_total = [oid for oid, o in plans.items() if o["sale_type"] == "primary" and
                 sum(cents(r["total_paid"]) for r in clean if r["order_id"] == oid) not in (0, cents(o["total_amount"]))]
    check("order totals tie to transactions (excluding conflict rows)", len(bad_total) - sum(1 for oid in bad_total if any(r["transaction_id"] in conflicts for r in raw if r["order_id"] == oid)), 0)

    # 6) plan lengths 5..10 all closed in the scenario orders
    lens = sorted({int(o["num_payments"]) for o in plans.values() if int(o["num_payments"]) > 1})
    check("plan lengths present", lens, [5, 6, 7, 8, 9, 10])

    print()
    if errs:
        print("FAILED:")
        print("\n".join("  " + e for e in errs))
        sys.exit(1)
    print("all checks passed")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "data/tix")
