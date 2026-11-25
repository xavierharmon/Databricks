#!/usr/bin/env python3
"""Synthetic data for the `tix` ticket-marketplace project. Everything is fictional; no real people or companies.

    python tools/generate_tix_data.py --out data/tix --seed 7 --batches 6

What it writes (under --out):
  batches/batch_0000/   history load: reference files + everything that happened before --start
  batches/batch_0001/   one simulated day of activity (a second day in batch_0002, and so on)
  answer_key/           what a correct pipeline must produce (counts, totals, closed sales, journeys, identity)
  truth/                ground truth only the generator knows (who is really who). NEVER upload this to the landing zone.

Why a generator and not a few CSVs: the project's rules (ledger chain, fees, installment closing, be-backs, identity
stitching) only prove themselves against data where the right answer is known. Every scenario is planted on purpose and
counted in the answer key, so a pipeline result can be compared to the expected number, not eyeballed.

Design decisions taken from the plan's "Decisions" table (defaults): money in whole cents; UTC timestamps; one currency;
closed sale = net captured payments >= 80 percent of the order total (by amount); be-back window 30 days, same event,
by session; price detail at event+section level; identity stitching from log-in and purchase signals plus a guarded
fuzzy signal; sales of a cancelled event are refunded; corrections are not generated yet (planned).
"""
from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import json
import math
import os
import random
import shutil
from collections import defaultdict
from datetime import date, datetime, timedelta

ISO = "%Y-%m-%dT%H:%M:%SZ"
COMPANY, PRIMARY = "COMPANY", "PRIMARY"          # the two non-customer parties in the ledger


def iso(ts):
    return ts.strftime(ISO) if ts else ""


def money(c):
    return ("-" if c < 0 else "") + f"{abs(c) // 100}.{abs(c) % 100:02d}"


def h(s):
    return hashlib.sha256(s.encode()).hexdigest()[:32]


def bp(cents, bps):
    """cents * basis points / 10000, rounded half up, in whole-number arithmetic (no floating point)."""
    return (cents * bps + 5000) // 10000


# ------------------------------------------------------------------------------------------------ reference lists
SPORTS = {
    "baseball": dict(league="FBL", mult=1.0, teams=[("T01", "Harbor City Gulls"), ("T02", "Ironvale Forgemen"),
        ("T03", "Pinecrest Timberwolves"), ("T04", "Lakeshore Mariners"), ("T05", "Sandhill Scorpions"),
        ("T06", "Redrock Coyotes"), ("T07", "Mesaverde Falcons"), ("T08", "Bayou Bullfrogs")]),   # the FBL teams: shared across projects
    "football": dict(league="FFL", mult=1.8, teams=[("FB01", "Granite Peak Bison"), ("FB02", "Copperfield Hawks"),
        ("FB03", "Seaview Sharks"), ("FB04", "Alder Ridge Stags"), ("FB05", "Cinder Bay Foxes"),
        ("FB06", "Maple Hollow Rangers"), ("FB07", "Stonebridge Lions"), ("FB08", "Windmere Ospreys")]),
    "basketball": dict(league="FBA", mult=1.5, teams=[("BK01", "Harborlight Comets"), ("BK02", "Ashford Owls"),
        ("BK03", "Redwood Titans"), ("BK04", "Crestline Pelicans"), ("BK05", "Dunmore Herons"),
        ("BK06", "Valley Forge Rockets"), ("BK07", "Silverton Bears"), ("BK08", "Eastgate Jets")]),
}
PERFORMERS = [("P01", "The Paper Lanterns"), ("P02", "Velvet Static"), ("P03", "Marlow and the Tides"),
              ("P04", "Neon Orchard"), ("P05", "Gravel Saints"), ("P06", "Aurora Dunes"), ("P07", "Copper Kites"),
              ("P08", "Hollow Parade")]
TAGS = {"baseball": ["rivalry", "family_friendly", "fireworks_night", "opening_day", "bobblehead"],
        "football": ["rivalry", "playoff", "premium", "student_night"],
        "basketball": ["rivalry", "playoff", "family_friendly", "premium"],
        "concert": ["headliner", "all_ages", "festival", "premium"]}
FIRST = ["Avery", "Jordan", "Riley", "Casey", "Morgan", "Quinn", "Parker", "Reese", "Skyler", "Rowan", "Devon", "Harper"]
LAST = ["Alder", "Brooks", "Calloway", "Dalton", "Ellery", "Fairchild", "Garrity", "Hollis", "Ingram", "Jessup"]


class Series:
    """A price history for one event section: sorted (timestamp, cents); `at(ts)` is the as-of lookup."""

    def __init__(self):
        self.ts, self.px = [], []

    def add(self, ts, cents):
        i = bisect.bisect_right(self.ts, ts)
        self.ts.insert(i, ts)
        self.px.insert(i, cents)

    def at(self, ts):
        i = bisect.bisect_right(self.ts, ts) - 1
        return self.px[max(i, 0)]

    def rows(self):
        return list(zip(self.ts, self.px))


# ------------------------------------------------------------------------------------------------ the world
def build_world(cfg, R):
    start = datetime.combine(cfg.start, datetime.min.time())
    hist_start = start - timedelta(days=cfg.history_days)
    end = start + timedelta(days=cfg.batches)
    W = dict(start=start, hist_start=hist_start, end=end)

    # venues and participants
    W["participants"] = []
    W["venues"] = []
    home_venue = {}
    vn = 0
    for sport, sd in SPORTS.items():
        for tid, tname in sd["teams"]:
            vn += 1
            vid = f"V{vn:02d}"
            home_venue[tid] = vid
            W["venues"].append(dict(venue_id=vid, venue_name=f"{tname.split()[0]} {'Park' if sport == 'baseball' else 'Arena'}",
                                    city=tname.split()[0], timezone="America/New_York"))
            W["participants"].append(dict(participant_id=tid, participant_name=tname, participant_type="team", sport=sport))
    for pid, pname in PERFORMERS:
        W["participants"].append(dict(participant_id=pid, participant_name=pname, participant_type="performer", sport=""))
    for k in range(3):
        vn += 1
        W["venues"].append(dict(venue_id=f"V{vn:02d}", venue_name=f"Festival Hall {k + 1}", city="Metro", timezone="America/New_York"))
    concert_venues = [v["venue_id"] for v in W["venues"] if v["venue_name"].startswith("Festival")]

    # events: one table for sports and concerts (category + nullable sport + participants as a link table)
    mix = [("baseball", 0.38), ("football", 0.2), ("basketball", 0.25), ("concert", 0.17)]
    W["events"], W["event_participants"], W["event_tags"], W["sections"] = [], [], [], {}
    for n in range(cfg.events):
        r, acc, kind = R.random(), 0.0, "concert"
        for k, p in mix:
            acc += p
            if r < acc:
                kind = k
                break
        eid = f"E{n + 1:04d}"
        day = R.randint(-4, 110)                                   # a few events already played, most in the future
        st = start + timedelta(days=day, hours=19, minutes=R.choice([0, 5, 30]))
        on_sale = max(hist_start, st - timedelta(days=R.randint(45, 90)))
        if kind == "concert":
            who = R.choice(PERFORMERS)
            venue = R.choice(concert_venues)
            title = f"{who[1]} live"
            category, sport, league = "concert", "", ""
            parts = [(who[0], "performer")]
            secs = {"RESERVED": (R.randint(7000, 14000), 30), "FLOOR_GA": (R.randint(5500, 9500), 40)}
        else:
            sd = SPORTS[kind]
            home, away = R.sample(sd["teams"], 2)
            venue = home_venue[home[0]]
            title = f"{away[1]} at {home[1]}"
            category, sport, league = "sport", kind, sd["league"]
            parts = [(home[0], "home"), (away[0], "away")]
            m = sd["mult"]
            secs = {"LOWER": (int(R.randint(8500, 16000) * m), 20), "MID": (int(R.randint(4500, 8500) * m), 30),
                    "UPPER": (int(R.randint(2000, 4500) * m), 40)}
        W["events"].append(dict(event_id=eid, event_title=title, event_category=category, sport=sport, league=league,
                                venue_id=venue, start_ts=st, on_sale_ts=on_sale, status="scheduled",
                                pop=R.choice([1, 1, 2, 3, 5])))
        for pid, role in parts:
            W["event_participants"].append(dict(event_id=eid, participant_id=pid, role=role))
        for tg in R.sample(TAGS[kind], R.randint(2, min(3, len(TAGS[kind])))):
            W["event_tags"].append(dict(event_id=eid, tag=tg))
        W["sections"][eid] = secs
    W["event_by_id"] = {e["event_id"]: e for e in W["events"]}

    # tickets and price history
    W["tickets"], W["series"] = [], {}
    for e in W["events"]:
        for sec, (base, count) in W["sections"][e["event_id"]].items():
            s = Series()
            t, px = e["on_sale_ts"], base
            s.add(t, px)
            while True:
                t = t + timedelta(days=R.randint(3, 9), hours=R.randint(0, 20))
                if t >= e["start_ts"] - timedelta(hours=3):
                    break
                if R.random() < 0.45:
                    px = max(1500, int(px * (1 - R.uniform(0.03, 0.12))))
                else:
                    px = int(px * (1 + R.uniform(0.02, 0.09)))
                s.add(t, px)
            W["series"][(e["event_id"], sec)] = s
            for i in range(count):
                row = "" if sec == "FLOOR_GA" else str(1 + i // 10)
                seat = "" if sec == "FLOOR_GA" else str(1 + i % 10)
                W["tickets"].append(dict(ticket_id=f"{e['event_id']}-{sec}-{i + 1:03d}", event_id=e["event_id"],
                                         section=sec, row=row, seat=seat, admission=("general" if sec == "FLOOR_GA" else "reserved")))

    # fee schedule (basis points); one primary fee change inside the history so fees must be looked up as-of
    far = hist_start - timedelta(days=365)
    chg = start - timedelta(days=10)
    W["fees"] = []

    def fee(sale, cat, b, s, a, z=None):
        W["fees"].append(dict(rule_id=f"F{len(W['fees']) + 1:03d}", sale_type=sale, event_category=cat, buyer_fee_bps=b,
                              seller_fee_bps=s, effective_from=a, effective_to=z))
    fee("primary", "sport", 1200, 0, far, chg); fee("primary", "sport", 1300, 0, chg)
    fee("primary", "concert", 1800, 0, far)
    fee("resale", "sport", 1000, 800, far); fee("resale", "concert", 1400, 1000, far)
    fee("buyback", "sport", 0, 0, far); fee("buyback", "concert", 0, 0, far)
    fee("company_resale", "sport", 1200, 0, far); fee("company_resale", "concert", 1800, 0, far)
    W["rules"] = [dict(rule_name="closed_threshold_pct", value="80", effective_from=far),
                  dict(rule_name="closed_basis", value="amount", effective_from=far),
                  dict(rule_name="be_back_window_days", value="30", effective_from=far),
                  dict(rule_name="plan_interval_days", value=str(cfg.plan_interval_days), effective_from=far)]

    # people, customer records (some people have two), devices (with cookie resets and shared household devices)
    W["persons"] = {}
    cn = 0
    for i in range(cfg.persons):
        pid = f"PER{i + 1:05d}"
        email = f"{pid.lower()}@example.test"
        p = dict(person_id=pid, email_hash=h(email), phone_hash=h(f"555-{i:07d}"), postal=f"{R.randint(10000, 99999)}",
                 first=R.choice(FIRST), last=R.choice(LAST), customers=[], devices=[],
                 pref_sport=R.choice(["baseball", "football", "basketball", "concert"]), pref_tier=R.choice([0, 1, 1, 2]))
        for variant in range(2 if R.random() < cfg.dup_customer_rate else 1):
            cn += 1
            by_email = (variant == 0) or R.random() < 0.6                       # the second record sometimes differs in email
            p["customers"].append(dict(customer_id=f"C{cn:06d}", person=pid, first_name=p["first"], last_name=p["last"],
                                       favorite_sport=(p["pref_sport"] if int(h(pid)[:2], 16) % 10 < 7 else ""),        # declared in the profile for about 70 percent of people
                                       email_hash=p["email_hash"] if by_email else h(f"alt.{email}"),
                                       phone_hash=p["phone_hash"], postal_code=p["postal"]))
        W["persons"][pid] = p
    W["customer_by_id"] = {c["customer_id"]: c for p in W["persons"].values() for c in p["customers"]}

    dn = 0

    def new_device(owner_ids, kind):
        nonlocal dn
        dn += 1
        did = f"D{dn:05d}"
        cookies, t = [], hist_start - timedelta(days=40)
        while t < end:
            cookies.append((t, f"{R.randint(10 ** 9, 2 * 10 ** 9 - 1)}.{int(t.timestamp())}"))
            t = t + timedelta(days=R.randint(15, 70))
        return dict(device_id=did, kind=kind, owners=owner_ids, cookies=cookies, fp=h(f"fp{did}"),
                    mobile=(kind == "phone"), ip=h(f"ip{owner_ids[0]}"))

    ids = list(W["persons"])
    R.shuffle(ids)
    shared_pairs = [(ids[2 * k], ids[2 * k + 1]) for k in range(int(len(ids) * cfg.shared_device_rate / 2))]
    W["devices"] = {}
    for a, b in shared_pairs:                                                  # a household device used by two people
        d = new_device([a, b], "desktop")
        W["devices"][d["device_id"]] = d
        W["persons"][a]["devices"].append(d["device_id"]); W["persons"][b]["devices"].append(d["device_id"])
    for pid, p in W["persons"].items():
        for kind in R.sample(["phone", "laptop", "tablet"], R.randint(1, 3)):
            d = new_device([pid], kind)
            W["devices"][d["device_id"]] = d
            p["devices"].append(d["device_id"])
    W["bots"] = [f"{R.randint(10 ** 9, 2 * 10 ** 9 - 1)}.{int(hist_start.timestamp())}" for _ in range(3)]
    W["shared_devices"] = [d["device_id"] for d in W["devices"].values() if len(d["owners"]) > 1]
    return W


def fee_for(W, sale, cat, ts):
    for f in W["fees"]:
        if f["sale_type"] == sale and f["event_category"] == cat and f["effective_from"] <= ts and (f["effective_to"] is None or ts < f["effective_to"]):
            return f["buyer_fee_bps"], f["seller_fee_bps"]
    raise KeyError((sale, cat, ts))


# ------------------------------------------------------------------------------------------------ intents, ledger
def sample_prior_visits(R, ts, on_sale):
    k = R.choices([0, 1, 2, 3, 4, 5], [40, 25, 15, 10, 6, 4])[0]
    out = []
    for i in range(k):
        gap = R.uniform(0.3, 26) if not (i == 0 and R.random() < 0.12) else R.uniform(32, 55)   # a few visits fall outside the window
        v = ts - timedelta(days=gap, hours=R.uniform(-3, 3))
        if v >= on_sale and v < ts - timedelta(minutes=30):
            out.append(v)
    return sorted(out)


def plan_intents(W, cfg, R):
    """Phase 1: who intends to buy what and when, plus their earlier visits. Planted price drops go in the price history
    BEFORE any ledger price is computed, so every price in the data agrees with the final history."""
    intents, day = [], W["hist_start"]
    while day < W["end"]:
        rate = cfg.history_rate if day < W["start"] else cfg.batch_rate
        for _ in range(max(1, int(rate * R.uniform(0.8, 1.2)))):
            ts = day + timedelta(seconds=R.randint(0, 86399))
            pool = [e for e in W["events"] if e["on_sale_ts"] <= ts < e["start_ts"] - timedelta(hours=3)]
            if not pool:
                continue
            e = R.choices(pool, [x["pop"] for x in pool])[0]
            if cfg.signal == "strong":                       # people mostly buy the sport they prefer
                plist = list(W["persons"].values())
                per = R.choices(plist, [12 if p["pref_sport"] == (e["sport"] or "concert") else 1 for p in plist])[0]
            else:
                per = R.choice(list(W["persons"].values()))
            secs = list(W["sections"][e["event_id"]])
            sec = secs[min(per["pref_tier"], len(secs) - 1)] if R.random() < 0.7 else R.choice(secs)
            qty = R.choices([1, 2, 3, 4], [50, 30, 12, 8])[0]
            prior = sample_prior_visits(R, ts, e["on_sale_ts"])
            drop_ts = None
            if prior and R.random() < 0.35 and (ts - prior[-1]) > timedelta(hours=4):   # plant a price drop between last visit and purchase
                drop_ts = prior[-1] + (ts - prior[-1]) * R.uniform(0.2, 0.8)
                s = W["series"][(e["event_id"], sec)]
                s.add(drop_ts, max(1500, int(s.at(drop_ts) * (1 - R.uniform(0.08, 0.15)))))
            intents.append(dict(ts=ts, event=e, person=per, section=sec, qty=qty, prior=prior, planted_drop=drop_ts))
        day += timedelta(days=1)
    intents.sort(key=lambda x: x["ts"])
    return intents


def run_ledger(W, cfg, R, intents):
    """Phase 2: walk the intents in time order, assign real tickets, write ledger rows and each ticket's later life."""
    avail = defaultdict(list)
    for t in W["tickets"]:
        avail[(t["event_id"], t["section"])].append(t)
    for v in avail.values():
        R.shuffle(v)
    cancel_ev = None
    if cfg.batches >= 3:
        cands = [e for e in W["events"] if e["start_ts"] > W["start"] + timedelta(days=5) and e["on_sale_ts"] < W["start"]]
        if cands:
            cancel_ev = R.choice(cands)
            cancel_ev["cancel_ts"] = W["start"] + timedelta(days=2, hours=10)
    W["cancel_event"] = cancel_ev
    tx, orders, journeys, counter = [], {}, [], [0, 0]
    forced_left = 6

    def cust_of(p):
        return R.choice(p["customers"])["customer_id"]

    def new_tx(**kw):
        counter[0] += 1
        return dict(transaction_id=f"TX{counter[0]:07d}", **kw)

    def new_order():
        counter[1] += 1
        return f"O{counter[1]:07d}"

    def row(sale, e, t, seller, buyer, price, ts, order):
        bb, sb = fee_for(W, sale, e["event_category"], ts)
        bf, sf = bp(price, bb), bp(price, sb)
        return new_tx(ticket_id=t["ticket_id"], event_id=e["event_id"], sale_type=sale, seller_id=seller, buyer_id=buyer,
                      sale_ts=ts, price=price, buyer_fee=bf, seller_fee=sf, total_paid=price + bf, seller_payout=price - sf,
                      order_id=order, status="completed", section=t["section"])

    for it in intents:
        e, per, sec, ts = it["event"], it["person"], it["section"], it["ts"]
        if e.get("cancel_ts") and ts >= e["cancel_ts"]:
            continue
        pool = avail[(e["event_id"], sec)]
        if len(pool) < it["qty"]:
            continue
        order = new_order()
        price = W["series"][(e["event_id"], sec)].at(ts)
        buyer = cust_of(per)
        tks = [pool.pop() for _ in range(it["qty"])]
        rows = [row("primary", e, t, PRIMARY, buyer, price, ts, order) for t in tks]
        tx.extend(rows)
        orders[order] = dict(order_id=order, customer_id=buyer, person=per["person_id"], event_id=e["event_id"], order_ts=ts,
                             sale_type="primary", total=sum(r["total_paid"] for r in rows), txs=[r["transaction_id"] for r in rows])
        journeys.append(dict(person=per["person_id"], event_id=e["event_id"], section=sec, ts=ts, order_id=order,
                             prior=it["prior"], sale_type="primary", planted_drop=it["planted_drop"]))
        cutoff = min(W["end"], e["start_ts"] - timedelta(hours=2))
        if e.get("cancel_ts"):
            cutoff = min(cutoff, e["cancel_ts"])
        for t, r0 in zip(tks, rows):                                           # the later life of each ticket
            force = forced_left > 0 and e["start_ts"] > ts + timedelta(days=50)
            if force:
                forced_left -= 1
            plan = ["resale", "resale", "buyback", "company_resale"] if force else None
            owner_person, owner_cust, last_price, state, t_cur, owners = per, buyer, price, "cust", ts, {per["person_id"]}
            hop = 0
            while True:
                if plan:
                    if hop >= len(plan):
                        break
                    nxt = plan[hop]
                elif state == "cust":
                    x = R.random()
                    nxt = "resale" if x < 0.28 else ("buyback" if x < 0.40 else None)
                else:
                    nxt = "company_resale" if R.random() < 0.85 else None
                if nxt is None:
                    break
                t_cur = t_cur + timedelta(days=(3 + 4 * hop) if plan else R.uniform(0.5, 15), minutes=R.randint(0, 600))
                if t_cur > cutoff:
                    break
                if nxt == "buyback":
                    p2 = int(last_price * 0.80)
                    rw = row("buyback", e, t, owner_cust, COMPANY, p2, t_cur, "")
                    tx.append(rw)
                    state, owner_cust, owner_person, last_price = "company", COMPANY, None, p2
                else:
                    base = W["series"][(e["event_id"], sec)].at(t_cur)
                    p2 = int(last_price * R.uniform(1.15, 1.45)) if nxt == "company_resale" else int(base * R.uniform(0.85, 1.35))
                    cands = [p for p in W["persons"].values() if p["person_id"] not in owners]
                    if cfg.signal == "strong":                       # resale buyers favour their sport too
                        nb = R.choices(cands, [12 if p["pref_sport"] == (e["sport"] or "concert") else 1 for p in cands])[0]
                    else:
                        nb = R.choice(cands)
                    owners.add(nb["person_id"])
                    bc = cust_of(nb)
                    oid = new_order()
                    rw = row(nxt, e, t, owner_cust, bc, p2, t_cur, oid)
                    tx.append(rw)
                    orders[oid] = dict(order_id=oid, customer_id=bc, person=nb["person_id"], event_id=e["event_id"], order_ts=t_cur,
                                       sale_type=nxt, total=rw["total_paid"], txs=[rw["transaction_id"]])
                    journeys.append(dict(person=nb["person_id"], event_id=e["event_id"], section=sec, ts=t_cur, order_id=oid,
                                         prior=sample_prior_visits(R, t_cur, e["on_sale_ts"]), sale_type=nxt, planted_drop=None))
                    state, owner_cust, owner_person, last_price = "cust", bc, nb, p2
                hop += 1
    tx = [r for r in tx if r["sale_ts"] <= W["end"]]
    keep = {r["order_id"] for r in tx if r["order_id"]}
    orders = {k: v for k, v in orders.items() if k in keep}
    journeys = [j for j in journeys if j["order_id"] in keep]
    tx.sort(key=lambda r: (r["sale_ts"], r["transaction_id"]))
    return tx, orders, journeys


# ------------------------------------------------------------------------------------------------ payments
def run_payments(W, cfg, R, orders):
    pays, pid = [], [0]
    interval = timedelta(days=cfg.plan_interval_days)

    def add(order, seq, attempt, due, ts, amount, status, orig=""):
        pid[0] += 1
        pays.append(dict(payment_id=f"PAY{pid[0]:07d}", order_id=order["order_id"], payment_seq=seq, attempt_no=attempt,
                         due_date=due.date().isoformat(), event_ts=ts, amount=amount, status=status, original_payment_id=orig))
        return pays[-1]

    prim = sorted((o for o in orders.values() if o["sale_type"] == "primary"), key=lambda o: o["order_ts"])
    forced = [5, 6, 7, 8, 9, 10, 5, 6, 7, 8, 9, 10]
    n_forced = 0
    for o in prim:
        e = W["event_by_id"][o["event_id"]]
        plan = None
        if n_forced < len(forced) and o["order_ts"] < W["start"] - timedelta(days=75) + timedelta(days=cfg.history_days):
            plan = forced[n_forced]
            n_forced += 1
            o["scenario"] = "forced_plan"
        elif R.random() < 0.35:
            plan = R.randint(5, 10)
        n = plan or 1
        o["num_payments"] = n
        o["plan_interval_days"] = cfg.plan_interval_days if plan else 0
        if o["total"] < n:
            o["num_payments"] = n = 1
        base = o["total"] // n
        amounts = [base] * (n - 1) + [o["total"] - base * (n - 1)]           # equal payments, last one absorbs the cents
        stop = False
        for k in range(n):
            due = o["order_ts"] + k * interval
            if stop or due > W["end"]:
                break
            cancel = e.get("cancel_ts")
            if cancel and due >= cancel:
                break
            clean = o.get("scenario") == "forced_plan"
            r = R.random()
            shift = 0.0
            if cfg.signal == "strong":                       # longer plans and concerts default more often (the default rate was a flat 2 percent)
                shift = min(0.4, 0.01 + 0.02 * (n - 4) + (0.05 if e["event_category"] == "concert" else 0.0)) - 0.02
            if clean or r < 0.92 - shift:
                add(o, k + 1, 1, due, due, amounts[k], "captured")
            elif r < 0.98 - shift:
                add(o, k + 1, 1, due, due, amounts[k], "failed")
                if due + timedelta(days=1) <= W["end"]:
                    add(o, k + 1, 2, due, due + timedelta(days=1), amounts[k], "captured")
            else:
                add(o, k + 1, 1, due, due, amounts[k], "failed")                 # a default: the plan stops here
                stop = True
    # refund-after-close scenarios: one order drops below 80 percent, one stays above it
    by = defaultdict(list)
    for p in pays:
        by[p["order_id"]].append(p)
    full = [o for o in prim if o["num_payments"] >= 8 and len(by[o["order_id"]]) == o["num_payments"] and
            all(p["status"] == "captured" for p in by[o["order_id"]]) and
            max(p["event_ts"] for p in by[o["order_id"]]) + timedelta(days=1) <= W["end"]]
    W["refund_scenarios"] = []
    for o, how_many in zip(full[:2], [3, 1]):                                  # 3 refunds drop below 80 percent; 1 refund stays above it
        last = max(p["event_ts"] for p in by[o["order_id"]])
        for p in sorted(by[o["order_id"]], key=lambda p: p["payment_seq"])[-how_many:]:
            add(o, p["payment_seq"], 1, datetime.fromisoformat(p["due_date"]), last + timedelta(days=1), p["amount"], "refunded", p["payment_id"])
        W["refund_scenarios"].append(dict(order_id=o["order_id"], plan_length=o["num_payments"], refunded_payments=how_many))
    ce = W["cancel_event"]
    if ce:                                                                      # a cancelled event: every captured payment is refunded
        for p in list(pays):
            o = orders[p["order_id"]]
            if o["event_id"] == ce["event_id"] and p["status"] == "captured" and p["event_ts"] < ce["cancel_ts"]:
                add(o, p["payment_seq"], 1, datetime.fromisoformat(p["due_date"]), ce["cancel_ts"] + timedelta(hours=1), p["amount"], "refunded", p["payment_id"])
    pays = [p for p in pays if p["event_ts"] <= W["end"]]
    pays.sort(key=lambda p: (p["event_ts"], p["payment_id"]))
    return pays


def closed_status(order, pays):
    """The expected answer: first time net captured money reaches 80 percent (by amount, integer maths) and whether it still does."""
    net, first = 0, None
    for p in sorted((p for p in pays if p["order_id"] == order["order_id"]), key=lambda p: (p["event_ts"], p["payment_id"])):
        if p["status"] == "captured":
            net += p["amount"]
        elif p["status"] in ("refunded", "chargeback"):
            net -= p["amount"]
        if first is None and net * 5 >= order["total"] * 4:
            first = p["event_ts"]
    return dict(order_id=order["order_id"], total=order["total"], net_paid=net, first_closed_ts=iso(first), closed_ever=first is not None,
                closed_now=net * 5 >= order["total"] * 4, num_payments=order["num_payments"])


# ------------------------------------------------------------------------------------------------ web analytics
def run_analytics(W, cfg, R, journeys, orders, tx):
    """One session per visit. Visits before the purchase are anonymous or logged in; some purchases are guest checkouts.
    Cookie resets, shared devices and bots are all planted and counted in the truth file."""
    events, truth_sessions, truth_journeys = [], [], []
    pers = W["persons"]
    tx_by_order = defaultdict(list)
    for r in tx:
        if r["order_id"]:
            tx_by_order[r["order_id"]].append(r)

    def cookie_at(dev, ts):
        i = bisect.bisect_right([c[0] for c in dev["cookies"]], ts) - 1
        return dev["cookies"][max(i, 0)][1]

    def one_session(person, ts, ev, section, purchase_order=None, guest_email=True, logged_in_p=0.45):
        dev = W["devices"][R.choice(person["devices"])]
        pseudo = cookie_at(dev, ts)
        sid = int(ts.timestamp())
        ip = dev["ip"] if not dev["mobile"] else h(f"mob{R.randint(1, 4000)}")
        seq = []
        logged = R.random() < logged_in_p
        user_id = R.choice(person["customers"])["customer_id"] if logged else None

        def add(name, offset, params=None, items=None, user=None, ecommerce=None):
            when = ts + timedelta(seconds=offset)
            ev_params = [dict(key="ga_session_id", value=dict(int_value=sid)), dict(key="client_fp", value=dict(string_value=dev["fp"])),
                         dict(key="ip_hash", value=dict(string_value=ip))]
            for k, v in (params or {}).items():
                ev_params.append(dict(key=k, value=dict(string_value=str(v)) if isinstance(v, str) else dict(int_value=v)))
            events.append(dict(event_date=when.strftime("%Y%m%d"), event_timestamp=int(when.timestamp() * 1_000_000), event_name=name,
                               user_pseudo_id=pseudo, user_id=user, device=dict(category=dev["kind"]), geo=dict(country="US"),
                               traffic_source=dict(source=R.choice(["google", "direct", "email", "social"]), medium=R.choice(["organic", "none", "cpc"])),
                               event_params=ev_params, items=items or [], ecommerce=ecommerce, _when=when))
        add("page_view", 0, dict(page_location="/home"))
        if logged:
            add("login", 12, dict(method="password"), user=user_id)
        uid = user_id
        if ev:
            e = ev
            price = W["series"][(e["event_id"], section)].at(ts)
            item = [dict(item_id=e["event_id"], item_name=e["event_title"], item_category=e["sport"] or "concert",
                         item_variant=section, price=money(price), quantity=1)]
            add("page_view", 30, dict(page_location=f"/events/{e['event_id']}", event_id=e["event_id"]), user=uid)
            add("view_item", 45, dict(event_id=e["event_id"]), items=item, user=uid)
            if purchase_order:
                add("begin_checkout", 160, dict(event_id=e["event_id"]), items=item, user=uid)
                p = dict(event_id=e["event_id"])
                if not logged and guest_email:
                    p["email_hash"] = person["email_hash"]                    # a guest checkout still leaves an email fingerprint
                add("purchase", 260, p, items=item, user=uid, ecommerce=dict(transaction_id=purchase_order))
        return dict(person=person["person_id"], device=dev["device_id"], pseudo=pseudo, session=sid, ts=ts, logged=logged,
                    shared=len(dev["owners"]) > 1, event_id=ev["event_id"] if ev else "")

    for j in journeys:
        per, e = pers[j["person"]], W["event_by_id"][j["event_id"]]
        if R.random() > cfg.analytics_coverage:
            continue
        visits = []
        for v in j["prior"]:
            visits.append(one_session(per, v, e, j["section"]))
        visits.append(one_session(per, j["ts"] - timedelta(seconds=240), e, j["section"], purchase_order=j["order_id"]))
        for v in visits:
            truth_sessions.append(dict(person=v["person"], device=v["device"], pseudo=v["pseudo"], session=v["session"], ts=iso(v["ts"]),
                                       event_id=v["event_id"], shared_device=v["shared"], logged_in=v["logged"]))
        win = W["rules_window_days"]
        within = [v for v in visits[:-1] if (j["ts"] - v["ts"]).total_seconds() <= win * 86400]
        s = W["series"][(j["event_id"], j["section"])]
        first_v = visits[0]["ts"]
        truth_journeys.append(dict(person=j["person"], event_id=j["event_id"], order_id=j["order_id"], sale_type=j["sale_type"],
                                   visits_before_purchase=len(visits), be_backs_all=len(visits) - 1, be_backs_within_window=len(within),
                                   first_visit_ts=iso(first_v), purchase_ts=iso(j["ts"]), days_to_purchase=round((j["ts"] - first_v).total_seconds() / 86400, 3),
                                   price_at_first_visit=money(s.at(first_v)), price_at_purchase=money(sum(r["price"] for r in tx_by_order[j["order_id"]]) // max(1, len(tx_by_order[j["order_id"]]))),
                                   catalog_price_at_purchase=money(s.at(j["ts"])), planted_price_drop=bool(j["planted_drop"]),
                                   drop_between_first_visit_and_purchase=any(first_v < t <= j["ts"] and p < s.at(first_v) for t, p in s.rows())))
    # non-buyers: they browse and leave
    pool = list(pers.values())
    for _ in range(int(len(journeys) * cfg.nonbuyer_ratio)):
        per, e = R.choice(pool), R.choice(W["events"])
        base = W["hist_start"] + timedelta(days=R.uniform(0, cfg.history_days + cfg.batches - 1))
        if not (e["on_sale_ts"] <= base < e["start_ts"]):
            continue
        sec = R.choice(list(W["sections"][e["event_id"]]))
        for k in range(R.randint(1, 4)):
            ts = base + timedelta(days=k * R.uniform(0.5, 6))
            if ts >= W["end"] or ts >= e["start_ts"]:
                break
            v = one_session(per, ts, e, sec)
            truth_sessions.append(dict(person=v["person"], device=v["device"], pseudo=v["pseudo"], session=v["session"], ts=iso(v["ts"]),
                                       event_id=v["event_id"], shared_device=v["shared"], logged_in=v["logged"]))
        truth_journeys.append(dict(person=per["person_id"], event_id=e["event_id"], order_id="", sale_type="", visits_before_purchase=0, purchased=False))
    for b in W["bots"]:                                                        # bots: very frequent, never buy, never log in
        t = W["hist_start"] + timedelta(hours=R.randint(1, 48))
        for k in range(180):
            e = R.choice(W["events"])
            events.append(dict(event_date=t.strftime("%Y%m%d"), event_timestamp=int(t.timestamp() * 1_000_000), event_name="page_view",
                               user_pseudo_id=b, user_id=None, device=dict(category="desktop"), geo=dict(country="US"),
                               traffic_source=dict(source="direct", medium="none"),
                               event_params=[dict(key="ga_session_id", value=dict(int_value=int(t.timestamp()) // 600 * 600)),
                                             dict(key="page_location", value=dict(string_value=f"/events/{e['event_id']}")),
                                             dict(key="client_fp", value=dict(string_value="botfp")), dict(key="ip_hash", value=dict(string_value="botip"))],
                               items=[], ecommerce=None, _when=t))
            t += timedelta(minutes=R.randint(1, 4))
    events = [x for x in events if x["_when"] <= W["end"]]
    events.sort(key=lambda x: x["event_timestamp"])
    return events, truth_sessions, truth_journeys


# ------------------------------------------------------------------------------------------------ batches, planted errors
def batch_of(W, ts):
    return 0 if ts < W["start"] else 1 + int((ts - W["start"]).total_seconds() // 86400)


TXN_COLS = ["transaction_id", "ticket_id", "event_id", "sale_type", "seller_id", "buyer_id", "sale_ts", "price", "buyer_fee", "seller_fee",
            "total_paid", "seller_payout", "order_id", "currency", "status"]


def tx_csv_row(r):
    return [r["transaction_id"], r["ticket_id"], r["event_id"], r["sale_type"], r["seller_id"], r["buyer_id"], iso(r["sale_ts"]), money(r["price"]),
            money(r["buyer_fee"]), money(r["seller_fee"]), money(r["total_paid"]), money(r["seller_payout"]), r["order_id"], "USD", r["status"]]


def plant_transaction_errors(W, cfg, R, tx):
    """Return {batch: [csv rows]} and the answer-key list of what was planted where. Planted rows are EXTRA rows or copies, so the
    expected ledger totals stay equal to the clean ledger, except for conflicts, which remove their (valid) original by design."""
    per_batch = defaultdict(list)
    delivered_late = 0
    for r in tx:
        b = batch_of(W, r["sale_ts"])
        if b < cfg.batches and R.random() < cfg.late_rate and b > 0:
            b += 1
            delivered_late += 1
        r["delivery_batch"] = b
        per_batch[b].append(r)
    last_of_ticket = {}
    for r in tx:
        last_of_ticket[r["ticket_id"]] = r
    out, planted = defaultdict(list), defaultdict(lambda: defaultdict(list))
    seq = [0]
    customers = list(W["customer_by_id"])

    def bad_id():
        seq[0] += 1
        return f"TXB{seq[0]:06d}"

    for b in range(cfg.batches + 1):
        rows = per_batch.get(b, [])
        csv_rows = [tx_csv_row(r) for r in rows]
        n = max(len(rows), 10)
        k = lambda frac: max(1, int(n * frac))
        sample = lambda m: R.sample(rows, min(m, len(rows))) if rows else []
        for r in sample(k(0.01)):                                              # E01 exact duplicate in the same file
            csv_rows.append(tx_csv_row(r)); planted[b]["TXN-E01_exact_duplicate_same_file"].append(r["transaction_id"])
        if b > 0:                                                              # E02 the same row delivered again in a later batch
            earlier = [r for bb in range(b) for r in per_batch.get(bb, [])]
            for r in (R.sample(earlier, min(k(0.01), len(earlier))) if earlier else []):
                csv_rows.append(tx_csv_row(r)); planted[b]["TXN-E02_redelivered_from_earlier_batch"].append(r["transaction_id"])
        finals = [r for r in rows if last_of_ticket[r["ticket_id"]] is r]
        for r in (R.sample(finals, min(2, len(finals))) if finals else []):    # E03 conflict: same id, different price; both rows are rejected
            c = dict(r); c["price"] = r["price"] + 500
            c["total_paid"] = c["price"] + c["buyer_fee"]; c["seller_payout"] = c["price"] - c["seller_fee"]
            csv_rows.append(tx_csv_row(c)); planted[b]["TXN-E03_conflicting_duplicate"].append(r["transaction_id"])
        pool = [r for r in rows if r["sale_type"] != "buyback"] or rows
        for r in (R.sample(pool, min(k(0.01), len(pool))) if pool else []):   # E04 fee arithmetic wrong
            c = dict(r); c["transaction_id"] = bad_id(); c["total_paid"] = r["total_paid"] + 137
            csv_rows.append(tx_csv_row(c)); planted[b]["TXN-E04_total_does_not_equal_price_plus_fee"].append(c["transaction_id"])
        bb_rows = [r for r in rows if r["sale_type"] == "buyback"]
        for r in (R.sample(bb_rows, min(1, len(bb_rows))) if bb_rows else []):  # E05 buyback whose buyer is not the company
            c = dict(r); c["transaction_id"] = bad_id(); c["buyer_id"] = R.choice(customers)
            csv_rows.append(tx_csv_row(c)); planted[b]["TXN-E05_buyback_buyer_is_not_company"].append(c["transaction_id"])
        for r in sample(1):                                                    # E06 unknown customer, and an unknown ticket
            c = dict(r); c["transaction_id"] = bad_id(); c["buyer_id"] = "C999999"
            csv_rows.append(tx_csv_row(c)); planted[b]["TXN-E06_unknown_customer"].append(c["transaction_id"])
            c = dict(r); c["transaction_id"] = bad_id(); c["ticket_id"] = "E9999-LOWER-001"
            csv_rows.append(tx_csv_row(c)); planted[b]["TXN-E06_unknown_ticket"].append(c["transaction_id"])
        for r in sample(1):                                                    # E07 invalid sale type
            c = dict(r); c["transaction_id"] = bad_id(); c["sale_type"] = "gift"
            csv_rows.append(tx_csv_row(c)); planted[b]["TXN-E07_invalid_sale_type"].append(c["transaction_id"])
        for r in sample(2):                                                    # E08 missing or negative price
            c = dict(r); c["transaction_id"] = bad_id(); c["price"] = -abs(r["price"])
            csv_rows.append(tx_csv_row(c)); planted[b]["TXN-E08_negative_or_missing_price"].append(c["transaction_id"])
        for r in sample(1):                                                    # E09 sale after the event started
            c = dict(r); c["transaction_id"] = bad_id(); c["sale_ts"] = W["event_by_id"][r["event_id"]]["start_ts"] + timedelta(hours=3)
            csv_rows.append(tx_csv_row(c)); planted[b]["TXN-E09_sale_after_event_start"].append(c["transaction_id"])
        if cfg.chain_break_batch == b and rows:                                # E10 chain break: passes every row rule, fails the gold chain audit
            cb_pool = [r for r in rows if r["sale_type"] in ("resale", "buyback")] or rows      # a primary or company-resale row would be caught by a row rule instead
            for r in (R.sample(cb_pool, min(2, len(cb_pool))) if cb_pool else []):
                c = dict(r); c["transaction_id"] = bad_id(); c["seller_id"] = R.choice(customers)
                c["sale_ts"] = r["sale_ts"] + timedelta(minutes=1)
                csv_rows.append(tx_csv_row(c)); planted[b]["TXN-E10_broken_ownership_chain"].append(c["transaction_id"])
        R.shuffle(csv_rows)
        out[b] = csv_rows
    conflicts = {t for b in planted for t in planted[b].get("TXN-E03_conflicting_duplicate", [])}
    return out, planted, conflicts, delivered_late


def write_csv(path, header, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="data/tix")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--start", type=date.fromisoformat, default=date(2026, 10, 1), help="first day after the history load")
    ap.add_argument("--history-days", type=int, default=60)
    ap.add_argument("--batches", type=int, default=6, help="simulated days after the history load")
    ap.add_argument("--events", type=int, default=48)
    ap.add_argument("--persons", type=int, default=300)
    ap.add_argument("--history-rate", type=int, default=16, help="orders per day in the history period")
    ap.add_argument("--batch-rate", type=int, default=40, help="orders per day in each batch")
    ap.add_argument("--plan-interval-days", type=int, default=7)
    ap.add_argument("--dup-customer-rate", type=float, default=0.08)
    ap.add_argument("--shared-device-rate", type=float, default=0.10)
    ap.add_argument("--analytics-coverage", type=float, default=0.75, help="share of purchases that have a web session")
    ap.add_argument("--nonbuyer-ratio", type=float, default=0.8)
    ap.add_argument("--late-rate", type=float, default=0.04)
    ap.add_argument("--signal", choices=["none", "strong"], default="none",
                    help="strong: buyers favour their preferred sport and longer or concert plans stall more often, so models have real structure to learn. "
                         "none (default) keeps the original numbers")
    ap.add_argument("--chain-break-batch", type=int, default=-1, help="plant ownership-chain breaks in this batch (-1 = none: a clean run)")
    cfg = ap.parse_args(argv)
    R = random.Random(cfg.seed)

    W = build_world(cfg, R)
    W["rules_window_days"] = 30
    intents = plan_intents(W, cfg, R)
    tx, orders, journeys = run_ledger(W, cfg, R, intents)
    pays = run_payments(W, cfg, R, orders)
    ga, truth_sessions, truth_journeys = run_analytics(W, cfg, R, journeys, orders, tx)
    tx_rows, planted, conflicts, late = plant_transaction_errors(W, cfg, R, tx)

    out = cfg.out
    if os.path.isdir(out):
        shutil.rmtree(out)
    nb = cfg.batches
    # ---- reference files, once, in batch 0
    b0 = f"{out}/batches/batch_0000"
    write_csv(f"{b0}/venue/venue_0000.csv", ["venue_id", "venue_name", "city", "timezone"], [[v[k] for k in ("venue_id", "venue_name", "city", "timezone")] for v in W["venues"]])
    write_csv(f"{b0}/participant/participant_0000.csv", ["participant_id", "participant_name", "participant_type", "sport"], [[p[k] for k in ("participant_id", "participant_name", "participant_type", "sport")] for p in W["participants"]])
    write_csv(f"{b0}/event_participant/event_participant_0000.csv", ["event_id", "participant_id", "role"], [[p["event_id"], p["participant_id"], p["role"]] for p in W["event_participants"]])
    write_csv(f"{b0}/event_tag/event_tag_0000.csv", ["event_id", "tag"], [[p["event_id"], p["tag"]] for p in W["event_tags"]])
    write_csv(f"{b0}/ticket_inventory/ticket_inventory_0000.csv", ["ticket_id", "event_id", "section", "row", "seat", "admission"], [[t[k] for k in ("ticket_id", "event_id", "section", "row", "seat", "admission")] for t in W["tickets"]])
    write_csv(f"{b0}/fee_schedule/fee_schedule_0000.csv", ["rule_id", "sale_type", "event_category", "buyer_fee_bps", "seller_fee_bps", "effective_from", "effective_to"],
              [[f["rule_id"], f["sale_type"], f["event_category"], f["buyer_fee_bps"], f["seller_fee_bps"], iso(f["effective_from"]), iso(f["effective_to"])] for f in W["fees"]])
    write_csv(f"{b0}/business_rule/business_rule_0000.csv", ["rule_name", "value", "effective_from"], [[r["rule_name"], r["value"], iso(r["effective_from"])] for r in W["rules"]])
    # ---- snapshots and streams, per batch
    first_buy = {}
    for r in tx:
        first_buy.setdefault(r["buyer_id"], r["sale_ts"])
    cust_rows = []
    for c in W["customer_by_id"].values():
        created = min(W["hist_start"] - timedelta(days=R.randint(1, 300)), first_buy.get(c["customer_id"], W["end"]) - timedelta(hours=1)) if R.random() < 0.85 \
            else first_buy.get(c["customer_id"], W["end"]) - timedelta(hours=1)
        cust_rows.append((created, c))
    for b in range(nb + 1):
        bdir = f"{out}/batches/batch_{b:04d}"
        b_end = W["start"] + timedelta(days=b)
        write_csv(f"{bdir}/customer/customer.csv", ["customer_id", "first_name", "last_name", "email_hash", "phone_hash", "postal_code", "favorite_sport", "created_ts"],
                  [[c["customer_id"], c["first_name"], c["last_name"], c["email_hash"], c["phone_hash"], c["postal_code"], c["favorite_sport"], iso(t)] for t, c in cust_rows if t < b_end])
        write_csv(f"{bdir}/event/event.csv", ["event_id", "event_title", "event_category", "sport", "league", "venue_id", "start_ts", "on_sale_ts", "status"],
                  [[e["event_id"], e["event_title"], e["event_category"], e["sport"], e["league"], e["venue_id"], iso(e["start_ts"]), iso(e["on_sale_ts"]),
                    "cancelled" if e.get("cancel_ts") and e["cancel_ts"] < b_end else "scheduled"] for e in W["events"]])
        write_csv(f"{bdir}/ticket_transaction/ticket_transaction_{b:04d}.csv", TXN_COLS, tx_rows.get(b, []))
        ords = [o for o in orders.values() if batch_of(W, o["order_ts"]) == b]
        write_csv(f"{bdir}/order_plan/order_plan_{b:04d}.csv", ["order_id", "customer_id", "event_id", "order_ts", "total_amount", "num_payments", "plan_interval_days", "sale_type"],
                  [[o["order_id"], o["customer_id"], o["event_id"], iso(o["order_ts"]), money(o["total"]), o.get("num_payments", 1), o.get("plan_interval_days", 0), o["sale_type"]] for o in ords])
        write_csv(f"{bdir}/payment/payment_{b:04d}.csv", ["payment_id", "order_id", "payment_seq", "attempt_no", "due_date", "event_ts", "amount", "status", "original_payment_id"],
                  [[p["payment_id"], p["order_id"], p["payment_seq"], p["attempt_no"], p["due_date"], iso(p["event_ts"]), money(p["amount"]), p["status"], p["original_payment_id"]] for p in pays if batch_of(W, p["event_ts"]) == b])
        prices = []
        for (eid, sec), s in W["series"].items():
            for t, px in s.rows():
                if batch_of(W, t) == b and t <= W["end"]:
                    prices.append([f"{eid}-{sec}", eid, sec, iso(t), money(px)])
        write_csv(f"{bdir}/price_change/price_change_{b:04d}.csv", ["price_change_id", "event_id", "section", "effective_ts", "price"], [[f"PC{b:02d}{n:06d}"] + r[1:] for n, r in enumerate(sorted(prices, key=lambda r: (r[3], r[0])))])
        os.makedirs(f"{bdir}/ga_event", exist_ok=True)
        with open(f"{bdir}/ga_event/ga_event_{b:04d}.jsonl", "w") as f:
            late_ga = 0
            for x in ga:
                bb = batch_of(W, x["_when"])
                if bb < nb and bb > 0 and (x["event_timestamp"] % 20 == 0):   # about 5 percent of events arrive one file late
                    bb += 1
                if bb == b:
                    f.write(json.dumps({k: v for k, v in x.items() if k != "_when"}) + "\n")

    # ---- truth and answer key
    os.makedirs(f"{out}/truth", exist_ok=True)
    os.makedirs(f"{out}/answer_key", exist_ok=True)
    write_csv(f"{out}/truth/identity_customers.csv", ["customer_id", "person_id"], [[c["customer_id"], c["person"]] for c in W["customer_by_id"].values()])
    write_csv(f"{out}/truth/identity_sessions.csv", ["person_id", "device_id", "user_pseudo_id", "ga_session_id", "session_ts", "event_id", "shared_device", "logged_in"],
              [[s["person"], s["device"], s["pseudo"], s["session"], s["ts"], s["event_id"], s["shared_device"], s["logged_in"]] for s in truth_sessions])
    with open(f"{out}/truth/journeys.json", "w") as f:
        json.dump(truth_journeys, f, indent=1)
    valid = [r for r in tx if r["transaction_id"] not in conflicts]
    totals = defaultdict(lambda: dict(count=0, gross_cents=0, fee_cents=0))
    profit, cost = 0, {}
    for r in sorted(valid, key=lambda r: (r["sale_ts"], r["transaction_id"])):   # cost basis = the MOST RECENT earlier buyback of that ticket
        t = totals[r["sale_type"]]
        t["count"] += 1; t["gross_cents"] += r["price"]; t["fee_cents"] += r["buyer_fee"] + r["seller_fee"]
        if r["sale_type"] == "buyback":
            cost[r["ticket_id"]] = r["price"]
        elif r["sale_type"] == "company_resale":
            profit += r["price"] - cost[r["ticket_id"]]
    closed = [closed_status(o, pays) for o in orders.values() if o["sale_type"] == "primary"]
    key = dict(
        seed=cfg.seed, generated_for_batches=nb,
        note="Expected results of a CORRECT pipeline. Conflicting duplicates remove their valid original on purpose (both rows are rejected).",
        signal=cfg.signal,
        planted_drivers=("none" if cfg.signal == "none" else
                         "buyers are 12x more likely to be people whose preferred sport matches the event (a declared favourite sport is on about 70 percent of customer records); plan default risk rises 2 points per payment above 4 and 5 points for concerts; "
                         "buyers sometimes see a planted price drop between their last visit and purchase, non-buyers do not"),
        planted_by_batch={str(b): {k: v for k, v in d.items()} for b, d in planted.items()},
        planted_counts_by_batch={str(b): {k: len(v) for k, v in d.items()} for b, d in planted.items()},
        conflict_transaction_ids_excluded=sorted(conflicts),
        late_delivered_transactions=late,
        ledger=dict(valid_transactions=len(valid), by_sale_type={k: dict(count=v["count"], gross=money(v["gross_cents"]), fees=money(v["fee_cents"])) for k, v in totals.items()},
                    fee_revenue=money(sum(v["fee_cents"] for v in totals.values())), owned_inventory_profit=money(profit),
                    total_earnings=money(sum(v["fee_cents"] for v in totals.values()) + profit),
                    forced_five_hop_tickets=len({r["ticket_id"] for r in tx if r["sale_type"] == "buyback"})),
        files_per_batch={str(b): len(tx_rows.get(b, [])) for b in range(nb + 1)},
        orders=dict(total=len(orders), plan_orders=sum(1 for o in orders.values() if o.get("num_payments", 1) > 1)),
        closed_sales=dict(closed_ever=sum(1 for c in closed if c["closed_ever"]), closed_now=sum(1 for c in closed if c["closed_now"]),
                          plan_orders_closed_ever=sum(1 for c in closed if c["closed_ever"] and c["num_payments"] > 1),
                          plan_orders_closed_now=sum(1 for c in closed if c["closed_now"] and c["num_payments"] > 1),
                          plan_orders_open=sum(1 for c in closed if not c["closed_now"] and c["num_payments"] > 1),
                          refund_scenarios=W["refund_scenarios"], detail=closed),
        cancelled_event=dict(event_id=W["cancel_event"]["event_id"], cancel_ts=iso(W["cancel_event"]["cancel_ts"])) if W["cancel_event"] else None,
        identity=dict(persons=len(W["persons"]), customer_records=len(W["customer_by_id"]), persons_with_two_customer_records=sum(1 for p in W["persons"].values() if len(p["customers"]) > 1),
                      shared_devices=W["shared_devices"], bot_pseudo_ids=W["bots"], sessions=len(truth_sessions)),
        analytics=dict(events=len(ga), journeys_with_purchase=sum(1 for j in truth_journeys if j.get("purchased", True) and j.get("order_id")),
                       journeys_without_purchase=sum(1 for j in truth_journeys if not j.get("order_id"))))
    with open(f"{out}/answer_key/answer_key.json", "w") as f:
        json.dump(key, f, indent=1)
    print(f"wrote {out}: {len(tx)} ledger rows, {len(orders)} orders, {len(pays)} payments, {len(ga)} analytics events, {len(W['persons'])} people")
    return 0                       # a Job task must see 0 for success (the answer key is in answer_key/answer_key.json)


if __name__ == "__main__":
    main()
