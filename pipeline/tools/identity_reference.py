#!/usr/bin/env python3
"""Reference identity resolution for the tix project, scored against the generator's ground truth.

    python tools/identity_reference.py data/tix

Question it answers: which PERSON was behind each anonymous web session, so that visits before a purchase can be counted
(be-backs) even when the visitor was not logged in?

Why a reference version in plain Python: the real pipeline will do this in SQL/PySpark. Before writing that, you need to know what
the right answer looks like and which signals can and cannot be trusted. Run this, read the scores, then port the logic.

The method, in order of trust:
  1. Merge duplicate customer RECORDS into one person: same email hash, or same phone hash and postal code.        (tier 0)
  2. Link a browser id (user_pseudo_id) to a person by strong evidence in a session:
       a. the session logged in (user_id)                                                                          (tier 1)
       b. the session bought, and the order belongs to a customer                                                  (tier 2)
       c. a guest checkout carried an email hash that matches a customer                                           (tier 3)
  3. Attribute EVERY session of that browser id to the person, including sessions before the link existed (retroactive stitching),
     but only if the browser id points at exactly one person.
  4. If one browser id points at two or more people it is a SHARED device. Only sessions with their own evidence are attributed; the
     rest are left 'ambiguous' on purpose. Guessing here is how identity graphs create false merges.
  5. A new browser id after a cookie reset has no evidence. Link it only through the client fingerprint, and only when that fingerprint
     belongs to exactly one person among already-resolved browser ids (tier 4, 'probable').
  6. Bots (very many page views, no login, no purchase) are excluded.
"""
import csv
import glob
import json
import sys
from collections import defaultdict, Counter
from datetime import datetime, timedelta


def rows(pattern):
    out = []
    for p in sorted(glob.glob(pattern)):
        with open(p, newline="") as f:
            out.extend(csv.DictReader(f))
    return out


class UF:
    def __init__(self):
        self.p = {}

    def find(self, x):
        self.p.setdefault(x, x)
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a, b):
        self.p[self.find(a)] = self.find(b)


def param(e, k):
    for p in e["event_params"]:
        if p["key"] == k:
            v = p["value"]
            return v.get("string_value", v.get("int_value"))
    return None


def resolve(root):
    last = sorted(glob.glob(f"{root}/batches/batch_*"))[-1]
    cust = rows(f"{last}/customer/customer.csv")
    orders = {r["order_id"]: r["customer_id"] for r in rows(f"{root}/batches/batch_*/order_plan/*.csv")}

    # 1) person clusters from customer records
    uf = UF()
    by_email, by_phone = defaultdict(list), defaultdict(list)
    for c in cust:
        uf.find(c["customer_id"])
        by_email[c["email_hash"]].append(c["customer_id"])
        by_phone[(c["phone_hash"], c["postal_code"])].append(c["customer_id"])
    for grp in list(by_email.values()) + list(by_phone.values()):
        for x in grp[1:]:
            uf.union(grp[0], x)
    person = {c["customer_id"]: uf.find(c["customer_id"]) for c in cust}
    email_to_person = {e: {person[c] for c in v} for e, v in by_email.items()}

    # 2) sessions from the analytics events
    sess = {}
    views_by_pseudo = Counter()
    for path in sorted(glob.glob(f"{root}/batches/batch_*/ga_event/*.jsonl")):
        for line in open(path):
            e = json.loads(line)
            k = (e["user_pseudo_id"], param(e, "ga_session_id"))
            s = sess.setdefault(k, dict(pseudo=k[0], session=k[1], ts=e["event_timestamp"], user_ids=set(), orders=set(), emails=set(),
                                        fp=None, events=set(), pageviews=0, purchased=False))
            s["ts"] = min(s["ts"], e["event_timestamp"])
            s["fp"] = s["fp"] or param(e, "client_fp")
            if e["user_id"]:
                s["user_ids"].add(e["user_id"])
            if e["event_name"] == "purchase":
                s["purchased"] = True
                if (e.get("ecommerce") or {}).get("transaction_id"):
                    s["orders"].add(e["ecommerce"]["transaction_id"])
                if param(e, "email_hash"):
                    s["emails"].add(param(e, "email_hash"))
            if e["event_name"] == "view_item":
                s["events"].add(param(e, "event_id"))
            if e["event_name"] == "page_view":
                s["pageviews"] += 1
                views_by_pseudo[k[0]] += 1

    # 3) evidence per session, then per browser id
    ev_by_session, ev_by_pseudo = {}, defaultdict(lambda: defaultdict(set))
    for k, s in sess.items():
        found = []
        for u in s["user_ids"]:
            if u in person:
                found.append((person[u], 1))
        for o in s["orders"]:
            if o in orders and orders[o] in person:
                found.append((person[orders[o]], 2))
        for em in s["emails"]:
            for pp in email_to_person.get(em, ()):
                found.append((pp, 3))
        ev_by_session[k] = found
        for pp, tier in found:
            ev_by_pseudo[s["pseudo"]][pp].add(tier)
    bots = {p for p, n in views_by_pseudo.items() if n > 100 and not any(sess[k]["purchased"] or sess[k]["user_ids"] for k in sess if k[0] == p)}

    # 4) attribute sessions
    out, fp_people = {}, defaultdict(set)
    for p, d in ev_by_pseudo.items():
        if p not in bots and len(d) == 1:
            fp_people[next(s["fp"] for k, s in sess.items() if k[0] == p)].add(next(iter(d)))
    shared = {p for p, d in ev_by_pseudo.items() if len(d) > 1}
    for k, s in sess.items():
        p = s["pseudo"]
        if p in bots:
            out[k] = (None, "bot", 0.0)
        elif p in shared:
            direct = ev_by_session[k]
            if direct and len({x[0] for x in direct}) == 1:
                out[k] = (direct[0][0], f"shared_device_direct_t{min(t for _, t in direct)}", 0.9)
            else:
                out[k] = (None, "shared_device_ambiguous", 0.0)
        elif p in ev_by_pseudo:
            pp = next(iter(ev_by_pseudo[p]))
            direct = [x for x in ev_by_session[k] if x[0] == pp]
            out[k] = (pp, f"direct_t{min(t for _, t in direct)}" if direct else "retroactive_same_browser_id", 0.95 if direct else 0.8)
        else:
            people = fp_people.get(s["fp"], set())
            if len(people) == 1:
                out[k] = (next(iter(people)), "fingerprint_after_cookie_reset", 0.6)
            else:
                out[k] = (None, "unresolved", 0.0)
    return dict(person=person, sessions=sess, attribution=out, orders=orders)


def score(root, R):
    truth_cust = {r["customer_id"]: r["person_id"] for r in rows(f"{root}/truth/identity_customers.csv")}
    truth = {}
    for r in rows(f"{root}/truth/identity_sessions.csv"):
        truth[(r["user_pseudo_id"], int(r["ga_session_id"]))] = r
    # customer merge quality (pairs of records)
    clusters = defaultdict(list)
    for c, pp in R["person"].items():
        clusters[pp].append(c)
    pred_pairs = {(a, b) for grp in clusters.values() for a in grp for b in grp if a < b}
    true_groups = defaultdict(list)
    for c, pp in truth_cust.items():
        true_groups[pp].append(c)
    true_pairs = {(a, b) for grp in true_groups.values() for a in grp for b in grp if a < b}
    tp = len(pred_pairs & true_pairs)
    print(f"customer record merge: precision {tp / max(1, len(pred_pairs)):.3f}, recall {tp / max(1, len(true_pairs)):.3f}  ({len(true_pairs)} true duplicate pairs)")
    # cluster -> true person
    cl_person = {}
    for pp, grp in clusters.items():
        cl_person[pp] = Counter(truth_cust[c] for c in grp).most_common(1)[0][0]
    by_method = defaultdict(Counter)
    for k, (pp, method, conf) in R["attribution"].items():
        t = truth.get(k)
        if not t:
            by_method[method]["not in truth (bot or noise)"] += 1
            continue
        if pp is None:
            by_method[method]["left unresolved"] += 1
        elif cl_person[pp] == t["person_id"]:
            by_method[method]["correct"] += 1
        else:
            by_method[method]["WRONG"] += 1
    total = Counter()
    print("\nsession attribution by method:")
    for m, c in sorted(by_method.items()):
        print(f"  {m:<38} " + ", ".join(f"{k} {v}" for k, v in sorted(c.items())))
        total.update(c)
    known = total["correct"] + total["WRONG"]
    print(f"\nattributed sessions: {known}  precision {total['correct'] / max(1, known):.3f}   "
          f"coverage of real sessions {known / max(1, known + total['left unresolved']):.3f}")
    return total


def be_backs(root, R):
    """Recompute visits before purchase from the RESOLVED sessions and compare with the truth file (30-day window, same event)."""
    truth = [j for j in json.load(open(f"{root}/truth/journeys.json")) if j.get("order_id")]
    by_person_event = defaultdict(list)
    for k, s in R["sessions"].items():
        pp = R["attribution"][k][0]
        if pp is None:
            continue
        for e in s["events"]:
            by_person_event[(pp, e)].append(s["ts"] / 1_000_000)
    truth_cust = {r["customer_id"]: r["person_id"] for r in rows(f"{root}/truth/identity_customers.csv")}
    order_cust = R["orders"]
    exact = close = n = 0
    for j in truth:
        pp = R["person"].get(order_cust.get(j["order_id"]))
        if pp is None:
            continue
        t_buy = datetime.strptime(j["purchase_ts"], "%Y-%m-%dT%H:%M:%SZ").timestamp()
        visits = [t for t in by_person_event[(pp, j["event_id"])] if t_buy - 30 * 86400 <= t <= t_buy]
        got = max(0, len(visits) - 1)
        n += 1
        exact += got == j["be_backs_within_window"]
        close += abs(got - j["be_backs_within_window"]) <= 1
    print(f"\nbe-backs (30-day window) recomputed for {n} purchases: exact {exact / n:.3f}, within one {close / n:.3f}")


def run(root):
    R = resolve(root)
    score(root, R)
    be_backs(root, R)


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "data/tix")
