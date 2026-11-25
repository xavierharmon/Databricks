#!/usr/bin/env python3
"""Write the first slice of tix contracts (reference data: venues, participants, events, tags, customers, tickets, fees).

    python tools/build_tix_contracts.py            # writes contracts/tix/... and transforms/gold/dim_event.sql, dim_customer.sql

Why a builder and not 25 hand-typed YAML files: the contracts for these tables follow one pattern (source = all text; silver = typed
with rules; gold = audited dimension). The builder keeps the pattern in ONE place so a change to the pattern is one edit. The YAML it
writes is the real artifact: commit it, review it, and edit it by hand afterwards if you prefer. This slice runs on the engine as it is
today (replace-each-run files); the ledger, payments and analytics need the engine changes listed in the plan.
"""
import os
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))
P = "tix"
PHYS = {"string": "STRING", "integer": "INT", "number": "DOUBLE", "timestamp": "TIMESTAMP", "date": "DATE", "money": "DECIMAL(12,2)", "ratio": "DECIMAL(10,4)"}
LOGICAL = {"money": "number", "ratio": "number"}


def col(name, t="string", req=True, desc="", **extra):
    return dict(name=name, t=t, req=req, desc=desc or name.replace("_", " "), **extra)


def rule(rid, rtype, cols, expr, action, desc, dim="conformity", sev="error", owner="Data Engineering"):
    return dict(id=rid, rtype=rtype, cols=cols, expr=expr, action=action, desc=desc, dim=dim, sev=sev, owner=owner)


def dedupe(prefix, what):
    return [rule(f"{prefix}-001", "exact_duplicate", "*", "", "dedupe", f"Identical duplicate {what} rows are removed automatically (first copy kept)", "uniqueness", "warning")]


ENTITIES = {
    "venue": dict(domain="events", folder="venue", purpose="Venues where events are held.", prefix="VEN", pk=["venue_id"], cols=[
        col("venue_id", desc="Venue id. Primary key.", primaryKey=1), col("venue_name"), col("city"), col("timezone")],
        rules=[rule("VEN-002", "unique_strict", ["venue_id"], "", "quarantine", "venue_id must identify exactly one venue", "uniqueness"),
               rule("VEN-003", "not_null", ["venue_id", "venue_name", "city", "timezone"], "", "quarantine", "Venue fields must be present", "completeness")]),
    "participant": dict(domain="events", folder="participant", purpose="Teams and performers that take part in events.", prefix="PAR", pk=["participant_id"], also=["fbl.silver.team"], cols=[
        col("participant_id", desc="Participant id. Primary key. Baseball teams use the FBL team ids.", primaryKey=1),
        col("participant_name"), col("participant_type", desc="team or performer."),
        col("sport", req=False, desc="Sport for a team; blank for a performer."),
        col("fbl_team_id", req=False, desc="The FBL team id when this is a baseball team, else blank. Checked against the shared FBL team table.",
            sourceColumn="participant_id", sourceExpression="CASE WHEN trim(sport) = 'baseball' THEN trim(participant_id) END",
            relationships=[dict(to="fbl_team.team_id", type="foreignKey")])],
        rules=[rule("PAR-002", "unique_strict", ["participant_id"], "", "quarantine", "participant_id must identify exactly one participant", "uniqueness"),
               rule("PAR-003", "not_null", ["participant_id", "participant_name", "participant_type"], "", "quarantine", "Required participant fields must be present", "completeness"),
               rule("PAR-004", "allowed_values", ["participant_type"], "'team','performer'", "quarantine", "participant_type must be team or performer"),
               rule("PAR-005", "referential", ["fbl_team_id"], "silver.fbl_team.team_id", "quarantine", "A baseball team must exist in the shared FBL team table", "consistency"),
               rule("PAR-006", "custom", [], "participant_type <> 'performer' OR sport IS NULL", "quarantine", "A performer has no sport")]),
    "event": dict(domain="events", folder="event", purpose="Every ticketed occasion: sports and special events share this one table.", prefix="EVT", pk=["event_id"], cols=[
        col("event_id", desc="Event id. Primary key.", primaryKey=1), col("event_title"),
        col("event_category", desc="sport or concert."), col("sport", req=False, desc="baseball, football or basketball; blank for a concert."),
        col("league", req=False, desc="League for a sport; blank for a concert."),
        col("venue_id", desc="Venue of the event.", relationships=[dict(to="tix_venue.venue_id", type="foreignKey")]),
        col("start_ts", "timestamp", desc="Event start, UTC."), col("on_sale_ts", "timestamp", desc="When tickets go on sale, UTC."),
        col("status", desc="scheduled or cancelled.")],
        rules=[rule("EVT-002", "unique_strict", ["event_id"], "", "quarantine", "event_id must identify exactly one event", "uniqueness"),
               rule("EVT-003", "not_null", ["event_id", "event_title", "event_category", "venue_id", "start_ts", "on_sale_ts", "status"], "", "quarantine", "Required event fields must be present", "completeness"),
               rule("EVT-004", "allowed_values", ["event_category"], "'sport','concert'", "quarantine", "event_category must be sport or concert"),
               rule("EVT-005", "allowed_values", ["status"], "'scheduled','cancelled'", "quarantine", "status must be scheduled or cancelled"),
               rule("EVT-006", "allowed_values", ["sport"], "'baseball','football','basketball'", "quarantine", "A sport must be baseball, football or basketball"),
               rule("EVT-007", "custom", [], "event_category <> 'concert' OR sport IS NULL", "quarantine", "A concert has no sport"),
               rule("EVT-008", "custom", [], "on_sale_ts < start_ts", "quarantine", "Tickets go on sale before the event starts", "consistency"),
               rule("EVT-009", "referential", ["venue_id"], "silver.tix_venue.venue_id", "quarantine", "Every event must name a known venue", "consistency")]),
    "event_participant": dict(domain="events", folder="event_participant", purpose="Who takes part in each event: a home and an away team, or a performer.", prefix="EPA", pk=["event_id", "participant_id"], cols=[
        col("event_id", desc="Event.", primaryKey=1, relationships=[dict(to="tix_event.event_id", type="foreignKey")]),
        col("participant_id", desc="Participant.", primaryKey=2, relationships=[dict(to="tix_participant.participant_id", type="foreignKey")]),
        col("role", desc="home, away or performer.")],
        rules=[rule("EPA-002", "unique_strict", ["event_id", "participant_id"], "", "quarantine", "A participant appears once per event", "uniqueness"),
               rule("EPA-003", "not_null", ["event_id", "participant_id", "role"], "", "quarantine", "Required fields must be present", "completeness"),
               rule("EPA-004", "allowed_values", ["role"], "'home','away','performer'", "quarantine", "role must be home, away or performer"),
               rule("EPA-005", "referential", ["event_id"], "silver.tix_event.event_id", "quarantine", "The event must exist", "consistency"),
               rule("EPA-006", "referential", ["participant_id"], "silver.tix_participant.participant_id", "quarantine", "The participant must exist", "consistency")]),
    "event_tag": dict(domain="events", folder="event_tag", purpose="Tags that describe events, many to many.", prefix="ETG", pk=["event_id", "tag"], cols=[
        col("event_id", desc="Event.", primaryKey=1, relationships=[dict(to="tix_event.event_id", type="foreignKey")]),
        col("tag", desc="Tag such as rivalry or family_friendly.", primaryKey=2, standardize="lower")],
        rules=[rule("ETG-002", "unique_strict", ["event_id", "tag"], "", "quarantine", "A tag appears once per event", "uniqueness"),
               rule("ETG-003", "not_null", ["event_id", "tag"], "", "quarantine", "Required fields must be present", "completeness"),
               rule("ETG-004", "referential", ["event_id"], "silver.tix_event.event_id", "quarantine", "The event must exist", "consistency")]),
    "customer": dict(domain="customer", folder="customer", purpose="Customer records. A person can have more than one record; identity resolution merges them later.", prefix="CUS", pk=["customer_id"], cols=[
        col("customer_id", desc="Customer record id. Primary key.", primaryKey=1), col("first_name"), col("last_name"),
        col("email_hash", desc="Hash of the email address, never the address."), col("phone_hash", desc="Hash of the phone number."),
        col("postal_code"), col("favorite_sport", req=False, desc="Sport the customer declared as a favourite (baseball, football, basketball or concert); blank if none."),
        col("created_ts", "timestamp", desc="When the record was created, UTC.")],
        rules=[rule("CUS-002", "unique_strict", ["customer_id"], "", "quarantine", "customer_id must identify exactly one record", "uniqueness"),
               rule("CUS-003", "not_null", ["customer_id", "first_name", "last_name", "email_hash", "phone_hash", "postal_code", "created_ts"], "", "quarantine", "Required customer fields must be present", "completeness"),
               rule("CUS-004", "custom", [], "length(postal_code) = 5", "quarantine", "Postal code has five characters"),
               rule("CUS-005", "allowed_values", ["favorite_sport"], "'baseball','football','basketball','concert'", "quarantine", "A declared favourite is one of the four kinds of event")]),
    "ticket_inventory": dict(domain="events", folder="ticket_inventory", purpose="Every ticket that exists for an event: a reserved seat or a general-admission place.", prefix="TIK", pk=["ticket_id"], cols=[
        col("ticket_id", desc="Ticket id. Primary key.", primaryKey=1),
        col("event_id", desc="Event.", relationships=[dict(to="tix_event.event_id", type="foreignKey")]),
        col("section"), col("row", req=False, desc="Row; blank for general admission."), col("seat", req=False, desc="Seat; blank for general admission."),
        col("admission", desc="reserved or general.")],
        rules=[rule("TIK-002", "unique_strict", ["ticket_id"], "", "quarantine", "ticket_id must identify exactly one ticket", "uniqueness"),
               rule("TIK-003", "not_null", ["ticket_id", "event_id", "section", "admission"], "", "quarantine", "Required ticket fields must be present", "completeness"),
               rule("TIK-004", "allowed_values", ["admission"], "'reserved','general'", "quarantine", "admission must be reserved or general"),
               rule("TIK-005", "custom", [], "admission <> 'general' OR seat IS NULL", "quarantine", "A general-admission ticket has no seat"),
               rule("TIK-006", "referential", ["event_id"], "silver.tix_event.event_id", "quarantine", "The event must exist", "consistency")]),
    "fee_schedule": dict(domain="sales", folder="fee_schedule", purpose="Fee rates in basis points by sale type and event category, with effective dates.", prefix="FEE", pk=["rule_id"], cols=[
        col("rule_id", desc="Fee rule id. Primary key.", primaryKey=1), col("sale_type"), col("event_category"),
        col("buyer_fee_bps", "integer", desc="Buyer fee in basis points (1200 = 12 percent)."), col("seller_fee_bps", "integer", desc="Seller fee in basis points."),
        col("effective_from", "timestamp", desc="Rule applies from this time, UTC."), col("effective_to", "timestamp", req=False, desc="Rule applies until this time; blank if open ended.")],
        rules=[rule("FEE-002", "unique_strict", ["rule_id"], "", "quarantine", "rule_id must identify exactly one rule", "uniqueness"),
               rule("FEE-003", "not_null", ["rule_id", "sale_type", "event_category", "buyer_fee_bps", "seller_fee_bps", "effective_from"], "", "quarantine", "Required fee fields must be present", "completeness"),
               rule("FEE-004", "allowed_values", ["sale_type"], "'primary','resale','buyback','company_resale'", "quarantine", "sale_type must be one of the four sale types"),
               rule("FEE-005", "allowed_values", ["event_category"], "'sport','concert'", "quarantine", "event_category must be sport or concert"),
               rule("FEE-006", "range", ["buyer_fee_bps"], "0 AND 5000", "quarantine", "Buyer fee is between 0 and 50 percent"),
               rule("FEE-007", "range", ["seller_fee_bps"], "0 AND 5000", "quarantine", "Seller fee is between 0 and 50 percent")]),
}

GOLD = {
    "dim_event": dict(domain="events", silver="event", also=["tix.silver.venue"], purpose="Event dimension with its venue: one row per event.", prefix="DEV", pk=["event_id"], cols=[
        ("event_id", "string"), ("event_title", "string"), ("event_category", "string"), ("sport", "string"), ("league", "string"), ("venue_id", "string"),
        ("venue_name", "string"), ("city", "string"), ("timezone", "string"), ("start_ts", "timestamp"), ("on_sale_ts", "timestamp"), ("status", "string")],
        sql="""-- GOLD transform for dim_event: one row per event, with its venue.
-- Reads CLEAN silver only. A concert has no sport or league, so they get the explicit label 'none' (gold never contains NULL).
SELECT e.event_id, e.event_title, e.event_category, coalesce(e.sport, 'none') AS sport, coalesce(e.league, 'none') AS league,
       e.venue_id, v.venue_name, v.city, v.timezone, e.start_ts, e.on_sale_ts, e.status
FROM silver.tix_event e
JOIN silver.tix_venue v ON e.venue_id = v.venue_id
"""),
    "dim_customer": dict(domain="customer", silver="customer", also=[], purpose="Customer record dimension: one row per customer record.", prefix="DCU", pk=["customer_id"], cols=[
        ("customer_id", "string"), ("first_name", "string"), ("last_name", "string"), ("email_hash", "string"), ("phone_hash", "string"),
        ("postal_code", "string"), ("created_ts", "timestamp")],
        sql="""-- GOLD transform for dim_customer: one row per customer record. Hashes only; never raw email or phone.
SELECT customer_id, first_name, last_name, email_hash, phone_hash, postal_code, created_ts
FROM silver.tix_customer
"""),
}


def q(r):
    imp = dict(rule_type=r["rtype"], columns=(r["cols"] if r["cols"] != "*" else "*"), expression=r["expr"], action=r["action"], owner=r["owner"])
    return dict(name=r["id"], type="custom", engine="fbl-dq-engine", dimension=r["dim"], severity=r["sev"], description=r["desc"], implementation=imp)


def header(cid, name, layer, domain, purpose, limits, usage, extra_cp):
    return dict(apiVersion="v3.1.0", kind="DataContract", id=cid, name=name, version="1.0.0", status="active", domain=domain, dataProduct=P,
                description=dict(purpose=purpose, limitations=limits, usage=usage), tags=[P, layer],
                customProperties=[dict(property="layer", value=layer), dict(property="owner", value="Data Engineering")] + extra_cp)


def source_contract(name, e):
    if e.get("source_cols"):
        props = [dict(name=n, logicalType="string", physicalType="STRING", required=True, description=d) for n, d in e["source_cols"]]
    else:
        props = [dict(name=c["name"], logicalType="string", physicalType="STRING", required=True, description=f"{c['desc']} (raw text, present in the file header)")
                 for c in e["cols"] if "sourceExpression" not in c]
    d = header(f"{P}.source.{name}", f"tix {name} file (as delivered)", "source", e["domain"], f"The {name} file: the listed columns are present and the file is not empty.",
               "Values arrive as text and may be blank, duplicated or invalid. Row-level validity is NOT promised here; silver enforces it.", "Load as strings into bronze.",
               [dict(property="sourceSystem", value="tix_generator_csv")])
    d["schema"] = [dict(name=f"{P}_{name}", physicalName=f"bronze.{P}_{name}", logicalType="object", physicalType="table", description=f"Raw {name} file, all columns as delivered.",
                        customProperties=[dict(property="entity", value=name), dict(property="landingFolder", value=e["folder"])]
                        + ([dict(property="format", value=e["format"])] if e.get("format") else []),
                        quality=[dict(metric="rowCount", mustBeGreaterThan=0, type="library", dimension="completeness", severity="error", description="The delivered files must contain at least one row.")],
                        properties=props)]
    return d


def silver_contract(name, e):
    cps = [dict(property="dependsOn", value=f"{P}.source.{name}")]
    if e.get("also"):
        cps.append(dict(property="alsoDependsOn", value=e["also"]))
    d = header(f"{P}.silver.{name}", f"tix {name} (silver)", "silver", e["domain"], f"{e['purpose']} Typed, cleaned, deduplicated. Every row has passed every quarantine rule.",
               "Rejected rows are not here; they are in governance.dq_quarantine with the reason.", "Internal layer. Consumers read gold or the semantic layer.", cps)
    props = []
    for c in e["cols"]:
        p = dict(name=c["name"], logicalType=LOGICAL.get(c["t"], c["t"]), physicalType=PHYS[c["t"]], required=c["req"], description=c["desc"])
        if "primaryKey" in c:
            p.update(primaryKey=True, primaryKeyPosition=c["primaryKey"])
        if c.get("relationships"):
            p["relationships"] = c["relationships"]
        cp = []
        for k in ("standardize", "sourceColumn", "sourceExpression"):
            if k in c:
                cp.append(dict(property=k, value=c[k]))
        if cp:
            p["customProperties"] = cp
        props.append(p)
    rules = dedupe(e["prefix"], name) + e["rules"]
    d["schema"] = [dict(name=f"{P}_{name}", physicalName=f"silver.{P}_{name}", logicalType="object", physicalType="table", description=e["purpose"],
                        customProperties=[dict(property="entity", value=name)], properties=props, quality=[q(r) for r in rules])]
    return d


def g_recon(g):
    mode = g.get("recon", "rowcount")
    silver = f"silver.{P}_{g['silver']}"
    if mode == "rowcount":
        return [dict(property="reconcilesTo", value=silver)]
    if mode == "none":
        return [dict(property="reconcileMode", value="none")]
    col, scol = mode.split(":")[1:]                      # "sum:<gold column>:<silver column>"
    return [dict(property="reconcileMode", value="sum"), dict(property="reconcilesTo", value=silver),
            dict(property="reconcileColumn", value=col), dict(property="reconcileSilverColumn", value=scol)]


def gold_contract(name, g):
    cps = [dict(property="dependsOn", value=g["deps"] if "deps" in g else [f"{P}.silver.{g['silver']}"] + g["also"])]
    d = header(f"{P}.gold.{name}", f"tix {name} (gold)", "gold", g["domain"], g["purpose"], "Gold never contains NULLs. Optional text is labelled 'none'.",
               "Safe for analysts and the semantic layer.", cps)
    props = []
    for i, (cn, t) in enumerate(g["cols"]):
        p = dict(name=cn, logicalType=LOGICAL.get(t, t), physicalType=PHYS[t], required=True, description=g.get("desc", {}).get(cn, cn.replace("_", " ")))
        if cn in g["pk"]:
            p.update(primaryKey=True, primaryKeyPosition=g["pk"].index(cn) + 1)
        props.append(p)
    rules = [rule(f"{g['prefix']}-001", "no_nulls", ["*"], "", "fail", "Gold has no NULL in any column", "completeness"),
             rule(f"{g['prefix']}-002", "unique", g["pk"], "", "fail", f"{', '.join(g['pk'])} identifies exactly one row", "uniqueness")]
    for i, (aname, adesc) in enumerate(g.get("audits", []), start=3):          # whole-table checks written as SQL files in transforms/audits/
        rules.append(rule(f"{g['prefix']}-{i:03d}", "sql_audit", [], aname + ".sql", "fail", adesc, "consistency"))
    d["schema"] = [dict(name=name, physicalName=f"gold.{P}_{name}", logicalType="object", physicalType="table", description=g["purpose"],
                        customProperties=[dict(property="entity", value=name)] + g_recon(g),
                        properties=props, quality=[q(r) for r in rules])]
    return d


def dump(path, doc, comment):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"# {comment}\n" + yaml.safe_dump(doc, sort_keys=False, allow_unicode=True, width=120, default_flow_style=False))


from tix_spec_ledger import ENTITIES as _E2, GOLD as _G2  # noqa: E402  (the ledger, payments, prices, analytics and ML features)
ENTITIES.update(_E2)
GOLD.update(_G2)


def main():
    out = ROOT / "contracts" / P
    n = 0
    for name, e in ENTITIES.items():
        dump(out / "source" / f"{P}_source_{name}.yaml", source_contract(name, e), f"Source contract: {name}. Generated by tools/build_tix_contracts.py. Bronze front door; rows are not validated here.")
        dump(out / "silver" / f"{P}_silver_{name}.yaml", silver_contract(name, e), f"Silver contract: {name}. Generated by tools/build_tix_contracts.py. Rules are executed by the DQ engine.")
        n += 2
    for name, g in GOLD.items():
        dump(out / "gold" / f"{P}_gold_{name}.yaml", gold_contract(name, g), f"Gold contract: {name}. Generated by tools/build_tix_contracts.py. Every gold rule is 'fail'.")
        (ROOT / "transforms" / "gold" / f"{name}.sql").write_text(g["sql"])
        for aname, _ in g.get("audits", []):
            (ROOT / "transforms" / "audits").mkdir(parents=True, exist_ok=True)
            (ROOT / "transforms" / "audits" / f"{aname}.sql").write_text(g["audit_sql"][aname])
        n += 1
    print(f"wrote {n} contracts under {out} and {len(GOLD)} gold transforms")


if __name__ == "__main__":
    main()
