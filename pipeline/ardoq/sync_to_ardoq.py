#!/usr/bin/env python3
"""Sync Unity Catalog metadata into Ardoq with the Ardoq Batch API (idempotent upserts).

What lands in Ardoq (one workspace):
  Catalog -> Schema -> Table / Metric View      (component hierarchy, via `parent`)
  Table -> Table                                 (lineage references, from system.access.table_lineage)
  Business Term -> Table / Metric View / Schema  (from <catalog>.governance.business_glossary)

Usage
  python ardoq/sync_to_ardoq.py --catalog mlb_prod --warehouse-id <id> --dry-run     # build + print payload only
  python ardoq/sync_to_ardoq.py --catalog mlb_prod --warehouse-id <id>               # real sync
  python ardoq/sync_to_ardoq.py --sample --dry-run                                   # no Databricks needed

Auth (env vars, never in git):  ARDOQ_API_TOKEN   plus the standard DATABRICKS_HOST / DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET.
The Ardoq API uses a Bearer token. The Batch API is transactional: if any operation fails, nothing is changed.
"""
import argparse
import json
import os
import sys
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent


# --------------------------------------------------------------------------- Unity Catalog side
def fetch_uc_metadata(catalog: str, schemas: list, warehouse_id: str) -> dict:
    from databricks.sdk import WorkspaceClient  # imported lazily so tests need no SDK

    w = WorkspaceClient()
    assets = []
    for schema in schemas:
        for t in w.tables.list(catalog_name=catalog, schema_name=schema):
            ttype = getattr(t.table_type, "value", str(t.table_type))
            assets.append({
                "full_name": t.full_name,
                "schema": schema,
                "name": t.name,
                "table_type": ttype,
                "comment": t.comment or "",
                "owner": t.owner or "",
            })

    def sql(statement: str) -> list:
        r = w.statement_execution.execute_statement(warehouse_id=warehouse_id, statement=statement, wait_timeout="50s")
        return (r.result.data_array if r.result and r.result.data_array else [])

    lineage = [
        {"source": s, "target": t}
        for s, t in sql(
            "SELECT DISTINCT source_table_full_name, target_table_full_name FROM system.access.table_lineage "
            f"WHERE target_table_catalog = '{catalog}' AND source_table_full_name IS NOT NULL "
            "AND target_table_full_name IS NOT NULL"
        )
    ]
    terms = [
        {"term": t, "definition": d or "", "related_asset": a or "", "owner": o or "", "formula": f or ""}
        for t, d, a, o, f in sql(
            f"SELECT term, definition, related_asset, owner, formula FROM {catalog}.governance.business_glossary"
        )
    ]
    return {"assets": assets, "lineage": lineage, "terms": terms}


# --------------------------------------------------------------------------- Pure payload builder (unit tested)
def is_metric_view(asset: dict) -> bool:
    return "METRIC" in asset["table_type"].upper() or asset["name"].startswith("mv_")


def build_batch(meta: dict, cfg: dict, catalog: str) -> dict:
    ws = cfg["workspace_id"]
    ct = cfg["component_types"]
    rt = cfg["reference_types"]
    comps, refs, ids = [], [], {}

    def comp(batch_id, type_key, name, description="", parent=None, fields=None):
        if batch_id in ids:
            return
        body = {"rootWorkspace": ws, "typeId": ct[type_key], "name": name, "description": description}
        if parent:
            body["parent"] = parent
        if fields and cfg.get("custom_fields"):
            body["customFields"] = fields
        ids[batch_id] = name
        comps.append({"batchId": batch_id, "uniqueBy": ["rootWorkspace", "name"], "body": body})

    comp(f"catalog:{catalog}", "catalog", catalog, "Unity Catalog catalog (environment boundary).")
    for schema in cfg["schemas"]:
        comp(f"schema:{catalog}.{schema}", "schema", f"{catalog}.{schema}", f"{schema} layer", f"catalog:{catalog}",
             {"layer": schema})

    by_full_name = {}
    for a in meta["assets"]:
        kind = "metric_view" if is_metric_view(a) else "table"
        bid = f"{kind}:{a['full_name']}"
        by_full_name[a["full_name"]] = bid
        comp(bid, kind, a["full_name"], a["comment"], f"schema:{catalog}.{a['schema']}",
             {"layer": a["schema"], "owner": a["owner"], "table_type": a["table_type"]})

    for term in meta["terms"]:
        desc = term["definition"] + (f"\n\nFormula: {term['formula']}" if term.get("formula") else "")
        comp(f"term:{term['term']}", "business_term", term["term"], desc, None, {"owner": term.get("owner", "")})

    for e in meta["lineage"]:
        s, t = by_full_name.get(e["source"]), by_full_name.get(e["target"])
        if s and t and s != t:
            refs.append({"body": {"source": s, "target": t, "type": rt["lineage"]}})

    for term in meta["terms"]:
        parts = term["related_asset"].split(".")
        target = None
        if len(parts) >= 3 and parts[2] != "*":          # semantic.mv_batting.home_runs -> the metric view
            target = by_full_name.get(f"{catalog}.{parts[0]}.{parts[1]}")
        elif len(parts) == 2 and parts[1] != "*":        # gold.dim_season -> the table
            target = by_full_name.get(f"{catalog}.{parts[0]}.{parts[1]}")
        elif len(parts) == 3 and parts[2] == "*":        # catalog.bronze.* -> the schema
            target = f"schema:{catalog}.{parts[1]}" if parts[1] in cfg["schemas"] else None
        if target:
            refs.append({"body": {"source": f"term:{term['term']}", "target": target, "type": rt["defines"]}})

    payload = {"components": {"upsert": comps}, "references": {"upsert": refs}}
    if cfg.get("prune"):
        payload["components"]["delete"] = [
            {"match": {"rootWorkspace": ws, "typeId": ct[k]}} for k in ("catalog", "schema", "table", "metric_view", "business_term")
        ]
    return payload


# --------------------------------------------------------------------------- Ardoq side
def post_batch(base_url: str, token: str, payload: dict) -> dict:
    import requests

    r = requests.post(f"{base_url.rstrip('/')}/api/v2/batch", json=payload,
                      headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, timeout=120)
    if r.status_code >= 300:
        sys.exit(f"Ardoq batch failed ({r.status_code}): {r.text[:2000]}")
    return r.json()


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=str(HERE / "config.yml"))
    ap.add_argument("--catalog", default="mlb_prod")
    ap.add_argument("--warehouse-id", default=os.environ.get("DATABRICKS_WAREHOUSE_ID", ""))
    ap.add_argument("--dry-run", action="store_true", help="build the payload and write ardoq/last_payload.json; call nothing")
    ap.add_argument("--sample", action="store_true", help="use ardoq/sample_metadata.json instead of calling Databricks")
    args = ap.parse_args(argv)

    cfg_path = Path(args.config)
    if not cfg_path.exists():
        cfg_path = HERE / "config.example.yml"
        print(f"[note] {args.config} not found, using {cfg_path.name} (fine for --dry-run)")
    cfg = yaml.safe_load(cfg_path.read_text())

    if args.sample:
        meta = json.loads((HERE / "sample_metadata.json").read_text())
    else:
        if not args.warehouse_id:
            sys.exit("--warehouse-id (or DATABRICKS_WAREHOUSE_ID) is required to read lineage and the glossary")
        meta = fetch_uc_metadata(args.catalog, cfg["schemas"], args.warehouse_id)

    payload = build_batch(meta, cfg, args.catalog)
    n_c, n_r = len(payload["components"]["upsert"]), len(payload["references"]["upsert"])
    print(f"payload: {n_c} component upserts, {n_r} reference upserts")
    (HERE / "last_payload.json").write_text(json.dumps(payload, indent=2))

    if args.dry_run:
        print("dry run: wrote ardoq/last_payload.json, nothing sent")
        return payload
    token = os.environ.get("ARDOQ_API_TOKEN")
    if not token:
        sys.exit("ARDOQ_API_TOKEN is not set")
    result = post_batch(cfg["base_url"], token, payload)
    print("Ardoq responded OK:", json.dumps(result)[:500])
    return payload


if __name__ == "__main__":
    main()
