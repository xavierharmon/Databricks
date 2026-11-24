import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ardoq"))
from sync_to_ardoq import build_batch  # noqa: E402


def payload():
    cfg = yaml.safe_load((ROOT / "ardoq" / "config.example.yml").read_text())
    meta = json.loads((ROOT / "ardoq" / "sample_metadata.json").read_text())
    return build_batch(meta, cfg, "mlb_prod"), cfg


def test_hierarchy_and_unique_batch_ids():
    p, _ = payload()
    comps = p["components"]["upsert"]
    ids = [c["batchId"] for c in comps]
    assert len(ids) == len(set(ids))
    by_id = {c["batchId"]: c for c in comps}
    assert by_id["schema:mlb_prod.gold"]["body"]["parent"] == "catalog:mlb_prod"
    assert by_id["table:mlb_prod.gold.fact_batting_season"]["body"]["parent"] == "schema:mlb_prod.gold"
    assert "metric_view:mlb_prod.semantic.mv_batting" in by_id
    for c in comps:
        assert c["uniqueBy"] == ["rootWorkspace", "name"]
        parent = c["body"].get("parent")
        assert parent is None or parent in by_id


def test_references_point_at_real_components_and_types():
    p, cfg = payload()
    ids = {c["batchId"] for c in p["components"]["upsert"]}
    refs = p["references"]["upsert"]
    assert refs
    for r in refs:
        assert r["body"]["source"] in ids and r["body"]["target"] in ids
    types = {r["body"]["type"] for r in refs}
    assert types == {cfg["reference_types"]["lineage"], cfg["reference_types"]["defines"]}
    # glossary term -> metric view and wildcard -> schema both resolved
    pairs = {(r["body"]["source"], r["body"]["target"]) for r in refs}
    assert ("term:Batting Average", "metric_view:mlb_prod.semantic.mv_batting") in pairs
    assert ("term:Bronze", "schema:mlb_prod.bronze") in pairs


def test_prune_only_when_enabled():
    cfg = yaml.safe_load((ROOT / "ardoq" / "config.example.yml").read_text())
    meta = json.loads((ROOT / "ardoq" / "sample_metadata.json").read_text())
    assert "delete" not in build_batch(meta, cfg, "mlb_prod")["components"]
    cfg["prune"] = True
    assert len(build_batch(meta, cfg, "mlb_prod")["components"]["delete"]) == 5
