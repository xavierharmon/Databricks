"""Tests for tools/catalog_tool.py: lint rules, render, tag SQL, and the publish plan against a fake Databricks."""
import copy
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import catalog_tool as ct  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]


def load():
    return ct.load_catalog(ROOT / "catalog"), ct.load_contracts([str(ROOT / "contracts")])


def msgs(found, sev="error"):
    return [m for s, _, m in found if s == sev]


def test_shipped_catalog_has_no_errors():
    cat, contracts = load()
    assert msgs(ct.lint(cat, contracts)) == []


def test_fbl_product_resolves_to_real_contracts():
    cat, contracts = load()
    assert not [m for s, w, m in ct.lint(cat, contracts) if "fbl-league-stats" in w]


def test_subdomain_tag_must_extend_parent():
    cat, contracts = load()
    bad = copy.deepcopy(cat)
    next(d for d in bad["domains"] if d["id"] == "sales/ledger")["tag_key"] = "ledger"
    assert any("subdomain's tag" in m for m in msgs(ct.lint(bad, contracts)))


def test_top_level_tag_cannot_contain_slash_and_icon_must_exist():
    cat, contracts = load()
    bad = copy.deepcopy(cat)
    d = next(d for d in bad["domains"] if d["id"] == "events")
    d["tag_key"], d["icon"] = "ev/ents", "NOT_AN_ICON"
    found = msgs(ct.lint(bad, contracts))
    assert any("slash" in m for m in found) and any("icon" in m for m in found)


def test_duplicate_page_title_in_a_domain_is_an_error():
    cat, contracts = load()
    bad = copy.deepcopy(cat)
    dup = copy.deepcopy(bad["pages"][0])
    dup["page"] = "another"
    bad["pages"].append(dup)
    assert any("share a title" in m for m in msgs(ct.lint(bad, contracts)))


def test_page_with_an_email_is_rejected():
    cat, contracts = load()
    bad = copy.deepcopy(cat)
    bad["pages"][0]["body"] += "\nask jane.doe@example.com"
    assert any("email" in m for m in msgs(ct.lint(bad, contracts)))


def test_unknown_domain_on_a_page_is_an_error():
    cat, contracts = load()
    bad = copy.deepcopy(cat)
    bad["pages"][0]["domain"] = "nowhere"
    assert any("not defined" in m for m in msgs(ct.lint(bad, contracts)))


def test_active_product_pointing_at_missing_contract_is_an_error_but_draft_is_a_warning():
    cat, contracts = load()
    bad = copy.deepcopy(cat)
    prod = next(p for p in bad["products"] if p["id"] == "tix-ticket-sales")
    assert not any("does not exist" in m for m in msgs(ct.lint(bad, contracts)))
    prod["status"] = "active"
    for a in prod["artifacts"]:
        for v in a["versions"]:
            v["status"] = "active"
    assert any("does not exist" in m for m in msgs(ct.lint(bad, contracts)))


def test_render_writes_a_page_per_term_and_a_product_catalog(tmp_path):
    cat, _ = load()
    n, m = ct.render(cat, tmp_path)
    assert (n, m) == (len(cat["pages"]), len(cat["products"]))
    assert (tmp_path / "closed-sale.md").exists() and "FBL League Stats" in (tmp_path / "products.md").read_text()


def test_tags_sql_tags_and_comments_real_tables_only():
    cat, contracts = load()
    sql = ct.tags_sql(cat, contracts, "cat1")
    assert "ALTER TABLE cat1.gold.fbl_fact_batting SET TAGS ('baseball_operations'" in sql
    assert "ALTER TABLE cat1.gold.tix_fact_ticket_transfer SET TAGS ('sales'" in sql      # the tix ledger now has a contract, so it is tagged
    assert "skipped tix.semantic.mv_sales" in sql                                       # metric views for tix are not built yet


class Fake:
    def __init__(self):
        self.calls = []
        self.domains = []

    def request(self, method, path, body=None, query=None):
        self.calls.append((method, path, body, query))
        if method == "GET" and path == "/api/2.0/domains":
            return {"domains": self.domains}
        if method == "POST" and path == "/api/2.0/domains":
            d = {"name": f"domains/{query['domain_id']}", "tag_key": body["tag_key"]}
            self.domains.append(d)
            return d
        if method == "GET" and path == "/api/2.0/knowledge/pages":
            return {"pages": []}
        return {}


def test_publish_dry_run_makes_no_calls():
    cat, _ = load()
    fake = Fake()
    plan = ct.publish(cat, fake, {}, apply=False, log=lambda *_: None)
    assert fake.calls == [] and any(p.startswith("create domain sales/ledger") for p in plan)


def test_publish_apply_creates_parents_before_children_and_drafts_pages():
    cat, _ = load()
    fake = Fake()
    ct.publish(cat, fake, {}, apply=True, log=lambda *_: None)
    posts = [c for c in fake.calls if c[0] == "POST" and c[1] == "/api/2.0/domains"]
    order = [c[3]["domain_id"] for c in posts]
    assert order.index("sales") < order.index("sales-ledger")
    child = next(c for c in posts if c[3]["domain_id"] == "sales-ledger")
    assert child[2]["parent_domain_id"] == "sales"
    pages = [c for c in fake.calls if c[0] == "POST" and c[1] == "/api/2.0/knowledge/pages"]
    assert len(pages) == len(cat["pages"]) and all(c[2]["state"] == "DRAFT" for c in pages)


def test_request_id_is_stable_and_looks_like_a_uuid4():
    a, b = ct.request_id("sales/closed-sale"), ct.request_id("sales/closed-sale")
    assert a == b and a[14] == "4"


def test_apply_sql_runs_each_statement_and_skips_failures():
    class Spark:
        def __init__(self):
            self.ran = []

        def sql(self, s):
            if "missing_table" in s:
                raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")
            self.ran.append(s)

    text = "-- header\nALTER TABLE a.b.c SET TAGS ('x');\nCOMMENT ON TABLE a.b.c IS 'it''s fine';\nALTER TABLE missing_table SET TAGS ('x');\n"
    sp = Spark()
    ok, failed = ct.apply_sql(text, sp, log=lambda *_: None)
    assert (ok, failed) == (2, 1) and sp.ran[1].startswith("COMMENT ON TABLE") and "it''s fine" in sp.ran[1]
