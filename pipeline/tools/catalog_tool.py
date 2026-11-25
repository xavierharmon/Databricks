#!/usr/bin/env python3
"""Domains, glossary Pages and data products as code.

    python tools/catalog_tool.py lint                 # check the YAML, and cross-check it against your contracts
    python tools/catalog_tool.py render --out docs/glossary      # Markdown glossary + product catalog + catalog.json (works anywhere)
    python tools/catalog_tool.py tags-sql --catalog pipeline_mlb_dev          # SQL to tag and comment tables (works on Free Edition)
    python tools/catalog_tool.py publish              # DRY RUN: show what would be sent to the Databricks Domains and Pages APIs
    python tools/catalog_tool.py publish --apply      # really call the APIs (needs DATABRICKS_HOST and DATABRICKS_TOKEN)

Why YAML files: a glossary term, a domain and a data product are definitions. Keeping them in Git gives review, history and the
same pull-request flow as your contracts. Databricks is then one place they are PUBLISHED to, not the only place they exist.

What is verified and what is not (read this):
  * lint, render, tags-sql run locally and are covered by tests/test_catalog_tool.py.
  * publish follows the documented Domains and Pages REST APIs (Beta). It has NOT been run against a real workspace. Pages and
    domains are account-level features, and Free Edition has no account console, so check that your workspace has a Discover page
    before relying on it. If it does not, `render` and `tags-sql` still give you the glossary and the tagging.
  * Governed tags must exist before a domain can use them. This tool does not create them.
"""
import argparse
import glob
import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
ICONS = {"ADDRESS_BOOK", "ALARM", "ARROWS_IN", "ATOM", "BALLOON", "BANK", "BARRICADE", "BASKET", "BRIDGE", "CACTUS", "CALL_BELL", "CARROT",
         "CHART_PIE_SLICE", "CITY", "CLOUD", "COINS", "COMPASS_ROSE", "CRANE_TOWER", "CROWN", "CUBE_TRANSPARENT", "FADERS", "FLAG_BANNER_FOLD",
         "FLAG_CHECKERED", "GAVEL", "HAMBURGER", "HEAD_CIRCUIT", "HOURGLASS_HIGH", "INTERSECT_THREE", "MICROSCOPE", "MOON_STARS", "PACKAGE",
         "PARACHUTE", "PEPPER", "PIGGY_BANK", "PILL", "PLANET", "PLANT", "PLUGS_CONNECTED", "POPCORN", "PRESENTATION_CHART", "PUZZLE_PIECE",
         "RAINBOW", "RANKING", "RECEIPT", "ROCKET", "RULER", "SAILBOAT", "SCALES", "SCAN_SMILEY", "SCROLL", "SHIELD_CHECKERED", "SNEAKER",
         "SNOWFLAKE", "SOLAR_ROOF", "SPEEDOMETER", "STAMP", "STEPS", "STRATEGY", "SWORD", "TELEVISION_SIMPLE", "TENT", "TICKET", "TRACTOR",
         "TRAFFIC_CONE", "TRAIN", "TREE_EVERGREEN", "TREE_STRUCTURE", "TROLLEY_SUITCASE", "TROPHY", "TRUCK_TRAILER", "USERS_THREE", "VECTOR_THREE"}
PAGE_STATES = {"draft", "published"}
PRODUCT_STATUS = {"draft", "active", "deprecated"}
ASSET_KINDS = {"contract", "metric_view", "table"}
PII = [(re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+"), "an email address"), (re.compile(r"\b\d{3}[-. ]\d{3}[-. ]\d{4}\b"), "a phone number")]


# ------------------------------------------------------------------------------------------------ loading
def read_yaml(path):
    """Parse one YAML file; a mistake is reported with the file name and line instead of a traceback."""
    try:
        return yaml.safe_load(open(path))
    except yaml.YAMLError as e:
        mark = getattr(e, "problem_mark", None)
        where = f" at line {mark.line + 1}, column {mark.column + 1}" if mark else ""
        raise SystemExit(f"{path} is not valid YAML{where}: {getattr(e, 'problem', e)}. A colon followed by a space inside a value needs the value in quotes.")


def load_catalog(cdir):
    cdir = Path(cdir)
    doms = read_yaml(cdir / "domains.yml") or {}
    flat = []

    def walk(d, parent=None, depth=1):
        item = dict(d, parent=parent, depth=depth)
        item.pop("subdomains", None)
        flat.append(item)
        for s in d.get("subdomains", []) or []:
            walk(s, d["id"], depth + 1)

    for d in doms.get("domains", []):
        walk(d)
    pages = []
    for f in sorted(glob.glob(str(cdir / "pages" / "**" / "*.y*ml"), recursive=True)):
        p = read_yaml(f)
        p["_file"] = os.path.relpath(f, cdir)
        pages.append(p)
    products = []
    for f in sorted(glob.glob(str(cdir / "products" / "*.y*ml"))):
        p = read_yaml(f)
        p["_file"] = os.path.relpath(f, cdir)
        products.append(p)
    return dict(domains=flat, pages=pages, products=products)


def load_contracts(dirs):
    out = {}
    for d in dirs:
        for f in glob.glob(os.path.join(d, "**", "*.y*ml"), recursive=True):
            try:
                c = yaml.safe_load(open(f))
            except yaml.YAMLError:
                continue
            if isinstance(c, dict) and c.get("kind") == "DataContract" and c.get("id"):
                out[c["id"]] = c
    return out


# ------------------------------------------------------------------------------------------------ lint
def lint(cat, contracts, strict=False):
    """Return [(severity, where, message)]."""
    out = []
    err = lambda w, m: out.append(("error", w, m))
    warn = lambda w, m: out.append(("error" if strict else "warn", w, m))
    ids = {}
    for d in cat["domains"]:
        w = f"domain {d.get('id')}"
        if d.get("id") in ids:
            err(w, "duplicate domain id")
        ids[d.get("id")] = d
        for k in ("id", "tag_key", "display_name", "subtitle", "description"):
            if not d.get(k):
                err(w, f"missing {k}")
        tag = d.get("tag_key", "")
        if d["parent"] is None and "/" in tag:
            err(w, "a top-level domain's tag must not contain a slash (the slash is reserved for subdomains)")
        if d["parent"]:
            ptag = ids[d["parent"]].get("tag_key", "")
            if not tag.startswith(ptag + "/") or tag.count("/") != ptag.count("/") + 1:
                err(w, f"a subdomain's tag must be '<parent tag>/<name>' (expected prefix {ptag}/)")
        if d["depth"] > 5:
            err(w, "domains nest at most five levels")
        if d.get("icon") and d["icon"] not in ICONS:
            err(w, f"icon {d['icon']!r} is not one of the icons Databricks offers")
        if d.get("color") and not re.fullmatch(r"#[0-9A-Fa-f]{6}", str(d["color"])):
            err(w, "color must look like #1B5E20")
        if len(d.get("subtitle", "")) > 280:
            err(w, "subtitle is longer than 280 characters")
        if len(d.get("description", "")) > 4096:
            err(w, "description is longer than 4096 characters")
        if d["parent"] is None and d.get("display_name", "").strip().lower() == "general":
            err(w, "'General' is reserved for the default domain")
    seen = set()
    for p in cat["pages"]:
        w = f"page {p.get('page')} ({p['_file']})"
        for k in ("page", "domain", "title", "description", "body", "state"):
            if not p.get(k):
                err(w, f"missing {k}")
        if p.get("domain") not in ids:
            err(w, f"domain {p.get('domain')!r} is not defined in domains.yml")
        if p.get("state") not in PAGE_STATES:
            err(w, f"state must be one of {sorted(PAGE_STATES)}")
        key = (p.get("domain"), str(p.get("title", "")).lower())
        if key in seen:
            err(w, "two pages share a title in the same domain (published titles must be unique per domain)")
        seen.add(key)
        if not 1 <= len(str(p.get("title", ""))) <= 200:
            err(w, "title must be 1 to 200 characters")
        syn = p.get("synonyms") or []
        if len(syn) > 50 or any(not 1 <= len(str(s)) <= 200 for s in syn):
            err(w, "at most 50 synonyms, each 1 to 200 characters")
        text = " ".join(str(p.get(k, "")) for k in ("title", "description", "body")) + " " + " ".join(map(str, syn))
        for rx, what in PII:
            if rx.search(text):
                err(w, f"looks like it contains {what}. Page text is replicated globally; keep personal data out")
        for a in p.get("related_assets") or []:
            if a.get("kind") not in ASSET_KINDS:
                err(w, f"related asset kind must be one of {sorted(ASSET_KINDS)}")
            elif a["kind"] in ("contract", "metric_view") and contracts and a.get("name") not in contracts:
                warn(w, f"related {a['kind']} {a.get('name')!r} has no contract yet")
    pids = set()
    for p in cat["products"]:
        w = f"product {p.get('id')} ({p['_file']})"
        for k in ("apiVersion", "kind", "id", "name", "version", "status", "domain", "description", "team", "artifacts"):
            if not p.get(k):
                err(w, f"missing {k}")
        if p.get("kind") != "DataProduct":
            err(w, "kind must be DataProduct")
        if p.get("id") in pids:
            err(w, "duplicate product id")
        pids.add(p.get("id"))
        if p.get("status") not in PRODUCT_STATUS:
            err(w, f"status must be one of {sorted(PRODUCT_STATUS)}")
        if p.get("domain") not in ids:
            err(w, f"domain {p.get('domain')!r} is not defined in domains.yml")
        desc = p.get("description") or {}
        for k in ("purpose", "limitations", "usage"):
            if not desc.get(k):
                err(w, f"description.{k} is empty")
        if not (p.get("team") or {}).get("owner"):
            err(w, "team.owner is required")
        for a in p.get("artifacts") or []:
            if not a.get("versions"):
                err(w, f"artifact {a.get('name')} has no versions")
            for v in a.get("versions") or []:
                for o in v.get("out") or []:
                    cid = o.get("contractId")
                    if not contracts:
                        continue
                    if cid not in contracts:
                        (err if p.get("status") == "active" and v.get("status") == "active" else warn)(w, f"artifact {a['name']} points at contract {cid}, which does not exist yet")
                        continue
                    c = contracts[cid]
                    layer = {x["property"]: x.get("value") for x in c.get("customProperties", [])}.get("layer")
                    if layer not in ("gold", "semantic"):
                        warn(w, f"artifact {a['name']}: {cid} is a {layer} contract; products should expose gold or semantic outputs")
                    if str(c.get("version")) != str(o.get("version")):
                        warn(w, f"artifact {a['name']}: {cid} is version {c.get('version')} but the product says {o.get('version')}")
    for cid, c in contracts.items():                                            # the contract's own `domain:` must be a catalog domain
        if c.get("domain") and c["domain"] not in ids:
            warn(f"contract {cid}", f"domain {c['domain']!r} is not defined in catalog/domains.yml")
    return out


# ------------------------------------------------------------------------------------------------ render
def render(cat, out):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    names = {d["id"]: d["display_name"] for d in cat["domains"]}
    by_dom = {}
    for p in cat["pages"]:
        by_dom.setdefault(p["domain"], []).append(p)
    index = ["# Glossary\n", "_Generated from catalog/ by tools/catalog_tool.py. Edit the YAML, not this file._\n"]
    for d in cat["domains"]:
        pages = sorted(by_dom.get(d["id"], []), key=lambda p: p["title"].lower())
        index.append(f"\n{'#' * (d['depth'] + 1)} {d['display_name']}\n\n{d['description']}\n")
        if pages:
            index.append("| Term | Meaning | Also called |\n| --- | --- | --- |")
            for p in pages:
                index.append(f"| [{p['title']}]({p['page']}.md) | {p['description']} | {', '.join(p.get('synonyms') or [])} |")
        for p in pages:
            rel = "\n".join(f"- {a['kind']}: `{a['name']}`" for a in p.get("related_assets") or [])
            (out / f"{p['page']}.md").write_text(f"# {p['title']}\n\n_{names[p['domain']]} · owner: {p.get('owner', '')} · {p['state']}_\n\n{p['description']}\n\n{p['body']}\n"
                                                  + (f"\n## Related\n{rel}\n" if rel else ""))
    (out / "index.md").write_text("\n".join(index) + "\n")
    prod = ["# Data products\n"]
    for p in cat["products"]:
        prod.append(f"\n## {p['name']} (`{p['id']}`)\n\n_{p['status']} · v{p['version']} · domain {names.get(p['domain'], p['domain'])} · owner {p['team'].get('owner')}_\n")
        d = p["description"]
        prod.append(f"**Purpose.** {d['purpose']}\n\n**Limitations.** {d['limitations']}\n\n**Usage.** {d['usage']}\n")
        prod.append("| Artifact | Version | Status | Backed by contracts |\n| --- | --- | --- | --- |")
        for a in p["artifacts"]:
            for v in a["versions"]:
                prod.append(f"| {a['name']} | {v['version']} | {v['status']} | {', '.join('`%s`' % o['contractId'] for o in v.get('out', []))} |")
    (out / "products.md").write_text("\n".join(prod) + "\n")
    (out / "catalog.json").write_text(json.dumps(dict(domains=cat["domains"], pages=[{k: v for k, v in p.items() if not k.startswith("_")} for p in cat["pages"]],
                                                      products=[{k: v for k, v in p.items() if not k.startswith("_")} for p in cat["products"]]), indent=1, default=str))
    return len(cat["pages"]), len(cat["products"])


# ------------------------------------------------------------------------------------------------ SQL for tags and comments
def tags_sql(cat, contracts, catalog):
    """Tag and comment the tables behind each product artifact. Plain Unity Catalog SQL: works without any account-level feature."""
    dom = {d["id"]: d for d in cat["domains"]}
    lines = [f"-- generated by tools/catalog_tool.py for catalog {catalog}. Review before running.",
             "-- A key-only tag marks domain membership (the Discover page uses a governed tag whose KEY is the domain tag).", ""]
    for p in cat["products"]:
        d = dom[p["domain"]]
        for a in p["artifacts"]:
            for v in a["versions"]:
                for o in v.get("out", []):
                    c = contracts.get(o["contractId"])
                    if not c:
                        lines.append(f"-- skipped {o['contractId']}: no contract yet")
                        continue
                    obj = c["schema"][0]
                    kind = "VIEW" if obj.get("physicalType") == "view" else "TABLE"
                    fq = f"{catalog}.{obj['physicalName']}"
                    purpose = (c.get("description") or {}).get("purpose", "").replace("'", "''")
                    lines.append(f"ALTER {kind} {fq} SET TAGS ('{d['tag_key']}', 'data_product' = '{p['id']}', 'product_artifact' = '{a['name']}');")
                    lines.append(f"COMMENT ON {kind} {fq} IS '{purpose} [data product: {p['name']}]';")
    return "\n".join(lines) + "\n"


def apply_sql(sql_text, spark, log=print):
    """Run the statements from tags_sql() inside Databricks. A failing statement (a table that does not exist yet, a tag key that is not
    allowed) is reported and skipped, so one problem does not stop the rest."""
    ok = failed = 0
    for stmt in [s.strip() for s in re.split(r";\s*(?:\n|$)", sql_text) if s.strip()]:
        body = "\n".join(l for l in stmt.splitlines() if not l.strip().startswith("--")).strip()
        if not body:
            continue
        try:
            spark.sql(body)
            ok += 1
        except Exception as e:  # noqa: BLE001
            failed += 1
            log(f"  skipped: {body[:90]}... -> {str(e)[:160]}")
    log(f"applied {ok} statements, {failed} skipped")
    return ok, failed


# ------------------------------------------------------------------------------------------------ publish
class Client:
    def __init__(self, host, token):
        self.host, self.token = host.rstrip("/"), token

    def request(self, method, path, body=None, query=None):
        url = self.host + path + (("?" + urllib.parse.urlencode(query)) if query else "")
        req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body is not None else None,
                                     headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read() or b"{}")
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"{method} {path} -> {e.code}: {e.read().decode()[:400]}")


def slug(s):
    return s.replace("/", "-").replace("_", "-")


def request_id(key):
    b = bytearray(hashlib.sha256(key.encode()).digest()[:16])
    return str(uuid.UUID(bytes=bytes(b), version=4))


def publish(cat, client, principals, apply=False, publish_pages=False, log=print):
    """Create or update domains, then pages. With apply=False nothing is sent, and no network call is made."""
    plan = []
    domain_name = {}                                                           # our id -> 'domains/{domain_id}'
    existing = {}
    if apply:
        for d in client.request("GET", "/api/2.0/domains").get("domains", []):
            existing[d["tag_key"]] = d
    for d in cat["domains"]:                                                   # parents come before children in the flattened list
        have = existing.get(d["tag_key"])
        owners = {k: [int(principals[n]) for n in (d.get("owners", {}).get(k) or []) if principals.get(n) not in (None, "0")] for k in ("technical", "business")}
        body = dict(tag_key=d["tag_key"], subtitle=d["subtitle"], description=d["description"], draft=True)
        if d.get("icon"):
            body["icon"] = dict(name=d["icon"], color=d.get("color", "#37474F"))
        if owners["technical"]:
            body["technical_owner_ids"] = owners["technical"]
        if owners["business"]:
            body["business_owner_ids"] = owners["business"]
        if d["parent"]:
            body["parent_domain_id"] = (domain_name.get(d["parent"]) or f"domains/{slug(d['parent'])}").split("/")[-1]
        if have:
            domain_name[d["id"]] = have["name"]
            upd = {k: body[k] for k in ("subtitle", "description", "icon") if k in body}
            plan.append(f"update domain {d['id']} ({','.join(upd)})")
            if apply:
                client.request("PATCH", f"/api/2.0/{have['name']}", dict(tag_key=d["tag_key"], **upd), {"update_mask": ",".join(upd)})
        else:
            plan.append(f"create domain {d['id']} (tag {d['tag_key']}) as a draft")
            domain_name[d["id"]] = f"domains/{slug(d['id'])}"
            if apply:
                r = client.request("POST", "/api/2.0/domains", body, {"domain_id": slug(d["id"])})
                domain_name[d["id"]] = r["name"]
    for p in cat["pages"]:
        dn = domain_name[p["domain"]]
        state = "PUBLISHED" if (publish_pages and p["state"] == "published") else "DRAFT"
        body = dict(domain=dn, display_name=p["title"], body=p["body"], description=p["description"], synonyms=p.get("synonyms") or [], state=state)
        found = None
        if apply:
            for x in client.request("GET", "/api/2.0/knowledge/pages", query={"filter": f'domain = "{dn}"', "page_size": 100}).get("pages", []):
                if x["display_name"] == p["title"]:
                    found = x
        if found:
            plan.append(f"update page '{p['title']}' in {p['domain']} -> {state}")
            if apply:
                client.request("PATCH", f"/api/2.0/knowledge/{found['name']}", {k: v for k, v in body.items() if k != "domain"},
                               {"update_mask": "display_name,body,description,synonyms,state"})
        else:
            plan.append(f"create page '{p['title']}' in {p['domain']} as {state}")
            if apply:
                client.request("POST", "/api/2.0/knowledge/pages", body, {"request_id": request_id(f"{p['domain']}/{p['page']}")})
    for line in plan:
        log(("  " if apply else "  would ") + line)
    return plan


# ------------------------------------------------------------------------------------------------ cli
def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--catalog-dir", default=str(ROOT / "catalog"))
    ap.add_argument("--contracts", action="append", help="contract folder to cross-check (repeatable; default ./contracts and ./student/contracts)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    l = sub.add_parser("lint"); l.add_argument("--strict", action="store_true", help="warnings become errors")
    r = sub.add_parser("render"); r.add_argument("--out", default=str(ROOT / "docs" / "glossary"))
    t = sub.add_parser("tags-sql"); t.add_argument("--catalog", required=True)
    ta = sub.add_parser("tags-apply", help="run the tag and comment SQL inside Databricks (needs a Spark session)"); ta.add_argument("--catalog", required=True)
    p = sub.add_parser("publish")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--publish-pages", action="store_true", help="publish pages marked published (default: create them as drafts)")
    a = ap.parse_args(argv)
    cat = load_catalog(a.catalog_dir)
    dirs = a.contracts or [str(ROOT / "contracts"), str(ROOT / "student" / "contracts")]
    contracts = load_contracts([d for d in dirs if os.path.isdir(d)])
    if a.cmd == "lint":
        found = lint(cat, contracts, a.strict)
        for sev, w, m in found:
            print(f"{sev.upper():5} {w}: {m}")
        n_err = sum(1 for f in found if f[0] == "error")
        print(f"{len(cat['domains'])} domains, {len(cat['pages'])} pages, {len(cat['products'])} products checked against {len(contracts)} contracts: {n_err} errors, {len(found) - n_err} warnings")
        return 1 if n_err else 0
    if a.cmd == "render":
        n, m = render(cat, a.out)
        print(f"wrote {n} pages and {m} products to {a.out}")
        return 0
    if a.cmd == "tags-sql":
        print(tags_sql(cat, contracts, a.catalog))
        return 0
    if a.cmd == "tags-apply":
        from pyspark.sql import SparkSession
        sql = tags_sql(cat, contracts, a.catalog)
        apply_sql(sql, SparkSession.builder.getOrCreate())
        return 0
    if a.cmd == "publish":
        pr = Path(a.catalog_dir) / "principals.yml"
        principals = yaml.safe_load(pr.read_text()) if pr.exists() else {}
        client = None
        if a.apply:
            host, token = os.environ.get("DATABRICKS_HOST"), os.environ.get("DATABRICKS_TOKEN")
            if not host or not token:
                print("set DATABRICKS_HOST and DATABRICKS_TOKEN first (a personal access token or OAuth token)")
                return 2
            client = Client(host, token)
        if any(sev == "error" for sev, _, _ in lint(cat, contracts)):
            print("lint errors: fix them first (python tools/catalog_tool.py lint)")
            return 1
        publish(cat, client, principals, apply=a.apply, publish_pages=a.publish_pages)
        if not a.apply:
            print("dry run only. Add --apply to send these to Databricks.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
