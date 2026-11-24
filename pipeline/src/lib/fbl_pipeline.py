"""Fictional Baseball League (FBL): contracts -> bronze -> silver (+DQ) -> gold (+assertions) -> semantic -> report.

CONTRACT-DRIVEN: stage_contracts publishes data contracts into governance.data_contracts and compiles their quality rules
into governance.dq_rules. Every later stage reads the REGISTRY: expected columns (bronze), the typed SELECT (silver),
the schema promises (all layers) and the semantic checks (metric views). If no contracts are published the stages fall
back to the built-in definitions below, so the pipeline still runs.

Runs in the CURRENT catalog (run `USE CATALOG <catalog>` first) using two-part names: bronze.x, silver.x, gold.x, governance.x.
Stage functions are plain Python so the same code runs in a Databricks notebook and in the local test.
"""
import html as _html
import json
import os
from datetime import datetime
from functools import reduce

from pyspark.sql import DataFrame, functions as F

from lib.dq_engine import (DQFailure, QUARANTINE_DDL, RESULTS_DDL, RULES_DDL, STATS_DDL, apply_rules, load_rules,
                           parse_rules_csv, raise_if_failed)
from lib.contracts import (ContractSet, checksum, compile_all_rules, cprops, expected_columns, layer_of, lint,
                           load_metric_view_definition, main_object, render, typed_select)
from lib.contract_checks import (CONTRACT_RESULTS_DDL, REGISTRY_DDL, ContractFailure, make_executor, raise_if_broken,
                                 schema_conformance, semantic_checks, write_results)

ENTITIES = ["team", "player", "batting"]          # order matters: parents before children (referential rules)
EXPECTED_COLUMNS = {
    "team": ["team_id", "team_name", "city", "league", "founded_year", "home_park", "payroll_cap_musd", "mascot"],
    "player": ["player_id", "first_name", "last_name", "birth_date", "bats", "throws", "team_id", "position",
               "height_in", "weight_lb", "nickname"],
    "batting": ["player_id", "season", "team_id", "g", "ab", "h", "doubles", "triples", "hr", "rbi", "bb", "so", "sb"],
}
FOLDERS = {"team": "teams", "player": "players", "batting": "batting"}

# ---- silver typing: trim, blank -> NULL, standardize case, try_cast (bad value -> NULL, caught later by not_null rules)
TYPED_SQL = {
"team": """
SELECT upper(trim(team_id)) AS team_id, NULLIF(trim(team_name), '') AS team_name, NULLIF(trim(city), '') AS city,
       initcap(NULLIF(trim(league), '')) AS league,
       try_cast(NULLIF(trim(founded_year), '') AS INT) AS founded_year, NULLIF(trim(home_park), '') AS home_park,
       try_cast(NULLIF(trim(payroll_cap_musd), '') AS DOUBLE) AS payroll_cap_musd, NULLIF(trim(mascot), '') AS mascot,
       _row_id, to_json(struct({RAW})) AS _raw, _ingested_at
FROM bronze.fbl_team t""",
"player": """
SELECT upper(trim(player_id)) AS player_id, NULLIF(trim(first_name), '') AS first_name, NULLIF(trim(last_name), '') AS last_name,
       try_cast(NULLIF(trim(birth_date), '') AS DATE) AS birth_date,
       upper(NULLIF(trim(bats), '')) AS bats, upper(NULLIF(trim(throws), '')) AS throws,
       upper(NULLIF(trim(team_id), '')) AS team_id, upper(NULLIF(trim(position), '')) AS position,
       try_cast(NULLIF(trim(height_in), '') AS INT) AS height_in, try_cast(NULLIF(trim(weight_lb), '') AS INT) AS weight_lb,
       NULLIF(trim(nickname), '') AS nickname,
       _row_id, to_json(struct({RAW})) AS _raw, _ingested_at
FROM bronze.fbl_player t""",
"batting": """
SELECT upper(trim(player_id)) AS player_id, try_cast(NULLIF(trim(season), '') AS INT) AS season, upper(trim(team_id)) AS team_id,
       try_cast(NULLIF(trim(g), '') AS INT) AS g, try_cast(NULLIF(trim(ab), '') AS INT) AS ab,
       try_cast(NULLIF(trim(h), '') AS INT) AS h, try_cast(NULLIF(trim(doubles), '') AS INT) AS doubles,
       try_cast(NULLIF(trim(triples), '') AS INT) AS triples, try_cast(NULLIF(trim(hr), '') AS INT) AS hr,
       try_cast(NULLIF(trim(rbi), '') AS INT) AS rbi, try_cast(NULLIF(trim(bb), '') AS INT) AS bb,
       try_cast(NULLIF(trim(so), '') AS INT) AS so, try_cast(NULLIF(trim(sb), '') AS INT) AS sb,
       _row_id, to_json(struct({RAW})) AS _raw, _ingested_at
FROM bronze.fbl_batting t""",
}

# ---- gold: optional descriptive text gets an explicit label (never default a numeric measure)
GOLD_SQL = {
"dim_team": """
SELECT team_id, team_name, city, league, founded_year, home_park, payroll_cap_musd, coalesce(mascot, 'None') AS mascot
FROM silver.fbl_team""",
"dim_player": """
SELECT player_id, first_name, last_name, concat(first_name, ' ', last_name) AS full_name, birth_date, bats, throws,
       team_id, position, height_in, weight_lb, coalesce(nickname, 'None') AS nickname
FROM silver.fbl_player""",
"fact_batting": """
SELECT player_id, season, team_id, g, ab, h, doubles, triples, hr, rbi, bb, so, sb,
       h - doubles - triples - hr AS singles,
       h + doubles + 2 * triples + 3 * hr AS total_bases,
       ab + bb AS plate_appearances_approx
FROM silver.fbl_batting""",
}
GOLD_SOURCE = {"dim_team": "team", "dim_player": "player", "fact_batting": "batting"}


def typed_sql(entity: str) -> str:
    """Typed silver SELECT. _raw keeps the ORIGINAL bronze text of the row so a rejected record shows what was really sent."""
    raw_cols = ", ".join(f"t.`{c}`" for c in EXPECTED_COLUMNS[entity])
    return TYPED_SQL[entity].replace("{RAW}", raw_cols)


def _append(df: DataFrame, table: str):
    df.write.mode("append").saveAsTable(table)


# --------------------------------------------------------------------------- contract registry
def load_registry(spark):
    """Published, active contracts as a ContractSet, or None if nothing has been published yet."""
    try:
        rows = spark.table("governance.data_contracts").where("status = 'active'").collect()
    except Exception:  # noqa: BLE001  (table missing before setup)
        return None
    return ContractSet([json.loads(r["contract_json"]) for r in rows]) if rows else None


def _entity_contracts(cs, layer, entity):
    return cs.entity(layer, entity, "fbl") if cs else None


# --------------------------------------------------------------------------- setup
def stage_setup(spark, rules_csv_text: str = None, reload_rules: bool = False):
    """Create schemas, governance tables and views. rules_csv_text is only for the legacy CSV rules mode."""
    for s in ["bronze", "silver", "gold", "semantic", "governance"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS governance.dq_rules ({RULES_DDL})")
    spark.sql(f"CREATE TABLE IF NOT EXISTS governance.dq_rule_results ({RESULTS_DDL})")
    spark.sql(f"CREATE TABLE IF NOT EXISTS governance.dq_table_stats ({STATS_DDL})")
    spark.sql(f"CREATE TABLE IF NOT EXISTS governance.dq_quarantine ({QUARANTINE_DDL})")
    spark.sql(f"CREATE TABLE IF NOT EXISTS governance.data_contracts ({REGISTRY_DDL})")
    spark.sql(f"CREATE TABLE IF NOT EXISTS governance.data_contracts_history ({REGISTRY_DDL})")
    spark.sql(f"CREATE TABLE IF NOT EXISTS governance.contract_results ({CONTRACT_RESULTS_DDL})")
    if rules_csv_text and (reload_rules or spark.table("governance.dq_rules").count() == 0):
        rules = parse_rules_csv(rules_csv_text)
        cols = [c.split()[0] for c in RULES_DDL.split(", ")]
        spark.createDataFrame([tuple(r[c] for c in cols) for r in rules], RULES_DDL).write.mode("overwrite").saveAsTable("governance.dq_rules")
    spark.sql("""CREATE OR REPLACE VIEW governance.v_dq_summary AS
        SELECT * FROM governance.dq_table_stats
        WHERE run_id = (SELECT run_id FROM governance.dq_table_stats ORDER BY checked_at DESC LIMIT 1)""")
    spark.sql("""CREATE OR REPLACE VIEW governance.v_dq_open_rejects AS
        SELECT run_id, source_table, row_id, rule_ids, payload, status, fix_notes
        FROM governance.dq_quarantine WHERE disposition = 'rejected' AND status = 'open'""")
    spark.sql("""CREATE OR REPLACE VIEW governance.v_contract_status AS
        SELECT contract_id, name, version, status, layer, dataset, owner, loaded_at FROM governance.data_contracts""")
    spark.sql("""CREATE OR REPLACE VIEW governance.v_contract_failures AS
        SELECT * FROM governance.contract_results WHERE passed = FALSE
        AND run_id = (SELECT run_id FROM governance.contract_results ORDER BY checked_at DESC LIMIT 1)""")
    print("setup complete; active rules:", spark.table("governance.dq_rules").where("active = 'Y'").count())


# --------------------------------------------------------------------------- publish contracts
def stage_contracts(spark, run_id: str, contracts: list, definitions: dict = None, origin: str = "unknown"):
    """Validate (lint) and publish contracts: registry + history + compiled DQ rules. Fails the run if lint finds errors."""
    cs = ContractSet(contracts)
    parsed = None
    if definitions:
        try:
            parsed = {name: load_metric_view_definition(sql) for name, sql in definitions.items()}
        except ImportError as e:
            print("NOTE: skipping metric-view definition checks (PyYAML missing); they are enforced in CI.", str(e)[:80])
    findings = lint(cs, parsed)
    rows = [(run_id, datetime.now(), cid, "", "", "lint", msg[:200], sev, False, "") for sev, cid, msg in findings]
    errors = [f for f in findings if f[0] == "error"]
    for sev, cid, msg in findings:
        print(f"  lint {sev}: {cid}: {msg}")
    if errors:
        write_results(spark, [(r[0], r[1], r[2], "0", "contract", r[5], r[6], r[7], False, "lint") for r in rows])
        raise ContractFailure(f"{len(errors)} contract lint error(s): " + "; ".join(f"{c}: {m}" for _, c, m in errors[:5]))

    now = datetime.now()
    reg = [(c["id"], c.get("name"), str(c["version"]), c["status"], layer_of(c), main_object(c)["physicalName"],
            cprops(c).get("owner"), checksum(c), now, json.dumps(c, sort_keys=True)) for c in contracts]
    spark.createDataFrame(reg, REGISTRY_DDL).write.mode("overwrite").saveAsTable("governance.data_contracts")
    hist = spark.table("governance.data_contracts_history")
    new = spark.createDataFrame(reg, REGISTRY_DDL).join(hist.select("contract_id", "checksum"), ["contract_id", "checksum"], "left_anti")
    n_new = new.count()
    if n_new:
        new.write.mode("append").saveAsTable("governance.data_contracts_history")
    rules = compile_all_rules(cs)
    cols = [c.split()[0] for c in RULES_DDL.split(", ")]
    spark.createDataFrame([tuple(r[c] for c in cols) for r in rules], RULES_DDL).write.mode("overwrite").saveAsTable("governance.dq_rules")
    warns = [(r[0], r[1], r[2], "0", "contract", r[5], r[6], r[7], False, "lint") for r in rows]
    write_results(spark, warns + [(run_id, now, "(all)", "", "contract", "publish", f"published {len(contracts)} contracts from {origin}",
                                    "info", True, f"{n_new} new or changed versions; {len(rules)} rules compiled")])
    print(f"published {len(contracts)} contracts ({n_new} new/changed) from {origin}; compiled {len(rules)} rules")


# --------------------------------------------------------------------------- bronze
def stage_bronze(spark, landing_dir: str, run_id: str):
    cs = load_registry(spark)
    results, contract_rows, now = [], [], datetime.now()
    for e in ENTITIES:
        sc = _entity_contracts(cs, "source", e)
        folder = cprops(main_object(sc))["landingFolder"] if sc else FOLDERS[e]
        expected = expected_columns(sc) if sc else EXPECTED_COLUMNS[e]
        path = f"{landing_dir}/{folder}/"
        df = spark.read.option("header", "true").option("inferSchema", "false").csv(path)   # all strings
        missing = [c for c in expected if c not in df.columns]
        n = df.count()
        results.append((run_id, now, "bronze", e, "BRZ-001", "columns_present", "fail", len(missing), n,
                        "Expected columns are present" + (f" (missing: {', '.join(missing)})" if missing else "")))
        results.append((run_id, now, "bronze", e, "BRZ-002", "not_empty", "fail", 1 if n == 0 else 0, n, "Source file is not empty"))
        if not missing:
            (df.select(*[F.col(c) for c in df.columns],
                       F.monotonically_increasing_id().alias("_row_id"),
                       F.col("_metadata.file_path").alias("_source_file"),
                       F.current_timestamp().alias("_ingested_at"))
               .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"bronze.fbl_{e}"))
            if sc:
                contract_rows += schema_conformance(spark, run_id, sc)
        print(f"bronze.fbl_{e}: {n} rows" + (f"  [contract {sc['id']} v{sc['version']}]" if sc else "  [no contract: built-in columns]"))
    res = spark.createDataFrame(results, RESULTS_DDL)
    spark.createDataFrame(results, RESULTS_DDL).write.mode("append").saveAsTable("governance.dq_rule_results")
    write_results(spark, contract_rows)
    raise_if_failed(res)
    raise_if_broken(contract_rows)


# --------------------------------------------------------------------------- silver
def stage_silver(spark, run_id: str):
    cs = load_registry(spark)
    all_results, contract_rows = [], []
    for e in ENTITIES:
        sv, src = _entity_contracts(cs, "silver", e), _entity_contracts(cs, "source", e)
        sql = typed_select(sv, src) if sv and src else typed_sql(e)
        typed = spark.sql(sql)
        rules = load_rules(spark, "silver", e)
        clean, log_df, res, stats = apply_rules(spark, typed, rules, run_id, "silver", e, mode="filter")
        clean.drop("_raw").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"silver.fbl_{e}")
        _append(log_df, "governance.dq_quarantine")
        _append(res, "governance.dq_rule_results")
        _append(_stats_df(spark, stats), "governance.dq_table_stats")
        all_results.append(res)
        if sv:
            contract_rows += schema_conformance(spark, run_id, sv)
        print(f"silver.fbl_{e}: in={stats['rows_in']} deduped={stats['rows_deduped']} rejected={stats['rows_rejected']} "
              f"out={stats['rows_out']}  [{'contract ' + sv['id'] + ' v' + sv['version'] if sv else 'built-in typing'}]")
    write_results(spark, contract_rows)
    raise_if_failed(reduce(DataFrame.unionByName, all_results))
    raise_if_broken(contract_rows)


def _stats_df(spark, s: dict) -> DataFrame:
    return (spark.createDataFrame([(s["run_id"], s["layer"], s["table_name"], s["rows_in"], s["rows_deduped"],
                                    s["rows_standardized"], s["rows_warned"], s["rows_rejected"], s["rows_out"])],
                                  "run_id STRING, layer STRING, table_name STRING, rows_in BIGINT, rows_deduped BIGINT, "
                                  "rows_standardized BIGINT, rows_warned BIGINT, rows_rejected BIGINT, rows_out BIGINT")
            .withColumn("checked_at", F.current_timestamp())
            .select(*[c.split()[0] for c in STATS_DDL.split(", ")]))


# --------------------------------------------------------------------------- gold
def stage_gold(spark, run_id: str):
    cs = load_registry(spark)
    all_results, contract_rows = [], []
    for g, src in GOLD_SOURCE.items():
        gc = _entity_contracts(cs, "gold", g)
        df = spark.sql(GOLD_SQL[g])
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"gold.fbl_{g}")
        built = spark.table(f"gold.fbl_{g}")
        rules = load_rules(spark, "gold", g)
        _, _, res, stats = apply_rules(spark, built, rules, run_id, "gold", g, mode="assert")
        # reconciliation: gold must neither lose nor invent rows relative to clean silver
        recon_table = cprops(main_object(gc)).get("reconcilesTo") if gc else f"silver.fbl_{src}"
        silver_n, gold_n = spark.table(recon_table).count(), built.count()
        rec = spark.createDataFrame([(run_id, datetime.now(), "gold", g, "GLD-RECON", "reconcile", "fail",
                                      abs(silver_n - gold_n), gold_n, f"Row count gold = silver ({silver_n} vs {gold_n})")], RESULTS_DDL)
        res = res.unionByName(rec)
        _append(res, "governance.dq_rule_results")
        _append(_stats_df(spark, stats), "governance.dq_table_stats")
        all_results.append(res)
        if gc:
            contract_rows += schema_conformance(spark, run_id, gc)
        print(f"gold.fbl_{g}: {gold_n} rows  [{'contract ' + gc['id'] + ' v' + gc['version'] if gc else 'no contract'}]")
    write_results(spark, contract_rows)
    raise_if_failed(reduce(DataFrame.unionByName, all_results))
    raise_if_broken(contract_rows)


# --------------------------------------------------------------------------- semantic (metric views + their contracts)
def stage_semantic(spark, run_id: str, catalog: str, definitions: dict = None, build: bool = True,
                   executor_factory=None, product: str = "fbl"):
    """Create the metric views from their definitions, then check each view against its semantic contract."""
    cs = load_registry(spark)
    if not cs:
        raise ContractFailure("no contracts published: run the contracts stage first")
    rows = []
    for c in cs.layer("semantic", product):
        obj = main_object(c)
        defname = os.path.basename(cprops(obj)["definition"])
        view_fqn = f"{catalog}.{obj['physicalName']}"
        if build:
            sql = (definitions or {}).get(defname)
            if not sql:
                raise ContractFailure(f"{c['id']}: definition {defname} not found (upload it with the contracts bundle)")
            spark.sql(render(sql, catalog).rstrip().rstrip(";"))
            print(f"created {view_fqn}")
        executor = executor_factory(c, view_fqn) if executor_factory else make_executor(spark, view_fqn)
        got = semantic_checks(spark, run_id, c, executor)
        rows += got
        bad = [r for r in got if r[7] == "error" and not r[8]]
        print(f"{c['id']} v{c['version']}: {len(got)} checks, {len(bad)} broken")
    write_results(spark, rows)
    raise_if_broken(rows)


# --------------------------------------------------------------------------- report
def build_report(spark, run_id: str) -> dict:
    stats = spark.table("governance.dq_table_stats").where(F.col("run_id") == run_id).orderBy("checked_at").collect()
    viol = (spark.table("governance.dq_rule_results").where((F.col("run_id") == run_id) & (F.col("violations") > 0))
            .orderBy("layer", "table_name", "rule_id").collect())
    rej = (spark.table("governance.dq_quarantine").where((F.col("run_id") == run_id) & (F.col("disposition") == "rejected"))
           .orderBy("source_table", "row_id").collect())
    fixed = (spark.table("governance.dq_quarantine").where((F.col("run_id") == run_id) & (F.col("disposition") == "auto_corrected"))
             .groupBy("source_table", "rule_ids").count().orderBy("source_table", "rule_ids").collect())
    cres = [r for r in spark.table("governance.contract_results").where((F.col("run_id") == run_id) & (F.col("passed") == False)).collect()]  # noqa: E712
    n_cbroken = sum(1 for r in cres if r["severity"] == "error")
    n_rej = len(rej)
    n_failed = sum(1 for v in viol if v["action"] == "fail")
    lines = [f"DQ run {run_id}: {n_rej} rows rejected, {sum(f['count'] for f in fixed)} auto-corrections, {n_failed} failed rules, "
             f"{n_cbroken} broken contract checks"]
    for r in cres[:30]:
        lines.append(f"  CONTRACT {r['severity'].upper()} {r['contract_id']} [{r['check_type']}] {r['check_name']}: {r['detail'][:120]}")
    for s in stats:
        lines.append(f"  {s['layer']}.{s['table_name']}: in={s['rows_in']} deduped={s['rows_deduped']} "
                     f"rejected={s['rows_rejected']} out={s['rows_out']}")
    for r in rej[:50]:
        lines.append(f"  REJECT {r['source_table']} row {r['row_id']} [{r['rule_ids']}] {r['payload'][:140]}")
    esc = _html.escape
    def table(head, rows):
        return ("<table border='1' cellpadding='4' style='border-collapse:collapse;font-family:sans-serif;font-size:13px'><tr>"
                + "".join(f"<th>{esc(h)}</th>" for h in head) + "</tr>"
                + "".join("<tr>" + "".join(f"<td>{esc(str(c))}</td>" for c in row) + "</tr>" for row in rows) + "</table>")
    html = (f"<h2>Data quality report</h2><p>Run <code>{esc(run_id)}</code>: <b>{n_rej}</b> rows rejected, "
            f"<b>{n_failed}</b> failed rules.</p><h3>Row flow by table</h3>"
            + table(["layer", "table", "in", "deduped", "standardized", "warned", "rejected", "out"],
                    [(s["layer"], s["table_name"], s["rows_in"], s["rows_deduped"], s["rows_standardized"], s["rows_warned"],
                      s["rows_rejected"], s["rows_out"]) for s in stats])
            + "<h3>Rules with violations</h3>"
            + table(["layer", "table", "rule", "action", "violations", "description"],
                    [(v["layer"], v["table_name"], v["rule_id"], v["action"], v["violations"], v["description"]) for v in viol])
            + "<h3>Auto-corrected (no action needed)</h3>"
            + table(["table", "rule", "rows"], [(f["source_table"], f["rule_ids"], f["count"]) for f in fixed])
            + "<h3>Contract checks that did not pass</h3>"
            + table(["contract", "type", "check", "severity", "detail"],
                    [(r["contract_id"], r["check_type"], r["check_name"], r["severity"], r["detail"]) for r in cres])
            + "<h3>Rejected rows: kick back to the source owner</h3>"
            + table(["table", "row", "rules", "original record"], [(r["source_table"], r["row_id"], r["rule_ids"], r["payload"]) for r in rej]))
    return {"text": "\n".join(lines), "html": html, "rejected": n_rej, "failed_rules": n_failed, "contract_failures": n_cbroken}
