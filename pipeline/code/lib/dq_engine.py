"""Rules-driven data-quality engine (PySpark).

A RULE is one row of governance.dq_rules (source: data/dq/dq_rules.csv):
  rule_id, layer, table_name, rule_type, columns, expression, action, description, owner, active

rule_type    what it checks (a row VIOLATES the rule when...)
  not_null          any listed column is NULL or blank
  allowed_values    column is not in the list in `expression`, e.g. 'L','R','B'
  range             column is outside `expression`, written like  60 AND 84
  custom            the SQL condition in `expression` is FALSE  (business rules). NULL = not a violation
                    (NULLs are caught by a not_null rule instead, so one problem is reported once)
  referential       column value not found in another table: expression = schema.table.column
  exact_duplicate   row is an identical copy of an earlier row (all data columns equal)
  unique            key columns repeat: later copies violate (the first copy is kept)
  unique_strict     key columns repeat: EVERY copy violates (conflicting duplicates need a human)
  standardized      value was changed by silver cleaning (e.g. 'r' -> 'R'): logged, never rejected
  no_nulls          gold guarantee: no NULL in any column (columns '*' minus `expression` exclusions)

action       what happens to violating rows
  dedupe       auto-corrected: extra exact copies removed (disposition = auto_corrected)
  standardize  auto-corrected value kept, logged (disposition = auto_corrected)
  quarantine   removed from the clean table, written to governance.dq_quarantine (disposition = rejected)
  warn         row stays, violation logged (disposition = warning)
  fail         the stage raises an error after logging, stopping the job

Conventions: the DataFrame has `_row_id` (unique per row) and optionally `_raw` (JSON of the original bronze row).
Columns starting with "_" are metadata, never checked as data.
"""
import csv
import io
from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

RULE_TYPES = {"not_null", "allowed_values", "range", "custom", "referential", "exact_duplicate",
              "unique", "unique_strict", "standardized", "no_nulls"}
ACTIONS = {"dedupe", "standardize", "quarantine", "warn", "fail"}

RESULTS_DDL = ("run_id STRING, checked_at TIMESTAMP, layer STRING, table_name STRING, rule_id STRING, "
               "rule_type STRING, action STRING, violations BIGINT, rows_checked BIGINT, description STRING")
STATS_DDL = ("run_id STRING, checked_at TIMESTAMP, layer STRING, table_name STRING, rows_in BIGINT, "
             "rows_deduped BIGINT, rows_standardized BIGINT, rows_warned BIGINT, rows_rejected BIGINT, rows_out BIGINT")
QUARANTINE_DDL = ("run_id STRING, checked_at TIMESTAMP, source_table STRING, row_id BIGINT, rule_ids STRING, "
                  "disposition STRING, payload STRING, status STRING, fix_notes STRING")
RULES_DDL = ("rule_id STRING, layer STRING, table_name STRING, rule_type STRING, columns STRING, expression STRING, "
             "action STRING, description STRING, owner STRING, active STRING")


class DQFailure(Exception):
    """Raised when a rule with action 'fail' (or any gold assertion) is violated."""


# --------------------------------------------------------------------------- rules
def parse_rules_csv(text: str) -> list:
    rules = [{k: (v or "").strip() for k, v in row.items()} for row in csv.DictReader(io.StringIO(text))]
    validate_rules(rules)
    return rules


def validate_rules(rules: list):
    seen = set()
    for r in rules:
        if r["rule_id"] in seen:
            raise ValueError(f"duplicate rule_id {r['rule_id']}")
        seen.add(r["rule_id"])
        if r["rule_type"] not in RULE_TYPES:
            raise ValueError(f"{r['rule_id']}: unknown rule_type {r['rule_type']!r}")
        if r["action"] not in ACTIONS:
            raise ValueError(f"{r['rule_id']}: unknown action {r['action']!r}")
        if r["rule_type"] in {"allowed_values", "range", "custom", "referential"} and not r["expression"]:
            raise ValueError(f"{r['rule_id']}: {r['rule_type']} needs an expression")
        if r["rule_type"] != "exact_duplicate" and not r["columns"] and r["rule_type"] != "custom":
            raise ValueError(f"{r['rule_id']}: columns required")
        if r["rule_type"] == "exact_duplicate" and r["action"] != "dedupe":
            raise ValueError(f"{r['rule_id']}: exact_duplicate must use action dedupe")


def load_rules(spark: SparkSession, layer: str, table_name: str) -> list:
    rows = (spark.table("governance.dq_rules")
            .where((F.col("layer") == layer) & (F.col("table_name") == table_name) & (F.col("active") == "Y"))
            .orderBy("rule_id").collect())
    return [r.asDict() for r in rows]


# --------------------------------------------------------------------------- helpers
def data_columns(df: DataFrame) -> list:
    return [c for c in df.columns if not c.startswith("_")]


def _cols(rule: dict) -> list:
    return [c.strip() for c in (rule.get("columns") or "").split(",") if c.strip()]


def _q(c: str) -> str:
    return f"`{c}`"


def _predicate(rule: dict, df_cols: list):
    t, cols, e = rule["rule_type"], _cols(rule), (rule.get("expression") or "").strip()
    if t == "not_null":
        return F.expr(" OR ".join(f"({_q(c)} IS NULL OR trim(CAST({_q(c)} AS STRING)) = '')" for c in cols))
    if t == "allowed_values":
        return F.expr(f"{_q(cols[0])} IS NOT NULL AND {_q(cols[0])} NOT IN ({e})")
    if t == "range":
        return F.expr(f"{_q(cols[0])} IS NOT NULL AND NOT ({_q(cols[0])} BETWEEN {e})")
    if t == "custom":
        return F.expr(f"NOT coalesce(({e}), TRUE)")
    if t == "no_nulls":
        exclude = {x.strip() for x in e.split(",") if x.strip()}
        check = [c for c in df_cols if c not in exclude] if cols == ["*"] else cols
        return F.expr(" OR ".join(f"{_q(c)} IS NULL" for c in check))
    if t == "standardized":
        c = cols[0]
        raw = f"get_json_object(_raw, '$.{c}')"
        return F.expr(f"{raw} IS NOT NULL AND {_q(c)} IS NOT NULL AND {raw} <> CAST({_q(c)} AS STRING)")
    raise ValueError(f"no predicate for {t}")


def _add_flag(spark, df: DataFrame, rule: dict, flag: str) -> DataFrame:
    t, cols = rule["rule_type"], _cols(rule)
    if t in ("unique", "unique_strict"):
        w = Window.partitionBy(*[F.col(_q(k)) for k in cols])
        dup = (F.row_number().over(w.orderBy("_row_id")) > 1) if t == "unique" else (F.count(F.lit(1)).over(w) > 1)
        keys_present = reduce(lambda a, b: a & b, [F.col(_q(k)).isNotNull() for k in cols])
        return df.withColumn(flag, F.coalesce(dup & keys_present, F.lit(False)))
    if t == "referential":
        ref_table, ref_col = rule["expression"].rsplit(".", 1)
        ref = spark.table(ref_table).select(F.col(ref_col).alias("_ref_k")).distinct()
        col = cols[0]
        return (df.join(ref, df[_q(col)] == F.col("_ref_k"), "left")
                  .withColumn(flag, F.col(_q(col)).isNotNull() & F.col("_ref_k").isNull())
                  .drop("_ref_k"))
    return df.withColumn(flag, F.coalesce(_predicate(rule, [c for c in df.columns if not c.startswith("_")]), F.lit(False)))


def _log_frame(df: DataFrame, run_id: str, table_label: str, rule_ids, disposition: str) -> DataFrame:
    payload = F.col("_raw") if "_raw" in df.columns else F.to_json(F.struct(*[F.col(_q(c)) for c in data_columns(df)]))
    rids = rule_ids if not isinstance(rule_ids, str) else F.lit(rule_ids)
    return df.select(
        F.lit(run_id).alias("run_id"), F.current_timestamp().alias("checked_at"), F.lit(table_label).alias("source_table"),
        F.col("_row_id").alias("row_id"), rids.alias("rule_ids"), F.lit(disposition).alias("disposition"),
        payload.alias("payload"), F.lit("open").alias("status"), F.lit(None).cast("string").alias("fix_notes"))


def _empty(spark, ddl):
    return spark.createDataFrame([], ddl)


# --------------------------------------------------------------------------- main entry point
def apply_rules(spark: SparkSession, df: DataFrame, rules: list, run_id: str, layer: str, table_label: str,
                mode: str = "filter"):
    """Apply rules to df.

    mode='filter': remove deduped/quarantined rows. Returns (clean_df, log_df, results_df, stats_dict).
    mode='assert': never remove rows (use for gold); every violation is treated as a failure.
    """
    assert mode in ("filter", "assert")
    if "_row_id" not in df.columns:      # e.g. gold tables: give every row a stable id for ordering/logging
        df = df.withColumn("_row_id", F.monotonically_increasing_id())
    log_frames, results = [], []
    rows_in = df.count()
    deduped = 0

    def result(r, n, checked):
        results.append((run_id, None, layer, table_label, r["rule_id"], r["rule_type"],
                        "fail" if mode == "assert" else r["action"], int(n), int(checked), r["description"]))

    # pass 1: exact duplicates are auto-corrected BEFORE key uniqueness is judged
    for r in [x for x in rules if x["action"] == "dedupe"]:
        w = Window.partitionBy(*[F.col(_q(c)) for c in data_columns(df)]).orderBy("_row_id")
        d = df.withColumn("_dup", F.row_number().over(w) > 1)
        extra = d.where("_dup")
        n = extra.count()
        if n and mode == "filter":
            log_frames.append(_log_frame(extra, run_id, table_label, r["rule_id"], "auto_corrected"))
            df = d.where(~F.col("_dup")).drop("_dup")
            deduped += n
        result(r, n, rows_in)

    after_dedupe = df.count()
    flagged, flags = df, []
    for i, r in enumerate([x for x in rules if x["action"] != "dedupe"]):
        flag = f"_v{i}"
        flagged = _add_flag(spark, flagged, r, flag)
        flags.append((r, flag))

    counts = flagged.agg(*[F.sum(F.col(f).cast("int")).alias(f) for _, f in flags]).first() if flags else None
    for r, f in flags:
        result(r, (counts[f] or 0), after_dedupe)

    flag_cols = [f for _, f in flags]
    quar = [(r, f) for r, f in flags if r["action"] == "quarantine"]
    reasons = F.concat_ws(";", *[F.when(F.col(f), F.lit(r["rule_id"])) for r, f in quar]) if quar else F.lit("")
    flagged = flagged.withColumn("_reasons", reasons)

    rejected = standardized = warned = 0
    if mode == "filter":
        bad = flagged.where(F.col("_reasons") != "")
        rejected = bad.count()
        if rejected:
            log_frames.append(_log_frame(bad, run_id, table_label, F.col("_reasons"), "rejected"))
        for r, f in flags:
            if r["action"] == "standardize":
                s = flagged.where(F.col(f))
                standardized += s.count()
                log_frames.append(_log_frame(s, run_id, table_label, r["rule_id"], "auto_corrected"))
            elif r["action"] == "warn":
                wdf = flagged.where(F.col(f))
                warned += wdf.count()
                log_frames.append(_log_frame(wdf, run_id, table_label, r["rule_id"], "warning"))
        clean = flagged.where(F.col("_reasons") == "").drop(*flag_cols, "_reasons")
    else:
        clean = flagged.drop(*flag_cols, "_reasons")

    rows_out = clean.count()
    log_df = reduce(DataFrame.unionByName, log_frames) if log_frames else _empty(spark, QUARANTINE_DDL)
    results_df = (spark.createDataFrame(results, RESULTS_DDL.replace("checked_at TIMESTAMP", "checked_at STRING"))
                  .withColumn("checked_at", F.current_timestamp()) if results else _empty(spark, RESULTS_DDL))
    results_df = results_df.select(*[c.split()[0] for c in RESULTS_DDL.split(", ")])
    stats = dict(run_id=run_id, layer=layer, table_name=table_label, rows_in=rows_in, rows_deduped=deduped,
                 rows_standardized=standardized, rows_warned=warned, rows_rejected=rejected, rows_out=rows_out)
    return clean, log_df, results_df, stats


def failures(results_df: DataFrame) -> list:
    """Rules with action 'fail' that were violated."""
    return [r.asDict() for r in results_df.where("action = 'fail' AND violations > 0").collect()]


def raise_if_failed(results_df: DataFrame):
    bad = failures(results_df)
    if bad:
        raise DQFailure("Data quality failure: " + "; ".join(
            f"{b['table_name']} {b['rule_id']} ({b['violations']}): {b['description']}" for b in bad))
