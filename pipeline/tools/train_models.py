#!/usr/bin/env python3
"""Train and judge two models on the gold feature tables. Runs as a Databricks Job task after the pipeline (run_tool.py train ...) or on a laptop.

    run_tool.py --code-dir <code folder> train --catalog pipeline_mlb_dev [--model-dir /Volumes/.../models]

What it trains
  purchase propensity  : will this visitor buy this event within 30 days of first looking at it?   (gold.tix_feature_purchase_propensity)
  installment risk     : will this payment plan reach 80 percent paid?                                (gold.tix_feature_plan_risk)

How it stays honest (the part that matters more than the algorithm)
  * Time split, not random split: it trains on the earliest 70 percent of rows by as_of_ts and tests on the latest 30 percent, as a real
    deployment would (train on the past, predict the future). A random split would let tomorrow's rows teach yesterday's.
  * Immature rows are dropped: a row whose future window is not complete has an unknown label, not a 'no'.
  * Every model is judged against a simple rule. A model that cannot beat a one-line heuristic is not worth deploying.
  * Results go to governance.ml_model_runs (always) and MLflow (if the workspace has it), and scores go to the ml schema.
"""
import argparse
import json
import sys
from datetime import datetime

SPECS = {
    "purchase_propensity": dict(
        table="gold.tix_feature_purchase_propensity", key="journey_key", label="label_bought_30d", time="as_of_ts", mature="label_mature",
        numeric=["days_to_event", "price_at_first_visit", "price_level_ratio", "prior_purchases", "prior_visits", "prior_visits_same_sport",
                 "prior_purchases_same_sport", "tenure_days", "has_favorite", "favorite_matches_event"],
        categorical=["event_category", "sport"], baseline=lambda d: d["prior_visits_same_sport"] / (d["prior_visits"] + 1.0),
        baseline_name="share of earlier visits that were the same sport", scores_table="ml.purchase_propensity_scores", lift_floor=0.03),
    "installment_risk": dict(
        table="gold.tix_feature_plan_risk", key="order_id", label="label_reached_80", time="as_of_ts", mature="label_mature",
        numeric=["num_payments", "total_amount", "days_to_event", "prior_orders", "prior_plan_orders"],
        categorical=["event_category", "sport"], baseline=lambda d: -d["num_payments"].astype(float),
        baseline_name="fewer payments is safer", scores_table="ml.installment_risk_scores", lift_floor=-0.05),
}


def time_split(df, time_col, frac=0.7):
    """Earliest rows train, latest rows test. Rows sharing the cutoff time stay together so no time is on both sides."""
    d = df.sort_values(time_col).reset_index(drop=True)
    cut = d[time_col].iloc[int(len(d) * frac)]
    return d[d[time_col] < cut], d[d[time_col] >= cut]


def fit_evaluate(df, spec, seed=0):
    """df: a pandas DataFrame shaped like the feature table. Returns (metrics dict, fitted model or None)."""
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.metrics import average_precision_score, roc_auc_score
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder

    d = df[df[spec["mature"]] == 1]
    if len(d) < 50:
        return dict(skipped=f"only {len(d)} mature rows"), None
    train, test = time_split(d, spec["time"])
    y_tr, y_te = train[spec["label"]].astype(int), test[spec["label"]].astype(int)
    if y_tr.nunique() < 2 or y_te.nunique() < 2:
        return dict(skipped="train or test has a single class", n_train=len(train), n_test=len(test)), None
    feats = spec["numeric"] + spec["categorical"]
    small = len(train) < 400                              # with few rows a boosted tree memorises noise; a regularised linear model is steadier
    if small:
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import StandardScaler
        pre = ColumnTransformer([("cat", OneHotEncoder(handle_unknown="ignore"), spec["categorical"]), ("num", StandardScaler(), spec["numeric"])])
        model = Pipeline([("pre", pre), ("clf", LogisticRegression(max_iter=2000, C=0.5))])
    else:
        pre = ColumnTransformer([("cat", OneHotEncoder(handle_unknown="ignore"), spec["categorical"])], remainder="passthrough")
        model = Pipeline([("pre", pre), ("clf", GradientBoostingClassifier(n_estimators=150, max_depth=3, random_state=seed))])
    X_tr, X_te = train[feats].astype({c: float for c in spec["numeric"]}), test[feats].astype({c: float for c in spec["numeric"]})
    model.fit(X_tr, y_tr)
    p = model.predict_proba(X_te)[:, 1]
    base = spec["baseline"](test)
    top = max(1, len(test) // 10)
    order = p.argsort()[::-1][:top]
    return dict(auc=float(roc_auc_score(y_te, p)), average_precision=float(average_precision_score(y_te, p)),
                baseline_auc=float(roc_auc_score(y_te, base)), baseline=spec["baseline_name"], base_rate=float(y_te.mean()),
                top_decile_rate=float(y_te.iloc[order].mean()), n_train=int(len(train)), n_test=int(len(test)), model_kind="logistic" if small else "boosted trees",
                train_through=str(train[spec["time"]].max()), test_from=str(test[spec["time"]].min()), features=feats), model


def experiment_path(user, override=""):
    """A Job has no default MLflow experiment (a notebook does), so name one: --experiment, else /Users/<you>/tix_models."""
    return override or f"/Users/{user}/tix_models"


def _to_pandas(spark, table):
    from pyspark.sql import functions as F
    df = spark.table(table)
    df = df.select(*[F.col(c.name).cast("double").alias(c.name) if c.dataType.simpleString().startswith("decimal") else F.col(c.name) for c in df.schema.fields])
    return df.toPandas()


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--catalog", default="")
    ap.add_argument("--model-dir", default="")
    ap.add_argument("--experiment", default="", help="MLflow experiment path (default /Users/<current user>/tix_models)")
    ap.add_argument("--min-lift", type=float, default=None, help="override each model's required AUC lift over its baseline (the defaults are in SPECS: a weak baseline must be beaten; a baseline that encodes the true driver need only be matched)")
    a = ap.parse_args(argv)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    if a.catalog:
        spark.sql(f"USE CATALOG {a.catalog}")
    spark.sql("CREATE SCHEMA IF NOT EXISTS ml")
    spark.sql("CREATE TABLE IF NOT EXISTS governance.ml_model_runs (run_id STRING, model_name STRING, trained_at TIMESTAMP, metrics STRING)")
    run_id, bad = datetime.now().strftime("%Y%m%d-%H%M%S"), 0
    experiment = experiment_path(spark.sql("SELECT current_user()").first()[0], a.experiment)
    for name, spec in SPECS.items():
        df = _to_pandas(spark, spec["table"])
        metrics, model = fit_evaluate(df, spec)
        print(f"{name}: " + json.dumps({k: (round(v, 4) if isinstance(v, float) else v) for k, v in metrics.items() if k != "features"}))
        spark.createDataFrame([(run_id, name, datetime.now(), json.dumps(metrics))], "run_id STRING, model_name STRING, trained_at TIMESTAMP, metrics STRING") \
            .write.mode("append").saveAsTable("governance.ml_model_runs")
        if model is None:
            continue
        need = spec["lift_floor"] if a.min_lift is None else a.min_lift
        if metrics["auc"] - metrics["baseline_auc"] < need:
            bad += 1
            print(f"  {name}: AUC {metrics['auc']:.3f} is not within {need:+.2f} of its baseline {metrics['baseline_auc']:.3f}: not scoring")
            continue
        try:                                                    # MLflow is optional: its availability on Free Edition is unconfirmed
            import mlflow
            mlflow.set_experiment(experiment)
            with mlflow.start_run(run_name=f"{name}-{run_id}"):
                mlflow.log_metrics({k: v for k, v in metrics.items() if isinstance(v, float)})
                mlflow.log_param("features", ",".join(metrics["features"]))
                mlflow.sklearn.log_model(model, "model")
        except Exception as e:  # noqa: BLE001
            print(f"  (MLflow not used: {str(e)[:160]})")
        feats = spec["numeric"] + spec["categorical"]
        scored = df[[spec["key"]]].copy()
        scored["score"] = model.predict_proba(df[feats].astype({c: float for c in spec["numeric"]}))[:, 1]
        scored["model_run_id"], scored["scored_at"] = run_id, datetime.now()
        spark.createDataFrame(scored).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(spec["scores_table"])
        if a.model_dir:
            import os
            import pickle
            os.makedirs(a.model_dir, exist_ok=True)
            with open(os.path.join(a.model_dir, f"{name}_{run_id}.pkl"), "wb") as fh:
                pickle.dump(model, fh)
    return 1 if bad else 0


if __name__ == "__main__":
    rc = main()
    if rc:                    # exit only on failure (Databricks treats sys.exit(0) as a failed task)
        sys.exit(rc)