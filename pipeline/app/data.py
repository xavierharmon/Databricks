"""Data access for the ticket analytics app. All SQL lives here, so it can be read, tested and reviewed without running the app.

The app is read-only and queries gold tables and the ml score tables through a SQL warehouse. Why gold and not silver: gold is audited
before publication (write, audit, publish), so the app never shows a table that failed its checks. (Metric views would be the next step up:
one governed definition per measure. They are not built for tix yet.)

Connection: inside a Databricks App, DATABRICKS_HOST and the app's credentials are provided by the platform, and the SQL warehouse is
passed as DATABRICKS_WAREHOUSE_ID. That is how I understand Apps to work; I have not run this app in a workspace. Locally, set the same
variables with a personal access token in DATABRICKS_TOKEN.
"""
import os

CATALOG = os.environ.get("TIX_CATALOG", "pipeline_mlb_dev")

QUERIES = {
    "earnings_by_sale_type": f"""
        SELECT sale_type, count(*) AS transfers, sum(price) AS gross_sales, sum(fee_total) AS fee_revenue, sum(owned_inventory_profit) AS owned_inventory_profit
        FROM {CATALOG}.gold.tix_fact_ticket_transfer GROUP BY sale_type ORDER BY gross_sales DESC""",
    "closed_sales_by_plan_length": f"""
        SELECT num_payments, count(*) AS orders, sum(closed_now) AS closed_now,
               round(sum(closed_now) / count(*), 3) AS closed_rate
        FROM {CATALOG}.gold.tix_fact_order_payment_status WHERE is_plan = 1 GROUP BY num_payments ORDER BY num_payments""",
    "be_backs": f"""
        SELECT be_backs, count(*) AS purchases
        FROM {CATALOG}.gold.tix_fact_purchase_journey WHERE purchased = 1 GROUP BY be_backs ORDER BY be_backs""",
    "conversion_by_price_drop": f"""
        SELECT CASE WHEN price_drops_since_first_visit > 0 THEN 'saw a price drop' ELSE 'no price drop' END AS exposure,
               count(*) AS journeys, round(avg(purchased), 3) AS conversion_rate
        FROM {CATALOG}.gold.tix_fact_purchase_journey GROUP BY 1 ORDER BY 1""",
    "identity_methods": f"""
        SELECT method, count(*) AS sessions FROM {CATALOG}.gold.tix_fact_session_identity GROUP BY method ORDER BY sessions DESC""",
    "top_propensity": f"""
        SELECT s.journey_key, round(s.score, 3) AS score, f.event_category, f.sport, f.days_to_event
        FROM {CATALOG}.ml.purchase_propensity_scores s JOIN {CATALOG}.gold.tix_feature_purchase_propensity f ON s.journey_key = f.journey_key
        WHERE f.label_mature = 0 ORDER BY s.score DESC LIMIT 25""",
    "model_runs": f"""SELECT model_name, trained_at, metrics FROM {CATALOG}.governance.ml_model_runs ORDER BY trained_at DESC LIMIT 10""",
}


def run_query(name, sql_runner):
    """sql_runner(sql) -> pandas DataFrame. Injected, so tests use a fake and the app uses the warehouse."""
    if name not in QUERIES:
        raise KeyError(f"unknown query {name!r}; known: {', '.join(QUERIES)}")
    return sql_runner(QUERIES[name])


def warehouse_runner():
    """A sql_runner backed by a Databricks SQL warehouse (needs databricks-sql-connector and databricks-sdk)."""
    from databricks import sql
    from databricks.sdk.core import Config
    cfg = Config()
    host, wid = cfg.host.replace("https://", ""), os.environ["DATABRICKS_WAREHOUSE_ID"]

    def run(statement):
        with sql.connect(server_hostname=host, http_path=f"/sql/1.0/warehouses/{wid}", credentials_provider=lambda: cfg.authenticate) as conn:
            with conn.cursor() as cur:
                cur.execute(statement)
                return cur.fetchall_arrow().to_pandas()
    return run


def kpis(earnings):
    """Headline numbers from the earnings_by_sale_type result: fee revenue, owned-inventory profit and total earnings."""
    fee = float(earnings["fee_revenue"].astype(float).sum()) if len(earnings) else 0.0
    prof = float(earnings["owned_inventory_profit"].astype(float).sum()) if len(earnings) else 0.0
    return {"fee_revenue": fee, "owned_inventory_profit": prof, "total_earnings": fee + prof}
