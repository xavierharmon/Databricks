"""Specs for the second slice of tix contracts: the ticket ledger, installment payments, price history, web analytics, identity
resolution, journeys and the ML feature tables. Read by tools/build_tix_contracts.py, which turns them into YAML and SQL files.

Design notes (the reasons, because this is where the project gets hard):
  * Money is DECIMAL(12,2). Floating point drifts when millions of fees are summed.
  * Each source file set is read WHOLE every run (bronze replaces, silver rebuilds). Re-delivered rows, conflicts and late files therefore
    all land in the same frame and the ordinary duplicate and uniqueness rules catch them. This is simple and correct; its cost grows
    with history, which is the point where incremental merge loads (a later step) become worth their complexity.
  * Rules that need another table's values (a sale after the event started) use the new `join_check` rule type. Rules that compare
    neighbouring rows or whole tables (the ownership chain) are SQL audits in transforms/audits/, run on the staged gold table.
  * Gold tables that read other gold tables read the STAGED copies (gold_stage.*), built earlier in the same run. Reading the
    published copy would silently use the previous run's data.
"""


def col(name, t="string", req=True, desc="", **extra):
    return dict(name=name, t=t, req=req, desc=desc or name.replace("_", " "), **extra)


def rule(rid, rtype, cols, expr, action, desc, dim="conformity", sev="error", owner="Data Engineering"):
    return dict(id=rid, rtype=rtype, cols=cols, expr=expr, action=action, desc=desc, dim=dim, sev=sev, owner=owner)


def dedupe(prefix, what):
    return [rule(f"{prefix}-001", "exact_duplicate", "*", "", "dedupe", f"Identical duplicate {what} rows are removed automatically (first copy kept)", "uniqueness", "warning")]


# ------------------------------------------------------------------------------------------------ analytics JSON parsing
SCHEMA = ("event_date STRING, event_timestamp BIGINT, event_name STRING, user_pseudo_id STRING, user_id STRING, "
          "event_params ARRAY<STRUCT<key: STRING, value: STRUCT<string_value: STRING, int_value: BIGINT>>>, "
          "items ARRAY<STRUCT<item_id: STRING, item_name: STRING, item_category: STRING, item_variant: STRING, price: STRING, quantity: BIGINT>>, "
          "ecommerce STRUCT<transaction_id: STRING>")
PARSE = f"from_json(raw_json, '{SCHEMA}')"


def top(field):
    return f"{PARSE}.{field}"


def param(key, field="string_value"):
    # try_element_at returns NULL (not an error) when the parameter is absent
    return f"try_element_at(filter({PARSE}.event_params, x -> x.key = '{key}'), 1).value.{field}"


def item(field):
    return f"try_element_at({PARSE}.items, 1).{field}"


GA_COLS = [
    col("event_ts", "timestamp", desc="Event time, UTC.", sourceColumn="raw_json",
        sourceExpression="date_format(timestamp_micros(from_json(raw_json, 'event_timestamp BIGINT').event_timestamp), 'yyyy-MM-dd HH:mm:ss')"),
    col("event_name", desc="page_view, view_item, login, begin_checkout, purchase ...", sourceColumn="raw_json", sourceExpression=top("event_name")),
    col("user_pseudo_id", desc="Anonymous browser id.", sourceColumn="raw_json", sourceExpression=top("user_pseudo_id")),
    col("user_id", req=False, desc="Customer id once the visitor logged in.", sourceColumn="raw_json", sourceExpression=top("user_id")),
    col("ga_session_id", "integer", desc="Session id.", sourceColumn="raw_json", sourceExpression=param("ga_session_id", "int_value")),
    col("page_event_id", req=False, desc="The event whose page was viewed.", sourceColumn="raw_json", sourceExpression=param("event_id")),
    col("client_fp", req=False, desc="First-party browser fingerprint hash.", sourceColumn="raw_json", sourceExpression=param("client_fp")),
    col("ip_hash", req=False, desc="Hash of the network address.", sourceColumn="raw_json", sourceExpression=param("ip_hash")),
    col("email_hash", req=False, desc="Email hash carried by a guest checkout.", sourceColumn="raw_json", sourceExpression=param("email_hash")),
    col("transaction_id", req=False, desc="Order id on a purchase event.", sourceColumn="raw_json", sourceExpression="" + PARSE + ".ecommerce.transaction_id"),
    col("item_price", "money", req=False, desc="Price shown on the page.", sourceColumn="raw_json", sourceExpression=item("price")),
    col("item_section", req=False, desc="Section shown on the page.", sourceColumn="raw_json", sourceExpression=item("item_variant")),
]

ENTITIES = {
    "ticket_transaction": dict(domain="sales", folder="ticket_transaction", purpose="Every ownership transfer of every ticket: the ledger.", prefix="TXN", pk=["transaction_id"], cols=[
        col("transaction_id", desc="Transaction id. Primary key.", primaryKey=1),
        col("ticket_id", desc="The ticket that changed hands.", relationships=[dict(to="tix_ticket_inventory.ticket_id", type="foreignKey")]),
        col("event_id", desc="Event.", relationships=[dict(to="tix_event.event_id", type="foreignKey")]),
        col("sale_type", desc="primary, resale, buyback or company_resale."),
        col("seller_id", desc="PRIMARY, COMPANY or a customer id."), col("buyer_id", desc="COMPANY or a customer id."),
        col("sale_ts", "timestamp", desc="Sale time, UTC."),
        col("price", "money", desc="Ticket price."), col("buyer_fee", "money", desc="Fee charged to the buyer."), col("seller_fee", "money", desc="Fee charged to the seller."),
        col("total_paid", "money", desc="Price plus buyer fee."), col("seller_payout", "money", desc="Price minus seller fee."),
        col("order_id", req=False, desc="Order the sale belongs to; blank for buybacks."), col("currency"), col("status"),
        col("buyer_customer_id", req=False, desc="The buyer when it is a customer, else blank.", sourceColumn="buyer_id",
            sourceExpression="CASE WHEN trim(buyer_id) IN ('COMPANY','PRIMARY') THEN NULL ELSE trim(buyer_id) END",
            relationships=[dict(to="tix_customer.customer_id", type="foreignKey")]),
        col("seller_customer_id", req=False, desc="The seller when it is a customer, else blank.", sourceColumn="seller_id",
            sourceExpression="CASE WHEN trim(seller_id) IN ('COMPANY','PRIMARY') THEN NULL ELSE trim(seller_id) END",
            relationships=[dict(to="tix_customer.customer_id", type="foreignKey")])],
        rules=[rule("TXN-002", "unique_strict", ["transaction_id"], "", "quarantine", "One transaction id means one transaction; conflicting copies are all rejected", "uniqueness"),
               rule("TXN-003", "not_null", ["transaction_id", "ticket_id", "event_id", "sale_type", "seller_id", "buyer_id", "sale_ts", "price", "buyer_fee", "seller_fee", "total_paid", "seller_payout", "currency", "status"], "", "quarantine", "Required ledger fields must be present", "completeness"),
               rule("TXN-004", "allowed_values", ["sale_type"], "'primary','resale','buyback','company_resale'", "quarantine", "sale_type must be one of the four sale types"),
               rule("TXN-005", "allowed_values", ["status"], "'completed'", "quarantine", "Only completed transfers are ledger rows"),
               rule("TXN-006", "range", ["price"], "0.01 AND 100000", "quarantine", "A price is positive and below 100,000"),
               rule("TXN-007", "custom", [], "total_paid = price + buyer_fee AND seller_payout = price - seller_fee", "quarantine", "Total paid is price plus buyer fee; payout is price minus seller fee", "consistency"),
               rule("TXN-008", "custom", [], "sale_type <> 'buyback' OR buyer_id = 'COMPANY'", "quarantine", "A buyback is bought by the company", "consistency"),
               rule("TXN-009", "custom", [], "sale_type <> 'company_resale' OR seller_id = 'COMPANY'", "quarantine", "A company resale is sold by the company", "consistency"),
               rule("TXN-010", "custom", [], "sale_type <> 'primary' OR seller_id = 'PRIMARY'", "quarantine", "A primary sale is sold by the box office", "consistency"),
               rule("TXN-011", "referential", ["ticket_id"], "silver.tix_ticket_inventory.ticket_id", "quarantine", "The ticket must exist", "consistency"),
               rule("TXN-012", "referential", ["event_id"], "silver.tix_event.event_id", "quarantine", "The event must exist", "consistency"),
               rule("TXN-013", "referential", ["buyer_customer_id"], "silver.tix_customer.customer_id", "quarantine", "A customer buyer must exist", "consistency"),
               rule("TXN-014", "referential", ["seller_customer_id"], "silver.tix_customer.customer_id", "quarantine", "A customer seller must exist", "consistency"),
               rule("TXN-015", "join_check", [], "silver.tix_event|event_id=event_id|sale_ts >= _p_start_ts", "quarantine", "Tickets cannot be sold after the event starts", "consistency")]),
    "order_plan": dict(domain="sales", folder="order_plan", purpose="One order and its payment plan.", prefix="ORD", pk=["order_id"], cols=[
        col("order_id", desc="Order id. Primary key.", primaryKey=1),
        col("customer_id", desc="Buyer.", relationships=[dict(to="tix_customer.customer_id", type="foreignKey")]),
        col("event_id", desc="Event.", relationships=[dict(to="tix_event.event_id", type="foreignKey")]),
        col("order_ts", "timestamp", desc="Order time, UTC."), col("total_amount", "money", desc="Order total."),
        col("num_payments", "integer", desc="Payments in the plan (1 means paid in full)."), col("plan_interval_days", "integer", desc="Days between payments."),
        col("sale_type", desc="primary, resale or company_resale.")],
        rules=[rule("ORD-002", "unique_strict", ["order_id"], "", "quarantine", "One order id means one order", "uniqueness"),
               rule("ORD-003", "not_null", ["order_id", "customer_id", "event_id", "order_ts", "total_amount", "num_payments", "plan_interval_days", "sale_type"], "", "quarantine", "Required order fields must be present", "completeness"),
               rule("ORD-004", "allowed_values", ["sale_type"], "'primary','resale','company_resale'", "quarantine", "Orders are primary, resale or company resale"),
               rule("ORD-005", "range", ["num_payments"], "1 AND 10", "quarantine", "A plan has 1 to 10 payments"),
               rule("ORD-006", "custom", [], "total_amount > 0", "quarantine", "An order total is positive"),
               rule("ORD-007", "referential", ["customer_id"], "silver.tix_customer.customer_id", "quarantine", "The buyer must exist", "consistency"),
               rule("ORD-008", "referential", ["event_id"], "silver.tix_event.event_id", "quarantine", "The event must exist", "consistency")]),
    "payment": dict(domain="sales", folder="payment", purpose="Every payment attempt, refund and chargeback against an order.", prefix="PAY", pk=["payment_id"], cols=[
        col("payment_id", desc="Payment attempt id. Primary key.", primaryKey=1),
        col("order_id", desc="Order paid.", relationships=[dict(to="tix_order_plan.order_id", type="foreignKey")]),
        col("payment_seq", "integer", desc="Which installment (1 to 10)."), col("attempt_no", "integer", desc="1 for the first try, 2 for a retry."),
        col("due_date", "date", desc="Installment due date."), col("event_ts", "timestamp", desc="When this attempt or refund happened, UTC."),
        col("amount", "money", desc="Amount."), col("status", desc="captured, failed, refunded or chargeback."),
        col("original_payment_id", req=False, desc="For a refund, the payment it reverses.")],
        rules=[rule("PAY-002", "unique_strict", ["payment_id"], "", "quarantine", "One payment id means one payment event", "uniqueness"),
               rule("PAY-003", "not_null", ["payment_id", "order_id", "payment_seq", "attempt_no", "due_date", "event_ts", "amount", "status"], "", "quarantine", "Required payment fields must be present", "completeness"),
               rule("PAY-004", "allowed_values", ["status"], "'captured','failed','refunded','chargeback'", "quarantine", "A known payment status"),
               rule("PAY-005", "range", ["payment_seq"], "1 AND 10", "quarantine", "Installments are numbered 1 to 10"),
               rule("PAY-006", "custom", [], "amount > 0", "quarantine", "An amount is positive; refunds are a status, not a negative number"),
               rule("PAY-007", "referential", ["order_id"], "silver.tix_order_plan.order_id", "quarantine", "The order must exist", "consistency")]),
    "price_change": dict(domain="sales", folder="price_change", purpose="Every price change for an event section: the price history.", prefix="PRC", pk=["price_change_id"], cols=[
        col("price_change_id", desc="Price change id. Primary key.", primaryKey=1),
        col("event_id", desc="Event.", relationships=[dict(to="tix_event.event_id", type="foreignKey")]),
        col("section"), col("effective_ts", "timestamp", desc="The price applies from this time, UTC."), col("price", "money", desc="Price from then on.")],
        rules=[rule("PRC-002", "unique_strict", ["price_change_id"], "", "quarantine", "One id means one change", "uniqueness"),
               rule("PRC-003", "not_null", ["price_change_id", "event_id", "section", "effective_ts", "price"], "", "quarantine", "Required fields must be present", "completeness"),
               rule("PRC-004", "range", ["price"], "1 AND 100000", "quarantine", "A plausible price"),
               rule("PRC-005", "referential", ["event_id"], "silver.tix_event.event_id", "quarantine", "The event must exist", "consistency")]),
    "ga_event": dict(domain="customer", folder="ga_event", format="jsonl", purpose="Web analytics events, parsed from the raw JSON.", prefix="GAE", pk=[],
        source_cols=[("raw_json", "One analytics event as a JSON document (raw text)")], cols=GA_COLS,
        rules=[rule("GAE-002", "not_null", ["event_ts", "event_name", "user_pseudo_id", "ga_session_id"], "", "quarantine", "Required analytics fields must be present", "completeness"),
               rule("GAE-003", "allowed_values", ["event_name"], "'page_view','view_item','add_to_cart','begin_checkout','purchase','login','sign_up','search'", "quarantine", "A known event name")]),
}

# ------------------------------------------------------------------------------------------------ gold
D = "DECIMAL(12,2)"
GOLD = {
    "customer_person": dict(domain="customer", silver="customer", also=[], prefix="CPE", pk=["customer_id"], recon="rowcount",
        purpose="Which person each customer record belongs to. Records sharing an email hash, or a phone hash and postal code, are one person.",
        cols=[("customer_id", "string"), ("person_id", "string")],
        sql="""-- Person clusters from customer records by repeated min-propagation. Two passes over each key reach every chain of up to four records,
-- far more than the generated data needs. A general connected-components job would loop (or use a graph library); this is the SQL-only version.
WITH c AS (SELECT customer_id, email_hash, phone_hash, postal_code FROM silver.tix_customer),
p1 AS (SELECT *, min(customer_id) OVER (PARTITION BY email_hash) AS a FROM c),
p2 AS (SELECT *, min(a) OVER (PARTITION BY phone_hash, postal_code) AS b FROM p1),
p3 AS (SELECT *, min(b) OVER (PARTITION BY email_hash) AS d FROM p2),
p4 AS (SELECT *, min(d) OVER (PARTITION BY phone_hash, postal_code) AS e FROM p3)
SELECT customer_id, e AS person_id FROM p4
"""),
    "fact_ticket_transfer": dict(domain="sales", silver="ticket_transaction", prefix="FTR", pk=["transaction_id"], recon="sum:price:price",
        deps=["tix.silver.ticket_transaction", "tix.silver.event"],
        purpose="The ledger with fees, cost basis and owned-inventory profit: one row per ownership transfer.",
        cols=[("transaction_id", "string"), ("ticket_id", "string"), ("event_id", "string"), ("event_category", "string"), ("sale_type", "string"),
              ("seller_id", "string"), ("buyer_id", "string"), ("expected_seller_id", "string"), ("sale_ts", "timestamp"), ("price", "money"),
              ("buyer_fee", "money"), ("seller_fee", "money"), ("total_paid", "money"), ("seller_payout", "money"), ("fee_total", "money"),
              ("cost_basis", "money"), ("owned_inventory_profit", "money"), ("order_id", "string")],
        audits=[("chain_breaks", "Each ticket's transfers form a chain: the seller is the previous buyer (the first seller is PRIMARY)")],
        audit_sql={"chain_breaks": "SELECT transaction_id FROM gold_stage.tix_fact_ticket_transfer WHERE seller_id <> expected_seller_id\n"},
        sql=f"""-- One row per ownership transfer. Fees are the sum of both fees. Cost basis is the price of the MOST RECENT earlier buyback of the same
-- ticket; profit exists only on company resales. expected_seller_id is the previous buyer: the audit compares it with the real seller.
WITH w AS (
  SELECT t.*,
    coalesce(lag(buyer_id) OVER (PARTITION BY ticket_id ORDER BY sale_ts, transaction_id), 'PRIMARY') AS expected_seller_id,
    last_value(CASE WHEN sale_type = 'buyback' THEN price END, true) OVER (PARTITION BY ticket_id ORDER BY sale_ts, transaction_id
                                                                         ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS last_buyback_price
  FROM silver.tix_ticket_transaction t)
SELECT w.transaction_id, w.ticket_id, w.event_id, e.event_category, w.sale_type, w.seller_id, w.buyer_id, w.expected_seller_id, w.sale_ts,
       w.price, w.buyer_fee, w.seller_fee, w.total_paid, w.seller_payout,
       CAST(w.buyer_fee + w.seller_fee AS {D}) AS fee_total,
       CAST(coalesce(w.last_buyback_price, 0) AS {D}) AS cost_basis,
       CAST(CASE WHEN w.sale_type = 'company_resale' THEN w.price - coalesce(w.last_buyback_price, 0) ELSE 0 END AS {D}) AS owned_inventory_profit,
       coalesce(w.order_id, 'none') AS order_id
FROM w JOIN silver.tix_event e ON w.event_id = e.event_id
"""),
    "fact_order_payment_status": dict(domain="sales", silver="order_plan", prefix="FOP", pk=["order_id"], recon="none",
        deps=["tix.silver.order_plan", "tix.silver.payment"],
        purpose="One row per primary order: money paid so far and whether it has closed (net captured payments reached 80 percent of the total).",
        cols=[("order_id", "string"), ("customer_id", "string"), ("event_id", "string"), ("order_ts", "timestamp"), ("sale_type", "string"),
              ("total_amount", "money"), ("num_payments", "integer"), ("is_plan", "integer"), ("net_paid", "money"), ("payments_captured", "integer"),
              ("first_closed_ts", "timestamp"), ("closed_ever", "integer"), ("closed_now", "integer")],
        audits=[("order_counts", "Every primary order in silver has exactly one status row")],
        audit_sql={"order_counts": """SELECT 1 FROM (SELECT (SELECT count(*) FROM gold_stage.tix_fact_order_payment_status) AS g,
                                     (SELECT count(*) FROM silver.tix_order_plan WHERE sale_type = 'primary') AS s) x WHERE g <> s
"""},
        sql=f"""-- Closed sale: net captured money (captures minus refunds and chargebacks) reached 80 percent of the order total, tested in exact
-- decimal arithmetic as net * 5 >= total * 4. first_closed_ts is the first payment event where that held and is never erased by a later
-- refund; closed_now says whether it still holds. A sentinel (1970-01-01) marks an order that never closed: gold has no NULLs.
WITH o AS (SELECT * FROM silver.tix_order_plan WHERE sale_type = 'primary'),
p AS (SELECT order_id, event_ts, payment_id,
        CASE status WHEN 'captured' THEN amount WHEN 'refunded' THEN -amount WHEN 'chargeback' THEN -amount ELSE CAST(0 AS {D}) END AS delta,
        CASE WHEN status = 'captured' THEN 1 ELSE 0 END AS is_captured
      FROM silver.tix_payment),
r AS (SELECT p.*, sum(delta) OVER (PARTITION BY order_id ORDER BY event_ts, payment_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_net FROM p),
a AS (SELECT r.order_id, sum(r.delta) AS net_paid, sum(r.is_captured) AS payments_captured,
             min(CASE WHEN r.running_net * 5 >= o.total_amount * 4 THEN r.event_ts END) AS first_closed_ts
      FROM r JOIN o ON r.order_id = o.order_id GROUP BY r.order_id)
SELECT o.order_id, o.customer_id, o.event_id, o.order_ts, o.sale_type, o.total_amount, o.num_payments,
       CASE WHEN o.num_payments > 1 THEN 1 ELSE 0 END AS is_plan,
       CAST(coalesce(a.net_paid, 0) AS {D}) AS net_paid, CAST(coalesce(a.payments_captured, 0) AS INT) AS payments_captured,
       coalesce(a.first_closed_ts, CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AS first_closed_ts,
       CASE WHEN a.first_closed_ts IS NULL THEN 0 ELSE 1 END AS closed_ever,
       CASE WHEN coalesce(a.net_paid, 0) * 5 >= o.total_amount * 4 THEN 1 ELSE 0 END AS closed_now
FROM o LEFT JOIN a ON o.order_id = a.order_id
"""),
    "fact_session_identity": dict(domain="customer", silver="ga_event", prefix="FSI", pk=["user_pseudo_id", "ga_session_id"], recon="none",
        deps=["tix.silver.ga_event", "tix.silver.customer", "tix.silver.order_plan", "tix.gold.customer_person"],
        purpose="Which person was behind each web session, with the method and how much it is trusted.",
        cols=[("user_pseudo_id", "string"), ("ga_session_id", "integer"), ("session_ts", "timestamp"), ("person_id", "string"), ("method", "string"), ("confidence_pct", "integer")],
        audits=[("session_counts", "Every distinct web session has exactly one identity row")],
        audit_sql={"session_counts": """SELECT 1 FROM (SELECT (SELECT count(*) FROM gold_stage.tix_fact_session_identity) AS g,
                                      (SELECT count(*) FROM (SELECT DISTINCT user_pseudo_id, ga_session_id FROM silver.tix_ga_event)) AS s) x WHERE g <> s
"""},
        sql="""-- Identity stitching, strongest evidence first. See the glossary page 'Identity stitching'.
--  tier 1: the session logged in.  tier 2: the session bought and the order belongs to a customer.  tier 3: a guest checkout carried an email hash.
-- A browser id linked to exactly one person is attributed to that person for ALL its sessions (retroactive). A browser id linked to two or
-- more people is a shared device: only sessions with their own evidence are attributed, the rest are left ambiguous on purpose.
-- A new browser id after a cookie reset is linked only through a fingerprint that belongs to exactly one person. Bots are labelled BOT.
WITH sess AS (
  SELECT user_pseudo_id, ga_session_id, min(event_ts) AS session_ts, max(client_fp) AS client_fp, max(user_id) AS login_customer,
         max(transaction_id) AS order_id, max(email_hash) AS guest_email_hash,
         sum(CASE WHEN event_name = 'page_view' THEN 1 ELSE 0 END) AS page_views,
         max(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS purchased,
         max(CASE WHEN user_id IS NOT NULL THEN 1 ELSE 0 END) AS has_login
  FROM silver.tix_ga_event GROUP BY user_pseudo_id, ga_session_id),
cp AS (SELECT customer_id, person_id FROM gold_stage.tix_customer_person),
email_person AS (SELECT DISTINCT c.email_hash, cp.person_id FROM silver.tix_customer c JOIN cp ON c.customer_id = cp.customer_id),
ev AS (
  SELECT s.user_pseudo_id, s.ga_session_id, cp.person_id, 1 AS tier FROM sess s JOIN cp ON s.login_customer = cp.customer_id
  UNION ALL
  SELECT s.user_pseudo_id, s.ga_session_id, cp.person_id, 2 AS tier FROM sess s JOIN silver.tix_order_plan o ON s.order_id = o.order_id JOIN cp ON o.customer_id = cp.customer_id
  UNION ALL
  SELECT s.user_pseudo_id, s.ga_session_id, ep.person_id, 3 AS tier FROM sess s JOIN email_person ep ON s.guest_email_hash = ep.email_hash),
direct AS (SELECT user_pseudo_id, ga_session_id, count(DISTINCT person_id) AS n, min(person_id) AS person_id, min(tier) AS tier FROM ev GROUP BY user_pseudo_id, ga_session_id),
pseudo AS (SELECT user_pseudo_id, count(DISTINCT person_id) AS n_people, min(person_id) AS only_person FROM ev GROUP BY user_pseudo_id),
fp AS (SELECT s.client_fp, count(DISTINCT p.only_person) AS n, min(p.only_person) AS person_id
       FROM sess s JOIN pseudo p ON s.user_pseudo_id = p.user_pseudo_id WHERE p.n_people = 1 GROUP BY s.client_fp),
bots AS (SELECT user_pseudo_id FROM sess GROUP BY user_pseudo_id HAVING sum(page_views) > 100 AND max(purchased) = 0 AND max(has_login) = 0)
SELECT s.user_pseudo_id, CAST(s.ga_session_id AS INT) AS ga_session_id, s.session_ts,
  CASE WHEN b.user_pseudo_id IS NOT NULL THEN 'BOT'
       WHEN p.n_people = 1 THEN p.only_person
       WHEN p.n_people > 1 THEN CASE WHEN d.n = 1 THEN d.person_id ELSE 'UNRESOLVED' END
       WHEN f.n = 1 THEN f.person_id
       ELSE 'UNRESOLVED' END AS person_id,
  CASE WHEN b.user_pseudo_id IS NOT NULL THEN 'bot'
       WHEN p.n_people = 1 THEN CASE WHEN d.n = 1 THEN concat('direct_t', CAST(d.tier AS STRING)) ELSE 'retroactive_same_browser_id' END
       WHEN p.n_people > 1 THEN CASE WHEN d.n = 1 THEN concat('shared_device_direct_t', CAST(d.tier AS STRING)) ELSE 'shared_device_ambiguous' END
       WHEN f.n = 1 THEN 'fingerprint_after_cookie_reset'
       ELSE 'unresolved' END AS method,
  CAST(CASE WHEN b.user_pseudo_id IS NOT NULL THEN 0
            WHEN p.n_people = 1 THEN CASE WHEN d.n = 1 THEN 95 ELSE 80 END
            WHEN p.n_people > 1 THEN CASE WHEN d.n = 1 THEN 90 ELSE 0 END
            WHEN f.n = 1 THEN 60 ELSE 0 END AS INT) AS confidence_pct
FROM sess s
LEFT JOIN bots b ON s.user_pseudo_id = b.user_pseudo_id
LEFT JOIN pseudo p ON s.user_pseudo_id = p.user_pseudo_id
LEFT JOIN direct d ON s.user_pseudo_id = d.user_pseudo_id AND s.ga_session_id = d.ga_session_id
LEFT JOIN fp f ON s.client_fp = f.client_fp
"""),
    "fact_event_visit": dict(domain="customer", silver="ga_event", prefix="FEV", pk=["user_pseudo_id", "ga_session_id", "event_id"], recon="none",
        deps=["tix.silver.ga_event", "tix.silver.price_change", "tix.gold.fact_session_identity"],
        purpose="One row per web session that viewed an event, with the person and the catalog price in force at that moment.",
        cols=[("person_id", "string"), ("event_id", "string"), ("user_pseudo_id", "string"), ("ga_session_id", "integer"), ("section", "string"),
              ("visit_ts", "timestamp"), ("price_at_visit", "money")],
        sql=f"""-- Price at visit: an as-of join. For each view, the latest price change for that event and section at or before the view time.
WITH v AS (
  SELECT user_pseudo_id, ga_session_id, page_event_id AS event_id, max(item_section) AS section, min(event_ts) AS visit_ts, max(item_price) AS shown_price
  FROM silver.tix_ga_event WHERE event_name = 'view_item' AND page_event_id IS NOT NULL
  GROUP BY user_pseudo_id, ga_session_id, page_event_id),
px AS (
  SELECT v.*, p.price AS catalog_price,
         row_number() OVER (PARTITION BY v.user_pseudo_id, v.ga_session_id, v.event_id ORDER BY p.effective_ts DESC) AS rn
  FROM v LEFT JOIN silver.tix_price_change p ON p.event_id = v.event_id AND p.section = v.section AND p.effective_ts <= v.visit_ts)
SELECT i.person_id, px.event_id, px.user_pseudo_id, CAST(px.ga_session_id AS INT) AS ga_session_id, coalesce(px.section, 'none') AS section, px.visit_ts,
       CAST(coalesce(px.catalog_price, px.shown_price, 0) AS {D}) AS price_at_visit
FROM px JOIN gold_stage.tix_fact_session_identity i ON px.user_pseudo_id = i.user_pseudo_id AND px.ga_session_id = i.ga_session_id
WHERE px.rn = 1
"""),
    "fact_purchase_journey": dict(domain="customer", silver="order_plan", prefix="FPJ", pk=["journey_id"], recon="none",
        deps=["tix.silver.order_plan", "tix.silver.price_change", "tix.silver.ticket_transaction", "tix.gold.customer_person", "tix.gold.fact_event_visit"],
        purpose="One row per purchase (with visits, be-backs and price movement) and one per person and event that never bought.",
        cols=[("journey_id", "string"), ("person_id", "string"), ("event_id", "string"), ("purchased", "integer"), ("order_id", "string"), ("sale_type", "string"),
              ("visits", "integer"), ("be_backs", "integer"), ("first_visit_ts", "timestamp"), ("purchase_ts", "timestamp"), ("days_to_purchase", "ratio"),
              ("price_at_first_visit", "money"), ("price_at_purchase", "money"), ("price_drops_since_first_visit", "integer")],
        sql=f"""-- Be-backs = sessions that viewed the event within 30 days up to and including the purchase session, minus one. Same event, by session.
-- Visitors who left without buying are kept (purchased = 0): without them you cannot tell what drew buyers from what drew everyone.
WITH vis AS (SELECT * FROM gold_stage.tix_fact_event_visit WHERE person_id NOT IN ('BOT', 'UNRESOLVED')),
buys AS (SELECT o.order_id, cp.person_id, o.event_id, o.order_ts AS purchase_ts, o.sale_type
         FROM silver.tix_order_plan o JOIN gold_stage.tix_customer_person cp ON o.customer_id = cp.customer_id),
paid AS (SELECT order_id, CAST(avg(price) AS {D}) AS price_at_purchase FROM silver.tix_ticket_transaction WHERE order_id IS NOT NULL GROUP BY order_id),
bj AS (
  SELECT b.order_id, count(*) AS visits, min(v.visit_ts) AS first_visit_ts, min_by(v.price_at_visit, v.visit_ts) AS price_at_first_visit, min_by(v.section, v.visit_ts) AS section
  FROM buys b JOIN vis v ON v.person_id = b.person_id AND v.event_id = b.event_id AND v.visit_ts <= b.purchase_ts AND v.visit_ts >= b.purchase_ts - INTERVAL 30 DAYS
  GROUP BY b.order_id),
drops AS (
  SELECT bj.order_id, count(pc.price_change_id) AS n
  FROM bj JOIN buys b ON bj.order_id = b.order_id
  LEFT JOIN silver.tix_price_change pc ON pc.event_id = b.event_id AND pc.section = bj.section AND pc.effective_ts > bj.first_visit_ts
       AND pc.effective_ts <= b.purchase_ts AND pc.price < bj.price_at_first_visit
  GROUP BY bj.order_id),
nb AS (
  SELECT v.person_id, v.event_id, count(*) AS visits, min(v.visit_ts) AS first_visit_ts, min_by(v.price_at_visit, v.visit_ts) AS price_at_first_visit
  FROM vis v LEFT ANTI JOIN buys b ON v.person_id = b.person_id AND v.event_id = b.event_id GROUP BY v.person_id, v.event_id)
SELECT b.order_id AS journey_id, b.person_id, b.event_id, 1 AS purchased, b.order_id, b.sale_type,
       CAST(bj.visits AS INT) AS visits, CAST(greatest(bj.visits - 1, 0) AS INT) AS be_backs, bj.first_visit_ts, b.purchase_ts,
       CAST((unix_timestamp(b.purchase_ts) - unix_timestamp(bj.first_visit_ts)) / 86400 AS DECIMAL(10,4)) AS days_to_purchase,
       bj.price_at_first_visit, coalesce(paid.price_at_purchase, CAST(0 AS {D})) AS price_at_purchase, CAST(coalesce(drops.n, 0) AS INT) AS price_drops_since_first_visit
FROM buys b JOIN bj ON b.order_id = bj.order_id LEFT JOIN paid ON b.order_id = paid.order_id LEFT JOIN drops ON b.order_id = drops.order_id
UNION ALL
SELECT concat('NB-', nb.person_id, '-', nb.event_id), nb.person_id, nb.event_id, 0, 'none', 'none', CAST(nb.visits AS INT), CAST(greatest(nb.visits - 1, 0) AS INT),
       nb.first_visit_ts, CAST('1970-01-01 00:00:00' AS TIMESTAMP), CAST(0 AS DECIMAL(10,4)), nb.price_at_first_visit, CAST(0 AS {D}), 0
FROM nb
"""),
    "customer_tag_affinity": dict(domain="customer", silver="event_tag", prefix="CTA", pk=["person_id", "tag"], recon="none",
        deps=["tix.silver.event_tag", "tix.silver.order_plan", "tix.gold.customer_person", "tix.gold.fact_event_visit"],
        purpose="How strongly each person leans to each event tag: views plus five times purchases.",
        cols=[("person_id", "string"), ("tag", "string"), ("views", "integer"), ("purchases", "integer"), ("affinity_score", "integer")],
        sql="""WITH v AS (SELECT person_id, event_id, count(*) AS n FROM gold_stage.tix_fact_event_visit WHERE person_id NOT IN ('BOT', 'UNRESOLVED') GROUP BY person_id, event_id),
b AS (SELECT cp.person_id, o.event_id, count(*) AS n FROM silver.tix_order_plan o JOIN gold_stage.tix_customer_person cp ON o.customer_id = cp.customer_id GROUP BY cp.person_id, o.event_id),
pe AS (SELECT coalesce(v.person_id, b.person_id) AS person_id, coalesce(v.event_id, b.event_id) AS event_id, coalesce(v.n, 0) AS views, coalesce(b.n, 0) AS purchases
       FROM v FULL OUTER JOIN b ON v.person_id = b.person_id AND v.event_id = b.event_id)
SELECT pe.person_id, t.tag, CAST(sum(pe.views) AS INT) AS views, CAST(sum(pe.purchases) AS INT) AS purchases, CAST(sum(pe.views) + 5 * sum(pe.purchases) AS INT) AS affinity_score
FROM pe JOIN silver.tix_event_tag t ON pe.event_id = t.event_id GROUP BY pe.person_id, t.tag
"""),
    "feature_purchase_propensity": dict(domain="customer", silver="order_plan", prefix="FPP", pk=["journey_key"], recon="none",
        deps=["tix.silver.order_plan", "tix.silver.event", "tix.silver.price_change", "tix.silver.customer", "tix.silver.ga_event", "tix.gold.customer_person", "tix.gold.fact_event_visit"],
        purpose="Training rows for 'will this visitor buy this event within 30 days of first looking at it?'. Every feature is known at as_of_ts (the first visit); the label looks forward.",
        cols=[("journey_key", "string"), ("person_id", "string"), ("event_id", "string"), ("as_of_ts", "timestamp"), ("event_category", "string"), ("sport", "string"),
              ("days_to_event", "ratio"), ("price_at_first_visit", "money"), ("price_level_ratio", "ratio"), ("prior_purchases", "integer"), ("prior_visits", "integer"),
              ("prior_visits_same_sport", "integer"), ("prior_purchases_same_sport", "integer"), ("tenure_days", "integer"),
              ("has_favorite", "integer"), ("favorite_matches_event", "integer"), ("label_bought_30d", "integer"), ("label_mature", "integer")],
        sql=f"""-- LEAK-FREE by construction: every feature uses only rows strictly before as_of_ts. label_mature = 1 only when 30 days of future exist in the
-- data, so recent rows (whose label is still unknown) are excluded from training instead of being counted as 'did not buy'.
WITH vis AS (SELECT * FROM gold_stage.tix_fact_event_visit WHERE person_id NOT IN ('BOT', 'UNRESOLVED')),
f AS (SELECT person_id, event_id, min(visit_ts) AS as_of_ts, min_by(price_at_visit, visit_ts) AS price_at_first_visit, min_by(section, visit_ts) AS section FROM vis GROUP BY person_id, event_id),
ev AS (SELECT event_id, event_category, coalesce(sport, 'none') AS sport, start_ts FROM silver.tix_event),
buys AS (SELECT cp.person_id, o.event_id, o.order_ts, ev.sport FROM silver.tix_order_plan o JOIN gold_stage.tix_customer_person cp ON o.customer_id = cp.customer_id JOIN ev ON o.event_id = ev.event_id),
vs AS (SELECT vis.person_id, vis.event_id, vis.visit_ts, ev.sport FROM vis JOIN ev ON vis.event_id = ev.event_id),
fp AS (SELECT event_id, section, min_by(price, effective_ts) AS first_price FROM silver.tix_price_change GROUP BY event_id, section),
pc AS (SELECT cp.person_id, min(c.created_ts) AS created_ts FROM silver.tix_customer c JOIN gold_stage.tix_customer_person cp ON c.customer_id = cp.customer_id GROUP BY cp.person_id),
h AS (SELECT max(event_ts) AS horizon FROM silver.tix_ga_event),
fav AS (SELECT cp.person_id, max(c.favorite_sport) AS favorite_sport FROM silver.tix_customer c JOIN gold_stage.tix_customer_person cp ON c.customer_id = cp.customer_id GROUP BY cp.person_id),
base AS (SELECT f.*, ev.event_category, ev.sport, ev.start_ts FROM f JOIN ev ON f.event_id = ev.event_id),
a1 AS (SELECT base.person_id, base.event_id, count(b.order_ts) AS n FROM base LEFT JOIN buys b ON b.person_id = base.person_id AND b.order_ts < base.as_of_ts GROUP BY base.person_id, base.event_id),
a2 AS (SELECT base.person_id, base.event_id, count(b.order_ts) AS n FROM base LEFT JOIN buys b ON b.person_id = base.person_id AND b.sport = base.sport AND b.order_ts < base.as_of_ts GROUP BY base.person_id, base.event_id),
a3 AS (SELECT base.person_id, base.event_id, count(v.visit_ts) AS n FROM base LEFT JOIN vs v ON v.person_id = base.person_id AND v.visit_ts < base.as_of_ts GROUP BY base.person_id, base.event_id),
a4 AS (SELECT base.person_id, base.event_id, count(v.visit_ts) AS n FROM base LEFT JOIN vs v ON v.person_id = base.person_id AND v.sport = base.sport AND v.visit_ts < base.as_of_ts GROUP BY base.person_id, base.event_id),
lb AS (SELECT base.person_id, base.event_id, count(b.order_ts) AS n FROM base LEFT JOIN buys b ON b.person_id = base.person_id AND b.event_id = base.event_id
         AND b.order_ts >= base.as_of_ts AND b.order_ts <= base.as_of_ts + INTERVAL 30 DAYS GROUP BY base.person_id, base.event_id)
SELECT concat(base.person_id, '|', base.event_id) AS journey_key, base.person_id, base.event_id, base.as_of_ts, base.event_category, base.sport,
       CAST((unix_timestamp(base.start_ts) - unix_timestamp(base.as_of_ts)) / 86400 AS DECIMAL(10,4)) AS days_to_event,
       base.price_at_first_visit,
       CAST(coalesce(base.price_at_first_visit / fp.first_price, 1) AS DECIMAL(10,4)) AS price_level_ratio,
       CAST(a1.n AS INT) AS prior_purchases, CAST(a3.n AS INT) AS prior_visits, CAST(a4.n AS INT) AS prior_visits_same_sport, CAST(a2.n AS INT) AS prior_purchases_same_sport,
       CAST(greatest(coalesce(datediff(base.as_of_ts, pc.created_ts), 0), 0) AS INT) AS tenure_days,
       CASE WHEN fav.favorite_sport IS NULL THEN 0 ELSE 1 END AS has_favorite,
       CASE WHEN fav.favorite_sport = CASE WHEN base.event_category = 'concert' THEN 'concert' ELSE base.sport END THEN 1 ELSE 0 END AS favorite_matches_event,
       CASE WHEN lb.n > 0 THEN 1 ELSE 0 END AS label_bought_30d,
       CASE WHEN base.as_of_ts + INTERVAL 30 DAYS <= h.horizon THEN 1 ELSE 0 END AS label_mature
FROM base
JOIN a1 ON base.person_id = a1.person_id AND base.event_id = a1.event_id JOIN a2 ON base.person_id = a2.person_id AND base.event_id = a2.event_id
JOIN a3 ON base.person_id = a3.person_id AND base.event_id = a3.event_id JOIN a4 ON base.person_id = a4.person_id AND base.event_id = a4.event_id
JOIN lb ON base.person_id = lb.person_id AND base.event_id = lb.event_id
LEFT JOIN fp ON base.event_id = fp.event_id AND base.section = fp.section LEFT JOIN pc ON base.person_id = pc.person_id
LEFT JOIN fav ON base.person_id = fav.person_id CROSS JOIN h
"""),
    "feature_plan_risk": dict(domain="sales", silver="order_plan", prefix="FPR", pk=["order_id"], recon="none",
        deps=["tix.silver.order_plan", "tix.silver.event", "tix.silver.payment", "tix.gold.customer_person", "tix.gold.fact_order_payment_status"],
        purpose="Training rows for 'will this installment plan reach 80 percent paid?'. Features are known when the order is placed; label_mature excludes plans that have not had time to finish.",
        cols=[("order_id", "string"), ("person_id", "string"), ("as_of_ts", "timestamp"), ("num_payments", "integer"), ("total_amount", "money"), ("event_category", "string"),
              ("sport", "string"), ("days_to_event", "ratio"), ("prior_orders", "integer"), ("prior_plan_orders", "integer"), ("label_reached_80", "integer"), ("label_mature", "integer")],
        sql="""WITH s AS (SELECT * FROM gold_stage.tix_fact_order_payment_status WHERE is_plan = 1),
cp AS (SELECT customer_id, person_id FROM gold_stage.tix_customer_person),
ev AS (SELECT event_id, event_category, coalesce(sport, 'none') AS sport, start_ts FROM silver.tix_event),
o AS (SELECT o.order_id, o.plan_interval_days, cp.person_id, o.order_ts, o.num_payments FROM silver.tix_order_plan o JOIN cp ON o.customer_id = cp.customer_id),
h AS (SELECT max(event_ts) AS horizon FROM silver.tix_payment),
base AS (SELECT s.order_id, cp.person_id, s.order_ts, s.num_payments, s.total_amount, ev.event_category, ev.sport, ev.start_ts, s.closed_ever, o.plan_interval_days
         FROM s JOIN cp ON s.customer_id = cp.customer_id JOIN ev ON s.event_id = ev.event_id JOIN o ON s.order_id = o.order_id),
p1 AS (SELECT base.order_id, count(x.order_id) AS n FROM base LEFT JOIN o x ON x.person_id = base.person_id AND x.order_ts < base.order_ts GROUP BY base.order_id),
p2 AS (SELECT base.order_id, count(x.order_id) AS n FROM base LEFT JOIN o x ON x.person_id = base.person_id AND x.order_ts < base.order_ts AND x.num_payments > 1 GROUP BY base.order_id)
SELECT base.order_id, base.person_id, base.order_ts AS as_of_ts, base.num_payments, base.total_amount, base.event_category, base.sport,
       CAST((unix_timestamp(base.start_ts) - unix_timestamp(base.order_ts)) / 86400 AS DECIMAL(10,4)) AS days_to_event,
       CAST(p1.n AS INT) AS prior_orders, CAST(p2.n AS INT) AS prior_plan_orders, base.closed_ever AS label_reached_80,
       CASE WHEN unix_timestamp(base.order_ts) + ((base.num_payments - 1) * base.plan_interval_days + 3) * 86400 <= unix_timestamp(h.horizon) THEN 1 ELSE 0 END AS label_mature
FROM base JOIN p1 ON base.order_id = p1.order_id JOIN p2 ON base.order_id = p2.order_id CROSS JOIN h
"""),
}
