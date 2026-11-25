WITH s AS (SELECT * FROM gold_stage.tix_fact_order_payment_status WHERE is_plan = 1),
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
