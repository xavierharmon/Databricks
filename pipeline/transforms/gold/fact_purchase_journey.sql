-- Be-backs = sessions that viewed the event within 30 days up to and including the purchase session, minus one. Same event, by session.
-- Visitors who left without buying are kept (purchased = 0): without them you cannot tell what drew buyers from what drew everyone.
WITH vis AS (SELECT * FROM gold_stage.tix_fact_event_visit WHERE person_id NOT IN ('BOT', 'UNRESOLVED')),
buys AS (SELECT o.order_id, cp.person_id, o.event_id, o.order_ts AS purchase_ts, o.sale_type
         FROM silver.tix_order_plan o JOIN gold_stage.tix_customer_person cp ON o.customer_id = cp.customer_id),
paid AS (SELECT order_id, CAST(avg(price) AS DECIMAL(12,2)) AS price_at_purchase FROM silver.tix_ticket_transaction WHERE order_id IS NOT NULL GROUP BY order_id),
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
       bj.price_at_first_visit, coalesce(paid.price_at_purchase, CAST(0 AS DECIMAL(12,2))) AS price_at_purchase, CAST(coalesce(drops.n, 0) AS INT) AS price_drops_since_first_visit
FROM buys b JOIN bj ON b.order_id = bj.order_id LEFT JOIN paid ON b.order_id = paid.order_id LEFT JOIN drops ON b.order_id = drops.order_id
UNION ALL
SELECT concat('NB-', nb.person_id, '-', nb.event_id), nb.person_id, nb.event_id, 0, 'none', 'none', CAST(nb.visits AS INT), CAST(greatest(nb.visits - 1, 0) AS INT),
       nb.first_visit_ts, CAST('1970-01-01 00:00:00' AS TIMESTAMP), CAST(0 AS DECIMAL(10,4)), nb.price_at_first_visit, CAST(0 AS DECIMAL(12,2)), 0
FROM nb
