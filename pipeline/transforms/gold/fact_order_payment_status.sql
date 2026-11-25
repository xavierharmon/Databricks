-- Closed sale: net captured money (captures minus refunds and chargebacks) reached 80 percent of the order total, tested in exact
-- decimal arithmetic as net * 5 >= total * 4. first_closed_ts is the first payment event where that held and is never erased by a later
-- refund; closed_now says whether it still holds. A sentinel (1970-01-01) marks an order that never closed: gold has no NULLs.
WITH o AS (SELECT * FROM silver.tix_order_plan WHERE sale_type = 'primary'),
p AS (SELECT order_id, event_ts, payment_id,
        CASE status WHEN 'captured' THEN amount WHEN 'refunded' THEN -amount WHEN 'chargeback' THEN -amount ELSE CAST(0 AS DECIMAL(12,2)) END AS delta,
        CASE WHEN status = 'captured' THEN 1 ELSE 0 END AS is_captured
      FROM silver.tix_payment),
r AS (SELECT p.*, sum(delta) OVER (PARTITION BY order_id ORDER BY event_ts, payment_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_net FROM p),
a AS (SELECT r.order_id, sum(r.delta) AS net_paid, sum(r.is_captured) AS payments_captured,
             min(CASE WHEN r.running_net * 5 >= o.total_amount * 4 THEN r.event_ts END) AS first_closed_ts
      FROM r JOIN o ON r.order_id = o.order_id GROUP BY r.order_id)
SELECT o.order_id, o.customer_id, o.event_id, o.order_ts, o.sale_type, o.total_amount, o.num_payments,
       CASE WHEN o.num_payments > 1 THEN 1 ELSE 0 END AS is_plan,
       CAST(coalesce(a.net_paid, 0) AS DECIMAL(12,2)) AS net_paid, CAST(coalesce(a.payments_captured, 0) AS INT) AS payments_captured,
       coalesce(a.first_closed_ts, CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AS first_closed_ts,
       CASE WHEN a.first_closed_ts IS NULL THEN 0 ELSE 1 END AS closed_ever,
       CASE WHEN coalesce(a.net_paid, 0) * 5 >= o.total_amount * 4 THEN 1 ELSE 0 END AS closed_now
FROM o LEFT JOIN a ON o.order_id = a.order_id
