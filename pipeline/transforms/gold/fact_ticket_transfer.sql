-- One row per ownership transfer. Fees are the sum of both fees. Cost basis is the price of the MOST RECENT earlier buyback of the same
-- ticket; profit exists only on company resales. expected_seller_id is the previous buyer: the audit compares it with the real seller.
WITH w AS (
  SELECT t.*,
    coalesce(lag(buyer_id) OVER (PARTITION BY ticket_id ORDER BY sale_ts, transaction_id), 'PRIMARY') AS expected_seller_id,
    last_value(CASE WHEN sale_type = 'buyback' THEN price END, true) OVER (PARTITION BY ticket_id ORDER BY sale_ts, transaction_id
                                                                         ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS last_buyback_price
  FROM silver.tix_ticket_transaction t)
SELECT w.transaction_id, w.ticket_id, w.event_id, e.event_category, w.sale_type, w.seller_id, w.buyer_id, w.expected_seller_id, w.sale_ts,
       w.price, w.buyer_fee, w.seller_fee, w.total_paid, w.seller_payout,
       CAST(w.buyer_fee + w.seller_fee AS DECIMAL(12,2)) AS fee_total,
       CAST(coalesce(w.last_buyback_price, 0) AS DECIMAL(12,2)) AS cost_basis,
       CAST(CASE WHEN w.sale_type = 'company_resale' THEN w.price - coalesce(w.last_buyback_price, 0) ELSE 0 END AS DECIMAL(12,2)) AS owned_inventory_profit,
       coalesce(w.order_id, 'none') AS order_id
FROM w JOIN silver.tix_event e ON w.event_id = e.event_id
