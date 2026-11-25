-- Price at visit: an as-of join. For each view, the latest price change for that event and section at or before the view time.
WITH v AS (
  SELECT user_pseudo_id, ga_session_id, page_event_id AS event_id, max(item_section) AS section, min(event_ts) AS visit_ts, max(item_price) AS shown_price
  FROM silver.tix_ga_event WHERE event_name = 'view_item' AND page_event_id IS NOT NULL
  GROUP BY user_pseudo_id, ga_session_id, page_event_id),
px AS (
  SELECT v.*, p.price AS catalog_price,
         row_number() OVER (PARTITION BY v.user_pseudo_id, v.ga_session_id, v.event_id ORDER BY p.effective_ts DESC) AS rn
  FROM v LEFT JOIN silver.tix_price_change p ON p.event_id = v.event_id AND p.section = v.section AND p.effective_ts <= v.visit_ts)
SELECT i.person_id, px.event_id, px.user_pseudo_id, CAST(px.ga_session_id AS INT) AS ga_session_id, coalesce(px.section, 'none') AS section, px.visit_ts,
       CAST(coalesce(px.catalog_price, px.shown_price, 0) AS DECIMAL(12,2)) AS price_at_visit
FROM px JOIN gold_stage.tix_fact_session_identity i ON px.user_pseudo_id = i.user_pseudo_id AND px.ga_session_id = i.ga_session_id
WHERE px.rn = 1
