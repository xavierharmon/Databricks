-- LEAK-FREE by construction: every feature uses only rows strictly before as_of_ts. label_mature = 1 only when 30 days of future exist in the
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
