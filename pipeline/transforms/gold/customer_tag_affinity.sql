WITH v AS (SELECT person_id, event_id, count(*) AS n FROM gold_stage.tix_fact_event_visit WHERE person_id NOT IN ('BOT', 'UNRESOLVED') GROUP BY person_id, event_id),
b AS (SELECT cp.person_id, o.event_id, count(*) AS n FROM silver.tix_order_plan o JOIN gold_stage.tix_customer_person cp ON o.customer_id = cp.customer_id GROUP BY cp.person_id, o.event_id),
pe AS (SELECT coalesce(v.person_id, b.person_id) AS person_id, coalesce(v.event_id, b.event_id) AS event_id, coalesce(v.n, 0) AS views, coalesce(b.n, 0) AS purchases
       FROM v FULL OUTER JOIN b ON v.person_id = b.person_id AND v.event_id = b.event_id)
SELECT pe.person_id, t.tag, CAST(sum(pe.views) AS INT) AS views, CAST(sum(pe.purchases) AS INT) AS purchases, CAST(sum(pe.views) + 5 * sum(pe.purchases) AS INT) AS affinity_score
FROM pe JOIN silver.tix_event_tag t ON pe.event_id = t.event_id GROUP BY pe.person_id, t.tag
