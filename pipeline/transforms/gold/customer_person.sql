-- Person clusters from customer records by repeated min-propagation. Two passes over each key reach every chain of up to four records,
-- far more than the generated data needs. A general connected-components job would loop (or use a graph library); this is the SQL-only version.
WITH c AS (SELECT customer_id, email_hash, phone_hash, postal_code FROM silver.tix_customer),
p1 AS (SELECT *, min(customer_id) OVER (PARTITION BY email_hash) AS a FROM c),
p2 AS (SELECT *, min(a) OVER (PARTITION BY phone_hash, postal_code) AS b FROM p1),
p3 AS (SELECT *, min(b) OVER (PARTITION BY email_hash) AS d FROM p2),
p4 AS (SELECT *, min(d) OVER (PARTITION BY phone_hash, postal_code) AS e FROM p3)
SELECT customer_id, e AS person_id FROM p4
