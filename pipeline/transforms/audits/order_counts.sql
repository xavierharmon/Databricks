SELECT 1 FROM (SELECT (SELECT count(*) FROM gold_stage.tix_fact_order_payment_status) AS g,
                                     (SELECT count(*) FROM silver.tix_order_plan WHERE sale_type = 'primary') AS s) x WHERE g <> s
