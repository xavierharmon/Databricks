SELECT 1 FROM (SELECT (SELECT count(*) FROM gold_stage.tix_fact_session_identity) AS g,
                                      (SELECT count(*) FROM (SELECT DISTINCT user_pseudo_id, ga_session_id FROM silver.tix_ga_event)) AS s) x WHERE g <> s
