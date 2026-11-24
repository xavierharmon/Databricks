-- Needs ASSIGN on the governed tag system.certification_status. Run only after the quality gates pass (the job order guarantees it).
-- Syntax per current Databricks docs; if your workspace rejects SET TAG ON VIEW, certify metric views in Catalog Explorer.
SET TAG ON TABLE {{catalog}}.gold.dim_player `system.certification_status` = `certified`;

-- COMMAND ----------
SET TAG ON TABLE {{catalog}}.gold.dim_franchise `system.certification_status` = `certified`;

-- COMMAND ----------
SET TAG ON TABLE {{catalog}}.gold.dim_season `system.certification_status` = `certified`;

-- COMMAND ----------
SET TAG ON TABLE {{catalog}}.gold.dim_team_season `system.certification_status` = `certified`;

-- COMMAND ----------
SET TAG ON TABLE {{catalog}}.gold.fact_batting_season `system.certification_status` = `certified`;

-- COMMAND ----------
SET TAG ON TABLE {{catalog}}.gold.fact_pitching_season `system.certification_status` = `certified`;

-- COMMAND ----------
SET TAG ON TABLE {{catalog}}.gold.fact_salary `system.certification_status` = `certified`;

-- COMMAND ----------
SET TAG ON TABLE {{catalog}}.gold.fact_player_award `system.certification_status` = `certified`;

-- COMMAND ----------
SET TAG ON TABLE {{catalog}}.gold.fact_team_season `system.certification_status` = `certified`;

-- COMMAND ----------
SET TAG ON VIEW {{catalog}}.semantic.mv_batting `system.certification_status` = `certified`;

-- COMMAND ----------
SET TAG ON VIEW {{catalog}}.semantic.mv_pitching `system.certification_status` = `certified`;

-- COMMAND ----------
SET TAG ON VIEW {{catalog}}.semantic.mv_team_season `system.certification_status` = `certified`;
