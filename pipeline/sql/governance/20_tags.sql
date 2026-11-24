-- Tags make assets discoverable and drive policy (search by tag in Catalog Explorer, tag-based access policies).
-- Free-form tags here; promote 'domain' and 'classification' to GOVERNED tags in the UI to enforce allowed values.
ALTER TABLE {{catalog}}.silver.player SET TAGS ('layer' = 'silver', 'domain' = 'baseball_operations', 'owner_team' = 'data_engineering');

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.franchise SET TAGS ('layer' = 'silver', 'domain' = 'baseball_operations', 'owner_team' = 'data_engineering');

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.park SET TAGS ('layer' = 'silver', 'domain' = 'baseball_operations', 'owner_team' = 'data_engineering');

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.team_season SET TAGS ('layer' = 'silver', 'domain' = 'baseball_operations', 'owner_team' = 'data_engineering');

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.batting_season SET TAGS ('layer' = 'silver', 'domain' = 'baseball_operations', 'owner_team' = 'data_engineering');

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.pitching_season SET TAGS ('layer' = 'silver', 'domain' = 'baseball_operations', 'owner_team' = 'data_engineering');

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.salary SET TAGS ('layer' = 'silver', 'domain' = 'baseball_operations', 'owner_team' = 'data_engineering');

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.award SET TAGS ('layer' = 'silver', 'domain' = 'baseball_operations', 'owner_team' = 'data_engineering');

-- COMMAND ----------
ALTER TABLE {{catalog}}.gold.dim_player SET TAGS ('layer' = 'gold', 'domain' = 'baseball_operations', 'owner_team' = 'data_engineering');

-- COMMAND ----------
ALTER TABLE {{catalog}}.gold.dim_franchise SET TAGS ('layer' = 'gold', 'domain' = 'baseball_operations', 'owner_team' = 'data_engineering');

-- COMMAND ----------
ALTER TABLE {{catalog}}.gold.dim_season SET TAGS ('layer' = 'gold', 'domain' = 'baseball_operations', 'owner_team' = 'data_engineering');

-- COMMAND ----------
ALTER TABLE {{catalog}}.gold.dim_team_season SET TAGS ('layer' = 'gold', 'domain' = 'baseball_operations', 'owner_team' = 'data_engineering');

-- COMMAND ----------
ALTER TABLE {{catalog}}.gold.fact_batting_season SET TAGS ('layer' = 'gold', 'domain' = 'baseball_operations', 'owner_team' = 'data_engineering');

-- COMMAND ----------
ALTER TABLE {{catalog}}.gold.fact_pitching_season SET TAGS ('layer' = 'gold', 'domain' = 'baseball_operations', 'owner_team' = 'data_engineering');

-- COMMAND ----------
ALTER TABLE {{catalog}}.gold.fact_salary SET TAGS ('layer' = 'gold', 'domain' = 'baseball_operations', 'owner_team' = 'data_engineering');

-- COMMAND ----------
ALTER TABLE {{catalog}}.gold.fact_player_award SET TAGS ('layer' = 'gold', 'domain' = 'baseball_operations', 'owner_team' = 'data_engineering');

-- COMMAND ----------
ALTER TABLE {{catalog}}.gold.fact_team_season SET TAGS ('layer' = 'gold', 'domain' = 'baseball_operations', 'owner_team' = 'data_engineering');

-- COMMAND ----------
ALTER VIEW {{catalog}}.semantic.mv_batting SET TAGS ('layer' = 'semantic', 'domain' = 'baseball_operations', 'owner_team' = 'data_engineering');

-- COMMAND ----------
ALTER VIEW {{catalog}}.semantic.mv_pitching SET TAGS ('layer' = 'semantic', 'domain' = 'baseball_operations', 'owner_team' = 'data_engineering');

-- COMMAND ----------
ALTER VIEW {{catalog}}.semantic.mv_team_season SET TAGS ('layer' = 'semantic', 'domain' = 'baseball_operations', 'owner_team' = 'data_engineering');

-- COMMAND ----------
ALTER TABLE {{catalog}}.gold.dim_player ALTER COLUMN birth_date SET TAGS ('classification' = 'personal');

-- COMMAND ----------
ALTER TABLE {{catalog}}.gold.dim_player ALTER COLUMN birth_city SET TAGS ('classification' = 'personal');

-- COMMAND ----------
ALTER TABLE {{catalog}}.gold.dim_player ALTER COLUMN birth_state SET TAGS ('classification' = 'personal');
