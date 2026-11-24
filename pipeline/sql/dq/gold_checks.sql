-- Gold is where silent fan-out (duplicate dimension rows) wrecks numbers. Test grain and referential integrity.
SELECT 'gold.dim_player: player_key unique' AS check_name, 'error' AS severity, COUNT(*) - COUNT(DISTINCT player_key) AS violations
FROM {{catalog}}.gold.dim_player;

-- COMMAND ----------
SELECT 'gold.dim_team_season: team_season_key unique' AS check_name, 'error' AS severity, COUNT(*) - COUNT(DISTINCT team_season_key) AS violations
FROM {{catalog}}.gold.dim_team_season;

-- COMMAND ----------
SELECT 'gold.dim_season: season unique' AS check_name, 'error' AS severity, COUNT(*) - COUNT(DISTINCT season) AS violations
FROM {{catalog}}.gold.dim_season;

-- COMMAND ----------
SELECT 'gold.fact_batting_season: orphan player_key' AS check_name, 'error' AS severity, COUNT(*) AS violations
FROM {{catalog}}.gold.fact_batting_season f LEFT ANTI JOIN {{catalog}}.gold.dim_player d ON f.player_key = d.player_key;

-- COMMAND ----------
SELECT 'gold.fact_batting_season: orphan team_season_key' AS check_name, 'error' AS severity, COUNT(*) AS violations
FROM {{catalog}}.gold.fact_batting_season f LEFT ANTI JOIN {{catalog}}.gold.dim_team_season d ON f.team_season_key = d.team_season_key;

-- COMMAND ----------
SELECT 'gold.fact_pitching_season: orphan team_season_key' AS check_name, 'error' AS severity, COUNT(*) AS violations
FROM {{catalog}}.gold.fact_pitching_season f LEFT ANTI JOIN {{catalog}}.gold.dim_team_season d ON f.team_season_key = d.team_season_key;

-- COMMAND ----------
SELECT 'gold.fact_salary: orphan player_key' AS check_name, 'error' AS severity, COUNT(*) AS violations
FROM {{catalog}}.gold.fact_salary f LEFT ANTI JOIN {{catalog}}.gold.dim_player d ON f.player_key = d.player_key;

-- COMMAND ----------
SELECT 'gold.fact_team_season: one row per team season' AS check_name, 'error' AS severity,
       COUNT(*) - COUNT(DISTINCT team_season_key) AS violations
FROM {{catalog}}.gold.fact_team_season;

-- COMMAND ----------
-- Reconciliation: gold must not lose or invent rows relative to silver.
SELECT 'reconcile: fact_batting_season rows = silver.batting_season rows' AS check_name, 'error' AS severity,
       abs((SELECT COUNT(*) FROM {{catalog}}.gold.fact_batting_season) - (SELECT COUNT(*) FROM {{catalog}}.silver.batting_season)) AS violations;

-- COMMAND ----------
SELECT 'reconcile: total home runs gold = silver' AS check_name, 'error' AS severity,
       abs((SELECT COALESCE(SUM(hr),0) FROM {{catalog}}.gold.fact_batting_season) - (SELECT COALESCE(SUM(hr),0) FROM {{catalog}}.silver.batting_season)) AS violations;

-- COMMAND ----------
SELECT 'reconcile: fact_salary total = silver.salary total' AS check_name, 'error' AS severity,
       abs((SELECT COALESCE(SUM(salary_usd),0) FROM {{catalog}}.gold.fact_salary) - (SELECT COALESCE(SUM(salary_usd),0) FROM {{catalog}}.silver.salary)) AS violations;

-- COMMAND ----------
SELECT 'reconcile: fact_team_season payroll = fact_salary total' AS check_name, 'error' AS severity,
       abs((SELECT COALESCE(SUM(payroll_usd),0) FROM {{catalog}}.gold.fact_team_season) - (SELECT COALESCE(SUM(salary_usd),0) FROM {{catalog}}.gold.fact_salary)) AS violations;
