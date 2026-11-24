-- Each cell returns exactly: check_name, severity ('error' blocks the pipeline, 'warn' only records), violations.
SELECT 'bronze rescued data: batting' AS check_name, 'warn' AS severity, COUNT(*) AS violations
FROM {{catalog}}.bronze.lahman_batting WHERE _rescued_data IS NOT NULL;

-- COMMAND ----------
SELECT 'silver.player: player_id unique' AS check_name, 'error' AS severity, COUNT(*) - COUNT(DISTINCT player_id) AS violations
FROM {{catalog}}.silver.player;

-- COMMAND ----------
SELECT 'silver.batting_season: key unique' AS check_name, 'error' AS severity,
       COUNT(*) - COUNT(DISTINCT player_id, year_id, stint) AS violations
FROM {{catalog}}.silver.batting_season;

-- COMMAND ----------
SELECT 'silver.pitching_season: key unique' AS check_name, 'error' AS severity,
       COUNT(*) - COUNT(DISTINCT player_id, year_id, stint) AS violations
FROM {{catalog}}.silver.pitching_season;

-- COMMAND ----------
SELECT 'silver.batting_season: hits <= at bats' AS check_name, 'error' AS severity, COUNT(*) AS violations
FROM {{catalog}}.silver.batting_season WHERE h > ab;

-- COMMAND ----------
SELECT 'silver.batting_season: home runs <= hits' AS check_name, 'error' AS severity, COUNT(*) AS violations
FROM {{catalog}}.silver.batting_season WHERE hr > h;

-- COMMAND ----------
SELECT 'silver.batting_season: orphan player_id' AS check_name, 'error' AS severity, COUNT(*) AS violations
FROM {{catalog}}.silver.batting_season b
LEFT ANTI JOIN {{catalog}}.silver.player p ON b.player_id = p.player_id;

-- COMMAND ----------
SELECT 'silver.pitching_season: orphan player_id' AS check_name, 'error' AS severity, COUNT(*) AS violations
FROM {{catalog}}.silver.pitching_season b
LEFT ANTI JOIN {{catalog}}.silver.player p ON b.player_id = p.player_id;

-- COMMAND ----------
SELECT 'silver.batting_season: orphan team-season' AS check_name, 'error' AS severity, COUNT(*) AS violations
FROM {{catalog}}.silver.batting_season b
LEFT ANTI JOIN {{catalog}}.silver.team_season t
  ON b.year_id = t.year_id AND b.league_id = t.league_id AND b.team_id = t.team_id;

-- COMMAND ----------
SELECT 'silver.salary: orphan team-season' AS check_name, 'error' AS severity, COUNT(*) AS violations
FROM {{catalog}}.silver.salary s
LEFT ANTI JOIN {{catalog}}.silver.team_season t
  ON s.year_id = t.year_id AND s.league_id = t.league_id AND s.team_id = t.team_id;

-- COMMAND ----------
-- Known upstream quirk (ties/forfeits): a handful of historic team rows have W + L > G. Recorded, not blocking.
SELECT 'silver.team_season: wins + losses <= games' AS check_name, 'warn' AS severity, COUNT(*) AS violations
FROM {{catalog}}.silver.team_season WHERE w + l > g;

-- COMMAND ----------
SELECT 'silver.salary: salary > 0' AS check_name, 'warn' AS severity, COUNT(*) AS violations
FROM {{catalog}}.silver.salary WHERE salary_usd <= 0;

-- COMMAND ----------
SELECT 'silver.batting_season: season in 1871..current year' AS check_name, 'error' AS severity, COUNT(*) AS violations
FROM {{catalog}}.silver.batting_season WHERE year_id < 1871 OR year_id > year(current_date());
