CREATE OR REPLACE TABLE {{catalog}}.silver.salary
COMMENT 'One row per player per team per season salary in USD (Lahman Salaries, 1985+).'
AS
SELECT
  trim(playerID)                       AS player_id,
  try_cast(trim(yearID) AS INT)        AS year_id,
  trim(lgID)                           AS league_id,
  trim(teamID)                         AS team_id,
  try_cast(NULLIF(trim(salary), '') AS BIGINT) AS salary_usd,
  current_timestamp()                  AS _loaded_at
FROM (
  SELECT * FROM {{catalog}}.bronze.lahman_salaries
  WHERE NULLIF(trim(playerID), '') IS NOT NULL
  QUALIFY row_number() OVER (PARTITION BY trim(playerID), trim(yearID), trim(teamID), trim(lgID) ORDER BY _ingested_at DESC) = 1
) AS b;

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.silver.award
COMMENT 'One row per player award won per season (Lahman AwardsPlayers). Ties are flagged.'
AS
SELECT
  trim(playerID)                        AS player_id,
  trim(awardID)                         AS award_id,
  try_cast(trim(yearID) AS INT)         AS year_id,
  NULLIF(trim(lgID), '')                AS league_id,
  coalesce(trim(tie) = 'Y', FALSE)      AS is_tie,
  NULLIF(trim(notes), '')               AS notes,
  current_timestamp()                   AS _loaded_at
FROM (
  SELECT * FROM {{catalog}}.bronze.lahman_awards_players
  WHERE NULLIF(trim(playerID), '') IS NOT NULL
  QUALIFY row_number() OVER (PARTITION BY trim(playerID), trim(awardID), trim(yearID), trim(lgID), coalesce(trim(notes), '') ORDER BY _ingested_at DESC) = 1
) AS b;
