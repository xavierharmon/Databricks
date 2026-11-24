-- GOLD dimensions. Surrogate keys are deterministic hashes (xxhash64) of the business key:
-- re-running the pipeline always produces the same keys, so facts and dims never drift apart.
CREATE OR REPLACE TABLE {{catalog}}.gold.dim_player
COMMENT 'Player dimension: one row per person. Descriptive attributes only, no measures.'
CLUSTER BY (player_key)
AS
SELECT
  xxhash64(player_id)                                  AS player_key,
  player_id,
  full_name,
  name_first,
  name_last,
  birth_date,
  birth_year,
  birth_country,
  birth_state,
  birth_city,
  bats,
  throws,
  CASE bats WHEN 'L' THEN 'Left' WHEN 'R' THEN 'Right' WHEN 'B' THEN 'Switch' ELSE 'Unknown' END AS batting_hand,
  height_in,
  weight_lb,
  debut_date,
  year(debut_date)                                     AS debut_year,
  final_game_date,
  death_date,
  death_date IS NULL                                   AS is_living
FROM {{catalog}}.silver.player;

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.gold.dim_franchise
COMMENT 'Franchise lineage dimension (e.g. the Brooklyn Dodgers and Los Angeles Dodgers share franchise LAN).'
AS
SELECT
  xxhash64(franchise_id)  AS franchise_key,
  franchise_id,
  franchise_name,
  is_active
FROM {{catalog}}.silver.franchise;

-- COMMAND ----------
-- Era is a BUSINESS-DEFINED classification. This CASE is the single place it is defined (see glossary term "Era").
CREATE OR REPLACE TABLE {{catalog}}.gold.dim_season
COMMENT 'Season dimension: one row per MLB season with decade and baseball era classification.'
AS
SELECT
  year_id                                   AS season,
  (year_id DIV 10) * 10                     AS decade,
  CASE
    WHEN year_id <= 1900 THEN 'Pioneer (to 1900)'
    WHEN year_id <= 1919 THEN 'Dead-ball (1901-1919)'
    WHEN year_id <= 1941 THEN 'Live-ball (1920-1941)'
    WHEN year_id <= 1960 THEN 'Integration (1942-1960)'
    WHEN year_id <= 1976 THEN 'Expansion (1961-1976)'
    WHEN year_id <= 1993 THEN 'Free agency (1977-1993)'
    WHEN year_id <= 2005 THEN 'Steroid (1994-2005)'
    ELSE 'Analytics (2006+)'
  END                                       AS era,
  CASE
    WHEN year_id <= 1900 THEN 1 WHEN year_id <= 1919 THEN 2 WHEN year_id <= 1941 THEN 3
    WHEN year_id <= 1960 THEN 4 WHEN year_id <= 1976 THEN 5 WHEN year_id <= 1993 THEN 6
    WHEN year_id <= 2005 THEN 7 ELSE 8
  END                                       AS era_sort
FROM (SELECT DISTINCT year_id FROM {{catalog}}.silver.team_season);

-- COMMAND ----------
-- Grain: one row per team per season. Denormalized (franchise name folded in) so the star stays one hop wide.
CREATE OR REPLACE TABLE {{catalog}}.gold.dim_team_season
COMMENT 'Team-season dimension: one row per team per season, with league, division, franchise, ballpark and postseason outcomes.'
CLUSTER BY (season)
AS
SELECT
  xxhash64(concat_ws('|', cast(t.year_id AS STRING), t.league_id, t.team_id)) AS team_season_key,
  t.year_id                    AS season,
  t.league_id,
  t.team_id,
  t.team_name,
  t.franchise_id,
  f.franchise_name,
  t.division_id,
  t.league_rank,
  t.park_name,
  coalesce(t.won_division, FALSE)     AS won_division,
  coalesce(t.won_wild_card, FALSE)    AS won_wild_card,
  coalesce(t.won_league, FALSE)       AS won_league,
  coalesce(t.won_world_series, FALSE) AS won_world_series
FROM {{catalog}}.silver.team_season t
LEFT JOIN {{catalog}}.silver.franchise f ON t.franchise_id = f.franchise_id;
