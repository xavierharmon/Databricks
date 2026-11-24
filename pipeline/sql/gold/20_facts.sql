-- GOLD facts. Store ADDITIVE components only (hits, at-bats, ipouts). Ratios (AVG, ERA, WHIP) are NOT stored:
-- they are defined once as measures in the metric views, because an average of averages is wrong.
CREATE OR REPLACE TABLE {{catalog}}.gold.fact_batting_season
COMMENT 'Grain: one row per player per season per stint. Additive batting components. Ratios live in semantic.mv_batting.'
CLUSTER BY (season)
AS
SELECT
  xxhash64(player_id)                                                           AS player_key,
  xxhash64(concat_ws('|', cast(year_id AS STRING), league_id, team_id))         AS team_season_key,
  year_id                                                                       AS season,
  stint,
  g, ab, r, h, doubles, triples, hr, rbi, sb, cs, bb, so, ibb, hbp, sh, sf, gidp,
  coalesce(h, 0) - coalesce(doubles, 0) - coalesce(triples, 0) - coalesce(hr, 0) AS singles,
  coalesce(h, 0) + coalesce(doubles, 0) + 2 * coalesce(triples, 0) + 3 * coalesce(hr, 0) AS total_bases,
  coalesce(ab, 0) + coalesce(bb, 0) + coalesce(hbp, 0) + coalesce(sf, 0) + coalesce(sh, 0) AS plate_appearances
FROM {{catalog}}.silver.batting_season;

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.gold.fact_pitching_season
COMMENT 'Grain: one row per player per season per stint. Additive pitching components (ipouts = outs). Ratios live in semantic.mv_pitching.'
CLUSTER BY (season)
AS
SELECT
  xxhash64(player_id)                                                           AS player_key,
  xxhash64(concat_ws('|', cast(year_id AS STRING), league_id, team_id))         AS team_season_key,
  year_id                                                                       AS season,
  stint,
  w, l, g, gs, cg, sho, sv, ipouts, h, er, hr, bb, so, ibb, wp, hbp, bk, bfp, gf, r
FROM {{catalog}}.silver.pitching_season;

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.gold.fact_salary
COMMENT 'Grain: one row per player per team per season. Salary in USD (1985 onward).'
CLUSTER BY (season)
AS
SELECT
  xxhash64(player_id)                                                           AS player_key,
  xxhash64(concat_ws('|', cast(year_id AS STRING), league_id, team_id))         AS team_season_key,
  year_id                                                                       AS season,
  salary_usd
FROM {{catalog}}.silver.salary;

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.gold.fact_player_award
COMMENT 'Grain: one row per player award per season. Count rows to count awards.'
CLUSTER BY (season)
AS
SELECT
  xxhash64(player_id)  AS player_key,
  year_id              AS season,
  award_id,
  league_id,
  is_tie,
  1                    AS award_count
FROM {{catalog}}.silver.award;

-- COMMAND ----------
-- Grain: one row per team per season. Payroll is rolled up from fact_salary so team measures can use it.
-- payroll is NULL (not 0) before 1985 when no salary data exists; measures must respect that.
CREATE OR REPLACE TABLE {{catalog}}.gold.fact_team_season
COMMENT 'Grain: one row per team per season. Standings, team totals, attendance and payroll (payroll NULL before 1985).'
CLUSTER BY (season)
AS
WITH payroll AS (
  SELECT team_season_key, SUM(salary_usd) AS payroll_usd
  FROM {{catalog}}.gold.fact_salary
  GROUP BY team_season_key
)
SELECT
  d.team_season_key,
  t.year_id AS season,
  t.g, t.g_home, t.w, t.l, t.r, t.ra, t.h, t.hr, t.bb, t.so, t.sb, t.errors, t.double_plays,
  t.attendance,
  p.payroll_usd
FROM {{catalog}}.silver.team_season t
JOIN {{catalog}}.gold.dim_team_season d
  ON d.season = t.year_id AND d.league_id = t.league_id AND d.team_id = t.team_id
LEFT JOIN payroll p ON p.team_season_key = d.team_season_key;
