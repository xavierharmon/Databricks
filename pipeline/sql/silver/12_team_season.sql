-- Grain: one row per team per season (year_id, league_id, team_id).
-- Note the backticks: bronze column names `2B` and `3B` start with a digit.
CREATE OR REPLACE TABLE {{catalog}}.silver.team_season
COMMENT 'One row per team per season (Lahman Teams): standings, team batting/pitching/fielding totals, attendance.'
AS
SELECT
  try_cast(trim(yearID) AS INT)            AS year_id,
  trim(lgID)                               AS league_id,
  trim(teamID)                             AS team_id,
  trim(franchID)                           AS franchise_id,
  NULLIF(trim(divID), '')                  AS division_id,
  try_cast(NULLIF(trim(Rank), '') AS INT)  AS league_rank,
  try_cast(NULLIF(trim(G), '') AS INT)     AS g,
  try_cast(NULLIF(trim(Ghome), '') AS INT) AS g_home,
  try_cast(NULLIF(trim(W), '') AS INT)     AS w,
  try_cast(NULLIF(trim(L), '') AS INT)     AS l,
  CASE trim(DivWin) WHEN 'Y' THEN TRUE WHEN 'N' THEN FALSE END AS won_division,
  CASE trim(WCWin)  WHEN 'Y' THEN TRUE WHEN 'N' THEN FALSE END AS won_wild_card,
  CASE trim(LgWin)  WHEN 'Y' THEN TRUE WHEN 'N' THEN FALSE END AS won_league,
  CASE trim(WSWin)  WHEN 'Y' THEN TRUE WHEN 'N' THEN FALSE END AS won_world_series,
  try_cast(NULLIF(trim(R), '') AS INT)     AS r,
  try_cast(NULLIF(trim(AB), '') AS INT)    AS ab,
  try_cast(NULLIF(trim(H), '') AS INT)     AS h,
  try_cast(NULLIF(trim(`2B`), '') AS INT)  AS doubles,
  try_cast(NULLIF(trim(`3B`), '') AS INT)  AS triples,
  try_cast(NULLIF(trim(HR), '') AS INT)    AS hr,
  try_cast(NULLIF(trim(BB), '') AS INT)    AS bb,
  try_cast(NULLIF(trim(SO), '') AS INT)    AS so,
  try_cast(NULLIF(trim(SB), '') AS INT)    AS sb,
  try_cast(NULLIF(trim(CS), '') AS INT)    AS cs,
  try_cast(NULLIF(trim(HBP), '') AS INT)   AS hbp,
  try_cast(NULLIF(trim(SF), '') AS INT)    AS sf,
  try_cast(NULLIF(trim(RA), '') AS INT)    AS ra,
  try_cast(NULLIF(trim(ER), '') AS INT)    AS er,
  try_cast(NULLIF(trim(IPouts), '') AS INT) AS ipouts,
  try_cast(NULLIF(trim(HA), '') AS INT)    AS ha,
  try_cast(NULLIF(trim(HRA), '') AS INT)   AS hra,
  try_cast(NULLIF(trim(BBA), '') AS INT)   AS bba,
  try_cast(NULLIF(trim(SOA), '') AS INT)   AS soa,
  try_cast(NULLIF(trim(E), '') AS INT)     AS errors,
  try_cast(NULLIF(trim(DP), '') AS INT)    AS double_plays,
  NULLIF(trim(name), '')                   AS team_name,
  NULLIF(trim(park), '')                   AS park_name,
  try_cast(NULLIF(trim(attendance), '') AS BIGINT) AS attendance,
  NULLIF(trim(teamIDBR), '')               AS team_id_bbref,
  current_timestamp()                      AS _loaded_at
FROM (
  SELECT * FROM {{catalog}}.bronze.lahman_teams
  WHERE try_cast(trim(yearID) AS INT) IS NOT NULL
  QUALIFY row_number() OVER (PARTITION BY trim(yearID), trim(lgID), trim(teamID) ORDER BY _ingested_at DESC) = 1
) AS b;

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.team_season ALTER COLUMN year_id SET NOT NULL;

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.team_season ALTER COLUMN league_id SET NOT NULL;

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.team_season ALTER COLUMN team_id SET NOT NULL;

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.team_season ADD CONSTRAINT team_season_pk PRIMARY KEY (year_id, league_id, team_id);
