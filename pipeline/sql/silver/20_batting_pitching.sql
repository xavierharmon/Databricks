-- Grain: one row per player per season per stint (a stint = a continuous spell with one team).
-- A traded player has 2+ rows in a season. NULL means "stat not recorded that year" (e.g. no SF before 1954),
-- which is different from 0, so we do NOT coalesce to zero in silver.
CREATE OR REPLACE TABLE {{catalog}}.silver.batting_season
COMMENT 'One row per player-season-stint of batting stats (Lahman Batting). NULL = not recorded, not zero.'
AS
SELECT
  trim(playerID)                             AS player_id,
  try_cast(trim(yearID) AS INT)              AS year_id,
  try_cast(trim(stint) AS INT)               AS stint,
  trim(teamID)                               AS team_id,
  trim(lgID)                                 AS league_id,
  try_cast(NULLIF(trim(G), '')    AS INT)    AS g,
  try_cast(NULLIF(trim(AB), '')   AS INT)    AS ab,
  try_cast(NULLIF(trim(R), '')    AS INT)    AS r,
  try_cast(NULLIF(trim(H), '')    AS INT)    AS h,
  try_cast(NULLIF(trim(`2B`), '') AS INT)    AS doubles,
  try_cast(NULLIF(trim(`3B`), '') AS INT)    AS triples,
  try_cast(NULLIF(trim(HR), '')   AS INT)    AS hr,
  try_cast(NULLIF(trim(RBI), '')  AS INT)    AS rbi,
  try_cast(NULLIF(trim(SB), '')   AS INT)    AS sb,
  try_cast(NULLIF(trim(CS), '')   AS INT)    AS cs,
  try_cast(NULLIF(trim(BB), '')   AS INT)    AS bb,
  try_cast(NULLIF(trim(SO), '')   AS INT)    AS so,
  try_cast(NULLIF(trim(IBB), '')  AS INT)    AS ibb,
  try_cast(NULLIF(trim(HBP), '')  AS INT)    AS hbp,
  try_cast(NULLIF(trim(SH), '')   AS INT)    AS sh,
  try_cast(NULLIF(trim(SF), '')   AS INT)    AS sf,
  try_cast(NULLIF(trim(GIDP), '') AS INT)    AS gidp,
  current_timestamp()                        AS _loaded_at
FROM (
  SELECT * FROM {{catalog}}.bronze.lahman_batting
  WHERE NULLIF(trim(playerID), '') IS NOT NULL AND try_cast(trim(yearID) AS INT) IS NOT NULL
  QUALIFY row_number() OVER (PARTITION BY trim(playerID), trim(yearID), trim(stint) ORDER BY _ingested_at DESC) = 1
) AS b;

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.batting_season ALTER COLUMN player_id SET NOT NULL;

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.batting_season ALTER COLUMN year_id SET NOT NULL;

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.batting_season ALTER COLUMN stint SET NOT NULL;

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.batting_season ADD CONSTRAINT batting_season_pk PRIMARY KEY (player_id, year_id, stint);

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.batting_season ADD CONSTRAINT batting_hits_le_ab CHECK (h <= ab);

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.silver.pitching_season
COMMENT 'One row per player-season-stint of pitching stats (Lahman Pitching). IPouts = outs recorded (innings x 3).'
AS
SELECT
  trim(playerID)                              AS player_id,
  try_cast(trim(yearID) AS INT)               AS year_id,
  try_cast(trim(stint) AS INT)                AS stint,
  trim(teamID)                                AS team_id,
  trim(lgID)                                  AS league_id,
  try_cast(NULLIF(trim(W), '')     AS INT)    AS w,
  try_cast(NULLIF(trim(L), '')     AS INT)    AS l,
  try_cast(NULLIF(trim(G), '')     AS INT)    AS g,
  try_cast(NULLIF(trim(GS), '')    AS INT)    AS gs,
  try_cast(NULLIF(trim(CG), '')    AS INT)    AS cg,
  try_cast(NULLIF(trim(SHO), '')   AS INT)    AS sho,
  try_cast(NULLIF(trim(SV), '')    AS INT)    AS sv,
  try_cast(NULLIF(trim(IPouts), '') AS INT)   AS ipouts,
  try_cast(NULLIF(trim(H), '')     AS INT)    AS h,
  try_cast(NULLIF(trim(ER), '')    AS INT)    AS er,
  try_cast(NULLIF(trim(HR), '')    AS INT)    AS hr,
  try_cast(NULLIF(trim(BB), '')    AS INT)    AS bb,
  try_cast(NULLIF(trim(SO), '')    AS INT)    AS so,
  try_cast(NULLIF(trim(IBB), '')   AS INT)    AS ibb,
  try_cast(NULLIF(trim(WP), '')    AS INT)    AS wp,
  try_cast(NULLIF(trim(HBP), '')   AS INT)    AS hbp,
  try_cast(NULLIF(trim(BK), '')    AS INT)    AS bk,
  try_cast(NULLIF(trim(BFP), '')   AS INT)    AS bfp,
  try_cast(NULLIF(trim(GF), '')    AS INT)    AS gf,
  try_cast(NULLIF(trim(R), '')     AS INT)    AS r,
  current_timestamp()                         AS _loaded_at
FROM (
  SELECT * FROM {{catalog}}.bronze.lahman_pitching
  WHERE NULLIF(trim(playerID), '') IS NOT NULL AND try_cast(trim(yearID) AS INT) IS NOT NULL
  QUALIFY row_number() OVER (PARTITION BY trim(playerID), trim(yearID), trim(stint) ORDER BY _ingested_at DESC) = 1
) AS b;

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.pitching_season ALTER COLUMN player_id SET NOT NULL;

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.pitching_season ALTER COLUMN year_id SET NOT NULL;

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.pitching_season ALTER COLUMN stint SET NOT NULL;

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.pitching_season ADD CONSTRAINT pitching_season_pk PRIMARY KEY (player_id, year_id, stint);
