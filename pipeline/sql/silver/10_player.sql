-- SILVER player. Pattern for EVERY silver table:
--   1. rename to snake_case, 2. trim + blank->NULL, 3. cast with try_cast (bad values become NULL, not failures),
--   4. dedupe on the business key, 5. add constraints, 6. comment.
CREATE OR REPLACE TABLE {{catalog}}.silver.player
COMMENT 'One row per person in Lahman People (players and managers). Typed, trimmed, deduplicated on player_id.'
AS
SELECT
  trim(playerID)                                              AS player_id,
  try_cast(NULLIF(trim(birthYear), '')  AS INT)               AS birth_year,
  try_cast(concat_ws('-', NULLIF(trim(birthYear), ''), NULLIF(trim(birthMonth), ''), NULLIF(trim(birthDay), '')) AS DATE)  AS birth_date,
  NULLIF(trim(birthCountry), '')                              AS birth_country,
  NULLIF(trim(birthState), '')                                AS birth_state,
  NULLIF(trim(birthCity), '')                                 AS birth_city,
  try_cast(concat_ws('-', NULLIF(trim(deathYear), ''), NULLIF(trim(deathMonth), ''), NULLIF(trim(deathDay), '')) AS DATE)  AS death_date,
  NULLIF(trim(nameFirst), '')                                 AS name_first,
  NULLIF(trim(nameLast), '')                                  AS name_last,
  NULLIF(trim(nameGiven), '')                                 AS name_given,
  trim(concat_ws(' ', NULLIF(trim(nameFirst), ''), NULLIF(trim(nameLast), ''))) AS full_name,
  try_cast(NULLIF(trim(weight), '') AS INT)                   AS weight_lb,
  try_cast(NULLIF(trim(height), '') AS INT)                   AS height_in,
  NULLIF(trim(bats), '')                                      AS bats,      -- L / R / B
  NULLIF(trim(throws), '')                                    AS throws,    -- L / R
  try_cast(NULLIF(trim(debut), '') AS DATE)                        AS debut_date,
  try_cast(NULLIF(trim(finalGame), '') AS DATE)                    AS final_game_date,
  NULLIF(trim(retroID), '')                                   AS retro_id,
  NULLIF(trim(bbrefID), '')                                   AS bbref_id,
  current_timestamp()                                         AS _loaded_at
FROM (
  SELECT * FROM {{catalog}}.bronze.lahman_people
  WHERE NULLIF(trim(playerID), '') IS NOT NULL
  QUALIFY row_number() OVER (PARTITION BY trim(playerID) ORDER BY _ingested_at DESC) = 1
) AS b;

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.player ALTER COLUMN player_id SET NOT NULL;

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.player ADD CONSTRAINT player_pk PRIMARY KEY (player_id);
