CREATE OR REPLACE TABLE {{catalog}}.silver.franchise
COMMENT 'One row per MLB franchise lineage (Lahman TeamsFranchises).'
AS
SELECT
  trim(franchID)                              AS franchise_id,
  trim(franchName)                            AS franchise_name,
  CASE trim(active) WHEN 'Y' THEN TRUE WHEN 'N' THEN FALSE END AS is_active,
  NULLIF(trim(NAassoc), '')                   AS national_assoc_id,
  current_timestamp()                         AS _loaded_at
FROM (
  SELECT * FROM {{catalog}}.bronze.lahman_teams_franchises
  WHERE NULLIF(trim(franchID), '') IS NOT NULL
  QUALIFY row_number() OVER (PARTITION BY trim(franchID) ORDER BY _ingested_at DESC) = 1
) AS b;

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.franchise ALTER COLUMN franchise_id SET NOT NULL;

-- COMMAND ----------
ALTER TABLE {{catalog}}.silver.franchise ADD CONSTRAINT franchise_pk PRIMARY KEY (franchise_id);

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.silver.park
COMMENT 'One row per ballpark (Lahman Parks).'
AS
SELECT
  trim(park_key)                AS park_key,
  trim(park_name)               AS park_name,
  NULLIF(trim(park_alias), '')  AS park_alias,
  NULLIF(trim(city), '')        AS city,
  NULLIF(trim(state), '')       AS state,
  NULLIF(trim(country), '')     AS country,
  current_timestamp()           AS _loaded_at
FROM (
  SELECT * FROM {{catalog}}.bronze.lahman_parks
  WHERE NULLIF(trim(park_key), '') IS NOT NULL
  QUALIFY row_number() OVER (PARTITION BY trim(park_key) ORDER BY _ingested_at DESC) = 1
) AS b;
