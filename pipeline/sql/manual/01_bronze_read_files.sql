-- MANUAL MODE bronze (no notebook, no Auto Loader). Run AFTER uploading the CSVs to the Volume folders.
-- Same result as notebook 02: every column STRING, plus _source_file / _ingested_at / _rescued_data.
-- NOT idempotent: re-running rebuilds the table from the files currently in the folder (fine for learning).
CREATE OR REPLACE TABLE {{catalog}}.bronze.lahman_people
COMMENT 'BRONZE (manual): raw Lahman People CSV, all STRING.'
AS
SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM read_files('/Volumes/{{catalog}}/bronze/landing/lahman/people/', format => 'csv', header => true, inferColumnTypes => false);

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.bronze.lahman_teams
COMMENT 'BRONZE (manual): raw Lahman Teams CSV, all STRING.'
AS
SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM read_files('/Volumes/{{catalog}}/bronze/landing/lahman/teams/', format => 'csv', header => true, inferColumnTypes => false);

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.bronze.lahman_teams_franchises
COMMENT 'BRONZE (manual): raw Lahman TeamsFranchises CSV, all STRING.'
AS
SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM read_files('/Volumes/{{catalog}}/bronze/landing/lahman/teams_franchises/', format => 'csv', header => true, inferColumnTypes => false);

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.bronze.lahman_batting
COMMENT 'BRONZE (manual): raw Lahman Batting CSV, all STRING.'
AS
SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM read_files('/Volumes/{{catalog}}/bronze/landing/lahman/batting/', format => 'csv', header => true, inferColumnTypes => false);

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.bronze.lahman_pitching
COMMENT 'BRONZE (manual): raw Lahman Pitching CSV, all STRING.'
AS
SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM read_files('/Volumes/{{catalog}}/bronze/landing/lahman/pitching/', format => 'csv', header => true, inferColumnTypes => false);

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.bronze.lahman_salaries
COMMENT 'BRONZE (manual): raw Lahman Salaries CSV, all STRING.'
AS
SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM read_files('/Volumes/{{catalog}}/bronze/landing/lahman/salaries/', format => 'csv', header => true, inferColumnTypes => false);

-- COMMAND ----------
CREATE OR REPLACE TABLE {{catalog}}.bronze.lahman_awards_players
COMMENT 'BRONZE (manual): raw Lahman AwardsPlayers CSV, all STRING.'
AS
SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM read_files('/Volumes/{{catalog}}/bronze/landing/lahman/awards_players/', format => 'csv', header => true, inferColumnTypes => false);

-- COMMAND ----------
-- Parks has dotted column names (park.key); rename them so SQL can use them.
CREATE OR REPLACE TABLE {{catalog}}.bronze.lahman_parks
COMMENT 'BRONZE (manual): raw Lahman Parks CSV, all STRING.'
AS
SELECT `park.key` AS park_key, `park.name` AS park_name, `park.alias` AS park_alias, city, state, country,
       _rescued_data, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM read_files('/Volumes/{{catalog}}/bronze/landing/lahman/parks/', format => 'csv', header => true, inferColumnTypes => false);
