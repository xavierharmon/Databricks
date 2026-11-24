-- Column comments are what analysts and AI tools (Genie) actually read. Write them like documentation.
COMMENT ON COLUMN {{catalog}}.gold.fact_batting_season.player_key IS 'Surrogate key to gold.dim_player (xxhash64 of player_id).';

-- COMMAND ----------
COMMENT ON COLUMN {{catalog}}.gold.fact_batting_season.team_season_key IS 'Surrogate key to gold.dim_team_season (hash of season|league|team).';

-- COMMAND ----------
COMMENT ON COLUMN {{catalog}}.gold.fact_batting_season.stint IS 'Spell with one team within a season. A mid-season trade creates stint 2.';

-- COMMAND ----------
COMMENT ON COLUMN {{catalog}}.gold.fact_batting_season.ab IS 'At bats: plate appearances excluding walks, hit-by-pitch, sacrifices and catcher interference.';

-- COMMAND ----------
COMMENT ON COLUMN {{catalog}}.gold.fact_batting_season.total_bases IS 'Singles + 2x doubles + 3x triples + 4x home runs. Numerator of slugging percentage.';

-- COMMAND ----------
COMMENT ON COLUMN {{catalog}}.gold.fact_batting_season.plate_appearances IS 'AB + BB + HBP + SF + SH (derived). Denominator of the rate measures.';

-- COMMAND ----------
COMMENT ON COLUMN {{catalog}}.gold.fact_pitching_season.ipouts IS 'Outs recorded. Innings pitched = ipouts / 3. Stored as outs so it stays additive.';

-- COMMAND ----------
COMMENT ON COLUMN {{catalog}}.gold.fact_team_season.payroll_usd IS 'Sum of player salaries for the team-season. NULL (not zero) before 1985.';

-- COMMAND ----------
COMMENT ON COLUMN {{catalog}}.gold.dim_season.era IS 'Business-defined baseball era. Definition owned by Baseball Analytics; see governance.business_glossary.';

-- COMMAND ----------
COMMENT ON COLUMN {{catalog}}.gold.dim_player.birth_date IS 'Date of birth. Treated as personal data in this project to demonstrate classification tags and masking.';
