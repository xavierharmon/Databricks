-- Unity Catalog objects. Three-level namespace: catalog.schema.object
-- Catalog = environment boundary (mlb_dev / mlb_stg / mlb_prod). Schema = medallion layer.
CREATE CATALOG IF NOT EXISTS pipeline_mlb_dev
COMMENT 'MLB lakehouse learning project (Lahman data). Environment boundary.';

-- COMMAND ----------
CREATE SCHEMA IF NOT EXISTS pipeline_mlb_dev.bronze
COMMENT 'BRONZE: raw files as landed, all STRING, append-only, replayable.';

-- COMMAND ----------
CREATE SCHEMA IF NOT EXISTS pipeline_mlb_dev.silver
COMMENT 'SILVER: typed, cleaned, deduplicated, conformed entities. Source of truth for engineers.';

-- COMMAND ----------
CREATE SCHEMA IF NOT EXISTS pipeline_mlb_dev.gold
COMMENT 'GOLD: star schema (dim_* and fact_*) modeled for analytics.';

-- COMMAND ----------
CREATE SCHEMA IF NOT EXISTS pipeline_mlb_dev.semantic
COMMENT 'SEMANTIC: governed metric views. The only layer BI tools, Genie and analysts should query.';

-- COMMAND ----------
CREATE SCHEMA IF NOT EXISTS pipeline_mlb_dev.governance
COMMENT 'GOVERNANCE: data quality results, business glossary and other platform metadata.';

-- COMMAND ----------
CREATE VOLUME IF NOT EXISTS pipeline_mlb_dev.bronze.landing
COMMENT 'Landing zone for raw files + Auto Loader checkpoints.';

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS pipeline_mlb_dev.governance.dq_results (
  check_name  STRING,
  severity    STRING,
  violations  BIGINT,
  suite       STRING,
  checked_at  TIMESTAMP
)
COMMENT 'One row per data-quality check per run. Written by the quality gate task.'
