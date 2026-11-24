-- Design choice: analysts get the SEMANTIC schema only. Gold/silver/bronze stay with engineers.
-- To also open gold (e.g. for data scientists) add: GRANT USE SCHEMA, SELECT ON SCHEMA {{catalog}}.gold TO `{{analyst_group}}`
-- Always grant to GROUPS, never individuals.
GRANT USE CATALOG ON CATALOG {{catalog}} TO `{{analyst_group}}`;

-- COMMAND ----------
GRANT USE SCHEMA ON SCHEMA {{catalog}}.semantic TO `{{analyst_group}}`;

-- COMMAND ----------
GRANT SELECT ON SCHEMA {{catalog}}.semantic TO `{{analyst_group}}`;
