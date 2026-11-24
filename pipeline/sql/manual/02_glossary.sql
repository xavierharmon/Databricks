-- MANUAL MODE glossary. Upload data/glossary/glossary_terms.csv to the Volume folder
-- /Volumes/{{catalog}}/bronze/landing/glossary/ first.
CREATE OR REPLACE TABLE {{catalog}}.governance.business_glossary
COMMENT 'Business glossary managed as code (data/glossary/glossary_terms.csv).'
AS
SELECT term, category, definition, formula, owner, related_asset, status, current_timestamp() AS loaded_at
FROM read_files('/Volumes/{{catalog}}/bronze/landing/glossary/', format => 'csv', header => true, multiLine => true, inferColumnTypes => false);
