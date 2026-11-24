# Manual mode: run everything by hand in the Databricks UI

No CLI, no bundle, no GitHub. Everything runs in the SQL editor plus one upload step.

## 0. Prepare the SQL (on your computer)
```bash
python tools/render_sql.py --catalog <your_catalog> --skip-create-catalog
```
Writes `rendered/<your_catalog>/` with your catalog name filled in (the editor flags `{{catalog}}` as a legacy parameter).
Use `--skip-create-catalog` when the catalog already exists. Find yours with `SHOW CATALOGS;`.
No Python? Open each `sql/` file in a text editor and find-and-replace `{{catalog}}` with the catalog name.

## 1. Create schemas, Volume, results table
SQL editor, pick your SQL warehouse, run `setup/00_objects.sql`. Check:
```sql
SHOW SCHEMAS IN <your_catalog>;
SHOW VOLUMES IN <your_catalog>.bronze;
```

## 2. Upload the raw files (this is "landing")
Catalog > `<your_catalog>` > `bronze` > `landing` > Upload to this volume. One folder per table:

| File (from `data/lahman/`) | Volume folder |
| --- | --- |
| People.csv | `lahman/people/` |
| Teams.csv | `lahman/teams/` |
| TeamsFranchises.csv | `lahman/teams_franchises/` |
| Batting.csv | `lahman/batting/` |
| Pitching.csv | `lahman/pitching/` |
| Salaries.csv | `lahman/salaries/` |
| Parks.csv | `lahman/parks/` |
| AwardsPlayers.csv | `lahman/awards_players/` |
| glossary_terms.csv (from `data/glossary/`) | `glossary/` |

Keep the original file names. One file per folder.

## 3. Bronze
Run `manual/01_bronze_read_files.sql`. This replaces notebooks 01 and 02 (it uses `read_files` instead of Auto Loader).
```sql
SELECT 'batting' t, COUNT(*) n FROM <c>.bronze.lahman_batting
UNION ALL SELECT 'people', COUNT(*) FROM <c>.bronze.lahman_people;   -- expect ~108,790 and ~20,093
DESCRIBE TABLE <c>.bronze.lahman_batting;                             -- all STRING
```

## 4. Silver, then its quality check
Run in order: `silver/10_player.sql`, `11_franchise_park.sql`, `12_team_season.sql`, `20_batting_pitching.sql`, `30_salary_award.sql`.
Then run `dq/silver_checks_combined.sql`: one grid, errors first. Any `error` row with violations above 0 means stop and fix before gold.
Expected warnings on the real data: 8 team rows where wins + losses exceed games, and 2 zero salaries.

## 5. Gold, then its quality check
Run `gold/10_dimensions.sql`, then `gold/20_facts.sql`, then `dq/gold_checks_combined.sql` (all violations should be 0).
Spot check: Barry Bonds 2001 should show 73 home runs and a .328 average (query in the plan, Block 4).

## 6. Metric views
Run `metric_views/10_mv_batting.sql`, `20_mv_pitching.sql`, `30_mv_team_season.sql`, then:
```sql
SELECT season, MEASURE(home_runs) AS hr, MEASURE(ops) AS ops
FROM <c>.semantic.mv_batting WHERE season >= 1990 GROUP BY season ORDER BY season;
```
If the `CREATE VIEW ... WITH METRICS` statement errors, note the message: it usually means your compute or workspace does not support that YAML version yet.

## 7. Governance (all optional, each can fail independently)
- `manual/02_glossary.sql` (needs the glossary CSV uploaded).
- `governance/10_comments.sql`, then `20_tags.sql` (run statement by statement; skip any you lack permission for).
- Skip `30_grants.sql` (needs a group) and `40_certification.sql` (needs special permission).

## 8. Re-running
Every silver and gold file uses `CREATE OR REPLACE TABLE`, so you can re-run any file. Bronze manual SQL rebuilds from the files in the folder.
If you add a file to a Volume folder, bronze picks up both files (silver removes duplicate rows).
