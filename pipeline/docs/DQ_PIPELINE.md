# Data-quality pipeline: Bronze -> Silver -> Gold with checks baked in

A second, self-contained pipeline for a **fictitious league (FBL)** with deliberately dirty data. It shows the full pattern:
**find** problems, **auto-fix** what is unambiguous, **kick back** what is not, and **guarantee** that gold has no NULLs.

```
CSV files (Volume) -> BRONZE (raw strings) -> SILVER typed + RULES -> clean silver table
                                                   |                \-> quarantine table (rejected rows + reasons)
                                                   \-> auto-corrections logged
                      clean silver -> GOLD tables -> GOLD ASSERTIONS (must be NULL-free, unique, no orphans, reconciled)
                      all of it -> REPORT (HTML + table) -> job FAILS if anything was rejected -> notification
```

> **Contract mode (new):** the rules, typing SQL, expected columns and semantic checks now come from data contracts published to
> `governance.data_contracts` (see `docs/DATA_CONTRACTS.md`). The job has two extra tasks: `contracts_publish` (before bronze) and
> `semantic_contracts` (after gold). The notebook widget `rules_source` is `contracts` by default; set it to `csv` for the old behaviour
> that reads `data/dq/dq_rules.csv`. A built-in copy of the contracts means the first run works without uploading anything.

## The ideas

**Three outcomes for a bad row** (the `action` column of a rule):

| action | Used when | What happens | Where you see it |
| --- | --- | --- | --- |
| `dedupe` | Identical duplicate rows | Extra copy removed. Nothing is lost. | quarantine, disposition `auto_corrected` |
| `standardize` | `r` vs `R`, stray spaces, `south` vs `South` | Value fixed, row kept | quarantine, disposition `auto_corrected` |
| `quarantine` | Missing, invalid, or business-rule violation | Row removed from the clean table and **kicked back** | quarantine, disposition `rejected` |
| `warn` | Suspicious but possible (a 900 lb player) | Row kept, logged | quarantine, disposition `warning` |
| `fail` | Gold guarantees | The stage raises an error and the job stops | `dq_rule_results` + job failure |

**Correct only what is unambiguous.** `r` is clearly `R`; a team with two different founded years is not clearly either, so both copies are rejected for a human to decide. That is the difference between `unique` (keeps the first copy) and `unique_strict` (rejects every copy).

**No NULLs in gold, two ways.** (1) Required columns are checked in silver and rows missing them are quarantined. (2) Gold has its own `no_nulls` assertion. Optional descriptive text (`mascot`, `nickname`) becomes `'None'` in gold. Numeric measures are never defaulted: a missing number is a rejection, not a zero.

**Cascade:** a player rejected in silver is missing from the clean player table, so their batting rows fail the referential rule and are rejected too. Parents are processed before children (team, then player, then batting).

**Rules are data, not code.** One row per rule in `governance.dq_rules` (loaded from `data/dq/dq_rules.csv`):

| rule_type | A row violates it when |
| --- | --- |
| `not_null` | any listed column is NULL or blank |
| `allowed_values` | value not in the list, e.g. `'L','R','B'` |
| `range` | value outside `60 AND 84` |
| `custom` | the SQL condition is FALSE (business rules, e.g. `h <= ab`) |
| `referential` | value missing from another table, e.g. `silver.fbl_team.team_id` |
| `exact_duplicate` / `unique` / `unique_strict` | identical row / repeated key keep-first / repeated key reject-all |
| `standardized` | silver cleaning changed the value |
| `no_nulls` | gold: any column is NULL |

## Run it by hand (no CLI)

1. **Upload the data** to your Volume (`<catalog>.bronze.landing`), one folder per file:
   - `fbl/teams/teams.csv`, `fbl/players/players.csv`, `fbl/batting/batting.csv` (from `data/fbl/`)
   - optional: `fbl/rules/dq_rules.csv` (a modified rules file; otherwise the built-in rules are used)
2. **Import the notebook:** Workspace > Import > upload `src/notebooks/fbl_dq_pipeline.py` (a single self-contained file).
3. **Run it:** open the notebook, set `catalog` (e.g. `mlb_dev`), leave `stage = all`, and run all cells.
4. **Expect it to end with an error that starts `KICK BACK`.** That is deliberate: rows were rejected, so the run is marked failed, which is what triggers a notification. Everything before the error still ran, the gold tables exist, and the HTML report is displayed. Set `fail_on_rejects = false` for a report-only run.
5. Compare your numbers with `docs/DQ_ANSWER_KEY.md`: in 15 / deduped 1 / rejected 6 / out 8 for teams, and so on.

## Run it as an automated Job

**In the UI** (Jobs & Pipelines > Create job). Create these notebook tasks (the first table is the original five; in contract mode add `contracts_publish` after `setup` and `semantic_contracts` after `gold_assert`, each with `stage=contracts` and `stage=semantic`, and make `report_and_notify` depend on `semantic_contracts`), all pointing at the imported notebook, serverless compute:

| Task | depends on | Parameters |
| --- | --- | --- |
| `setup` | none | `catalog`, `stage=setup`, `run_id={{job.run_id}}` |
| `bronze` | setup | `stage=bronze` (plus the same `catalog` and `run_id`) |
| `silver_dq` | bronze | `stage=silver` |
| `gold_assert` | silver_dq | `stage=gold` |
| `report_and_notify` | gold_assert | `stage=report`, `fail_on_rejects=true`; set **Run if dependencies: All done** so it always runs |

If silver or gold fails a hard rule, later tasks are skipped, but the report task still runs and tells you why.

**With the CLI:** `resources/fbl_dq_pipeline.job.yml` defines exactly this job. Deploy with `databricks bundle deploy` and run `databricks bundle run fbl_dq_pipeline`.

**Notifications:** the job fails when rows are rejected or a gate fails, so job-level "Notify on failure" email is the simplest alert (uncomment `email_notifications` in the YAML, or add it under the job's notification settings). A second option is a SQL Alert on `SELECT COUNT(*) FROM governance.v_dq_open_rejects` that triggers above 0. I have not verified that email notifications or alerts are available on Free Edition.

## Read the results

```sql
SELECT * FROM governance.v_dq_summary;                       -- rows in / deduped / rejected / out per table, latest run
SELECT * FROM governance.v_dq_open_rejects;                  -- the kick-back list, with the original record
SELECT table_name, rule_id, action, violations, description
FROM governance.dq_rule_results WHERE violations > 0 ORDER BY checked_at DESC, layer, table_name;
SELECT * FROM gold.fbl_dim_player;                           -- clean, NULL-free
```
The HTML report is also saved to `/Volumes/<catalog>/bronze/landing/reports/dq_report_<run_id>.html`.

## The kick-back loop

1. Send `v_dq_open_rejects` to the data owner (the `payload` column is the original record as received, so `N/A` is still `N/A`).
2. Track it: `UPDATE governance.dq_quarantine SET status = 'sent_to_owner', fix_notes = 'emailed Sam, due Fri' WHERE row_id = 11 AND source_table = 'team';`
3. The owner fixes the source file. You re-upload it to the same Volume folder and re-run. Corrected rows flow to gold; fixed rows no longer appear in the next run's rejects.

## Experiments (this is where the learning is)

1. Fix every planted error in the CSVs and re-run. Does it finish green with no rejects?
2. Add your own bad rows: a 17-team league, a batter with 500 home runs, a duplicate with different spacing (`" P001"`).
3. Soften a rule without changing code: `UPDATE governance.dq_rules SET action = 'warn' WHERE rule_id = 'TEAM-004';` then re-run (rules are read from the table each run).
4. Add a rule: insert a row, e.g. `custom` on batting `so <= ab` as a business rule.
5. **Prove gold is the safety net:** deactivate a silver rule (`UPDATE governance.dq_rules SET active = 'N' WHERE rule_id = 'PLR-003';`). The blank last_name now flows into silver and gold, and the `GLD-002` NULL assertion stops the job. Re-enable it afterwards.
6. Reload rules from the CSV: set `reload_rules = true` after editing `fbl/rules/dq_rules.csv` in the Volume.

## How this relates to the main project
The Lahman pipeline uses SQL checks (`sql/dq/*.sql`) that record results and stop on errors. This pipeline adds quarantine, automatic fixes, kick-back reporting and notification. The same engine (`src/lib/dq_engine.py`) can be pointed at Lahman silver tables by adding rules with `layer = silver` and a matching `table_name`, but the stage code in `fbl_pipeline.py` is written for the three FBL tables.

## Limits, stated plainly
- Tested on a local Spark session (all stages, plus the failure paths: rejects raise `KICK BACK`, a NULL in silver makes gold fail). **Not run in your Databricks workspace.** Expect small fixes on first run, for example permissions or the `USE CATALOG` statement.
- Keep-first duplicate handling relies on file order (`monotonically_increasing_id`). Fine for one small file per folder; use an explicit sequence column for multi-file loads.
- FBL bronze tables are rebuilt on every run (not append-only like the Lahman Auto Loader bronze) so each run is easy to reason about.
- Built for learning-sized data. Each rule triggers a Spark job, which is fine here and would need tuning at millions of rows.
- Notification options are described but untested.
