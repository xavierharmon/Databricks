# Hands-on labs

Do these in order. Each one breaks or extends something so you learn why the design is the way it is.

## Bronze
1. **Prove idempotency.** Run the `bronze` task twice. Row counts must not change. Why? (Auto Loader checkpoint.)
2. **Incremental load.** Copy `Batting.csv` to `Batting_2.csv` after deleting 100 rows from it, drop it in
   `/Volumes/<catalog>/bronze/landing/lahman/batting/`, re-run bronze. Only the new file loads; check `_source_file`.
3. **Schema drift.** Add a column `foo` to a copy of `Parks.csv`, land it, re-run. What does `_rescued_data` / schema evolution do?
4. **Full refresh.** Run bronze with `full_refresh=true`. What is rebuilt and what is lost?

## Silver
5. Find a Lahman column that is blank for old seasons (`SF`, `HBP`). Count NULLs in `silver.batting_season`. Why is NULL not 0?
6. Insert a duplicate row into `bronze.lahman_people` (same `playerID`, newer `_ingested_at`). Re-run silver. Which row wins?
7. Try to `INSERT` a batting row with `h > ab` into silver. The CHECK constraint rejects it. Now remove the constraint and let the DQ gate catch it instead. Which approach would you pick for which table?
8. Add a new silver table `hall_of_fame` from Lahman `HallOfFame.csv` (add it to `01_land_files.py`, `02_bronze_ingest.py`, new file in `sql/silver/`).

## Gold
9. Explain the **grain** of every gold table in one sentence. Add the answer to the table COMMENT.
10. Add `dim_park` and a `park_key` to `dim_team_season`. Add a DQ check for it.
11. Create `gold.agg_player_career` (career totals). Why is this a table and not a metric view?
12. **Fan-out lab.** Temporarily duplicate a row in `dim_player`, rebuild, and compare `SUM(hr)` from the fact vs. through the join. Then see the gold DQ gate catch it.

## Metric views
13. Add a measure `extra_base_hits` (doubles + triples + home runs) to `mv_batting`. Query it grouped by `baseball_era`.
14. Query `OPS` for a decade, then for each player in that decade. Confirm the decade OPS is **not** the average of player OPS values.
15. Add a `filter:` so a new view `mv_batting_modern` only includes seasons >= 1994.
16. Make `mv_batting` a source of a second metric view (composability).
17. Add `format` and richer `synonyms`, then ask a Genie space "who hit the most homers in the steroid era?" and inspect the SQL it writes.
18. Add `materialization` for `season x league` aggregates and compare query time.

## Unity Catalog & governance
19. Tag `dim_player.birth_date` as personal (already done). Create a **column mask** that shows only the birth year to everyone except a group.
20. Create a group `analysts` (or use yours), run governance with `analyst_group` set, then log in as a member: what can they see?
21. Use `system.access.table_lineage` to draw lineage from `bronze.lahman_batting` to `semantic.mv_batting`. Compare with the lineage graph in Catalog Explorer.
22. Create a Unity Catalog **domain** called Baseball Operations and assign the gold and semantic schemas to it. Write two glossary entries as UC Pages (check your workspace for the feature).

## CI/CD
23. Break a metric view (typo in a column). Open a PR. Which check fails first, and how fast?
24. Break a DQ rule on purpose in staging. Watch the job stop before `gold`.
25. Add a `schedule:` to the job in `prod` only.
26. Add a path filter so docs-only PRs skip the Databricks deploy.
27. Move to GitHub OIDC federation instead of a client secret.

## Ardoq
28. Run the Ardoq sync with `--sample --dry-run`, read the payload, then point it at your sandbox workspace.
29. Add a new component type `Dashboard` and a `consumes` reference from dashboards to metric views.
