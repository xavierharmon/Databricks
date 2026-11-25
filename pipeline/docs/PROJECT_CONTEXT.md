# Project context: contract-driven Databricks lakehouse (FBL + tix)

Paste or upload this into a Claude web Project as background. It describes what the repo is, how it is wired, current state, and known gaps. Handbooks are supplied separately.

## 1. What this is

A learning-and-reference Databricks project that builds a **medallion lakehouse (bronze -> silver -> gold -> semantic)** driven entirely by **ODCS v3.1-style data contracts written in YAML**. The runner reads contracts, derives the execution plan from them, enforces them, and always writes a report.

Two data products ride on the same engine:

| Product | Data | Purpose |
| --- | --- | --- |
| `fbl` | Fictional Baseball League: teams, players, batting (small CSVs, deliberately dirty, ~41 DQ rules, planted answer key) | Teaching DQ, quarantine, contracts, labs |
| `tix` | Synthetic ticket marketplace (events, venues, participants, tags, ticket ledger, fees, price changes, installment plans/payments, GA web events, customers) produced by `tools/generate_tix_data.py` with an answer key | Larger realistic slice: ownership chain, fees, installment "closed sale" (>=80% recouped), be-backs, identity stitching, ML features |
| `lahman` | Real MLB Lahman data (original notebook/SQL route) | Original metric-view examples; semantic contracts only |

- Repo: GitHub `xavierharmon/Databricks`, working branch `platform-v3` (main is `main`). Path in repo root: `pipeline/`.
- Unity Catalog catalog used: `pipeline_mlb_dev` (bundle default var is `mlb_dev`; synced runtime root is `/Volumes/pipeline_mlb_dev/governance/pipeline`). Schemas: bronze, silver, gold, semantic, governance, ml, plus `*_stage` schemas for write-audit-publish.
- Everything is synthetic/fictional except Lahman (CC BY-SA 3.0).

## 2. Two execution routes

1. **Script runner edition (current, preferred)**: no notebooks. `run_pipeline.py` (tiny launcher) puts `lakehouse.zip` (or the checkout) on `sys.path` and calls `lakehouse.runner.main()`. Run as a Databricks Job *Python script task*. `run_tool.py` is the same idea for helper tools (`generate`, `stage`, `check`, `identity`, `train`, `catalog`).
2. **Older notebook route**: `src/notebooks/*.py`, `src/notebooks/fbl_dq_pipeline.py` (generated single notebook, built by `tools/build_fbl_notebook.py`), `notebooks/fbl_dq_pipeline.ipynb`, job `resources/fbl_dq_pipeline.job.yml` (7 tasks: setup, contracts_publish, bronze, silver_dq, gold_assert, semantic_contracts, report_and_notify), plus `sql/*` numbered SQL files and `resources/mlb_pipeline.job.yml` for Lahman. Still works; `src/lib/fbl_pipeline.py` is its engine.

The `lakehouse/` package and `src/lib/` (shared: `contracts.py`, `contract_checks.py`, `dq_engine.py`, `sql_runner.py`) together are zipped into `code/lakehouse.zip`.

## 3. Runner internals (`pipeline/lakehouse/`)

- `runner.py`: args `--catalog --pipeline-root --data-root --product (fbl|tix|fbl,tix|all) --stages --entities --steps --failure-policy stop|isolate --plan-only`. Sequence: `setup` (ensure schemas + governance tables/views) -> `contracts` (lint, publish to registry, compile rules) -> planned steps -> `report` (always runs). Fails with non-zero exit on failure (does NOT `sys.exit(0)` on success, because Databricks marks that as failed). Failure policy `stop` skips everything after first failure; `isolate` skips only dependents.
- `plan.py`: derives step order and dependencies from contracts: `dependsOn`, `alsoDependsOn`, and foreign-key `relationships` within a layer. Step kinds: `bronze`, `silver`, `gold`, `gold-publish`, `semantic`. Multi-product plans prefix step keys with the product.
- `stages/bronze.py`: read landed files, check expected columns/not-empty, schema conformance against the source contract.
- `stages/silver.py`: typing SELECT **generated from silver + source contracts**; DQ rules compiled from the contract; actions per rule (`quarantine`, `dedupe`, `fix`/standardize, `warn`, etc.); rejects go to a quarantine table.
- `stages/gold.py`: **Write-Audit-Publish**. Each gold table is built from `transforms/gold/<entity>.sql` into `gold_stage`, audited (assertions from rules, reconciliation to silver, SQL audits in `transforms/audits/`, schema conformance); only `gold-publish` copies to `gold` after all audits pass. Gold tables that read other gold tables read the *staged* copies. Gold is NULL-free (optional text labelled `none`).
- `stages/semantic.py`: creates metric views from `definitions/` SQL, then checks the semantic contract: interface (fields/measures exist, types; `strictTypes` off => warning), meaning (`exprHash` fingerprint of formulas), behaviour (`reconcile`, `expect` golden values, `bounds`), service (freshness SLA).
- `stages/report.py`: summary; fails the run on rejected rows (`fail_on_rejects`) or broken contracts.
- `governance.py`: DDL for `governance.*` tables (`data_contracts`, `data_contracts_history`, `dq_rules`, `dq_rule_results`, `dq_table_stats`, `contract_results`, step log, quarantine/rejects) and views (`v_dq_summary`, `v_dq_open_rejects`, `v_contract_status`, `v_contract_failures`).
- `contract_store.py`: loads contracts from the Volume (bundle JSON or YAML) and metric-view definitions.

Stages read the **registry in Databricks**, not git, so what runs is exactly what was published (history keyed by checksum).

## 4. Contracts (`pipeline/contracts/`)

- Layout: `contracts/{source,silver,gold,semantic}/fbl_*.yaml` and `contracts/tix/{source,silver,gold}/tix_*.yaml`. `contracts_reference/` is the FBL answer key for the lab handbook (students write their own in `student/contracts/`, which is not in the repo). `contracts_bundle.json` is the lint-checked bundle (`contract_tool.py bundle`).
- ID convention `<product>.<layer>.<name>`; semver; breaking change needs MAJOR bump. Custom behaviour goes in `customProperties` (`layer`, `owner`, `dependsOn`, `entity`, `reconcileMode/reconcilesTo/reconcileColumn`, `transformSql`, `standardize`...). Rules use quality type `custom`, engine `fbl-dq-engine`, with `implementation: {rule_type, columns, expression, action}`.
- Rule types seen: not_null, unique, allowed_values, range/bounds, referential, exact_duplicate, `join_check` (cross-table values, added for tix), plus SQL audits.
- **Lint enforces schema/rule agreement** (required column needs a not_null rule; foreignKey needs a referential rule; metric-view SQL change without contract stamp fails).
- `tools/contract_tool.py`: `lint | plan | typed-sql | compare --mine | stamp | diff | export-rules | bundle`. `diff --git-ref` classifies breaking changes against version bumps.
- tix contracts are **generated** by `tools/build_tix_contracts.py` from specs in `tools/tix_spec_ledger.py` (and related), then hand-adjusted. Money is DECIMAL(12,2); each source file set is read whole every run (bronze replaces, silver rebuilds), so re-delivered rows/late files hit the normal dedupe/uniqueness rules. Incremental merge is a noted future step.
- Counts: 41 FBL DQ rules (legacy `data/dq/dq_rules.csv` is equivalent and kept for `rules_source=csv`).

## 5. Deployment / sync

- `tools/sync_to_volume.py` + `deploy/sync.yml`: lints first (nothing copies if lint fails), compares by hash, copies contracts (prune: true so the Volume exactly matches the repo), `transforms/gold`, `transforms/audits`, metric view SQL (flattened into `definitions/`), `catalog/` (excluding `principals.yml`), launchers, and builds `code/lakehouse.zip`. Writes `VERSION.json` last (content fingerprint, commit best-effort); the runner logs it at setup. Flags: `--dry-run`, `--verify`, `--prune`, `--dest`, `--commit`. Can itself run as a Databricks Job task from a Git folder.
- `tools/build_runner_kit.py` builds `dist/runner_kit` (+ `UPLOAD_MAP.md`) for manual upload.
- `databricks.yml`: bundle `mlb_lakehouse`, targets dev/staging/prod, vars `catalog`, `analyst_group`, `apply_certification`. The CI/CD workflows (`ci.yml`, `deploy.yml`, `contracts.yml`) are described in README/docs but **`.github/` is not in the repo**.
- Recent commit history themes: tix contracts excluded from deployment, then added back; contracts prune to exact-match the Volume; app updates for Databricks App builder.

## 6. Catalog as code (`pipeline/catalog/`, `tools/catalog_tool.py`)

YAML for **domains** (baseball_operations, events, sales with subdomains sales/ledger and sales/payments, customer with engagement/identity subdomains, data_platform), **glossary pages** (e.g. total-earnings = fee revenue + owned-inventory profit, closed-sale, be-back, identity-stitching, data-contract, quarantine, medallion-architecture, batting-average, OPS), and **data products** (`fbl-league-stats`, `tix-ticket-sales`, `tix-customer-engagement`). `catalog_tool.py`: `lint` (cross-checks against contracts), `render` (Markdown glossary + catalog.json), `tags-sql` / `tags-apply` (works on Free Edition), `publish [--apply]` (documented Domains/Pages Beta APIs; **never run against a real workspace**; needs account-level Discover page). Governed tags must pre-exist. `principals.example.yml` maps owner names to Databricks principal IDs.

## 7. Other tools (`pipeline/tools/`)

`generate_tix_data.py` (synthetic data with answer_key/ and truth/ - never upload `truth/`), `stage_landing.py` (simulate daily batches arriving), `check_tix_data.py`, `identity_reference.py` (reference identity-stitching answer), `train_models.py` (purchase-propensity and installment-risk models; **time split not random**, immature labels dropped, must beat a one-line baseline; results to `governance.ml_model_runs`, MLflow if available, scores to `ml.*`), `make_fbl_data.py`, `render_sql.py` (render `{{catalog}}` for pasting in SQL editor), `build_fbl_notebook.py`.

## 8. Streamlit app (`pipeline/app/`)

Read-only **Databricks App** "Ticket analytics" (`app.py`, `data.py`, `demo.py`, `app.yaml`, `.streamlit/config.toml`, `requirements.txt`). Four tabs: Sales, Installments, Visitors, Models. All SQL lives in `data.py` `QUERIES` (earnings by sale type, closed sales by plan length, be-backs, conversion by price drop, identity methods, top propensity, sales by week/category, model runs); queried via SQL warehouse (`DATABRICKS_WAREHOUSE_ID` from the app resource `sql-warehouse`, `TIX_CATALOG` env, default `pipeline_mlb_dev`). It reads **gold and ml tables, not metric views**. `TIX_DEMO=1` renders invented data for layout preview. Tests: `tests/test_app_data.py`, `tests/test_app_visual.py` (the only tests currently in the repo). Author notes the app has not been proven inside a workspace in the docs' own words.

## 9. Ardoq (`pipeline/ardoq/`)

`sync_to_ardoq.py` pushes Unity Catalog assets (catalog/schema/table/metric view), lineage (`system.access.table_lineage` -> "feeds") and glossary terms (-> "defines") to Ardoq via the transactional Batch API, upsert by `[rootWorkspace, name]`; `--sample` and `--dry-run` modes; `prune` off by default. Needs `config.yml` (copy `config.example.yml`) with Ardoq type/reference IDs and `ARDOQ_API_TOKEN`.

## 10. Docs and handbooks in repo (`pipeline/docs/`)

`DATA_CONTRACTS.md`, `CONTRACTS_CHANGELOG.md`, `DQ_PIPELINE.md`, `DQ_ANSWER_KEY.md`, `EXERCISES.md`, `MANUAL_MODE.md`, `PYSPARK_STARTER.md`, and the **Script Runner Lab Handbook** (PDF + Markdown source in `script_runner_handbook_md/`, chapters 00-08: kit, intro, architecture, Lab 0 setup, Labs 1-2 team source/silver, Labs 3-4 gold + scale player/batting, Labs 5-6 semantic + whole Job, Labs 7-8 failure drills/automation, further reference). The labs build the pipeline one entity at a time and each ends with "break it on purpose".

## 11. Design principles to preserve

- Contracts are the single source of truth; adding a table = adding contracts (+ a gold transform SQL), not editing the runner.
- Fail loudly, but always produce the report. Gold never published until audited.
- Explain "why" in code comments and docs; honest statements of what has/hasn't been verified.
- Silver rules have an explicit action per rule; lint blocks contradictory contracts.
- Keep the runtime reading from a versioned Volume copy (sync + VERSION.json), not from a Git pull.

## 12. Known gaps and caveats (as of 2026-11-25)

- **No tix semantic contracts or metric views yet.** The catalog pages/products reference `tix.semantic.mv_sales`, `mv_installments`, etc. and a `tix.gold.ticket_current_owner` contract, but no matching files exist under `contracts/tix/semantic/` or in the metric-view SQL; `catalog_tool.py lint` may flag this. The app queries gold directly for that reason.
- tix gold contracts present (11): customer_person, customer_tag_affinity, dim_customer, dim_event, fact_event_visit, fact_order_payment_status, fact_purchase_journey, fact_session_identity, fact_ticket_transfer, feature_plan_risk, feature_purchase_propensity. Transforms for each are in `transforms/gold/`. FBL also has dim_player/team/fact_batting.
- **Not tested in Databricks** (per docs): `CREATE VIEW ... WITH METRICS`, real `MEASURE()` return types, reading Volume bundles, PyYAML availability, Domains/Pages publish, app inside a workspace. `strictTypes` for semantic interface checks starts off for this reason.
- ODCS structure follows v3.1 docs but is not validated with an official ODCS tool.
- README is partly stale: it describes `data/lahman/`, `.github/workflows/`, `student/contracts/`, `tests/check_runner.py`, `tests/local_pipeline_check.py`, `tests/test_contracts.py` etc., none of which are in the current repo tree. Treat those as missing/removed unless confirmed.
- `__pycache__/*.pyc` files are tracked in git (show as modified); should be removed from the index and ignored.
- Known data finding: the 900 lb player (P027) is only a `warning` in silver (PLR-008) so it reaches gold; the roster contract test SEM-FBLR-004 surfaces it. Decide whether to promote PLR-008 to `quarantine` (changes the answer key).
- Corrections/voids are not modelled in tix; revenue is reported on sale date.
- Cost is a standing concern of the design: whole-file reads each run scale poorly; incremental merge loads are the planned next step.

## 13. Likely next steps

1. Add tix semantic contracts + metric view SQL (`mv_sales`, `mv_installments`, customer engagement) and point the app at them.
2. Restore/add CI (`ci.yml`, `deploy.yml`, `contracts.yml`) and a test suite for the runner (plan, failure paths, WAP, zip launcher).
3. Run `catalog_tool.py publish` for real once a Discover page is confirmed; wire up Ardoq config.
4. Incremental loads / MERGE for large tables; scheduling and alerting for the Job.
5. Validate contracts against an ODCS tool and flip `strictTypes: true` after first real-workspace run.
6. Untrack `__pycache__`.

## 14. Conventions when helping with this project

- The user's environment is Windows + PowerShell/Git Bash and a Databricks workspace (Unity Catalog; serverless is assumed; Free Edition constraints are considered in places).
- Prefer editing contracts/transform SQL over changing the runner; run `python tools/contract_tool.py lint` and `plan` after contract edits; bump contract `version` when behaviour changes (MAJOR if breaking); re-sync with `tools/sync_to_volume.py` before running a Job.
- Never put real secrets/tokens in the repo (`.gitignore` already blocks common credential files).
