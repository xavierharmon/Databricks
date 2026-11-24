## Taking it further: what to change when you have far more data and tables

The structure you built (launcher, runner, plan from contracts, one script per layer, evidence in tables, write-audit-publish) does not need to change at scale. What changes is how much each piece does, and where it runs. These are the steps, in the order I would take them, each with the reason and how it fits what you built. None of them has been built or tested here.

| # | Change | Why it matters at scale | How it fits the current design |
| --- | --- | --- | --- |
| 1 | **One Job task per step, run in parallel where the plan allows** | Today steps run one after another. With hundreds of independent tables, serial runs take hours, and one slow table delays everything | The plan already knows the dependencies. Generate the Job's task graph from `contract_tool plan` instead of writing it by hand, and add a `--no-report` option plus a final report task |
| 2 | **Incremental loads** | Rebuilding every table from the whole file stops working when history grows | Bronze appends with a `batch_id` (Auto Loader reads only new files); silver applies the rules to the new batch and `MERGE`s the clean rows by key; gold `MERGE`s the changed keys. Duplicate and reference checks must now look at history as well as the new batch |
| 3 | **A quarantine lifecycle** | The same bad row is logged again every run, and nothing records who fixed what | A stable key per rejected record, statuses (open, sent, corrected, waived), and a reprocess step that retries rows once their parent arrives |
| 4 | **Thresholds and circuit breakers** | If 90 percent of a file is rejected, the pipeline still "succeeds" with the rest, which may be an unrepresentative sample | A per-table rule that stops the run when the reject rate or the drop in row count exceeds a limit, set in the contract |
| 5 | **Volume and distribution monitors** | Rules catch bad rows. They do not catch a good-looking, wrong dataset (a file a tenth the usual size, a column that went empty) | Baselines in a table; compare each run; start with row counts and null rates |
| 6 | **Per-entity retries, timeouts and ownership** | Transient failures should retry; data failures should not. Each table has a different owner to alert | Task-level settings once steps are separate tasks; an `owner` already exists in every contract |
| 7 | **Ordering and timestamps for duplicates** | "Keep the first copy" depends on file order, which means nothing across many files | A business timestamp or sequence column named in the contract and used by the dedupe rule |
| 8 | **Surrogate keys and history in dimensions** | Source keys change, and "what was true then" matters | Hash surrogate keys in gold (the MLB gold already uses them); Type 2 slowly changing dimensions where history matters |
| 9 | **Package the code as a wheel** | A zip on a path is simple; a versioned wheel is installed and pinned like any library | Same modules; the Job installs the wheel, the launcher disappears or shrinks further |
| 10 | **Identity and permissions** | Real workloads should not run as a person | Run the Job as a service principal; give it only the schemas it writes; separate code and data permissions (the two Volumes already allow this) |
| 11 | **Observability** | Problems should be noticed before a consumer notices | Dashboards over the governance tables: reject rate per table per day, top violated rules, age of open rejects, step durations, contract failures |
| 12 | **Consider declarative pipelines for the data movement** | They provide incremental processing, ordering and retries as a managed service, with expectations that map onto warn, drop and fail | Keep the contracts, the quarantine pattern and the report; let the platform run the movement. Verify the current feature set first; I have not built this variant |

**What would not change.** The contracts as the source of truth, the plan derived from them, the separation of stages, the evidence tables keyed by run, the write-audit-publish gold, and the failure policy. If you keep those, the pipeline can grow ten-thousand-fold without anyone having to relearn how it works.

**Known limits today**

- Steps run serially and stop at the first hard failure, including steps that did not depend on it.
- Every table is rebuilt in full on every run.
- A mid-way failure of `gold-publish` could leave some gold tables published and others not (each table's overwrite is atomic, the set is not).
- The `gold_stage` tables are left in place after each run; they are a debugging aid, not a cleanup problem yet.
- The MLB contracts are not run by this runner (it runs one product at a time; use `--product`).
- Nothing has been run in your Databricks workspace; the first run there will likely need small fixes, and the plan-only run in Lab 0 is designed to find them cheaply.

## Reference

**Command-line arguments (launcher and runner)**

| Argument | Meaning | Default |
| --- | --- | --- |
| `--catalog` | Unity Catalog catalog | required |
| `--pipeline-root` | Volume folder with `contracts/`, `transforms/`, `definitions/`, `code/` | required |
| `--data-root` | Volume folder with the landed files and `reports/` | required |
| `--contracts-dir`, `--transforms-dir`, `--definitions-dir`, `--landing-dir` | Override one of the folders | derived from the roots |
| `--code-dir` | Folder holding `lakehouse.zip` | `<pipeline-root>/code` |
| `--run-id` | Ties one run's evidence together | a timestamp |
| `--product` | Which `dataProduct` to run | `fbl` |
| `--steps` | Exact steps, such as `silver:team,gold-publish`, or `all` | all |
| `--stages` | `bronze`, `silver`, `gold`, `semantic`, or `all` | all |
| `--entities` | Limit to some entities, or `all` | all |
| `--plan-only` | `true` prints the plan and stops | `false` |
| `--fail-on-rejects` | `true` ends the run in failure when rows were rejected or a contract check is broken | `true` |
| `--local-mode` | Tests on open-source Spark only | off |

**Volume layout**

```
/Volumes/<catalog>/governance/pipeline/
  run_pipeline.py
  code/lakehouse.zip
  contracts/                 your ODCS YAML
  contracts_reference/       the answer key
  transforms/gold/<entity>.sql
  definitions/<view>.sql
/Volumes/<catalog>/bronze/landing/
  fbl/teams|players|batting/<file>.csv
  reports/pipeline_report_<run_id>.html
```

**Steps the runner knows**

| Step | What it does |
| --- | --- |
| `setup` | Creates schemas (including `gold_stage`), governance tables and views |
| `contracts` | Lints every contract, records versions, compiles the rules |
| `bronze:<entity>` | File to bronze, checked against the source contract |
| `silver:<entity>` | Types, cleans, applies the rules, quarantines rejects |
| `gold:<entity>` | Builds into `gold_stage` and audits |
| `gold-publish` | Copies audited tables into `gold` |
| `semantic:<view>` | Creates a metric view and tests it |
| `report` | Summarises, writes the HTML, decides whether the run failed |

**Governance tables written**

| Table | One row per |
| --- | --- |
| `pipeline_step_log` | step per run, with status and detail |
| `dq_rules` | rule, compiled from the contracts |
| `dq_rule_results` | rule per run, with violations |
| `dq_table_stats` | table per run: in, deduped, rejected, out |
| `dq_quarantine` | affected row per run, with the original record |
| `data_contracts`, `data_contracts_history` | contract (current, and every version published) |
| `contract_results` | contract check per run |

**Contract tool commands** (`python tools/contract_tool.py [--contracts DIR] <command>`): `lint`, `plan`, `typed-sql <entity>`, `compare --mine DIR`, `stamp`, `diff --git-ref main`, `export-rules`, `bundle` (only for the older notebook path).

**Platform facts this edition relies on, from Databricks documentation:** a Python script task can run a file stored in a Unity Catalog Volume; its parameters are passed as command-line arguments; `{{job.parameters.name}}` can be used inside them; `{{job.trigger.type}}` can report a `file_arrival` trigger; and the serverless base environment version 5 lists PyYAML 6.0.2. **Everything else in the labs about your workspace is to be confirmed by running it.**
