# Runbook: contract-driven lakehouse (fbl + tix)

One document for operating this project end to end. It replaces the three-part document series. Each phase of the roadmap adds or
fills a chapter, so this file is written while the details are fresh.

**How to read the status tags**

| Tag | Meaning |
|---|---|
| `[workspace-verified]` | Run in the real Databricks workspace and matched expectations. |
| `[sandbox-verified]` | Run on Linux with local Spark 3.5.1 and Java 21 (not Windows, not Databricks). |
| `[unverified]` | Designed and reasoned, never run. Treat as a hypothesis until it is. |
| `[TODO]` | Chapter or step not written yet (the roadmap phase that fills it is named). |

**Key numbers** (a run that does not produce these has changed behaviour: stop and find out why)

| Product | Check | Value |
|---|---|---|
| fbl | rows rejected / gold tables | 46 / 8, 31, 53 rows, ending in the intentional KICK BACK `[workspace-verified]` |
| tix (seed 7) | valid ledger rows | 2,319 |
| tix | fee revenue / owned-inventory profit / total earnings | 26,846.06 / 3,112.75 / 29,958.81 |
| tix | closed sales ever / now | 620 / 619 |
| tix | plans closed / open | 107 / 193 |
| both | `--product fbl,tix --plan-only` | 53 numbered lines (50 steps + setup + contracts + report) `[sandbox-verified]` |
| both | contracts | 51 (13 source, 13 silver, 11 gold tix + 14 fbl/lahman) |

---

## 1. Environments and where things live

| Thing | Value |
|---|---|
| Repo | `xavierharmon/Databricks`, project under `pipeline/`, working branch `platform-v3` |
| Catalog | `pipeline_mlb_dev` (the bundle's default variable is `mlb_dev`) |
| Runtime root (Volume) | `/Volumes/pipeline_mlb_dev/governance/pipeline` |
| Schemas | bronze, silver, gold, semantic, governance, ml, plus `*_stage` for write-audit-publish |
| Runtime reads from | the Volume copy written by `tools/sync_to_volume.py`, **not** from Git. `VERSION.json` says exactly which code ran |

Rules that follow from the design:
1. Contract edits belong in Git. A pruned sync folder is an exact mirror of the repo, so a hand edit in the Volume is deleted by the next sync.
2. Every run rebuilds all tables from all landed files (until incremental load lands: chapter 9).
3. Entity names are unique across products. Gold reads silver or *staged* gold, never published gold.
4. `generate` and `stage --reset` truncate.

---

## 2. First-time setup

### 2.1 Your laptop (Windows, PowerShell)
```powershell
cd path\to\Databricks\pipeline
python -m pip install pytest pyyaml
python -m pytest -m "not spark"        # the fast tier, no Spark needed
python tools/contract_tool.py lint     # expect: 51 contracts, 0 errors
```
Use `python -m pytest`, not bare `pytest`: on Windows the `pytest` command is often not on PATH.

### 2.2 A Spark environment for local Tier B tests (optional)
PySpark 3.5 needs Python 3.8 to 3.12. Python 3.14 will not do, and Java 8 "Client VM" (32-bit) is unreliable. Options, easiest first:
Conda (`conda create -n lakehouse python=3.11 openjdk=17`), WSL2 Ubuntu, GitHub Codespaces, Docker. Then
`python -m pip install pytest pyyaml pyspark==3.5.1` and `python -m pytest -m spark`. `[sandbox-verified]` on Linux; Windows needs a
`winutils` workaround only for tests that write tables `[unverified]`.

### 2.3 Workspace
Serverless compute, Unity Catalog, the Volume above. Pass plain `/Volumes/...` paths everywhere (no `dbfs:` prefix) `[workspace-verified]`.

---

## 3. Daily operations

Normal cycle:
```
edit contract / SQL / policy  ->  lint + tests (local)  ->  commit  ->  sync to Volume  ->  run Job  ->  read the report
```
1. `python tools/contract_tool.py lint`
2. `python -m pytest -m "not spark"`
3. `python tools/sync_to_volume.py --dry-run`, then the real sync (chapter 4).
4. Run the pipeline Job. Pass `--env dev` to the pipeline task so policy overrides apply (chapter 6).
5. Read the result: `governance.v_dq_summary`, `v_dq_open_rejects`, `v_contract_failures`, `pipeline_step_log`.

A run **ends in failure on purpose** when rows were rejected or a contract is broken (`--fail-on-rejects true`). The FBL run ending in the
intentional "KICK BACK" is the expected outcome. The report is always written first.

Failure policy: `--failure-policy stop` (default: skip everything after the first failure) or `isolate` (skip only steps that depend on a
failed one). Both still run the report. `[sandbox-verified]` by `tests/test_runner_policy.py`.

---

## 4. Deploy and sync

`tools/sync_to_volume.py` (config: `deploy/sync.yml`):
1. **Lints first.** Contract or policy lint errors stop everything: nothing is copied.
2. Compares by hash, copies changed files, deletes extras where `prune: true`.
3. Writes `VERSION.json` last. The runner logs it at setup.

Flags: `--dry-run`, `--verify`, `--prune`, `--dest`, `--commit`. A healthy dry run: 51 contracts, 0 lint errors `[workspace-verified]`.

What the sync now ships (new in this change): `policy/` (action overrides), `tests/` and `pytest.ini` (so Tier B can run in Databricks).
After a sync, `lakehouse.zip` also contains `lib/policy.py` and `toolkit/run_tests.py`.

---

## 5. Testing: Tier A locally, Tier B in Databricks

**Why two tiers.** Spark is built for clusters, so every Spark test pays JVM start-up and scheduling overhead. Plain-Python logic (plan order,
policy rules, lint) is tested without Spark in seconds. Spark-dependent behaviour is tested with Spark, less often.

| | Tier A | Tier B |
|---|---|---|
| Marker | none (`-m "not spark"`) | `@pytest.mark.spark` (`-m spark`) |
| Needs | Python + pytest + pyyaml | pyspark (+ Java for session tests) |
| Where | **your laptop, every change** | **Databricks**, after a sync (and optionally local) |
| Today | plan (10), policy (28), lint, app tests | runner policies (6), launcher (2), Spark smoke (2); local-only governance test (1) |

### 5.1 Tier A on your laptop
```powershell
cd path\to\Databricks\pipeline
python -m pytest -m "not spark"        # about 1 second; skips are listed by -ra: a skip is not a pass
```

### 5.2 Tier B in Databricks `[unverified]` (every step below has been reasoned, none run in your workspace)
The Job task runs `run_tool.py test`, which calls pytest against the **synced** `tests/` folder, so the tests exercise the same code,
contracts and policy your pipeline runs from.

1. **Sync** (chapter 4). Confirm `tests/`, `pytest.ini`, `policy/` and an updated `code/lakehouse.zip` are in the Volume.
2. **Create a Job** named `lakehouse-tests` (Jobs & Pipelines > Create job), one task:
   - **Task key:** `tests`
   - **Type:** Python script
   - **Script path:** `/Volumes/pipeline_mlb_dev/governance/pipeline/run_tool.py` (point at it the same way your existing `train` and `catalog` tool tasks do)
   - **Compute:** Serverless. Under *Environment and libraries* add the dependencies `pytest` and `pyyaml`.
   - **Parameters:**
     ```
     ["--code-dir","/Volumes/pipeline_mlb_dev/governance/pipeline/code",
      "test","--pipeline-root","/Volumes/pipeline_mlb_dev/governance/pipeline",
      "-m","spark"]
     ```
3. **First run, one test only** (proves the plumbing before anything else): add `"-k","test_spark_session_works"` to the parameters.
   Expect `1 passed`. If it fails with *no module named pytest*, the environment dependencies were not applied.
4. **Full Tier B:** remove the `-k` pair. Expect about **10 passed** and 3 skipped (two placeholders for the WAP and golden-number tests, and the local-only
   governance test, which refuses to run in a workspace because it overwrites `governance.dq_rules`).
5. **Read the result in the task output.** `-ra` prints the reason for every skip. A non-zero pytest exit fails the task. (`run_tool.py` exits only on
   failure, so you do not trip the `sys.exit(0)` problem.)

How it fits the pipeline: run it **after a sync and before the pipeline Job** (add it as the first task of a release Job, or run it by hand). It does
not write to bronze, silver or gold. The suite deliberately contains nothing that writes to your real schemas. When the WAP and golden-number
tests are written they must use a scratch schema; a test that writes to `gold` is a bug.

**Things this depends on that are not verified** (check each on the first run, in this order): serverless script tasks accept environment
dependencies; `DATABRICKS_RUNTIME_VERSION` (or a `/databricks` folder) is present so the `spark` fixture reuses the cluster session; pytest can
read `tests/` from a Volume path.

### 5.3 What the tests guard (explain it to the team in one breath)
Data-quality rules check **today's data**. These tests check that **the machinery which enforces the rules** still works: order, failure
policy, atomic publish, policy overrides. A mutation check (break the code on purpose, watch the right test fail) is how each test earned trust.

---

## 6. Changing what a quality rule does (action policy)

Contracts own rules. `policy/dq_actions.yml` is a thin per-environment layer that can change a rule's **action** (`warn` < `quarantine` < `fail`).

Why it is shaped this way: a second source of truth for rules would let contract and runtime disagree. So the policy can only name an existing silver
rule, only change its action, and lint rejects everything else.

| Rule | Why |
|---|---|
| Silver rules only, declared action `warn`/`quarantine`/`fail` | Gold rules always assert (an override would change nothing); `dedupe`/`standardize` are mechanical corrections |
| Needs `reason` and `approved_by` | Every override is a recorded decision |
| **Downgrade** (more bad data gets through) needs `expires` | Leniency must end. After the date the runner ignores it and uses the contract's action |
| Downgrade is a **breaking change**: MAJOR bump of `version` | It changes future results. Also breaking: removing a stricter override, extending an expiry, widening to another env |
| Any change needs a version change | The file's history is the audit trail |
| Applied only with `--env dev|staging|prod` | No `--env` = no overrides: a run that forgot where it is never inherits another environment's leniency |

### 6.1 Procedure
1. Edit `policy/dq_actions.yml`; bump `version` (MAJOR if it lets more through).
2. `python tools/contract_tool.py lint` (validates the policy too).
3. `python tools/contract_tool.py policy-diff --git-ref origin/main` (classifies the change; fails if the bump is wrong).
4. `python -m pytest -m "not spark"`.
5. Commit, sync, run the Job with `--env dev` first.
6. Confirm in the audit tables (6.2). Check the answer-key numbers moved the way you intended.

### 6.2 Audit trail `[sandbox-verified]` (tables and logic; SQL below not yet run in the workspace)
Three layers: **Git** (who changed the file, when, why), **`governance.dq_policy_history`** (every version of the policy that ever ran, keyed by checksum),
and **`governance.dq_action_overrides`** (one row per override evaluated in every run: declared action, effective action, direction, status, reason,
approver, expiry, policy version and checksum). `governance.dq_rule_results.action` already records the action that actually ran, because the engine reads
the effective rules.
```sql
SELECT * FROM pipeline_mlb_dev.governance.v_dq_overrides_latest;                 -- overrides in the latest run
SELECT version, loaded_at FROM pipeline_mlb_dev.governance.dq_policy_history ORDER BY loaded_at DESC;
SELECT r.rule_id, r.action AS ran_as, o.declared_action, o.reason, o.approved_by, r.violations
FROM pipeline_mlb_dev.governance.dq_rule_results r
JOIN pipeline_mlb_dev.governance.dq_action_overrides o ON o.run_id = r.run_id AND o.rule_id = r.rule_id
WHERE r.run_id = '<run id>';
```
Status values: `applied`, `expired_ignored` (the date passed; the contract's action ran).

### 6.3 Decision on record
PLR-008 (the 900 lb player) stays a `warn` by decision. That is deliberate so SEM-FBLR-004 can demonstrate catch-it-later. Do not "fix" it without
re-baselining the answer key. To experiment, use the commented example in the policy file with `--env dev`.

---

## 7. Failure drills
`[TODO]` (grows with each lab). Ready now:
- **stop vs isolate:** make `bronze:batting` fail; with `stop` everything after it is skipped, with `isolate` only `silver:batting` and its dependents. Both write the report and exit non-zero.
- **Planted policy mistake:** add an override for a rule that does not exist; lint and the runner must both refuse it.
- **Expired downgrade:** set `expires` to yesterday; the run logs `expired_ignored` and the contract's action runs.
- `[TODO]` WAP drill: break a gold audit and confirm published gold is unchanged (needs the WAP test).
- `[TODO]` Delta time travel and `RESTORE`.

## 8. Alert response `[TODO]` (Phase 4)
## 9. Incremental operations: backfill and replay `[TODO]` (Phase 3)
Acceptance test to write first: deliver the tix data in N batches; the end state must equal the key numbers above; re-delivering a batch changes nothing.
## 10. Catalog publish (domains, pages, products) `[TODO]` (Phase 5)
Order: `tags-apply` first (works on Free Edition), then a single domain with `--dry-run`, then pages, then products. Never run `publish` against a workspace
you have not read the dry run for.

---

## 11. Troubleshooting (environment lessons)

| Symptom | Cause and fix |
|---|---|
| `NameError: __file__` | Script tasks run files with `exec`, so `__file__` is undefined. Pass `--code-dir <.../code>` (tested in `test_launcher_works_without___file__`) |
| Task shows failed though the script succeeded | `sys.exit(0)` counts as failure in a Databricks script task. Exit only when non-zero (`run_pipeline.py` and `run_tool.py` already do) |
| Path not found | Use plain `/Volumes/...`, never `dbfs:/...`. `run_tool.py` needs `--code-dir /Volumes/.../code` |
| `pytest` is not recognized (Windows) | `python -m pytest`; install with `python -m pip install pytest` |
| `ModuleNotFoundError: pyspark` when running the runner | The runner imports pyspark at module level even for `--plan-only`. Use a Spark-capable Python |
| `Unknown pytest.mark.spark` warning | `pytest.ini` is not being read. In the repo the file was committed as `pytest.INI`: `git mv pytest.INI pytest.ini` (Linux and CI are case-sensitive) |
| Spark tests: `Cannot find catalog plugin class ... DeltaCatalog` | Delta jar not loaded. Local tests default to plain Spark; set `LAKEHOUSE_TEST_DELTA=1` only with internet access |
| Spark tests: `CREATE Hive TABLE ... not supported` | Needs `spark.sql.legacy.createHiveTableByDefault=false` (set in `tests/conftest.py`) |
| Run fails at setup with `policy error` | `policy/dq_actions.yml` failed validation: the message names the rule and the problem. Fix and re-sync |
| An override seems ignored | Was the task started with `--env`? Check `governance.dq_action_overrides` for `expired_ignored`; check the env list in the override |
| `sync` STOPPED, lint errors | Nothing was copied by design. Fix the named contract or policy error |
| Plan test fails: step count | A contract was added or removed. Update `EXPECTED_STEPS` in `tests/test_plan.py` and the key-numbers table here, on purpose |

## 12. Open items register
Not verified in the workspace: `CREATE VIEW ... WITH METRICS`, real `MEASURE()` return types, Volume bundle reads, `tags-apply`, domain/page publish,
the app inside a workspace, Tier B in Databricks (5.2), the new governance tables on first creation, ODCS validation with an official tool.
Known gaps: no tix semantic contracts yet; corrections/voids not modelled; whole-file reads each run (incremental load is the fix).
