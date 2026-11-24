## Architecture and the decisions behind it

The pipeline is a launcher, a runner and one script per layer, all steered by contracts. Read this chapter once now, and again after Lab 4, when each decision will have a concrete example behind it.

**How one run flows**

1. A Databricks Job starts a Python script task that runs `run_pipeline.py`, the **launcher**, with command-line arguments (catalog, folders, which steps).
2. The launcher makes the code importable from `code/lakehouse.zip` and hands over to the **runner**.
3. The runner reads every contract YAML in the contracts folder and works out the **plan**: which steps exist and in what order, from each contract's `dependsOn` and foreign keys.
4. It runs `setup` (create governance objects), then `contracts` (lint every contract, record the versions, compile the rules).
5. It runs the plan's steps: `bronze:<entity>`, `silver:<entity>`, `gold:<entity>`, `gold-publish`, `semantic:<view>`. Each step is one small function in its own file.
6. Every step writes its evidence to governance tables under the run's `run_id`.
7. It always finishes with `report`, even after a failure, and ends the Job in failure if anything was rejected or broken.

**What lives where**

| File or folder | Role | Who changes it |
| --- | --- | --- |
| `run_pipeline.py` | The launcher a Job runs. Makes the code importable, then calls the runner | Almost never |
| `code/lakehouse.zip` | All the Python: runner, stages, planner, engine, contract library | When behaviour changes; rebuilt by the kit builder |
| `contracts/*.yaml` | The ODCS contracts. The source of truth for schema, rules, order | **You** |
| `transforms/gold/<entity>.sql` | One SELECT per gold table, from clean silver | **You** |
| `definitions/*.sql` | Metric view definitions | You (optional lab) |
| Landing folder, e.g. `fbl/teams/teams.csv` | The delivered files | The data source (you, by hand, for now) |
| `reports/` | One HTML report per run | The pipeline |

**The decisions, with what each one costs**

| Decision | Why | What it costs |
| --- | --- | --- |
| **Scripts and a runner, not notebooks** | Plain files diff cleanly in Git, are unit-testable, take arguments, and run identically on a laptop and in a Job. With thousands of tables you want one reviewed code path, not thousands of notebooks to open, attach and run | No inline results for exploring. Keep a SQL editor for ad hoc questions |
| **A tiny launcher plus a code zip** | The launcher never changes; the code is one versioned artifact. A CI job can later build and publish that zip with no other change | You rebuild and re-upload the zip when the code changes. A wheel is the next step (chapter at the end) |
| **Two Volumes: code and data** | Code and data have different owners, permissions and lifecycles, and you do not want a data engineer able to overwrite the pipeline code by mistake | Two places to upload to |
| **Contracts read as YAML directly** | The file you wrote is the file that runs. No generated bundle to forget to rebuild | The compute needs PyYAML. It is documented in serverless environment 5; Lab 0 confirms it |
| **The plan comes from the contracts** | The order of tables is already written down in `dependsOn` and foreign keys. Repeating it in a script means two places to keep right | Contracts must declare relationships truthfully. Lint checks that they do |
| **One entity per step** | You can rerun just `silver:player`, retry one table alone, see exactly which table failed, and later run independent steps in parallel | More steps to schedule. They are cheap |
| **Rules are data, compiled from the contract** | One engine applies every rule the same way; adding a check is adding a contract line | An extra level of indirection when debugging |
| **Rejected rows go to a quarantine, never deleted** | A steward can see exactly what the source sent and why it was refused | The quarantine grows and needs a lifecycle (chapter at the end) |
| **Write, audit, publish for gold** | Gold is built and checked in a staging schema; a failed audit leaves the published tables as they were | Storage for a second copy; a third step to understand |
| **Stop on a hard failure, skip what depended on it, always report, then fail** | The report is the explanation, and a green run must mean nothing was silently set aside | Independent branches stop too (improvement noted at the end) |
| **Record the contract versions used in every run** | Months later you can say which promise was in force on any run | A few rows per run |
| **PySpark for every transformation** | The same code scales from 84 rows to billions; you learn the real tool now | Overhead that is pointless at this size, accepted on purpose |

**What the runner deliberately does not do yet.** It runs steps one after another (no parallelism), rebuilds each table fully every time (no incremental loads), does not retry a failed step, and loads whole files rather than watching for new ones. Each of these is a clean next step and is covered in the last chapter; none changes the structure you learn here.
