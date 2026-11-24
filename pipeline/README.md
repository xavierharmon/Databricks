# MLB Lakehouse: learn Bronze / Silver / Gold / Metric Views / Unity Catalog / CI-CD / Ardoq

A complete, runnable Databricks project built on **real MLB data** (Lahman Baseball Databank, 1871-2020).
Bronze is raw CSV. You model, then build silver, gold, and governed metric views, then ship it all with
Databricks Asset Bundles + GitHub Actions and publish the metadata to Ardoq.

```
data/lahman/            8 raw CSVs  = your BRONZE source (strings, blanks, cryptic names)
data/glossary/          business glossary as code (CSV)
sql/setup/              catalog, schemas, landing volume
sql/silver/             typing, cleaning, dedupe, constraints
sql/dq/                 data-quality gates (error blocks the job, warn is recorded)
sql/gold/               star schema: dim_* and fact_*
sql/metric_views/       semantic layer (metric view YAML 1.1)
sql/governance/         comments, tags, grants, certification
src/notebooks/          thin notebooks that run the SQL (this is what the job executes)
src/lib/sql_runner.py   renders {{catalog}} and runs cell-separated SQL files
resources/              the Databricks job (the DAG)
databricks.yml          the bundle: dev / staging / prod targets
.github/workflows/      ci.yml (pull requests) and deploy.yml (merge to main)
ardoq/                  Unity Catalog -> Ardoq sync + metamodel guide
tests/                  pytest suite + local Spark run of the whole pipeline
docs/EXERCISES.md       hands-on labs
```

## Quick start (about 15 minutes)

**0. Prove it works without Databricks** (needs Python 3.10+ and Java 17+):
```bash
pip install -r requirements-dev.txt
pytest -q                              # static tests
python tests/local_pipeline_check.py   # runs silver -> DQ -> gold -> metric-view expressions on the real CSVs
```

**1. Deploy to your workspace** (needs a recent Databricks CLI and serverless compute enabled):
```bash
databricks auth login --host https://<your-workspace-url>
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run mlb_medallion -t dev      # creates catalog mlb_dev and builds everything
```
If `CREATE CATALOG` is blocked in your workspace, create `mlb_dev` once in Catalog Explorer, then re-run.

**2. Explore:** Catalog Explorer > `mlb_dev` > bronze / silver / gold / semantic. Open `semantic.mv_batting`, then try:
```sql
SELECT season, MEASURE(home_runs) AS hr, MEASURE(ops) AS ops
FROM mlb_dev.semantic.mv_batting
WHERE season >= 1990
GROUP BY season ORDER BY season;
```

**Pasting SQL into the SQL editor?** The editor flags `{{catalog}}` as a legacy parameter. Run `python tools/render_sql.py --catalog <your_catalog>` and paste from `rendered/<your_catalog>/` instead.

**No CLI or GitHub (manual mode)?** Follow `docs/MANUAL_MODE.md`: everything runs in the SQL editor plus one file upload.

**Want to add PySpark?** See `docs/PYSPARK_STARTER.md` (profiling, loops, a quality gate that fails, and a notebook that runs your SQL files).

**Data-quality pipeline with dirty fictitious data?** See `docs/DQ_PIPELINE.md` and `docs/DQ_ANSWER_KEY.md` (quarantine, auto-fixes, gold NULL guarantee, report + notification).

**Data contracts (source > silver > gold > semantic) enforced by the pipeline?** See `docs/DATA_CONTRACTS.md` and the `contracts/` folder.

**3. Learn:** follow the weekend plan, then `docs/EXERCISES.md`.

## CI/CD setup (GitHub)
Repository secrets: `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` (a service principal with
OAuth M2M), `ARDOQ_API_TOKEN`. Repository variables: `DATABRICKS_WAREHOUSE_ID`, `ARDOQ_CONFIG_YAML`.
Create GitHub Environments `staging` and `prod`; add **required reviewers** to `prod`.
Grant the service principal: `CREATE CATALOG` (or pre-create `mlb_stg` / `mlb_prod` and grant ALL PRIVILEGES), and
`CAN USE` on a SQL warehouse for the Ardoq sync.

Flow: PR -> `ci.yml` (tests, validate, deploy to **staging**, run job incl. DQ gates) -> merge -> `deploy.yml`
(approval, deploy to **prod**, run job, sync Ardoq).

## Data license
Lahman / Chadwick Bureau Baseball Databank, CC BY-SA 3.0. See `data/lahman/ATTRIBUTION.md`.

## Script runner edition (no notebooks)

A second way to run the FBL pipeline: plain Python scripts and a runner, steered by ODCS contracts you write yourself.

```
run_pipeline.py              launcher a Databricks Job (Python script task) runs
lakehouse/                   runner, planner, governance plumbing, one script per layer (stages/)
transforms/gold/*.sql        one SELECT per gold table
student/contracts/           where you write your own contracts (reference answer key: contracts/)
tools/build_runner_kit.py    builds dist/runner_kit: the exact folders to upload to your Volumes
tools/contract_tool.py       lint | plan | typed-sql | compare --mine | stamp | diff | export-rules
tests/check_runner.py        end-to-end local checks (plan, failures, write-audit-publish, zip launcher)
```

Quick start on your computer:

```bash
python tools/contract_tool.py plan                          # the execution plan derived from the reference contracts
python tools/build_runner_kit.py                            # then upload dist/runner_kit as described in UPLOAD_MAP.md
python run_pipeline.py --catalog mlb_dev --pipeline-root <Volume path> --data-root <Volume path> --plan-only true
```

The labs are in `docs/Script_Runner_Lab_Handbook.pdf` (also copied into the kit; the Markdown source is in `docs/script_runner_handbook_md/`, and an editable online edition exists as a Claude Doc). They build the pipeline one entity at a time. The older notebook
route (`src/notebooks/`, `docs/`) still works and is covered by its own tests.
