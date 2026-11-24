## How to use this edition

This edition teaches you to build, by hand and one piece at a time, a **contract-driven pipeline that runs as scripts, not notebooks**: bronze to silver to gold, each step checked against an ODCS 3.1 data contract that you write in YAML, executed by a runner script inside Databricks. You finish with a pipeline you understand piece by piece and can later automate end to end.

**Your situation, and how this edition fits it**

| You said | What this edition does |
| --- | --- |
| Uploading data and files by hand is fine for now | Every file goes in through the Volume upload screen; nothing needs the CLI or GitHub |
| Fail the scripts if I miss something | Each step checks its contract and exits with an error; lint rejects a contract that disagrees with itself |
| No notebooks, a runner script inside Databricks | A tiny launcher plus a runner run as a Job's Python script task; all code is plain `.py` files |
| Contracts follow ODCS 3.1 | You write the YAML yourself in the ODCS 3.1 layout, then compare it with a reference set |
| PySpark, built to scale | Every transformation and check is PySpark; the structure is the one a larger estate would use |
| Understand the choices, not just copy code | Every lab explains why before it says what to do, and ends with a way to break it |
| Separate pieces, one per layer | Bronze, silver, gold, semantic and report are separate scripts you can run alone or through the runner |

**What you will build**

1. Two Volumes: one for code, contracts and transforms, one for landed data.
2. A launcher and a code zip that a Databricks Job runs.
3. Contracts, written by you, for the team slice first: source, silver, gold. Then player (adds a foreign key and a cascade) and batting (a composite key and business rules).
4. A runner that works out the order itself from your contracts, stops on a hard failure, always writes a report, and ends in a failure when rows were rejected.
5. Optionally, a semantic contract and metric view, and a scheduled Job with alerts.

**What has and has not been tested.** The runner, the stage scripts, the planner, the contract tooling, the failure handling and the zip launcher were run end to end on a local Spark session against the real sample files (68 unit tests plus 18 end-to-end checks). **None of it has run in your Databricks workspace.** Three platform facts come from Databricks documentation: a Job's Python script task can run a file stored in a Unity Catalog Volume; it passes parameters as command-line arguments (including `{{job.parameters.x}}`); and PyYAML 6.0.2 is part of the serverless base environment version 5. Whether your Free Edition workspace allows Python script tasks, and the exact Volume path form the task accepts, are things you will confirm in Lab 0.

**Prerequisites**

- [ ] A Databricks workspace with Unity Catalog and serverless compute, and a catalog you can write to (examples use `mlb_dev`).
- [ ] Permission to create schemas, Volumes and Jobs.
- [ ] The unzipped project, and Python 3.10+ with `pip install pyyaml` on your computer for the contract tools (you edit contracts locally and upload them). pyspark and Java are needed only if you want to run the project's local tests.
- [ ] On Databricks Free Edition: non-commercial use only, so do not load company data.

**The learning path**

| Lab | You will | You will understand | Time |
| --- | --- | --- | --- |
| 0 | Create Volumes, upload the kit, run a Job in plan-only mode | How a launcher, a code zip and a Job fit together; whether your workspace runs script tasks | 1 h |
| 1 | Write the team source contract, run bronze | What a source contract promises and why bronze stays dumb | 1.5 h |
| 2 | Write the team silver contract, run silver | How a contract generates typing SQL, and how rules reject, repair and log | 2 h |
| 3 | Write the gold contract and transform, run gold | Write, audit, publish; why gold stages first | 1.5 h |
| 4 | Add player, then batting | How the plan grows from the contracts alone; foreign keys and cascades | 2 h |
| 5 | Add the semantic contract and metric view (optional) | Contract tests on meaning, not just shape | 1 h |
| 6 | Run the whole pipeline as one Job | Parameters, selecting steps, schedules, alerts, reading the evidence | 1.5 h |
| 7 | Failure drills and safe contract changes | What each check catches; the version rules | 2 h |
| 8 | Plan the move to full automation | What to automate next, in what order | 45 min |

After the labs there is a chapter on how to take the pipeline further when you have far more data and tables.
