## Using this handbook with the runner kit

This handbook is the lab guide for the script runner pipeline. The project gives you the code, the upload kit and the reference contracts; this handbook tells you what to do with them, step by step, and why.

**What you have**

| You have | What it is | Where it is |
| --- | --- | --- |
| `mlb-lakehouse.zip` | The whole project: runner, stage scripts, tools, tests, reference contracts, sample data | Unzip it on your computer |
| `runner_kit.zip` | Only the files to upload to Databricks, already laid out in the right folders | Also in the project at `dist/runner_kit/` |
| `UPLOAD_MAP.md` | A table of every kit file and its destination Volume path | Inside the kit |
| This handbook | The labs: what to write, run, check and break | You are reading it |
| `student/contracts/` | An empty folder where you write your own contracts | Inside the project |
| `contracts/` | The reference contracts, the answer key | Inside the project |

**The working loop you will repeat in every lab**

1. **Write** a contract (YAML) or a transform (SQL) on your computer, in `student/contracts/` or `transforms/gold/`.
2. **Check** it on your computer with the contract tool: `lint`, `compare --mine student/contracts`, `plan`, `typed-sql`.
3. **Upload** the file to the Volume folder named in the lab (Catalog Explorer, Upload to this volume).
4. **Run** the Databricks Job with the parameters the lab gives (`steps`, `entities`, `fail_on_rejects`).
5. **Read the evidence** with the SQL queries in the lab, and compare with the expected numbers.
6. **Break it on purpose** with the drill at the end of the lab, so you learn what each check catches.

**Commands you will use on your computer** (run from the project folder; needs Python 3.10+ and `pip install pyyaml`)

```
python tools/contract_tool.py --contracts student/contracts lint
python tools/contract_tool.py compare --mine student/contracts
python tools/contract_tool.py --contracts student/contracts plan
python tools/contract_tool.py --contracts student/contracts typed-sql team
python tools/contract_tool.py --contracts student/contracts stamp
python tools/build_runner_kit.py
```

**Where the labs differ from the online edition.** This handbook contains the same labs as the online Script Runner Edition, a living document you can edit and comment on. If the two ever differ, treat the online edition as the newer one.

**What is verified and what is not.** The runner, the stage scripts, the contract tools, the failure handling and the zip launcher were run end to end on a local Spark session against the sample files, and the messages and counts quoted in the labs come from running those exact changes. **Nothing has run in your Databricks workspace yet.** Lab 0 is designed to find any plumbing problems cheaply, before you write a single contract.
